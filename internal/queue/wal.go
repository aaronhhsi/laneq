package queue

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// record is one WAL entry: the full new state of a job or a lane. Replaying is
// "last write wins", so applying a record twice is harmless — which is what makes
// a crash between writing a snapshot and truncating the log safe.
type record struct {
	Job      *Job       `json:"job,omitempty"`
	Lane     *LaneState `json:"lane,omitempty"`
	Schedule *Schedule  `json:"schedule,omitempty"`
}

// snapshot is the whole queue at one point in time.
type snapshot struct {
	NextID    int64       `json:"next_id"`
	Jobs      []*Job      `json:"jobs"`
	Lanes     []LaneState `json:"lanes"`
	Schedules []Schedule  `json:"schedules,omitempty"`
}

const (
	walName      = "wal.log"
	snapshotName = "snapshot.json"
)

// wal is an append-only log. Each line is "<crc32 hex> <json>\n"; a line whose
// checksum doesn't match, or that has no newline, is a torn write from a crash.
//
// Group commit: append only adds records to an in-memory buffer (under the
// queue lock, so log order is the order changes were applied), and syncTo waits
// until they're durable. Whoever finds no flush in flight becomes the leader:
// it takes the whole buffer, writes it to the file and fsyncs, while everyone
// else waits on it — so under load, one write+fsync covers many operations.
// Doing the file write in the leader too, not under the queue lock, matters on
// Windows, where WriteFile blocks while a FlushFileBuffers on the file runs.
type wal struct {
	f       *os.File
	written atomic.Int64 // logical bytes ever appended; never goes back, even when the file is truncated

	smu     sync.Mutex
	cond    *sync.Cond
	buf     []byte // appended, not yet written to the file
	synced  int64  // every record up to this offset is durable
	syncing bool
	err     error // a failed write or fsync: the log can't be trusted, so every later write fails
}

// openWAL replays the log, calling apply for every intact record, and truncates
// any torn tail so new appends start on a clean line. It returns the record count.
func openWAL(dir string, apply func(record)) (*wal, int, error) {
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, 0, err
	}
	r := bufio.NewReader(f)
	var good int64
	n := 0
	for {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break // a partial final line (no newline) is a torn write; dropped below
		}
		if err != nil {
			f.Close()
			return nil, 0, err
		}
		rec, ok := decodeLine(line)
		if !ok {
			break
		}
		apply(rec)
		good += int64(len(line))
		n++
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, 0, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, 0, err
	}
	if err := f.Sync(); err != nil { // what was replayed must be durable before it's built on
		f.Close()
		return nil, 0, err
	}
	w := &wal{f: f, synced: good}
	w.cond = sync.NewCond(&w.smu)
	w.written.Store(good)
	return w, n, nil
}

func decodeLine(line []byte) (record, bool) {
	var rec record
	line = bytes.TrimSuffix(line, []byte("\n"))
	sum, body, found := bytes.Cut(line, []byte(" "))
	if !found || len(sum) != 8 {
		return rec, false
	}
	if fmt.Sprintf("%08x", crc32.ChecksumIEEE(body)) != string(sum) {
		return rec, false
	}
	if err := json.Unmarshal(body, &rec); err != nil {
		return rec, false
	}
	return rec, true
}

// append adds records to the log. They aren't durable until syncTo returns for
// the offset after them.
func (w *wal) append(recs ...record) error {
	var buf bytes.Buffer
	for _, rec := range recs {
		body, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Fprintf(&buf, "%08x ", crc32.ChecksumIEEE(body))
		buf.Write(body)
		buf.WriteByte('\n')
	}
	w.smu.Lock()
	defer w.smu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.buf = append(w.buf, buf.Bytes()...)
	w.written.Add(int64(buf.Len()))
	return nil
}

// syncTo returns once everything appended up to off is durable.
func (w *wal) syncTo(off int64) error {
	w.smu.Lock()
	defer w.smu.Unlock()
	for w.synced < off && w.err == nil {
		if w.syncing { // a flush is in flight; wait, then see whether it covered us
			w.cond.Wait()
			continue
		}
		w.syncing = true
		data, target := w.buf, w.written.Load() // everything appended so far rides along
		w.buf = nil
		w.smu.Unlock()
		_, err := w.f.Write(data)
		if err == nil {
			err = w.f.Sync()
		}
		w.smu.Lock()
		w.syncing = false
		if err != nil {
			// A partial write leaves a torn line, and replay stops at the first one,
			// so anything appended after it would be lost: refuse all further writes.
			w.err = err
		} else {
			w.synced = max(w.synced, target)
		}
		w.cond.Broadcast()
	}
	return w.err
}

// reset empties the log after a snapshot has made its contents redundant:
// everything appended so far is durable in the snapshot. It waits out an
// in-flight flush first — records written after the truncate would land after
// the snapshot and, replayed over it, roll state back.
func (w *wal) reset() error {
	w.smu.Lock()
	defer w.smu.Unlock()
	for w.syncing {
		w.cond.Wait()
	}
	w.buf = nil
	if err := w.f.Truncate(0); err != nil {
		w.err = err
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		w.err = err
		return err
	}
	w.synced = max(w.synced, w.written.Load())
	w.cond.Broadcast()
	return nil
}

func (w *wal) close() error {
	return errors.Join(w.syncTo(w.written.Load()), w.f.Close())
}

func readSnapshot(dir string) (*snapshot, error) {
	data, err := os.ReadFile(filepath.Join(dir, snapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	return &s, nil
}

// writeSnapshot replaces the snapshot atomically: write a temp file, fsync, rename.
func writeSnapshot(dir string, s *snapshot) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, snapshotName+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, snapshotName))
}
