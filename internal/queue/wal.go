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
)

// record is one WAL entry: the full new state of a job or a lane. Replaying is
// "last write wins", so applying a record twice is harmless — which is what makes
// a crash between writing a snapshot and truncating the log safe.
type record struct {
	Job  *Job       `json:"job,omitempty"`
	Lane *LaneState `json:"lane,omitempty"`
}

// snapshot is the whole queue at one point in time.
type snapshot struct {
	NextID int64       `json:"next_id"`
	Jobs   []*Job      `json:"jobs"`
	Lanes  []LaneState `json:"lanes"`
}

const (
	walName      = "wal.log"
	snapshotName = "snapshot.json"
)

// wal is an append-only log. Each line is "<crc32 hex> <json>\n"; a line whose
// checksum doesn't match, or that has no newline, is a torn write from a crash.
type wal struct {
	f *os.File
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
	return &wal{f: f}, n, nil
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

// append writes the records in one write and fsyncs, so a change is durable
// before the caller applies it in memory or answers a client.
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
	if _, err := w.f.Write(buf.Bytes()); err != nil {
		return err
	}
	return w.f.Sync()
}

// reset empties the log after a snapshot has made its contents redundant.
func (w *wal) reset() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	_, err := w.f.Seek(0, io.SeekStart)
	return err
}

func (w *wal) close() error { return w.f.Close() }

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
