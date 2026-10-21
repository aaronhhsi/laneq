package queue

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newClock() *clock { return &clock{t: time.Date(2026, 10, 21, 23, 0, 0, 0, time.UTC)} }

func open(t *testing.T, dir string, c *clock) *Queue {
	t.Helper()
	q, err := Open(Options{Dir: dir, Now: c.Now, Jitter: func() float64 { return 0.5 }})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func newQueue(t *testing.T) (*Queue, *clock, string) {
	t.Helper()
	dir, c := t.TempDir(), newClock()
	q := open(t, dir, c)
	t.Cleanup(func() { q.Close() })
	return q, c, dir
}

func enqueue(t *testing.T, q *Queue, req EnqueueRequest) Job {
	t.Helper()
	j, _, err := q.Enqueue(req)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func claim(t *testing.T, q *Queue, types ...string) *Lease {
	t.Helper()
	l, err := q.Claim("w1", types, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func mustClaimID(t *testing.T, q *Queue, want int64) *Lease {
	t.Helper()
	l := claim(t, q)
	if l == nil {
		t.Fatalf("claim: got nothing, want job %d", want)
	}
	if l.Job.ID != want {
		t.Fatalf("claim: got job %d, want %d", l.Job.ID, want)
	}
	return l
}

func mustClaimNone(t *testing.T, q *Queue) {
	t.Helper()
	if l := claim(t, q); l != nil {
		t.Fatalf("claim: got job %d, want nothing", l.Job.ID)
	}
}

func TestLaneRunsOneAtATimeInOrder(t *testing.T) {
	q, _, _ := newQueue(t)
	a := enqueue(t, q, EnqueueRequest{Type: "post", Lane: "photos"})
	b := enqueue(t, q, EnqueueRequest{Type: "post", Lane: "photos"})
	other := enqueue(t, q, EnqueueRequest{Type: "post", Lane: "minju"})

	la := mustClaimID(t, q, a.ID)
	lo := mustClaimID(t, q, other.ID) // other lanes aren't blocked
	mustClaimNone(t, q)               // b waits for a

	if _, err := q.Complete(la.Job.ID, la.Token, nil); err != nil {
		t.Fatal(err)
	}
	mustClaimID(t, q, b.ID)
	if _, err := q.Complete(lo.Job.ID, lo.Token, nil); err != nil {
		t.Fatal(err)
	}
}

func TestUnlanedJobsRunInParallel(t *testing.T) {
	q, _, _ := newQueue(t)
	a := enqueue(t, q, EnqueueRequest{Type: "x"})
	b := enqueue(t, q, EnqueueRequest{Type: "x"})
	mustClaimID(t, q, a.ID)
	mustClaimID(t, q, b.ID)
}

func TestClaimFiltersByType(t *testing.T) {
	q, _, _ := newQueue(t)
	enqueue(t, q, EnqueueRequest{Type: "convert"})
	p := enqueue(t, q, EnqueueRequest{Type: "publish"})
	l := claim(t, q, "publish")
	if l == nil || l.Job.ID != p.ID {
		t.Fatalf("got %+v, want publish job", l)
	}
}

func TestRunAtDelaysJob(t *testing.T) {
	q, c, _ := newQueue(t)
	j := enqueue(t, q, EnqueueRequest{Type: "x", RunAt: c.Now().Add(time.Hour)})
	mustClaimNone(t, q)
	c.Add(time.Hour)
	mustClaimID(t, q, j.ID)
}

func TestFailRetriesWithBackoffThenDies(t *testing.T) {
	q, c, _ := newQueue(t)
	j := enqueue(t, q, EnqueueRequest{Type: "x", MaxAttempts: 3})

	for attempt, wait := range []time.Duration{10 * time.Second, 20 * time.Second} {
		l := mustClaimID(t, q, j.ID)
		got, err := q.Fail(l.Job.ID, l.Token, FailRequest{Error: "boom"})
		if err != nil {
			t.Fatal(err)
		}
		if got.State != Queued || !got.RunAt.Equal(c.Now().Add(wait)) {
			t.Fatalf("attempt %d: state %s run_at %v, want queued after %v", attempt+1, got.State, got.RunAt, wait)
		}
		mustClaimNone(t, q)
		c.Add(wait)
	}
	l := mustClaimID(t, q, j.ID)
	got, err := q.Fail(l.Job.ID, l.Token, FailRequest{Error: "boom"})
	if err != nil {
		t.Fatal(err)
	}
	if got.State != Dead || got.LastError != "boom" || got.Attempts != 3 {
		t.Fatalf("got %s after %d attempts, want dead after 3", got.State, got.Attempts)
	}
}

func TestPermanentFailureAndRetryAfter(t *testing.T) {
	q, c, _ := newQueue(t)
	a := enqueue(t, q, EnqueueRequest{Type: "x"})
	l := mustClaimID(t, q, a.ID)
	got, _ := q.Fail(l.Job.ID, l.Token, FailRequest{Error: "bad input", Permanent: true})
	if got.State != Dead {
		t.Fatalf("permanent failure: got %s, want dead", got.State)
	}

	b := enqueue(t, q, EnqueueRequest{Type: "x"})
	l = mustClaimID(t, q, b.ID)
	got, _ = q.Fail(l.Job.ID, l.Token, FailRequest{Error: "rate limited", RetryAfter: 5 * time.Minute})
	if !got.RunAt.Equal(c.Now().Add(5 * time.Minute)) {
		t.Fatalf("retry_after: run_at %v, want +5m", got.RunAt)
	}
}

func TestDeadJobBlocksLaneUntilRetriedOrCanceled(t *testing.T) {
	q, _, _ := newQueue(t)
	a := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l", MaxAttempts: 1})
	b := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l"})

	l := mustClaimID(t, q, a.ID)
	q.Fail(l.Job.ID, l.Token, FailRequest{Error: "boom"})
	mustClaimNone(t, q) // b must not jump ahead of the dead job

	if _, err := q.Retry(a.ID); err != nil {
		t.Fatal(err)
	}
	l = mustClaimID(t, q, a.ID)
	if l.Job.Attempts != 1 {
		t.Fatalf("retry should reset attempts; got %d", l.Job.Attempts)
	}
	q.Fail(l.Job.ID, l.Token, FailRequest{Error: "boom"})

	if _, err := q.Cancel(a.ID); err != nil {
		t.Fatal(err)
	}
	mustClaimID(t, q, b.ID)
}

func TestExpiredLeaseIsRetriedAndOldTokenRejected(t *testing.T) {
	q, c, _ := newQueue(t)
	j := enqueue(t, q, EnqueueRequest{Type: "x"})
	stale := mustClaimID(t, q, j.ID)

	c.Add(2 * time.Minute) // lease was 1 minute; the worker "died"
	if err := q.ExpireLeases(); err != nil {
		t.Fatal(err)
	}
	got, _ := q.Get(j.ID)
	if got.State != Queued || got.LastError == "" {
		t.Fatalf("after expiry: %s %q, want queued with an error", got.State, got.LastError)
	}

	c.Add(time.Minute) // past the backoff
	fresh := mustClaimID(t, q, j.ID)
	if _, err := q.Complete(j.ID, stale.Token, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale worker completing: got %v, want ErrLeaseLost", err)
	}
	if _, err := q.Complete(j.ID, fresh.Token, nil); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatExtendsLeaseAndSavesCheckpoint(t *testing.T) {
	q, c, _ := newQueue(t)
	j := enqueue(t, q, EnqueueRequest{Type: "x"})
	l := mustClaimID(t, q, j.ID)

	c.Add(50 * time.Second)
	if _, err := q.Heartbeat(j.ID, l.Token, json.RawMessage(`{"step":2}`), time.Minute); err != nil {
		t.Fatal(err)
	}
	c.Add(50 * time.Second) // past the original lease, within the extended one
	q.ExpireLeases()
	if got, _ := q.Get(j.ID); got.State != Running {
		t.Fatalf("heartbeat didn't extend the lease: %s", got.State)
	}

	q.Fail(j.ID, l.Token, FailRequest{Error: "crash", RetryAfter: time.Second})
	c.Add(time.Second)
	l = mustClaimID(t, q, j.ID)
	if string(l.Job.Checkpoint) != `{"step":2}` {
		t.Fatalf("checkpoint not handed to the next attempt: %s", l.Job.Checkpoint)
	}
}

func TestIdempotencyKey(t *testing.T) {
	q, _, _ := newQueue(t)
	a, created, _ := q.Enqueue(EnqueueRequest{Type: "x", Key: "post-1"})
	b, createdAgain, _ := q.Enqueue(EnqueueRequest{Type: "x", Key: "post-1"})
	if !created || createdAgain || a.ID != b.ID {
		t.Fatalf("got %d (created %v) and %d (created %v), want one job", a.ID, created, b.ID, createdAgain)
	}
}

func TestHeldJobsReleaseInLaneOrder(t *testing.T) {
	q, c, _ := newQueue(t)
	var ids []int64
	for range 5 {
		ids = append(ids, enqueue(t, q, EnqueueRequest{Type: "post", Lane: "l", Hold: true}).ID)
	}
	mustClaimNone(t, q)

	midnight := c.Now().Add(time.Hour)
	got, err := q.Release("l", 3, midnight)
	if err != nil || len(got) != 3 {
		t.Fatalf("release: %v, %d jobs", err, len(got))
	}
	for i, j := range got {
		if j.ID != ids[i] || j.State != Queued || !j.RunAt.Equal(midnight) {
			t.Fatalf("released %+v, want job %d queued at midnight", j, ids[i])
		}
	}
	mustClaimNone(t, q)
	c.Add(time.Hour)
	l := mustClaimID(t, q, ids[0])
	q.Complete(l.Job.ID, l.Token, nil)
	l = mustClaimID(t, q, ids[1])
	q.Complete(l.Job.ID, l.Token, nil)
	l = mustClaimID(t, q, ids[2])
	q.Complete(l.Job.ID, l.Token, nil)
	mustClaimNone(t, q) // 4 and 5 are still held
}

func TestPausedLane(t *testing.T) {
	q, _, _ := newQueue(t)
	j := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l"})
	q.SetPaused("l", true)
	mustClaimNone(t, q)
	q.SetPaused("l", false)
	mustClaimID(t, q, j.ID)
}

func TestClaimWaitWakesOnEnqueue(t *testing.T) {
	q, _, _ := newQueue(t)
	got := make(chan *Lease)
	go func() {
		l, _ := q.ClaimWait(context.Background(), "w", nil, time.Minute, 5*time.Second)
		got <- l
	}()
	time.Sleep(50 * time.Millisecond)
	j := enqueue(t, q, EnqueueRequest{Type: "x"})
	select {
	case l := <-got:
		if l == nil || l.Job.ID != j.ID {
			t.Fatalf("got %+v", l)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ClaimWait didn't wake up for the new job")
	}
}

func TestRecoversStateAfterRestart(t *testing.T) {
	dir, c := t.TempDir(), newClock()
	q := open(t, dir, c)
	a := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l", Key: "a"})
	b := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l", Key: "b"})
	l := mustClaimID(t, q, a.ID)
	q.Heartbeat(a.ID, l.Token, json.RawMessage(`{"container":"123"}`), time.Minute)
	q.SetPaused("other", true)
	q.wal.close() // simulate a crash: no snapshot, no clean close

	q = open(t, dir, c)
	defer q.Close()
	got, err := q.Get(a.ID)
	if err != nil || got.State != Running || string(got.Checkpoint) != `{"container":"123"}` {
		t.Fatalf("after restart: %+v, %v", got, err)
	}
	if _, err := q.Complete(a.ID, l.Token, nil); err != nil {
		t.Fatalf("lease should survive a restart: %v", err)
	}
	mustClaimID(t, q, b.ID)
	if j, created, _ := q.Enqueue(EnqueueRequest{Type: "x", Key: "a"}); created || j.ID != a.ID {
		t.Fatal("idempotency keys should survive a restart")
	}
	if c := enqueue(t, q, EnqueueRequest{Type: "x"}); c.ID <= b.ID {
		t.Fatalf("IDs reused after restart: %d", c.ID)
	}
	if lanes := q.Lanes(); !lanes[len(lanes)-1].Paused {
		t.Fatal("lane pause should survive a restart")
	}
}

func TestTornWriteIsDiscarded(t *testing.T) {
	dir, c := t.TempDir(), newClock()
	q := open(t, dir, c)
	a := enqueue(t, q, EnqueueRequest{Type: "x"})
	q.wal.close()

	// A crash mid-append leaves half a line at the end of the log.
	f, _ := os.OpenFile(filepath.Join(dir, walName), os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(`0badf00d {"job":{"id":99,"ty`)
	f.Close()

	q = open(t, dir, c)
	defer q.Close()
	if _, err := q.Get(a.ID); err != nil {
		t.Fatalf("intact record lost: %v", err)
	}
	if _, err := q.Get(99); !errors.Is(err, ErrNotFound) {
		t.Fatal("torn record should be dropped")
	}
	b := enqueue(t, q, EnqueueRequest{Type: "x"}) // appends must start on a clean line
	q.wal.close()
	q = open(t, dir, c)
	defer q.Close()
	if _, err := q.Get(b.ID); err != nil {
		t.Fatalf("record written after a torn tail was lost: %v", err)
	}
}

func TestCorruptRecordStopsReplay(t *testing.T) {
	dir, c := t.TempDir(), newClock()
	q := open(t, dir, c)
	enqueue(t, q, EnqueueRequest{Type: "x"})
	q.wal.close()

	data, _ := os.ReadFile(filepath.Join(dir, walName))
	data[len(data)-5] ^= 0xff // flip bits inside the JSON; the checksum no longer matches
	os.WriteFile(filepath.Join(dir, walName), data, 0o644)

	q = open(t, dir, c)
	defer q.Close()
	if jobs, _ := q.List(Filter{}); len(jobs) != 0 {
		t.Fatalf("corrupt record was applied: %+v", jobs)
	}
}

func TestSnapshotCompactsLog(t *testing.T) {
	dir, c := t.TempDir(), newClock()
	q, err := Open(Options{Dir: dir, Now: c.Now, SnapshotEvery: 10})
	if err != nil {
		t.Fatal(err)
	}
	for range 25 {
		enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l"})
	}
	if q.walRecords >= 10 {
		t.Fatalf("log not compacted: %d records", q.walRecords)
	}
	q.wal.close()

	q = open(t, dir, c)
	defer q.Close()
	if _, n := q.List(Filter{}); n != 25 {
		t.Fatalf("after snapshot + replay: %d jobs, want 25", n)
	}
	mustClaimID(t, q, 1) // lane order rebuilt from the snapshot
}

func TestConcurrentWorkersNeverShareAJob(t *testing.T) {
	q, _, _ := newQueue(t)
	const n = 200
	for range n {
		enqueue(t, q, EnqueueRequest{Type: "x"})
	}
	var mu sync.Mutex
	seen := map[int64]bool{}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				l, err := q.Claim(string(rune('a'+w)), nil, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if l == nil {
					return
				}
				mu.Lock()
				if seen[l.Job.ID] {
					t.Errorf("job %d claimed twice", l.Job.ID)
				}
				seen[l.Job.ID] = true
				mu.Unlock()
				q.Complete(l.Job.ID, l.Token, nil)
			}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("ran %d jobs, want %d", len(seen), n)
	}
}
