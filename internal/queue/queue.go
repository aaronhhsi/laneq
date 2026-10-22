// Package queue is a durable job queue with ordered lanes, leases and retries.
//
// Every change is written to a write-ahead log and fsynced before it is applied
// in memory, so a crash at any point loses nothing that was acknowledged.
// Delivery is at-least-once: a worker that dies mid-job loses its lease and the
// job runs again, so handlers must be safe to repeat (see Job.Checkpoint).
package queue

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"slices"
	"sort"
	"sync"
	"time"
)

var (
	ErrNotFound  = errors.New("job not found")
	ErrLeaseLost = errors.New("lease lost: job is no longer leased with this token")
	ErrBadState  = errors.New("job is not in a state that allows this")
	ErrInvalid   = errors.New("invalid request")
)

// Options configures a Queue. Zero values get the defaults noted on each field.
type Options struct {
	Dir string // where the WAL and snapshot live (required)

	Now func() time.Time // clock; time.Now

	DefaultMaxAttempts int            // 5
	DefaultLease       time.Duration  // 60s
	BackoffBase        time.Duration  // 10s; doubled per attempt
	BackoffMax         time.Duration  // 1h
	Jitter             func() float64 // returns [0,1); spreads retries ±20%

	SnapshotEvery int // compact the WAL after this many records; 5000
}

func (o *Options) defaults() {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.DefaultMaxAttempts == 0 {
		o.DefaultMaxAttempts = 5
	}
	if o.DefaultLease == 0 {
		o.DefaultLease = 60 * time.Second
	}
	if o.BackoffBase == 0 {
		o.BackoffBase = 10 * time.Second
	}
	if o.BackoffMax == 0 {
		o.BackoffMax = time.Hour
	}
	if o.Jitter == nil {
		o.Jitter = mrand.Float64
	}
	if o.SnapshotEvery == 0 {
		o.SnapshotEvery = 5000
	}
}

type lane struct {
	state  LaneState
	active []int64 // unfinished job IDs, sorted by Seq
}

// Queue is safe for concurrent use.
type Queue struct {
	opts Options

	mu         sync.Mutex
	wal        *wal
	walRecords int
	jobs       map[int64]*Job
	byKey      map[string]int64
	lanes      map[string]*lane
	schedules  map[string]Schedule
	running    map[int64]struct{} // leased jobs, so lease expiry doesn't scan every job
	ready      readyHeap          // unordered queued jobs by (RunAt, Seq), so a claim doesn't either
	nextID     int64
	changed    chan struct{} // closed and replaced on every change, waking waiters
}

// Open loads the queue from dir, creating it if needed.
func Open(opts Options) (*Queue, error) {
	opts.defaults()
	if opts.Dir == "" {
		return nil, fmt.Errorf("%w: Dir is required", ErrInvalid)
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	q := &Queue{
		opts:      opts,
		jobs:      map[int64]*Job{},
		byKey:     map[string]int64{},
		lanes:     map[string]*lane{},
		schedules: map[string]Schedule{},
		running:   map[int64]struct{}{},
		nextID:    1,
		changed:   make(chan struct{}),
	}
	snap, err := readSnapshot(opts.Dir)
	if err != nil {
		return nil, err
	}
	if snap != nil {
		for _, ls := range snap.Lanes {
			q.applyLane(ls)
		}
		for _, j := range snap.Jobs {
			q.applyJob(j)
		}
		for _, s := range snap.Schedules {
			q.schedules[s.Lane] = s
		}
		q.nextID = max(q.nextID, snap.NextID)
	}
	w, n, err := openWAL(opts.Dir, q.apply)
	if err != nil {
		return nil, err
	}
	q.wal, q.walRecords = w, n
	return q, nil
}

// Close flushes a snapshot and closes the log.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	err := q.snapshotLocked()
	return errors.Join(err, q.wal.close())
}

// --- applying changes -------------------------------------------------------

func (q *Queue) apply(r record) {
	if r.Lane != nil {
		q.applyLane(*r.Lane)
	}
	if r.Job != nil {
		q.applyJob(r.Job)
	}
	if r.Schedule != nil {
		q.schedules[r.Schedule.Lane] = *r.Schedule
	}
}

func (q *Queue) lane(name string) *lane {
	l := q.lanes[name]
	if l == nil {
		l = &lane{state: LaneState{Name: name}}
		q.lanes[name] = l
	}
	return l
}

func (q *Queue) applyLane(ls LaneState) { q.lane(ls.Name).state = ls }

func (q *Queue) applyJob(j *Job) {
	old := q.jobs[j.ID]
	q.jobs[j.ID] = j
	if j.Key != "" {
		q.byKey[j.Key] = j.ID
	}
	q.nextID = max(q.nextID, j.ID+1)

	l := q.lane(j.Lane)
	wasActive := old != nil && !old.State.Finished()
	isActive := !j.State.Finished()
	moved := old != nil && old.Seq != j.Seq
	if wasActive && (!isActive || moved) {
		l.active = slices.DeleteFunc(l.active, func(id int64) bool { return id == j.ID })
	}
	if isActive && (!wasActive || moved) {
		i := sort.Search(len(l.active), func(i int) bool { return q.jobs[l.active[i]].Seq > j.Seq })
		l.active = slices.Insert(l.active, i, j.ID)
	}

	if j.State == Running {
		q.running[j.ID] = struct{}{}
	} else {
		delete(q.running, j.ID)
	}
	if j.Lane == "" && j.State == Queued &&
		(old == nil || old.State != Queued || !old.RunAt.Equal(j.RunAt) || old.Seq != j.Seq) {
		heap.Push(&q.ready, readyEntry{id: j.ID, runAt: j.RunAt, seq: j.Seq})
	}
}

// readyHeap orders unordered queued jobs by (RunAt, Seq). Entries go stale when
// a job is claimed, canceled or rescheduled; they're dropped when they surface.
type readyEntry struct {
	id    int64
	runAt time.Time
	seq   int64
}

type readyHeap []readyEntry

func (h readyHeap) Len() int { return len(h) }
func (h readyHeap) Less(i, k int) bool {
	if !h[i].runAt.Equal(h[k].runAt) {
		return h[i].runAt.Before(h[k].runAt)
	}
	return h[i].seq < h[k].seq
}
func (h readyHeap) Swap(i, k int) { h[i], h[k] = h[k], h[i] }
func (h *readyHeap) Push(x any)   { *h = append(*h, x.(readyEntry)) }
func (h *readyHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

func (q *Queue) fresh(e readyEntry) bool {
	j := q.jobs[e.id]
	return j != nil && j.Lane == "" && j.State == Queued && j.RunAt.Equal(e.runAt) && j.Seq == e.seq
}

// nextUnordered is the earliest runnable unordered job of the given types, or
// nil, plus when the earliest one not yet due becomes due.
func (q *Queue) nextUnordered(types []string, now time.Time) (*Job, time.Time) {
	var skipped []readyEntry
	defer func() {
		for _, e := range skipped {
			heap.Push(&q.ready, e)
		}
	}()
	for q.ready.Len() > 0 {
		e := q.ready[0]
		if !q.fresh(e) {
			heap.Pop(&q.ready)
			continue
		}
		if e.runAt.After(now) {
			return nil, e.runAt
		}
		j := q.jobs[e.id]
		if len(types) == 0 || slices.Contains(types, j.Type) {
			return j, time.Time{}
		}
		skipped = append(skipped, heap.Pop(&q.ready).(readyEntry)) // another worker's type
	}
	return nil, time.Time{}
}

// done ends an operation: it releases the lock, then waits until everything
// written so far — this operation's changes and anything it read — is durable.
// Operations running at the same time share one fsync (group commit), and a
// result is never returned before the changes behind it are on disk.
//
// Use it as `q.mu.Lock(); defer q.done(&err)`.
func (q *Queue) done(errp *error) {
	off := q.wal.written.Load()
	q.mu.Unlock()
	if err := q.wal.syncTo(off); err != nil && errp != nil && *errp == nil {
		*errp = fmt.Errorf("write-ahead log: %w", err)
	}
}

// commit writes changes to the log, then applies them and wakes waiters. The
// caller's done() makes them durable before anyone is told they happened.
func (q *Queue) commit(recs ...record) error {
	if err := q.wal.append(recs...); err != nil {
		return fmt.Errorf("write-ahead log: %w", err)
	}
	for _, r := range recs {
		q.apply(r)
	}
	q.walRecords += len(recs)
	close(q.changed)
	q.changed = make(chan struct{})
	if q.walRecords >= q.opts.SnapshotEvery {
		// The change is already in the log; a failed compaction just means a
		// longer replay next time, so it isn't reported to the caller.
		_ = q.snapshotLocked()
	}
	return nil
}

func (q *Queue) snapshotLocked() error {
	s := &snapshot{NextID: q.nextID}
	for _, j := range q.jobs {
		s.Jobs = append(s.Jobs, j)
	}
	slices.SortFunc(s.Jobs, func(a, b *Job) int { return int(a.ID - b.ID) })
	for _, l := range q.lanes {
		s.Lanes = append(s.Lanes, l.state)
	}
	for _, sc := range q.schedules {
		s.Schedules = append(s.Schedules, sc)
	}
	if err := writeSnapshot(q.opts.Dir, s); err != nil {
		return err
	}
	if err := q.wal.reset(); err != nil {
		return err
	}
	q.walRecords = 0
	return nil
}

func (q *Queue) get(id int64) (Job, error) {
	j := q.jobs[id]
	if j == nil {
		return Job{}, ErrNotFound
	}
	return *j, nil
}

func (q *Queue) put(j Job) error {
	j.UpdatedAt = q.opts.Now()
	return q.commit(record{Job: &j})
}

// --- producers --------------------------------------------------------------

// EnqueueRequest describes a new job.
type EnqueueRequest struct {
	Type        string          `json:"type"`
	Lane        string          `json:"lane,omitempty"`
	Key         string          `json:"key,omitempty"` // idempotency key: enqueueing it again returns the existing job
	Payload     json.RawMessage `json:"payload,omitempty"`
	RunAt       time.Time       `json:"run_at,omitzero"` // default now
	Hold        bool            `json:"hold,omitempty"`  // start held, waiting for Release
	MaxAttempts int             `json:"max_attempts,omitempty"`
}

// Enqueue adds a job. If Key matches an existing job, that job is returned with created=false.
func (q *Queue) Enqueue(req EnqueueRequest) (job Job, created bool, err error) {
	if req.Type == "" {
		return Job{}, false, fmt.Errorf("%w: type is required", ErrInvalid)
	}
	if req.MaxAttempts < 0 {
		return Job{}, false, fmt.Errorf("%w: max_attempts must be positive", ErrInvalid)
	}
	q.mu.Lock()
	defer q.done(&err)
	if req.Key != "" {
		if id, ok := q.byKey[req.Key]; ok {
			return *q.jobs[id], false, nil
		}
	}
	now := q.opts.Now()
	j := Job{
		ID:          q.nextID,
		Seq:         q.nextID,
		Type:        req.Type,
		Lane:        req.Lane,
		Key:         req.Key,
		Payload:     req.Payload,
		State:       Queued,
		RunAt:       req.RunAt,
		MaxAttempts: req.MaxAttempts,
		CreatedAt:   now,
	}
	if j.RunAt.IsZero() {
		j.RunAt = now
	}
	if req.Hold {
		j.State = Held
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = q.opts.DefaultMaxAttempts
	}
	if err := q.put(j); err != nil {
		return Job{}, false, err
	}
	return *q.jobs[j.ID], true, nil
}

// Release queues the first n held jobs of a lane (in lane order) to run at runAt.
func (q *Queue) Release(laneName string, n int, runAt time.Time) (out []Job, err error) {
	q.mu.Lock()
	defer q.done(&err)
	if runAt.IsZero() {
		runAt = q.opts.Now()
	}
	l := q.lanes[laneName]
	if l == nil {
		return nil, nil
	}
	for _, id := range slices.Clone(l.active) {
		if len(out) == n {
			break
		}
		j := *q.jobs[id]
		if j.State != Held {
			continue
		}
		j.State, j.RunAt = Queued, runAt
		if err := q.put(j); err != nil {
			return out, err
		}
		out = append(out, *q.jobs[id])
	}
	return out, nil
}

// --- workers ----------------------------------------------------------------

// Lease is a claimed job and the token that proves ownership of it.
type Lease struct {
	Job   Job    `json:"job"`
	Token string `json:"token"`
}

// Claim leases the next runnable job of one of the given types (any type if
// empty), or returns nil if none is runnable now.
func (q *Queue) Claim(worker string, types []string, lease time.Duration) (l *Lease, err error) {
	q.mu.Lock()
	defer q.done(&err)
	l, _, _, err = q.claimLocked(worker, types, lease)
	return l, err
}

// ClaimWait is Claim, but waits up to wait for a job to become runnable.
func (q *Queue) ClaimWait(ctx context.Context, worker string, types []string, lease, wait time.Duration) (*Lease, error) {
	deadline := q.opts.Now().Add(wait)
	for {
		l, changed, wake, err := q.claimOnce(worker, types, lease)
		if l != nil || err != nil {
			return l, err
		}
		now := q.opts.Now()
		if !now.Before(deadline) {
			return nil, nil
		}
		sleep := deadline.Sub(now)
		if !wake.IsZero() {
			sleep = min(sleep, max(wake.Sub(now), time.Millisecond))
		}
		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-changed:
		case <-t.C:
		}
		t.Stop()
	}
}

func (q *Queue) claimOnce(worker string, types []string, lease time.Duration) (l *Lease, changed chan struct{}, wake time.Time, err error) {
	q.mu.Lock()
	defer q.done(&err)
	return q.claimLocked(worker, types, lease)
}

// claimLocked also returns the change channel to wait on and the next time
// something could become runnable (a RunAt or a lease expiry), for ClaimWait.
func (q *Queue) claimLocked(worker string, types []string, lease time.Duration) (*Lease, chan struct{}, time.Time, error) {
	now := q.opts.Now()
	if err := q.expireLocked(now); err != nil {
		return nil, nil, time.Time{}, err
	}
	if lease <= 0 {
		lease = q.opts.DefaultLease
	}
	var best *Job
	var wake time.Time
	consider := func(j *Job) {
		switch j.State {
		case Queued:
			if len(types) > 0 && !slices.Contains(types, j.Type) {
				return
			}
			if j.RunAt.After(now) {
				wake = earliest(wake, j.RunAt)
				return
			}
			if best == nil || j.RunAt.Before(best.RunAt) || (j.RunAt.Equal(best.RunAt) && j.Seq < best.Seq) {
				best = j
			}
		}
	}
	for name, l := range q.lanes {
		if name == "" || len(l.active) == 0 || l.state.Paused {
			continue
		}
		consider(q.jobs[l.active[0]]) // ordered: only the head may run
	}
	if l := q.lanes[""]; l != nil && !l.state.Paused { // unordered: the earliest ready job
		j, due := q.nextUnordered(types, now)
		if j != nil {
			consider(j)
		}
		if !due.IsZero() {
			wake = earliest(wake, due)
		}
	}
	for id := range q.running { // a lease expiring can unblock a lane
		wake = earliest(wake, q.jobs[id].LeaseExpires)
	}
	if best == nil {
		return nil, q.changed, wake, nil
	}
	j := *best
	j.State = Running
	j.Attempts++
	j.Worker = worker
	j.LeaseToken = newToken()
	j.LeaseExpires = now.Add(lease)
	if err := q.put(j); err != nil {
		return nil, nil, time.Time{}, err
	}
	return &Lease{Job: j.Public(), Token: j.LeaseToken}, nil, time.Time{}, nil
}

func earliest(a, b time.Time) time.Time {
	if a.IsZero() || b.Before(a) {
		return b
	}
	return a
}

func newToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// leased returns the job if token still holds its lease.
func (q *Queue) leased(id int64, token string) (Job, error) {
	j, err := q.get(id)
	if err != nil {
		return j, err
	}
	if j.State != Running || token == "" || j.LeaseToken != token {
		return j, ErrLeaseLost
	}
	return j, nil
}

// Heartbeat extends a lease and optionally saves a checkpoint.
func (q *Queue) Heartbeat(id int64, token string, checkpoint json.RawMessage, lease time.Duration) (_ Job, err error) {
	q.mu.Lock()
	defer q.done(&err)
	now := q.opts.Now()
	if err := q.expireLocked(now); err != nil {
		return Job{}, err
	}
	j, err := q.leased(id, token)
	if err != nil {
		return Job{}, err
	}
	if lease <= 0 {
		lease = q.opts.DefaultLease
	}
	j.LeaseExpires = now.Add(lease)
	if checkpoint != nil {
		j.Checkpoint = checkpoint
	}
	if err := q.put(j); err != nil {
		return Job{}, err
	}
	return q.jobs[id].Public(), nil
}

// Complete marks a leased job succeeded.
func (q *Queue) Complete(id int64, token string, result json.RawMessage) (_ Job, err error) {
	q.mu.Lock()
	defer q.done(&err)
	if err := q.expireLocked(q.opts.Now()); err != nil {
		return Job{}, err
	}
	j, err := q.leased(id, token)
	if err != nil {
		return Job{}, err
	}
	j.State = Succeeded
	j.Result = result
	j.LastError = ""
	j.FinishedAt = q.opts.Now()
	clearLease(&j)
	if err := q.put(j); err != nil {
		return Job{}, err
	}
	return q.jobs[id].Public(), nil
}

// FailRequest is a worker reporting that an attempt failed.
type FailRequest struct {
	Error      string        `json:"error"`
	RetryAfter time.Duration `json:"-"`                   // overrides backoff, e.g. for a rate limit
	Permanent  bool          `json:"permanent,omitempty"` // don't retry; go straight to dead
}

// Fail records a failed attempt: the job is retried after backoff, or goes dead.
func (q *Queue) Fail(id int64, token string, req FailRequest) (_ Job, err error) {
	q.mu.Lock()
	defer q.done(&err)
	now := q.opts.Now()
	if err := q.expireLocked(now); err != nil {
		return Job{}, err
	}
	j, err := q.leased(id, token)
	if err != nil {
		return Job{}, err
	}
	if err := q.failLocked(j, req, now); err != nil {
		return Job{}, err
	}
	return q.jobs[id].Public(), nil
}

func (q *Queue) failLocked(j Job, req FailRequest, now time.Time) error {
	clearLease(&j)
	j.LastError = req.Error
	if req.Permanent || j.Attempts >= j.MaxAttempts {
		j.State = Dead
	} else {
		j.State = Queued
		delay := req.RetryAfter
		if delay <= 0 {
			delay = q.backoff(j.Attempts)
		}
		j.RunAt = now.Add(delay)
	}
	return q.put(j)
}

// backoff is BackoffBase·2^(attempt-1), capped at BackoffMax, ±20% jitter.
func (q *Queue) backoff(attempt int) time.Duration {
	d := q.opts.BackoffBase
	for i := 1; i < attempt && d < q.opts.BackoffMax; i++ {
		d *= 2
	}
	d = min(d, q.opts.BackoffMax)
	return time.Duration(float64(d) * (0.8 + 0.4*q.opts.Jitter()))
}

func clearLease(j *Job) {
	j.Worker, j.LeaseToken, j.LeaseExpires = "", "", time.Time{}
}

// ExpireLeases fails every running job whose lease has run out. It runs on
// every queue operation anyway; call it periodically so expiries are recorded
// even when nothing else is happening.
func (q *Queue) ExpireLeases() (err error) {
	q.mu.Lock()
	defer q.done(&err)
	return q.expireLocked(q.opts.Now())
}

func (q *Queue) expireLocked(now time.Time) error {
	var expired []int64
	for id := range q.running {
		if !q.jobs[id].LeaseExpires.After(now) {
			expired = append(expired, id)
		}
	}
	slices.Sort(expired) // deterministic log order
	for _, id := range expired {
		j := *q.jobs[id]
		msg := fmt.Sprintf("lease expired (worker %q stopped responding)", j.Worker)
		if err := q.failLocked(j, FailRequest{Error: msg}, now); err != nil {
			return err
		}
	}
	return nil
}

// --- operators --------------------------------------------------------------

// Retry runs a dead job again with fresh attempts, or runs a job waiting on
// backoff right away.
func (q *Queue) Retry(id int64) (_ Job, err error) {
	q.mu.Lock()
	defer q.done(&err)
	j, err := q.get(id)
	if err != nil {
		return Job{}, err
	}
	switch j.State {
	case Dead:
		j.Attempts = 0
	case Queued:
	default:
		return Job{}, fmt.Errorf("%w: can't retry a %s job", ErrBadState, j.State)
	}
	j.State, j.RunAt = Queued, q.opts.Now()
	if err := q.put(j); err != nil {
		return Job{}, err
	}
	return q.jobs[id].Public(), nil
}

// Cancel removes a job that isn't running or finished, unblocking its lane.
func (q *Queue) Cancel(id int64) (_ Job, err error) {
	q.mu.Lock()
	defer q.done(&err)
	j, err := q.get(id)
	if err != nil {
		return Job{}, err
	}
	if j.State == Running || j.State.Finished() {
		return Job{}, fmt.Errorf("%w: can't cancel a %s job", ErrBadState, j.State)
	}
	j.State = Canceled
	j.FinishedAt = q.opts.Now()
	if err := q.put(j); err != nil {
		return Job{}, err
	}
	return q.jobs[id].Public(), nil
}

// Hold returns a queued job to held, undoing a release: it waits for the next
// release instead of running.
func (q *Queue) Hold(id int64) (_ Job, err error) {
	q.mu.Lock()
	defer q.done(&err)
	j, err := q.get(id)
	if err != nil {
		return Job{}, err
	}
	if j.State != Queued {
		return Job{}, fmt.Errorf("%w: can't hold a %s job", ErrBadState, j.State)
	}
	j.State = Held
	if err := q.put(j); err != nil {
		return Job{}, err
	}
	return q.jobs[id].Public(), nil
}

// Move swaps a waiting job with its neighbor in the lane: delta -1 moves it one
// place earlier, +1 one place later. Running jobs stay put.
func (q *Queue) Move(id int64, delta int) (_ Job, err error) {
	if delta != -1 && delta != 1 {
		return Job{}, fmt.Errorf("%w: delta must be -1 or 1", ErrInvalid)
	}
	q.mu.Lock()
	defer q.done(&err)
	j, err := q.get(id)
	if err != nil {
		return Job{}, err
	}
	if j.Lane == "" || j.State == Running || j.State.Finished() {
		return Job{}, fmt.Errorf("%w: only a waiting job in a lane can move", ErrBadState)
	}
	l := q.lanes[j.Lane]
	i := slices.Index(l.active, id)
	k := i + delta
	if k < 0 || k >= len(l.active) {
		return Job{}, fmt.Errorf("%w: already at the %s", ErrBadState, map[int]string{-1: "front", 1: "back"}[delta])
	}
	other := *q.jobs[l.active[k]]
	if other.State == Running {
		return Job{}, fmt.Errorf("%w: can't move past a running job", ErrBadState)
	}
	now := q.opts.Now()
	j.Seq, other.Seq = other.Seq, j.Seq
	j.UpdatedAt, other.UpdatedAt = now, now
	if err := q.commit(record{Job: &j}, record{Job: &other}); err != nil {
		return Job{}, err
	}
	return q.jobs[id].Public(), nil
}

// SetPaused stops or resumes a lane. Pausing is a full stop: the running job
// finishes (stopping it mid-way could leave its side effect half done), nothing
// new starts, and if the lane has a schedule:
//   - jobs released but not started go back to held, at the front of the line,
//     so resuming doesn't immediately run them;
//   - the schedule releases nothing while paused, and on resume the paused days
//     are skipped rather than made up, so nothing builds up.
//
// Everything is one log append, so a crash can't leave a half-applied pause.
func (q *Queue) SetPaused(laneName string, paused bool) (_ LaneState, err error) {
	q.mu.Lock()
	defer q.done(&err)
	l := q.lane(laneName)
	ls := l.state
	ls.Paused = paused
	recs := []record{{Lane: &ls}}
	if sched, ok := q.schedules[laneName]; ok {
		now := q.opts.Now()
		if paused {
			for _, id := range l.active {
				if j := *q.jobs[id]; j.State == Queued {
					j.State, j.UpdatedAt = Held, now
					recs = append(recs, record{Job: &j})
				}
			}
		} else if last := sched.prevSlot(now); last.After(sched.LastRun) {
			s := cloneSchedule(sched)
			s.LastRun = last // the days it was paused count as run: skipped, not owed
			recs = append(recs, record{Schedule: &s})
		}
	}
	if err := q.commit(recs...); err != nil {
		return LaneState{}, err
	}
	return ls, nil
}

// --- reading ----------------------------------------------------------------

// Get returns one job.
func (q *Queue) Get(id int64) (Job, error) {
	q.mu.Lock()
	defer q.done(nil)
	j, err := q.get(id)
	return j.Public(), err
}

// Filter selects jobs for List. Empty fields match everything.
type Filter struct {
	Lane   *string
	States []State
	Type   string
	Newest bool // reverse order: latest first (e.g. for history)
	Offset int
	Limit  int // 0 = all
}

// List returns a page of matching jobs ordered by lane, then lane order, and
// how many match in total.
func (q *Queue) List(f Filter) ([]Job, int) {
	q.mu.Lock()
	defer q.done(nil)
	var out []Job
	for _, j := range q.jobs {
		if f.Lane != nil && j.Lane != *f.Lane {
			continue
		}
		if len(f.States) > 0 && !slices.Contains(f.States, j.State) {
			continue
		}
		if f.Type != "" && j.Type != f.Type {
			continue
		}
		out = append(out, j.Public())
	}
	slices.SortFunc(out, func(a, b Job) int {
		if a.Lane != b.Lane {
			if a.Lane < b.Lane {
				return -1
			}
			return 1
		}
		if f.Newest {
			if !a.FinishedAt.Equal(b.FinishedAt) {
				return b.FinishedAt.Compare(a.FinishedAt)
			}
			return int(b.Seq - a.Seq)
		}
		return int(a.Seq - b.Seq)
	})
	total := len(out)
	out = out[min(f.Offset, total):]
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, total
}

// Lanes summarizes every lane.
func (q *Queue) Lanes() []LaneSummary {
	q.mu.Lock()
	defer q.done(nil)
	byLane := map[string]map[State]int{}
	for _, j := range q.jobs {
		if byLane[j.Lane] == nil {
			byLane[j.Lane] = map[State]int{}
		}
		byLane[j.Lane][j.State]++
	}
	var out []LaneSummary
	for name, l := range q.lanes {
		s := LaneSummary{Name: name, Paused: l.state.Paused, Counts: byLane[name]}
		if s.Counts == nil {
			s.Counts = map[State]int{}
		}
		if name != "" && len(l.active) > 0 {
			h := q.jobs[l.active[0]].Public()
			s.Head = &h
		}
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b LaneSummary) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return out
}
