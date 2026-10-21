// Package queue is a durable job queue with ordered lanes, leases and retries.
//
// Every change is written to a write-ahead log and fsynced before it is applied
// in memory, so a crash at any point loses nothing that was acknowledged.
// Delivery is at-least-once: a worker that dies mid-job loses its lease and the
// job runs again, so handlers must be safe to repeat (see Job.Checkpoint).
package queue

import (
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
		opts:    opts,
		jobs:    map[int64]*Job{},
		byKey:   map[string]int64{},
		lanes:   map[string]*lane{},
		nextID:  1,
		changed: make(chan struct{}),
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
}

// commit makes changes durable, then applies them and wakes waiters.
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
		// The change is already durable in the log; a failed compaction just means
		// a longer replay next time, so it isn't reported to the caller.
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
	defer q.mu.Unlock()
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
func (q *Queue) Release(laneName string, n int, runAt time.Time) ([]Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if runAt.IsZero() {
		runAt = q.opts.Now()
	}
	var out []Job
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
func (q *Queue) Claim(worker string, types []string, lease time.Duration) (*Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	l, _, _, err := q.claimLocked(worker, types, lease)
	return l, err
}

// ClaimWait is Claim, but waits up to wait for a job to become runnable.
func (q *Queue) ClaimWait(ctx context.Context, worker string, types []string, lease, wait time.Duration) (*Lease, error) {
	deadline := q.opts.Now().Add(wait)
	for {
		q.mu.Lock()
		l, changed, wake, err := q.claimLocked(worker, types, lease)
		q.mu.Unlock()
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
		case Running:
			wake = earliest(wake, j.LeaseExpires)
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
		if len(l.active) == 0 || l.state.Paused {
			continue
		}
		if name == "" { // unordered: every job is eligible
			for _, id := range l.active {
				consider(q.jobs[id])
			}
		} else { // ordered: only the head may run
			consider(q.jobs[l.active[0]])
		}
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
func (q *Queue) Heartbeat(id int64, token string, checkpoint json.RawMessage, lease time.Duration) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
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
func (q *Queue) Complete(id int64, token string, result json.RawMessage) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
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
func (q *Queue) Fail(id int64, token string, req FailRequest) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
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
func (q *Queue) ExpireLeases() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.expireLocked(q.opts.Now())
}

func (q *Queue) expireLocked(now time.Time) error {
	for _, l := range q.lanes {
		for _, id := range slices.Clone(l.active) {
			j := *q.jobs[id]
			if j.State == Running && !j.LeaseExpires.After(now) {
				msg := fmt.Sprintf("lease expired (worker %q stopped responding)", j.Worker)
				if err := q.failLocked(j, FailRequest{Error: msg}, now); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// --- operators --------------------------------------------------------------

// Retry runs a dead job again with fresh attempts, or runs a job waiting on
// backoff right away.
func (q *Queue) Retry(id int64) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
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
func (q *Queue) Cancel(id int64) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
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

// SetPaused pauses or resumes a lane. A paused lane's running job finishes,
// but nothing new starts.
func (q *Queue) SetPaused(laneName string, paused bool) (LaneState, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	ls := q.lane(laneName).state
	ls.Paused = paused
	if err := q.commit(record{Lane: &ls}); err != nil {
		return LaneState{}, err
	}
	return ls, nil
}

// --- reading ----------------------------------------------------------------

// Get returns one job.
func (q *Queue) Get(id int64) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	j, err := q.get(id)
	return j.Public(), err
}

// Filter selects jobs for List. Empty fields match everything.
type Filter struct {
	Lane   *string
	States []State
	Type   string
}

// List returns matching jobs ordered by lane, then lane order.
func (q *Queue) List(f Filter) []Job {
	q.mu.Lock()
	defer q.mu.Unlock()
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
		return int(a.Seq - b.Seq)
	})
	return out
}

// Lanes summarizes every lane.
func (q *Queue) Lanes() []LaneSummary {
	q.mu.Lock()
	defer q.mu.Unlock()
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
