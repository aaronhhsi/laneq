package queue

import (
	"encoding/json"
	"time"
)

// State is where a job is in its lifecycle.
//
//	held ──release──▶ queued ──claim──▶ running ──complete──▶ succeeded
//	                    ▲                  │
//	                    └──retry/backoff───┤ fail or lease expiry
//	                                       └──out of attempts──▶ dead
//
// Any job that is not running or finished can be canceled.
type State string

const (
	// Held jobs wait for an explicit release (e.g. a nightly scheduler).
	Held State = "held"
	// Queued jobs run once RunAt has passed and they are at the head of their lane.
	Queued State = "queued"
	// Running jobs are leased to a worker until LeaseExpires.
	Running State = "running"
	// Succeeded jobs finished; Result holds what the worker returned.
	Succeeded State = "succeeded"
	// Dead jobs ran out of attempts. They block their lane until retried or canceled,
	// so later jobs in the lane never run out of order.
	Dead State = "dead"
	// Canceled jobs were removed by an operator.
	Canceled State = "canceled"
)

// Finished reports whether the job no longer occupies its lane.
func (s State) Finished() bool { return s == Succeeded || s == Canceled }

// Job is one unit of work. Jobs that share a Lane run one at a time in Seq order.
// Jobs with no lane are independent and run in RunAt order.
type Job struct {
	ID          int64           `json:"id"`
	Seq         int64           `json:"seq"`
	Type        string          `json:"type"`
	Lane        string          `json:"lane,omitempty"`
	Key         string          `json:"key,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	State       State           `json:"state"`
	RunAt       time.Time       `json:"run_at"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`

	Worker       string    `json:"worker,omitempty"`
	LeaseToken   string    `json:"lease_token,omitempty"`
	LeaseExpires time.Time `json:"lease_expires,omitzero"`

	// Checkpoint is progress a worker saved mid-job, handed back on the next attempt
	// so a retry resumes instead of starting over.
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	LastError  string          `json:"last_error,omitempty"`

	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	FinishedAt time.Time `json:"finished_at,omitzero"`
}

// Public returns a copy without the lease token, for showing to anyone but the lease holder.
func (j Job) Public() Job {
	j.LeaseToken = ""
	return j
}

// LaneState is the persisted per-lane setting.
type LaneState struct {
	Name   string `json:"name"`
	Paused bool   `json:"paused"`
}

// LaneSummary describes a lane for listing.
type LaneSummary struct {
	Name   string        `json:"name"`
	Paused bool          `json:"paused"`
	Head   *Job          `json:"head,omitempty"`
	Counts map[State]int `json:"counts"`
}
