package queue

import (
	"os"
	"path/filepath"
	"time"
)

// Stats summarizes everything the queue holds: finished jobs are kept, so these
// are totals since the queue was created.
type Stats struct {
	Jobs      int           `json:"jobs"`
	ByState   map[State]int `json:"by_state"`
	Attempts  int           `json:"attempts"` // every time a worker took a job, retries included
	Lanes     int           `json:"lanes"`
	Schedules int           `json:"schedules"`
	DataBytes int64         `json:"data_bytes"` // the write-ahead log and snapshot on disk
	Since     time.Time     `json:"since,omitzero"`
}

// Stats counts jobs by state, lanes, schedules and the size of the data on disk.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	s := Stats{Jobs: len(q.jobs), ByState: map[State]int{}, Schedules: len(q.schedules)}
	lanes := map[string]bool{}
	for _, j := range q.jobs {
		s.ByState[j.State]++
		s.Attempts += j.Attempts
		if j.Lane != "" {
			lanes[j.Lane] = true
		}
		if s.Since.IsZero() || j.CreatedAt.Before(s.Since) {
			s.Since = j.CreatedAt
		}
	}
	s.Lanes = len(lanes)
	q.mu.Unlock()
	for _, name := range []string{walName, snapshotName} {
		if fi, err := os.Stat(filepath.Join(q.opts.Dir, name)); err == nil {
			s.DataBytes += fi.Size()
		}
	}
	return s
}
