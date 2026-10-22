package queue

import (
	"fmt"
	"maps"
	"slices"
	"time"
)

// Schedule releases a lane's held jobs in a batch at the same local time every day.
//
// A run that was missed because the machine was asleep or off happens as soon as
// the queue is running again, and makes up for up to MaxCatchUp missed days.
// The released jobs and the record that the day has run are written in a single
// log append, so a crash can never release the same day twice.
type Schedule struct {
	Lane       string `json:"lane"`
	Enabled    bool   `json:"enabled"`
	At         string `json:"at"`           // "HH:MM", local time
	Count      int    `json:"count"`        // jobs released per run
	GapSeconds int    `json:"gap_seconds"`  // spacing between the released jobs' run_at
	MaxCatchUp int    `json:"max_catch_up"` // most missed days made up at once

	// LastRun is the most recent slot (not wall-clock time) that has been released.
	LastRun time.Time `json:"last_run"`
	// Overrides sets a different count for one day's run, keyed by the slot's date ("2026-10-23").
	Overrides map[string]int `json:"overrides,omitempty"`
}

// ScheduleStatus is a schedule plus when it will next run, for display.
type ScheduleStatus struct {
	Schedule
	NextRun   time.Time `json:"next_run"`
	NextCount int       `json:"next_count"`
	Held      int       `json:"held"`
}

const slotDate = "2006-01-02"

func (s *Schedule) validate() error {
	if s.Lane == "" {
		return fmt.Errorf("%w: lane is required", ErrInvalid)
	}
	if _, err := time.Parse("15:04", s.At); err != nil {
		return fmt.Errorf("%w: at must be HH:MM", ErrInvalid)
	}
	if s.Count < 0 || s.GapSeconds < 0 || s.MaxCatchUp < 0 {
		return fmt.Errorf("%w: count, gap_seconds and max_catch_up can't be negative", ErrInvalid)
	}
	for day, n := range s.Overrides {
		if _, err := time.Parse(slotDate, day); err != nil || n < 0 {
			return fmt.Errorf("%w: bad override %q: %d", ErrInvalid, day, n)
		}
	}
	return nil
}

// slotOn is the run time on the calendar day of t, in t's location.
func (s *Schedule) slotOn(t time.Time) time.Time {
	hm, _ := time.Parse("15:04", s.At)
	return time.Date(t.Year(), t.Month(), t.Day(), hm.Hour(), hm.Minute(), 0, 0, t.Location())
}

// nextSlot is the first run time strictly after t.
func (s *Schedule) nextSlot(t time.Time) time.Time {
	slot := s.slotOn(t)
	if !slot.After(t) {
		slot = s.slotOn(time.Date(t.Year(), t.Month(), t.Day()+1, 12, 0, 0, 0, t.Location()))
	}
	return slot
}

// prevSlot is the last run time at or before t.
func (s *Schedule) prevSlot(t time.Time) time.Time {
	slot := s.slotOn(t)
	if slot.After(t) {
		slot = s.slotOn(time.Date(t.Year(), t.Month(), t.Day()-1, 12, 0, 0, 0, t.Location()))
	}
	return slot
}

func (s *Schedule) countFor(slot time.Time) int {
	if n, ok := s.Overrides[slot.Format(slotDate)]; ok {
		return n
	}
	return s.Count
}

func cloneSchedule(s Schedule) Schedule {
	s.Overrides = maps.Clone(s.Overrides)
	return s
}

// SetSchedule creates or replaces a lane's schedule. A new schedule's first run
// is the next slot after now — creating one never releases anything immediately.
func (q *Queue) SetSchedule(s Schedule) (_ ScheduleStatus, err error) {
	if s.GapSeconds == 0 {
		s.GapSeconds = 120
	}
	if s.MaxCatchUp == 0 {
		s.MaxCatchUp = 7
	}
	if err := s.validate(); err != nil {
		return ScheduleStatus{}, err
	}
	q.mu.Lock()
	defer q.done(&err)
	now := q.opts.Now()
	if old, ok := q.schedules[s.Lane]; ok && s.At == old.At && old.Enabled {
		s.LastRun = old.LastRun // keep progress
	} else {
		// New, re-timed, or switched back on: start from now. Days a schedule
		// was off aren't missed runs, so they're never caught up.
		s.LastRun = s.prevSlot(now)
	}
	s = cloneSchedule(s)
	if err := q.commit(record{Schedule: &s}); err != nil {
		return ScheduleStatus{}, err
	}
	return q.statusLocked(s, now), nil
}

// SetOverride sets how many jobs the run on a given day releases. A negative
// count removes the override.
func (q *Queue) SetOverride(lane string, day string, count int) (_ ScheduleStatus, err error) {
	if _, err := time.Parse(slotDate, day); err != nil {
		return ScheduleStatus{}, fmt.Errorf("%w: day must be YYYY-MM-DD", ErrInvalid)
	}
	q.mu.Lock()
	defer q.done(&err)
	cur, ok := q.schedules[lane]
	if !ok {
		return ScheduleStatus{}, fmt.Errorf("%w: lane %q has no schedule", ErrNotFound, lane)
	}
	s := cloneSchedule(cur)
	if s.Overrides == nil {
		s.Overrides = map[string]int{}
	}
	if count < 0 {
		delete(s.Overrides, day)
	} else {
		s.Overrides[day] = count
	}
	if err := q.commit(record{Schedule: &s}); err != nil {
		return ScheduleStatus{}, err
	}
	return q.statusLocked(s, q.opts.Now()), nil
}

// Schedules lists every schedule with its next run.
func (q *Queue) Schedules() []ScheduleStatus {
	q.mu.Lock()
	defer q.done(nil)
	now := q.opts.Now()
	var out []ScheduleStatus
	for _, s := range q.schedules {
		out = append(out, q.statusLocked(s, now))
	}
	slices.SortFunc(out, func(a, b ScheduleStatus) int {
		if a.Lane < b.Lane {
			return -1
		}
		if a.Lane > b.Lane {
			return 1
		}
		return 0
	})
	return out
}

func (q *Queue) statusLocked(s Schedule, now time.Time) ScheduleStatus {
	st := ScheduleStatus{Schedule: s}
	st.NextRun = s.nextSlot(s.LastRun.In(now.Location()))
	if st.NextRun.Before(now) { // a missed run; it happens on the next tick
		st.NextRun = now
	}
	st.NextCount = s.countFor(s.nextSlot(s.LastRun.In(now.Location())))
	if l := q.lanes[s.Lane]; l != nil {
		for _, id := range l.active {
			if q.jobs[id].State == Held {
				st.Held++
			}
		}
	}
	return st
}

// RunSchedules releases every batch that is due. Call it periodically; it is
// cheap when nothing is due. It returns the jobs released.
func (q *Queue) RunSchedules() (released []Job, err error) {
	q.mu.Lock()
	defer q.done(&err)
	now := q.opts.Now()
	for _, cur := range q.schedules {
		if !cur.Enabled {
			continue
		}
		if l := q.lanes[cur.Lane]; l != nil && l.state.Paused {
			continue // stopped: release nothing; resuming skips these days (SetPaused)
		}
		var due []time.Time
		for slot := cur.nextSlot(cur.LastRun.In(now.Location())); !slot.After(now); slot = cur.nextSlot(slot) {
			due = append(due, slot)
		}
		if len(due) == 0 {
			continue
		}
		s := cloneSchedule(cur)
		s.LastRun = due[len(due)-1]
		if len(due) > s.MaxCatchUp {
			due = due[len(due)-s.MaxCatchUp:] // the oldest missed days are dropped, not made up
		}
		n := 0
		for _, slot := range due {
			n += s.countFor(slot)
		}
		for day := range s.Overrides { // overrides for days that have run are spent
			if t, _ := time.ParseInLocation(slotDate, day, now.Location()); !s.slotOn(t).After(s.LastRun) {
				delete(s.Overrides, day)
			}
		}

		recs := []record{{Schedule: &s}}
		gap := time.Duration(s.GapSeconds) * time.Second
		if l := q.lanes[s.Lane]; l != nil {
			for _, id := range l.active {
				if len(recs)-1 == n {
					break
				}
				j := *q.jobs[id]
				if j.State != Held {
					continue
				}
				j.State = Queued
				j.RunAt = now.Add(time.Duration(len(recs)-1) * gap)
				j.UpdatedAt = now
				recs = append(recs, record{Job: &j})
			}
		}
		if err := q.commit(recs...); err != nil {
			return released, err
		}
		for _, r := range recs[1:] {
			released = append(released, r.Job.Public())
		}
	}
	return released, nil
}
