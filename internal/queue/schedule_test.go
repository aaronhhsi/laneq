package queue

import (
	"testing"
	"time"
)

var taipei = time.FixedZone("Taipei", 8*3600)

// newNightly makes a queue whose clock reads 23:00 Taipei time, with `held` jobs
// waiting in lane "photos" and a 3-a-night schedule at midnight.
func newNightly(t *testing.T, held int) (*Queue, *clock, string) {
	t.Helper()
	dir := t.TempDir()
	c := &clock{t: time.Date(2026, 10, 22, 23, 0, 0, 0, taipei)}
	q := open(t, dir, c)
	t.Cleanup(func() { q.Close() })
	for range held {
		enqueue(t, q, EnqueueRequest{Type: "post", Lane: "photos", Hold: true})
	}
	if _, err := q.SetSchedule(Schedule{Lane: "photos", Enabled: true, At: "00:00", Count: 3, GapSeconds: 120}); err != nil {
		t.Fatal(err)
	}
	return q, c, dir
}

func runSchedules(t *testing.T, q *Queue) []Job {
	t.Helper()
	out, err := q.RunSchedules()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestScheduleReleasesAtMidnightWithGaps(t *testing.T) {
	q, c, _ := newNightly(t, 10)
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("released %d before midnight", len(got))
	}
	st := q.Schedules()[0]
	want := time.Date(2026, 10, 23, 0, 0, 0, 0, taipei)
	if !st.NextRun.Equal(want) || st.NextCount != 3 || st.Held != 10 {
		t.Fatalf("status: next %v count %d held %d", st.NextRun, st.NextCount, st.Held)
	}

	c.Add(time.Hour) // 00:00
	got := runSchedules(t, q)
	if len(got) != 3 {
		t.Fatalf("released %d at midnight, want 3", len(got))
	}
	for i, j := range got {
		if j.ID != int64(i+1) || !j.RunAt.Equal(c.Now().Add(time.Duration(i)*2*time.Minute)) {
			t.Fatalf("job %d: id %d run_at %v", i, j.ID, j.RunAt)
		}
	}
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("released %d more in the same night", len(got))
	}
	if st := q.Schedules()[0]; !st.NextRun.Equal(want.AddDate(0, 0, 1)) || st.Held != 7 {
		t.Fatalf("after run: next %v held %d", st.NextRun, st.Held)
	}
}

func TestScheduleCatchesUpAfterSleep(t *testing.T) {
	q, c, _ := newNightly(t, 20)
	c.Add(24*time.Hour + 8*time.Hour) // 23:00 → 07:00 the day after next: asleep through two midnights
	if got := runSchedules(t, q); len(got) != 6 {
		t.Fatalf("released %d after missing two nights, want 6", len(got))
	}
}

func TestScheduleCatchUpIsCapped(t *testing.T) {
	q, c, _ := newNightly(t, 100)
	s := q.Schedules()[0].Schedule
	s.MaxCatchUp = 2
	q.SetSchedule(s)
	c.Add(10 * 24 * time.Hour) // away ten days
	if got := runSchedules(t, q); len(got) != 6 {
		t.Fatalf("released %d after ten days with max_catch_up 2, want 6", len(got))
	}
}

func TestOverrideAppliesToOneNight(t *testing.T) {
	q, c, _ := newNightly(t, 20)
	if _, err := q.SetOverride("photos", "2026-10-23", 5); err != nil {
		t.Fatal(err)
	}
	if st := q.Schedules()[0]; st.NextCount != 5 {
		t.Fatalf("next count %d, want the override 5", st.NextCount)
	}
	c.Add(time.Hour)
	if got := runSchedules(t, q); len(got) != 5 {
		t.Fatalf("released %d, want 5", len(got))
	}
	st := q.Schedules()[0]
	if len(st.Overrides) != 0 || st.NextCount != 3 {
		t.Fatalf("override should be spent: %v, next count %d", st.Overrides, st.NextCount)
	}
	c.Add(24 * time.Hour)
	if got := runSchedules(t, q); len(got) != 3 {
		t.Fatalf("next night released %d, want the default 3", len(got))
	}
}

func TestZeroNightReleasesNothing(t *testing.T) {
	q, c, _ := newNightly(t, 5)
	q.SetOverride("photos", "2026-10-23", 0)
	c.Add(time.Hour)
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("released %d on a night set to 0", len(got))
	}
}

func TestScheduleNeverReleasesTheSameNightTwiceAcrossRestart(t *testing.T) {
	q, c, dir := newNightly(t, 10)
	c.Add(time.Hour)
	runSchedules(t, q)
	q.wal.close() // crash right after the run

	q = open(t, dir, c)
	defer q.Close()
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("released %d again after restart", len(got))
	}
	if _, n := q.List(Filter{States: []State{Queued}}); n != 3 {
		t.Fatalf("%d queued after restart, want 3", n)
	}
}

func TestNewScheduleDoesNotFireImmediately(t *testing.T) {
	dir, c := t.TempDir(), &clock{t: time.Date(2026, 10, 23, 0, 30, 0, 0, taipei)} // just after midnight
	q := open(t, dir, c)
	defer q.Close()
	enqueue(t, q, EnqueueRequest{Type: "post", Lane: "l", Hold: true})
	q.SetSchedule(Schedule{Lane: "l", Enabled: true, At: "00:00", Count: 3})
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("new schedule released %d right away", len(got))
	}
}

func TestDisabledScheduleDoesNothing(t *testing.T) {
	q, c, _ := newNightly(t, 5)
	s := q.Schedules()[0].Schedule
	s.Enabled = false
	q.SetSchedule(s)
	c.Add(time.Hour)
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("disabled schedule released %d", len(got))
	}
}

func TestReenabledScheduleDoesNotCatchUpDaysItWasOff(t *testing.T) {
	q, c, _ := newNightly(t, 30)
	s := q.Schedules()[0].Schedule
	s.Enabled = false
	q.SetSchedule(s)
	c.Add(3 * 24 * time.Hour) // off for three nights
	s.Enabled = true
	q.SetSchedule(s)
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("re-enabling released %d for days the schedule was off", len(got))
	}
	c.Add(24 * time.Hour)
	if got := runSchedules(t, q); len(got) != 3 {
		t.Fatalf("next night released %d, want 3", len(got))
	}
}

func TestPauseIsAFullStop(t *testing.T) {
	q, c, _ := newNightly(t, 20)
	c.Add(time.Hour) // midnight: 3 released
	runSchedules(t, q)
	running := mustClaimID(t, q, 1)

	if _, err := q.SetPaused("photos", true); err != nil {
		t.Fatal(err)
	}
	// The running job finishes; the two released-but-not-started go back in line.
	if _, err := q.Complete(running.Job.ID, running.Token, nil); err != nil {
		t.Fatalf("running job should be allowed to finish: %v", err)
	}
	for _, id := range []int64{2, 3} {
		if j, _ := q.Get(id); j.State != Held {
			t.Fatalf("job %d is %s after pause, want held", id, j.State)
		}
	}

	c.Add(3 * 24 * time.Hour) // three nights pass while stopped
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("paused lane released %d", len(got))
	}
	q.SetPaused("photos", false)
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("resuming released %d: paused nights must not build up", len(got))
	}
	mustClaimNone(t, q) // and nothing posts the moment it resumes

	c.Add(24 * time.Hour) // the next night is normal, starting with the held-back jobs
	got := runSchedules(t, q)
	if len(got) != 3 || got[0].ID != 2 || got[1].ID != 3 || got[2].ID != 4 {
		t.Fatalf("first night after resume released %+v, want jobs 2, 3, 4", got)
	}
}

func TestPauseSwitchesTheScheduleOffAndRestoresIt(t *testing.T) {
	q, c, _ := newNightly(t, 20)
	q.SetPaused("photos", true)
	if s := q.Schedules()[0]; s.Enabled || !s.EnabledBeforePause {
		t.Fatalf("paused: enabled %v, remembered %v; want off, remembering on", s.Enabled, s.EnabledBeforePause)
	}
	q.SetPaused("photos", true) // pausing twice must not overwrite the memory with "off"
	if s := q.Schedules()[0]; !s.EnabledBeforePause {
		t.Fatal("second pause forgot the schedule was on")
	}

	// Settings saved while stopped are remembered, not applied.
	s := q.Schedules()[0].Schedule
	s.Enabled, s.Count = true, 1
	q.SetSchedule(s)
	if s := q.Schedules()[0]; s.Enabled || !s.EnabledBeforePause || s.Count != 1 {
		t.Fatalf("saved while paused: %+v", s.Schedule)
	}

	q.SetPaused("photos", false)
	if s := q.Schedules()[0]; !s.Enabled || s.EnabledBeforePause {
		t.Fatalf("resumed: enabled %v, remembered %v; want on, cleared", s.Enabled, s.EnabledBeforePause)
	}
	c.Add(time.Hour)
	if got := runSchedules(t, q); len(got) != 1 {
		t.Fatalf("first night after resume released %d, want the count saved while paused (1)", len(got))
	}
}

func TestPauseKeepsAnOffScheduleOff(t *testing.T) {
	q, c, _ := newNightly(t, 20)
	s := q.Schedules()[0].Schedule
	s.Enabled = false
	q.SetSchedule(s)
	q.SetPaused("photos", true)
	q.SetPaused("photos", false)
	if s := q.Schedules()[0]; s.Enabled {
		t.Fatal("resuming switched on a schedule that was off before the pause")
	}
	c.Add(time.Hour)
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("released %d from a schedule that should be off", len(got))
	}
}

func TestNothingCanBeReleasedOrRetriedIntoAPausedLane(t *testing.T) {
	q, c, _ := newNightly(t, 5)
	c.Add(time.Hour)
	runSchedules(t, q)
	l := mustClaimID(t, q, 1)
	q.Fail(l.Job.ID, l.Token, FailRequest{Error: "boom", Permanent: true})
	q.SetPaused("photos", true)

	if _, err := q.Release("photos", 1, time.Time{}); err == nil {
		t.Fatal("released into a paused lane")
	}
	j, err := q.Retry(1)
	if err != nil || j.State != Held {
		t.Fatalf("retry while paused: %s, %v; want held", j.State, err)
	}
	q.SetPaused("photos", false)
	mustClaimNone(t, q) // the retried job waits for the next run, like everything else
	c.Add(24 * time.Hour)
	if got := runSchedules(t, q); len(got) == 0 || got[0].ID != 1 {
		t.Fatalf("next run after resume: %+v, want the retried job first", got)
	}
}

func TestPauseSurvivesRestart(t *testing.T) {
	q, c, dir := newNightly(t, 10)
	q.SetPaused("photos", true)
	q.wal.close()
	q = open(t, dir, c)
	defer q.Close()
	c.Add(time.Hour)
	if got := runSchedules(t, q); len(got) != 0 {
		t.Fatalf("pause was lost across a restart: released %d", len(got))
	}
}

func TestHoldUndoesRelease(t *testing.T) {
	q, _, _ := newQueue(t)
	a := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l", Hold: true})
	b := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l", Hold: true})
	q.Release("l", 2, time.Time{})
	if _, err := q.Hold(b.ID); err != nil {
		t.Fatal(err)
	}
	l := mustClaimID(t, q, a.ID)
	q.Complete(l.Job.ID, l.Token, nil)
	mustClaimNone(t, q) // b is held again
	if got, _ := q.Release("l", 1, time.Time{}); len(got) != 1 || got[0].ID != b.ID {
		t.Fatalf("release after hold: %+v", got)
	}
	if _, err := q.Hold(a.ID); err == nil {
		t.Fatal("held a finished job")
	}
}

func TestBadSchedulesRejected(t *testing.T) {
	q, _, _ := newQueue(t)
	for _, s := range []Schedule{{Lane: "l", At: "25:00"}, {At: "00:00"}, {Lane: "l", At: "00:00", Count: -1}} {
		if _, err := q.SetSchedule(s); err == nil {
			t.Errorf("accepted %+v", s)
		}
	}
}

func TestMoveSwapsNeighbors(t *testing.T) {
	q, _, _ := newQueue(t)
	a := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l", Hold: true})
	b := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l", Hold: true})
	c := enqueue(t, q, EnqueueRequest{Type: "x", Lane: "l", Hold: true})
	if _, err := q.Move(c.ID, -1); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Move(a.ID, -1); err == nil {
		t.Fatal("moved the first job earlier")
	}
	got, _ := q.Release("l", 3, time.Time{})
	if got[0].ID != a.ID || got[1].ID != c.ID || got[2].ID != b.ID {
		t.Fatalf("order after move: %d %d %d, want a c b", got[0].ID, got[1].ID, got[2].ID)
	}
	l := mustClaimID(t, q, a.ID)
	if _, err := q.Move(c.ID, -1); err == nil {
		t.Fatal("moved past a running job")
	}
	q.Complete(l.Job.ID, l.Token, nil)
}

// newSpread is newNightly with the schedule spread hourly over the day.
func newSpread(t *testing.T, held, count int) (*Queue, *clock) {
	t.Helper()
	q, c, _ := newNightly(t, held)
	if _, err := q.SetSchedule(Schedule{Lane: "photos", Enabled: true, At: "00:00", Count: count, GapSeconds: 120, IntervalSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	return q, c
}

func midnight(c *clock) time.Time {
	n := c.Now()
	return time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, taipei)
}

func TestSpreadPutsWhatDoesntFitAtTheRunAndTheRestHourly(t *testing.T) {
	q, c := newSpread(t, 40, 30)
	c.Add(time.Hour) // 00:00
	got := runSchedules(t, q)
	if len(got) != 30 {
		t.Fatalf("released %d, want 30", len(got))
	}
	m := midnight(c)
	for i, j := range got {
		want := m // 23 hourly slots (01:00-23:00), so 7 go at midnight
		if i >= 7 {
			want = m.Add(time.Duration(i-6) * time.Hour)
		}
		if !j.RunAt.Equal(want) {
			t.Fatalf("job %d runs at %v, want %v", i, j.RunAt, want)
		}
	}
}

func TestSpreadFewerThanTheHoursAreAllHourly(t *testing.T) {
	q, c := newSpread(t, 40, 3)
	c.Add(time.Hour)
	got := runSchedules(t, q)
	m := midnight(c)
	for i, j := range got {
		if want := m.Add(time.Duration(i+1) * time.Hour); !j.RunAt.Equal(want) {
			t.Fatalf("job %d runs at %v, want %v", i, j.RunAt, want)
		}
	}
}

func TestSpreadLateJobsMoveAlongFromNow(t *testing.T) {
	q, c := newSpread(t, 40, 5)
	c.Add(time.Hour)
	runSchedules(t, q)
	m := midnight(c)
	c.Add(3*time.Hour + 30*time.Minute) // asleep: 01:00, 02:00 and 03:00 were missed
	runSchedules(t, q)
	jobs := photosIn(t, q, Queued)
	for i, j := range jobs {
		if want := m.Add(3*time.Hour + 30*time.Minute + time.Duration(i)*time.Hour); !j.RunAt.Equal(want) {
			t.Fatalf("job %d runs at %v, want %v", i, j.RunAt, want)
		}
	}
	if len(jobs) != 5 {
		t.Fatalf("%d queued, want 5", len(jobs))
	}
}

func TestSpreadALittleLateIsLeftAlone(t *testing.T) {
	q, c := newSpread(t, 40, 5)
	c.Add(time.Hour)
	runSchedules(t, q)
	c.Add(time.Hour + 5*time.Minute) // the worker is just busy
	runSchedules(t, q)
	if j := photosIn(t, q, Queued)[0]; !j.RunAt.Equal(midnight(c).Add(time.Hour)) {
		t.Fatalf("moved to %v while only 5 minutes late", j.RunAt)
	}
}

func TestSpreadWhatDoesntFitWaitsForTheNextRun(t *testing.T) {
	q, c := newSpread(t, 40, 5)
	c.Add(time.Hour) // 00:00 on the 23rd
	runSchedules(t, q)
	c.Add(21*time.Hour + 30*time.Minute) // 21:30: only 21:30, 22:30 and 23:30 fit
	runSchedules(t, q)
	next := midnight(c).AddDate(0, 0, 1)
	jobs := photosIn(t, q, Queued)
	for i, want := range []time.Duration{0, time.Hour, 2 * time.Hour} {
		if w := c.Now().Add(want); !jobs[i].RunAt.Equal(w) {
			t.Fatalf("job %d runs at %v, want %v", i, jobs[i].RunAt, w)
		}
	}
	for _, j := range jobs[3:] {
		if !j.RunAt.Equal(next) {
			t.Fatalf("leftover runs at %v, want the next run %v", j.RunAt, next)
		}
	}
	// The next run releases the new day after them, and leaves the leftovers at the run.
	c.Add(2*time.Hour + 30*time.Minute) // 00:00 on the 24th, nothing was claimed
	runSchedules(t, q)
	runSchedules(t, q)
	jobs = photosIn(t, q, Queued)
	if len(jobs) != 10 {
		t.Fatalf("%d queued, want 10", len(jobs))
	}
	for _, j := range jobs[:5] { // yesterday's five, all due now at the run (in order)
		if j.RunAt.After(c.Now()) {
			t.Fatalf("yesterday's job waits until %v", j.RunAt)
		}
	}
	for i, j := range jobs[5:] {
		if want := c.Now().Add(time.Duration(i+1) * time.Hour); !j.RunAt.Equal(want) {
			t.Fatalf("today's job %d runs at %v, want %v", i, j.RunAt, want)
		}
	}
}

func TestSpreadMissedRunGoesOnWaking(t *testing.T) {
	q, c := newSpread(t, 40, 30)
	c.Add(9 * time.Hour) // asleep through midnight; awake at 08:00
	got := runSchedules(t, q)
	if len(got) != 30 {
		t.Fatalf("released %d, want 30", len(got))
	}
	// 15 hourly slots are left (09:00-23:00), so 15 go now, at the missed run.
	now := 0
	for _, j := range got {
		if !j.RunAt.After(c.Now()) {
			now++
		}
	}
	if now != 15 {
		t.Fatalf("%d due on waking, want 15", now)
	}
}

func TestBadIntervalRejected(t *testing.T) {
	q, _, _ := newNightly(t, 0)
	for _, iv := range []int{-1, 30} {
		if _, err := q.SetSchedule(Schedule{Lane: "photos", Enabled: true, At: "00:00", Count: 3, IntervalSeconds: iv}); err == nil {
			t.Fatalf("interval %d accepted", iv)
		}
	}
}

// photosIn is the "photos" lane's jobs in the given state, in lane order.
func photosIn(t *testing.T, q *Queue, state State) []Job {
	t.Helper()
	name := "photos"
	out, _ := q.List(Filter{Lane: &name, States: []State{state}})
	return out
}
