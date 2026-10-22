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
