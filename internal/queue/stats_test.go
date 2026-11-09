package queue

import "testing"

func TestStatsCountsJobsLanesAndAttempts(t *testing.T) {
	q, _, _ := newQueue(t)
	a := enqueue(t, q, EnqueueRequest{Type: "post", Lane: "photos"})
	enqueue(t, q, EnqueueRequest{Type: "post", Lane: "photos"})
	enqueue(t, q, EnqueueRequest{Type: "post", Lane: "minju"})
	enqueue(t, q, EnqueueRequest{Type: "refresh"})
	l := mustClaimID(t, q, a.ID)
	if _, err := q.Complete(l.Job.ID, l.Token, nil); err != nil {
		t.Fatal(err)
	}

	s := q.Stats()
	if s.Jobs != 4 || s.Lanes != 2 || s.Attempts != 1 {
		t.Fatalf("jobs %d lanes %d attempts %d, want 4 2 1", s.Jobs, s.Lanes, s.Attempts)
	}
	if s.ByState[Succeeded] != 1 || s.ByState[Queued] != 3 {
		t.Fatalf("by state %v", s.ByState)
	}
	if s.DataBytes == 0 {
		t.Fatal("no data on disk counted")
	}
	if s.Since.IsZero() {
		t.Fatal("since not set")
	}
}
