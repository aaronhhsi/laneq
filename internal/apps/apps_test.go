package apps

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func newTest(t *testing.T) (*Registry, *fakeClock, *[]int) {
	c := &fakeClock{time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	var killed []int
	r := New()
	r.Now = c.now
	r.Kill = func(pid int) error { killed = append(killed, pid); return nil }
	return r, c, &killed
}

func status(r *Registry, name string) string {
	for _, a := range r.List() {
		if a.Name == name {
			return a.Status
		}
	}
	return "unknown"
}

func TestStopIsCooperativeAndSticky(t *testing.T) {
	r, c, killed := newTest(t)
	if r.Heartbeat("posts", Info{PID: 10}) {
		t.Fatal("a new app was told to stop")
	}
	if got := status(r, "posts"); got != "running" {
		t.Fatalf("status %q; want running", got)
	}
	if err := r.Stop("posts"); err != nil {
		t.Fatal(err)
	}
	if got := status(r, "posts"); got != "stopping" {
		t.Fatalf("after Stop: %q; want stopping (it finishes its job first)", got)
	}
	if !r.Heartbeat("posts", Info{PID: 10}) {
		t.Fatal("the worker wasn't told to stop")
	}
	r.Bye("posts", 10)
	if got := status(r, "posts"); got != "stopped" {
		t.Fatalf("after bye: %q; want stopped", got)
	}
	// Restarted by its launcher: still stopped until Start.
	if !r.Heartbeat("posts", Info{PID: 11}) {
		t.Fatal("a restarted worker of a stopped app wasn't told to stop")
	}
	r.Start("posts")
	if r.Heartbeat("posts", Info{PID: 12}) {
		t.Fatal("still stopped after Start")
	}
	c.t = c.t.Add(Stale + time.Second)
	if got := status(r, "posts"); got != "not responding" {
		t.Fatalf("silent: %q; want not responding", got)
	}
	if len(*killed) != 0 {
		t.Fatalf("killed %v; stopping never kills", *killed)
	}
	if err := r.Stop("nope"); err != ErrNotFound {
		t.Errorf("Stop(unknown) = %v", err)
	}
}

func TestKillEndsTheProcessAndStaysStopped(t *testing.T) {
	r, c, killed := newTest(t)
	r.Heartbeat("scraper", Info{PID: 42})
	if err := r.KillApp("scraper"); err != nil {
		t.Fatal(err)
	}
	if len(*killed) != 1 || (*killed)[0] != 42 {
		t.Fatalf("killed %v; want [42]", *killed)
	}
	if got := status(r, "scraper"); got != "stopped" {
		t.Fatalf("after kill: %q", got)
	}
	if err := r.KillApp("scraper"); err != nil || len(*killed) != 1 {
		t.Fatalf("killing a dead app again: %v, killed %v", err, *killed)
	}
	r.Heartbeat("other", Info{PID: 7})
	c.t = c.t.Add(Stale + time.Second) // gone quiet: its PID may belong to something else by now
	if err := r.KillApp("other"); err != nil || len(*killed) != 1 {
		t.Fatalf("killed a stale PID: %v, %v", err, *killed)
	}
}

func TestStopAllAndRunning(t *testing.T) {
	r, _, _ := newTest(t)
	r.Heartbeat("a", Info{PID: 1})
	r.Heartbeat("b", Info{PID: 2})
	r.StopAll()
	if n := len(r.Running()); n != 2 {
		t.Fatalf("running %d; stopping apps still count until they exit", n)
	}
	r.Bye("a", 1)
	r.Bye("b", 99) // a stale goodbye from another process doesn't count
	if got := r.Running(); len(got) != 1 || got[0] != "b" {
		t.Fatalf("running %v; want [b]", got)
	}
}
