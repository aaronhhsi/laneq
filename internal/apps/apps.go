// Package apps keeps track of the applications using this laneq: each one's
// worker checks in every few seconds (a heartbeat), and the answer tells it
// whether to keep going. Stopping an app is cooperative: its worker finishes the
// job it's on, says goodbye and exits. Killing is not: the process is ended at
// once, and its job's lease runs out and the job is retried later.
//
// A stop is sticky: a worker that comes back (restarted by whatever launched
// it) is told to stop again, until Start clears it. The registry lives in
// memory; workers check in again within seconds of laneq restarting.
package apps

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Stale is how long an app can go without checking in before it counts as gone.
const Stale = 15 * time.Second

var ErrNotFound = errors.New("no such app")

// Info is what a worker says about itself when it checks in.
type Info struct {
	Title     string `json:"title,omitempty"`
	Dashboard string `json:"dashboard,omitempty"`  // the app's own dashboard, if it has one
	StartHint string `json:"start_hint,omitempty"` // how to start it again, e.g. "run start.cmd"
	PID       int    `json:"pid"`
}

// App is one app as the dashboard sees it.
type App struct {
	Name string `json:"name"`
	Info
	LastSeen time.Time `json:"last_seen"`
	Stop     bool      `json:"stop"`   // asked to stop (sticky until Start)
	Status   string    `json:"status"` // running | stopping | stopped | not responding
}

type app struct {
	info     Info
	lastSeen time.Time
	stop     bool
	exited   bool // said goodbye, or was killed
}

// Registry is safe for concurrent use.
type Registry struct {
	mu   sync.Mutex
	apps map[string]*app
	Now  func() time.Time
	Kill func(pid int) error // ends a process and its children
}

func New() *Registry {
	return &Registry{apps: map[string]*app{}, Now: time.Now, Kill: killTree}
}

// Heartbeat records that an app's worker is alive; it returns whether the worker should stop.
func (r *Registry) Heartbeat(name string, info Info) (stop bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.apps[name]
	if a == nil {
		a = &app{}
		r.apps[name] = a
	}
	a.info, a.lastSeen, a.exited = info, r.Now(), false
	return a.stop
}

// Bye records that a worker exited cleanly.
func (r *Registry) Bye(name string, pid int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a := r.apps[name]; a != nil && a.info.PID == pid {
		a.exited = true
	}
}

// Stop asks an app's worker to finish its current job and exit.
func (r *Registry) Stop(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.apps[name]
	if a == nil {
		return ErrNotFound
	}
	a.stop = true
	return nil
}

// Start clears a stop, so the app's worker may run again once it's started.
// An app laneq hasn't heard from is fine: there's nothing to clear.
func (r *Registry) Start(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a := r.apps[name]; a != nil {
		a.stop = false
	}
}

// KillApp ends an app's worker process now (and keeps the app stopped).
func (r *Registry) KillApp(name string) error {
	r.mu.Lock()
	a := r.apps[name]
	if a == nil {
		r.mu.Unlock()
		return ErrNotFound
	}
	a.stop = true
	pid, alive := a.info.PID, !a.exited && r.Now().Sub(a.lastSeen) < Stale
	r.mu.Unlock()
	if !alive || pid <= 0 {
		return nil // already gone
	}
	if err := r.Kill(pid); err != nil {
		return err
	}
	r.Bye(name, pid)
	return nil
}

// StopAll asks every app to stop, for powering off.
func (r *Registry) StopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, a := range r.apps {
		a.stop = true
	}
}

// Running lists the apps whose workers are still up.
func (r *Registry) Running() []string {
	var out []string
	for _, a := range r.List() {
		if a.Status == "running" || a.Status == "stopping" {
			out = append(out, a.Name)
		}
	}
	return out
}

// List returns every app laneq has heard from, by name.
func (r *Registry) List() []App {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.Now()
	out := []App{}
	for name, a := range r.apps {
		alive := !a.exited && now.Sub(a.lastSeen) < Stale
		status := "running"
		switch {
		case alive && a.stop:
			status = "stopping"
		case !alive && (a.stop || a.exited):
			status = "stopped"
		case !alive:
			status = "not responding"
		}
		out = append(out, App{Name: name, Info: a.info, LastSeen: a.lastSeen, Stop: a.stop, Status: status})
	}
	slices.SortFunc(out, func(a, b App) int { return strings.Compare(a.Name, b.Name) })
	return out
}

func killTree(pid int) error {
	if runtime.GOOS == "windows" {
		// /T takes its children too (ffmpeg, git, ...), which Process.Kill would leave running.
		out, err := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).CombinedOutput()
		if err != nil {
			return errors.New(strings.TrimSpace(string(out)))
		}
		return nil
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Kill()
}
