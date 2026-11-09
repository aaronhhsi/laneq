// Package api serves a Queue over HTTP/JSON, so workers can be written in any language.
//
//	POST /v1/jobs                    enqueue            {type, lane, key, payload, run_at, hold, max_attempts}
//	GET  /v1/jobs?lane=&state=&type=&newest=&offset=&limit=   list (total in X-Total-Count)
//	GET  /v1/jobs/{id}               get
//	POST /v1/jobs/{id}/retry         retry a dead job (or skip its backoff)
//	POST /v1/jobs/{id}/cancel        cancel
//	POST /v1/jobs/{id}/move          swap with a neighbor in its lane   {delta: -1 | 1}
//	POST /v1/jobs/{id}/hold          undo a release: queued → held
//	POST /v1/claim                   lease a job        {worker, types, lease_seconds, wait_seconds} → 200 {job, token} | 204
//	POST /v1/jobs/{id}/heartbeat     extend the lease   {token, checkpoint, lease_seconds}
//	POST /v1/jobs/{id}/complete      finish             {token, result}
//	POST /v1/jobs/{id}/fail          report a failure   {token, error, retry_after_seconds, permanent}
//	GET  /v1/lanes                   lane summaries
//	POST /v1/lanes/{lane}/release    release held jobs  {n, run_at}
//	POST /v1/lanes/{lane}/pause | /resume
//	POST /v1/lanes/{lane}/clear      cancel the lane's waiting jobs {states} (default queued, held, dead) → {canceled}
//	POST /v1/lanes/{lane}/rename     move its jobs, pause and schedule to a new name {to} → {moved}
//	GET  /v1/schedules               schedules with their next run
//	PUT  /v1/schedules/{lane}        set a daily release {enabled, at: "HH:MM", count, gap_seconds, max_catch_up}
//	POST /v1/schedules/{lane}/override   one day's count {day: "YYYY-MM-DD", count} (count < 0 clears it)
//	GET  /v1/apps                    the apps using this laneq, with their status
//	POST /v1/apps/{name}/heartbeat   a worker checks in {title, dashboard, start_hint, pid} → {stop}
//	POST /v1/apps/{name}/bye         a worker exited cleanly {pid}
//	POST /v1/apps/{name}/stop | /start | /kill   from this machine: stop after the current job, allow again, end now
//	GET  /files/{name}/...           read-only static files mounted with Options.Files (e.g. media previews)
//	GET  /                           the dashboard
//	GET  /healthz
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aaronhhsi/laneq/internal/apps"
	"github.com/aaronhhsi/laneq/internal/queue"
	"github.com/aaronhhsi/laneq/internal/ui"
)

// Options configures the handler.
type Options struct {
	// Files mounts directories read-only at /files/<name>/, so job payloads can
	// point the dashboard at previews (e.g. "/files/archive/2023-10/1.jpg").
	Files map[string]string
	// Apps tracks the applications using this laneq (nil: a fresh registry).
	Apps *apps.Registry
}

// MaxWait caps how long a claim may long-poll.
const MaxWait = 60 * time.Second

type server struct {
	q    *queue.Queue
	apps *apps.Registry
}

// Handler returns the HTTP API for q.
func Handler(q *queue.Queue, opts Options) http.Handler {
	if opts.Apps == nil {
		opts.Apps = apps.New()
	}
	s := &server{q, opts.Apps}
	mux := http.NewServeMux()
	mux.Handle("GET /{$}", ui.Handler())
	for name, dir := range opts.Files {
		prefix := "/files/" + name + "/"
		mux.Handle("GET "+prefix, http.StripPrefix(prefix, http.FileServer(http.Dir(dir))))
	}
	mux.HandleFunc("POST /v1/jobs/{id}/move", s.withID(s.move))
	mux.HandleFunc("POST /v1/jobs/{id}/hold", s.withID(func(id int64, r *http.Request) (any, error) { return s.q.Hold(id) }))
	mux.HandleFunc("GET /v1/schedules", func(w http.ResponseWriter, r *http.Request) {
		out := s.q.Schedules()
		if out == nil {
			out = []queue.ScheduleStatus{}
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("PUT /v1/schedules/{lane}", s.setSchedule)
	mux.HandleFunc("POST /v1/schedules/{lane}/override", s.override)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /v1/jobs", s.enqueue)
	mux.HandleFunc("GET /v1/jobs", s.list)
	mux.HandleFunc("GET /v1/jobs/{id}", s.withID(func(id int64, r *http.Request) (any, error) { return s.q.Get(id) }))
	mux.HandleFunc("POST /v1/jobs/{id}/retry", s.withID(func(id int64, r *http.Request) (any, error) { return s.q.Retry(id) }))
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.withID(func(id int64, r *http.Request) (any, error) { return s.q.Cancel(id) }))
	mux.HandleFunc("POST /v1/jobs/{id}/heartbeat", s.withID(s.heartbeat))
	mux.HandleFunc("POST /v1/jobs/{id}/complete", s.withID(s.complete))
	mux.HandleFunc("POST /v1/jobs/{id}/fail", s.withID(s.fail))
	mux.HandleFunc("POST /v1/claim", s.claim)
	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, struct {
			queue.Stats
			Apps int `json:"apps"`
		}{s.q.Stats(), len(s.apps.List())})
	})
	mux.HandleFunc("GET /v1/lanes", func(w http.ResponseWriter, r *http.Request) {
		out := s.q.Lanes()
		if out == nil {
			out = []queue.LaneSummary{}
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("POST /v1/lanes/{lane}/release", s.release)
	mux.HandleFunc("POST /v1/lanes/{lane}/pause", s.pause(true))
	mux.HandleFunc("POST /v1/lanes/{lane}/resume", s.pause(false))
	mux.HandleFunc("POST /v1/lanes/{lane}/clear", s.clear)
	mux.HandleFunc("POST /v1/lanes/{lane}/rename", s.rename)
	mux.HandleFunc("GET /v1/apps", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.apps.List()) })
	mux.HandleFunc("POST /v1/apps/{name}/heartbeat", s.appHeartbeat)
	mux.HandleFunc("POST /v1/apps/{name}/bye", s.bye)
	mux.HandleFunc("POST /v1/apps/{name}/stop", s.appAction(s.apps.Stop))
	mux.HandleFunc("POST /v1/apps/{name}/start", s.appAction(func(n string) error { s.apps.Start(n); return nil }))
	mux.HandleFunc("POST /v1/apps/{name}/kill", s.appAction(s.apps.KillApp))
	return sameSite(mux)
}

// FromThisMachine: the request comes from this computer. Killing processes and
// shutting down are only accepted from here.
func FromThisMachine(r *http.Request) bool {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return net.ParseIP(host).IsLoopback()
}

// sameSite refuses changes requested by pages from other sites: the API has no
// auth, and a browser sends any site's requests to localhost along with its
// Origin. Workers and scripts send no Origin.
func sameSite(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); r.Method != http.MethodGet && o != "" && o != "http://"+r.Host {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "requests from other sites are refused"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) appHeartbeat(w http.ResponseWriter, r *http.Request) {
	var info apps.Info
	if !readJSON(w, r, &info) {
		return
	}
	writeJSON(w, 200, map[string]bool{"stop": s.apps.Heartbeat(r.PathValue("name"), info)})
}

func (s *server) bye(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PID int `json:"pid"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	s.apps.Bye(r.PathValue("name"), req.PID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) appAction(fn func(name string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !FromThisMachine(r) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "only accepted from this machine"})
			return
		}
		if err := fn(r.PathValue("name")); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, apps.ErrNotFound) {
				status = http.StatusNotFound
			}
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, s.apps.List())
	}
}

func (s *server) enqueue(w http.ResponseWriter, r *http.Request) {
	var req queue.EnqueueRequest
	if !readJSON(w, r, &req) {
		return
	}
	job, created, err := s.q.Enqueue(req)
	if err != nil {
		writeErr(w, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK // idempotency key matched an existing job
	}
	writeJSON(w, status, job.Public())
}

func (s *server) list(w http.ResponseWriter, r *http.Request) {
	var f queue.Filter
	v := r.URL.Query()
	if v.Has("lane") {
		lane := v.Get("lane")
		f.Lane = &lane
	}
	for _, st := range strings.Split(v.Get("state"), ",") {
		if st != "" {
			f.States = append(f.States, queue.State(st))
		}
	}
	f.Type = v.Get("type")
	f.Newest = v.Get("newest") == "true"
	f.Offset, _ = strconv.Atoi(v.Get("offset"))
	f.Limit, _ = strconv.Atoi(v.Get("limit"))
	f.Offset = max(f.Offset, 0)
	jobs, total := s.q.List(f)
	if jobs == nil {
		jobs = []queue.Job{}
	}
	w.Header().Set("X-Total-Count", strconv.Itoa(total))
	writeJSON(w, 200, jobs)
}

func (s *server) move(id int64, r *http.Request) (any, error) {
	var req struct {
		Delta int `json:"delta"`
	}
	if err := decode(r, &req); err != nil {
		return nil, err
	}
	return s.q.Move(id, req.Delta)
}

func (s *server) setSchedule(w http.ResponseWriter, r *http.Request) {
	var sc queue.Schedule
	if !readJSON(w, r, &sc) {
		return
	}
	sc.Lane = r.PathValue("lane")
	st, err := s.q.SetSchedule(sc)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, st)
}

func (s *server) override(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Day   string `json:"day"`
		Count int    `json:"count"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	st, err := s.q.SetOverride(r.PathValue("lane"), req.Day, req.Count)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, st)
}

type claimRequest struct {
	Worker       string   `json:"worker"`
	Types        []string `json:"types"`
	LeaseSeconds float64  `json:"lease_seconds"`
	WaitSeconds  float64  `json:"wait_seconds"`
}

func (s *server) claim(w http.ResponseWriter, r *http.Request) {
	var req claimRequest
	if !readJSON(w, r, &req) {
		return
	}
	wait := min(seconds(req.WaitSeconds), MaxWait)
	lease, err := s.q.ClaimWait(r.Context(), req.Worker, req.Types, seconds(req.LeaseSeconds), wait)
	if err != nil {
		if r.Context().Err() != nil {
			return // client went away
		}
		writeErr(w, err)
		return
	}
	if lease == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, 200, lease)
}

type heartbeatRequest struct {
	Token        string          `json:"token"`
	Checkpoint   json.RawMessage `json:"checkpoint"`
	LeaseSeconds float64         `json:"lease_seconds"`
}

func (s *server) heartbeat(id int64, r *http.Request) (any, error) {
	var req heartbeatRequest
	if err := decode(r, &req); err != nil {
		return nil, err
	}
	return s.q.Heartbeat(id, req.Token, req.Checkpoint, seconds(req.LeaseSeconds))
}

type completeRequest struct {
	Token  string          `json:"token"`
	Result json.RawMessage `json:"result"`
}

func (s *server) complete(id int64, r *http.Request) (any, error) {
	var req completeRequest
	if err := decode(r, &req); err != nil {
		return nil, err
	}
	return s.q.Complete(id, req.Token, req.Result)
}

type failRequest struct {
	Token             string  `json:"token"`
	Error             string  `json:"error"`
	RetryAfterSeconds float64 `json:"retry_after_seconds"`
	Permanent         bool    `json:"permanent"`
}

func (s *server) fail(id int64, r *http.Request) (any, error) {
	var req failRequest
	if err := decode(r, &req); err != nil {
		return nil, err
	}
	return s.q.Fail(id, req.Token, queue.FailRequest{
		Error:      req.Error,
		RetryAfter: seconds(req.RetryAfterSeconds),
		Permanent:  req.Permanent,
	})
}

type releaseRequest struct {
	N     int       `json:"n"`
	RunAt time.Time `json:"run_at"`
}

func (s *server) release(w http.ResponseWriter, r *http.Request) {
	var req releaseRequest
	if !readJSON(w, r, &req) {
		return
	}
	if req.N <= 0 {
		writeErr(w, fmt.Errorf("%w: n must be positive", queue.ErrInvalid))
		return
	}
	jobs, err := s.q.Release(r.PathValue("lane"), req.N, req.RunAt)
	if err != nil {
		writeErr(w, err)
		return
	}
	if jobs == nil {
		jobs = []queue.Job{}
	}
	writeJSON(w, 200, jobs)
}

func (s *server) rename(w http.ResponseWriter, r *http.Request) {
	var req struct {
		To string `json:"to"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	n, err := s.q.RenameLane(r.PathValue("lane"), req.To)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]int{"moved": n})
}

func (s *server) clear(w http.ResponseWriter, r *http.Request) {
	var req struct {
		States []queue.State `json:"states"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	n, err := s.q.Clear(r.PathValue("lane"), req.States)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]int{"canceled": n})
}

func (s *server) pause(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ls, err := s.q.SetPaused(r.PathValue("lane"), paused)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, 200, ls)
	}
}

// withID parses {id} and writes whatever fn returns.
func (s *server) withID(fn func(id int64, r *http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeErr(w, fmt.Errorf("%w: bad job id", queue.ErrInvalid))
			return
		}
		out, err := fn(id, r)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, 200, out)
	}
}

func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

func decode(r *http.Request, v any) error {
	if r.ContentLength == 0 {
		return nil
	}
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", queue.ErrInvalid, err)
	}
	return nil
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := decode(r, v); err != nil {
		writeErr(w, err)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, queue.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, queue.ErrLeaseLost), errors.Is(err, queue.ErrBadState):
		status = http.StatusConflict
	case errors.Is(err, queue.ErrInvalid):
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
