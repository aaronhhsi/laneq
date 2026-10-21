// Package api serves a Queue over HTTP/JSON, so workers can be written in any language.
//
//	POST /v1/jobs                    enqueue            {type, lane, key, payload, run_at, hold, max_attempts}
//	GET  /v1/jobs?lane=&state=&type= list
//	GET  /v1/jobs/{id}               get
//	POST /v1/jobs/{id}/retry         retry a dead job (or skip its backoff)
//	POST /v1/jobs/{id}/cancel        cancel
//	POST /v1/claim                   lease a job        {worker, types, lease_seconds, wait_seconds} → 200 {job, token} | 204
//	POST /v1/jobs/{id}/heartbeat     extend the lease   {token, checkpoint, lease_seconds}
//	POST /v1/jobs/{id}/complete      finish             {token, result}
//	POST /v1/jobs/{id}/fail          report a failure   {token, error, retry_after_seconds, permanent}
//	GET  /v1/lanes                   lane summaries
//	POST /v1/lanes/{lane}/release    release held jobs  {n, run_at}
//	POST /v1/lanes/{lane}/pause | /resume
//	GET  /healthz
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aaronhhsi/laneq/internal/queue"
)

// MaxWait caps how long a claim may long-poll.
const MaxWait = 60 * time.Second

type server struct{ q *queue.Queue }

// Handler returns the HTTP API for q.
func Handler(q *queue.Queue) http.Handler {
	s := &server{q}
	mux := http.NewServeMux()
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
	mux.HandleFunc("GET /v1/lanes", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.q.Lanes()) })
	mux.HandleFunc("POST /v1/lanes/{lane}/release", s.release)
	mux.HandleFunc("POST /v1/lanes/{lane}/pause", s.pause(true))
	mux.HandleFunc("POST /v1/lanes/{lane}/resume", s.pause(false))
	return mux
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
	jobs := s.q.List(f)
	if jobs == nil {
		jobs = []queue.Job{}
	}
	writeJSON(w, 200, jobs)
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
