package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aaronhhsi/laneq/internal/queue"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	q, err := queue.Open(queue.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(q, Options{}))
	t.Cleanup(func() { srv.Close(); q.Close() })
	return srv
}

func call(t *testing.T, srv *httptest.Server, method, path string, body any, out any) int {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, srv.URL+path, &buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode
}

func TestWorkerLifecycleOverHTTP(t *testing.T) {
	srv := newServer(t)

	var job queue.Job
	if code := call(t, srv, "POST", "/v1/jobs", map[string]any{"type": "publish", "lane": "photos", "key": "post-1", "payload": map[string]string{"post": "a"}}, &job); code != 201 {
		t.Fatalf("enqueue: %d", code)
	}
	if code := call(t, srv, "POST", "/v1/jobs", map[string]any{"type": "publish", "key": "post-1"}, nil); code != 200 {
		t.Fatalf("duplicate enqueue: %d, want 200 (existing job)", code)
	}

	var lease queue.Lease
	if code := call(t, srv, "POST", "/v1/claim", map[string]any{"worker": "py-1", "types": []string{"publish"}}, &lease); code != 200 {
		t.Fatalf("claim: %d", code)
	}
	if lease.Job.ID != job.ID || lease.Token == "" || string(lease.Job.Payload) != `{"post":"a"}` {
		t.Fatalf("claim returned %+v", lease)
	}

	hb := map[string]any{"token": lease.Token, "checkpoint": map[string]string{"container": "17"}}
	if code := call(t, srv, "POST", "/v1/jobs/1/heartbeat", hb, nil); code != 200 {
		t.Fatalf("heartbeat: %d", code)
	}
	if code := call(t, srv, "POST", "/v1/jobs/1/complete", map[string]any{"token": "wrong"}, nil); code != 409 {
		t.Fatalf("complete with wrong token: %d, want 409", code)
	}
	var done queue.Job
	if code := call(t, srv, "POST", "/v1/jobs/1/complete", map[string]any{"token": lease.Token, "result": map[string]string{"media_id": "99"}}, &done); code != 200 {
		t.Fatalf("complete: %d", code)
	}
	if done.State != queue.Succeeded || done.LeaseToken != "" {
		t.Fatalf("completed job: %+v", done)
	}

	if code := call(t, srv, "POST", "/v1/claim", map[string]any{"worker": "py-1"}, nil); code != 204 {
		t.Fatalf("claim on empty queue: %d, want 204", code)
	}
}

func TestFailAndRetryOverHTTP(t *testing.T) {
	srv := newServer(t)
	call(t, srv, "POST", "/v1/jobs", map[string]any{"type": "x", "lane": "l"}, nil)
	var lease queue.Lease
	call(t, srv, "POST", "/v1/claim", map[string]any{"worker": "w"}, &lease)

	var failed queue.Job
	call(t, srv, "POST", "/v1/jobs/1/fail", map[string]any{"token": lease.Token, "error": "token expired", "permanent": true}, &failed)
	if failed.State != queue.Dead || failed.LastError != "token expired" {
		t.Fatalf("fail: %+v", failed)
	}

	var dead []queue.Job
	call(t, srv, "GET", "/v1/jobs?state=dead", nil, &dead)
	if len(dead) != 1 {
		t.Fatalf("list dead: %d jobs", len(dead))
	}
	if code := call(t, srv, "POST", "/v1/jobs/1/retry", nil, nil); code != 200 {
		t.Fatalf("retry: %d", code)
	}
	if code := call(t, srv, "POST", "/v1/jobs/1/cancel", nil, nil); code != 200 {
		t.Fatalf("cancel: %d", code)
	}
	if code := call(t, srv, "POST", "/v1/jobs/1/cancel", nil, nil); code != 409 {
		t.Fatalf("cancel twice: %d, want 409", code)
	}
	if code := call(t, srv, "GET", "/v1/jobs/42", nil, nil); code != 404 {
		t.Fatalf("missing job: %d, want 404", code)
	}
}

func TestReleaseAndLanesOverHTTP(t *testing.T) {
	srv := newServer(t)
	for range 4 {
		call(t, srv, "POST", "/v1/jobs", map[string]any{"type": "post", "lane": "photos", "hold": true}, nil)
	}
	var released []queue.Job
	call(t, srv, "POST", "/v1/lanes/photos/release", map[string]any{"n": 3}, &released)
	if len(released) != 3 {
		t.Fatalf("released %d, want 3", len(released))
	}
	call(t, srv, "POST", "/v1/lanes/photos/pause", nil, nil)

	var lanes []queue.LaneSummary
	call(t, srv, "GET", "/v1/lanes", nil, &lanes)
	if len(lanes) != 1 || !lanes[0].Paused || lanes[0].Counts[queue.Queued] != 3 || lanes[0].Counts[queue.Held] != 1 {
		t.Fatalf("lanes: %+v", lanes)
	}
	if code := call(t, srv, "POST", "/v1/claim", map[string]any{"worker": "w"}, nil); code != 204 {
		t.Fatalf("claim from paused lane: %d, want 204", code)
	}
	if code := call(t, srv, "POST", "/v1/jobs", map[string]any{"lane": "x"}, nil); code != 400 {
		t.Fatalf("enqueue without type: %d, want 400", code)
	}
}

func TestClearLane(t *testing.T) {
	srv := newServer(t)
	for range 3 {
		call(t, srv, "POST", "/v1/jobs", map[string]any{"type": "post", "lane": "photos", "hold": true}, nil)
	}
	call(t, srv, "POST", "/v1/jobs", map[string]any{"type": "post", "lane": "other"}, nil)
	var out struct{ Canceled int }
	if code := call(t, srv, "POST", "/v1/lanes/photos/clear", nil, &out); code != 200 || out.Canceled != 3 {
		t.Fatalf("clear: %d, %+v; want 200, 3 canceled", code, out)
	}
	var left []json.RawMessage
	call(t, srv, "GET", "/v1/jobs?state=queued,held", nil, &left)
	if len(left) != 1 {
		t.Fatalf("%d jobs left waiting; want only the other lane's", len(left))
	}
	if code := call(t, srv, "POST", "/v1/lanes/photos/clear", map[string]any{"states": []string{"running"}}, nil); code != 400 {
		t.Errorf("clearing running jobs: %d; want 400", code)
	}
}

func TestAppsOverHTTP(t *testing.T) {
	srv := newServer(t)
	var hb struct{ Stop bool }
	call(t, srv, "POST", "/v1/apps/posts/heartbeat", map[string]any{"title": "Posts", "pid": 4242, "dashboard": "http://127.0.0.1:9/"}, &hb)
	if hb.Stop {
		t.Fatal("told to stop on first check-in")
	}
	if code := call(t, srv, "POST", "/v1/apps/posts/stop", nil, nil); code != 200 {
		t.Fatalf("stop: %d", code)
	}
	call(t, srv, "POST", "/v1/apps/posts/heartbeat", map[string]any{"pid": 4242}, &hb)
	if !hb.Stop {
		t.Fatal("not told to stop")
	}
	var list []struct{ Name, Status, Title string }
	call(t, srv, "GET", "/v1/apps", nil, &list)
	if len(list) != 1 || list[0].Status != "stopping" {
		t.Fatalf("apps = %+v", list)
	}
	if code := call(t, srv, "POST", "/v1/apps/nope/stop", nil, nil); code != 404 {
		t.Errorf("stopping an unknown app: %d", code)
	}

	// A page on another site can't change anything; reading is fine.
	req, _ := http.NewRequest("POST", srv.URL+"/v1/apps/posts/start", nil)
	req.Header.Set("Origin", "https://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-site POST: %d; want 403", resp.StatusCode)
	}
}
