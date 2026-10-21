package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCrashLosesNothingAcknowledged runs the real binary, kills it (SIGKILL /
// TerminateProcess, no shutdown hooks) while producers and workers are hammering
// it, restarts it, and checks that every enqueue and completion the server
// acknowledged before the kill is still there.
func TestCrashLosesNothingAcknowledged(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and kills a real server")
	}
	bin := filepath.Join(t.TempDir(), "laneq-test.exe")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	data := t.TempDir()

	var mu sync.Mutex
	enqueued := map[int64]bool{}  // acknowledged enqueues
	completed := map[int64]bool{} // acknowledged completions

	const rounds = 5
	for round := range rounds {
		addr := freeAddr(t)
		srv := exec.Command(bin, "-addr", addr, "-data", data)
		var stderr bytes.Buffer
		srv.Stderr = &stderr
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
		base := "http://" + addr
		waitUp(t, base, &stderr)

		var stop atomic.Bool
		var wg sync.WaitGroup
		for p := range 4 { // producers
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; !stop.Load(); i++ {
					var j struct{ ID int64 }
					body := map[string]any{"type": "x", "lane": fmt.Sprintf("l%d", p%2), "key": fmt.Sprintf("r%d-p%d-%d", round, p, i)}
					if post(base+"/v1/jobs", body, &j) == http.StatusCreated {
						mu.Lock()
						enqueued[j.ID] = true
						mu.Unlock()
					}
				}
			}()
		}
		for w := range 4 { // workers
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stop.Load() {
					var l struct {
						Job   struct{ ID int64 }
						Token string
					}
					if post(base+"/v1/claim", map[string]any{"worker": fmt.Sprint("w", w), "lease_seconds": 1}, &l) != 200 {
						continue
					}
					if post(fmt.Sprintf("%s/v1/jobs/%d/complete", base, l.Job.ID), map[string]any{"token": l.Token}, nil) == 200 {
						mu.Lock()
						completed[l.Job.ID] = true
						mu.Unlock()
					}
				}
			}()
		}

		time.Sleep(time.Duration(300+round*150) * time.Millisecond)
		srv.Process.Kill() // mid-traffic, no clean shutdown
		srv.Wait()
		stop.Store(true)
		wg.Wait()
	}

	// Restart once more and audit everything acknowledged across all rounds.
	addr := freeAddr(t)
	srv := exec.Command(bin, "-addr", addr, "-data", data)
	var stderr bytes.Buffer
	srv.Stderr = &stderr
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { srv.Process.Kill(); srv.Wait() }()
	base := "http://" + addr
	waitUp(t, base, &stderr)

	var jobs []struct {
		ID    int64
		State string
	}
	resp, err := http.Get(base + "/v1/jobs")
	if err != nil {
		t.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&jobs)
	resp.Body.Close()
	state := map[int64]string{}
	for _, j := range jobs {
		state[j.ID] = j.State
	}
	for id := range enqueued {
		if _, ok := state[id]; !ok {
			t.Errorf("acknowledged job %d lost", id)
		}
	}
	for id := range completed {
		if state[id] != "succeeded" {
			t.Errorf("acknowledged completion of job %d lost (state %q)", id, state[id])
		}
	}
	if len(enqueued) < 50 || len(completed) < 10 {
		t.Fatalf("too little traffic to mean anything: %d enqueued, %d completed", len(enqueued), len(completed))
	}
	t.Logf("%d kills; %d acknowledged enqueues and %d acknowledged completions all survived", rounds, len(enqueued), len(completed))
}

// Enough idle connections for every goroutine, so requests reuse connections
// instead of exhausting Windows' ephemeral ports.
var client = &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 16}}

func post(url string, body any, out any) int {
	b, _ := json.Marshal(body)
	resp, err := client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return 0 // the server was killed mid-request: not acknowledged
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 && resp.StatusCode != http.StatusNoContent {
		if json.NewDecoder(resp.Body).Decode(out) != nil {
			return 0 // cut off mid-response: not acknowledged either
		}
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return 0
	}
	return resp.StatusCode
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitUp(t *testing.T, base string, stderr *bytes.Buffer) {
	for range 200 {
		if resp, err := client.Get(base + "/healthz"); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server didn't start:\n%s", stderr)
}
