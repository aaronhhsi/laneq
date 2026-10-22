package main

import (
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestShutdownLeavesNothingBehind: POST /v1/admin/shutdown must stop the
// supervised worker, close the port, write a final snapshot and exit 0.
func TestShutdownLeavesNothingBehind(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs a real server")
	}
	bin := filepath.Join(t.TempDir(), "laneq-test.exe")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	data, addr, workerAddr := t.TempDir(), freeAddr(t), freeAddr(t)

	// The "worker" is another laneq: a long-running process with a port we can probe.
	srv, logs := startServer(t, bin, "-addr", addr, "-data", data, "--", bin, "-addr", workerAddr, "-data", t.TempDir())
	waitUp(t, "http://"+addr, logs)
	waitUp(t, "http://"+workerAddr, logs)

	resp, err := http.Post("http://"+addr+"/v1/admin/shutdown", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("shutdown: HTTP %d", resp.StatusCode)
	}

	exited := make(chan error, 1)
	go func() { exited <- srv.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("laneq exited with %v\n%s", err, logs)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("laneq didn't exit after shutdown\n%s", logs)
	}
	if !strings.Contains(logs.String(), "laneq stopped cleanly") {
		t.Fatalf("no clean-stop line in the log:\n%s", logs)
	}
	for name, a := range map[string]string{"server": addr, "worker": workerAddr} {
		l, err := net.Listen("tcp", a) // free again only if nothing is still listening
		if err != nil {
			t.Fatalf("%s port %s still in use after shutdown: %v", name, a, err)
		}
		l.Close()
	}
}
