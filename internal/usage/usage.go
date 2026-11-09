// Package usage sends a few totals once a day, so the project can show how much
// it's used. It's on by default, said so at startup and in the README, and off
// with -usage-stats=false or LANEQ_USAGE_STATS=off.
//
// A report is {install_id, kind, version, totals}: a random id made on first run
// (kept in the data directory), and numbers only. Never lane names, job payloads
// or anything else from the queue.
package usage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Report is what one install sends.
type Report struct {
	InstallID string         `json:"install_id"`
	Kind      string         `json:"kind"`
	Version   string         `json:"version"`
	Totals    map[string]any `json:"totals"`
}

// InstallID is this install's random id, made on first use and kept in dir.
func InstallID(dir string) (string, error) {
	path := filepath.Join(dir, "install-id")
	if b, err := os.ReadFile(path); err == nil && len(strings.TrimSpace(string(b))) == 32 {
		return strings.TrimSpace(string(b)), nil
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b)
	return id, os.WriteFile(path, []byte(id+"\n"), 0o644)
}

// Send posts one report.
func Send(ctx context.Context, url string, r Report) error {
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("usage stats: %s", resp.Status)
	}
	return nil
}

// Run sends a report a minute after start, then once a day, until ctx ends.
// totals is called each time; failures are logged once and otherwise ignored.
func Run(ctx context.Context, url string, base Report, totals func() map[string]any, logf func(string, ...any)) {
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	failed := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		r := base
		r.Totals = totals()
		if err := Send(ctx, url, r); err != nil && !failed {
			logf("usage stats not sent (%v); trying again tomorrow", err)
			failed = true
		} else if err == nil {
			failed = false
		}
		timer.Reset(24 * time.Hour)
	}
}
