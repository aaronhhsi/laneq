// Command laneq runs the job queue server and, optionally, a worker it supervises.
//
//	laneq [flags] [-- worker command...]
//
// Anything after "--" is run as a worker process: started with the server,
// restarted with backoff if it exits, and stopped when the server stops.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aaronhhsi/laneq/internal/api"
	"github.com/aaronhhsi/laneq/internal/apps"
	"github.com/aaronhhsi/laneq/internal/queue"
	"github.com/aaronhhsi/laneq/internal/usage"
)

// version is set at build time (-ldflags "-X main.version=v1.2.0").
var version = "dev"

// statsURL receives the daily usage totals (see internal/usage and the README).
const statsURL = "https://bpplyuckjuiuuyazcvzi.supabase.co/functions/v1/stats"

// PowerOffWait is how long power off lets apps finish their current jobs.
const PowerOffWait = 2 * time.Minute

type mounts map[string]string

func (m mounts) String() string { return fmt.Sprint(map[string]string(m)) }
func (m mounts) Set(v string) error {
	name, dir, ok := strings.Cut(v, "=")
	if !ok || name == "" || dir == "" {
		return errors.New("want name=dir")
	}
	m[name] = dir
	return nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7070", "listen address (localhost only by default: the API has no auth)")
	dir := flag.String("data", "laneq-data", "directory for the write-ahead log and snapshot")
	logPath := flag.String("log", "", "append logs to this file instead of stderr")
	files := mounts{}
	flag.Var(files, "files", "serve a directory read-only at /files/<name>/ (name=dir; repeatable)")
	usageStats := flag.Bool("usage-stats", true, "send a few totals (jobs, lanes, apps, data size) once a day; see the README")
	flag.Parse()

	var out io.Writer = os.Stderr
	if *logPath != "" {
		f, err := os.OpenFile(*logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatalf("open log: %v", err)
		}
		defer f.Close()
		out = f
	}
	log.SetOutput(out)

	q, err := queue.Open(queue.Options{Dir: *dir})
	if err != nil {
		log.Fatalf("open queue: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := apps.New()
	mux := http.NewServeMux()
	mux.Handle("/", api.Handler(q, api.Options{Files: files, Apps: registry}))
	// Only from this machine, and not from a page on another site.
	admin := func(r *http.Request) bool {
		o := r.Header.Get("Origin")
		return api.FromThisMachine(r) && (o == "" || o == "http://"+r.Host)
	}
	// A clean shutdown for a laneq running without a console (no Ctrl+C to send):
	// stop the worker, stop serving, write a final snapshot, exit 0.
	mux.HandleFunc("POST /v1/admin/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if !admin(r) {
			http.Error(w, "shutdown is only accepted from this machine", http.StatusForbidden)
			return
		}
		log.Printf("shutdown requested")
		w.WriteHeader(http.StatusAccepted)
		stop()
	})
	// Power off: stop every app (each finishes the job it's on), end any still
	// running after PowerOffWait, then shut down. The kill switch for the whole thing.
	mux.HandleFunc("POST /v1/admin/poweroff", func(w http.ResponseWriter, r *http.Request) {
		if !admin(r) {
			http.Error(w, "power off is only accepted from this machine", http.StatusForbidden)
			return
		}
		log.Printf("power off requested: stopping %v", registry.Running())
		registry.StopAll()
		w.WriteHeader(http.StatusAccepted)
		go func() {
			deadline := time.Now().Add(PowerOffWait)
			for len(registry.Running()) > 0 && time.Now().Before(deadline) {
				time.Sleep(time.Second)
			}
			for _, name := range registry.Running() {
				log.Printf("power off: %s didn't stop in %s; ending it", name, PowerOffWait)
				if err := registry.KillApp(name); err != nil {
					log.Printf("power off: %s: %v", name, err)
				}
			}
			stop()
		}()
	})
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	ln, err := listen(*addr)
	if err != nil {
		// Most likely another laneq is already running; one per data dir is enough.
		log.Fatalf("listen: %v", err)
	}

	go every(ctx, time.Second, func() {
		if err := q.ExpireLeases(); err != nil {
			log.Printf("expire leases: %v", err)
		}
	})
	// Ticks keep coming after the machine wakes from sleep, so a run that was due
	// while it slept happens within seconds of waking.
	go every(ctx, 10*time.Second, func() {
		released, err := q.RunSchedules()
		if err != nil {
			log.Printf("schedules: %v", err)
		}
		for _, j := range released {
			log.Printf("released #%d %s (%s) to run at %s", j.ID, j.Key, j.Lane, j.RunAt.Format(time.TimeOnly))
		}
	})

	done := make(chan struct{})
	if cmd := flag.Args(); len(cmd) > 0 {
		go func() { supervise(ctx, cmd, out); close(done) }()
	} else {
		close(done)
	}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()

	if *usageStats && os.Getenv("LANEQ_USAGE_STATS") != "off" && statsURL != "" {
		if id, err := usage.InstallID(*dir); err != nil {
			log.Printf("usage stats off: %v", err)
		} else {
			log.Printf("usage stats: sending daily totals (jobs, lanes, apps, data size; no names or payloads) to %s; turn off with -usage-stats=false", statsURL)
			go usage.Run(ctx, statsURL, usage.Report{InstallID: id, Kind: "laneq", Version: version}, func() map[string]any {
				s := q.Stats()
				return map[string]any{"jobs": s.Jobs, "succeeded": s.ByState[queue.Succeeded], "attempts": s.Attempts,
					"lanes": s.Lanes, "schedules": s.Schedules, "apps": len(registry.List()), "data_bytes": s.DataBytes}
			}, log.Printf)
		}
	}
	log.Printf("laneq %s listening on http://%s (data in %s)", version, *addr, *dir)
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-done // the worker has exited
	if err := q.Close(); err != nil {
		log.Printf("close: %v", err)
		os.Exit(1)
	}
	log.Printf("laneq stopped cleanly")
}

func every(ctx context.Context, d time.Duration, fn func()) {
	fn()
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}
