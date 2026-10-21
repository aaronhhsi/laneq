// Command laneq runs the job queue server.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aaronhhsi/laneq/internal/api"
	"github.com/aaronhhsi/laneq/internal/queue"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7070", "listen address (localhost only by default: the API has no auth)")
	dir := flag.String("data", "laneq-data", "directory for the write-ahead log and snapshot")
	flag.Parse()

	q, err := queue.Open(queue.Options{Dir: *dir})
	if err != nil {
		log.Fatalf("open queue: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Record lease expiries even when no client is calling in.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := q.ExpireLeases(); err != nil {
					log.Printf("expire leases: %v", err)
				}
			}
		}
	}()

	srv := &http.Server{Addr: *addr, Handler: api.Handler(q), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()

	log.Printf("laneq listening on http://%s (data in %s)", *addr, *dir)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	if err := q.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}
