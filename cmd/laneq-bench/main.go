// Command laneq-bench measures laneq over HTTP: it starts a server on a
// throwaway data directory, then producers enqueue jobs while workers claim and
// complete them, every request durable (fsynced) before it's acknowledged.
//
//	go run ./cmd/laneq-bench -jobs 20000 -producers 8 -workers 8
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaronhhsi/laneq/internal/api"
	"github.com/aaronhhsi/laneq/internal/queue"
)

func main() {
	jobs := flag.Int("jobs", 20000, "jobs to push through")
	producers := flag.Int("producers", 8, "concurrent enqueuers")
	workers := flag.Int("workers", 8, "concurrent workers")
	lanes := flag.Int("lanes", 0, "spread jobs over this many ordered lanes (0 = unordered)")
	profile := flag.String("cpuprofile", "", "write a CPU profile of the run here")
	flag.Parse()
	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	dir, err := os.MkdirTemp("", "laneq-bench-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	q, err := queue.Open(queue.Options{Dir: dir})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go http.Serve(ln, api.Handler(q, api.Options{}))
	base := "http://" + ln.Addr().String()

	tr := &http.Transport{MaxIdleConnsPerHost: *producers + *workers}
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}
	call := func(path string, body any, out any) (int, time.Duration) {
		b, _ := json.Marshal(body)
		start := time.Now()
		resp, err := client.Post(base+path, "application/json", bytes.NewReader(b))
		if err != nil {
			log.Fatal(err)
		}
		if out != nil && resp.StatusCode == 200 {
			json.NewDecoder(resp.Body).Decode(out)
		}
		io.Copy(io.Discard, resp.Body) // drain, or the connection can't be reused
		resp.Body.Close()
		return resp.StatusCode, time.Since(start)
	}

	var (
		mu                     sync.Mutex
		enqLat, claimLat, cLat []time.Duration
		next, done             atomic.Int64
		wg                     sync.WaitGroup
	)
	start := time.Now()
	for range *producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local []time.Duration
			for {
				i := next.Add(1)
				if i > int64(*jobs) {
					break
				}
				body := map[string]any{"type": "bench", "payload": map[string]int64{"i": i}}
				if *lanes > 0 {
					body["lane"] = fmt.Sprint("lane-", i%int64(*lanes))
				}
				_, d := call("/v1/jobs", body, nil)
				local = append(local, d)
			}
			mu.Lock()
			enqLat = append(enqLat, local...)
			mu.Unlock()
		}()
	}
	var enqDone time.Duration
	go func() { wg.Wait(); enqDone = time.Since(start) }()

	var wwg sync.WaitGroup
	for w := range *workers {
		wwg.Add(1)
		go func() {
			defer wwg.Done()
			var cl, co []time.Duration
			for done.Load() < int64(*jobs) {
				var lease struct {
					Job   struct{ ID int64 }
					Token string
				}
				code, d := call("/v1/claim", map[string]any{"worker": fmt.Sprint("w", w), "wait_seconds": 1}, &lease)
				if code != 200 {
					continue
				}
				cl = append(cl, d)
				if code, d := call(fmt.Sprintf("/v1/jobs/%d/complete", lease.Job.ID), map[string]any{"token": lease.Token}, nil); code == 200 {
					co = append(co, d)
					done.Add(1)
				}
			}
			mu.Lock()
			claimLat, cLat = append(claimLat, cl...), append(cLat, co...)
			mu.Unlock()
		}()
	}
	wwg.Wait()
	total := time.Since(start)
	wg.Wait()

	order := "unordered"
	if *lanes > 0 {
		order = fmt.Sprintf("%d ordered lanes", *lanes)
	}
	fmt.Printf("%d jobs, %d producers, %d workers, %s — every write fsynced before it's acknowledged\n\n", *jobs, *producers, *workers, order)
	fmt.Printf("enqueue:     %8.0f jobs/s\n", float64(*jobs)/enqDone.Seconds())
	fmt.Printf("end to end:  %8.0f jobs/s  (enqueue → claim → complete, %s total)\n\n", float64(*jobs)/total.Seconds(), total.Round(time.Millisecond))
	fmt.Printf("%-10s %9s %9s %9s\n", "latency", "p50", "p99", "max")
	for _, row := range []struct {
		name string
		d    []time.Duration
	}{{"enqueue", enqLat}, {"claim", claimLat}, {"complete", cLat}} {
		slices.Sort(row.d)
		fmt.Printf("%-10s %9s %9s %9s\n", row.name, pct(row.d, 50), pct(row.d, 99), row.d[len(row.d)-1].Round(time.Microsecond))
	}
}

func pct(d []time.Duration, p int) time.Duration {
	return d[min(len(d)-1, len(d)*p/100)].Round(time.Microsecond)
}
