# laneq

A durable job queue in Go with ordered lanes, leases and retries, served over HTTP/JSON so workers can be written in any language.

- **Ordered lanes** — jobs that share a lane run one at a time, in order; a job that keeps failing blocks its lane instead of letting later jobs jump ahead. Jobs without a lane run in parallel.
- **Durable** — every change is appended to a checksummed write-ahead log and fsynced before it is acknowledged; torn writes from a crash are detected and discarded on restart. The log is compacted into atomic snapshots.
- **At-least-once delivery** — workers lease jobs and heartbeat to keep them. A worker that dies loses its lease and the job is retried; a stale worker's late result is rejected (fencing tokens). Workers can save a checkpoint so a retry resumes where the last attempt stopped.
- **Retries** — exponential backoff with jitter, worker-supplied `retry_after` for rate limits, a dead state after the last attempt, and retry/cancel for operators.
- **Scheduling** — delayed jobs (`run_at`), and held jobs released in batches.
- **Idempotent enqueue** — an idempotency key returns the existing job instead of a duplicate.

No dependencies outside the Go standard library.

```sh
go run ./cmd/laneq -data ./laneq-data   # → http://127.0.0.1:7070
go test ./...
```

See [internal/api/api.go](internal/api/api.go) for the HTTP API.
