# laneq

[![jobs processed](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fbpplyuckjuiuuyazcvzi.supabase.co%2Ffunctions%2Fv1%2Fstats&query=%24.badges.jobs&label=jobs%20processed&color=3d5afe&cacheSeconds=3600)](#usage-stats)
[![laneq data](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fbpplyuckjuiuuyazcvzi.supabase.co%2Ffunctions%2Fv1%2Fstats&query=%24.badges.laneq_data&label=data%20managed%20by%20laneq&color=3d5afe&cacheSeconds=3600)](#usage-stats)
[![installs](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fbpplyuckjuiuuyazcvzi.supabase.co%2Ffunctions%2Fv1%2Fstats&query=%24.laneq.installs&label=installs&color=3d5afe&cacheSeconds=3600)](#usage-stats)
[![posts published with lanecast](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fbpplyuckjuiuuyazcvzi.supabase.co%2Ffunctions%2Fv1%2Fstats&query=%24.badges.posts&label=posts%20published%20with%20lanecast&color=1b7f4b&cacheSeconds=3600)](https://github.com/aaronhhsi/lanecast)
[![media managed by lanecast](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fbpplyuckjuiuuyazcvzi.supabase.co%2Ffunctions%2Fv1%2Fstats&query=%24.badges.media&label=media%20managed%20by%20lanecast&color=1b7f4b&cacheSeconds=3600)](https://github.com/aaronhhsi/lanecast)

Live totals across every laneq and [lanecast](https://github.com/aaronhhsi/lanecast) install, reported once a
day ([how](#usage-stats)).

A durable job queue in Go with ordered lanes, leases and retries, served over
HTTP/JSON so workers can be written in any language. Standard library only.

It runs an automated publishing pipeline in production: [lanecast](https://github.com/aaronhhsi/lanecast),
a social media scheduler, posts thousands of queued photos and videos to six Instagram accounts on a nightly
schedule, where a duplicate or out-of-order post would be publicly visible.

```mermaid
flowchart LR
    P[Producers] -- "POST /v1/jobs" --> API
    W["Workers (any language)"] -- "claim · heartbeat · complete · fail" --> API
    D[Dashboard] --> API
    subgraph laneq
        API[HTTP API] --> Q["Queue<br/>lanes · leases · retries · schedules"]
        Q -- "append" --> B[(WAL buffer)]
        B -- "group commit:<br/>one write + fsync" --> L[(wal.log)]
        Q -- "compaction" --> S[(snapshot.json)]
    end
```

## What it guarantees

- **Nothing acknowledged is lost.** Every change is appended to a checksummed
  write-ahead log and fsynced before the client hears back. On restart the
  queue replays the latest snapshot plus the log; a torn write from a crash
  fails its CRC32 and is discarded. A test kills the real server mid-traffic
  (no shutdown hooks) five times and checks every acknowledged enqueue and
  completion survived — ~20,000 per run.
- **At-least-once delivery.** A worker leases a job and heartbeats to keep it.
  If it dies, the lease expires and the job runs again; if the stale worker
  comes back, its result is rejected, because each lease carries a fencing
  token. Workers can save a checkpoint with each heartbeat so a retry resumes
  where the last attempt stopped.
- **Ordered lanes.** Jobs that share a lane run one at a time, in order. A job
  that runs out of attempts blocks its lane (it's never skipped), so later jobs
  can't overtake it. Jobs without a lane run in parallel.

## In use

**Running on my laptop** (local links, so they only open there):

One laneq, shared by every app: **[127.0.0.1:7070](http://127.0.0.1:7070)** (its dashboard: each app with
Stop / Kill, every lane and job, and a Power off for the whole thing).

| App | Its dashboard | Its lanes |
|---|---|---|
| illitlauver: Instagram fan accounts, posted nightly | [127.0.0.1:7080](http://127.0.0.1:7080) | `illitlauver/*` |
| Real Volleyball App: men's volleyball box scores every 6 hours | [real-volleyball-app.vercel.app](https://real-volleyball-app.vercel.app) | `real-volleyball/*` |

## Features

- Retries with exponential backoff and jitter; `retry_after` for rate limits;
  `permanent` failures go straight to dead; retry/cancel/reorder for operators
- Idempotency keys: enqueueing the same key twice returns the original job
- Delayed jobs (`run_at`), held jobs released in batches, and daily schedules
  per lane ("release 3 at 00:00") that catch up on runs missed while the
  machine was asleep — without ever releasing the same day twice
- Pausing a lane is a full stop: the running job finishes, released jobs go back
  in line, and paused days are skipped rather than made up
- One laneq for many apps: lanes are named `<app>/<lane>`, and each app's worker checks in
  (`/v1/apps/{name}/heartbeat`), so the dashboard can stop it (after its current job) or kill it, and a
  power-off stops every app before laneq itself
- Long-polling claims, and a built-in dashboard for watching the queue: every app, lane and job, with retry,
  cancel, clear, rename, pause and schedule on/off
- Optional worker supervision: `laneq -- python worker.py` restarts the worker
  if it crashes, and (on Windows, via a job object) never leaves it orphaned

## Performance

`go run ./cmd/laneq-bench` — over HTTP on localhost, every write fsynced before
it's acknowledged (Intel Core Ultra 9 185H, NVMe SSD, Windows 11):

| | end to end (enqueue → claim → complete) | enqueue only | p50 / p99 claim |
|---|---|---|---|
| 32 producers, 32 workers, unordered | **6,834 jobs/s** | 13,784 jobs/s | 1.0 ms / 8.0 ms |
| 16 producers, 16 workers, 100 lanes | **5,633 jobs/s** | 12,217 jobs/s | 1.0 ms / 4.3 ms |

How it got there, each step found with a profile:

| Change | End to end |
|---|---|
| Baseline: fsync per write, under the queue lock | 1,011 jobs/s |
| Group commit: one fsync covers every writer waiting on it | 1,420 jobs/s |
| Index running leases and ready jobs (heap) instead of scanning every job per request | 2,268 jobs/s |
| Buffer appends in memory; the commit leader does the file write and fsync outside the lock (on Windows, `WriteFile` blocks behind an in-flight `FlushFileBuffers`) | 6,834 jobs/s |

## Design notes

- **Group commit.** Operations append to an in-memory buffer while holding the
  queue lock, so log order is apply order. After releasing the lock, each one
  waits until a flush covers its offset: the first to find no flush in flight
  becomes the leader, writes the whole buffer and fsyncs, and wakes everyone it
  covered. Reads wait the same way, so a client never sees state that isn't
  durable yet.
- **Compaction.** After 5,000 log records the queue writes a snapshot (temp
  file, fsync, atomic rename) and truncates the log. Truncation waits for any
  in-flight flush; otherwise records flushed after it would land after the
  snapshot and roll state back on replay. Log records hold full object state,
  so replaying one twice is harmless.
- **Failure is sticky.** After a failed write or fsync the log can't be trusted
  (a torn line in the middle would hide everything after it from replay), so
  the queue refuses further writes rather than continue on a broken log.

## Running it

```sh
go run ./cmd/laneq -data ./laneq-data          # → http://127.0.0.1:7070 (dashboard + API)
go run ./cmd/laneq -data ./laneq-data -- python worker.py   # with a supervised worker
go test ./...                                   # includes the kill-and-restart test
go run ./cmd/laneq-bench -jobs 50000 -producers 32 -workers 32
```

The API listens on localhost by default and has no authentication.

Why it is built this way, decision by decision: [DESIGN.md](DESIGN.md).

## HTTP API

| | |
|---|---|
| `POST /v1/jobs` | enqueue `{type, lane, key, payload, run_at, hold, max_attempts}` |
| `GET /v1/jobs?lane=&state=&type=&newest=&offset=&limit=` | list (total in `X-Total-Count`) |
| `GET /v1/jobs/{id}` | one job |
| `POST /v1/claim` | lease a job `{worker, types, lease_seconds, wait_seconds}` → `{job, token}` or 204 |
| `POST /v1/jobs/{id}/heartbeat` | extend the lease, save a checkpoint `{token, checkpoint, lease_seconds}` |
| `POST /v1/jobs/{id}/complete` | `{token, result}` |
| `POST /v1/jobs/{id}/fail` | `{token, error, retry_after_seconds, permanent}` |
| `POST /v1/jobs/{id}/retry` · `/cancel` · `/move` | operator actions (`move`: `{delta: -1 or 1}`) |
| `GET /v1/stats` | totals since the queue was created: jobs (and by state), attempts, lanes, schedules, apps, bytes on disk |
| `GET /v1/lanes` · `POST /v1/lanes/{lane}/pause` · `/resume` · `/release` · `/clear` · `/rename` | lanes (`release`: `{n, run_at}`; `clear` cancels the lane's waiting jobs: `{states}`, default queued, held, dead; `rename`: `{to}`, moves its jobs, pause and schedule) |
| `GET /v1/apps` · `POST /v1/apps/{name}/heartbeat` · `/bye` · `/stop` · `/start` · `/kill` | apps: a worker checks in `{title, dashboard, start_hint, pid}` → `{stop}`; stop (after its job, sticky until start) and kill are accepted from this machine only |
| `GET /v1/schedules` · `PUT /v1/schedules/{lane}` · `POST /v1/schedules/{lane}/override` | daily releases |
| `POST /v1/admin/shutdown` | clean shutdown, from this machine only: stops the supervised worker, closes the port, writes a final snapshot, exits 0 |
| `POST /v1/admin/poweroff` | the kill switch, from this machine only: stops every app (each finishes its current job; any still running after 2 minutes is ended), then shuts down |

A minimal worker:

```python
while True:
    lease = post("/v1/claim", {"worker": "w1", "types": ["resize"], "wait_seconds": 30})
    if not lease:
        continue
    try:
        result = handle(lease["job"]["payload"])
        post(f"/v1/jobs/{lease['job']['id']}/complete", {"token": lease["token"], "result": result})
    except Exception as e:
        post(f"/v1/jobs/{lease['job']['id']}/fail", {"token": lease["token"], "error": str(e)})
```

## Usage stats

To show how much laneq is used, each install sends a few totals once a day to a small
[Supabase function](usage-stats/): a random install id (made on first run, kept in the data
directory), the version, and counts of jobs, succeeded jobs, attempts, lanes, schedules and apps, plus
the size of the data on disk. Never lane names, job payloads or anything else from the queue. laneq
says so when it starts, and `-usage-stats=false` (or `LANEQ_USAGE_STATS=off`) turns it off.

The function accepts only that shape, drops numbers out of range, keeps one report per install per day,
and limits how many installs one address can report. The table isn't readable with the public key; the
totals it serves are what the badges above show: `curl https://bpplyuckjuiuuyazcvzi.supabase.co/functions/v1/stats`.
