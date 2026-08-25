# Distributed Job Platform

A distributed background job processing system built from scratch using Go, Redis, and Docker.

The goal of this project is to understand how distributed job queues work internally, including worker coordination, job ownership, leases, failure recovery, retries, and reliable execution semantics.

> **Status:** Core queue, workers, leases, lease renewal, failure recovery, retries, and failed-job persistence are implemented. Job lifecycle tracking and stronger reliability guarantees are next.

## Overview

The platform allows an API to create jobs which are placed into a Redis-backed queue. Multiple worker processes can independently consume and execute jobs.

Workers claim jobs using a lease mechanism. A heartbeat periodically renews the lease while a worker is healthy. If a worker crashes, a separate Reaper process detects the expired lease and puts the job back into the queue so another worker can process it.

```text
                    ┌──────────────┐
                    │     API      │
                    │   Go HTTP    │
                    └──────┬───────┘
                           │
                           │ enqueue
                           ▼
                    ┌──────────────┐
                    │    Redis     │
                    │              │
                    │ jobs         │
                    │ active_jobs  │
                    │ job_leases   │
                    │ failed_jobs  │
                    └──────┬───────┘
                           │
                    BRPOP  │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌─────────┐  ┌─────────┐  ┌─────────┐
         │ Worker  │  │ Worker  │  │ Worker  │
         │    1    │  │    2    │  │    3    │
         └─────────┘  └─────────┘  └─────────┘
              │            │            │
              └────────────┼────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │    Reaper    │
                    └──────────────┘
```

## Current Features

### Job Creation

The API creates jobs with:

- Unique job ID
- Job type
- Job status
- Duration
- Retry count

Example:

```json
{
  "id": "1787669074517250300",
  "type": "sleep",
  "status": "queued",
  "duration": 60,
  "retries": 0
}
```

### Redis-backed Queue

Jobs are serialized as JSON and stored in a Redis list.

```text
jobs
┌──────────────────────────────┐
│ Job C │ Job B │ Job A │ ... │
└──────────────────────────────┘
```

Workers use Redis `BRPOP` to wait for jobs instead of repeatedly polling an empty queue.

### Multiple Workers

Multiple independent worker processes can consume from the same Redis queue.

Redis atomically removes jobs from the queue when they are popped, allowing different workers to receive different jobs.

### Job Execution

Jobs currently support a basic `sleep` job.

For example, a duration of `60` causes the worker to sleep for 60 seconds.

Job execution is separated from queue management so additional job types can be added later.

### Job Retries

Failed jobs are retried up to the configured retry limit.

The current configuration allows three retries after the initial attempt.

```text
attempt 1 → failure → retry 1
attempt 2 → failure → retry 2
attempt 3 → failure → retry 3
attempt 4 → failure → permanently failed
```

Permanently failed jobs are stored in the `failed_jobs` Redis Hash using the job ID as the field.

## Job Claims and Leases

When a worker receives a job, it records a claim containing:

```text
Job
Worker ID
```

The claim is stored in the Redis hash `active_jobs`.

The worker also creates a lease for the job in the Redis sorted set `job_leases`.

The current lease duration is 30 seconds.

```text
member = Job ID
score  = lease expiration timestamp
```

This allows the Reaper to efficiently find expired jobs.

### Lease Renewal

Workers renew their lease every 15 seconds while executing a job.

Each renewal pushes the expiration 30 seconds into the future.

```text
Initial claim
     ↓
30 second lease
     ↓
15 seconds
     ↓
renew → +30 seconds
     ↓
15 seconds
     ↓
renew → +30 seconds
     ↓
...
```

If the worker dies, the heartbeat stops and the lease eventually expires.

## Successful Job Completion

When a worker successfully finishes a job, its current claim and lease are removed:

```text
claim
  ↓
execute
  ↓
success
  ↓
HDEL active_jobs
  ↓
ZREM job_leases
```

Completed-job persistence and atomic state transitions are planned next so that successful jobs remain queryable without introducing inconsistent intermediate states.

## Failure Recovery

A separate process called the **Reaper** periodically checks for expired leases.

```text
Worker claims Job A
        ↓
Worker crashes
        ↓
Lease expires
        ↓
Reaper detects Job A
        ↓
Retrieve JobClaim
        ↓
Requeue Job A
        ↓
Remove old claim
        ↓
Remove old lease
        ↓
Another worker processes Job A
```

This recovery mechanism has been tested by:

1. Starting a worker.
2. Submitting a long-running job.
3. Killing the worker while the job was executing.
4. Waiting for the lease to expire.
5. Allowing the Reaper to detect the expired job.
6. Starting another worker.
7. Verifying that the recovered job was processed again.

Lease renewal has also been tested with long-running jobs to verify that healthy workers do not get incorrectly reaped.

## Redis Data Model

### `jobs`

A Redis List containing jobs waiting to be processed.

Jobs are stored as JSON strings.

### `active_jobs`

A Redis Hash containing currently claimed jobs.

```text
JobID → JobClaim JSON
```

### `job_leases`

A Redis Sorted Set used as an expiration index.

```text
JobID → lease expiration timestamp
```

The score is the lease expiration time in Unix milliseconds. The Reaper queries for members whose score is less than or equal to the current time.

### `failed_jobs`

A Redis Hash containing jobs that have exhausted their retry attempts.

```text
JobID → Failed Job JSON
```

Using a hash allows a failed job to be looked up directly by its ID.

## Project Structure

```text
distributed-job-platform/
│
├── cmd/
│   ├── api/
│   │   └── main.go
│   ├── worker/
│   │   └── main.go
│   └── reaper/
│       └── main.go
│
├── internal/
│   └── job/
│       └── job.go
│
├── go.mod
└── README.md
```

The shared `job` package contains the domain structures used by the API, workers, and Reaper.

## Technologies

- **Go** — API, workers, and Reaper
- **Redis** — job queue and distributed state
- **Docker** — local Redis environment

## What I Learned

### Redis

- Redis Lists
- Redis Hashes
- Redis Sorted Sets
- `BRPOP`
- `HSET` / `HGET` / `HDEL`
- `ZADD` / `ZRANGEBYSCORE` / `ZREM`
- Blocking vs polling queue consumption
- Using sorted-set scores as lease expiration timestamps

### Go

- HTTP handlers
- JSON serialization and deserialization
- Shared packages
- `context.Context`
- Redis client usage
- Multiple processes
- Goroutines
- Channels
- Tickers
- `select`
- Struct composition
- Error handling

### Distributed Systems

A basic queue has a serious failure mode:

```text
Worker receives Job A
        ↓
Job removed from queue
        ↓
Worker crashes
        ↓
Job is lost
```

Adding leases allows the system to detect abandoned work:

```text
Worker receives Job A
        ↓
Job gets a lease
        ↓
Worker crashes
        ↓
Lease expires
        ↓
Reaper requeues Job A
```

Lease renewal prevents healthy workers processing long-running jobs from being incorrectly requeued.

The retry mechanism also introduces an important distributed-systems property: execution is moving toward **at-least-once semantics**, meaning a job may execute more than once in some failure scenarios.

## Known Limitations

### At-Least-Once Execution

The system does not guarantee exactly-once execution.

A worker can successfully perform a job's external side effect and then fail before the system records completion.

```text
Worker executes job
        ↓
External side effect succeeds
        ↓
Worker crashes
        ↓
System cannot confirm completion
        ↓
Job may be retried
        ↓
Side effect may happen again
```

This is one of the next reliability problems to address.

### Non-Atomic State Transitions

Job completion currently involves multiple Redis operations:

```text
HDEL active_jobs
ZREM job_leases
```

If a worker crashes between operations, Redis can temporarily contain inconsistent state.

The next iteration will make job state transitions more robust and determine how completed jobs should be persisted and queried.

## Roadmap

### Reliability

- [x] Lease renewal / worker heartbeats
- [x] Worker crash recovery
- [x] Retry failed jobs
- [x] Retry limits
- [x] Failed-job persistence
- [ ] Handle lease ownership races
- [ ] Idempotency
- [ ] Atomic job state transitions
- [ ] Better failure handling
- [ ] Graceful worker shutdown

### Job Management

- [ ] Completed-job persistence
- [ ] Job status endpoint
- [ ] Job lookup by ID
- [ ] Job cancellation
- [ ] Job priorities
- [ ] Scheduled jobs
- [ ] Retry backoff

### Observability & Infrastructure

- [ ] Metrics
- [ ] Structured logging
- [ ] Health checks
- [ ] Load testing
- [ ] Failure/chaos testing
- [ ] Better Redis error handling

### Deployment

- [ ] Docker Compose
- [ ] Containerize API
- [ ] Containerize workers
- [ ] Containerize Reaper
- [ ] Production deployment

## Current Milestone

The system can currently:

```text
Create a job
     ↓
Persist it in Redis
     ↓
Distribute it across multiple workers
     ↓
Claim ownership
     ↓
Assign a lease
     ↓
Renew the lease while executing
     ↓
Execute the job
     ↓
Clean up completed attempts
     ↓
Detect crashed workers
     ↓
Requeue abandoned jobs
     ↓
Retry failed jobs
     ↓
Persist permanently failed jobs
```

The next major milestone is **robust job lifecycle management**, including completed-job persistence, status lookup, and safer state transitions.
