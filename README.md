# Distributed Job Platform

A distributed background job processing system built from scratch using Go, Redis, and Docker.

The goal of this project is to understand how distributed job queues work internally, including worker coordination, job ownership, leases, failure recovery, retries, graceful shutdown, and reliable execution semantics.

> **Status:** Core queue, workers, job ownership, leases, lease renewal, failure recovery, retries, persistent job state, job lookup, and graceful worker shutdown are implemented. Distributed lease ownership and stronger execution guarantees are next.

## Overview

The platform allows an API to create jobs which are stored in Redis and placed into a Redis-backed queue. Multiple independent worker processes can consume and execute jobs.

Workers claim jobs using a lease mechanism. A separate Reaper process detects expired leases and requeues abandoned jobs.

The system maintains a canonical job record in `job_data`, while Redis structures such as `jobs`, `active_jobs`, and `job_leases` represent the current processing state.

```text
                    ┌──────────────┐
                    │     API      │
                    │   Go HTTP    │
                    └──────┬───────┘
                           │
                           │ create job
                           ▼
                    ┌──────────────┐
                    │    Redis     │
                    │              │
                    │ job_data     │
                    │ jobs         │
                    │ active_jobs  │
                    │ job_leases   │
                    │ failed_jobs  │
                    │ completed... │
                    └──────┬───────┘
                           │
                     BRPOP │
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
  "id": "1787753848209597500",
  "type": "sleep",
  "status": "queued",
  "duration": 60,
  "retries": 0
}
```

### Redis-backed Queue

The queue stores **job IDs**, rather than entire job objects.

```text
jobs

┌──────────────────────────────┐
│ JobID C │ JobID B │ JobID A  │
└──────────────────────────────┘
```

Workers use Redis `BRPOP` to wait for jobs instead of repeatedly polling an empty queue.

After receiving a job ID, the worker retrieves the canonical job record from `job_data`.

### Multiple Workers

Multiple independent worker processes can consume from the same Redis queue.

```text
              Redis
                │
        ┌───────┼───────┐
        ▼       ▼       ▼
      Worker  Worker  Worker
        1       2       3
```

Redis atomically removes jobs from the queue when they are popped, allowing different workers to receive different jobs.

### Job Execution

Jobs currently support a basic `sleep` job.

For example, a duration of `60` causes the worker to sleep for 60 seconds.

Job execution is separated from queue management so additional job types can be added later.

### Job Status Tracking

Each job has a canonical record stored in the Redis hash:

```text
job_data

JobID → Job JSON
```

The status changes throughout the lifecycle:

```text
queued
   ↓
running
   ↓
completed
```

or:

```text
queued
   ↓
running
   ↓
failed
```

Failed jobs may pass through multiple retry cycles before becoming permanently failed.

### Job Lookup

Jobs can be queried by ID through the API:

```text
GET /jobs/{id}
```

The endpoint retrieves the canonical job record from `job_data`.

This means the same lookup works regardless of whether the job is currently:

- Queued
- Running
- Completed
- Failed

Example:

```json
{
  "id": "1787753848209597500",
  "type": "sleep",
  "status": "completed",
  "duration": 60,
  "retries": 0
}
```

## Job Claims and Leases

When a worker receives a job, it records ownership in:

```text
active_jobs
```

The current representation is:

```text
JobID → WorkerID
```

The worker also creates a lease for the job in the Redis sorted set:

```text
job_leases
```

The current lease duration is **30 seconds**.

```text
member = Job ID
score  = lease expiration timestamp
```

This allows the Reaper to efficiently find expired jobs.

### Lease Renewal

Workers renew their lease every **15 seconds** while executing a job.

Each renewal pushes the expiration another **30 seconds** into the future.

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

If the worker dies, lease renewal stops and the lease eventually expires.

Lease renewal has been tested with long-running jobs to verify that healthy workers do not get incorrectly reaped.

## Successful Job Completion

When a worker successfully finishes a job:

```text
claim
  ↓
execute
  ↓
success
  ↓
remove active ownership
  ↓
remove lease
  ↓
status = completed
  ↓
persist updated job
```

The completed job remains available through `job_data` and can be retrieved through the job lookup API.

## Failure and Retries

If execution fails, the worker checks the retry count.

The current configuration allows three retries after the initial attempt.

```text
attempt 1 → failure → retry 1

attempt 2 → failure → retry 2

attempt 3 → failure → retry 3

attempt 4 → failure → permanently failed
```

Before retrying, the job status is changed back to:

```text
queued
```

and its ID is placed back into the queue.

Once the retry limit is exhausted, the job is marked:

```text
failed
```

and persisted in:

```text
failed_jobs
```

## Failure Recovery

A separate process called the **Reaper** periodically checks for expired leases.

```text
Worker claims Job A
        ↓
Worker crashes
        ↓
Lease renewal stops
        ↓
Lease expires
        ↓
Reaper detects Job A
        ↓
Retrieve job information
        ↓
Requeue Job A
        ↓
Remove old ownership
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

## Graceful Worker Shutdown

Workers handle `Ctrl+C` using Go OS signal handling and context cancellation.

The shutdown flow is:

```text
Ctrl+C
   ↓
os.Interrupt
   ↓
shutdown channel
   ↓
cancel shutdown context
   ↓
stop waiting for new jobs
   ↓
finish current job
   ↓
cleanup ownership and lease
   ↓
worker exits
```

The worker uses separate contexts for shutdown and active job processing.

The shutdown context controls blocking operations such as `BRPOP`, while the active job continues using a normal context so that cancellation does not interrupt cleanup or lease renewal for the job currently being processed.

This allows the worker to stop accepting new work without abandoning its current job.

## Redis Data Model

### `job_data`

The canonical storage for all jobs.

```text
JobID → Job JSON
```

Example:

```json
{
  "id": "1787753848209597500",
  "type": "sleep",
  "status": "running",
  "duration": 60,
  "retries": 0
}
```

This allows job state to remain queryable regardless of which processing structure currently contains the job.

### `jobs`

A Redis List containing IDs of jobs waiting to be processed.

```text
jobs

┌──────┬──────┬──────┐
│ JobA │ JobB │ JobC │
└──────┴──────┴──────┘
```

Workers use `BRPOP` to atomically consume IDs.

### `active_jobs`

A Redis Hash containing currently claimed jobs.

```text
active_jobs

JobID → WorkerID
```

This represents the current worker ownership of a job.

### `job_leases`

A Redis Sorted Set used as an expiration index.

```text
job_leases

Job A → 1787750000000
Job B → 1787750015000
Job C → 1787750030000
```

The score is the lease expiration time in Unix milliseconds.

The Reaper queries for members whose score is less than or equal to the current time.

### `completed_jobs`

A Redis Hash used to persist successfully completed jobs.

```text
completed_jobs

JobID → Completed Job JSON
```

This provides a separate record of completed work while the canonical job state remains available in `job_data`.

### `failed_jobs`

A Redis Hash containing jobs that have exhausted their retry attempts.

```text
failed_jobs

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
│   │
│   ├── worker/
│   │   └── main.go
│   │
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
- Separating canonical data from processing state

### Go

- HTTP handlers
- JSON serialization and deserialization
- Shared packages
- `context.Context`
- Context cancellation
- OS signal handling
- `os.Interrupt`
- `signal.Notify`
- Goroutines
- Channels
- Tickers
- `select`
- Struct composition
- Error handling
- Graceful shutdown

### Distributed Systems

The project focuses heavily on failure modes.

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

Retries and recovery also mean the system is moving toward **at-least-once execution**, where the same job may execute more than once under certain failure scenarios.

## Known Limitations

### Lease Ownership Races

The current implementation does not yet fully protect against stale workers operating on jobs that have been reassigned.

For example:

```text
Worker A owns Job X
        ↓
A's lease expires
        ↓
Reaper recovers X
        ↓
Worker B claims X
        ↓
Worker A is still alive
        ↓
Worker A finishes X
```

If Worker A blindly performs cleanup or lease renewal, it could interfere with Worker B's newer ownership.

The next reliability milestone is to make lease renewal, cleanup, and recovery ownership-aware.

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

Exactly-once execution is difficult because failures can occur between otherwise separate operations.

Idempotency will eventually be used to reduce the impact of duplicate execution.

### Non-Atomic State Transitions

Several job state transitions currently involve multiple Redis operations.

For example:

```text
HDEL active_jobs
       ↓
ZREM job_leases
       ↓
update job status
```

A worker crash between operations can leave Redis in an inconsistent intermediate state.

Future work will make important state transitions atomic.

## Roadmap

### Reliability

- [x] Lease renewal
- [x] Worker crash recovery
- [x] Retry failed jobs
- [x] Retry limits
- [x] Failed-job persistence
- [x] Graceful worker shutdown
- [ ] Handle lease ownership races
- [ ] Atomic lease operations
- [ ] Atomic job state transitions
- [ ] Idempotency
- [ ] Better failure handling

### Job Management

- [x] Persistent job data
- [x] Job status tracking
- [x] Job lookup by ID
- [x] Completed-job persistence
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
Persist canonical job data
     ↓
Place job ID in Redis queue
     ↓
Distribute work across multiple workers
     ↓
Claim ownership
     ↓
Assign a lease
     ↓
Renew the lease while executing
     ↓
Execute the job
     ↓
Update job status
     ↓
Clean up ownership and lease
     ↓
Retry failed jobs
     ↓
Persist permanently failed jobs
     ↓
Detect crashed workers
     ↓
Requeue abandoned jobs
     ↓
Look up jobs by ID
     ↓
Gracefully shut down workers
```

The next major milestone is **robust lease ownership**, where stale workers are prevented from modifying jobs that have already been reassigned.
