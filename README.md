# Distributed Job Platform

A distributed background job processing system built from scratch using
Go, Redis, and Docker.

The goal of this project is to understand how distributed job queues
work internally, including worker coordination, job ownership, leases,
failure recovery, and eventually reliable execution semantics.

> **Status:** Day 1 --- Core queue, workers, leases, and failure
> recovery implemented. Lease renewal and further reliability mechanisms
> are next.

## Overview

The platform allows an API to create jobs which are placed into a
Redis-backed queue. Multiple worker processes can independently consume
and execute jobs.

Workers claim jobs using a lease mechanism. If a worker crashes while
processing a job, a separate Reaper process detects the expired lease
and puts the job back into the queue so another worker can process it.

``` text
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

-   Unique job ID
-   Job type
-   Job status
-   Duration

Example:

``` json
{
  "id": "1787669074517250300",
  "type": "sleep",
  "status": "queued",
  "duration": 10
}
```

### Redis-backed Queue

Jobs are serialized as JSON and stored in a Redis list.

``` text
jobs
┌──────────────────────────────┐
│ Job C │ Job B │ Job A │ ... │
└──────────────────────────────┘
```

Workers use Redis `BRPOP` to wait for jobs instead of repeatedly polling
an empty queue.

### Multiple Workers

Multiple independent worker processes can consume from the same Redis
queue.

``` text
              Redis
                │
        ┌───────┼───────┐
        ▼       ▼       ▼
      Worker  Worker  Worker
        1       2       3
```

Redis atomically removes jobs from the queue when they are popped,
allowing different workers to receive different jobs.

### Job Execution

Jobs currently support a basic `sleep` job.

For example, a duration of `10` causes the worker to sleep for 10
seconds.

Job execution is separated from queue management so additional job types
can be added later.

## Job Claims and Leases

When a worker receives a job, it records a claim containing:

``` text
Job
Worker ID
```

The claim is stored in the Redis hash:

``` text
active_jobs
```

The worker also creates a lease for the job in the Redis sorted set:

``` text
job_leases
```

The current lease duration is 30 seconds.

The sorted set uses:

``` text
member = Job ID
score  = lease expiration timestamp
```

This allows the Reaper to efficiently find expired jobs.

## Successful Job Completion

When a worker successfully finishes a job, it cleans up its claim and
lease:

``` text
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

This prevents completed jobs from being treated as abandoned work later.

## Failure Recovery

A separate process called the **Reaper** periodically checks for expired
leases.

``` text
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

1.  Starting a worker.
2.  Submitting a long-running job.
3.  Killing the worker while the job was executing.
4.  Waiting for the lease to expire.
5.  Allowing the Reaper to detect the expired job.
6.  Starting another worker.
7.  Verifying that the recovered job was processed again.

## Redis Data Model

### `jobs`

A Redis List containing jobs waiting to be processed.

``` text
jobs
┌──────┬──────┬──────┐
│ JobA │ JobB │ JobC │
└──────┴──────┴──────┘
```

Jobs are stored as JSON strings.

### `active_jobs`

A Redis Hash containing currently claimed jobs.

``` text
active_jobs

JobID → JobClaim JSON
```

Example:

``` json
{
  "Job": {
    "id": "1787669074517250300",
    "type": "sleep",
    "status": "queued",
    "duration": 10
  },
  "WorkerId": "Worker-1787669064545528900"
}
```

### `job_leases`

A Redis Sorted Set used as an expiration index.

``` text
job_leases

Job A → 1787669104531
Job B → 1787669152000
Job C → 1787669201000
```

The score is the lease expiration time in Unix milliseconds.

The Reaper queries for members whose score is less than or equal to the
current time.

## Project Structure

``` text
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

The shared `job` package contains the domain structures used by the API,
workers, and Reaper.

## Technologies

-   **Go** --- API, workers, and Reaper
-   **Redis** --- job queue and distributed state
-   **Docker** --- local Redis environment

## What I Learned

### Redis

-   Redis Lists
-   Redis Hashes
-   Redis Sorted Sets
-   `BRPOP`
-   `HSET` / `HGET` / `HDEL`
-   `ZADD` / `ZRANGEBYSCORE` / `ZREM`
-   Blocking vs polling queue consumption

### Go

-   HTTP handlers
-   JSON serialization and deserialization
-   Shared packages
-   `context.Context`
-   Redis client usage
-   Multiple processes
-   Basic concurrency concepts
-   Struct composition
-   Error handling

### Distributed Systems

The biggest focus so far has been understanding failure modes.

A basic queue has a serious problem:

``` text
Worker receives Job A
        ↓
Job removed from queue
        ↓
Worker crashes
        ↓
Job is lost
```

Adding leases allows the system to detect abandoned work:

``` text
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

However, this does **not** guarantee exactly-once execution.

## Known Limitations

### Lease Expiration During Valid Execution

The current lease lasts 30 seconds.

If a job takes longer than 30 seconds:

``` text
Worker 1
   ↓
Job A
   ↓
30 seconds
   ↓
lease expires
   ↓
Reaper requeues Job A
   ↓
Worker 2 receives Job A
```

Worker 1 may still be processing the original job.

This can result in the same job being executed simultaneously by two
workers.

### Next: Lease Renewal

Workers will periodically renew their leases while executing jobs.

Planned mechanism:

``` text
Claim
  ↓
30 second lease
  ↓
renew after ~15 seconds
  ↓
renew again
  ↓
...
```

If the worker actually dies, renewals stop and the lease eventually
expires.

## Execution Semantics

The system is currently moving toward **at-least-once execution**.

The goal is:

> Jobs should not be permanently lost, but a job may execute more than
> once.

Exactly-once execution is significantly harder because failures can
occur between operations.

For example:

``` text
Worker finishes job
        ↓
sends ACK
        ↓
network failure
        ↓
Worker does not know whether ACK reached Redis
```

A retry/recovery mechanism may therefore cause the same job to execute
again.

Future work will address duplicate execution through mechanisms such as
idempotency.

## Roadmap

### Reliability

-   [ ] Lease renewal / worker heartbeats
-   [ ] Handle lease ownership races
-   [ ] Retry failed jobs
-   [ ] Idempotency
-   [ ] Better failure handling
-   [ ] Graceful worker shutdown

### Job Management

-   [ ] Job status endpoint
-   [ ] Job lookup by ID
-   [ ] Job cancellation
-   [ ] Job priorities
-   [ ] Scheduled jobs
-   [ ] Retry limits

### Infrastructure

-   [ ] PostgreSQL persistence
-   [ ] Better Redis error handling
-   [ ] Metrics
-   [ ] Structured logging
-   [ ] Health checks
-   [ ] Load testing
-   [ ] Failure/chaos testing

### Deployment

-   [ ] Docker Compose
-   [ ] Containerize API
-   [ ] Containerize workers
-   [ ] Containerize Reaper
-   [ ] Production deployment

## Day 1 Milestone

At the end of Day 1, the system can:

``` text
Create a job
     ↓
Persist it in Redis
     ↓
Distribute it across multiple workers
     ↓
Execute it
     ↓
Track worker ownership
     ↓
Assign a lease
     ↓
Clean up completed jobs
     ↓
Detect crashed workers
     ↓
Requeue abandoned jobs
     ↓
Allow another worker to execute them
```

The next major milestone is **lease renewal**, which will prevent
healthy workers processing long-running jobs from having their work
incorrectly requeued.
