# Distributed Job Platform

A small distributed background-job system built with Go and Redis. The project is intentionally implemented from first principles to explore queues, worker ownership, leases, recovery, retries, and shutdown behavior.

## Current Status

Implemented:

- HTTP API for creating and looking up jobs
- Redis-backed queue of job IDs
- Multiple independent workers consuming the same queue
- Worker ownership recorded in Redis
- 30-second leases with 15-second renewal
- Ownership-aware lease renewal and completion/failure cleanup
- Atomic worker state transitions using Redis Lua scripts
- Reaper-based recovery for expired leases
- Up to three retries after the initial attempt
- Graceful worker shutdown that stops taking new jobs and finishes the active job

Still limited or planned:

- The only job type is `sleep`.
- Job creation persists the job and queues its ID in two separate Redis commands.
- There is no cancellation, priority, scheduling, retry backoff, metrics, or structured logging.
- Execution is at-least-once rather than exactly-once.

## Architecture

```text
                         HTTP
                          |
                          v
                    +-----------+
                    |    API    |
                    +-----+-----+
                          |
                          v
                    +-----------+
                    |   Redis   |
                    | job_data  |
                    |   jobs    |
                    | active... |
                    | leases    |
                    +-----+-----+
                          |
                    BRPOP jobs
              +-----------+-----------+
              v                       v
        +-----------+           +-----------+
        |  Worker   |           |  Worker   |
        +-----------+           +-----------+
              ^                       ^
              +-----------+-----------+
                          |
                    +-----------+
                    |  Reaper   |
                    +-----------+
```

The API stores a complete job JSON document in `job_data` and pushes only the job ID to `jobs`. A worker pops the ID, loads the canonical record, claims it, and creates a lease. The Reaper looks for expired lease scores and puts abandoned jobs back into the queue.

## Job Lifecycle

```text
queued -> running -> completed
                   |
                   +-> queued -> running   (retry)
                   |
                   +-> failed               (retry limit reached)
```

The worker supports one job type:

```json
{
  "type": "sleep",
  "duration": 5
}
```

`duration` is measured in seconds. Unknown job types fail and follow the retry policy.

The retry counter starts at zero. A failed initial attempt can be followed by three retries; once `retries >= 3`, the job is marked `failed` instead of being queued again.

## Redis Data Model

| Key | Redis type | Purpose |
| --- | --- | --- |
| `job_data` | Hash | Canonical job JSON, keyed by job ID |
| `jobs` | List | IDs waiting to be processed |
| `active_jobs` | Hash | Current owner worker ID, keyed by job ID |
| `job_leases` | Sorted set | Lease expiration time in Unix milliseconds |

There are currently no separate `completed_jobs` or `failed_jobs` structures. Completed and failed states remain in `job_data`, which is also used by the lookup endpoint.

## Ownership and Atomicity

Claiming a job atomically:

1. Sets the job status to `running` in `job_data`.
2. Adds `job ID -> worker ID` to `active_jobs`.
3. Adds the lease expiration to `job_leases`.

While executing, the worker renews the lease every 15 seconds. Renewal checks that the worker still owns the job before changing the lease.

Completion, permanent failure, and retry requeue operations also check ownership in Lua before changing state. On a successful ownership check, they update `job_data` and remove or requeue the processing state as one Redis script operation. A worker that has lost ownership reports that fact and does not clean up the newer owner's state.

The Reaper rechecks the lease expiration inside its recovery script before requeueing a job. This prevents a lease that was renewed between the initial scan and recovery from being requeued.

## API

The API listens on `localhost:8080`.

### Health check

```text
GET /health
```

Returns:

```text
OK
```

### Create a job

```text
POST /jobs
Content-Type: application/json
```

Request:

```json
{
  "type": "sleep",
  "duration": 5
}
```

The response contains the generated ID, type, status, duration, and retry count:

```json
{
  "id": "1787753848209597500",
  "type": "sleep",
  "status": "queued",
  "duration": 5,
  "retries": 0
}
```

### Look up a job

```text
GET /jobs/{id}
```

The endpoint reads from `job_data`, so it works while a job is queued, running, completed, or failed.

## Running Locally

Start Redis on `localhost:6379`, then run each process from the project root in a separate terminal:

```powershell
go run ./cmd/api
go run ./cmd/worker
go run ./cmd/reaper
```

Create a job:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8080/jobs `
  -ContentType "application/json" `
  -Body '{"type":"sleep","duration":5}'
```

Look it up using the returned ID:

```powershell
Invoke-RestMethod http://localhost:8080/jobs/{id}
```

Start multiple worker processes to distribute queued jobs between them. To exercise recovery, stop a worker while it is running a job and leave the Reaper running; after the 30-second lease expires, the job is returned to `jobs`.

## Project Structure

```text
distributed-job-platform/
|
|-- cmd/
|   |-- api/main.go       HTTP API and Redis persistence
|   |-- worker/
|   |   |-- main.go       worker loop and execution
|   |   |-- jobs.go       claim, completion, retry, and queue operations
|   |   |-- lease.go      lease renewal
|   |   `-- shutdown.go   signal handling
|   `-- reaper/main.go    expired-lease recovery
|
|-- internal/job/job.go   shared job data structures
|-- go.mod
`-- README.md
```

## Design Notes

The system aims for at-least-once execution. A worker can perform work successfully and fail before recording completion, after which the Reaper may make the job available again. External work should therefore be idempotent if duplicate execution would be harmful.

Redis Lua scripts now protect the worker and recovery transitions from stale-worker cleanup races. The next reliability improvements are to make job creation atomic, improve Redis error handling, add tests and observability, and support idempotency or cancellation semantics.

## Technologies

- Go 1.27
- Redis
- `github.com/redis/go-redis/v9`
