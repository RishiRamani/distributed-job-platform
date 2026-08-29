# Distributed Job Platform

A small distributed background-job system built with Go and Redis. The project explores job queues, worker ownership, lease-based coordination, recovery, retries, and API-level atomicity/idempotency.

## Current Status

Implemented:

- HTTP API for creating and looking up jobs
- Redis-backed queue of job IDs
- Multiple independent workers consuming the same queue
- Worker ownership tracked in Redis
- 30-second leases with 15-second renewal
- Ownership-aware lease renewal and completion/failure cleanup
- Atomic worker state transitions using Redis Lua scripts
- Atomic, idempotent job creation in the API using a Redis Lua script
- Reaper-based recovery for expired leases
- Up to three retries after the initial attempt
- Graceful worker shutdown and Redis-based coordination
- Docker Compose setup for Redis, API, worker, and reaper

Still limited or planned:

- The only job type is `sleep`.
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
                    | idempot...|
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

The API stores a complete job JSON document in `job_data`, pushes only the job ID to `jobs`, and records idempotency keys in `idempotency_keys` so repeated requests with the same `Idempotency-Key` return the same job instead of creating duplicate work.

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
| `job_data` | Hash | Canonical job JSON keyed by job ID |
| `jobs` | List | Job IDs waiting to be processed |
| `active_jobs` | Hash | Current owner worker ID keyed by job ID |
| `job_leases` | Sorted set | Lease expiration timestamps in Unix milliseconds |
| `idempotency_keys` | Hash | Idempotency key -> job ID mapping |

Completed and failed states remain in `job_data`, which is also used by the lookup endpoint.

## Ownership and Atomicity

The project uses Redis Lua scripts to keep state transitions consistent and race-safe.

### Job creation atomicity and idempotency

When the API creates a job, it runs a single Redis script that:

1. Checks whether the provided `Idempotency-Key` already maps to a job.
2. Returns the existing job ID if it does.
3. Stores the full job payload in `job_data`.
4. Pushes the job ID onto `jobs`.
5. Stores the `idempotency_key -> job_id` mapping when an idempotency key is provided.

This makes creation atomic and prevents duplicate work from repeated API requests with the same idempotency key.

### Worker ownership and lease safety

Claiming a job atomically:

1. Sets the job status to `running` in `job_data`.
2. Adds `job ID -> worker ID` to `active_jobs`.
3. Adds the lease expiration to `job_leases`.

While executing, the worker renews the lease every 15 seconds. Renewal checks that the worker still owns the job before changing the lease.

Completion, permanent failure, and retry requeue operations also check ownership in Lua before changing state. On a successful ownership check, they update `job_data` and remove or requeue the processing state as one Redis script operation. A worker that has lost ownership reports that fact and does not clean up the newer owner's state.

The Reaper rechecks the lease expiration inside its recovery script before requeueing a job. This prevents a lease that was renewed between the initial scan and recovery from being requeued.

## API

The API listens on `localhost:8080` by default, and also accepts `REDIS_ADDR` for containerized use.

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

Optional request header:

```text
Idempotency-Key: <unique client-generated key>
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

If the same `Idempotency-Key` is sent again, the API returns the same job instead of creating a second record.

### Look up a job

```text
GET /jobs/{id}
```

The endpoint reads from `job_data`, so it works while a job is queued, running, completed, or failed.

## Running Locally

### Option 1: Run with Docker Compose

From the project root:

```powershell
docker compose up --build
```

This starts:

- Redis on `localhost:6379`
- API on `localhost:8080`
- Worker
- Reaper

To stop everything:

```powershell
docker compose down
```

### Option 2: Run directly with Go

Start Redis locally, then run each process in a separate terminal from the project root:

```powershell
go run ./cmd/api
go run ./cmd/worker
go run ./cmd/reaper
```

The API and workers read from the `REDIS_ADDR` environment variable if set; otherwise they default to `localhost:6379`.

Create a job:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8080/jobs `
  -ContentType "application/json" `
  -Headers @{ "Idempotency-Key" = "client-req-123" } `
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
|   |-- api/main.go       API entrypoint and Redis client setup
|   |-- worker/
|   |   |-- main.go       worker loop and execution
|   |   |-- jobs.go       claim, completion, retry, and queue operations
|   |   |-- lease.go      lease renewal
|   |   `-- shutdown.go   signal handling
|   `-- reaper/main.go    expired-lease recovery
|
|-- internal/
|   |-- api/
|   |   `-- handlers.go   HTTP handlers and idempotent create endpoint
|   |-- job/
|   |   `-- job.go        shared job models
|   |-- reaper/
|   |   `-- recovery.go   lease expiry handling
|   |-- worker/
|   |   `-- *.go          worker logic
|   `-- testutil/
|       `-- redis.go      Redis test setup helper
|
|-- Dockerfile
|-- docker-compose.yml
|-- go.mod
|-- go.sum
|-- README.md
```

## Design Notes

The system aims for at-least-once execution. A worker can perform work successfully and fail before recording completion, after which the Reaper may make the job available again. External work should therefore be idempotent if duplicate execution would be harmful.

Redis Lua scripts protect the worker and recovery transitions from stale-worker cleanup races, and the API creation path now prevents duplicate job creation with idempotency keys.

## Technologies

- Go 1.27
- Redis 7
- Docker and Docker Compose
- `github.com/redis/go-redis/v9`
