package worker

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"distributed-job-platform/internal/job"
	"distributed-job-platform/internal/testutil"

	"github.com/redis/go-redis/v9"
)

func testJob() job.Job {
	return job.Job{ID: "test-job", Type: "sleep", Status: "queued", Duration: 10}
}

func storeJob(t *testing.T, client *redis.Client, newJob job.Job) {
	t.Helper()
	jobData, err := json.Marshal(newJob)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), "job_data", newJob.ID, jobData).Err(); err != nil {
		t.Fatal(err)
	}
}

func claimTestJob(t *testing.T, client *redis.Client, newJob job.Job, workerID string) {
	t.Helper()
	if err := ClaimJob(context.Background(), client, newJob, workerID); err != nil {
		t.Fatal(err)
	}
}

func TestClaimJob(t *testing.T) {
	ctx := context.Background()
	client := testutil.SetupRedis(t)

	newJob := job.Job{
		ID:       "test-job-1",
		Type:     "sleep",
		Status:   "queued",
		Duration: 10,
		Retries:  0,
	}

	workerID := "test-worker-1"

	// Store the initial queued job.
	jobData, err := json.Marshal(newJob)
	if err != nil {
		t.Fatal(err)
	}

	err = client.HSet(
		ctx,
		"job_data",
		newJob.ID,
		string(jobData),
	).Err()

	if err != nil {
		t.Fatal(err)
	}

	// Claim the job.
	err = ClaimJob(
		ctx,
		client,
		newJob,
		workerID,
	)

	if err != nil {
		t.Fatal(err)
	}

	// Check that the job status became running.
	storedData, err := client.HGet(
		ctx,
		"job_data",
		newJob.ID,
	).Result()

	if err != nil {
		t.Fatal(err)
	}

	var storedJob job.Job

	err = json.Unmarshal(
		[]byte(storedData),
		&storedJob,
	)

	if err != nil {
		t.Fatal(err)
	}

	if storedJob.Status != "running" {
		t.Errorf(
			"expected job status running, got %s",
			storedJob.Status,
		)
	}

	// Check that ownership was recorded.
	owner, err := client.HGet(
		ctx,
		"active_jobs",
		newJob.ID,
	).Result()

	if err != nil {
		t.Fatal(err)
	}

	if owner != workerID {
		t.Errorf(
			"expected owner %s, got %s",
			workerID,
			owner,
		)
	}

	// Check that a lease was created.
	leaseExpiry, err := client.ZScore(
		ctx,
		"job_leases",
		newJob.ID,
	).Result()

	if err != nil {
		t.Fatal(err)
	}

	currentTime := float64(time.Now().UnixMilli())

	if leaseExpiry <= currentTime {
		t.Errorf(
			"expected lease expiry to be in the future, got %f",
			leaseExpiry,
		)
	}
}

func TestCompleteJob(t *testing.T) {
	ctx := context.Background()
	client := testutil.SetupRedis(t)
	newJob := testJob()
	workerID := "worker-1"
	storeJob(t, client, newJob)
	claimTestJob(t, client, newJob, workerID)

	completed, err := CompleteJob(ctx, client, newJob, workerID)
	if err != nil || !completed {
		t.Fatalf("CompleteJob() = %v, %v; want true, nil", completed, err)
	}
	assertStoredStatus(t, client, newJob.ID, "completed")
	assertJobReleased(t, client, newJob.ID)
}

func TestFailJobRejectsNonOwner(t *testing.T) {
	ctx := context.Background()
	client := testutil.SetupRedis(t)
	newJob := testJob()
	storeJob(t, client, newJob)
	claimTestJob(t, client, newJob, "worker-1")

	failed, err := FailJob(ctx, client, newJob, "worker-2")
	if err != nil || failed {
		t.Fatalf("FailJob() = %v, %v; want false, nil", failed, err)
	}
	assertStoredStatus(t, client, newJob.ID, "running")
}

func TestFailJob(t *testing.T) {
	ctx := context.Background()
	client := testutil.SetupRedis(t)
	newJob := testJob()
	workerID := "worker-1"
	storeJob(t, client, newJob)
	claimTestJob(t, client, newJob, workerID)

	failed, err := FailJob(ctx, client, newJob, workerID)
	if err != nil || !failed {
		t.Fatalf("FailJob() = %v, %v; want true, nil", failed, err)
	}
	assertStoredStatus(t, client, newJob.ID, "failed")
	assertJobReleased(t, client, newJob.ID)
}

func TestGetJob(t *testing.T) {
	ctx := context.Background()
	client := testutil.SetupRedis(t)
	newJob := testJob()
	storeJob(t, client, newJob)
	if err := client.LPush(ctx, "jobs", newJob.ID).Err(); err != nil {
		t.Fatal(err)
	}

	got, err := GetJob(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if got != newJob {
		t.Fatalf("GetJob() = %+v, want %+v", got, newJob)
	}
}

func TestGetJobReturnsQueueError(t *testing.T) {
	client := testutil.SetupRedis(t)
	_, err := GetJob(context.Background(), client)
	if err != redis.Nil {
		t.Fatalf("GetJob() error = %v, want %v", err, redis.Nil)
	}
}

func TestRequeue(t *testing.T) {
	ctx := context.Background()
	client := testutil.SetupRedis(t)
	newJob := testJob()
	workerID := "worker-1"
	storeJob(t, client, newJob)
	claimTestJob(t, client, newJob, workerID)

	requeued, err := Requeue(ctx, client, newJob, workerID)
	if err != nil || !requeued {
		t.Fatalf("Requeue() = %v, %v; want true, nil", requeued, err)
	}
	assertStoredStatus(t, client, newJob.ID, "queued")
	data, err := client.HGet(ctx, "job_data", newJob.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	var stored job.Job
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Retries != 1 {
		t.Fatalf("requeued retries = %d, want 1", stored.Retries)
	}
	if count, err := client.LLen(ctx, "jobs").Result(); err != nil || count != 1 {
		t.Fatalf("queue length = %d, %v; want 1, nil", count, err)
	}
}

func TestRenewLeaseStopsWhenDone(t *testing.T) {
	client := testutil.SetupRedis(t)
	done := make(chan struct{})
	close(done)
	finished := make(chan struct{})
	go func() {
		RenewLease(context.Background(), client, "job-id", "worker-id", done)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("RenewLease did not stop after done was closed")
	}
}

func TestExitWorkerCancelsContext(t *testing.T) {
	shutdown := make(chan os.Signal, 1)
	cancelled := make(chan struct{})
	shutdown <- os.Interrupt
	go ExitWorker(shutdown, func() { close(cancelled) })

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("ExitWorker did not call cancel")
	}
}

func assertStoredStatus(t *testing.T, client *redis.Client, jobID, want string) {
	t.Helper()
	data, err := client.HGet(context.Background(), "job_data", jobID).Result()
	if err != nil {
		t.Fatal(err)
	}
	var stored job.Job
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status != want {
		t.Fatalf("stored status = %q, want %q", stored.Status, want)
	}
}

func assertJobReleased(t *testing.T, client *redis.Client, jobID string) {
	t.Helper()
	if exists, err := client.HExists(context.Background(), "active_jobs", jobID).Result(); err != nil || exists {
		t.Fatalf("active job exists = %v, %v; want false, nil", exists, err)
	}
	if _, err := client.ZScore(context.Background(), "job_leases", jobID).Result(); err != redis.Nil {
		t.Fatalf("lease lookup error = %v, want %v", err, redis.Nil)
	}
}
