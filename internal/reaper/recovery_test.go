package reaper

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"distributed-job-platform/internal/job"
	"distributed-job-platform/internal/testutil"

	"github.com/redis/go-redis/v9"
)

func TestFindExpiredJobs(t *testing.T) {
	ctx := context.Background()
	client := testutil.SetupRedis(t)
	now := time.Now().UnixMilli()
	if err := client.ZAdd(ctx, "job_leases",
		redis.Z{Score: float64(now - 1000), Member: "expired-job"},
		redis.Z{Score: float64(now + 60000), Member: "live-job"},
	).Err(); err != nil {
		t.Fatal(err)
	}

	got,err := FindExpiredJobs(ctx, client)
	if(err!=nil){
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "expired-job" {
		t.Fatalf("FindExpiredJobs() = %v, want [expired-job]", got)
	}
}

func TestRecoverJob(t *testing.T) {
	ctx := context.Background()
	client := testutil.SetupRedis(t)
	newJob := job.Job{ID: "recover-job", Type: "sleep", Status: "running", Duration: 3, Retries: 2}
	data, err := json.Marshal(newJob)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, "job_data", newJob.ID, data).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, "active_jobs", newJob.ID, "dead-worker").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, "job_leases", redis.Z{Score: float64(time.Now().UnixMilli() - 1000), Member: newJob.ID}).Err(); err != nil {
		t.Fatal(err)
	}

	recovered, err := RecoverJob(ctx, client, newJob.ID)
	if err != nil || !recovered {
		t.Fatalf("RecoverJob() = %v, %v; want true, nil", recovered, err)
	}
	stored, err := client.HGet(ctx, "job_data", newJob.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	var recoveredJob job.Job
	if err := json.Unmarshal([]byte(stored), &recoveredJob); err != nil {
		t.Fatal(err)
	}
	if recoveredJob.Status != "queued" || recoveredJob.Retries != newJob.Retries {
		t.Fatalf("recovered job = %+v, want queued status and unchanged retries", recoveredJob)
	}
	if count, err := client.LLen(ctx, "jobs").Result(); err != nil || count != 1 {
		t.Fatalf("queue length = %d, %v; want 1, nil", count, err)
	}
}

func TestRecoverJobMissingData(t *testing.T) {
	client := testutil.SetupRedis(t)
	recovered, err := RecoverJob(context.Background(), client, "missing-job")
	if err != nil || recovered {
		t.Fatalf("RecoverJob() = %v, %v; want false, nil", recovered, err)
	}
}
