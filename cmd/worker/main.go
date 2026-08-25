package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"distributed-job-platform/internal/job"
	"github.com/redis/go-redis/v9"
)

func getJob(ctx context.Context, client *redis.Client) (job.Job, error) {
	res, err := client.BRPop(ctx, 0, "jobs").Result()
	if err != nil {
		return job.Job{}, err
	}

	var newJob job.Job

	err = json.Unmarshal([]byte(res[1]), &newJob)
	if err != nil {
		return job.Job{}, err
	}

	return newJob, nil
}

func claimJob(
	ctx context.Context,
	client *redis.Client,
	newJob job.Job,
	workerID string,
) error {
	newJobClaim := job.JobClaim{
		Job:      newJob,
		WorkerId: workerID,
	}

	jsonData, err := json.Marshal(newJobClaim)
	if err != nil {
		return err
	}

	_, err = client.HSet(
		ctx,
		"active_jobs",
		newJob.ID,
		string(jsonData),
	).Result()
	if err != nil {
		return err
	}

	expiry := time.Now().Add(30 * time.Second).UnixMilli()

	_, err = client.ZAdd(
		ctx,
		"job_leases",
		redis.Z{
			Score:  float64(expiry),
			Member: newJob.ID,
		},
	).Result()
	if err != nil {
		return err
	}

	return nil
}

func executeJob(newJob job.Job) {
	fmt.Println(newJob)
	if newJob.Type == "sleep" {
		time.Sleep(time.Duration(newJob.Duration) * time.Second)
	}
}

func renewLease(ctx context.Context,client *redis.Client,jobID string,done chan struct{}){
	ticker := time.NewTicker(15 * time.Second)
  defer ticker.Stop()
	for{
		select{
			case <-done:
				return
			case <-ticker.C:
				renewTime := time.Now().Add(5*time.Second).UnixMilli()
				_, err := client.ZAdd(
					ctx,
					"job_leases",
					redis.Z{
						Score:  float64(renewTime),
						Member: jobID,
					},
				).Result()
				if err != nil {
					panic(err)
				}
		}
	}
}

func completeJob(ctx context.Context,client *redis.Client,jobID string,done chan struct{}){
	_,err:=client.HDel(ctx, "active_jobs", jobID).Result()
	if(err!=nil){
		panic(err)
	}
	_,err=client.ZRem(ctx, "job_leases", jobID).Result()
	if(err!=nil){
		panic(err)
	}
	close(done)
}

func main() {
	workerID := fmt.Sprintf("Worker-%d", time.Now().UnixNano())

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	ctx := context.Background()

	for {
		newJob, err := getJob(ctx, client)
		if err != nil {
			panic(err)
		}

		err = claimJob(ctx, client, newJob, workerID)
		if err != nil {
			panic(err)
		}
		done := make(chan struct{})
		go renewLease(ctx,client,newJob.ID,done)
		executeJob(newJob)
		completeJob(ctx,client,newJob.ID,done)
	}
}