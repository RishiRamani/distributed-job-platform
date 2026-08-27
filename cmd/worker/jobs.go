package main

import (
	"context"
	"encoding/json"
	"time"
	"distributed-job-platform/internal/job"
	"github.com/redis/go-redis/v9"

)


func ownershipOfJob(ctx context.Context,client *redis.Client,jobId string,workerID string)(bool,error){

	res,err:= client.HGet(ctx,"active_jobs",jobId).Result()
	if(err==redis.Nil){
		return false,nil
	}
	if(err!=nil){
		return false,err
	}
	return workerID==res,nil

}

func getJob(ctx context.Context, client *redis.Client) (job.Job, error) {
	res, err := client.BRPop(ctx, 0, "jobs").Result()
	if err != nil {
		return job.Job{}, err
	}

	jobID := res[1]

	jobData, err := client.HGet(
		ctx,
		"job_data",
		jobID,
	).Result()
	if err != nil {
		return job.Job{}, err
	}

	var newJob job.Job

	err = json.Unmarshal([]byte(jobData), &newJob)
	if err != nil {
		return job.Job{}, err
	}

	return newJob, nil
}

func updateJob(ctx context.Context, client *redis.Client, newJob job.Job) error {
	jsonData, err := json.Marshal(newJob)
	if err != nil {
		return err
	}

	_, err = client.HSet(
		ctx,
		"job_data",
		newJob.ID,
		string(jsonData),
	).Result()

	return err
}

func claimJob(
	ctx context.Context,
	client *redis.Client,
	newJob job.Job,
	workerID string,
) error {

	newJob.Status = "running"

	err := updateJob(ctx, client, newJob)
	if err != nil {
		return err
	}

	_, err = client.HSet(
		ctx,
		"active_jobs",
		newJob.ID,
		workerID,
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

	return err
}


func requeue(
	ctx context.Context,
	client *redis.Client,
	newJob job.Job,
) {
	newJob.Status = "queued"

	err := updateJob(ctx, client, newJob)
	if err != nil {
		panic(err)
	}

	_, err = client.LPush(
		ctx,
		"jobs",
		newJob.ID,
	).Result()
	if err != nil {
		panic(err)
	}
}