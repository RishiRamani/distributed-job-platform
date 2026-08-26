package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"github.com/redis/go-redis/v9"
	"distributed-job-platform/internal/job"
)

func findExpiredJobs(
	ctx context.Context,
	client *redis.Client,
) []string {
	jobs, err := client.ZRangeByScore(
		ctx,
		"job_leases",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf("%d", time.Now().UnixMilli()),
		},
	).Result()

	if err != nil {
		panic(err)
	}

	return jobs
}

func recoverJob(
	ctx context.Context,
	client *redis.Client,
	jobID string,
) {
	// Make sure the job is still active.
	workerID, err := client.HGet(
		ctx,
		"active_jobs",
		jobID,
	).Result()

	if err == redis.Nil {
		return
	}

	if err != nil {
		panic(err)
	}

	fmt.Printf(
		"Recovering job %s from %s\n",
		jobID,
		workerID,
	)

	// Get the canonical job data.
	jobData, err := client.HGet(
		ctx,
		"job_data",
		jobID,
	).Result()

	if err != nil {
		panic(err)
	}

	var newJob job.Job

	err = json.Unmarshal(
		[]byte(jobData),
		&newJob,
	)
	if err != nil {
		panic(err)
	}

	newJob.Status = "queued"

	updatedData, err := json.Marshal(newJob)
	if err != nil {
		panic(err)
	}

	// Update canonical job state.
	_, err = client.HSet(
		ctx,
		"job_data",
		jobID,
		string(updatedData),
	).Result()

	if err != nil {
		panic(err)
	}

	// Put the job back into the queue.
	_, err = client.LPush(
		ctx,
		"jobs",
		jobID,
	).Result()

	if err != nil {
		panic(err)
	}

	// Remove old ownership information.
	_, err = client.HDel(
		ctx,
		"active_jobs",
		jobID,
	).Result()

	if err != nil {
		panic(err)
	}

	_, err = client.ZRem(
		ctx,
		"job_leases",
		jobID,
	).Result()

	if err != nil {
		panic(err)
	}
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	ctx := context.Background()

	for {
		jobs := findExpiredJobs(ctx, client)

		for _, jobID := range jobs {
			recoverJob(ctx, client, jobID)
		}

		time.Sleep(time.Second)
	}
}