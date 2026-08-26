package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"distributed-job-platform/internal/job"
	"github.com/redis/go-redis/v9"
	"os"
	"os/signal"
)

const maxRetries = 3

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

func executeJob(newJob job.Job) error {
	fmt.Println(newJob)

	if newJob.Type == "sleep" {
		time.Sleep(time.Duration(newJob.Duration) * time.Second)
		return nil
	}

	return fmt.Errorf("unknown job type: %s", newJob.Type)
}

func renewLease(
	ctx context.Context,
	client *redis.Client,
	jobID string,
	done chan struct{},
) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return

		case <-ticker.C:
			renewTime := time.Now().Add(30 * time.Second).UnixMilli()

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

func cleanupJob(
	ctx context.Context,
	client *redis.Client,
	jobID string,
	done chan struct{},
) {
	_, err := client.HDel(
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

	close(done)
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

func exitWorker(shutdown chan os.Signal, cancel context.CancelFunc){
	<-shutdown
	fmt.Println("Shutdown requested")
	cancel()
}

func main() {
	shutdown := make(chan os.Signal,1)
	signal.Notify(shutdown,os.Interrupt)

	workerID := fmt.Sprintf(
		"Worker-%d",
		time.Now().UnixNano(),
	)

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	shutdownCtx, cancel := context.WithCancel(context.Background())
	defer cancel()	

	ctx := context.Background()

	go exitWorker(shutdown,cancel)
	
	for {
		newJob, err := getJob(shutdownCtx, client)
		if err != nil {
			if shutdownCtx.Err() != nil {
					fmt.Println("Worker shutting down")
					break
			}
			panic(err)
		}

		err = claimJob(ctx, client, newJob, workerID)
		if err != nil {
			panic(err)
		}

		done := make(chan struct{})

		go renewLease(
			ctx,
			client,
			newJob.ID,
			done,
		)

		err = executeJob(newJob)

		cleanupJob(
			ctx,
			client,
			newJob.ID,
			done,
		)

		if err != nil {
			if newJob.Retries >= maxRetries {
				newJob.Status = "failed"

				err = updateJob(ctx, client, newJob)
				if err != nil {
					panic(err)
				}
			} else {
				newJob.Retries++
				requeue(ctx, client, newJob)
			}
		} else {
			newJob.Status = "completed"

			err = updateJob(ctx, client, newJob)
			if err != nil {
				panic(err)
			}
		}
	}
}