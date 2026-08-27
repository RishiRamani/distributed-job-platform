package main

import (
	"context"
	"fmt"
	"time"
	"distributed-job-platform/internal/job"
	"github.com/redis/go-redis/v9"
	"os"
	"os/signal"
)

const maxRetries = 3



func executeJob(newJob job.Job) error {
	fmt.Println(newJob)

	if newJob.Type == "sleep" {
		time.Sleep(time.Duration(newJob.Duration) * time.Second)
		return nil
	}

	return fmt.Errorf("unknown job type: %s", newJob.Type)
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
			workerID,
			done,
		)

		err = executeJob(newJob)

		isOwner, ownershipErr := ownershipOfJob(
				ctx,
				client,
				newJob.ID,
				workerID,
		)

		if ownershipErr != nil {
				panic(ownershipErr)
		}

		if !isOwner {
				fmt.Println("Lost ownership of job:", newJob.ID)
				close(done)
				continue
		}

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

		cleanupJob(
			ctx,
			client,
			newJob.ID,
			workerID,
			done,
		)
	}
}