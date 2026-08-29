package main

import (
	"context"
	"fmt"
	"time"
	"distributed-job-platform/internal/job"
	"distributed-job-platform/internal/worker"
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

	go worker.ExitWorker(shutdown,cancel)
	
	for {
		newJob, err := worker.GetJob(shutdownCtx, client)
		if err != nil {
			if shutdownCtx.Err() != nil {
				fmt.Println("Worker shutting down")
				break
			}

			if err == redis.Nil {
				continue
			}

			panic(err)
		}

		err = worker.ClaimJob(ctx, client, newJob, workerID)
		if err != nil {
			panic(err)
		}

		done := make(chan struct{})

		go worker.RenewLease(
			ctx,
			client,
			newJob.ID,
			workerID,
			done,
		)

		err = executeJob(newJob)

		if err != nil {
			if newJob.Retries >= maxRetries {
				success,err := worker.FailJob(ctx,client,newJob,workerID)
				if err != nil {
					panic(err)
				}
				if !success {
					fmt.Println("Lost ownership of job:", newJob.ID)
				}
			} else {
				success,err:=worker.Requeue(ctx, client, newJob,workerID)
				if(err!=nil){
					panic(err)
				}
				if !success {
					fmt.Println("Lost ownership of job:", newJob.ID)
				}
			}
		} else {
			success,err := worker.CompleteJob(ctx,client,newJob,workerID)
			if err != nil {
				panic(err)
			}
			if !success {
				fmt.Println("Lost ownership of job:", newJob.ID)
			}
		}
		close(done)
	}
}