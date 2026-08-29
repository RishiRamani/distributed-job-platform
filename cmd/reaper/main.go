package main

import (
	"context"

	"fmt"
	"time"
	"distributed-job-platform/internal/reaper"
	"github.com/redis/go-redis/v9"
	"os"
)



func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if(redisAddr==""){
		redisAddr="localhost:6379"
	}
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	ctx := context.Background()

	for {
		jobs := reaper.FindExpiredJobs(ctx, client)

		for _, jobID := range jobs {
			recovered, err := reaper.RecoverJob(ctx, client, jobID)

			if err != nil {
				panic(err)
			}

			if recovered {
				fmt.Println("Recovered job:", jobID)
			}
		}

		time.Sleep(time.Second)
	}
}