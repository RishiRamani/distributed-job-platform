package main

import (
	"context"
	"os/signal"

	"distributed-job-platform/internal/reaper"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
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

	shutdown:= make(chan os.Signal,1)
	signal.Notify(shutdown,os.Interrupt)
	shutdownCtx,cancel:=context.WithCancel(context.Background()) 
	defer cancel()
	ctx := context.Background()

	go reaper.HandleShutdown(shutdown,cancel)

	for {
		jobs,err := reaper.FindExpiredJobs(shutdownCtx, client)

		if(err!=nil){
			return
		}

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