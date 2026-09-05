package main

import (
	"context"
	"distributed-job-platform/internal/api"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/redis/go-redis/v9"
)

func main() {
	http.HandleFunc("/health", api.HealthHandler)

	fmt.Println("API Server Running on localhost:8080")

	redisAddr := os.Getenv("REDIS_ADDR")
	if(redisAddr==""){
		redisAddr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown,os.Interrupt)

	ctx := context.Background()

	http.HandleFunc("/jobs", api.CreateJobHandler(ctx,client))
	http.HandleFunc("/jobs/", api.GetJobHandler(ctx,client))

	server := &http.Server{
    Addr: ":8080",
	}

	go api.HandleShutdown(shutdown,server)

	err := server.ListenAndServe()
	if err != nil {
		fmt.Println(err)
	}

	client.Close()
}