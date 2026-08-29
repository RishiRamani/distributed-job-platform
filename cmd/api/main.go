package main

import (
	"fmt"
	"net/http"
	"distributed-job-platform/internal/api"
	"github.com/redis/go-redis/v9"
)



func main() {
	http.HandleFunc("/health", api.HealthHandler)

	fmt.Println("API Server Running on localhost:8080")

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	http.HandleFunc("/jobs", api.CreateJobHandler(client))
	http.HandleFunc("/jobs/", api.GetJobHandler(client))

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println(err)
	}

	client.Close()
}