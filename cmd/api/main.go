package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"distributed-job-platform/internal/job"
	"github.com/redis/go-redis/v9"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}

func createJobHandler(client *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		newJob := job.Job{
			ID:       fmt.Sprintf("%d", time.Now().UnixNano()),
			Type:     "sleep",
			Status:   "queued",
			Duration: 60,
			Retries:  0,
		}

		ctx := context.Background()

		jsonData, err := json.Marshal(newJob)
		if err != nil {
			panic(err)
		}

		_, err = client.HSet(
			ctx,
			"job_data",
			newJob.ID,
			string(jsonData),
		).Result()
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

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(newJob)
	}
}

func getJobHandler(client *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")

		ctx := context.Background()

		jobData, err := client.HGet(
			ctx,
			"job_data",
			jobID,
		).Result()

		if err == redis.Nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		var newJob job.Job

		err = json.Unmarshal([]byte(jobData), &newJob)
		if err != nil {
			http.Error(w, "invalid job data", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(newJob)
	}
}

func main() {
	http.HandleFunc("/health", healthHandler)

	fmt.Println("API Server Running on localhost:8080")

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	http.HandleFunc("/jobs", createJobHandler(client))
	http.HandleFunc("/jobs/", getJobHandler(client))

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println(err)
	}

	client.Close()
}