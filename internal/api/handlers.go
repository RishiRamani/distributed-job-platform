package api

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


func HealthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}

func CreateJobHandler(client *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request job.CreateJobRequest

		err:= json.NewDecoder(r.Body).Decode(&request)
		if(err!=nil){
			http.Error(w,"invalid request",http.StatusBadRequest)
			return
		}

		ctx := context.Background()

		idempotencyKey := r.Header.Get("Idempotency-Key")

		script := redis.NewScript(`
			if ARGV[1] ~= "" then
    			local jobID = redis.call("HGET", KEYS[1], ARGV[1])
					if jobID then
							return jobID
					end
			end
			redis.call("HSET",KEYS[2],ARGV[2],ARGV[3])
			redis.call("LPUSH",KEYS[3],ARGV[2])
			if ARGV[1] ~= "" then
				redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
			end
			return ARGV[2]
		`)

		newJob := job.Job{
			ID:       fmt.Sprintf("%d", time.Now().UnixNano()),
			Type:     request.Type,
			Status:   "queued",
			Duration: request.Duration,
			Retries:  0,
		}

		jsonData, err := json.Marshal(newJob)
		if err != nil {
			panic(err)
		}

		jobID, err := script.Run(
			ctx,
			client,
			[]string{
				"idempotency_keys",
				"job_data",
				"jobs",
			},
			idempotencyKey,
			newJob.ID,
			string(jsonData),
		).Result()

		if(err!=nil){
			panic(err)
		}

		jobData, err := client.HGet(
				ctx,
				"job_data",
				jobID.(string),
			).Result()

			if err == redis.Nil {
				http.Error(w, "job not found", http.StatusNotFound)
				return
			}

			if err != nil {
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}

			var outputJob job.Job

			err = json.Unmarshal([]byte(jobData), &outputJob)
			if err != nil {
				http.Error(w, "invalid job data", http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(outputJob)
	}
}

func GetJobHandler(client *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

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