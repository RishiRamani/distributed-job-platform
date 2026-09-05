package reaper

import (
	"context"
	"distributed-job-platform/internal/job"
	"encoding/json"
	"fmt"
	"time"
	"os"
	"github.com/redis/go-redis/v9"
)

func HandleShutdown(shutdown chan os.Signal,cancel context.CancelFunc){
	<-shutdown
	fmt.Println("Shutting down the reaper")
	cancel()
}

func FindExpiredJobs(
	ctx context.Context,
	client *redis.Client,
) ([]string,error) {
	jobs, err := client.ZRangeByScore(
		ctx,
		"job_leases",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf("%d", time.Now().UnixMilli()),
		},
	).Result()
	
	if(err!=nil){
		return nil,err
	}

	return jobs,nil
}

func RecoverJob(
	ctx context.Context,
	client *redis.Client,
	jobID string,
) (bool, error) {

	jobData, err := client.HGet(
		ctx,
		"job_data",
		jobID,
	).Result()

	if err == redis.Nil {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	var newJob job.Job

	err = json.Unmarshal(
		[]byte(jobData),
		&newJob,
	)
	if err != nil {
		return false, err
	}

	newJob.Status = "queued"

	updatedData, err := json.Marshal(newJob)
	if err != nil {
		return false, err
	}

	now := time.Now().UnixMilli()

	script := redis.NewScript(`
		local expiry = redis.call("ZSCORE", KEYS[4], ARGV[1])

		if not expiry then
			return 0
		end

		if tonumber(expiry) > tonumber(ARGV[3]) then
			return 0
		end

		local owner = redis.call("HGET", KEYS[1], ARGV[1])

		if not owner then
			return 0
		end

		redis.call("HSET", KEYS[2], ARGV[1], ARGV[2])
		redis.call("LPUSH", KEYS[3], ARGV[1])
		redis.call("HDEL", KEYS[1], ARGV[1])
		redis.call("ZREM", KEYS[4], ARGV[1])

		return 1
	`)

	res, err := script.Run(
		ctx,
		client,
		[]string{
			"active_jobs",
			"job_data",
			"jobs",
			"job_leases",
		},
		jobID,
		string(updatedData),
		now,
	).Int64()

	if err != nil {
		return false, err
	}

	return res == 1, nil
}