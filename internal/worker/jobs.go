package worker

import (
	"context"
	"distributed-job-platform/internal/job"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

func CompleteJob(ctx context.Context,client *redis.Client,newJob job.Job,workerID string)(bool,error){
	newJob.Status = "completed"

	jobData, err := json.Marshal(newJob)
	if err != nil {
		return false,err
	}

	script := redis.NewScript(`
			local owner = redis.call("HGET",KEYS[1],ARGV[1])
			if owner==ARGV[2] then
				redis.call("HSET",KEYS[2],ARGV[1],ARGV[3])
				redis.call("HDEL",KEYS[1],ARGV[1])
				redis.call("ZREM",KEYS[3],ARGV[1])
				return 1
			else
				return 0
			end
	`)

	res, err := script.Run(
		ctx,
		client,
		[]string{
			"active_jobs",
			"job_data",
			"job_leases",
		},
		newJob.ID,
		workerID,
		string(jobData),
	).Int64()
	if err != nil {
    return false, err
}

	return res == 1, nil
}

func FailJob(ctx context.Context,client *redis.Client,newJob job.Job,workerID string)(bool,error){
	newJob.Status = "failed"

	jobData, err := json.Marshal(newJob)
	if err != nil {
		return false,err
	}

	script := redis.NewScript(`
			local owner = redis.call("HGET",KEYS[1],ARGV[1])
			if owner==ARGV[2] then
				redis.call("HSET",KEYS[2],ARGV[1],ARGV[3])
				redis.call("HDEL",KEYS[1],ARGV[1])
				redis.call("ZREM",KEYS[3],ARGV[1])
				return 1
			else
				return 0
			end
	`)

	res, err := script.Run(
		ctx,
		client,
		[]string{
			"active_jobs",
			"job_data",
			"job_leases",
		},
		newJob.ID,
		workerID,
		string(jobData),
	).Int64()
	if err != nil {
			return false, err
	}

	return res == 1, nil
}

func GetJob(ctx context.Context, client *redis.Client) (job.Job, error) {
	res, err := client.BRPop(ctx, time.Second, "jobs").Result()
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

func ClaimJob(
	ctx context.Context,
	client *redis.Client,
	newJob job.Job,
	workerID string,
) error {

	newJob.Status = "running"

	jobData, err := json.Marshal(newJob)
	if err != nil {
		return err
	}

	expiry := time.Now().Add(30 * time.Second).UnixMilli()

	script := redis.NewScript(`
			redis.call("HSET",KEYS[1],ARGV[1],ARGV[2])
			redis.call("HSET",KEYS[2],ARGV[1],ARGV[3])
			redis.call("ZADD",KEYS[3],ARGV[4],ARGV[1])
			return 1
	`)

	_, err = script.Run(
		ctx,
		client,
		[]string{
			"job_data",
			"active_jobs",
			"job_leases",
		},
		newJob.ID,
		string(jobData),
		workerID,
		expiry,
	).Result()

	return err
}

func Requeue(
	ctx context.Context,
	client *redis.Client,
	newJob job.Job,
	workerID string,
) (bool,error){
	newJob.Retries++;
	newJob.Status = "queued"

	jobData, err := json.Marshal(newJob)
	if err != nil {
		return false,err
	}

	script := redis.NewScript(`
			local owner = redis.call("HGET",KEYS[1],ARGV[1])
			if owner==ARGV[2] then
				redis.call("HSET",KEYS[2],ARGV[1],ARGV[3])
				redis.call("LPUSH",KEYS[3],ARGV[1])
				redis.call("HDEL",KEYS[1],ARGV[1])
				redis.call("ZREM",KEYS[4],ARGV[1])
				return 1
			else
				return 0
			end
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
		newJob.ID,
		workerID,
		string(jobData),
	).Int64()

	if err != nil {
			return false, err
	}

	return res == 1, nil
}
