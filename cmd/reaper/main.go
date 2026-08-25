package main

import (
	"context"
	"distributed-job-platform/internal/job"
	"encoding/json"
	"fmt"
	"time"
	"github.com/redis/go-redis/v9"
)

func findExpiredJobs(ctx context.Context, client *redis.Client) []string {
	jobs, err := client.ZRangeByScore(ctx, "job_leases", &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", time.Now().UnixMilli()),
	}).Result()

	if err != nil {
		panic(err)
	}

	return jobs
}

func recoverJob(ctx context.Context, client *redis.Client, jobID string){
	val,err:= client.HGet(ctx,"active_jobs",jobID).Result()
	if(err==redis.Nil){
		return
	}else if(err!=nil){
		panic(err)
	}
	var newJobClaim job.JobClaim
	json.Unmarshal([]byte(val),&newJobClaim)

	res,err:=json.Marshal(newJobClaim.Job)
	if(err!=nil){
		panic(err)
	}
	_,err=client.LPush(ctx,"jobs",string(res)).Result()
	if(err!=nil){
		panic(err)
	}

	_,err=client.HDel(ctx, "active_jobs", newJobClaim.Job.ID).Result()
	if(err!=nil){
		panic(err)
	}
	_,err=client.ZRem(ctx, "job_leases", newJobClaim.Job.ID).Result()
	if(err!=nil){
		panic(err)
	}
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})
	ctx := context.Background()
	for {
		jobs:=findExpiredJobs(ctx, client)
		for _, val := range jobs {
    	recoverJob(ctx,client,val)
		}
		time.Sleep(time.Second)

	}
}
