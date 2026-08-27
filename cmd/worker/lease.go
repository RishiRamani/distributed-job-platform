package main

import (
	"context"
	"time"
	"github.com/redis/go-redis/v9"
)

func renewLease(
	ctx context.Context,
	client *redis.Client,
	jobID string,
	workerID string,
	done chan struct{},
) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return

		case <-ticker.C:

			isOwner,err:=ownershipOfJob(ctx,client,jobID,workerID)

			if(err!=nil){
				return
			}

			if(!isOwner){
				return
			}

			renewTime := time.Now().Add(30 * time.Second).UnixMilli()

			_, err = client.ZAdd(
				ctx,
				"job_leases",
				redis.Z{
					Score:  float64(renewTime),
					Member: jobID,
				},
			).Result()

			if err != nil {
				panic(err)
			}
		}
	}
}

func cleanupJob(
	ctx context.Context,
	client *redis.Client,
	jobID string,
	workerID string,
	done chan struct{},
) {

	isOwner,err:=ownershipOfJob(ctx,client,jobID,workerID)

	if(err!=nil){
		return
	}

	if(!isOwner){
		return
	}

	_, err = client.HDel(
		ctx,
		"active_jobs",
		jobID,
	).Result()
	if err != nil {
		panic(err)
	}

	_, err = client.ZRem(
		ctx,
		"job_leases",
		jobID,
	).Result()
	if err != nil {
		panic(err)
	}

	close(done)
}