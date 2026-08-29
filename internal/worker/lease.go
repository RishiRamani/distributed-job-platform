package worker

import (
	"context"
	"fmt"
	"time"
	"github.com/redis/go-redis/v9"
)

func RenewLease(
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
			renewTime := time.Now().Add(30 * time.Second).UnixMilli()
			script := redis.NewScript(`
					local owner = redis.call("HGET",KEYS[1],ARGV[1])
					if owner==ARGV[2] then
						redis.call("ZADD",KEYS[2],ARGV[3],ARGV[1])
						return 1
					else
						return 0
					end
			`)

			result, err := script.Run(
				ctx,
				client,
				[]string{
					"active_jobs",
					"job_leases",
				},
				jobID,
				workerID,
				renewTime,
			).Result()

			if err != nil {
				panic(err)
			}

			if result == 0{
				fmt.Println("Not the owner anymore")
				return
			}
		}
	}
}