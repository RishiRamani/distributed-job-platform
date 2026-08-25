package main

import(
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"context"
	"time"
	"distributed-job-platform/internal/job"
)

func executeJob(newJob job.Job){
	if(newJob.Type=="sleep"){
		time.Sleep(time.Duration(newJob.Duration)*time.Second)
	}
	fmt.Println(newJob)
}

func main(){
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
		Protocol: 2,
	})

	ctx := context.Background()

	timeout := 0*time.Second

	for{
		res1,err := client.BRPop(ctx,timeout,"jobs").Result()

		if(err==redis.Nil){
			continue
		}else if(err!=nil){
			panic(err)
		}else{
			var newJob job.Job
			err := json.Unmarshal([]byte(res1[1]),&newJob)

			if(err!=nil){
				panic(err)
			}
			executeJob(newJob)
		}
	}
	
}