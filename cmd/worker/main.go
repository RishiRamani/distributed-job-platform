package main

import(
	"fmt"
	"github.com/redis/go-redis/v9"
	"context"
)

func main(){
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
		Protocol: 2,
	})
	ctx := context.Background()
	for{
		res1,err := client.RPop(ctx,"jobs").Result()
		if(err==redis.Nil){
			continue
		}else if(err!=nil){
			panic(err)
		}else{
			fmt.Println(res1)
		}
	}
	
}