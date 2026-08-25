package main

import (
	"encoding/json"
	"context"
	"fmt" 
	"net/http"
	"time"
	"github.com/redis/go-redis/v9"
)

type Job struct{
	ID		 string `json:"id"`
	Type	 string `json:"type"`
	Status string `json:"status"`
}

func healthHandler(w http.ResponseWriter, r *http.Request){
	fmt.Fprintln(w,"OK")
}

func createJobHandler(client *redis.Client) http.HandlerFunc{
	return func(w http.ResponseWriter, r *http.Request){
		job := Job{
			ID:			fmt.Sprintf("%d",time.Now().UnixNano()),
			Type: 	"sleep",
			Status: "queued",
		}

		ctx := context.Background()

		jsonData, err:= json.Marshal(job)
		if(err!=nil){
			panic(err)
		}
		
		res1,err:=client.LPush(ctx,"jobs",string(jsonData)).Result()
		if(err!=nil){
			panic(err)
		}else{
			w.Header().Set("Content-Type","application/json")
			json.NewEncoder(w).Encode(job)
		}

		fmt.Println(res1)
	}
}

func main() {
		http.HandleFunc("/health",healthHandler)
		
		
		fmt.Println("API Server Running on localhost:8080")

		client := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			Password: "",
			DB: 0,
			Protocol: 2,
		})
		
		http.HandleFunc("/jobs",createJobHandler(client))

		err := http.ListenAndServe(":8080",nil)
		if err!=nil{
			fmt.Println(err)
		}

		client.Close()
}