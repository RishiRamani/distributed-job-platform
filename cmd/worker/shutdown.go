package main

import(
	"context"
	"fmt"
	"os"
)

func exitWorker(shutdown chan os.Signal, cancel context.CancelFunc){
	<-shutdown
	fmt.Println("Shutdown requested")
	cancel()
}