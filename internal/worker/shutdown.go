package worker

import(
	"context"
	"fmt"
	"os"
)

func ExitWorker(shutdown chan os.Signal, cancel context.CancelFunc){
	<-shutdown
	fmt.Println("Shutdown requested")
	cancel()
}