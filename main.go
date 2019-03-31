package main

import (
	"backend-nats/backend"
	"os"
	"os/signal"
)

func main() {
	backendService := backend.GetBackendInstance()
	go backendService.Serve()
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint
	backendService.Stop()
}
