package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aetherbus/aetherbus-tachyon/internal/delivery/zmq"
	"github.com/aetherbus/aetherbus-tachyon/internal/repository"
	"github.com/aetherbus/aetherbus-tachyon/internal/usecase"
)

func main() {
	// 1. Create Dependencies (Wiring)

	// Create the routing table repository
	routeStore := repository.NewART_RouteStore()
	fmt.Println("Initialized Adaptive Radix Tree Route Store.")

	// Populate with some dummy routes for demonstration
	routeStore.AddRoute("telemetry.sensor.alpha", "node-alpha-1")
	routeStore.AddRoute("telemetry.sensor.beta", "node-beta-1")
	routeStore.AddRoute("logs.system", "node-logger-1")
	fmt.Println("Populated dummy routes.")

	// Create the core application logic (use case)
	eventRouter := usecase.NewEventRouter(routeStore)
	fmt.Println("Initialized Event Router use case.")

	// 2. Start Delivery Layers

	// Create and start the internal ZeroMQ router
	zmqRouter := zmq.NewRouter("tcp://*:5555", eventRouter)

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	if err := zmqRouter.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start ZMQ Router: %v\n", err)
		os.Exit(1)
	}

	// 3. Wait for shutdown signal
	fmt.Println("AetherBus Node is running. Press Ctrl+C to exit.")

	// Set up a channel to listen for OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal is received
	<-sigChan

	// 4. Graceful Shutdown
	fmt.Println("\nReceived shutdown signal. Gracefully stopping...")
	cancel() // Cancel the context to stop the ZMQ router

	// Add a small delay to allow components to shut down
	// time.Sleep(200 * time.Millisecond)

	fmt.Println("AetherBus Node stopped.")
}
