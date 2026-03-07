// cmd/tachyon/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/app"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime := app.NewRuntime(cfg, map[string]string{
		"user.created": "node-1",
	})
	zmqRouter := runtime.Router

	// Start the ZMQ router in a goroutine
	if err := zmqRouter.Start(ctx); err != nil {
		fmt.Printf("Failed to start ZMQ router: %v\n", err)
		os.Exit(1)
	}

	// Wait for termination signals
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-signalCh:
		fmt.Printf("Received signal: %s. Shutting down...\n", sig)
		cancel() // Trigger context cancellation
	case <-ctx.Done():
		// Context was cancelled from somewhere else
	}

	// In a real app, you might wait here for services to shut down gracefully.
	fmt.Println("AetherBus-Tachyon server has stopped.")
}
