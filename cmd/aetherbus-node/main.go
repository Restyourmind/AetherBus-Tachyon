package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/delivery/zmq"
	"github.com/aetherbus/aetherbus-tachyon/internal/media"
	"github.com/aetherbus/aetherbus-tachyon/internal/repository"
	"github.com/aetherbus/aetherbus-tachyon/internal/usecase"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	routeStore := repository.NewART_RouteStore()
	fmt.Println("Initialized Adaptive Radix Tree Route Store.")

	routeStore.AddRoute("telemetry.sensor.alpha", "node-alpha-1")
	routeStore.AddRoute("telemetry.sensor.beta", "node-beta-1")
	routeStore.AddRoute("logs.system", "node-logger-1")
	fmt.Println("Populated dummy routes.")

	codec := media.NewJSONCodec()
	compressor := media.NewLZ4Compressor()
	eventRouter := usecase.NewEventRouter(routeStore)
	fmt.Println("Initialized Event Router use case.")

	zmqRouter := zmq.NewRouter(
		cfg.ZmqBindAddress,
		cfg.ZmqPubAddress,
		eventRouter,
		codec,
		compressor,
	)

	if err := zmqRouter.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start ZMQ Router: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf(
		"AetherBus Node is running. ROUTER=%s PUB=%s\nPress Ctrl+C to exit.\n",
		cfg.ZmqBindAddress,
		cfg.ZmqPubAddress,
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nReceived shutdown signal. Gracefully stopping...")
	cancel()
	fmt.Println("AetherBus Node stopped.")
}
