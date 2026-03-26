package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/delivery/zmq"
	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
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

	_ = routeStore.AddRoute(domain.RouteKey{Topic: "telemetry.sensor.alpha"}, "node-alpha-1")
	_ = routeStore.AddRoute(domain.RouteKey{Topic: "telemetry.sensor.beta"}, "node-beta-1")
	_ = routeStore.AddRoute(domain.RouteKey{Topic: "logs.system"}, "node-logger-1")
	fmt.Println("Populated dummy routes.")

	codec := media.NewJSONCodec()
	compressor := media.NewLZ4Compressor()
	eventRouter := usecase.NewEventRouter(routeStore)
	fmt.Println("Initialized Event Router use case.")

	var durability zmq.WAL
	if cfg.WALEnabled {
		durability = zmq.NewFileWAL(cfg.WALPath)
	}

	zmqRouter := zmq.NewRouterWithDurability(
		cfg.ZmqBindAddress,
		cfg.ZmqPubAddress,
		eventRouter,
		codec,
		compressor,
		3,
		time.Duration(cfg.DeliveryTimeoutMS)*time.Millisecond,
		durability,
	)

	zmqRouter.SetMaxInflightPerConsumer(cfg.MaxInflightPerConsumer)
	zmqRouter.SetQueueBounds(cfg.MaxPerTopicQueue, cfg.MaxQueuedDirect)
	zmqRouter.SetGlobalIngressLimit(cfg.MaxGlobalIngress)
	zmqRouter.SetPriorityPolicy(
		cfg.SupportedPriorityClasses,
		cfg.PriorityClassWeights,
		cfg.PriorityPreemption,
		cfg.PriorityBoostThreshold,
		cfg.PriorityBoostOffset,
	)
	zmqRouter.SetQueueLimitPolicy(zmq.QueueLimitPolicy{
		Enabled:                     cfg.QueueLimitPolicy.Enabled,
		EvaluationInterval:          time.Duration(cfg.QueueLimitPolicy.EvaluationIntervalMS) * time.Millisecond,
		MinHoldTime:                 time.Duration(cfg.QueueLimitPolicy.MinHoldTimeMS) * time.Millisecond,
		MemoryLimitBytes:            cfg.QueueLimitPolicy.MemoryLimitBytes,
		RetryRateHighWatermark:      cfg.QueueLimitPolicy.RetryRateHighWatermark,
		QueueGrowthHighWatermark:    cfg.QueueLimitPolicy.QueueGrowthHighWatermark,
		ConsumerLagHighWatermark:    cfg.QueueLimitPolicy.ConsumerLagHighWatermark,
		MemoryPressureHighWatermark: cfg.QueueLimitPolicy.MemoryPressureHighWatermark,
		InflightStep:                cfg.QueueLimitPolicy.InflightStep,
		QueueStep:                   cfg.QueueLimitPolicy.QueueStep,
		MinInflightPerConsumer:      cfg.QueueLimitPolicy.MinInflightPerConsumer,
		MaxInflightPerConsumer:      cfg.QueueLimitPolicy.MaxInflightPerConsumer,
		MinPerTopicQueue:            cfg.QueueLimitPolicy.MinPerTopicQueue,
		MaxPerTopicQueue:            cfg.QueueLimitPolicy.MaxPerTopicQueue,
		AdaptivePriorityWeights:     cfg.QueueLimitPolicy.AdaptivePriorityWeights,
		AdaptiveStep:                cfg.QueueLimitPolicy.AdaptiveStep,
		AgingBoostAfter:             time.Duration(cfg.QueueLimitPolicy.AgingBoostAfterMS) * time.Millisecond,
		ClassCircuitBreaker:         cfg.QueueLimitPolicy.ClassCircuitBreaker,
		ClassBreakerQueueFraction:   cfg.QueueLimitPolicy.ClassBreakerQueueFraction,
	})

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
