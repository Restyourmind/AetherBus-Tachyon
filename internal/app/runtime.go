package app

import (
	"time"

	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/delivery/zmq"
	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/media"
	"github.com/aetherbus/aetherbus-tachyon/internal/repository"
	"github.com/aetherbus/aetherbus-tachyon/internal/usecase"
)

// Runtime contains the wired application components used by command entrypoints.
type Runtime struct {
	RouteStore *repository.ART_RouteStore
	Router     *zmq.Router
}

// NewRuntime wires the core Tachyon runtime from config and bootstrap routes.
func NewRuntime(cfg *config.Config, bootstrapRoutes map[string]string) *Runtime {
	return NewRuntimeWithCompressor(cfg, bootstrapRoutes, media.NewLZ4Compressor())
}

// NewRuntimeWithCompressor wires the core Tachyon runtime with an explicit compressor.
func NewRuntimeWithCompressor(cfg *config.Config, bootstrapRoutes map[string]string, compressor domain.Compressor) *Runtime {
	routeStore := repository.NewART_RouteStore()
	for topic, nodeID := range bootstrapRoutes {
		routeStore.AddRoute(topic, nodeID)
	}

	codec := media.NewJSONCodec()
	eventRouter := usecase.NewEventRouter(routeStore)
	var durability zmq.WAL
	if cfg.WALEnabled {
		durability = zmq.NewFileWAL(cfg.WALPath)
	}

	router := zmq.NewRouterWithDurability(
		cfg.ZmqBindAddress,
		cfg.ZmqPubAddress,
		eventRouter,
		codec,
		compressor,
		3,
		time.Duration(cfg.DeliveryTimeoutMS)*time.Millisecond,
		durability,
	)
	router.SetMaxInflightPerConsumer(cfg.MaxInflightPerConsumer)
	router.SetQueueBounds(cfg.MaxPerTopicQueue, cfg.MaxQueuedDirect)
	router.SetGlobalIngressLimit(cfg.MaxGlobalIngress)

	return &Runtime{
		RouteStore: routeStore,
		Router:     router,
	}
}

// NewBenchmarkRuntime wires runtime for benchmark scenarios and allows compression toggle.
func NewBenchmarkRuntime(cfg *config.Config, bootstrapRoutes map[string]string, compress bool) *Runtime {
	if compress {
		return NewRuntimeWithCompressor(cfg, bootstrapRoutes, media.NewLZ4Compressor())
	}

	return NewRuntimeWithCompressor(cfg, bootstrapRoutes, media.NewNoopCompressor())
}
