package app

import (
	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/delivery/zmq"
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
	routeStore := repository.NewART_RouteStore()
	for topic, nodeID := range bootstrapRoutes {
		routeStore.AddRoute(topic, nodeID)
	}

	codec := media.NewJSONCodec()
	compressor := media.NewLZ4Compressor()
	eventRouter := usecase.NewEventRouter(routeStore)
	router := zmq.NewRouter(cfg.ZmqBindAddress, cfg.ZmqPubAddress, eventRouter, codec, compressor)

	return &Runtime{
		RouteStore: routeStore,
		Router:     router,
	}
}
