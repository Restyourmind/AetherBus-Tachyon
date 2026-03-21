package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/media"
	"github.com/aetherbus/aetherbus-tachyon/internal/repository"
)

func testRuntimeConfig(path string) *config.Config {
	return &config.Config{
		ZmqBindAddress:         "inproc://router",
		ZmqPubAddress:          "inproc://pub",
		DeliveryTimeoutMS:      1000,
		MaxInflightPerConsumer: 10,
		MaxPerTopicQueue:       10,
		MaxQueuedDirect:        10,
		MaxGlobalIngress:       10,
		RouteCatalogPath:       path,
	}
}

func TestRuntimeRestoresRoutesFromCatalogOnRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.json")
	catalog := repository.NewFileRouteCatalog(path)
	if err := catalog.Save(domain.RouteCatalogSnapshot{
		Version: domain.RouteCatalogVersion,
		Routes:  []domain.Route{{Pattern: "orders.created", DestinationID: "node-restored", RouteType: "direct", Priority: 7, Enabled: true, Tenant: "tenant-a"}},
	}); err != nil {
		t.Fatalf("seed route catalog: %v", err)
	}

	runtime := NewRuntimeWithCompressor(testRuntimeConfig(path), map[string]string{"orders.created": "bootstrap-node"}, media.NewNoopCompressor())

	if got := runtime.RouteStore.Match("orders.created"); got != "node-restored" {
		t.Fatalf("expected restored destination node-restored, got %q", got)
	}
	routes := runtime.RouteStore.Routes()
	if len(routes) != 1 || routes[0].Tenant != "tenant-a" || routes[0].Priority != 7 {
		t.Fatalf("unexpected restored routes: %+v", routes)
	}
}

func TestRuntimeFallsBackToBootstrapRoutesOnCorruptedCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.json")
	if err := os.WriteFile(path, []byte("corrupted-json"), 0o644); err != nil {
		t.Fatalf("write corrupted catalog: %v", err)
	}

	runtime := NewRuntimeWithCompressor(testRuntimeConfig(path), map[string]string{"orders.created": "bootstrap-node"}, media.NewNoopCompressor())

	if got := runtime.RouteStore.Match("orders.created"); got != "bootstrap-node" {
		t.Fatalf("expected bootstrap fallback destination, got %q", got)
	}
}
