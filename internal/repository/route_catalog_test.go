package repository

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

func TestFileRouteCatalogEmptyCatalog(t *testing.T) {
	catalog := NewFileRouteCatalog(filepath.Join(t.TempDir(), "routes.json"))

	snapshot, err := catalog.Load()
	if err == nil {
		t.Fatal("expected missing catalog error")
	}
	if snapshot.Version != domain.RouteCatalogVersion {
		t.Fatalf("expected default version %d, got %d", domain.RouteCatalogVersion, snapshot.Version)
	}
	if len(snapshot.Routes) != 0 {
		t.Fatalf("expected no routes, got %d", len(snapshot.Routes))
	}
}

func TestFileRouteCatalogCorruptedCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.json")
	if err := os.WriteFile(path, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write corrupted catalog: %v", err)
	}

	_, err := NewFileRouteCatalog(path).Load()
	if err == nil {
		t.Fatal("expected decode error for corrupted catalog")
	}
}

func TestARTStoreDeduplicatesDuplicateRoutesInSnapshot(t *testing.T) {
	store := NewART_RouteStore()
	err := store.Restore(domain.RouteCatalogSnapshot{
		Version: domain.RouteCatalogVersion,
		Routes: []domain.Route{
			{Pattern: "orders.created", DestinationID: "node-1", RouteType: "direct", Priority: 5, Enabled: true},
			{Pattern: "orders.created", DestinationID: "node-1", RouteType: "direct", Priority: 5, Enabled: true},
		},
	})
	if err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	routes := store.Routes()
	if len(routes) != 1 {
		t.Fatalf("expected duplicate route collapse, got %d routes", len(routes))
	}
	if got := store.Match(domain.RouteKey{Topic: "orders.created"}); got != "node-1" {
		t.Fatalf("expected restored route to match node-1, got %q", got)
	}
}

func TestARTStorePersistsMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.json")
	store := NewART_RouteStoreWithCatalog(NewFileRouteCatalog(path))

	if err := store.UpsertRoute(domain.Route{Pattern: "orders.created", DestinationID: "node-1", RouteType: "direct", Priority: 10, Enabled: true, Tenant: "tenant-a"}); err != nil {
		t.Fatalf("upsert route: %v", err)
	}
	if err := store.RemoveRoute(domain.RouteKey{TenantID: "tenant-a", Topic: "orders.created"}, "node-1"); err != nil {
		t.Fatalf("remove route: %v", err)
	}
	if err := store.AddRoute(domain.RouteKey{Topic: "orders.updated"}, "node-2"); err != nil {
		t.Fatalf("add route: %v", err)
	}

	snapshot, err := NewFileRouteCatalog(path).Load()
	if err != nil {
		t.Fatalf("load persisted catalog: %v", err)
	}
	if snapshot.Version != domain.RouteCatalogVersion {
		t.Fatalf("expected persisted version %d, got %d", domain.RouteCatalogVersion, snapshot.Version)
	}
	if len(snapshot.Routes) != 1 {
		t.Fatalf("expected 1 persisted route, got %d", len(snapshot.Routes))
	}
	if snapshot.Routes[0].Pattern != "orders.updated" || snapshot.Routes[0].DestinationID != "node-2" {
		t.Fatalf("unexpected persisted route: %+v", snapshot.Routes[0])
	}
}

func TestARTStoreIsolatesRoutesAcrossTenants(t *testing.T) {
	store := NewART_RouteStore()
	if err := store.UpsertRoute(domain.Route{Pattern: "orders.created", DestinationID: "node-a", RouteType: "direct", Enabled: true, Tenant: "tenant-a"}); err != nil {
		t.Fatalf("upsert tenant-a route: %v", err)
	}
	if err := store.UpsertRoute(domain.Route{Pattern: "orders.created", DestinationID: "node-b", RouteType: "direct", Enabled: true, Tenant: "tenant-b"}); err != nil {
		t.Fatalf("upsert tenant-b route: %v", err)
	}

	if got := store.Match(domain.RouteKey{TenantID: "tenant-a", Topic: "orders.created"}); got != "node-a" {
		t.Fatalf("expected tenant-a isolation, got %q", got)
	}
	if got := store.Match(domain.RouteKey{TenantID: "tenant-b", Topic: "orders.created"}); got != "node-b" {
		t.Fatalf("expected tenant-b isolation, got %q", got)
	}
	if got := store.Match(domain.RouteKey{TenantID: "tenant-c", Topic: "orders.created"}); got != "" {
		t.Fatalf("expected no cross-tenant route leakage, got %q", got)
	}
}
