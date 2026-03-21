package repository

import (
	"fmt"
	"sort"
	"sync"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

// ART_RouteStore is a thread-safe, high-performance routing table using Adaptive Radix Tree.
// It implements the domain.RouteStore interface.
type ART_RouteStore struct {
	tree    art.Tree
	routes  map[string]domain.Route
	catalog *FileRouteCatalog
	mu      sync.RWMutex
}

// NewART_RouteStore creates a new ART_RouteStore.
func NewART_RouteStore() *ART_RouteStore {
	return NewART_RouteStoreWithCatalog(nil)
}

// NewART_RouteStoreWithCatalog creates a new ART route store backed by an optional catalog.
func NewART_RouteStoreWithCatalog(catalog *FileRouteCatalog) *ART_RouteStore {
	return &ART_RouteStore{
		tree:    art.New(),
		routes:  make(map[string]domain.Route),
		catalog: catalog,
	}
}

// AddRoute adds a new direct route to the table.
func (r *ART_RouteStore) AddRoute(key domain.RouteKey, destNodeID string) error {
	return r.UpsertRoute(domain.Route{
		Pattern:       key.Topic,
		DestinationID: destNodeID,
		RouteType:     "direct",
		Enabled:       true,
		Tenant:        key.TenantID,
	})
}

// UpsertRoute inserts or updates a serializable route entry.
func (r *ART_RouteStore) UpsertRoute(route domain.Route) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	route = route.Normalize()
	key := routeIdentity(route)
	r.routes[key] = route
	r.rebuildTreeLocked()
	return r.persistLocked()
}

// RemoveRoute removes an exact route identity from the table.
func (r *ART_RouteStore) RemoveRoute(key domain.RouteKey, destNodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var removed bool
	for identity, route := range r.routes {
		if route.Pattern == key.Topic && route.Tenant == key.TenantID && route.DestinationID == destNodeID {
			delete(r.routes, identity)
			removed = true
		}
	}
	if !removed {
		return nil
	}

	r.rebuildTreeLocked()
	return r.persistLocked()
}

// Match finds the appropriate destination node ID for a given topic.
// For now, it performs an exact match.
func (r *ART_RouteStore) Match(key domain.RouteKey) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	value, found := r.tree.Search(art.Key(partitionedTopicKey(key.TenantID, key.Topic)))
	if found {
		return value.(string)
	}

	return ""
}

// Routes returns a stable snapshot of the current route set.
func (r *ART_RouteStore) Routes() []domain.Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshotRoutesLocked()
}

// Restore replaces the in-memory route set from a versioned snapshot.
func (r *ART_RouteStore) Restore(snapshot domain.RouteCatalogSnapshot) error {
	if snapshot.Version == 0 {
		snapshot.Version = domain.RouteCatalogVersion
	}
	if err := domain.ValidateSnapshotVersion(snapshot.Version); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.routes = make(map[string]domain.Route, len(snapshot.Routes))
	for _, route := range snapshot.Routes {
		route = route.Normalize()
		r.routes[routeIdentity(route)] = route
	}
	r.rebuildTreeLocked()
	return nil
}

func (r *ART_RouteStore) rebuildTreeLocked() {
	r.tree = art.New()
	for _, route := range r.routes {
		if !route.Enabled {
			continue
		}
		r.tree.Insert(art.Key(partitionedTopicKey(route.Tenant, route.Pattern)), route.DestinationID)
	}
}

func (r *ART_RouteStore) snapshotRoutesLocked() []domain.Route {
	routes := make([]domain.Route, 0, len(r.routes))
	for _, route := range r.routes {
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool {
		return routeIdentity(routes[i]) < routeIdentity(routes[j])
	})
	return routes
}

func (r *ART_RouteStore) persistLocked() error {
	if r.catalog == nil {
		return nil
	}
	return r.catalog.Save(domain.RouteCatalogSnapshot{Version: domain.RouteCatalogVersion, Routes: r.snapshotRoutesLocked()})
}

func routeIdentity(route domain.Route) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%t\x00%s", route.Pattern, route.DestinationID, route.RouteType, route.Priority, route.Enabled, route.Tenant)
}

func partitionedTopicKey(tenantID, topic string) string {
	return fmt.Sprintf("%s\x00%s", tenantID, topic)
}
