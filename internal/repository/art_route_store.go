package repository

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/exporter"
	art "github.com/plar/go-adaptive-radix-tree"
)

type ART_RouteStore struct {
	tree      art.Tree
	routes    map[string]domain.Route
	catalog   *FileRouteCatalog
	exporter  exporter.Exporter
	exportSeq uint64
	mu        sync.RWMutex
}

func NewART_RouteStore() *ART_RouteStore { return NewART_RouteStoreWithCatalog(nil) }
func NewART_RouteStoreWithCatalog(catalog *FileRouteCatalog) *ART_RouteStore {
	return &ART_RouteStore{tree: art.New(), routes: make(map[string]domain.Route), catalog: catalog}
}
func (r *ART_RouteStore) SetExporter(ex exporter.Exporter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exporter = ex
}
func (r *ART_RouteStore) AddRoute(key domain.RouteKey, destNodeID string) error {
	return r.UpsertRoute(domain.Route{Pattern: key.Topic, DestinationID: destNodeID, RouteType: domain.RouteTypeDirect, Enabled: true, Tenant: key.TenantID})
}
func (r *ART_RouteStore) UpsertRoute(route domain.Route) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	route = route.Normalize()
	if err := domain.ValidateRoutePattern(route.Pattern); err != nil {
		return err
	}
	r.routes[routeIdentity(route)] = route
	r.rebuildTreeLocked()
	if err := r.persistLocked(); err != nil {
		return err
	}
	r.emitRouteEventLocked("upsert", route)
	return nil
}
func (r *ART_RouteStore) RemoveRoute(key domain.RouteKey, destNodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := false
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
	if err := r.persistLocked(); err != nil {
		return err
	}
	r.emitRouteEventLocked("remove", domain.Route{Pattern: key.Topic, DestinationID: destNodeID, RouteType: domain.RouteTypeDirect, Enabled: false, Tenant: key.TenantID})
	return nil
}
func (r *ART_RouteStore) Resolve(key domain.RouteKey) []domain.ResolvedDestination {
	r.mu.RLock()
	defer r.mu.RUnlock()
	type routeMatch struct {
		domain.Route
		class int
	}
	var matches []routeMatch
	for _, route := range r.routes {
		if !route.Enabled || route.Tenant != key.TenantID {
			continue
		}
		class, ok := classifyMatch(route.Pattern, key.Topic)
		if !ok {
			continue
		}
		matches = append(matches, routeMatch{Route: route, class: class})
	}
	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].class != matches[j].class {
			return matches[i].class < matches[j].class
		}
		if matches[i].Priority != matches[j].Priority {
			return matches[i].Priority > matches[j].Priority
		}
		if matches[i].Pattern != matches[j].Pattern {
			return matches[i].Pattern < matches[j].Pattern
		}
		if matches[i].DestinationID != matches[j].DestinationID {
			return matches[i].DestinationID < matches[j].DestinationID
		}
		return matches[i].RouteType < matches[j].RouteType
	})
	out := make([]domain.ResolvedDestination, 0, len(matches))
	for _, match := range matches {
		out = append(out, domain.ResolvedDestination{Pattern: match.Pattern, RouteType: match.RouteType, DestinationID: match.DestinationID, Priority: match.Priority})
	}
	return out
}
func (r *ART_RouteStore) Match(key domain.RouteKey) string {
	resolved := r.Resolve(key)
	if len(resolved) == 0 {
		return ""
	}
	return resolved[0].DestinationID
}
func (r *ART_RouteStore) Routes() []domain.Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshotRoutesLocked()
}
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
		if err := domain.ValidateRoutePattern(route.Pattern); err != nil {
			return err
		}
		r.routes[routeIdentity(route)] = route
	}
	r.rebuildTreeLocked()
	for _, route := range snapshot.Routes {
		r.emitRouteEventLocked("replay", route)
	}
	return nil
}
func (r *ART_RouteStore) rebuildTreeLocked() {
	r.tree = art.New()
	for _, route := range r.routes {
		if route.Enabled {
			r.tree.Insert(art.Key(partitionedTopicKey(route.Tenant, route.Pattern)), route.DestinationID)
		}
	}
}
func (r *ART_RouteStore) snapshotRoutesLocked() []domain.Route {
	routes := make([]domain.Route, 0, len(r.routes))
	for _, route := range r.routes {
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool { return routeIdentity(routes[i]) < routeIdentity(routes[j]) })
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
func (r *ART_RouteStore) emitRouteEventLocked(action string, route domain.Route) {
	if r.exporter == nil {
		return
	}
	r.exportSeq++
	route = route.Normalize()
	r.exporter.Emit(exporter.Event{Kind: "route", Action: action, Source: "route_store", Cursor: exporter.Cursor("route_store", r.exportSeq), OccurredAt: time.Now().UTC(), TenantID: route.Tenant, Topic: route.Pattern, RouteKey: routeIdentity(route), DestinationID: route.DestinationID, Status: route.RouteType, Labels: map[string]string{"enabled": fmt.Sprintf("%t", route.Enabled)}})
}
func classifyMatch(pattern, topic string) (int, bool) {
	if domain.ValidateRoutePattern(pattern) != nil {
		return 0, false
	}
	if pattern == topic {
		return 0, true
	}
	pp := strings.Split(pattern, ".")
	tp := strings.Split(topic, ".")
	class := 0
	for i := 0; i < len(pp); i++ {
		if i >= len(tp) {
			return 0, false
		}
		switch pp[i] {
		case "*":
			if class < 1 {
				class = 1
			}
		case ">":
			if i != len(pp)-1 || i >= len(tp) {
				return 0, false
			}
			return 2, true
		default:
			if pp[i] != tp[i] {
				return 0, false
			}
		}
	}
	if len(pp) != len(tp) {
		return 0, false
	}
	return class, true
}
