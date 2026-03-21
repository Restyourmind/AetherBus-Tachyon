package repository

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/statefile"
)

var errRouteCatalogNotFound = errors.New("route catalog not found")

// FileRouteCatalog persists route snapshots as versioned JSON files.
type FileRouteCatalog struct {
	store *statefile.FileStateStore[[]domain.Route]
}

func NewFileRouteCatalog(path string) *FileRouteCatalog {
	return &FileRouteCatalog{store: statefile.NewFileStateStore(path, domain.RouteCatalogVersion, func() []domain.Route { return nil })}
}

func (c *FileRouteCatalog) Path() string {
	if c == nil || c.store == nil {
		return ""
	}
	return c.store.Path()
}

func (c *FileRouteCatalog) Load() (domain.RouteCatalogSnapshot, error) {
	if c == nil || c.store == nil || c.store.Path() == "" {
		return domain.RouteCatalogSnapshot{Version: domain.RouteCatalogVersion}, nil
	}
	envelope, err := c.store.Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.RouteCatalogSnapshot{Version: domain.RouteCatalogVersion}, errRouteCatalogNotFound
		}
		return domain.RouteCatalogSnapshot{}, err
	}
	if err := domain.ValidateSnapshotVersion(envelope.Version); err != nil {
		return domain.RouteCatalogSnapshot{}, fmt.Errorf("validate route catalog version: %w", err)
	}
	routes := append([]domain.Route(nil), envelope.Data...)
	for i := range routes {
		routes[i] = routes[i].Normalize()
		if err := domain.ValidateRoutePattern(routes[i].Pattern); err != nil {
			return domain.RouteCatalogSnapshot{}, fmt.Errorf("validate route pattern %q: %w", routes[i].Pattern, err)
		}
	}
	sort.Slice(routes, func(i, j int) bool { return routeKey(routes[i]) < routeKey(routes[j]) })
	return domain.RouteCatalogSnapshot{Version: envelope.Version, Routes: routes}, nil
}

func (c *FileRouteCatalog) Save(snapshot domain.RouteCatalogSnapshot) error {
	if c == nil || c.store == nil || c.store.Path() == "" {
		return nil
	}
	if snapshot.Version == 0 {
		snapshot.Version = domain.RouteCatalogVersion
	}
	if err := domain.ValidateSnapshotVersion(snapshot.Version); err != nil {
		return fmt.Errorf("validate route catalog version: %w", err)
	}
	routes := append([]domain.Route(nil), snapshot.Routes...)
	for i := range routes {
		routes[i] = routes[i].Normalize()
		if err := domain.ValidateRoutePattern(routes[i].Pattern); err != nil {
			return fmt.Errorf("validate route pattern %q: %w", routes[i].Pattern, err)
		}
	}
	sort.Slice(routes, func(i, j int) bool { return routeKey(routes[i]) < routeKey(routes[j]) })
	return c.store.Save(routes)
}

func routeKey(route domain.Route) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%t\x00%s", route.Pattern, route.DestinationID, route.RouteType, route.Priority, route.Enabled, route.Tenant)
}

func ErrRouteCatalogNotFound() error { return errRouteCatalogNotFound }
