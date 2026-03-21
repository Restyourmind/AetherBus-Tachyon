package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

var errRouteCatalogNotFound = errors.New("route catalog not found")

// FileRouteCatalog persists route snapshots as versioned JSON files.
type FileRouteCatalog struct {
	path string
}

func NewFileRouteCatalog(path string) *FileRouteCatalog {
	return &FileRouteCatalog{path: path}
}

func (c *FileRouteCatalog) Path() string {
	return c.path
}

func (c *FileRouteCatalog) Load() (domain.RouteCatalogSnapshot, error) {
	if c == nil || c.path == "" {
		return domain.RouteCatalogSnapshot{Version: domain.RouteCatalogVersion}, nil
	}

	data, err := os.ReadFile(c.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.RouteCatalogSnapshot{Version: domain.RouteCatalogVersion}, errRouteCatalogNotFound
		}
		return domain.RouteCatalogSnapshot{}, fmt.Errorf("read route catalog: %w", err)
	}

	var snapshot domain.RouteCatalogSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return domain.RouteCatalogSnapshot{}, fmt.Errorf("decode route catalog: %w", err)
	}
	if err := domain.ValidateSnapshotVersion(snapshot.Version); err != nil {
		return domain.RouteCatalogSnapshot{}, fmt.Errorf("validate route catalog version: %w", err)
	}

	sort.Slice(snapshot.Routes, func(i, j int) bool {
		left := routeKey(snapshot.Routes[i])
		right := routeKey(snapshot.Routes[j])
		return left < right
	})

	return snapshot, nil
}

func (c *FileRouteCatalog) Save(snapshot domain.RouteCatalogSnapshot) error {
	if c == nil || c.path == "" {
		return nil
	}
	if snapshot.Version == 0 {
		snapshot.Version = domain.RouteCatalogVersion
	}
	if err := domain.ValidateSnapshotVersion(snapshot.Version); err != nil {
		return fmt.Errorf("validate route catalog version: %w", err)
	}

	sort.Slice(snapshot.Routes, func(i, j int) bool {
		return routeKey(snapshot.Routes[i]) < routeKey(snapshot.Routes[j])
	})

	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return fmt.Errorf("create route catalog directory: %w", err)
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode route catalog: %w", err)
	}

	tmpPath := c.path + ".tmp"
	if err := os.WriteFile(tmpPath, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write route catalog temp file: %w", err)
	}
	if err := os.Rename(tmpPath, c.path); err != nil {
		return fmt.Errorf("replace route catalog: %w", err)
	}
	return nil
}

func routeKey(route domain.Route) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%t\x00%s", route.Pattern, route.DestinationID, route.RouteType, route.Priority, route.Enabled, route.Tenant)
}

func ErrRouteCatalogNotFound() error {
	return errRouteCatalogNotFound
}
