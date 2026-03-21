package domain

import "errors"

const RouteCatalogVersion = 1

var ErrUnsupportedRouteCatalogVersion = errors.New("unsupported route catalog version")

// Route describes a serializable runtime route entry.
type Route struct {
	Pattern       string            `json:"pattern"`
	DestinationID string            `json:"destination_id"`
	RouteType     string            `json:"route_type"`
	Priority      int               `json:"priority"`
	Enabled       bool              `json:"enabled"`
	Tenant        string            `json:"tenant,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// RouteCatalogSnapshot is the versioned persistence shape for route catalogs.
type RouteCatalogSnapshot struct {
	Version int     `json:"version"`
	Routes  []Route `json:"routes"`
}

// Normalize applies defaults needed by the runtime and snapshot format.
func (r Route) Normalize() Route {
	if r.RouteType == "" {
		r.RouteType = "direct"
	}
	if !r.Enabled {
		return r
	}
	return r
}

// ValidateSnapshotVersion confirms whether the snapshot version is supported.
func ValidateSnapshotVersion(version int) error {
	if version != RouteCatalogVersion {
		return ErrUnsupportedRouteCatalogVersion
	}
	return nil
}
