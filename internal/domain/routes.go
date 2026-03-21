package domain

import (
	"errors"
	"strings"
)

const RouteCatalogVersion = 1

const (
	RouteTypeDirect   = "direct"
	RouteTypeFanout   = "fanout"
	RouteTypeBridge   = "bridge"
	RouteTypeInternal = "internal"
	RouteTypeSystem   = "system"
)

var (
	ErrUnsupportedRouteCatalogVersion = errors.New("unsupported route catalog version")
	ErrInvalidRoutePattern            = errors.New("invalid route pattern")
)

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

// RouteKey identifies a route within a tenant partition and topic namespace.
type RouteKey struct {
	TenantID string
	Topic    string
}

// RouteCatalogSnapshot is the versioned persistence shape for route catalogs.
type RouteCatalogSnapshot struct {
	Version int     `json:"version"`
	Routes  []Route `json:"routes"`
}

func ValidateRoutePattern(pattern string) error {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || strings.Contains(pattern, "..") {
		return ErrInvalidRoutePattern
	}
	parts := strings.Split(pattern, ".")
	hasSingleWildcard := false
	for i, part := range parts {
		if part == "" {
			return ErrInvalidRoutePattern
		}
		if part == "*" {
			hasSingleWildcard = true
		}
		if part == ">" {
			if i != len(parts)-1 || hasSingleWildcard {
				return ErrInvalidRoutePattern
			}
		}
		if strings.Contains(part, ">") && part != ">" {
			return ErrInvalidRoutePattern
		}
		if strings.Contains(part, "*") && part != "*" {
			return ErrInvalidRoutePattern
		}
	}
	return nil
}

// Normalize applies defaults needed by the runtime and snapshot format.
func (r Route) Normalize() Route {
	r.Pattern = strings.TrimSpace(r.Pattern)
	r.DestinationID = strings.TrimSpace(r.DestinationID)
	r.Tenant = strings.TrimSpace(r.Tenant)
	if r.RouteType == "" {
		r.RouteType = RouteTypeDirect
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
