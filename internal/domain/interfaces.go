package domain

import "context"

// RouteStatus describes the routing decision for a publish attempt.
type RouteStatus string

const (
	RouteStatusRouted     RouteStatus = "routed"
	RouteStatusUnroutable RouteStatus = "unroutable"
)

type ResolvedDestination struct {
	Pattern       string
	RouteType     string
	DestinationID string
	Priority      int
}

// PublishResult captures the outcome of a routing decision.
type PublishResult struct {
	Status               RouteStatus
	DestinationID        string
	RouteType            string
	ResolvedDestinations []ResolvedDestination
	TenantID             string
	Topic                string
}

// RouteStore defines the interface for a thread-safe, high-performance routing table.
// It's responsible for mapping topics to destination nodes.
type RouteStore interface {
	// AddRoute adds a new direct route to the table.
	AddRoute(key RouteKey, destNodeID string) error

	// UpsertRoute inserts or updates a serializable route entry.
	UpsertRoute(route Route) error

	// RemoveRoute removes an exact route identity from the table.
	RemoveRoute(key RouteKey, destNodeID string) error

	// Routes returns a stable snapshot of the current route set.
	Routes() []Route

	// Resolve returns all matching destinations ordered by routing precedence.
	Resolve(key RouteKey) []ResolvedDestination

	// Match finds the highest-precedence destination node ID for a given topic.
	Match(key RouteKey) string
}

type EventPublisher interface {
	Publish(ctx context.Context, envelope Envelope) error
}

type EventPublisherWithResult interface {
	EventPublisher
	PublishWithResult(ctx context.Context, envelope Envelope) (PublishResult, error)
}
