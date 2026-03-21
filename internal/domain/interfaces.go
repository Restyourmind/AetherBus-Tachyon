package domain

import "context"

// RouteStatus describes the routing decision for a publish attempt.
type RouteStatus string

const (
	RouteStatusRouted     RouteStatus = "routed"
	RouteStatusUnroutable RouteStatus = "unroutable"
)

// PublishResult captures the outcome of a routing decision.
type PublishResult struct {
	Status        RouteStatus
	DestinationID string
	TenantID      string
	Topic         string
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

	// Match finds the appropriate destination node ID for a given topic.
	Match(key RouteKey) string
}

// EventPublisher defines the interface for publishing events to the bus.
// This is the primary entry point for the application logic (usecase) to send data.
type EventPublisher interface {
	Publish(ctx context.Context, envelope Envelope) error
}

// EventPublisherWithResult augments EventPublisher with structured routing outcomes.
// This keeps EventPublisher backward compatible for existing integrations.
type EventPublisherWithResult interface {
	EventPublisher
	PublishWithResult(ctx context.Context, envelope Envelope) (PublishResult, error)
}
