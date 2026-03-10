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
	Topic         string
}

// RouteStore defines the interface for a thread-safe, high-performance routing table.
// It's responsible for mapping topics to destination nodes.
type RouteStore interface {
	// AddRoute adds a new route to the table.
	AddRoute(topic string, destNodeID string)

	// Match finds the appropriate destination node ID for a given topic.
	Match(topic string) string
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
