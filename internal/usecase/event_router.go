package usecase

import (
	"context"
	"fmt"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

// EventRouter is a use case that routes events to the appropriate destination.
// It uses a RouteStore to find the destination node ID for a given topic.
type EventRouter struct {
	routeStore domain.RouteStore
}

// NewEventRouter creates a new EventRouter.
func NewEventRouter(routeStore domain.RouteStore) *EventRouter {
	return &EventRouter{
		routeStore: routeStore,
	}
}

// Publish finds the destination node for the event and (for now) prints the routing decision.
func (er *EventRouter) Publish(ctx context.Context, envelope domain.Envelope) error {
	// For now, we'll just find the destination and print it.
	// In a real implementation, this would involve more complex logic,
	// such as forwarding the event to another node.
	destNodeID := er.routeStore.Match(envelope.Event.Topic)
	if destNodeID == "" {
		// If no specific route is found, it might be a broadcast
		// or an error, depending on the desired system behavior.
		fmt.Printf("No route found for topic: %s\n", envelope.Event.Topic)
		return nil // Or return an error
	}

	fmt.Printf("Routing event %s (topic: %s) from client %s to node %s\n",
		envelope.Event.ID, envelope.Event.Topic, string(envelope.ClientID), destNodeID)

	// In the future, this is where you would publish the event to the destination node.
	return nil
}
