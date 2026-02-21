package usecase

import (
	"context"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

// EventRouter is the core application logic for routing events.
// It orchestrates the interaction between the delivery layer and the repository layer.
type EventRouter struct {
	routeStore domain.RouteStore
	// In a real implementation, this would likely be a more complex
	// component for inter-node communication (e.g., a ZMQ publisher).
	// For this example, we'll keep it simple.
}

// NewEventRouter creates a new EventRouter.
func NewEventRouter(routeStore domain.RouteStore) *EventRouter {
	return &EventRouter{
		routeStore: routeStore,
	}
}

// Publish implements the domain.EventPublisher interface.
// It receives an event, finds the destination, and "sends" it.
func (r *EventRouter) Publish(ctx context.Context, event domain.Event) error {
	// 1. Match the topic to a destination node ID.
	destNodeID := r.routeStore.Match(event.Topic)
	if destNodeID == "" {
		// Handle case where no route is found
		// This could be logging, returning an error, or sending to a default handler.
		return nil // Or an error like `ErrNoRouteFound`
	}

	// 2. In a real system, you would now use the destNodeID to
	//    look up the node's address and publish the event to it
	//    over the network (e.g., using the ZMQ DEALER-ROUTER pattern).
	//    For this example, we'll just simulate this action.

	// fmt.Printf("Routing event %s for topic %s to node %s\n", event.ID, event.Topic, destNodeID)

	return nil
}
