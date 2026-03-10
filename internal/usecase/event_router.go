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

var _ domain.EventPublisher = (*EventRouter)(nil)
var _ domain.EventPublisherWithResult = (*EventRouter)(nil)

// NewEventRouter creates a new EventRouter.
func NewEventRouter(routeStore domain.RouteStore) *EventRouter {
	return &EventRouter{
		routeStore: routeStore,
	}
}

// Publish preserves the legacy publisher contract for existing callers.
func (er *EventRouter) Publish(ctx context.Context, envelope domain.Envelope) error {
	_, err := er.PublishWithResult(ctx, envelope)
	return err
}

// PublishWithResult returns a structured routing outcome for observability and policy layers.
func (er *EventRouter) PublishWithResult(ctx context.Context, envelope domain.Envelope) (domain.PublishResult, error) {
	_ = ctx
	result := domain.PublishResult{Topic: envelope.Event.Topic}

	destNodeID := er.routeStore.Match(envelope.Event.Topic)
	if destNodeID == "" {
		result.Status = domain.RouteStatusUnroutable
		fmt.Printf("Routing unroutable event %s (topic: %s) from client %s\n",
			envelope.Event.ID, envelope.Event.Topic, string(envelope.ClientID))
		return result, nil
	}

	result.Status = domain.RouteStatusRouted
	result.DestinationID = destNodeID
	fmt.Printf("Routing event %s (topic: %s) from client %s to node %s\n",
		envelope.Event.ID, envelope.Event.Topic, string(envelope.ClientID), destNodeID)

	return result, nil
}
