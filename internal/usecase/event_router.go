package usecase

import (
	"context"
	"fmt"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

type EventRouter struct {
	routeStore domain.RouteStore
}

var _ domain.EventPublisher = (*EventRouter)(nil)
var _ domain.EventPublisherWithResult = (*EventRouter)(nil)

func NewEventRouter(routeStore domain.RouteStore) *EventRouter {
	return &EventRouter{routeStore: routeStore}
}

func (er *EventRouter) Publish(ctx context.Context, envelope domain.Envelope) error {
	_, err := er.PublishWithResult(ctx, envelope)
	return err
}

func (er *EventRouter) PublishWithResult(ctx context.Context, envelope domain.Envelope) (domain.PublishResult, error) {
	_ = ctx
	tenantID := envelope.TenantID
	if tenantID == "" {
		tenantID = envelope.Event.TenantID
	}
	result := domain.PublishResult{Topic: envelope.Event.Topic, TenantID: tenantID}

	resolved := er.routeStore.Resolve(domain.RouteKey{TenantID: tenantID, Topic: envelope.Event.Topic})
	if len(resolved) == 0 {
		result.Status = domain.RouteStatusUnroutable
		fmt.Printf("Routing unroutable event %s (topic: %s) from client %s\n", envelope.Event.ID, envelope.Event.Topic, string(envelope.ClientID))
		return result, nil
	}

	result.Status = domain.RouteStatusRouted
	result.ResolvedDestinations = append([]domain.ResolvedDestination(nil), resolved...)
	result.DestinationID = resolved[0].DestinationID
	result.RouteType = resolved[0].RouteType
	fmt.Printf("Routing event %s (topic: %s) from client %s to destination %s (%s)\n", envelope.Event.ID, envelope.Event.Topic, string(envelope.ClientID), result.DestinationID, result.RouteType)
	return result, nil
}
