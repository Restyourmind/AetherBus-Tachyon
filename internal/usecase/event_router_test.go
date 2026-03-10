package usecase

import (
	"context"
	"testing"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

func TestEventRouterPublishWithResult(t *testing.T) {
	routes := stubRouteStore{routes: map[string]string{"orders.created": "node-a"}}
	router := NewEventRouter(routes)

	t.Run("routed outcome", func(t *testing.T) {
		result, err := router.PublishWithResult(context.Background(), domain.Envelope{
			ClientID: []byte("client-1"),
			Event: domain.Event{
				ID:    "evt-1",
				Topic: "orders.created",
			},
		})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if result.Status != domain.RouteStatusRouted {
			t.Fatalf("expected routed status, got %q", result.Status)
		}
		if result.DestinationID != "node-a" {
			t.Fatalf("expected destination node-a, got %q", result.DestinationID)
		}
		if result.Topic != "orders.created" {
			t.Fatalf("expected topic orders.created, got %q", result.Topic)
		}
	})

	t.Run("unroutable outcome", func(t *testing.T) {
		result, err := router.PublishWithResult(context.Background(), domain.Envelope{
			ClientID: []byte("client-2"),
			Event: domain.Event{
				ID:    "evt-2",
				Topic: "orders.unknown",
			},
		})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if result.Status != domain.RouteStatusUnroutable {
			t.Fatalf("expected unroutable status, got %q", result.Status)
		}
		if result.DestinationID != "" {
			t.Fatalf("expected empty destination for unroutable, got %q", result.DestinationID)
		}
		if result.Topic != "orders.unknown" {
			t.Fatalf("expected topic orders.unknown, got %q", result.Topic)
		}
	})
}

func TestEventRouterPublishBackwardCompatible(t *testing.T) {
	routes := stubRouteStore{routes: map[string]string{"orders.created": "node-a"}}
	router := NewEventRouter(routes)

	err := router.Publish(context.Background(), domain.Envelope{
		ClientID: []byte("client-1"),
		Event:    domain.Event{ID: "evt-3", Topic: "orders.created"},
	})
	if err != nil {
		t.Fatalf("expected nil error from Publish, got %v", err)
	}
}
