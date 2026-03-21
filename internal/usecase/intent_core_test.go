package usecase

import (
	"context"
	"strings"
	"testing"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

type stubRouteStore struct {
	routes map[string]string
}

func routeMapKey(tenantID, topic string) string {
	return tenantID + "\x00" + topic
}

func (s stubRouteStore) AddRoute(key domain.RouteKey, destNodeID string) error {
	s.routes[routeMapKey(key.TenantID, key.Topic)] = destNodeID
	return nil
}

func (s stubRouteStore) UpsertRoute(route domain.Route) error {
	s.routes[routeMapKey(route.Tenant, route.Pattern)] = route.DestinationID
	return nil
}

func (s stubRouteStore) RemoveRoute(key domain.RouteKey, destNodeID string) error {
	delete(s.routes, routeMapKey(key.TenantID, key.Topic))
	return nil
}

func (s stubRouteStore) Routes() []domain.Route {
	routes := make([]domain.Route, 0, len(s.routes))
	for composite, dest := range s.routes {
		parts := strings.SplitN(composite, "\x00", 2)
		route := domain.Route{DestinationID: dest, RouteType: "direct", Enabled: true}
		if len(parts) == 2 {
			route.Tenant = parts[0]
			route.Pattern = parts[1]
		} else {
			route.Pattern = composite
		}
		routes = append(routes, route)
	}
	return routes
}

func (s stubRouteStore) Match(key domain.RouteKey) string {
	return s.routes[routeMapKey(key.TenantID, key.Topic)]
}

func TestIntentAdmissionValidator(t *testing.T) {
	validator := IntentAdmissionValidator{}

	t.Run("reject missing issuer", func(t *testing.T) {
		outcome := validator.Validate(domain.Intent{ID: "intent-1", Scope: domain.IntentScopeLocal, Kind: "route", Goal: domain.GoalPredicate{Topic: "jobs.run"}})
		if outcome.Admitted || outcome.Reason != "missing_issuer" {
			t.Fatalf("expected missing_issuer reject, got admitted=%v reason=%q", outcome.Admitted, outcome.Reason)
		}
	})

	t.Run("reject non-local scope", func(t *testing.T) {
		outcome := validator.Validate(domain.Intent{ID: "intent-2", Issuer: "svc-a", Scope: domain.IntentScopeGlobal, Kind: "route", Goal: domain.GoalPredicate{Topic: "jobs.run"}})
		if outcome.Admitted || outcome.Reason != "non_local_scope_not_supported" {
			t.Fatalf("expected non_local_scope_not_supported reject, got admitted=%v reason=%q", outcome.Admitted, outcome.Reason)
		}
	})

	t.Run("admit valid local intent", func(t *testing.T) {
		outcome := validator.Validate(domain.Intent{ID: "intent-3", Issuer: "svc-a", Scope: domain.IntentScopeLocal, Kind: "route", Goal: domain.GoalPredicate{Topic: "jobs.run"}})
		if !outcome.Admitted {
			t.Fatalf("expected admitted intent, got reason=%q", outcome.Reason)
		}
	})
}

func TestIntentDecomposerStub(t *testing.T) {
	decomposer := IntentDecomposer{}
	intent := domain.Intent{ID: "intent-10", Issuer: "svc-a", Scope: domain.IntentScopeLocal, Kind: "route", Goal: domain.GoalPredicate{Topic: "payments.created"}}
	actions := decomposer.Decompose(intent)

	if len(actions) != 1 {
		t.Fatalf("expected one action, got %d", len(actions))
	}
	if actions[0].IntentID != intent.ID {
		t.Fatalf("expected action to reference parent intent %q, got %q", intent.ID, actions[0].IntentID)
	}
	if actions[0].Topic != "payments.created" {
		t.Fatalf("expected decomposed topic payments.created, got %q", actions[0].Topic)
	}
}

func TestIntentCoordinatorLocalResolution(t *testing.T) {
	routes := stubRouteStore{routes: map[string]string{routeMapKey("", "payments.created"): "node-1"}}
	coordinator := NewIntentCoordinator(routes, NewLocalIntentGraph())

	intent := domain.Intent{ID: "intent-20", Issuer: "svc-a", Scope: domain.IntentScopeLocal, Kind: "route", Goal: domain.GoalPredicate{Topic: "payments.created"}}
	result, outcome := coordinator.Process(context.Background(), intent)
	if !outcome.Admitted {
		t.Fatalf("expected admitted result, got reason=%q", outcome.Reason)
	}
	if len(result.Actions) != 1 {
		t.Fatalf("expected one resolved action, got %d", len(result.Actions))
	}
	if result.Actions[0].DestinationNodeID != "node-1" {
		t.Fatalf("expected route destination node-1, got %q", result.Actions[0].DestinationNodeID)
	}

	unroutableIntent := domain.Intent{ID: "intent-21", Issuer: "svc-a", Scope: domain.IntentScopeLocal, Kind: "route", Goal: domain.GoalPredicate{Topic: "payments.refunded"}}
	result, outcome = coordinator.Process(context.Background(), unroutableIntent)
	if !outcome.Admitted {
		t.Fatalf("expected admitted unroutable intent, got reason=%q", outcome.Reason)
	}
	if len(result.Unroutable) != 1 {
		t.Fatalf("expected one unroutable action, got %d", len(result.Unroutable))
	}
	if len(result.GraphEdges) == 0 {
		t.Fatalf("expected decomposition graph edge to exist")
	}
}
