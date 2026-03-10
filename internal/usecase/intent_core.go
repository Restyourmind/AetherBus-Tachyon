package usecase

import (
	"context"
	"fmt"
	"sort"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

// LocalIntentGraph is a minimal in-memory graph for local-only intent relationships.
type LocalIntentGraph struct {
	nodes map[string]domain.IntentGraphNode
	edges []domain.IntentGraphEdge
}

// NewLocalIntentGraph creates an empty local graph representation.
func NewLocalIntentGraph() *LocalIntentGraph {
	return &LocalIntentGraph{
		nodes: make(map[string]domain.IntentGraphNode),
		edges: make([]domain.IntentGraphEdge, 0),
	}
}

// AddIntentNode inserts or updates an intent node.
func (g *LocalIntentGraph) AddIntentNode(intent domain.Intent) {
	g.nodes[intent.ID] = domain.IntentGraphNode{
		IntentID: intent.ID,
		Status:   intent.Status,
	}
}

// AddEdge appends a graph edge.
func (g *LocalIntentGraph) AddEdge(edge domain.IntentGraphEdge) {
	g.edges = append(g.edges, edge)
}

// Snapshot returns a deterministic copy for read-only inspection/testing.
func (g *LocalIntentGraph) Snapshot() ([]domain.IntentGraphNode, []domain.IntentGraphEdge) {
	nodes := make([]domain.IntentGraphNode, 0, len(g.nodes))
	for _, n := range g.nodes {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].IntentID < nodes[j].IntentID })

	edges := make([]domain.IntentGraphEdge, len(g.edges))
	copy(edges, g.edges)
	return nodes, edges
}

// IntentAdmissionValidator validates local-only phase intent submissions.
type IntentAdmissionValidator struct{}

// Validate performs minimal local admission checks.
func (v IntentAdmissionValidator) Validate(intent domain.Intent) domain.IntentAdmissionOutcome {
	if intent.ID == "" {
		return domain.IntentAdmissionOutcome{Reason: "missing_intent_id"}
	}
	if intent.Issuer == "" {
		return domain.IntentAdmissionOutcome{Reason: "missing_issuer"}
	}
	if intent.Scope != domain.IntentScopeLocal {
		return domain.IntentAdmissionOutcome{Reason: "non_local_scope_not_supported"}
	}
	if intent.Kind == "" {
		return domain.IntentAdmissionOutcome{Reason: "missing_kind"}
	}
	if intent.Goal.Topic == "" {
		return domain.IntentAdmissionOutcome{Reason: "missing_goal_topic"}
	}
	if intent.TrustRequirement > 1.0 {
		return domain.IntentAdmissionOutcome{Reason: "trust_requirement_out_of_range"}
	}
	if intent.TrustRequirement > 0.0 && intent.Issuer == "anonymous" {
		return domain.IntentAdmissionOutcome{Reason: "trust_threshold_not_met"}
	}
	if intent.Kind == "resource_heavy" {
		return domain.IntentAdmissionOutcome{Deferred: true, Reason: "resource_feasibility_deferred"}
	}
	return domain.IntentAdmissionOutcome{Admitted: true}
}

// IntentDecomposer creates routable broker actions from an admitted intent.
type IntentDecomposer struct{}

// Decompose maps an intent into one local executable action as a phase-1 scaffold.
func (d IntentDecomposer) Decompose(intent domain.Intent) []domain.RoutableBrokerAction {
	childID := fmt.Sprintf("%s.step.1", intent.ID)
	return []domain.RoutableBrokerAction{
		{
			ActionID: childID,
			IntentID: intent.ID,
			Topic:    intent.Goal.Topic,
			Payload: map[string]string{
				"intent_id": intent.ID,
				"kind":      intent.Kind,
				"issuer":    intent.Issuer,
			},
		},
	}
}

// IntentCoordinator runs admission, decomposition, graph updates, and local route resolution.
type IntentCoordinator struct {
	routeStore domain.RouteStore
	validator  IntentAdmissionValidator
	decomposer IntentDecomposer
	graph      *LocalIntentGraph
}

// NewIntentCoordinator constructs a local-only intent coordinator.
func NewIntentCoordinator(routeStore domain.RouteStore, graph *LocalIntentGraph) *IntentCoordinator {
	return &IntentCoordinator{
		routeStore: routeStore,
		validator:  IntentAdmissionValidator{},
		decomposer: IntentDecomposer{},
		graph:      graph,
	}
}

// Process runs intent processing without modifying existing broker topic-routing behavior.
func (c *IntentCoordinator) Process(ctx context.Context, intent domain.Intent) (domain.IntentResolutionResult, domain.IntentAdmissionOutcome) {
	_ = ctx
	outcome := c.validator.Validate(intent)
	if !outcome.Admitted {
		intent.Status = domain.IntentStatusRejected
		c.graph.AddIntentNode(intent)
		nodes, edges := c.graph.Snapshot()
		return domain.IntentResolutionResult{IntentID: intent.ID, GraphNodes: nodes, GraphEdges: edges}, outcome
	}

	intent.Status = domain.IntentStatusAdmitted
	c.graph.AddIntentNode(intent)

	actions := c.decomposer.Decompose(intent)
	for _, action := range actions {
		c.graph.AddIntentNode(domain.Intent{ID: action.ActionID, Status: domain.IntentStatusDecomposed})
		c.graph.AddEdge(domain.IntentGraphEdge{From: action.ActionID, To: intent.ID, Type: domain.IntentEdgeDerivedFrom})
	}

	resolved := make([]domain.ResolvedBrokerAction, 0, len(actions))
	unroutable := make([]domain.RoutableBrokerAction, 0)
	for _, action := range actions {
		dest := c.routeStore.Match(action.Topic)
		if dest == "" {
			unroutable = append(unroutable, action)
			continue
		}
		resolved = append(resolved, domain.ResolvedBrokerAction{RoutableBrokerAction: action, DestinationNodeID: dest})
	}

	nodes, edges := c.graph.Snapshot()
	return domain.IntentResolutionResult{
		IntentID:   intent.ID,
		Actions:    resolved,
		Unroutable: unroutable,
		GraphNodes: nodes,
		GraphEdges: edges,
	}, outcome
}
