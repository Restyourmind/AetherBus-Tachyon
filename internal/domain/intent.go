package domain

import "time"

// IntentScope defines the intended execution locality.
type IntentScope string

const (
	IntentScopeLocal  IntentScope = "local"
	IntentScopeShard  IntentScope = "shard"
	IntentScopeGlobal IntentScope = "global"
)

// IntentStatus tracks the lifecycle for an intent.
type IntentStatus string

const (
	IntentStatusProposed   IntentStatus = "proposed"
	IntentStatusAdmitted   IntentStatus = "admitted"
	IntentStatusDecomposed IntentStatus = "decomposed"
	IntentStatusMatched    IntentStatus = "matched"
	IntentStatusScheduled  IntentStatus = "scheduled"
	IntentStatusExecuting  IntentStatus = "executing"
	IntentStatusSatisfied  IntentStatus = "satisfied"
	IntentStatusFailed     IntentStatus = "failed"
	IntentStatusRejected   IntentStatus = "rejected"
	IntentStatusSuperseded IntentStatus = "superseded"
)

// Intent captures a minimal canonical intent record for local coordination.
type Intent struct {
	ID               string
	Issuer           string
	Kind             string
	Scope            IntentScope
	Goal             GoalPredicate
	Constraints      []Constraint
	Deadline         time.Time
	PriorityHint     float64
	TrustRequirement float64
	Status           IntentStatus
}

// GoalPredicate contains user intent goals.
type GoalPredicate struct {
	Topic       string
	Description string
}

// Constraint captures minimal key-value restrictions on an intent.
type Constraint struct {
	Key   string
	Value string
}

// IntentGraphNode stores one node in the local intent graph.
type IntentGraphNode struct {
	IntentID string
	Status   IntentStatus
}

// IntentEdgeType defines supported graph edge kinds.
type IntentEdgeType string

const (
	IntentEdgeDependsOn   IntentEdgeType = "depends_on"
	IntentEdgeDerivedFrom IntentEdgeType = "derived_from"
)

// IntentGraphEdge stores a directional relationship between intents.
type IntentGraphEdge struct {
	From string
	To   string
	Type IntentEdgeType
}

// RoutableBrokerAction is a local-only mapping target into broker routes.
type RoutableBrokerAction struct {
	ActionID string
	IntentID string
	Topic    string
	Payload  map[string]string
}

// ResolvedBrokerAction is a routable action bound to a destination.
type ResolvedBrokerAction struct {
	RoutableBrokerAction
	DestinationNodeID string
}

// IntentAdmissionOutcome captures admission decisions for a proposed intent.
type IntentAdmissionOutcome struct {
	Admitted bool
	Deferred bool
	Reason   string
}

// IntentResolutionResult captures the result of local intent processing.
type IntentResolutionResult struct {
	IntentID   string
	Actions    []ResolvedBrokerAction
	Unroutable []RoutableBrokerAction
	GraphNodes []IntentGraphNode
	GraphEdges []IntentGraphEdge
}
