# Intent Core Phase 1 (Single-Node Scaffold)

This document describes the **currently implemented** intent-core scaffold and what remains future work for shard-aware evolution.

## Implemented in this phase

Scope: local process only, additive to the existing topic-routing broker.

- Minimal intent domain model with lifecycle states (`proposed` through terminal states).
- Admission validator for local intents (identity presence, local scope gate, goal/topic checks, trust-range checks, and a defer path for resource-heavy intents).
- Local in-memory intent graph representation (intent nodes + derived-from edges).
- Decomposition stub that maps one admitted intent into one local routable broker action.
- Local action resolution against existing `RouteStore` topic matching.
- Existing broker routing path is unchanged; intent-core is a separate use case layer.

## Explicit non-goals in this phase

- No distributed consensus.
- No cross-shard/global execution protocol.
- No remote graph replication or durable intent graph storage.
- No policy engine, conflict solver, or scheduler implementation beyond stubs.

## Current lifecycle handling

The implemented scaffold can produce/track these immediate state transitions:

- `proposed -> rejected` (failed admission)
- `proposed -> admitted -> decomposed` (admitted + decomposition into local actions)

Later lifecycle stages (`matched`, `scheduled`, `executing`, `satisfied`, `failed`, `superseded`) are represented in the domain model but not orchestrated yet.

## Follow-up list (shard-aware evolution)

1. Add shard metadata and intent ownership boundaries to route local vs shard/global intents.
2. Introduce shard-level intent graph partitioning and neighborhood indexing.
3. Add cross-shard handoff protocol (`shard_prepare`, `shard_vote`, `global_commit|abort`) with compensation hooks.
4. Add durable intent graph/WAL snapshots for crash recovery and replay.
5. Add deterministic conflict resolution and policy precedence engine.
6. Add scheduler integration with capacity/resource brokers.
7. Add observability for intent admission latency, decomposition latency, and local resolution outcomes.
