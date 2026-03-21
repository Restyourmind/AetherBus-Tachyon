# AetherBus-Tachyon Routing Specification
## RFC 0002
### Status: Draft

## 1. Abstract

This document defines the routing model for AetherBus-Tachyon, including topic matching,
route precedence, wildcard semantics, consumer route resolution, fanout behavior,
and RouteStore responsibilities.

AetherBus-Tachyon uses topic-based routing backed by a RouteStore abstraction.
The reference implementation stores routes using an Adaptive Radix Tree (ART) in
order to provide low-latency lookup behavior even at larger routing table sizes.

---

## 2. Routing Model Overview

The routing subsystem is responsible for transforming an inbound envelope into one
or more outbound deliveries.

Conceptually:

```text
Envelope → Topic → RouteStore lookup → Route resolution → Delivery decision
```

The routing subsystem does not define raw transport behavior; transport selection
belongs to delivery policy and the outbound delivery layer.

### 2.1 Direct-Delivery Priority Policy

Direct delivery MAY apply a normalized priority class after route resolution and before broker dispatch.

The reference broker accepts the configured classes case-insensitively, normalizes them to canonical tokens, and dispatches backlog in this order by default:

1. `urgent`
2. `high`
3. `normal`
4. `low`

Tie-breaks inside the same class MUST be deterministic. The reference implementation uses a monotonically increasing enqueue sequence, then topic name as the final stable tie-break when two queues become otherwise equivalent.

When starvation prevention is enabled, the broker MAY age old backlog entries using deterministic weighting so lower-priority work is eventually selected without breaking replayability.

## 3. RouteStore Role

The RouteStore is the broker’s route index.

Its responsibilities include:

- storing route patterns
- resolving exact matches
- resolving wildcard matches
- returning route candidates
- supporting efficient lookup
- enabling safe route insertion and deletion
- preserving deterministic precedence rules

The RouteStore MUST be abstracted behind an interface so that alternate implementations
can replace the ART-backed reference implementation without changing use case logic.

## 4. Route Object

A route SHOULD be modeled logically as:

```text
Route
├── pattern
├── route_type
├── destination_id
├── priority
├── metadata
└── enabled
```

### 4.1 Route Fields

| Field | Description |
|---|---|
| pattern | Topic pattern used for matching |
| route_type | Direct, fanout, internal, bridge, or system |
| destination_id | Consumer, channel, or bridge target |
| priority | Precedence within same match class |
| metadata | Arbitrary route metadata |
| enabled | Whether route is active |

## 5. Topic Grammar

Topics are dot-separated paths.

### 5.1 Allowed Form

```text
segment(.segment)*
```

### 5.2 Recommended Character Set

A segment SHOULD use:

- lowercase letters
- digits
- underscore
- hyphen

### 5.3 Examples

Valid examples:

- orders.created
- orders.updated
- payments.authorized
- system.node.heartbeat
- agents.risk.alerted

## 6. Wildcard Semantics

abtp/1 routing defines two wildcard operators:

- `*` matches exactly one segment
- `>` matches the remainder of the path and MUST only appear as the final segment

### 6.1 Single-Segment Wildcard

Pattern:

```text
orders.*
```

Matches:

- orders.created
- orders.updated

Does not match:

- orders.eu.created
- orders

### 6.2 Remainder Wildcard

Pattern:

```text
orders.>
```

Matches:

- orders.created
- orders.eu.created
- orders.eu.warehouse.created

Does not match:

- payments.created

### 6.3 Invalid Wildcard Examples

The following SHOULD be treated as invalid patterns:

- orders.>.created
- >.orders
- orders..created
- orders.*.>
- empty pattern

## 7. Match Classes

A route match belongs to one of the following classes:

- exact match
- single-segment wildcard match
- remainder wildcard match

These classes have strict precedence.

## 8. Route Precedence

When multiple routes match a topic, the broker MUST resolve precedence deterministically.

The recommended precedence order is:

1. exact match
2. single-segment wildcard match
3. remainder wildcard match
4. higher route priority
5. stable tie-breaker

### 8.1 Stable Tie-Breaker

A stable tie-breaker MAY be:

- lexical route pattern order
- insertion sequence number
- deterministic destination order

The broker implementation MUST document its chosen tie-breaker.

## 9. Exact Matching

An exact match occurs when the topic equals the route pattern exactly.

Example:

Topic:

```text
orders.created
```

Route pattern:

```text
orders.created
```

This is the highest precedence match and SHOULD win over any wildcard routes.

## 10. Single-Segment Wildcard Matching

A `*` wildcard matches exactly one segment.

Example route:

```text
system.*.heartbeat
```

Matches:

- system.node.heartbeat
- system.worker.heartbeat

Does not match:

- system.heartbeat
- system.cluster.node.heartbeat

## 11. Remainder Wildcard Matching

A `>` wildcard matches the rest of the topic path.

Example route:

```text
agents.>
```

Matches:

- agents.alert
- agents.risk.alert
- agents.risk.region.eu.alert

This route is less specific than exact and single-segment wildcard routes.

## 12. Direct vs Fanout Resolution

Routing resolution determines candidate routes.
Delivery policy determines whether the result is:

- one selected destination
- all matching destinations
- a bridge target
- a system channel

For direct delivery, destination selection happens first and backlog dispatch policy is applied second. This means route precedence and direct-delivery priority are separate concerns: route precedence selects *where* the event can go, while direct-delivery priority selects *which queued event* is dispatched next for an eligible consumer.

### 12.1 Direct Route

A direct route maps to one chosen destination.

### 12.2 Fanout Route

A fanout route maps to multiple destinations and MUST preserve all valid route matches.

## 13. Route Types

Recommended route types:

- direct
- fanout
- bridge
- internal
- system

### 13.1 Direct

Used for worker-style routing or command processing.

### 13.2 Fanout

Used for event broadcasts or subscription-based dispatch.

### 13.3 Bridge

Used to forward messages to external or downstream systems.

### 13.4 Internal

Used for broker internal control messages.

### 13.5 System

Used for diagnostics, heartbeats, and broker-level events.

## 14. Route Registration

Routes MAY be:

- statically configured
- dynamically registered
- session-bound
- broker-generated

### 14.1 Static Routes

Routes defined at startup through configuration or bootstrap code.

### 14.2 Dynamic Routes

Routes added at runtime through administrative APIs or registration mechanisms.

### 14.3 Session-Bound Routes

Routes attached to a consumer session and removed when the session expires.

## 15. Consumer Subscription Model

A consumer or subscriber MAY register a set of topic patterns.

Example:

```json
{
  "type": "consumer.register",
  "consumer_id": "worker.invoice.1",
  "mode": "direct",
  "subscriptions": [
    "orders.created",
    "payments.*",
    "billing.>"
  ]
}
```

The broker SHOULD transform each subscription into one or more internal routes.

## 16. Route Insertion Rules

A route insertion operation:

- MUST validate pattern syntax
- MUST normalize route metadata
- SHOULD reject duplicate conflicting registrations unless duplicate routes are allowed by policy
- SHOULD record insertion order if tie-breaker logic depends on it

## 17. Route Deletion Rules

A route deletion operation:

- MUST remove the exact route identity or session-bound route
- SHOULD leave unrelated routes untouched
- SHOULD be idempotent where practical

## 18. Route Lookup Rules

Given a topic, the broker MUST:

- normalize or validate the topic
- query the RouteStore
- collect exact matches
- collect wildcard matches
- apply precedence rules
- determine route type behavior
- pass selected destination(s) to delivery logic

## 19. ART-Specific Design Considerations

The ART-backed RouteStore is intended to optimize route lookup.

### 19.1 Why ART

ART is suitable because it offers:

- efficient prefix-like lookup behavior
- lower-latency matching than naive linear scan
- good scalability when the number of routes grows
- natural fit for path-like or segment-based keys

### 19.2 Canonical Key Representation

An implementation SHOULD define one canonical representation for route patterns.

Recommended options:

- raw topic string
- tokenized segment path
- normalized internal trie key

### 19.3 Wildcard Representation

Wildcard segments SHOULD be represented explicitly and MUST NOT be confused with literal symbols in user-defined segment content.

## 20. Determinism Requirements

Two brokers with the same route set and same topic MUST produce the same resolved routing result,
assuming identical tie-breaker policy.

This property is REQUIRED for predictable behavior, reproducible tests, and replayability.

## 21. Unroutable Messages

If no route matches a topic:

- the broker MAY reject the message
- the broker MAY emit a system event
- the broker MAY write the message to an unroutable topic
- the broker SHOULD log the routing miss

Recommended unroutable topic:

```text
_system.unroutable
```

Recommended payload fields:

- message_id
- topic
- producer_id
- timestamp_unix_ms
- reason

## 22. Ambiguous Matches

Ambiguous matches occur when multiple routes of the same precedence class and same priority are valid.

The broker MUST use a stable tie-breaker in direct delivery mode.

In fanout mode, all valid routes MAY be preserved.

## 23. Route Priority

Priority is a secondary ordering mechanism used inside the same match class.

Higher priority SHOULD win.

Example:

| Pattern | Match Class | Priority |
|---|---|---|
| orders.* | wildcard | 10 |
| orders.* | wildcard | 5 |

The route with priority 10 SHOULD be preferred in direct mode.

## 24. Fanout Route Combination

Fanout delivery MAY combine:

- exact route matches
- wildcard route matches
- session-bound subscribers
- static subscribers

A broker SHOULD avoid sending duplicate deliveries to the same destination if multiple matching routes resolve to the same endpoint, unless configured otherwise.

## 25. Deduplication of Destinations

If a single destination is matched by multiple routes during fanout:

- the broker SHOULD deduplicate by destination identity
- the broker MAY preserve per-route metadata internally
- the broker MUST document whether duplicate deliveries are possible

Recommended default: deduplicate.

## 26. Route Metadata

Route metadata MAY include:

- tenant scope
- required delivery policy
- route source
- health state
- weight
- affinity group
- partition hint
- retention class

The routing engine MUST NOT rely on undocumented metadata for core correctness.

## 27. Tenancy and Namespacing

Multi-tenant deployments SHOULD isolate routes by tenant.

Recommended models:

### 27.1 Header-Based Tenant Isolation

Tenant provided in header, e.g.:

```json
"tenant_id": "tenant_a"
```

### 27.2 Topic Prefix Isolation

Tenant encoded in topic prefix, e.g.:

```text
tenant_a.orders.created
```

### 27.3 Internal Route Namespace Isolation

Preferred for clarity:

- logical tenant context kept outside topic path
- routing table partitioned internally by tenant

The reference implementation now uses **internal partitioning**, not topic prefixing.
`tenant_id` is carried separately from `topic` in the route key model, and the ART-backed
`RouteStore` indexes routes inside a tenant-partitioned keyspace. This means two tenants
can both define `orders.created` without colliding, while a missing or empty tenant falls
back to the default/global partition for backward compatibility.

Routing and direct-delivery bookkeeping also preserve `tenant_id` through:

- publish result accounting
- inflight message records
- deferred direct queues
- scheduled retry/deferred promotion state
- dead-letter records
- per-tenant metric/quota counters

## 28. Route Health and Availability

Routes MAY be temporarily unavailable due to:

- disconnected consumers
- degraded bridge targets
- administrative disablement
- overload protection

A routing implementation SHOULD distinguish:

- pattern match success
- destination availability

This distinction allows the broker to report “route exists but no live destination”.

## 29. Route Resolution Output

A route resolution result SHOULD include:

- resolved route(s)
- route type
- destination identity or identities
- match class
- effective priority
- resolution status

Example:

```json
{
  "topic": "orders.created",
  "match_class": "exact",
  "route_type": "direct",
  "destinations": ["worker.orders.1"],
  "status": "ok"
}
```

## 29.1 Route Catalog Snapshot Persistence

The reference implementation persists route state as a versioned JSON snapshot so broker restarts can restore runtime routes without replaying `bootstrapRoutes`.

Recommended snapshot shape:

```json
{
  "version": 1,
  "routes": [
    {
      "pattern": "orders.created",
      "destination_id": "worker.orders.1",
      "route_type": "direct",
      "priority": 10,
      "enabled": true,
      "tenant": "tenant_a",
      "metadata": {
        "source": "runtime"
      }
    }
  ]
}
```

Persistence requirements:

- startup SHOULD restore the snapshot before considering bootstrap routes
- startup MAY fall back to bootstrap routes when the snapshot is absent or corrupted
- every route mutation SHOULD rewrite the snapshot atomically
- snapshot `version` MUST gate decoding so future metadata additions remain backward-compatible by explicit migration
- duplicate logical route identities SHOULD collapse to a single stored entry during restore

## 30. Recommended Tests

A compliant routing implementation SHOULD test:

- exact match lookup
- wildcard match lookup
- precedence correctness
- invalid pattern rejection
- unroutable behavior
- duplicate destination deduplication
- route insertion/removal
- stability of direct tie-breaker logic
- large route table lookup latency

## 31. Future Extensions

Potential routing extensions include:

- weighted direct routing
- sticky routing by partition key
- hash-based sharding
- locality-aware routing
- health-aware route preference
- broker federation route propagation

## 32. Summary

The AetherBus-Tachyon routing model is built around:

- topic-based matching
- deterministic precedence
- ART-backed fast lookup
- support for direct and fanout semantics
- evolvable route metadata and destination policies

Routing remains intentionally separate from transport delivery so that
route resolution can evolve independently of ZeroMQ socket strategy.
