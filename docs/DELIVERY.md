# AetherBus-Tachyon Delivery Specification
## RFC 0003
### Status: Draft

## 1. Abstract

This document defines the delivery model for AetherBus-Tachyon, including transport mapping,
direct delivery, fanout delivery, acknowledgments, retries, dead-letter behavior,
session tracking, and operational expectations for broker-driven message dispatch.

The routing subsystem decides *where* a message should go.
The delivery subsystem decides *how* that message gets there and how the broker tracks the result.

---

## 2. Delivery Overview

AetherBus-Tachyon supports multiple delivery behaviors while maintaining a unified routing layer.

At minimum, the broker SHOULD support:

- direct delivery
- fanout delivery

It MAY also support:

- bridge delivery
- internal control delivery
- dead-letter delivery

Conceptually:

```text
Resolved route(s) → Delivery mode → Outbound transport → Optional ACK/NACK → Final status
```

## 3. Transport Model

AetherBus-Tachyon is built on ZeroMQ transport patterns.

The recommended mapping is:

- ROUTER for broker ingress and direct-session addressing
- DEALER or worker-side socket patterns for active consumers
- PUB for fanout event egress
- SUB for subscriber clients

Transport details do not change the canonical envelope model.

## 4. Delivery Modes

### 4.1 Direct Delivery

Direct delivery is intended for:

- commands
- job dispatch
- worker processing
- critical event handling requiring tracking

Properties:

- one selected destination
- optional or required ACK/NACK
- suitable for retry tracking
- suitable for inflight accounting

### 4.2 Fanout Delivery

Fanout delivery is intended for:

- broadcast events
- subscriptions
- observability events
- multi-consumer notifications

Properties:

- one message sent to all matching subscribers
- usually best-effort
- ACK/NACK typically omitted
- optimized for low overhead

### 4.3 Bridge Delivery

Bridge delivery is intended for:

- forwarding to other brokers
- connector-based forwarding
- future federation or cluster routing

Properties:

- may preserve original envelope
- may add bridge metadata
- may track success/failure separately

## 5. Direct Delivery Semantics

When a message resolves to a direct route:

- the broker selects one destination
- the broker emits the message to the destination transport binding
- the broker optionally records inflight state
- the consumer processes the message
- the consumer returns ACK or NACK if required
- the broker finalizes delivery state

### 5.1 Direct Selection

Direct selection MAY be based on:

- exact route resolution
- stable route precedence
- session availability
- health filtering
- future weighted policy

The selection algorithm MUST be deterministic enough for reproducible behavior.

## 6. Fanout Delivery Semantics

When a message resolves to fanout delivery:

- the broker computes the set of valid subscriber destinations
- the broker deduplicates destinations where appropriate
- the broker publishes or dispatches the message to all destinations
- the broker MAY record aggregate fanout counters
- the broker typically does not await per-subscriber ACKs

Fanout mode prioritizes dissemination efficiency over individual delivery guarantees.

## 7. Session Model

Consumers that receive direct deliveries SHOULD maintain sessions with the broker.

### 7.1 Session State

A session SHOULD include:

- session_id
- consumer_id
- socket_identity
- subscriptions
- connected_at
- last_heartbeat
- max_inflight
- supports_ack
- status

### 7.2 Session Lifecycle

A session typically progresses through:

- registration
- active heartbeat
- message delivery eligibility
- graceful deregistration or timeout
- cleanup

## 8. Consumer Registration

Consumers MAY register their delivery capabilities.

Example:

```json
{
  "type": "consumer.register",
  "consumer_id": "worker.orders.1",
  "mode": "direct",
  "subscriptions": [
    "orders.created",
    "orders.updated"
  ],
  "capabilities": {
    "supports_ack": true,
    "supports_compression": ["lz4"],
    "supports_codec": ["json"],
    "max_inflight": 1024
  }
}
```

### 8.1 Registration Acknowledgment

```json
{
  "type": "consumer.registered",
  "consumer_id": "worker.orders.1",
  "session_id": "sess_01HV...",
  "status": "ok"
}
```

## 9. Inflight Tracking

For direct delivery with acknowledgment enabled, the broker SHOULD maintain an inflight registry.

### 9.1 Inflight Record

An inflight record SHOULD contain:

- message_id
- consumer_id
- session_id
- topic
- delivery_attempt
- dispatched_at
- ack_deadline
- status

### 9.2 Inflight Status Values

Recommended values:

- dispatched
- acked
- nacked
- expired
- retry_scheduled
- dead_lettered

## 10. ACK Handling

### 10.1 Successful ACK

A successful ACK indicates the consumer has completed processing.

The broker SHOULD:

- mark the inflight record as acked
- update counters and latency metrics
- remove or archive inflight state according to retention policy

### 10.2 Duplicate ACK

If an ACK is received twice:

- the broker SHOULD treat the second ACK as harmless
- the broker SHOULD log it at debug or warning level depending on policy

### 10.3 Stale ACK

If an ACK is received for a missing or expired session:

- the broker SHOULD ignore it safely
- the broker SHOULD log enough metadata for diagnostics

## 11. NACK Handling

A NACK indicates processing failure.

### 11.1 Retryable NACK

If the status indicates a recoverable failure:

- the broker MAY schedule a retry
- the broker SHOULD increment delivery attempt count
- the broker SHOULD preserve the original message identity

### 11.2 Non-Retryable NACK

If the status indicates a terminal failure:

- the broker MAY dead-letter the message
- the broker MAY emit a system event
- the broker SHOULD stop retrying

## 12. Retry Semantics

Retries apply primarily to direct delivery.

### 12.1 Retry Policy Fields

A retry policy MAY include:

- max_attempts
- initial_delay_ms
- backoff_multiplier
- max_delay_ms
- jitter
- dead_letter_on_exhaustion

### 12.2 Recommended Defaults

Reasonable implementation defaults MAY be:

- max_attempts = 3
- initial_delay_ms = 100
- exponential backoff enabled

### 12.3 Retry Invariants

A retrying broker:

- MUST preserve message_id
- SHOULD increment attempt
- SHOULD preserve correlation_id
- SHOULD preserve idempotency_key
- SHOULD record retry reason

## 13. Dead-Letter Delivery

Messages that cannot be delivered successfully MAY be sent to a dead-letter topic.

### 13.1 Recommended DLQ Topic Convention

```text
_dlq.<original_topic>
```

Examples:

- _dlq.orders.created
- _dlq.payments.authorized

### 13.2 Dead-Letter Metadata

A dead-lettered envelope SHOULD include metadata such as:

- dead_letter_reason
- dead_letter_count
- original_topic
- last_consumer_id
- last_error

### 13.3 Dead-Letter Criteria

A message MAY be dead-lettered if:

- max retries exceeded
- consumer repeatedly NACKs terminally
- routing target unavailable past policy threshold
- TTL expired before successful delivery
- broker policy requires quarantine

## 14. TTL and Expiration

If ttl_ms is set:

- the broker SHOULD compute expiry relative to the message timestamp
- expired direct messages SHOULD NOT be newly dispatched
- expired inflight messages MAY be finalized as failed or dead-lettered
- expired fanout messages MAY be dropped

## 15. Backpressure

The broker MUST define a backpressure strategy to avoid runaway memory growth and slow-consumer collapse.

### 15.1 Recommended Backpressure Controls

- per-consumer max_inflight
- per-topic queue size
- global ingress limit
- retry queue bound
- dead-letter queue bound

### 15.2 Slow Consumer Handling

If a consumer is too slow:

- the broker MAY stop assigning new direct work
- the broker MAY mark the session as degraded
- the broker MAY disconnect the session after policy-defined thresholds
- the broker SHOULD emit a health signal or diagnostic event

## 16. Ordering

AetherBus-Tachyon does not guarantee global ordering.

### 16.1 Recommended Ordering Guarantees

The broker MAY preserve local ordering only under constrained conditions, such as:

- same producer session
- same direct destination queue
- same partition key, if supported in future

### 16.2 Fanout Ordering

Fanout ordering SHOULD be treated as best-effort.

## 17. Delivery Status Model

A broker SHOULD model delivery status transitions explicitly.

### 17.1 Suggested Status Values

- accepted
- resolved
- dispatched
- acked
- nacked
- retry_scheduled
- dead_lettered
- expired
- dropped

### 17.2 Transition Rules

Examples:

- accepted → resolved → dispatched
- dispatched → acked
- dispatched → nacked → retry_scheduled
- retry_scheduled → dispatched
- dispatched → dead_lettered
- accepted → dropped

The implementation SHOULD avoid impossible or ambiguous transitions.

## 18. Delivery Errors

The broker SHOULD distinguish between:

- protocol errors
- routing errors
- transport errors
- consumer processing errors
- retry exhaustion
- policy rejection

This distinction is important for observability and operator debugging.

## 19. Direct Delivery Selection Policies

Future implementations MAY support:

- first available
- round-robin
- weighted
- sticky-by-key
- health-aware selection

The initial implementation SHOULD document one deterministic default strategy.

## 20. Fanout Deduplication

If a subscriber matches multiple routes:

- the broker SHOULD deduplicate by destination identity
- the broker MAY log all matched routes for debugging
- the broker SHOULD avoid duplicate delivery unless explicitly configured

## 21. Heartbeats

Consumers SHOULD periodically heartbeat if persistent direct delivery sessions are used.

### 21.1 Heartbeat Message

```json
{
  "type": "consumer.heartbeat",
  "consumer_id": "worker.orders.1",
  "session_id": "sess_01HV...",
  "timestamp_unix_ms": 1741337009999
}
```

### 21.2 Heartbeat Handling

The broker SHOULD:

- update last_heartbeat
- refresh session liveness
- use heartbeat timeout to expire stale sessions

## 22. Disconnect Behavior

When a consumer disconnects unexpectedly:

- inflight direct messages MAY be retried
- session-bound routes SHOULD be invalidated
- fanout subscribers MAY simply stop receiving future broadcasts
- the broker SHOULD emit a diagnostic event

## 23. Bridge Delivery Behavior

Bridge delivery is intended for future expansion and connector-based forwarding.

A bridge handler SHOULD:

- preserve envelope identity fields
- preserve or annotate topic
- preserve correlation_id
- add bridge metadata if necessary
- record success/failure outcomes separately from local direct delivery state

## 24. Metrics

The delivery subsystem SHOULD expose metrics such as:

- messages_dispatched_total
- messages_acked_total
- messages_nacked_total
- messages_retried_total
- messages_dead_lettered_total
- fanout_deliveries_total
- direct_deliveries_total
- inflight_messages
- slow_consumers
- ack_latency_ms
- delivery_latency_ms

## 24A. Current Runtime Implementation Notes

Current broker runtime behavior (`internal/delivery/zmq.Router`) includes:

- direct consumer session registration via control messages on `_control`
- direct inflight registry keyed by `message_id`
- ACK handling that removes inflight state and treats duplicate/stale ACK as harmless no-op
- NACK handling with deterministic outcomes:
  - `status=retryable_error` retries up to the configured max attempt count
  - any other status is treated as terminal and finalized as dead-lettered
- direct delivery counters for dispatched, acked, nacked, retried, and dead-lettered messages
- fanout path remains lightweight via PUB dispatch, without per-subscriber ACK tracking

This behavior provides at-least-once style retry handling for direct mode and does not provide exactly-once semantics.

## 25. Logging

Delivery logs SHOULD be structured.

Recommended fields:

- message_id
- correlation_id
- topic
- consumer_id
- session_id
- delivery_mode
- attempt
- status
- reason
- timestamp

## 26. Recommended Failure Handling Matrix

| Failure Type | Suggested Action |
|---|---|
| malformed envelope | reject immediately |
| no route found | emit unroutable event |
| direct consumer unavailable | retry or dead-letter |
| retryable NACK | retry |
| terminal NACK | dead-letter |
| fanout subscriber absent | ignore and continue |
| TTL expired | drop or dead-letter |
| stale ACK | ignore and log |

## 27. Operational Recommendations

A production-ready deployment SHOULD define:

- retry policy
- ACK timeout
- heartbeat timeout
- dead-letter policy
- queue size limits
- consumer inflight limits
- routing miss policy
- duplicate ACK handling policy

## 28. Security Considerations

The delivery layer SHOULD integrate with:

- connection-level authentication where available
- topic authorization policy
- consumer identity verification
- session-scoped permissions
- optional encryption or trusted transport boundary

These concerns are deployment- and environment-specific.

## 29. Non-Goals

This specification does not define:

- cluster replication
- exactly-once delivery
- distributed consensus
- transactional multi-destination fanout
- mandatory persistent storage

These may be layered on in future versions.

## 30. Summary

The AetherBus-Tachyon delivery model separates:

- route resolution
- transport dispatch
- consumer liveness
- acknowledgment tracking
- retries
- dead-letter handling

This separation enables the broker to remain lightweight while still supporting production-grade message handling behavior.


### 9.3 Timeout and Retry on Missing ACK

For direct delivery with ACK enabled, each inflight record includes `dispatched_at`.

- Broker configuration: `delivery_timeout_ms`
- If `now - dispatched_at >= delivery_timeout_ms`, broker treats the message as retryable.
- Broker increments retry counters and re-dispatches while retry budget remains.
- When retry budget is exhausted, broker transitions the message to dead-letter state.

### 9.4 Timeout-related Metrics

Broker delivery metrics include:

- `delivery_timeout`: count of inflight messages that crossed the ACK timeout window.
- `retry_due_to_timeout`: count of retries triggered specifically by ACK timeout (subset of total retries).
