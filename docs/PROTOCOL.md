# AetherBus-Tachyon Protocol Specification
## RFC 0001
### Status: Draft
### Version: abtp/1

## 1. Abstract

This document specifies the AetherBus-Tachyon wire protocol, canonical envelope model,
message metadata, serialization expectations, compression rules, protocol semantics,
and interoperability constraints for producers, consumers, and broker implementations.

AetherBus-Tachyon is a high-performance, lightweight message broker designed for the
AetherBus ecosystem. It is built around structured envelopes, topic-based routing,
pluggable codecs, and pluggable compressors.

This specification defines the minimum interoperable behavior required for any producer,
consumer, broker, router, or bridge participating in an AetherBus-Tachyon deployment.

---

## 2. Terminology

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**,
**SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **NOT RECOMMENDED**, **MAY**, and
**OPTIONAL** in this document are to be interpreted as described in RFC 2119.

### 2.1 Definitions

- **Broker**: The AetherBus-Tachyon server process that accepts inbound messages,
  resolves routes, and dispatches messages to consumers or subscribers.
- **Producer**: A client or service that publishes or sends a message to the broker.
- **Consumer**: A worker or service that receives direct or routed messages.
- **Subscriber**: A client that receives fanout events through subscription.
- **Envelope**: The canonical broker-level representation of a message.
- **Topic**: A dot-separated routing key used for route lookup and event distribution.
- **Codec**: The serialization format used for the payload.
- **Compressor**: The compression algorithm used on encoded bytes.
- **Delivery Mode**: The broker dispatch strategy for a message.
- **Session**: A logical registration or transport binding between a consumer and the broker.
- **ACK**: Positive acknowledgment indicating successful processing.
- **NACK**: Negative acknowledgment indicating unsuccessful processing.
- **DLQ**: Dead-letter queue or dead-letter topic.

---

## 3. Design Goals

The protocol is designed to satisfy the following requirements:

1. Low-latency routing
2. Minimal framing overhead
3. Clear topic semantics
4. Structured message metadata
5. Optional compression
6. Pluggable serialization
7. Broker-driven observability
8. Evolvable versioning
9. Support for direct delivery and fanout delivery
10. Compatibility with lightweight message-processing workers

---

## 4. Protocol Versioning

Every envelope MUST declare a protocol version.

The current protocol version is:

```text
abtp/1
```

A broker implementation:

- MUST reject unsupported protocol versions
- SHOULD expose broker-supported versions through diagnostics
- MAY support multiple versions concurrently

A producer:

- MUST send a protocol version
- SHOULD default to abtp/1 unless explicitly configured otherwise

## 5. Canonical Envelope

The canonical envelope is the logical message structure used inside the broker.

### 5.1 Envelope Structure

```json
{
  "spec_version": "abtp/1",
  "message_id": "msg_01HV8J1K9C7K7Q...",
  "correlation_id": "corr_01HV8J1KE0H...",
  "producer_id": "svc.billing.api",
  "topic": "billing.invoice.created",
  "headers": {
    "content_type": "application/json",
    "codec": "json",
    "compression": "lz4",
    "priority": "normal",
    "ttl_ms": 30000,
    "delivery_mode": "direct"
  },
  "payload": {
    "invoice_id": "inv_1001",
    "customer_id": "cust_42",
    "amount": 199.95
  },
  "timestamp_unix_ms": 1741337000000,
  "deliver_at_unix_ms": 1741337060000,
  "next_attempt_unix_ms": 1741337090000,
  "priority": "normal"
}
```

### 5.2 Required Fields

The following fields are REQUIRED:

- spec_version
- message_id
- producer_id
- topic
- payload
- timestamp_unix_ms

### 5.3 Optional Fields

The following fields are OPTIONAL:

- correlation_id
- headers
- reply_to
- partition_key
- trace_id
- auth
- deadline_unix_ms
- attempt
- deliver_at_unix_ms
- next_attempt_unix_ms
- priority

### 5.4 Field Semantics

#### spec_version

Protocol version string.

#### message_id

Globally unique message identifier.
This value MUST be unique enough for deduplication, tracing, and ACK correlation.

#### correlation_id

Identifier used to correlate a message with an upstream request, workflow, or causal chain.

#### producer_id

Stable producer identifier. Examples:

- svc.orders.api
- worker.billing.reconcile
- client.web.dashboard

#### topic

Topic used for route lookup and dispatch.

#### headers

Metadata used by the broker and consumers.

#### payload

Application-defined message body.

#### timestamp_unix_ms

Message creation timestamp in Unix milliseconds.

#### deliver_at_unix_ms

Earliest broker delivery timestamp in Unix milliseconds. When present, the broker MUST hold the message until this time before first dispatch. Past values MUST be rejected, and values beyond the broker scheduling horizon SHOULD be rejected.

#### next_attempt_unix_ms

Broker-managed retry scheduler timestamp in Unix milliseconds. Producers SHOULD omit this field on ingress. Brokers MAY set it when persisting delayed retries or forwarding an internal scheduled envelope.

#### priority

Normalized direct-delivery priority class. Brokers MUST accept configured class names case-insensitively and normalize them to the canonical configured token before queuing, WAL persistence, retry scheduling, or replay. The default classes are `urgent`, `high`, `normal`, and `low`.

If a producer omits `priority`, the broker MUST normalize the message to the configured default class. The reference implementation defaults to `normal`.

## 6. Headers

Headers are broker-visible metadata and MUST be a string-keyed object.

### 6.1 Standard Header Keys

| Header Key | Type | Description |
|---|---|---|
| content_type | string | MIME-style payload content type |
| codec | string | Serialization codec |
| compression | string | Compression codec |
| priority | string | Normalized direct-delivery class (`urgent`, `high`, `normal`, `low` by default) |
| ttl_ms | integer | Time-to-live in milliseconds |
| delivery_mode | string | Direct, fanout, or bridge |
| reply_expected | boolean | Whether producer expects a reply |
| idempotency_key | string | Application-side dedupe key |
| schema | string | Payload schema identifier |
| tenant_id | string | Tenant or namespace |
| trace_id | string | Distributed trace identifier |

### 6.2 Unknown Headers

Unknown headers:

- MUST be preserved if the envelope is forwarded
- MUST NOT cause protocol failure unless explicitly forbidden by broker policy

### 6.3 Priority Ordering and Replay Rules

For direct delivery, brokers MUST dispatch higher-priority backlog entries before lower-priority backlog entries.

Within the same priority class, ordering MUST be deterministic. The reference implementation uses a stable enqueue sequence and preserves that sequence in WAL-backed replay metadata.

If starvation prevention is enabled, a broker MAY age older backlog entries so lower-priority work eventually becomes dispatch-eligible. Any such aging rule MUST still be deterministic and replayable from persisted metadata.

## 7. Codecs

The protocol supports pluggable codecs.

### 7.1 Initial Codec Registry

The following codec identifiers are defined in abtp/1:

- json
- raw

### 7.2 JSON Codec

If codec=json:

- the payload MUST be encoded as JSON
- the broker MUST decode JSON before constructing the in-memory canonical envelope
- consumers SHOULD assume structured payload semantics

### 7.3 Raw Codec

If codec=raw:

- the payload MAY be arbitrary bytes
- the broker MAY treat the payload as opaque
- route lookup MUST still be possible without decoding payload content

### 7.4 Future Codecs

Future compatible codecs MAY include:

- msgpack
- protobuf
- cbor

These are outside the mandatory scope of abtp/1.

## 8. Compression

The protocol supports pluggable compression.

### 8.1 Initial Compression Registry

The following compression identifiers are defined:

- none
- lz4

### 8.2 LZ4

If compression=lz4:

- payload bytes MUST be compressed after codec serialization
- the broker MUST decompress before codec decode
- consumers receiving broker-decoded envelopes SHOULD NOT assume wire-level compressed payload visibility

### 8.3 No Compression

If compression=none:

- the payload MUST remain uncompressed after serialization

## 9. Topic Model

Topics are the broker’s routing keys.

### 9.1 Topic Syntax

The canonical topic format is dot-separated:

```text
segment.segment.segment
```

Examples:

- orders.created
- orders.updated
- payments.authorized
- system.node.heartbeat
- agents.risk.alerted

### 9.2 Topic Rules

A topic:

- MUST contain at least one segment
- MUST NOT be empty
- SHOULD use lowercase ASCII plus digits, hyphen, and underscore
- SHOULD use semantic hierarchy
- MUST NOT contain whitespace
- SHOULD avoid unbounded cardinality in high-level segments

### 9.3 Recommended Naming Pattern

Recommended pattern:

```text
domain.entity.action
```

Extended pattern:

```text
domain.entity.subentity.action
```

## 10. Delivery Modes

The following delivery modes are defined:

- direct
- fanout
- bridge

### 10.1 Direct

Direct delivery is intended for point-to-point or worker-style consumption.

Properties:

- one message routed to one selected destination or route target
- MAY require ACK/NACK
- RECOMMENDED for critical processing workflows

### 10.2 Fanout

Fanout delivery is intended for broadcast-style event distribution.

Properties:

- one message routed to all matching subscribers
- typically best-effort
- ACKs are OPTIONAL and usually omitted

### 10.3 Bridge

Bridge delivery is intended for forwarding to another system, broker, cluster, or connector.

Properties:

- MAY preserve envelope as-is
- MAY normalize protocol representation
- SHOULD preserve message identity metadata

## 11. Producer Requirements

A producer:

- MUST provide all required envelope fields
- MUST choose a valid topic
- SHOULD set correlation_id when participating in a larger workflow
- SHOULD set idempotency_key for side-effect-bearing operations
- SHOULD avoid oversized payloads unless explicitly allowed by broker configuration
- MUST use supported codec and compression identifiers
- SHOULD maintain clock synchronization within operationally acceptable bounds

## 12. Broker Requirements

A broker:

- MUST validate required fields
- MUST reject malformed envelopes
- MUST validate topic syntax
- MUST decode codec and compression correctly
- MUST normalize messages into canonical envelope representation
- MUST preserve message_id
- MUST preserve correlation_id if present
- MUST route based on topic
- MUST record enough metadata for observability and diagnostics
- SHOULD expose structured error causes for protocol rejection

## 13. Consumer Requirements

A consumer:

- MUST identify itself when registration is required by the broker
- SHOULD support ACK/NACK for direct mode if broker policy requires it
- SHOULD treat message_id as immutable
- SHOULD treat unknown headers as opaque
- SHOULD process duplicate direct deliveries safely if at-least-once delivery is enabled
- SHOULD use idempotent application behavior for externally visible side effects

## 14. Message Lifecycle

The logical lifecycle of a message is:

1. Producer constructs envelope
2. Producer serializes payload using codec
3. Producer compresses encoded payload if configured
4. Producer sends message to broker transport
5. Broker receives raw frames
6. Broker decompresses payload
7. Broker decodes payload
8. Broker validates envelope
9. Broker resolves route
10. Broker dispatches according to delivery mode
11. Consumer processes message
12. Consumer optionally returns ACK or NACK
13. Broker updates delivery status

## 15. ACK and NACK

### 15.1 ACK Message

```json
{
  "type": "ack",
  "message_id": "msg_01HV8J1K9C7K7Q...",
  "consumer_id": "worker.invoice.1",
  "session_id": "sess_000123",
  "status": "processed",
  "processed_at": 1741337000500
}
```

### 15.2 NACK Message

```json
{
  "type": "nack",
  "message_id": "msg_01HV8J1K9C7K7Q...",
  "consumer_id": "worker.invoice.1",
  "session_id": "sess_000123",
  "status": "retryable_error",
  "reason": "temporary downstream timeout"
}
```

### 15.3 ACK Rules

For direct delivery with acknowledgment enabled:

- a consumer SHOULD ACK successful processing
- a consumer SHOULD NACK recoverable failures
- a broker SHOULD correlate ACK/NACK by message_id
- a broker SHOULD validate consumer_id and (when provided) session_id before mutating inflight state
- a broker MUST ignore ACK/NACK for unknown or expired sessions unless explicitly configured otherwise
- control-plane ACK/NACK exchanges MAY be sent as JSON control messages on a reserved transport topic such as `_control`

## 16. Errors

### 16.1 Broker Rejection Reasons

The broker MAY reject a message for reasons including:

- unsupported protocol version
- malformed envelope
- invalid topic
- unsupported codec
- unsupported compression
- oversized payload
- unauthorized producer
- policy denial

### 16.2 Error Response Shape

```json
{
  "type": "error",
  "code": "invalid_topic",
  "message": "topic syntax is invalid",
  "message_id": "msg_01HV..."
}
```

## 17. Idempotency

The protocol does not guarantee exactly-once transport.

Applications requiring exactly-once observable behavior SHOULD use an idempotency_key.

A broker:

- MAY pass idempotency_key unchanged
- MAY use it for dedupe extension logic
- MUST NOT silently rewrite it

## 18. TTL and Expiry

If ttl_ms is present:

- the broker SHOULD compute expiration relative to timestamp_unix_ms
- expired messages SHOULD be dropped, rejected, or redirected according to policy
- expired messages SHOULD NOT be delivered as normal direct traffic unless explicitly configured

## 19. Traceability

The following metadata are RECOMMENDED for observability:

- message_id
- correlation_id
- trace_id
- producer_id
- topic
- delivery_mode
- tenant_id

A broker SHOULD emit traceable structured logs using these fields.

## 20. Security Considerations

abtp/1 does not mandate a single authentication mechanism.

Deployments MAY add:

- producer authentication
- session authentication
- envelope signing
- ACL-based topic authorization
- network-layer security
- broker-side policy checks

Future protocol revisions MAY standardize an auth block.

## 21. Compatibility Notes

Implementations using ZeroMQ and the Go pebbe/zmq4 wrapper SHOULD account for:

- libzmq availability
- development headers
- cgo requirements
- transport pattern selection appropriate to delivery mode

These implementation constraints are runtime/deployment concerns and are outside the wire semantics of abtp/1.

## 22. Non-Goals

The following are outside the scope of abtp/1:

- global ordering
- distributed transactions
- cluster replication protocol
- schema registry standardization
- exactly-once transport semantics
- consensus or quorum routing
- cryptographic proof systems

## 23. Future Extensions

Potential extensions include:

- binary envelope framing
- schema registry negotiation
- protocol-level auth and signatures
- partition-aware routing
- consumer capability negotiation
- cluster federation
- broker-to-broker replication
- adaptive compression negotiation
- streaming chunked payloads

## 24. Compliance Checklist

An implementation is minimally compliant with abtp/1 if it:

- accepts abtp/1
- validates required fields
- supports json
- supports lz4 and/or none
- supports topic-based routing
- supports canonical envelope normalization
- supports direct and/or fanout delivery semantics as documented
- preserves message_id
- preserves correlation_id if provided

## 25. Appendix A: Minimal Envelope

```json
{
  "spec_version": "abtp/1",
  "message_id": "msg_001",
  "producer_id": "svc.example",
  "topic": "example.created",
  "payload": {
    "id": "123"
  },
  "timestamp_unix_ms": 1741337000000,
  "deliver_at_unix_ms": 1741337060000,
  "next_attempt_unix_ms": 1741337090000
}
```

## 26. Appendix B: Recommended Full Envelope

```json
{
  "spec_version": "abtp/1",
  "message_id": "msg_01HV8J1K9C7K7Q...",
  "correlation_id": "corr_01HV8J1KE0H...",
  "producer_id": "svc.orders.api",
  "topic": "orders.created",
  "headers": {
    "content_type": "application/json",
    "codec": "json",
    "compression": "lz4",
    "priority": "high",
    "ttl_ms": 30000,
    "delivery_mode": "direct",
    "idempotency_key": "idem_order_1001",
    "tenant_id": "tenant_a",
    "trace_id": "trace_9a1"
  },
  "payload": {
    "order_id": "ord_1001",
    "customer_id": "cust_42"
  },
  "timestamp_unix_ms": 1741337000000,
  "deliver_at_unix_ms": 1741337060000,
  "next_attempt_unix_ms": 1741337090000
}
```
