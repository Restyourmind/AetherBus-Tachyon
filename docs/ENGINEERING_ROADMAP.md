# AetherBus-Tachyon Engineering Roadmap (Repo-Specific)

This roadmap is grounded in the current repository shape and documented protocol/performance intent.

## Current Snapshot (from existing code/docs)

- Broker ingress is currently ZeroMQ ROUTER/PUB with strict multipart parsing and full decompress+decode before route publish. (`internal/delivery/zmq/router.go`)
- Route lookup is exact-topic match in an in-memory ART store with RW lock protection. (`internal/repository/art_route_store.go`)
- Event routing use case resolves destination and logs decisions; direct forwarding/session lifecycle/ACK state are not yet implemented. (`internal/usecase/event_router.go`)
- Protocol docs define richer envelope semantics (delivery mode, ACK/NACK, sessions, inflight, retries) than currently implemented runtime behavior. (`docs/PROTOCOL.md`, `docs/DELIVERY.md`)
- Performance docs already call out a Phase 2 binary framing fast path and allocation reduction goals. (`docs/PERFORMANCE.md`)

---

## Prioritized Roadmap

## Do now (highest leverage, minimal architecture change)

### 1) Add broker-readable binary frame header + dual parser path
- **Goal:** Enable topic-based routing from compact header metadata without forcing full payload decode on hot path.
- **Impacted files/packages:**
  - `internal/delivery/zmq/router.go`
  - `internal/delivery/zmq/router_frames_test.go`
  - `internal/domain/events.go` (optional metadata extension only if needed)
  - `docs/PROTOCOL.md`
- **Risk:** **Medium** (wire compatibility + parser correctness).
- **Acceptance criteria:**
  - Existing JSON+LZ4 multipart path remains default-compatible.
  - Binary frame with version/topic/codec/compression/delivery-mode is accepted.
  - Broker performs route lookup using header topic before payload decode.
  - Invalid header variants are rejected with deterministic parser errors.
  - Tests cover valid/invalid frame parsing and route lookup behavior for both paths.

### 2) Route decision outcomes + explicit unroutable handling contract
- **Goal:** Convert current log-only routing into explicit outcomes that can feed counters and retry/dlq decisions later.
- **Impacted files/packages:**
  - `internal/usecase/event_router.go`
  - `internal/domain/interfaces.go`
  - `internal/repository/art_route_store.go` (if wildcard/prefix matching is staged)
  - `docs/DELIVERY.md`
- **Risk:** **Low/Medium** (interface change ripple).
- **Acceptance criteria:**
  - Publish returns structured routing result (`routed`, `unroutable`, destination).
  - Deterministic handling for no-route case (policy documented + tested).
  - No change to external CLI/runtime bootstrap behavior required.

### 3) Add repeatable benchmark matrix for baseline vs fast path
- **Goal:** Make performance claims measurable and regression-detectable.
- **Impacted files/packages:**
  - `cmd/tachyon-bench/main.go`
  - `scripts/` (benchmark runner/report helper)
  - `docs/PERFORMANCE.md`
- **Risk:** **Low** (tooling/reporting only).
- **Acceptance criteria:**
  - Bench scenario set includes: default JSON+LZ4, binary fast path, compress on/off.
  - Results report throughput, p50/p95/p99, alloc/op, bytes/op.
  - A standard invocation and output format is documented for CI/perf runs.

### 4) Harden transport/media error taxonomy
- **Goal:** Distinguish malformed frame, unsupported codec/compression, decode failure, and route miss.
- **Impacted files/packages:**
  - `internal/delivery/zmq/router.go`
  - `pkg/errors/errors.go`
  - `internal/media/*.go`
- **Risk:** **Low**.
- **Acceptance criteria:**
  - Error classes are typed/sentinel and observable in logs/metrics.
  - Parser and media failures do not crash the loop and are counted separately.

### 5) Lightweight metrics scaffolding in hot path
- **Goal:** Add counters/timers aligned with existing performance doc metric names.
- **Impacted files/packages:**
  - `internal/delivery/zmq/router.go`
  - `internal/usecase/event_router.go`
  - `cmd/tachyon/main.go` (exposure hook)
- **Risk:** **Low/Medium** (avoid adding contention in hot path).
- **Acceptance criteria:**
  - Minimum counters: ingress, routed, unroutable, decode errors.
  - Timings: decode and route lookup latency.
  - Metrics names align with `docs/PERFORMANCE.md`.

---

## Do next (close protocol + delivery semantics gaps)

### 6) Session registry + consumer capability registration
- **Protocol/delivery gap addressed:** Session model and registration semantics are documented but not implemented.
- **Impacted files/packages:** `internal/domain`, `internal/usecase`, `internal/delivery/zmq`, `docs/DELIVERY.md`.
- **Risk:** **Medium**.
- **Acceptance criteria:** In-memory session table with heartbeat expiry + capability map (`supports_ack`, codec/compression support).

### 7) Direct delivery ACK/NACK inflight state machine (in-memory first)
- **Gap addressed:** Documented ACK/NACK and inflight lifecycle not present in runtime.
- **Impacted files/packages:** `internal/domain`, `internal/usecase`, `internal/delivery/zmq`, `docs/DELIVERY.md`.
- **Risk:** **Medium/High**.
- **Acceptance criteria:** Dispatch->acked/nacked/expired transitions with idempotent ACK handling and tests.

### 8) Route matching upgrade path (exact + wildcard policy)
- **Gap addressed:** docs describe richer routing semantics than exact-only matching.
- **Impacted files/packages:** `internal/repository/art_route_store.go`, tests, `docs/PROTOCOL.md`.
- **Risk:** **Medium** (determinism + precedence rules).
- **Acceptance criteria:** deterministic precedence and benchmarked lookup impact.

### 9) Dead-letter + retry queue scaffolding
- **Durability/semantics gap addressed:** NACK retry and DLQ are documented as SHOULD/MAY behavior.
- **Impacted files/packages:** `internal/usecase`, `internal/domain`, potentially `internal/repository` for queue backing.
- **Risk:** **Medium**.
- **Acceptance criteria:** retry policy config + DLQ emission path with counters.

---

## Later (durability, scale, federation, intent-aware runway)

### 10) Pluggable durability layer (WAL first, queue snapshots second)
- **Durability gap addressed:** Current runtime is in-memory only.
- **Impacted files/packages:** new `internal/durability/*`, integration in `internal/app/runtime.go`.
- **Risk:** **High** (ordering, fsync tradeoffs, recovery logic).
- **Acceptance criteria:** crash recovery for inflight/retry metadata and at-least-once replay boundary documented.

### 11) Federation/bridge control plane minimum
- **Scalability/federation gap addressed:** Bridge mode exists in docs, no practical multi-broker control plane yet.
- **Impacted files/packages:** `internal/usecase`, `internal/delivery`, new federation package, `docs/PROTOCOL.md`.
- **Risk:** **High**.
- **Acceptance criteria:** broker-to-broker forwarding with loop prevention metadata and route namespace boundaries.

### 12) Intent-aware coordination scaffolding (minimum viable)
- **Future runway goal:** prepare policy hooks without rewriting broker core.
- **Impacted files/packages:**
  - `internal/domain` (policy/intents structs)
  - `internal/usecase/event_router.go` (pre-route policy hook)
  - `config/config.go` (feature flag / policy profile)
- **Risk:** **Medium**.
- **Acceptance criteria:**
  - pre-route hook interface with no-op default,
  - deterministic policy input schema (topic, headers, delivery mode, tenant),
  - capability to run in shadow mode (decision logged, not enforced).

---

## Top 3 Tasks Codex Should Implement First

### Task 1: Binary header fast path with backward-compatible fallback
- **Why first:** Unblocks performance and protocol alignment with minimal blast radius.
- **Exact files likely to change:**
  - `internal/delivery/zmq/router.go`
  - `internal/delivery/zmq/router_frames_test.go`
  - `docs/PROTOCOL.md`

### Task 2: Structured route outcome + unroutable policy
- **Why second:** Clarifies core broker semantics and enables metrics/retry later.
- **Exact files likely to change:**
  - `internal/usecase/event_router.go`
  - `internal/domain/interfaces.go`
  - `docs/DELIVERY.md`

### Task 3: Benchmark matrix + reproducible reporting for default vs fast path
- **Why third:** Prevents regressions and makes future optimizations evidence-driven.
- **Exact files likely to change:**
  - `cmd/tachyon-bench/main.go`
  - `docs/PERFORMANCE.md`
  - `scripts/benchmark_matrix.sh` (new)

---

## Tradeoff Notes (short)

- **Dual-path parsing vs single parser rewrite:** keeping both legacy envelope and binary-header paths avoids migration risk at the cost of short-term code complexity.
- **In-memory first for sessions/inflight:** fastest way to validate semantics, but durability guarantees remain limited until WAL/snapshots arrive.
- **Deterministic routing before advanced matching:** exact/wildcard precedence must be explicit early to avoid unpredictable behavior under federation.
