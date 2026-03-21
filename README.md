# AetherBus-Tachyon

**AetherBus-Tachyon** is a high-performance, lightweight message broker designed for the AetherBus ecosystem. It serves as a central routing point for events, ensuring efficient and reliable delivery from producers to consumers.

This project is currently under active development and aims to be a foundational component for building scalable, event-driven architectures.

## ✨ Features

- **High-Performance Routing:** Utilizes an **Adaptive Radix Tree** for fast and efficient topic-based routing, ensuring low-latency message delivery even with a large number of routes.
- **Extensible Media Handling:** Supports pluggable codecs and compressors to optimize message payloads.
  - **Codec:** Defaulting to `JSON` for structured data.
  - **Compressor:** Defaulting to `LZ4` for high-speed compression and decompression.
- **ZeroMQ Integration:** Built on top of ZeroMQ (using `pebbe/zmq4`), leveraging its powerful and battle-tested messaging patterns (ROUTER-DEALER, PUB-SUB).
- **Clean Architecture:** Organized with a clear separation of concerns (domain, use case, delivery, repository, media, app runtime) for maintainability and testability.
- **Continuous Integration:** Includes a **GitHub Actions workflow** that automatically builds the application and runs tests (including race detection) on every push and pull request to the `main` branch.

## 🚀 Getting Started

### Prerequisites

- [Go](https://golang.org/dl/) (version 1.22 or later)
- [ZeroMQ](https://zeromq.org/download/) (version 4.x)

On Debian/Ubuntu, you can install ZeroMQ development libraries with:

```bash
sudo apt-get update && sudo apt-get install -y libzmq3-dev
```

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/aetherbus/aetherbus-tachyon.git
   cd aetherbus-tachyon
   ```

2. **Install dependencies:**
   ```bash
   go mod tidy
   ```

3. **Run the server:**
   ```bash
   go run ./cmd/tachyon
   ```

The server will start and bind to the addresses specified in the configuration (defaults to `tcp://127.0.0.1:5555` for the ROUTER and `tcp://127.0.0.1:5556` for the PUB socket).

Optional direct-delivery durability can be enabled with:

- `WAL_ENABLED=true`
- `WAL_PATH=./data/direct_delivery.wal`

When enabled, direct messages that require ACK are appended to an append-only WAL before dispatch, ACK marks entries committed, terminal outcomes are marked dead-lettered, and remaining unfinalized records are replayed when matching consumers reconnect after restart.


Dead-letter records are now materialized in a structured DLQ store at `WAL_PATH.dlq`, while broker-scheduled replays are written to `WAL_PATH.scheduled`. Operators can browse and inspect DLQ entries, then replay or purge them with explicit confirmation and exact target matching so replay cannot silently change the original consumer/topic boundary.

### DLQ operator workflow

```bash
# Browse dead letters
go run ./cmd/tachyon dlq list --consumer worker-1

# Inspect a single record
go run ./cmd/tachyon dlq inspect --id msg-123

# Replay only when the original target is restated exactly
go run ./cmd/tachyon dlq replay --ids msg-123 --target-consumer worker-1 --target-topic orders.created --confirm REPLAY

# Purge an acknowledged bad record
go run ./cmd/tachyon dlq purge --ids msg-123 --confirm PURGE
```

The demo control-surface gateway exposes matching admin endpoints under `/api/admin/dlq/*`. Set `ADMIN_TOKEN` to require the `X-Admin-Token` header for browse, inspect, replay, and purge requests. Replay and purge responses include requested/replayed-or-purged counts plus per-record failure details.

Direct-delivery admission control defaults are intentionally conservative and can be tuned with:

- `MAX_INFLIGHT_PER_CONSUMER` (default `1024`)
- `MAX_PER_TOPIC_QUEUE` (default `256`)
- `MAX_QUEUED_DIRECT` (default `4096`)
- `MAX_GLOBAL_INGRESS` (default `8192`)

When limits are reached, direct messages are deferred or dropped with explicit broker counters (`deferred`, `throttled`, `dropped`).

Direct-delivery admission control defaults are intentionally conservative and can be tuned with:

- `MAX_INFLIGHT_PER_CONSUMER` (default `1024`)
- `MAX_PER_TOPIC_QUEUE` (default `256`)
- `MAX_QUEUED_DIRECT` (default `4096`)
- `MAX_GLOBAL_INGRESS` (default `8192`)

When limits are reached, direct messages are deferred or dropped with explicit broker counters (`deferred`, `throttled`, `dropped`).

## 🧰 Build recovery under restricted network environments

This repository may require external Go module resolution to complete full recovery of
`go.mod` / `go.sum` and to run `go test ./...`.

To make troubleshooting easier, use the recovery helper:

### Offline-safe checks

Use this mode when your environment cannot reach external Go module infrastructure:

```bash
bash scripts/go_mod_recovery.sh check
```

This mode is useful for:

- validating repository structure
- checking command entrypoints
- running package-level tests for explicitly selected offline-safe packages

By default, it tests:

```bash
go test ./cmd/aetherbus
```

### Full online recovery

Use this mode on a machine or CI runner with module download access:

```bash
bash scripts/go_mod_recovery.sh recover
```

This runs:

- `go mod download`
- `go mod tidy`
- `go build ./...`
- `go test ./...`

### Diagnostics

To inspect the current Go environment:

```bash
bash scripts/go_mod_recovery.sh doctor
```

### Why this split exists

Some failures are caused by local source issues, while others are caused by incomplete
module metadata (`go.sum`) that cannot be repaired without downloading or verifying
dependencies.

In restricted-network environments, the offline-safe path helps confirm whether a failure
is local to the codebase or caused by module resolution limits.

If `recover` fails with module download/verification errors in restricted environments,
treat that as an environment limitation first (not an automatic source regression).


## ⚡ Benchmark harness

A first-class benchmark harness is available via `cmd/tachyon-bench`:

```bash
# direct mode with ACK
go run ./cmd/tachyon-bench harness --mode direct-ack --payload-class small --compress=true --duration 20s

# fanout benchmark
go run ./cmd/tachyon-bench harness --mode fanout --fanout-subs 8 --payload-class medium --compress=false --duration 20s

# mixed topic distribution
go run ./cmd/tachyon-bench harness --mode mixed --mixed-topics 8 --payload-class medium --compress=true --duration 30s

# CI-friendly matrix
go run ./cmd/tachyon-bench matrix --duration 10s --connections 2
```

The harness reports p50/p95/p99 latency, throughput, CPU usage, memory RSS, and allocations/op. See `docs/PERFORMANCE.md` for full interpretation guidance and comparison workflow.

## 🏗️ System Architecture Diagram

```mermaid
flowchart TD
    subgraph Runtime[Broker Runtime]
        CLI[cmd/tachyon<br/>cmd/aetherbus-node]
        CFG[config.Config<br/>Bind + durability settings]
        APP[internal/app.Runtime]
        ZMQ[delivery/zmq.Router<br/>ROUTER + PUB sockets]
        CODEC[media.JSONCodec]
        COMP[media.LZ4Compressor]
        UC[usecase.EventRouter]
    end

    subgraph MemoryDB[Logical Data Store (In-Memory)]
        ROUTES[(Route Store<br/>ART index)]
        SESSIONS[(Consumer Sessions<br/>consumer_id -> session)]
        INFLIGHT[(Inflight Deliveries<br/>message_id -> state)]
    end

    subgraph DurableDB[Logical Durable Store]
        WAL[(Delivery WAL<br/>append-only JSONL)]
    end

    PROD[Producers / DEALER clients]
    CONS[Consumers / SUB or direct clients]

    CLI --> CFG --> APP
    APP --> ZMQ
    APP --> UC
    APP --> ROUTES
    APP --> SESSIONS
    APP --> INFLIGHT
    APP --> WAL
    APP --> CODEC
    APP --> COMP

    PROD -->|multipart frames| ZMQ
    ZMQ -->|decode + validate| UC
    UC -->|topic lookup| ROUTES
    ZMQ -->|register / heartbeat / capability| SESSIONS
    ZMQ -->|dispatch + ack tracking| INFLIGHT
    INFLIGHT -->|persist required direct deliveries| WAL
    WAL -->|replay unfinalized entries| INFLIGHT
    UC -->|fanout route| ZMQ
    INFLIGHT -->|eligible direct message| ZMQ
    ZMQ -->|topic payload / direct frames| CONS
```

### Runtime composition

- **Command layer:** `cmd/tachyon` and `cmd/aetherbus-node` load configuration, durability flags, and start the broker runtime.
- **Configuration layer:** `config.Config` and environment variables define bind addresses, admission limits, timeout behavior, and WAL activation.
- **Composition layer:** `internal/app.Runtime` wires transport, routing, session tracking, inflight control, and persistence together.
- **Transport layer:** `internal/delivery/zmq.Router` owns the ZeroMQ ROUTER/PUB sockets, parses frames, handles consumer registration/heartbeats, and emits direct/fanout deliveries.
- **Media layer:** `internal/media.JSONCodec` and `internal/media.LZ4Compressor` handle event encoding and payload compression.
- **Application layer:** `internal/usecase.EventRouter` resolves fanout routes and coordinates routing decisions with broker state.
- **Logical data layer:** the runtime operates over four logical data structures — ART route index, consumer session table, inflight delivery table, and append-only WAL.

### Message + state flow

1. **Producers** publish multipart frames to the ZeroMQ ROUTER.
2. **`delivery/zmq.Router`** validates frame shape, decodes/compresses payloads via the media layer, and forwards routing work into the application flow.
3. **`usecase.EventRouter`** resolves topic matches through the **route store (ART)** for fanout delivery.
4. **Consumer registration and heartbeat traffic** updates the **consumer session table**, which tracks active direct-delivery capability.
5. **Direct deliveries** create or update **inflight delivery records** so ACK/NACK, retry, timeout, and dead-letter behavior can be evaluated.
6. When ACK durability is required, the broker appends dispatch state to the **delivery WAL**, finalizes records on terminal outcomes, and replays unfinalized entries after restart.
7. The transport layer emits the final topic payload or direct-delivery frame back to **subscribers / workers**.

This version of the diagram is aligned with the current logical storage model described below, so the architecture view now reflects both the runtime components and the broker-managed data structures.

## 🗃️ Data Storage Structure (Current)

The broker currently uses a **hybrid in-memory + append-only WAL** model instead of a full relational database. The logical data structures are:

### 1) Route store (ART + persisted catalog)

- Purpose: topic-to-destination lookup for routing decisions
- Shape: adaptive radix tree in memory plus a versioned JSON route catalog on disk
- Lifecycle: loaded from `ROUTE_CATALOG_PATH` on startup, mutated in memory during runtime, persisted after route changes

| Field | Type | Description |
|---|---|---|
| `topic` | string | Topic key used for route lookup |
| `destination` | string | Target consumer/node identifier |

### 2) Direct consumer session table (in-memory + resumable snapshots)

- Purpose: active consumer capability/session tracking for direct delivery
- Shape: map keyed by `consumer_id`
- Lifecycle: active state lives in memory; resumable metadata can be restored from WAL-backed session snapshots

| Field | Type | Description |
|---|---|---|
| `consumer_id` | string | Stable consumer identity |
| `session_id` | string | Active session identifier |
| `socket_identity` | bytes | ZeroMQ ROUTER identity for direct send |
| `supports_ack` | bool | Whether consumer participates in ACK flow |
| `subscriptions` | set[string] | Topics subscribed for direct delivery |
| `max_inflight` | int | Consumer inflight window cap |
| `inflight_count` | int | Current number of inflight messages |
| `last_heartbeat` | timestamp | Last heartbeat seen from consumer |

### 3) Inflight + scheduled delivery tables

- Purpose: ACK/NACK, retry, timeout, dead-letter control, and delayed delivery scheduling for direct mode
- Shape: maps keyed by `message_id` plus an ordered scheduled queue keyed by `deliver_at`
- Lifecycle: inflight state lives in memory; retry/delayed queue ordering can be restored from WAL-backed scheduled entries

| Field | Type | Description |
|---|---|---|
| `message_id` | string | Message identity used for ACK/NACK correlation |
| `consumer_id` | string | Target consumer for this attempt |
| `session_id` | string | Session that received the dispatch |
| `topic` | string | Routed topic |
| `payload` | bytes | Original payload bytes |
| `attempt` | int | Delivery attempt count |
| `dispatched_at` | timestamp | Dispatch time used for timeout evaluation |
| `status` | enum | `dispatched` / `acked` / `nacked` / `expired` / `retry_scheduled` / `dead_lettered` |

### 4) Delivery WAL (append-only file)

- Purpose: durability for direct messages requiring ACK
- Storage: JSON-line append log (default path `./data/direct_delivery.wal`)
- Recovery: uncommitted dispatch records are replayed when matching consumers re-register

| Field | Type | Description |
|---|---|---|
| `type` | enum | `dispatched`, `committed`, or `dead_lettered` |
| `message_id` | string | Message identity |
| `consumer` | string | Consumer identity for dispatched records |
| `session_id` | string | Session ID for dispatched records |
| `topic` | string | Topic for dispatched records |
| `payload` | bytes | Payload for dispatched records |
| `attempt` | int | Attempt number for dispatched records |

> Note: if you need SQL/NoSQL persistence in the future, this model can be mapped directly to tables/collections (`routes`, `consumer_sessions`, `inflight_messages`, `delivery_wal`) while preserving existing runtime semantics.


### Durability guarantees and non-goals

**Guarantees (when `WAL_ENABLED=true`):**
- Direct deliveries that require ACK are written to WAL before broker send.
- ACK and terminal dead-letter outcomes finalize WAL records, preventing replay.
- On restart, only unfinalized direct deliveries are replayed, preserving `message_id`, `consumer_id`, topic, payload, and attempt counter.

**Non-goals / current limitations:**
- WAL is local append-only file storage (single-node durability, no replication or consensus).
- WAL replay is scoped to consumers that re-register; replay is not global fanout recovery.
- WAL file compaction/retention is not implemented in this version.

## 💡 Function Proposals & Future Extensions

### English

- **Priority-aware Delivery Classes:** Introduce weighted priority classes so operator commands, retries, and bulk sync traffic can coexist with predictable fairness.
- **Dead-letter Inspection API + Replay Console:** Add a safe API/UI for browsing dead-lettered items, replaying subsets, and annotating operator decisions.
- **Replay Audit Trail:** Persist operator-triggered replay actions and delivery state transitions for compliance and post-incident analysis.
- **Tenant-aware Quotas and Isolation:** Extend route namespaces with per-tenant queue budgets, metrics, and admission-control policy.
- **Geo-redundant Durability:** Replicate WAL, route catalog, and delayed queue state to a standby node or object storage target.
- **Analytics Export Pipeline:** Stream route, inflight, backlog, and WAL transitions into PostgreSQL, ClickHouse, or warehouse tooling.
- **SLO-driven Autoscaling Signals:** Emit broker pressure indicators that can feed orchestration or capacity planning automation.
- **AuthN/AuthZ Control Plane:** Add operator authentication, signed control messages, and role-based access for administrative APIs.

### ภาษาไทย

- **Priority-aware Delivery Classes:** เพิ่มระดับความสำคัญของการส่งแบบถ่วงน้ำหนัก เพื่อให้คำสั่งของผู้ปฏิบัติงาน งาน retry และทราฟฟิกปริมาณมากอยู่ร่วมกันได้อย่างเป็นธรรม
- **Dead-letter Inspection API + Replay Console:** เพิ่ม API/UI สำหรับดูรายการ dead-letter, replay เป็นชุดย่อย และบันทึกเหตุผลของผู้ปฏิบัติงานอย่างปลอดภัย
- **Replay Audit Trail:** บันทึกประวัติการ replay ที่ผู้ปฏิบัติงานสั่ง รวมถึง state transition ของการส่ง เพื่อรองรับ compliance และ post-incident analysis
- **Tenant-aware Quotas and Isolation:** ขยาย route namespace ให้รองรับ quota, metrics และ admission-control policy แยกตาม tenant
- **Geo-redundant Durability:** ทำสำเนา WAL, route catalog และสถานะ delayed queue ไปยัง standby node หรือ object storage
- **Analytics Export Pipeline:** ส่งการเปลี่ยนแปลงของ route, inflight, backlog และ WAL ไปยัง PostgreSQL, ClickHouse หรือ warehouse tooling
- **SLO-driven Autoscaling Signals:** ปล่อยสัญญาณแรงกดดันของ broker เพื่อนำไปใช้กับระบบ orchestration หรือ automation ด้าน capacity planning
- **AuthN/AuthZ Control Plane:** เพิ่มการยืนยันตัวตนของผู้ปฏิบัติงาน, signed control messages และสิทธิ์แบบ role-based สำหรับ administrative APIs

## 📘 Deep Architecture & Protocol Docs

To move AetherBus-Tachyon toward a production-grade broker spec, the repository now defines deeper system contracts in dedicated documents:

- [Protocol Specification v1 (draft)](docs/PROTOCOL.md)
- [Routing Semantics (ART)](docs/ROUTING.md)
- [Delivery Semantics (ACK/Retry/Backpressure/DLQ)](docs/DELIVERY.md)
- [Performance Model and Benchmarking](docs/PERFORMANCE.md)
- [Rust Fast-path Sidecar Scaffold](docs/FASTPATH_SIDECAR.md)
- [Intent Graph Algorithm Specification](docs/INTENT_GRAPH_ALGORITHM_SPEC.md)
- [Intent Core Phase 1 (single-node scaffold)](docs/INTENT_CORE_PHASE1.md)

### Delivery timeout configuration

Direct-delivery ACK tracking supports timeout-driven retries. Configure via:

- `DELIVERY_TIMEOUT_MS` (default: `30000`)

If an inflight direct message is not ACKed before this timeout, the broker treats it as retryable, retries within the direct retry budget, and dead-letters it once retries are exhausted.

These docs lock down the key areas that must be explicit for production evolution:

- Protocol envelope and control messages (register/ack/nack)
- Topic grammar and wildcard matching precedence
- Delivery guarantees and retry/dead-letter behavior
- Operational model (backpressure, failure handling, observability)

## Rust fast-path adapter boundary (scaffold)

The repository includes a scaffolded Rust sidecar (`rust/tachyon-fastpath`) and a narrow Go adapter boundary (`internal/fastpath`).

- Default runtime mode remains **Go-only** for backward-compatible behavior.
- Rust sidecar is an explicit opt-in integration path for large payload framing/compression offload.
- The first iteration intentionally uses a process boundary (Unix socket sidecar) to minimize risk to broker delivery semantics.

Fast-path sidecar configuration knobs are available for explicit developer testing:

- `FASTPATH_SIDECAR_ENABLED` (default `false`)
- `FASTPATH_SOCKET_PATH` (default `/tmp/tachyon-fastpath.sock`)
- `FASTPATH_CUTOVER_BYTES` (default `262144`)
- `FASTPATH_REQUIRE` (default `false`)
- `FASTPATH_FALLBACK_TO_GO` (default `true`)

See `docs/FASTPATH_SIDECAR.md` for architecture, activation criteria, and measurable migration candidates.

## Specifications

- [Protocol Specification](docs/PROTOCOL.md)
- [Routing Specification](docs/ROUTING.md)
- [Delivery Specification](docs/DELIVERY.md)
- [Intent Graph Algorithm Specification](docs/INTENT_GRAPH_ALGORITHM_SPEC.md)
- [Intent Core Phase 1](docs/INTENT_CORE_PHASE1.md)
