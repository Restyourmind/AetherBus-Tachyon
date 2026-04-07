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


Dead-letter records are now materialized in a structured DLQ store at `WAL_PATH.dlq`, while broker-scheduled replays are written to `WAL_PATH.scheduled`. Administrative mutations are recorded in a separate append-only audit chain at `WAL_PATH.audit`, so compliance retention can differ from hot-path dispatch durability. Operators can browse and inspect DLQ entries, then replay or purge them with explicit confirmation and exact target matching so replay cannot silently change the original consumer/topic boundary.

### DLQ operator workflow

```bash
# Browse dead letters
go run ./cmd/tachyon dlq list --consumer worker-1

# Inspect a single record
go run ./cmd/tachyon dlq inspect --id msg-123

# Replay only when the original target is restated exactly
go run ./cmd/tachyon dlq replay --ids msg-123 --target-consumer worker-1 --target-topic orders.created --actor ops@example.com --reason "customer-approved replay" --confirm REPLAY

# Manually quarantine a message into the dead-letter store
go run ./cmd/tachyon dlq dead-letter --id msg-123 --consumer worker-1 --topic orders.created --payload "raw-body" --actor ops@example.com --reason "manual quarantine"

# Purge an acknowledged bad record
go run ./cmd/tachyon dlq purge --ids msg-123 --actor ops@example.com --mutation-reason "retention cleanup" --confirm PURGE

# Query immutable audit history by message, actor, or time window
go run ./cmd/tachyon dlq audit --id msg-123 --actor ops@example.com --start 2026-03-21T00:00:00Z --end 2026-03-22T00:00:00Z
```

The demo control-surface gateway exposes matching admin endpoints under `/api/admin/dlq/*` plus audit queries at `/api/admin/audit/events`. Set `ADMIN_TOKEN` to require the `X-Admin-Token` header for browse, inspect, replay, manual dead-letter, purge, and audit requests. Replay and purge responses include requested/replayed-or-purged counts plus per-record failure details.

### Audit retention and tamper evidence

- `WAL_PATH.audit` is intentionally separate from `WAL_PATH`, `WAL_PATH.dlq`, and `WAL_PATH.scheduled` so compliance retention can be longer than dispatch/replay retention.
- Each audit line stores actor, timestamp, operation, target message IDs, requested reason, prior state, resulting state, the previous record hash, and the current record hash.
- The `prev_hash` → `hash` chain is meant to make offline tampering detectable during export or forensic review; it is not a substitute for WORM/object-lock storage.
- Operationally, treat the audit log as append-only, rotate it with retention tooling that preserves line order, and export it to immutable storage when regulatory retention exceeds local disk policy.

Direct-delivery admission control defaults are intentionally conservative and can be tuned with:

- `MAX_INFLIGHT_PER_CONSUMER` (default `1024`)
- `MAX_PER_TOPIC_QUEUE` (default `256`)
- `MAX_QUEUED_DIRECT` (default `4096`)
- `MAX_GLOBAL_INGRESS` (default `8192`)
- `TENANT_QUOTAS_JSON` (optional per-tenant quota overrides)

Example `TENANT_QUOTAS_JSON`:

```json
{
  "tenant-a": { "max_inflight": 256, "max_queued": 2048, "max_ingress": 4096 },
  "tenant-b": { "max_queued": 512 }
}
```

Each field is optional per tenant. Any configured positive value overrides broker defaults for that tenant only.

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

### Policy simulation sandbox (non-writing rehearsal)

When you need to rehearse policy/ruleset behavior without mutating module files, use
`--simulate-policy`:

```bash
bash scripts/go_mod_recovery.sh --simulate-policy --policy-ruleset pr-healing recover
```

This prints the planned recovery commands and exits without running write-capable steps.

### Workflow drift dashboard export (JSON + CSV)

To emit trend bundles consumable by dashboard tooling:

```bash
bash scripts/go_mod_recovery.sh --drift-export-dir artifacts/recovery recover
```

The command writes:

- `trend_bundle.json`
- `trend_bundle.csv`

Both include UTC timestamp, mode, status, policy-ruleset label, simulation status, and
`go.mod`/`go.sum` hashes so nightly and PR healing jobs can track drift over time.

### Controlled auto-rollback hook

If post-fix verification (`go test ./...`) fails, you can opt into a rollback command:

```bash
bash scripts/go_mod_recovery.sh \
  --auto-rollback \
  --auto-rollback-cmd "git checkout -- go.mod go.sum" \
  recover
```

Rollback execution is disabled by default and only runs when explicitly enabled.

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

## 🏗️ System Architecture Diagram (Database + Module Contracts)

> เป้าหมาย: ทำให้ภาพสถาปัตยกรรมอ้างอิง “โครงสร้างข้อมูลที่ persist จริง” และ “เส้นทางควบคุมระหว่างโมดูล” เพื่อไม่ปะปนกับรายการงานที่ปิดแล้ว

```mermaid
erDiagram
    ROUTE_CATALOG_SNAPSHOT {
        int version PK
    }

    ROUTE_ENTRY {
        string pattern PK
        string destination_id PK
        string route_type
        int priority
        bool enabled
        string tenant
    }

    SESSION_SNAPSHOT {
        string session_id PK
        string consumer_id PK
        string tenant_id
        string subscriptions_json
        datetime last_heartbeat
        int max_inflight
        bool supports_ack
        bool resumable
    }

    WAL_RECORD {
        string message_id PK
        string type
        string consumer_id
        string session_id
        string tenant_id
        string topic
        uint64 enqueue_sequence
        int attempt
    }

    SCHEDULED_MESSAGE {
        uint64 sequence PK
        string message_id
        string tenant_id
        string topic
        string destination_id
        string route_type
        uint64 enqueue_sequence
        int delivery_attempt
        datetime deliver_at
        string reason
    }

    DLQ_RECORD {
        string message_id PK
        string consumer_id
        string session_id
        string tenant_id
        string topic
        uint64 enqueue_sequence
        int attempt
        string reason
        datetime dead_lettered_at
        int replay_count
    }

    AUDIT_EVENT {
        string event_id PK
        string actor
        datetime timestamp
        string operation
        string target_message_ids_json
        string requested_reason
        string prev_hash
        string hash
    }

    ROUTE_CATALOG_SNAPSHOT ||--o{ ROUTE_ENTRY : contains
    SESSION_SNAPSHOT }o--o{ WAL_RECORD : replay_join
    WAL_RECORD ||--o{ SCHEDULED_MESSAGE : retry_schedule
    WAL_RECORD ||--o| DLQ_RECORD : terminal_failure
    DLQ_RECORD ||--o{ AUDIT_EVENT : mutation_trail
```

### High-Level Augmented Perception Layer (L9 blueprint)

```mermaid
flowchart LR
    U[Voice / Intent / App Request] --> G[Genesis
Intent -> Visual Plan]
    G --> M[Manifest
Intent+Visual+Scene Contract]
    E[Environment Sensing] --> B[BioVision
Day/Night/Fog/Rain/Motion]
    B --> GV[Governor
Brightness/Curfew/Geo-fence]
    B --> P[PRGX
Policy+Safety Gate+Audit]
    M --> GV
    M --> P
    GV --> T[Tachyon Runtime
Realtime Stream + Time Sync]
    P --> T
    T --> X[Edge/WASM Runtime]
    X --> O1[AR/VR Glasses]
    X --> O2[Projector / Building Facade]
    X --> O3[Screen / Legacy OS Surface]
```

1. **Producers** publish multipart frames to the ZeroMQ ROUTER.
2. **`delivery/zmq.Router`** validates frame shape, decodes/compresses payloads via the media layer, and forwards routing work into the application flow.
3. **`usecase.EventRouter`** resolves topic matches through the **route store (ART)** for fanout delivery.
4. **Consumer registration and heartbeat traffic** updates the **consumer session table**, which tracks active direct-delivery capability.
5. **Direct deliveries** create or update **inflight delivery records** so ACK/NACK, retry, timeout, and dead-letter behavior can be evaluated.
6. When ACK durability is required, the broker appends dispatch state to **segmented WAL files**, snapshots resumable sessions, and persists scheduled retries for restart recovery.
7. Terminal failures are materialized into the **DLQ store**, while replay/purge/manual dead-letter mutations are chained into the **admin audit log** for forensic review.
8. The transport layer emits the final topic payload or direct-delivery frame back to **subscribers / workers**.

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
- Dispatch WAL compaction/retention is not implemented in this version.
- Audit retention is operator-managed and can be longer than WAL retention because the audit chain is stored separately in `WAL_PATH.audit`.

## 💡 Function Proposals & Future Extensions

### English

- **Priority-aware Delivery Classes:** Introduce weighted priority classes so operator commands, retries, and bulk sync traffic can coexist with predictable fairness.
- **Tenant-aware Quotas and Isolation:** ✅ Route namespaces are tenant-scoped, tenant metrics are emitted, and per-tenant admission quotas can now be set through `TENANT_QUOTAS_JSON`.
- **Geo-redundant Durability:** Replicate WAL, route catalog, and delayed queue state to a standby node or object storage target.
- **SLO-driven Autoscaling Signals:** Emit broker pressure indicators that can feed orchestration or capacity planning automation.
- **AuthN/AuthZ Control Plane:** Add operator authentication, signed control messages, and role-based access for administrative APIs.

### ภาษาไทย

- **Priority-aware Delivery Classes:** เพิ่มระดับความสำคัญของการส่งแบบถ่วงน้ำหนัก เพื่อให้คำสั่งของผู้ปฏิบัติงาน งาน retry และทราฟฟิกปริมาณมากอยู่ร่วมกันได้อย่างเป็นธรรม
- **Tenant-aware Quotas and Isolation:** ✅ route namespace แยกตาม tenant อยู่แล้ว พร้อม tenant metrics และสามารถกำหนด admission quota แยก tenant ผ่าน `TENANT_QUOTAS_JSON` ได้แล้ว
- **Geo-redundant Durability:** ทำสำเนา WAL, route catalog และสถานะ delayed queue ไปยัง standby node หรือ object storage
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
