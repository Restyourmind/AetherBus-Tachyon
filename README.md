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

### 1) Route store (in-memory ART)

- Purpose: topic-to-destination lookup for routing decisions
- Shape: key-value map over ART nodes
- Lifecycle: runtime memory only (reconstructed from bootstrap routes on restart)

| Field | Type | Description |
|---|---|---|
| `topic` | string | Topic key used for route lookup |
| `destination` | string | Target consumer/node identifier |

### 2) Direct consumer session table (in-memory)

- Purpose: active consumer capability/session tracking for direct delivery
- Shape: map keyed by `consumer_id`
- Lifecycle: runtime memory only

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

### 3) Inflight delivery table (in-memory)

- Purpose: ACK/NACK, retry, timeout, and dead-letter control for direct mode
- Shape: map keyed by `message_id`
- Lifecycle: runtime memory; can be repopulated from WAL replay for unacked messages

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

> This section intentionally lists **forward-looking proposals only**. Items that are already implemented have been removed so this backlog stays focused on future work.

### English

- **Route Bootstrap Persistence:** Add a persistent route catalog so the ART route store can be restored automatically without manual bootstrap configuration.
- **Session Snapshot & Warm Restart:** Persist consumer session metadata and resumable capability hints to shorten recovery time after broker restarts.
- **Delayed Delivery / Scheduled Publish:** Support future delivery timestamps and retry schedules as first-class broker behavior.
- **Priority Queues for Direct Delivery:** Let operators assign message priority classes so urgent commands can bypass bulk background traffic.
- **Dead-letter Inspection API:** Expose a dedicated API/CLI for browsing, replaying, and purging dead-lettered records safely.
- **Replay Audit Trail:** Record operator-triggered replay actions and delivery state transitions for compliance and debugging.
- **Tenant-aware Route Namespaces:** Isolate route lookup, quotas, and observability by tenant while preserving shared broker infrastructure.
- **Adaptive Backpressure Policies:** Dynamically tune inflight and queue limits based on consumer lag, retry rate, and broker memory pressure.
- **Geo-redundant WAL Replication:** Replicate WAL segments to a standby broker/object store for stronger disaster recovery.
- **SQL/Analytics Export Pipeline:** Stream route, inflight, and WAL state changes into PostgreSQL, ClickHouse, or a warehouse for reporting.
- **Operational Admin UI:** Build a lightweight dashboard for route topology, consumer sessions, inflight backlog, and replay controls.

### ภาษาไทย

- **Route Bootstrap Persistence:** เพิ่มที่เก็บ route catalog แบบถาวร เพื่อให้ ART route store ฟื้นคืนได้อัตโนมัติโดยไม่ต้อง bootstrap ด้วยมือทุกครั้ง
- **Session Snapshot และ Warm Restart:** บันทึก metadata ของ consumer session และ capability ที่นำกลับมาใช้ต่อได้ เพื่อลดเวลา recovery หลัง broker restart
- **Delayed Delivery / Scheduled Publish:** รองรับการตั้งเวลาส่งล่วงหน้าและตาราง retry ในระดับความสามารถหลักของ broker
- **Priority Queues สำหรับ Direct Delivery:** เปิดให้กำหนดลำดับความสำคัญของข้อความ เพื่อให้คำสั่งเร่งด่วนวิ่งแซงงานพื้นหลังที่มีปริมาณมากได้
- **Dead-letter Inspection API:** เพิ่ม API/CLI สำหรับดูรายการ dead-letter, สั่ง replay และลบข้อมูลอย่างปลอดภัย
- **Replay Audit Trail:** เก็บประวัติการ replay ที่ผู้ปฏิบัติงานสั่งเอง รวมถึง state transition ของการส่ง เพื่อใช้ด้าน compliance และ debugging
- **Tenant-aware Route Namespaces:** แยก route lookup, quota และ observability ตาม tenant โดยยังใช้โครงสร้าง broker ร่วมกันได้
- **Adaptive Backpressure Policies:** ปรับเพดาน inflight และ queue แบบไดนามิกตาม lag ของ consumer, อัตรา retry และแรงกดดันด้านหน่วยความจำ
- **Geo-redundant WAL Replication:** ทำสำเนา WAL ไปยัง standby broker หรือ object store เพื่อเพิ่มความพร้อมด้าน disaster recovery
- **SQL/Analytics Export Pipeline:** ส่งการเปลี่ยนแปลงของ route, inflight และ WAL ไปยัง PostgreSQL, ClickHouse หรือ data warehouse สำหรับงานรายงาน
- **Operational Admin UI:** สร้างแดชบอร์ดขนาดเบาสำหรับดู topology ของ route, consumer sessions, inflight backlog และเครื่องมือควบคุม replay

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
