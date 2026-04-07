# Change-data export layer

## Schema

All exported records use `schema_version: "v1"` and the normalized envelope implemented in `internal/exporter/exporter.go`.

Core fields:
- `event_id`: deterministic hash for idempotent sink-side dedupe.
- `kind`: `route`, `session`, `delivery`, or `replay`.
- `action`: `upsert`, `remove`, `replay`, `register`, `restore`, `heartbeat`, `dispatch`, `ack`, `nack`, `retry`, `dead_letter`, `schedule`, `defer`, or `wal_replay`.
- `source`: source subsystem such as `route_store` or `zmq_router`.
- `cursor`: source-local monotonic cursor used for checkpoints.
- entity fields: `tenant_id`, `message_id`, `consumer_id`, `session_id`, `topic`, `route_key`, `destination_id`.
- delivery fields: `attempt`, `status`, `reason`, `deliver_at`, `enqueue_sequence`, `payload_size`.
- `labels`: extensible key/value metadata for evolution without breaking readers.

## Delivery guarantees

The hot path uses the non-blocking `exporter.AsyncExporter`; if its in-memory channel is full, export events are dropped rather than blocking routing or WAL work.

Durable sinks should therefore be treated as **best-effort, non-blocking, at-least-once from the exporter worker**. The default file sink makes this safer by:
- persisting NDJSON events durably,
- persisting per-source checkpoints, and
- suppressing duplicate `event_id` values on restart.

## Restart and backfill semantics

- Route catalog restores emit `kind=route, action=replay` records.
- Session snapshot restores emit `kind=session, action=restore` records.
- Scheduled queue reload emits `kind=replay, action=scheduled_restore` records.
- WAL unacked replay emits `kind=replay, action=wal_replay` records.

Exporters resume from the last sink checkpoint per source cursor. In addition, deterministic `event_id` values let sinks deduplicate replayed rows if a process restarts after writing some records but before advancing the checkpoint.

## Sink adapters

Implemented now:
- `FileSink`: durable NDJSON sink with checkpoint files.
- `BatchFileSink`: batch-export friendly file sink wrapper.

Pluggable adapters scaffolded now:
- `NewPostgresSink(...)`
- `NewClickHouseSink(...)`

Those SQL adapters intentionally keep the same sink contract but require deployment-specific driver wiring and table DDL.

## Schema evolution

Rules for future changes:
1. Additive fields should go into `labels` first when experimentation is needed.
2. Stable promoted fields should increment `schema_version` only when a breaking interpretation change is required.
3. Consumers must ignore unknown fields.
4. `event_id`, `kind`, `action`, `source`, `occurred_at`, and `cursor` should remain backward compatible within a major schema version.
