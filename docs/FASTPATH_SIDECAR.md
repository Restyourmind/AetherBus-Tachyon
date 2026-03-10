# Rust Fast-path Sidecar (Scaffold)

## Goal

Introduce a **narrow offload boundary** for framing/compression and large-payload handling without migrating broker orchestration out of Go.

## Current hot path (transport/media)

Ingress flow in `internal/delivery/zmq.Router.loop` today:

1. Parse ROUTER frames (`parseFrames`)
2. Validate topic (`validateTopic`)
3. Decompress payload (`compressor.Decompress`)
4. Decode event (`codec.Decode`)
5. Publish/routable envelope + direct dispatch + PUB fanout

This means decompression and decode cost are on every ingress message path.

## Adapter boundary (Go)

`internal/fastpath/adapter.go` defines:

- `FrameAdapter` interface
  - `EncodeFrame(flags, topic, payload)`
  - `Mode()`
- `GoOnlyAdapter` (default)
- `SidecarAdapter` (optional)

Runtime wiring is exposed through:

- `app.NewDefaultFrameAdapter()` (always Go-only)
- `app.NewFrameAdapterFromConfig(cfg)` (optional sidecar dial + fallback policy)

The default runtime keeps Go-only behavior unless sidecar mode is explicitly enabled.

## Developer activation (explicit opt-in)

Set env vars only when intentionally testing sidecar mode:

- `FASTPATH_SIDECAR_ENABLED=true`
- `FASTPATH_SOCKET_PATH=/tmp/tachyon-fastpath.sock`
- `FASTPATH_CUTOVER_BYTES=262144` (default 256 KiB)
- `FASTPATH_REQUIRE=false` (set `true` for strict fail-fast sidecar requirement)
- `FASTPATH_FALLBACK_TO_GO=true` (default safety path)

By default (`FASTPATH_SIDECAR_ENABLED=false`) runtime stays on Go-only path.

## When Rust path is used

By default: **never** (Go-only).

Rust sidecar path is used only when all are true:

1. Sidecar mode is explicitly enabled.
2. Payload size is above cutover (`FASTPATH_CUTOVER_BYTES`).
3. Sidecar socket is reachable.

If sidecar fails and fallback is enabled, Go local framing is used immediately.

## Non-goals for this iteration

- No protocol migration.
- No change to ACK/NACK/retry/backpressure semantics.
- No forced dependency on Rust for default broker startup.

## Measurable hot-path migration candidates

1. **LZ4 decompress on ingress**
   - Metric: `decode_latency_ns`, `allocs/op`, p99 latency.
2. **Frame encode for large payload direct sends**
   - Metric: `publish_latency_ns`, CPU%, bytes/op for payload classes `large`.
3. **Chunked large-payload reassembly/emit path (future)**
   - Metric: RSS peak, tail latency under 1MB+ payload tests.
4. **Header peeking / frame length parsing**
   - Metric: messages/sec and branch-miss impact under mixed-topic load.

## Next implementation steps

1. Wire `NewFrameAdapterFromConfig` into one runtime entrypoint behind explicit flag/env.
2. Implement LZ4 compress/decompress sidecar opcodes with bounds checks.
3. Add benchmark matrix columns for adapter mode (`go-only`, `rust-sidecar`).
4. Define SLO gate: sidecar mode must not regress p99 latency or delivery correctness.
5. Add parity tests for Go local frame output vs sidecar frame output.
