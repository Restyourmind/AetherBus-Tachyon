# Performance Model and Benchmarking

This document defines performance phases, benchmark scope, and baseline targets.

## 1) Data Path Roadmap

### Phase 1 (current practical baseline)

- ZeroMQ transport
- JSON codec
- LZ4 compression
- ART-backed in-memory route resolution

### Phase 2 (broker optimization)

- Binary framing on transport
- Pooled buffers and lower allocations
- Lock minimization in hot path
- Reduced copy and parse overhead

### Phase 3 (extreme path)

- Shared-memory and advanced I/O integration
- Transport specialization by workload
- Cluster/federation-oriented broker mesh

## 2) Benchmark Methodology

Track throughput and latency per scenario:

- Direct mode with ACK (`--mode direct-ack`)
- Fanout mode under varying subscriber counts (`--mode fanout --fanout-subs N`)
- Mixed topic distributions (`--mode mixed --mixed-topics N`)
- Small/medium/large payload classes (`--payload-class small|medium|large`)

Always report:

- p50 / p95 / p99 latency
- messages/sec throughput
- CPU and memory usage
- allocations/op (hot path)

## 3) First-class benchmark harness

The benchmark harness lives at `cmd/tachyon-bench` and now has dedicated commands for local use and CI:

```bash
# Single scenario (direct ACK)
go run ./cmd/tachyon-bench harness \
  --mode direct-ack \
  --payload-class small \
  --compress=true \
  --duration 20s

# Fanout with 8 subscribers
go run ./cmd/tachyon-bench harness \
  --mode fanout \
  --fanout-subs 8 \
  --payload-class medium \
  --compress=false \
  --duration 20s

# Mixed topic distribution (70/30 hot-cold weighting)
go run ./cmd/tachyon-bench harness \
  --mode mixed \
  --mixed-topics 8 \
  --payload-class medium \
  --compress=true \
  --duration 30s

# CI-friendly matrix across mode/payload/compression
go run ./cmd/tachyon-bench matrix --duration 10s --connections 2
```

### Payload classes

- `small` = 512B
- `medium` = 4KB
- `large` = 64KB

### Compression modes

- `--compress=true`: LZ4 in sender + benchmark broker runtime path
- `--compress=false`: no-op compressor path for fair A/B overhead comparison

## 4) Sample output format

`harness` output:

```text
--- AetherBus Tachyon Benchmark Harness ---
mode=fanout payload_class=medium payload=4.00 KB compress=true
duration=20s connections=2 fanout_subs=8 mixed_topics=6
sent=39840 recv=315617 errors=0 elapsed=22.004s
throughput_msgs_sec=14343.65 throughput_mb_sec=56.03
latency_p50=1.329ms latency_p95=3.982ms latency_p99=7.441ms
cpu_percent=176.42 memory_rss=148.00 MB alloc_bytes_total=1.20 GB allocs_total=5661221 bytes_per_op=4096.31 allocs_per_op=17.94
```

`matrix` output (CSV line per run):

```text
mode,payload_class,compress,recv_msgs,msgs_per_sec,p50,p95,p99,cpu_percent,memory_rss_mb,allocs_per_op
fanout,medium,true,315617,14343.65,1.329ms,3.982ms,7.441ms,176.42,148.00,17.94
```

## 5) Output interpretation

- Compare `p99` first for tail behavior under stress.
- Use `throughput_msgs_sec` with `errors` and drop rate (`sent - recv`) to avoid optimizing for raw speed only.
- `cpu_percent` is process CPU consumption over wall-clock for the benchmark process.
- `memory_rss` shows peak resident usage; `allocs_per_op` and `bytes_per_op` help identify allocator pressure.

## 6) Key Metrics

Broker counters:

- `ingress_messages_total`
- `routed_messages_total`
- `unroutable_messages_total`
- `fanout_messages_total`
- `direct_messages_total`

Latency metrics:

- `decode_latency_ns`
- `route_lookup_latency_ns`
- `publish_latency_ns`
- `ack_latency_ns`
- `end_to_end_latency_ns`

Health metrics:

- `connected_consumers`
- `inflight_messages`
- `retry_queue_depth`
- `dlq_depth`
- `queue_policy_evaluations_total` / `queue_policy_adjustments_total`
- queue-policy health inputs: `consumer_lag`, retry rate, queue growth rate, and memory pressure

## 6.1 Runtime queue-limit controller

Direct delivery queue limits can now be managed by a policy controller instead of remaining fixed for the life of the process.

Recommended operating notes:

- Enable with `QUEUE_POLICY_ENABLED=true` when a workload has bursty consumer lag or fluctuating memory pressure.
- Use `QUEUE_POLICY_CONSUMER_LAG_HIGH_WATERMARK`, `QUEUE_POLICY_RETRY_RATE_HIGH_WATERMARK`, `QUEUE_POLICY_QUEUE_GROWTH_HIGH_WATERMARK`, and `QUEUE_POLICY_MEMORY_PRESSURE_HIGH_WATERMARK` to define pressure signals.
- Keep `QUEUE_POLICY_INFLIGHT_STEP` and `QUEUE_POLICY_QUEUE_STEP` modest so each recalculation changes capacity gradually.
- Use `QUEUE_POLICY_MIN_HOLD_MS` to avoid oscillation; the router will not apply another limit change until the hold window expires.
- The controller only changes capacity ceilings. Queue ordering still follows priority + enqueue sequence, and inflight reductions never evict already-dispatched messages.

## 7) Initial Target Template

The project should define explicit target SLOs over time, for example:

- p99 direct-route latency target under nominal load
- minimum sustained throughput target
- maximum acceptable unroutable rate

Set concrete numbers once baseline benchmark results are available in CI/perf environments.

## 8) Comparing future optimizations

To compare future optimizations consistently:

1. Use `matrix` with a fixed duration/connections and both compression modes.
2. Store each run's CSV output as CI artifact (or commit under a `perf/baselines/` folder).
3. Compare candidate vs baseline on:
   - p99 latency (must not regress)
   - throughput (should improve or hold)
   - allocs/op and bytes/op (should improve for hot-path changes)
   - cpu_percent and memory_rss (watch for hidden cost shifts)
4. Treat improvements as valid only when repeated at least 3 times and median trend is favorable.

## 9) Compression Toggle for Benchmarking

The default runtime wiring (`internal/app.NewRuntime`) uses LZ4 compression.
For benchmark scenarios that require `--compress=false`, use benchmark runtime wiring:

- `cmd/tachyon/main.go` exposes a `--compress` flag (default `true`).
- `internal/app.NewBenchmarkRuntime(...)` selects LZ4 when enabled and a no-op compressor when disabled.

This allows fair comparison between compressed and uncompressed broker paths without introducing cgo/FFI complexity.
