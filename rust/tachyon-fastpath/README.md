# tachyon-fastpath (Rust sidecar scaffold)

This crate provides a **minimal fast-path sidecar scaffold** for high-cost media-path work while keeping Go as the primary orchestration runtime.

## Scope of this scaffold

- ✅ Framing encode sidecar opcode (`OP_ENCODE_FRAME`)
- ✅ Shared request/response protocol constants
- ✅ Process-boundary unix-socket server (`src/bin/sidecar.rs`)
- 🚧 Compression opcodes are scaffolded but intentionally return `not implemented`

## Why sidecar-first

The first iteration prefers a sidecar/process boundary over deep cgo/unsafe integration to reduce blast radius for broker correctness and delivery behavior.

## Next implementation steps

1. Implement `OP_COMPRESS_LZ4` and `OP_DECOMPRESS_LZ4` with bounded buffers and metrics hooks.
2. Add request IDs in protocol for tracing sidecar latency and failures.
3. Add golden-frame compatibility tests between Go local framing and Rust sidecar framing.
4. Add benchmark mode that compares `go-only` vs `rust-sidecar` frame encode paths.
