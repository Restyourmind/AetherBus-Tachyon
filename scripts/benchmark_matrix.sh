#!/usr/bin/env bash
set -euo pipefail

DURATION="${1:-10s}"
CONNECTIONS="${2:-2}"

exec go run ./cmd/tachyon-bench matrix --duration "$DURATION" --connections "$CONNECTIONS"
