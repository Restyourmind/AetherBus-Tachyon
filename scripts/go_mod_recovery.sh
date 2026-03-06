#!/usr/bin/env bash
set -euo pipefail

# go_mod_recovery.sh
#
# Purpose:
#   Separate "offline-safe checks" from "online recovery" for Go module recovery.
#
# Usage:
#   scripts/go_mod_recovery.sh check
#   scripts/go_mod_recovery.sh recover
#   scripts/go_mod_recovery.sh doctor

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

GO_BIN="${GO_BIN:-go}"
OFFLINE_PKGS="${OFFLINE_PKGS:-./cmd/aetherbus}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/go_mod_recovery.sh check
      Run offline-safe checks only. Does not intentionally fetch modules.

  scripts/go_mod_recovery.sh recover
      Run online module recovery workflow:
      - go mod download
      - go mod tidy
      - go build ./...
      - go test ./...

  scripts/go_mod_recovery.sh doctor
      Print environment diagnostics relevant to module recovery.

Environment variables:
  GO_BIN       Override Go binary path (default: go)
  OFFLINE_PKGS Space-separated package patterns to test in offline mode
               (default: ./cmd/aetherbus)
USAGE
}

require_go() {
  if ! command -v "$GO_BIN" >/dev/null 2>&1; then
    echo "[go-mod] Go binary not found: $GO_BIN" >&2
    exit 1
  fi
}

network_hint() {
  cat <<'MSG'
[go-mod] Online recovery could not complete.
[go-mod] If this environment blocks external module endpoints, this is expected.
[go-mod] This is an environment limitation, not necessarily a regression in source code.
[go-mod] Retry on a network-enabled runner/machine.
MSG
}

run_online_step() {
  local label="$1"
  shift
  echo "==> ${label}"
  if ! "$@"; then
    network_hint
    return 1
  fi
}

doctor() {
  require_go
  echo "==> Environment diagnostics"
  echo "pwd: $PWD"
  echo "go binary: $(command -v "$GO_BIN" || true)"
  echo "go version: $($GO_BIN version || true)"
  echo "GOMOD: $($GO_BIN env GOMOD || true)"
  echo "GOMODCACHE: $($GO_BIN env GOMODCACHE || true)"
  echo "GOCACHE: $($GO_BIN env GOCACHE || true)"
  echo "GOPROXY: $($GO_BIN env GOPROXY || true)"
  echo "GOSUMDB: $($GO_BIN env GOSUMDB || true)"
  echo "GOFLAGS: $($GO_BIN env GOFLAGS || true)"
  echo "CGO_ENABLED: $($GO_BIN env CGO_ENABLED || true)"

  echo
  echo "==> Repository files"
  [[ -f go.mod ]] && echo "[go-mod] Found go.mod" || echo "[go-mod] go.mod not found"
  [[ -f go.sum ]] && echo "[go-mod] Found go.sum" || echo "[go-mod] go.sum not found"

  echo
  echo "==> cmd package scan"
  if [[ -d cmd ]]; then
    find cmd -maxdepth 2 -type f -name '*.go' | sort
  else
    echo "[go-mod] No cmd/ directory found"
  fi
}

check() {
  require_go

  echo "==> Offline-safe checks"
  echo "[go-mod] This mode avoids recovery actions expected to require external module access."

  echo
  echo "==> Step 1: basic repository sanity"
  [[ -f go.mod ]] || { echo "[go-mod] go.mod not found" >&2; exit 1; }
  echo "[go-mod] go.mod present"
  [[ -f go.sum ]] && echo "[go-mod] go.sum present" || echo "[go-mod] go.sum is missing"

  echo
  echo "==> Step 2: command entrypoint sanity"
  if [[ -f cmd/aetherbus/main.go ]]; then
    if grep -q '^package main' cmd/aetherbus/main.go && grep -q 'func main()' cmd/aetherbus/main.go; then
      echo "[go-mod] cmd/aetherbus/main.go looks like a valid command entrypoint"
    else
      echo "[go-mod] cmd/aetherbus/main.go exists but may not be a valid command entrypoint"
    fi
  else
    echo "[go-mod] cmd/aetherbus/main.go not found"
  fi

  echo
  echo "==> Step 3: offline-safe package tests"
  local test_failed=0
  for pkg in $OFFLINE_PKGS; do
    echo "[go-mod] Running: $GO_BIN test $pkg"
    if "$GO_BIN" test "$pkg"; then
      echo "[go-mod] Passed: $pkg"
    else
      echo "[go-mod] Failed: $pkg"
      test_failed=1
    fi
  done

  echo
  echo "==> Step 4: module drift advisory"
  echo "[go-mod] Full validation (go mod tidy / go test ./...) may still require network access."
  echo "[go-mod] Use 'recover' on a machine or CI runner with module download access."

  if [[ "$test_failed" -eq 0 ]]; then
    echo "[go-mod] Offline-safe checks completed successfully"
    return 0
  fi

  echo "[go-mod] Offline-safe checks completed with failures" >&2
  return 1
}

recover() {
  require_go

  echo "==> Online recovery workflow"
  echo "[go-mod] This mode may download modules and update go.mod / go.sum."

  echo
  run_online_step "Step 1: download dependencies" "$GO_BIN" mod download

  echo
  run_online_step "Step 2: tidy module metadata" "$GO_BIN" mod tidy

  echo
  run_online_step "Step 3: build all packages" "$GO_BIN" build ./...

  echo
  run_online_step "Step 4: run all tests" "$GO_BIN" test ./...

  echo "[go-mod] Recovery workflow completed successfully"
}

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

case "$1" in
  check)
    check
    ;;
  recover)
    recover
    ;;
  doctor)
    doctor
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    exit 2
    ;;
esac
