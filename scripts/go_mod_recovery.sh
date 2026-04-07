#!/usr/bin/env bash
set -euo pipefail

# go_mod_recovery.sh
#
# Purpose:
#   Separate "offline-safe checks" from "online recovery" for Go module recovery.
#
# Usage:
#   scripts/go_mod_recovery.sh [flags] check|recover|doctor

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

GO_BIN="${GO_BIN:-go}"
OFFLINE_PKGS="${OFFLINE_PKGS:-./cmd/aetherbus}"
SIMULATE_POLICY=0
POLICY_RULESET="${POLICY_RULESET:-default}"
DRIFT_EXPORT_DIR="${DRIFT_EXPORT_DIR:-}"
AUTO_ROLLBACK=0
AUTO_ROLLBACK_CMD="${AUTO_ROLLBACK_CMD:-}"
ACTION=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/go_mod_recovery.sh [flags] check
      Run offline-safe checks only. Does not intentionally fetch modules.

  scripts/go_mod_recovery.sh [flags] recover
      Run online module recovery workflow:
      - go mod download
      - go mod tidy
      - go build ./...
      - go test ./...

  scripts/go_mod_recovery.sh [flags] doctor
      Print environment diagnostics relevant to module recovery.

Flags:
  --simulate-policy          Run policy/ruleset rehearsal mode without mutating files.
  --policy-ruleset NAME      Policy profile label to annotate logs/exports (default: default).
  --drift-export-dir DIR     Emit workflow drift bundle (JSON + CSV) to DIR.
  --auto-rollback            Enable rollback hook if post-fix verification fails.
  --auto-rollback-cmd CMD    Rollback command to execute when --auto-rollback is enabled.

Environment variables:
  GO_BIN       Override Go binary path (default: go)
  OFFLINE_PKGS Space-separated package patterns to test in offline mode
               (default: ./cmd/aetherbus)
  POLICY_RULESET     Default value for --policy-ruleset
  DRIFT_EXPORT_DIR   Default value for --drift-export-dir
  AUTO_ROLLBACK_CMD  Default rollback command used with --auto-rollback
USAGE
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --simulate-policy)
        SIMULATE_POLICY=1
        shift
        ;;
      --policy-ruleset)
        POLICY_RULESET="${2:-}"
        shift 2
        ;;
      --drift-export-dir)
        DRIFT_EXPORT_DIR="${2:-}"
        shift 2
        ;;
      --auto-rollback)
        AUTO_ROLLBACK=1
        shift
        ;;
      --auto-rollback-cmd)
        AUTO_ROLLBACK_CMD="${2:-}"
        shift 2
        ;;
      -h|--help|help)
        usage
        exit 0
        ;;
      check|recover|doctor)
        ACTION="$1"
        shift
        ;;
      *)
        echo "[go-mod] Unknown argument: $1" >&2
        usage
        exit 2
        ;;
    esac
  done

  if [[ -z "$ACTION" ]]; then
    usage
    exit 2
  fi
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
  if [[ "$SIMULATE_POLICY" -eq 1 ]]; then
    echo "[go-mod] (simulate-policy) would run: $*"
    return 0
  fi
  if ! "$@"; then
    network_hint
    return 1
  fi
}

emit_drift_bundle() {
  local mode="$1"
  local status="$2"
  [[ -n "$DRIFT_EXPORT_DIR" ]] || return 0
  mkdir -p "$DRIFT_EXPORT_DIR"
  local ts epoch json_file csv_file
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  epoch="$(date -u +%s)"
  json_file="${DRIFT_EXPORT_DIR}/trend_bundle.json"
  csv_file="${DRIFT_EXPORT_DIR}/trend_bundle.csv"
  local mod_sha="missing" sum_sha="missing"
  [[ -f go.mod ]] && mod_sha="$(sha256sum go.mod | awk '{print $1}')"
  [[ -f go.sum ]] && sum_sha="$(sha256sum go.sum | awk '{print $1}')"

  cat >"$json_file" <<JSON
{
  "timestamp_utc": "${ts}",
  "epoch_utc": ${epoch},
  "mode": "${mode}",
  "status": "${status}",
  "simulate_policy": ${SIMULATE_POLICY},
  "policy_ruleset": "${POLICY_RULESET}",
  "go_version": "$($GO_BIN version 2>/dev/null || true)",
  "go_mod_sha256": "${mod_sha}",
  "go_sum_sha256": "${sum_sha}"
}
JSON

  {
    echo "timestamp_utc,epoch_utc,mode,status,simulate_policy,policy_ruleset,go_mod_sha256,go_sum_sha256"
    echo "${ts},${epoch},${mode},${status},${SIMULATE_POLICY},${POLICY_RULESET},${mod_sha},${sum_sha}"
  } >"$csv_file"

  echo "[go-mod] Drift bundle exported:"
  echo "[go-mod] - $json_file"
  echo "[go-mod] - $csv_file"
}

run_rollback_hook() {
  [[ "$AUTO_ROLLBACK" -eq 1 ]] || return 0
  if [[ -z "$AUTO_ROLLBACK_CMD" ]]; then
    echo "[go-mod] Auto-rollback enabled but no command configured; skipping rollback."
    return 0
  fi
  echo "[go-mod] Running auto-rollback hook: $AUTO_ROLLBACK_CMD"
  if bash -lc "$AUTO_ROLLBACK_CMD"; then
    echo "[go-mod] Auto-rollback hook completed."
  else
    echo "[go-mod] Auto-rollback hook failed." >&2
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
  echo "[go-mod] Policy ruleset: $POLICY_RULESET"
  if [[ "$SIMULATE_POLICY" -eq 1 ]]; then
    echo "[go-mod] Simulation mode enabled; commands are rehearsed and not executed."
  fi

  echo
  if ! run_online_step "Step 1: download dependencies" "$GO_BIN" mod download; then
    emit_drift_bundle "recover" "failed_step_1"
    return 1
  fi

  echo
  if ! run_online_step "Step 2: tidy module metadata" "$GO_BIN" mod tidy; then
    emit_drift_bundle "recover" "failed_step_2"
    return 1
  fi

  echo
  if ! run_online_step "Step 3: build all packages" "$GO_BIN" build ./...; then
    emit_drift_bundle "recover" "failed_step_3"
    return 1
  fi

  echo
  if ! run_online_step "Step 4: run all tests" "$GO_BIN" test ./...; then
    echo "[go-mod] Post-fix verification failed."
    run_rollback_hook
    emit_drift_bundle "recover" "failed_post_fix_verification"
    return 1
  fi

  emit_drift_bundle "recover" "success"
  echo "[go-mod] Recovery workflow completed successfully"
}

parse_args "$@"

case "$ACTION" in
  check)
    check
    ;;
  recover)
    recover
    ;;
  doctor)
    doctor
    ;;
  *)
    usage
    exit 2
    ;;
esac
