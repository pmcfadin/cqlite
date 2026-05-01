#!/usr/bin/env bash
set -euo pipefail

# Local runner for cqlite-core/tests/sstableloader_integration.rs (Issue #396).
#
# Starts a plain Cassandra 5.0 container, waits for readiness, exports
# CQLITE_CASSANDRA_CONTAINER, and runs the docker-integration-gated tests.
#
# Matches the launch recipe used by scripts/e2e_phase1.sh (no env overrides),
# intentionally avoiding test-data/docker/docker-compose-cassandra5.yml which
# sets CASSANDRA_LISTEN_ADDRESS=0.0.0.0 (Cassandra rejects the wildcard and
# refuses to start).
#
# Usage:
#   scripts/local/run-sstableloader-tests.sh                # Tier 1 + Tier 2
#   scripts/local/run-sstableloader-tests.sh --stress       # include Tier 3
#   scripts/local/run-sstableloader-tests.sh --filter NAME  # filter tests
#   scripts/local/run-sstableloader-tests.sh --keep         # keep container on exit

CONTAINER_NAME="cqlite-sstableloader-test"
IMAGE="cassandra:5.0"
READY_TIMEOUT_SECS=300
KEEP_CONTAINER=0
RUN_STRESS=0
TEST_FILTER=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_error()   { echo -e "${RED}[FAIL]${NC} $1" >&2; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }

while [[ $# -gt 0 ]]; do
  case $1 in
    --keep)    KEEP_CONTAINER=1; shift ;;
    --stress)  RUN_STRESS=1; shift ;;
    --filter)  TEST_FILTER="${2:-}"; shift 2 ;;
    -h|--help)
      sed -n '3,18p' "$0"
      exit 0
      ;;
    *)
      log_error "Unknown option: $1"
      exit 2
      ;;
  esac
done

cleanup() {
  local exit_code=$?
  if [[ $KEEP_CONTAINER -eq 1 ]]; then
    log_info "Leaving container '$CONTAINER_NAME' running (--keep)."
  elif docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_info "Removing container '$CONTAINER_NAME'..."
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
  exit $exit_code
}
trap cleanup EXIT INT TERM

# Remove stale container from previous runs up-front.
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  log_info "Removing stale container '$CONTAINER_NAME' from a previous run..."
  docker rm -f "$CONTAINER_NAME" >/dev/null
fi

log_info "Starting Cassandra 5.0 container '$CONTAINER_NAME'..."
docker run --name "$CONTAINER_NAME" -d "$IMAGE" >/dev/null

log_info "Waiting for Cassandra to accept CQL (timeout ${READY_TIMEOUT_SECS}s)..."
deadline=$(( $(date +%s) + READY_TIMEOUT_SECS ))
while ! docker exec "$CONTAINER_NAME" cqlsh -e "SELECT now() FROM system.local;" >/dev/null 2>&1; do
  if (( $(date +%s) >= deadline )); then
    log_error "Cassandra did not become ready within ${READY_TIMEOUT_SECS}s."
    log_error "Last 40 log lines:"
    docker logs --tail 40 "$CONTAINER_NAME" >&2 || true
    exit 1
  fi
  sleep 5
done
log_success "Cassandra is ready."

export CQLITE_CASSANDRA_CONTAINER="$CONTAINER_NAME"

# Build test command. Tier 3 (stress) is gated behind --stress because it is
# expensive; default run exercises Tier 1 (loader acceptance) and Tier 2 (CQL
# query verification).
declare -a CARGO_ARGS=(
  test --release --package cqlite-core
  --test sstableloader_integration
  --features write-support,docker-integration
  --
  --test-threads=1
  --nocapture
)

if [[ -n "$TEST_FILTER" ]]; then
  CARGO_ARGS+=("$TEST_FILTER")
elif [[ $RUN_STRESS -eq 0 ]]; then
  # Skip Tier 3 stress tests by default (they take ~10+ minutes).
  CARGO_ARGS+=(--skip "large_partition")
  CARGO_ARGS+=(--skip "many_partitions")
  CARGO_ARGS+=(--skip "concurrent_writes")
fi

log_info "Running: cargo ${CARGO_ARGS[*]}"
cargo "${CARGO_ARGS[@]}"
log_success "sstableloader integration tests completed."
