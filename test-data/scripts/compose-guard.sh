#!/usr/bin/env bash

set -euo pipefail

# CQLite: Container Compose guard for Cassandra 5 service (Podman/Docker)
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/container_env.sh"
# Ensures the specified compose stack is up and service is healthy.
#
# Usage:
#   bash test-data/scripts/compose-guard.sh \
#     --compose-file test-data/docker/docker-compose-cassandra5.yml \
#     --service cassandra-5-0 \
#     [--timeout 900] \
#     [--interval 5]

COMPOSE_FILE="test-data/docker/docker-compose-cassandra5.yml"
SERVICE_NAME="cassandra-5-0"
TIMEOUT_SECS=900
INTERVAL_SECS=5

log() { echo "[compose-guard] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[compose-guard][ERROR] $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compose-file) COMPOSE_FILE="$2"; shift 2 ;;
    --service) SERVICE_NAME="$2"; shift 2 ;;
    --timeout) TIMEOUT_SECS="$2"; shift 2 ;;
    --interval) INTERVAL_SECS="$2"; shift 2 ;;
    *) fail "Unknown arg: $1" ;;
  esac
done

[[ -f "$COMPOSE_FILE" ]] || fail "Compose file not found: $COMPOSE_FILE"

# Ensure compose provider sees the specified file
export COMPOSE_FILE="$COMPOSE_FILE"

# Bring up the requested service
log "Bringing up service '$SERVICE_NAME' using compose file: $COMPOSE_FILE"
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d "$SERVICE_NAME"

start_ts=$(date +%s)

log "Waiting for Cassandra service health (timeout=${TIMEOUT_SECS}s, interval=${INTERVAL_SECS}s)"
while true; do
  # Quick existence check (provider-agnostic)
  if ! $COMPOSE_CMD -f "$COMPOSE_FILE" ps | grep -E "\b$SERVICE_NAME\b" | grep -E "Up|running|Started|healthy" >/dev/null 2>&1; then
    log "Service not reported as running yet"
  else
    # If health marker present in ps output, accept and proceed
    if $COMPOSE_CMD -f "$COMPOSE_FILE" ps | grep -E "\b$SERVICE_NAME\b" | grep -qi "(healthy)"; then
      log "Service '$SERVICE_NAME' reports Docker health: healthy"
      break
    fi
    # Health check via cqlsh + nodetool
    if compose_exec_nontty "$SERVICE_NAME" sh -lc \
      "cqlsh -e \"SELECT cluster_name FROM system.local;\" >/dev/null 2>&1 && nodetool status | grep -q 'UN'"; then
      log "Service '$SERVICE_NAME' is healthy (cqlsh OK, nodetool UN)"
      break
    fi
    log "Health not ready yet (cqlsh/nodetool)"
  fi

  now=$(date +%s)
  if (( now - start_ts > TIMEOUT_SECS )); then
    fail "Timed out waiting for '$SERVICE_NAME' to become healthy"
  fi
  sleep "$INTERVAL_SECS"
done

log "SUCCESS: '$SERVICE_NAME' is ready"

