#!/usr/bin/env bash

set -euo pipefail

# Detect container engine and compose provider with sensible fallbacks.
# Exports: ENGINE_CMD, COMPOSE_CMD
# Helpers: compose_ps_has_service, compose_ps_running, compose_exec_nontty

if [[ -n "${COMPOSE_CMD:-}" ]]; then
  : # Respect pre-set COMPOSE_CMD
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD="podman-compose"
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD="podman compose"
elif command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  echo "[container-env] ERROR: No supported compose provider found (podman-compose, podman compose, docker compose, docker-compose)" >&2
  exit 1
fi

# ENGINE_CMD for standalone container runs (used for sstabledump)
if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
  ENGINE_CMD="$CONTAINER_ENGINE"
elif [[ "$COMPOSE_CMD" == podman* ]]; then
  ENGINE_CMD="podman"
else
  ENGINE_CMD="docker"
fi

# Whether compose 'ps' supports a service argument (heuristic probe)
compose_ps_has_service() {
  set +e
  $COMPOSE_CMD ps dummy_service >/dev/null 2>&1
  local rc=$?
  set -e
  # docker compose returns non-zero for unknown service; podman-compose may error on extra arg
  if [[ $rc -ne 2 ]]; then
    return 0
  fi
  return 1
}

# Check if a service appears running/started in 'ps' output
compose_ps_running() {
  local service="$1"
  set +e
  $COMPOSE_CMD ps 2>/dev/null | grep -E "\\b${service}\\b" | grep -E "Up|running|Started|healthy" >/dev/null 2>&1
  local rc=$?
  set -e
  return $rc
}

# Exec inside a service without TTY, compatible with docker/podman compose variants.
compose_exec_nontty() {
  local service="$1"; shift
  # Try with -T first (docker compose supports -T to disable TTY)
  set +e
  $COMPOSE_CMD exec -T "$service" "$@"
  local rc=$?
  if [[ $rc -eq 0 ]]; then
    set -e; return 0
  fi
  # Retry without -T for providers that don't support it
  $COMPOSE_CMD exec "$service" "$@"
  rc=$?
  set -e
  return $rc
}

# Run a one-off container without allocating a TTY so stdin piping works (heredoc, etc.)
compose_run_nontty() {
  local service="$1"; shift
  set +e
  $COMPOSE_CMD run -T --rm --no-deps "$service" "$@"
  local rc=$?
  if [[ $rc -eq 0 ]]; then
    set -e; return 0
  fi
  # Fallback without -T if provider doesn't support it
  $COMPOSE_CMD run --rm --no-deps "$service" "$@"
  rc=$?
  set -e
  return $rc
}


