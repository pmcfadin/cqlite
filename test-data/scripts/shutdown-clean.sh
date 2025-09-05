#!/bin/bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/docker/docker-compose-cassandra5.yml"
. "$ROOT/scripts/container_env.sh"

# Export for compose providers that read COMPOSE_FILE env
export COMPOSE_FILE="$COMPOSE"

$COMPOSE_CMD -f "$COMPOSE" down -v
echo "[shutdown-clean] Stack stopped and volumes removed."


