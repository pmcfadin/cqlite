#!/bin/bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/docker/docker-compose-cassandra5.yml"

docker compose -f "$COMPOSE" down -v
echo "[shutdown-clean] Stack stopped and volumes removed."


