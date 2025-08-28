#!/bin/bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/docker/docker-compose-cassandra5.yml"

# Environment-driven options
ROWS="${ROWS:-}"
TABLES="${TABLES:-}"
SCALE="${SCALE:-SMALL}"

args=(python3 /scripts/generate_comprehensive_test_data.py --version 5.0 --host cassandra-5-0 --port 9042 --scale "$SCALE")
[[ -n "$ROWS" ]] && args+=(--rows-per-table "$ROWS")
[[ -n "$TABLES" ]] && args+=(--tables "$TABLES")

docker compose -f "$COMPOSE" run --rm --no-deps data-generator "${args[@]}"


