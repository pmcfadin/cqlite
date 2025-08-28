#!/bin/bash

set -euo pipefail
shopt -s extglob

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/docker/docker-compose-cassandra5.yml"

# Bring up Cassandra 5 and wait until healthy
bash "$ROOT/scripts/compose-guard.sh" --compose-file "$COMPOSE" --service cassandra-5-0

# Apply schemas inside the Cassandra container (curated by default)
SCHEMA_SET="${SCHEMA_SET:-core}"
CORE_LIST_FILE="$ROOT/schemas/core.list"

if [[ "$SCHEMA_SET" == "core" && -f "$CORE_LIST_FILE" ]]; then
  while IFS= read -r raw; do
    # skip comments/blank
    [[ -z "$raw" || "$raw" =~ ^[[:space:]]*# ]] && continue
    # trim CRs and surrounding whitespace
    fname="${raw//$'\r'/}"
    fname="${fname##+([[:space:]])}"
    fname="${fname%%+([[:space:]])}"
    [[ -z "$fname" ]] && continue
    if [[ -f "$ROOT/schemas/$fname" ]]; then
      echo "[start-clean] Applying schema: $fname"
      docker compose -f "$COMPOSE" exec -T cassandra-5-0 cqlsh -f "/opt/schemas/${fname}" </dev/null
    else
      echo "[start-clean] Skipping missing schema listed in core.list: $fname" >&2
    fi
  done < "$CORE_LIST_FILE"
else
  for schema in "$ROOT/schemas"/*.cql; do
    [ -f "$schema" ] || continue
    echo "[start-clean] Applying schema: $(basename "$schema")"
    docker compose -f "$COMPOSE" exec -T cassandra-5-0 cqlsh -f "/opt/schemas/$(basename "$schema")" </dev/null
  done
fi

echo "[start-clean] Cassandra 5.0 ready and schemas applied."


