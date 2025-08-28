#!/usr/bin/env bash
set -euo pipefail

# CQLite: Real dataset refresh orchestrator (single entrypoint)
# Orchestrates the simplified flow: start-clean -> generate -> export -> shutdown
# Produces real SSTables under test-data/datasets/ preserving Cassandra layout
#
# Usage:
#   bash test-data/scripts/refresh-real-datasets.sh \
#     [--scale SMALL|MEDIUM|COMPREHENSIVE|LARGE] \
#     [--rows N] \
#     [--tables basic,collections,timeseries,wide]
#
# Notes:
# - Uses docker compose stack defined in docker/docker-compose-cassandra5.yml
# - Writes metadata to test-data/datasets/metadata.yml and copies SSTables to datasets/sstables/

SCALE="${SCALE:-SMALL}"
ROWS="${ROWS:-}"
TABLES="${TABLES:-basic,collections,timeseries,wide}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$ROOT/scripts"

log() { echo "[refresh-real] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[refresh-real][ERROR] $*" >&2; exit 1; }

# Parse flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale) SCALE="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --tables) TABLES="$2"; shift 2 ;;
    *) fail "Unknown arg: $1" ;;
  esac
done

log "Starting clean Cassandra 5 and applying schemas"
/bin/bash "$SCRIPTS/start-clean.sh"

log "Generating data: SCALE=$SCALE ROWS=${ROWS:-auto} TABLES=$TABLES"
TABLES="$TABLES" SCALE="$SCALE" ${ROWS:+ROWS="$ROWS"} /bin/bash "$SCRIPTS/generate.sh"

log "Exporting SSTables and metadata to test-data/datasets/"
/bin/bash "$SCRIPTS/export.sh"

log "Shutting down stack and removing volumes"
/bin/bash "$SCRIPTS/shutdown-clean.sh"

log "SUCCESS: Datasets refreshed under $ROOT/datasets"


