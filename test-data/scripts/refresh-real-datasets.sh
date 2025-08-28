#!/usr/bin/env bash
set -euo pipefail

# CQLite: Real dataset refresh orchestrator (single entrypoint)
# - Starts Cassandra 5 compose stack if needed
# - Generates REAL data driven by dataset_list.txt schemas
# - Exports SSTables from the container
# - Copies exported files to test-data/real/<version>
#
# Usage:
#   bash test-data/scripts/refresh-real-datasets.sh \
#     --version 5.0 \
#     --dataset-file test-data/cassandra5/bti/dataset_list.txt \
#     --compose-file test-data/docker/docker-compose-cassandra5.yml \
#     --service cassandra-node1 \
#     --output-dir test-data/real \
#     [--run-validator]
#
# Notes:
# - Synthetic data is NOT allowed for M1; this script produces real data only.
# - Ensure Docker Desktop/daemon is running.

VERSION="5.0"
DATASET_FILE="test-data/cassandra5/bti/dataset_list.txt"
COMPOSE_FILE="test-data/docker/docker-compose-cassandra5.yml"
SERVICE_NAME="cassandra-5-0"
OUTPUT_DIR="test-data/real"
RUN_VALIDATOR="false"

log() { echo "[refresh-real] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[refresh-real][ERROR] $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --dataset-file) DATASET_FILE="$2"; shift 2 ;;
    --compose-file) COMPOSE_FILE="$2"; shift 2 ;;
    --service) SERVICE_NAME="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --run-validator) RUN_VALIDATOR="true"; shift 1 ;;
    *) fail "Unknown arg: $1" ;;
  esac
done

[[ -f "$DATASET_FILE" ]] || fail "dataset file not found: $DATASET_FILE"

mkdir -p "$OUTPUT_DIR"
REAL_ROOT="$OUTPUT_DIR/v$VERSION"
HOST_REAL_ABS="$(cd "$OUTPUT_DIR" && pwd)/v$VERSION"

log "Starting with a clean compose stack (down -v --remove-orphans)"
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans || true
log "Starting/updating compose stack ($COMPOSE_FILE)"
docker compose -f "$COMPOSE_FILE" up -d --wait --remove-orphans

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
log "Generating REAL data via data-generator image (version=$VERSION)"
# Prepare ephemeral log/output mount for generator
GEN_TMP_DIR="$(mktemp -d)"

# Ensure data-generator image exists, build if missing
if ! docker image inspect cqlite/data-generator:py311 >/dev/null 2>&1; then
  log "Building data-generator image cqlite/data-generator:py311"
  TESTDATA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
  docker build -t cqlite/data-generator:py311 \
    -f "$TESTDATA_DIR/docker/Dockerfile.data-generator" \
    "$TESTDATA_DIR"
fi

# Run generator container on the compose network
docker run --rm \
  --network docker_cqlite-test-network \
  -v "$SCRIPT_DIR":/scripts \
  -v "$GEN_TMP_DIR":/generated \
  -w /scripts \
  cqlite/data-generator:py311 \
  python /scripts/generate_comprehensive_test_data.py --version "$VERSION" --host cassandra-5-0 --port 9042

log "Flushing memtables to SSTables before export"
docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE_NAME" bash -lc 'nodetool flush || true'

log "Exporting SSTables from container service: $SERVICE_NAME"
# Run exporter inside container if available
if docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE_NAME" bash -lc 'type /opt/scripts/export-sstables.sh >/dev/null 2>&1'; then
  docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE_NAME" bash -lc \
    "SOURCE_DATA_DIR=/var/lib OUTPUT_DIR=/opt/generated-real /opt/scripts/export-sstables.sh"
  log "Copying exported files from container to host: $HOST_REAL_ABS"
  mkdir -p "$HOST_REAL_ABS"
  docker compose -f "$COMPOSE_FILE" cp "$SERVICE_NAME":/opt/generated-real/v$VERSION "$HOST_REAL_ABS/.." >/dev/null
else
  log "No in-container exporter found; attempting host-side exporter (if available)"
  if [[ -x test-data/scripts/export-sstables.sh ]]; then
    export SOURCE_DATA_DIR="/var/lib"  # best-effort; script is designed for container
    export OUTPUT_DIR="$HOST_REAL_ABS/.."
    bash test-data/scripts/export-sstables.sh || fail "host-side export failed"
  else
    fail "export-sstables.sh not found; cannot export on host"
  fi
fi

[[ -d "$REAL_ROOT" ]] || fail "expected exported directory not found: $REAL_ROOT"

log "Validating presence of mandatory files (spot-check)"
missing=0
for req in Data.db Index.db Summary.db Statistics.db TOC.txt; do
  if ! find "$REAL_ROOT" -type f -name "*-$req" | grep -q . ; then
    log "Missing expected file type: $req"
    missing=1
  fi
done
[[ $missing -eq 0 ]] || fail "export appears incomplete under $REAL_ROOT"

if [[ "$RUN_VALIDATOR" == "true" ]]; then
  log "Building validator and running comprehensive parity (REAL data)"
  cargo build -r -p sstabledump-validator
  cargo run -r -p sstabledump-validator -- comprehensive --scope bti --fail-fast true
fi

log "SUCCESS: REAL datasets refreshed under $REAL_ROOT"


