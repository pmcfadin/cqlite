#!/usr/bin/env bash
# generate-write-load-parity.sh — Cassandra 5.0.2 write-load byte-parity fixtures
# (issue #1190, epic #969).
#
# Produces committed Cassandra-written reference SSTables for the three
# write_load_path byte-parity scenarios in cassandra-parity-manifest.yml:
#   * cass.write_load_path.cql_sstable_writer.finished_data_db_artifacts
#   * cass.write_load_path.flush.tombstone_and_ttl_artifacts
#   * cass.write_load_path.flush.partition_boundary_artifacts
#
# Each table is written via cqlsh with an explicit USING TIMESTAMP so the cell
# writetime (and therefore the EncodingStats.minTimestamp delta baseline) is
# fixed and reproducible by CQLite's SSTableWriter — enabling a byte-for-byte
# Data.db diff. Tables are UNCOMPRESSED so Data.db is a direct byte slice.
#
# Lifecycle / flag parsing / export / sstabledump-JSONL steps mirror
# generate-tombstone-parity.sh exactly so committed reference files
# (`*-Data.db.jsonl`, `*-Statistics.db.txt`, `TOC.txt`, `Digest.crc32`) are
# consistent with the rest of the corpus.
#
# ============================================================================
# FIXED TIMESTAMP (also documented in schemas/write-load-parity.cql)
#   T_WRITE = 1700000000000000   (2023-11-14T22:13:20Z), micros
# All INSERTs use USING TIMESTAMP T_WRITE. No TTL, no DELETE in these tables:
# TTL/DELETE localDeletionTime is wall-clock-derived and cannot byte-match two
# independent writers; this fixture establishes whole-artifact byte parity for
# the LIVE finished / static-clustering / partition-boundary shapes.
# ============================================================================
#
# Usage:
#   bash test-data/scripts/generate-write-load-parity.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~4 GB RAM for the container.
#
# Backs: issue #1190 (epic #969).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-writeparity"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_writeparity"

# Fixed writetime (micros). MUST match the constant the byte-comparison test
# feeds CQLite's writer (issue_1190_write_load_byte_parity.rs).
T_WRITE=1700000000000000   # 2023-11-14T22:13:20Z

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[wlp] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[wlp] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[wlp][ERROR] $*" >&2; exit 1; }

run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

if command -v docker >/dev/null 2>&1; then
  ENGINE="docker"
elif command -v podman >/dev/null 2>&1; then
  ENGINE="podman"
else
  if [[ "$DRY_RUN" -eq 1 ]]; then
    ENGINE="docker"
    echo "[wlp] (dry-run) no container engine found; using placeholder 'docker'"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME"
fi

cleanup() {
  if [[ "$DRY_RUN" -eq 0 ]]; then
    log "Cleaning up container..."
    $ENGINE rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_cassandra() {
  local max_retries=60
  local delay=5
  log "Waiting for Cassandra to become ready (max ${max_retries}x${delay}s)..."
  for i in $(seq 1 "$max_retries"); do
    if $ENGINE exec "$CONTAINER_NAME" \
        cqlsh -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; then
      log "Cassandra is ready (attempt $i)."
      return 0
    fi
    sleep "$delay"
  done
  fail "Cassandra did not become ready in time."
}

apply_schema() {
  local schema_file="$1"
  local dest_name
  dest_name="$(basename "$schema_file")"
  log "Applying schema: $dest_name"
  run $ENGINE cp "$schema_file" "$CONTAINER_NAME:/tmp/$dest_name"
  run $ENGINE exec "$CONTAINER_NAME" cqlsh -f "/tmp/$dest_name"
}

cql() {
  local stmt="$1"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $ENGINE exec $CONTAINER_NAME cqlsh -k $KEYSPACE -e \"$stmt\""
  else
    $ENGINE exec "$CONTAINER_NAME" cqlsh -k "$KEYSPACE" -e "$stmt"
  fi
}

# ----------------------------------------------------------------------------
# Insert the three tables' data. The exact same logical rows/timestamps are
# re-emitted by CQLite in issue_1190_write_load_byte_parity.rs.
# ----------------------------------------------------------------------------
insert_data() {
  log "=== Inserting fixture data (USING TIMESTAMP $T_WRITE) ==="
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] finished_data: 6 partitions id=0..5, name='name<i>'"
    echo "[dry-run] static_clustering_shape: id=1 static 'static-val' + ck=7 rdata 'row-val'"
    echo "[dry-run] partition_boundary: id=1 ck=1..4 + id=2 ck=1..2, v='v_<id>_<ck>'"
    return 0
  fi

  # finished_data — 6 single-cell partitions. Matches issue_908 write_bti shape.
  for i in 0 1 2 3 4 5; do
    cql "INSERT INTO finished_data (id, name) VALUES ($i, 'name$i') USING TIMESTAMP $T_WRITE"
  done

  # static_clustering_shape — one partition with a static column + one clustering row.
  # Matches issue_821 static_schema (sdata STATIC, rdata regular, ck=7).
  cql "UPDATE static_clustering_shape USING TIMESTAMP $T_WRITE SET sdata='static-val' WHERE id=1"
  cql "INSERT INTO static_clustering_shape (id, ck, rdata) VALUES (1, 7, 'row-val') USING TIMESTAMP $T_WRITE"

  # partition_boundary — two partitions, multiple clustering rows each.
  for ck in 1 2 3 4; do
    cql "INSERT INTO partition_boundary (id, ck, v) VALUES (1, $ck, 'v_1_$ck') USING TIMESTAMP $T_WRITE"
  done
  for ck in 1 2; do
    cql "INSERT INTO partition_boundary (id, ck, v) VALUES (2, $ck, 'v_2_$ck') USING TIMESTAMP $T_WRITE"
  done
  log "Insert complete."
}

generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  log "Generating sstabledump JSONL golden files for $KEYSPACE..."
  while IFS= read -r -d '' data_file; do
    local rel
    rel="${data_file#"$sstables_dir"/}"
    local jsonl_file="${data_file%.db}.db.jsonl"
    log "  sstabledump: $rel"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "[dry-run] sstabledump /data/${rel} -l > $jsonl_file"
    else
      $ENGINE run --rm \
        -v "$sstables_dir:/data" \
        "$CASSANDRA_IMAGE" \
        bash -lc "/opt/cassandra/tools/bin/sstabledump /data/${rel} -l" \
        | python3 -c "
import json, sys
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    item = json.loads(line)
    print(json.dumps(item, separators=(',', ':')))
" > "$jsonl_file"
      if [[ ! -s "$jsonl_file" ]]; then
        log "  WARNING: JSONL file is empty: $jsonl_file"
      else
        local lines
        lines=$(wc -l < "$jsonl_file" | tr -d ' ')
        log "  OK: $jsonl_file ($lines partitions)"
      fi
    fi
  done < <(find "$sstables_dir/$KEYSPACE" -type f -name "*-Data.db" -not -name "._*" -print0 \
            2>/dev/null || true)
}

# ----------------------------------------------------------------------------
# OUT_DIR safety
# ----------------------------------------------------------------------------
if [[ "${#OUT_DIR}" -lt 4 ]]; then
  fail "OUT_DIR '$OUT_DIR' is suspiciously short (< 4 chars). Refusing."
fi
case "$OUT_DIR" in
  /) fail "Refusing to operate on '/'." ;;
  /tmp) fail "Refusing to use '/tmp' directly. Use a subdirectory." ;;
esac
_under_repo=0
_under_tmp=0
[[ "$OUT_DIR" == "$REPO_ROOT/"* ]] && _under_repo=1
[[ "$OUT_DIR" == /tmp/*          ]] && _under_tmp=1
if [[ "$_under_repo" -eq 0 && "$_under_tmp" -eq 0 ]]; then
  fail "OUT_DIR '$OUT_DIR' is not under the repo root or /tmp/."
fi

log "Starting $KEYSPACE generation (issue #1190)"
log "Output directory: $OUT_DIR"
log "Fixed writetime: T_WRITE=$T_WRITE"

SSTABLES_DIR="$OUT_DIR/sstables"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-writeparity \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$ROOT/schemas/write-load-parity.cql"

# Disable autocompaction so the freshly-flushed generation is not compacted away.
log "Disabling autocompaction for $KEYSPACE..."
run $ENGINE exec "$CONTAINER_NAME" nodetool disableautocompaction "$KEYSPACE" || true

insert_data

log "Flushing $KEYSPACE..."
run $ENGINE exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"

log "=== Exporting $KEYSPACE SSTables from container ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE into $SSTABLES_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.wlp_export_tmp"
  rm -rf "$TMPDIR_EXPORT"
  mkdir -p "$TMPDIR_EXPORT"

  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$TMPDIR_EXPORT" -xf -; then
    if [[ -d "$TMPDIR_EXPORT/data/$KEYSPACE" ]]; then
      rm -rf "$SSTABLES_DIR/$KEYSPACE"
      mkdir -p "$SSTABLES_DIR/$KEYSPACE"
      cp -r "$TMPDIR_EXPORT/data/$KEYSPACE/." "$SSTABLES_DIR/$KEYSPACE/"
      log "$KEYSPACE SSTables placed in $SSTABLES_DIR/$KEYSPACE"
    else
      fail "Expected $TMPDIR_EXPORT/data/$KEYSPACE but it was not found. Export failed."
    fi
    rm -rf "$TMPDIR_EXPORT"
  else
    fail "tar export from container failed."
  fi

  local_count=$(find "$SSTABLES_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" | wc -l | tr -d ' ')
  if [[ "$local_count" -eq 0 ]]; then
    fail "No Data.db files found under $SSTABLES_DIR/$KEYSPACE — export is empty!"
  fi
  log "  Found $local_count Data.db file(s) in $KEYSPACE"

  generate_sstabledump_jsonl "$SSTABLES_DIR"

  log "Generating Statistics.db.txt for $KEYSPACE tables..."
  while IFS= read -r -d '' data_file; do
    rel="${data_file#"$SSTABLES_DIR"/}"
    stats_base="${data_file%Data.db}Statistics.db.txt"
    log "  sstablemetadata: $rel"
    $ENGINE run --rm \
      -v "$SSTABLES_DIR:/data" \
      "$CASSANDRA_IMAGE" \
      bash -lc "/opt/cassandra/tools/bin/sstablemetadata /data/${rel}" \
      > "$stats_base" 2>/dev/null || true
    if [[ -s "$stats_base" ]]; then
      log "  OK: $stats_base"
    else
      log "  WARNING: Empty statistics for $rel"
    fi
  done < <(find "$SSTABLES_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" -print0)

  find "$SSTABLES_DIR/$KEYSPACE" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

  log "=== $KEYSPACE generation COMPLETE ==="
  log "SSTables: $SSTABLES_DIR/$KEYSPACE"
fi
