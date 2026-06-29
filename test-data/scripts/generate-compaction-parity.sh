#!/usr/bin/env bash
# generate-compaction-parity.sh — Cassandra 5.0.2 live-cell COMPACTION byte-parity
# fixtures (issue #1017, epic #973).
#
# Produces committed Cassandra-COMPACTED reference SSTables for the two live-cell
# compaction byte-parity scenarios in cassandra-parity-manifest.yml:
#   * cqlite.compaction_parity.live_cells.no_clustering   (live_no_clustering)
#   * cqlite.compaction_parity.live_cells.clustering_lww  (live_clustering)
#
# For each table the generator writes TWO overlapping SSTables (group A at
# USING TIMESTAMP T_A, group B at the newer T_B that wins overlaps), flushes each
# independently, then runs a single explicit MAJOR compaction
# (`nodetool compact`) so exactly two inputs merge into exactly one output
# SSTable. autocompaction is disabled so the two flushed generations are not
# compacted before the explicit major compaction.
#
# The COMPACTED output SSTable's components are exported and committed. CQLite's
# `compact_sstables` re-produces the same merge over the same two inputs; the
# byte-comparison test diffs the two compacted outputs.
#
# Lifecycle / flag parsing / export / sstabledump-JSONL / sstablemetadata steps
# mirror generate-write-load-parity.sh exactly so committed reference files
# (`*-Data.db.jsonl`, `*-Statistics.db.txt`, `TOC.txt`, `Digest.crc32`) are
# consistent with the rest of the corpus.
#
# ============================================================================
# FIXED TIMESTAMPS (also documented in schemas/compaction-parity.cql)
#   T_A = 1000   (older generation), micros
#   T_B = 2000   (newer generation, wins overlaps), micros
# All INSERTs use an explicit USING TIMESTAMP. No TTL, no DELETE: purge never
# fires for live cells, so the compaction output is independent of
# gcBefore / nowInSeconds and reproducible by CQLite's compactor.
# ============================================================================
#
# Usage:
#   bash test-data/scripts/generate-compaction-parity.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~4 GB RAM for the container.
#
# Backs: issue #1017 (epic #973).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-compactionparity"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_compactionparity"

# Fixed writetimes (micros). MUST match the constants the byte-comparison test
# feeds CQLite's compactor (issue_1017_live_cell_compaction_byte_parity.rs).
T_A=1000   # older generation
T_B=2000   # newer generation (wins overlaps)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[ccp] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[ccp] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[ccp][ERROR] $*" >&2; exit 1; }

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
    echo "[ccp] (dry-run) no container engine found; using placeholder 'docker'"
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

flush_ks() {
  log "Flushing $KEYSPACE..."
  run $ENGINE exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"
}

# ----------------------------------------------------------------------------
# Per-table data. The SAME logical rows/timestamps are re-emitted by CQLite in
# issue_1017_live_cell_compaction_byte_parity.rs. Each table is written as TWO
# overlapping flushed SSTables, then explicitly major-compacted into one.
# ----------------------------------------------------------------------------

# live_no_clustering: partition-key-only LWW overlap.
#   A (T_A): id 1,2,3 v='a-1','a-2','a-3'
#   B (T_B): id 2,3 override v='b-2','b-3'; id 4 v='b-4'
# Surviving after merge: 1='a-1', 2='b-2', 3='b-3', 4='b-4'.
insert_no_clustering() {
  log "=== live_no_clustering: group A (USING TIMESTAMP $T_A) ==="
  cql "INSERT INTO live_no_clustering (id, v) VALUES (1, 'a-1') USING TIMESTAMP $T_A"
  cql "INSERT INTO live_no_clustering (id, v) VALUES (2, 'a-2') USING TIMESTAMP $T_A"
  cql "INSERT INTO live_no_clustering (id, v) VALUES (3, 'a-3') USING TIMESTAMP $T_A"
  flush_ks
  log "=== live_no_clustering: group B (USING TIMESTAMP $T_B) ==="
  cql "INSERT INTO live_no_clustering (id, v) VALUES (2, 'b-2') USING TIMESTAMP $T_B"
  cql "INSERT INTO live_no_clustering (id, v) VALUES (3, 'b-3') USING TIMESTAMP $T_B"
  cql "INSERT INTO live_no_clustering (id, v) VALUES (4, 'b-4') USING TIMESTAMP $T_B"
  flush_ks
}

# live_clustering: clustering LWW overlap with preserved clustering order.
#   A (T_A): (1,0,'a-1-0'),(1,1,'a-1-1'),(2,0,'a-2-0'),(3,0,'a-3-0')
#   B (T_B): (1,1,'b-1-1') override, (1,2,'b-1-2') new ck, (4,0,'b-4-0') new partition
# Surviving: (1,0,'a-1-0'),(1,1,'b-1-1'),(1,2,'b-1-2'),(2,0,'a-2-0'),(3,0,'a-3-0'),(4,0,'b-4-0').
insert_clustering() {
  log "=== live_clustering: group A (USING TIMESTAMP $T_A) ==="
  cql "INSERT INTO live_clustering (id, ck, v) VALUES (1, 0, 'a-1-0') USING TIMESTAMP $T_A"
  cql "INSERT INTO live_clustering (id, ck, v) VALUES (1, 1, 'a-1-1') USING TIMESTAMP $T_A"
  cql "INSERT INTO live_clustering (id, ck, v) VALUES (2, 0, 'a-2-0') USING TIMESTAMP $T_A"
  cql "INSERT INTO live_clustering (id, ck, v) VALUES (3, 0, 'a-3-0') USING TIMESTAMP $T_A"
  flush_ks
  log "=== live_clustering: group B (USING TIMESTAMP $T_B) ==="
  cql "INSERT INTO live_clustering (id, ck, v) VALUES (1, 1, 'b-1-1') USING TIMESTAMP $T_B"
  cql "INSERT INTO live_clustering (id, ck, v) VALUES (1, 2, 'b-1-2') USING TIMESTAMP $T_B"
  cql "INSERT INTO live_clustering (id, ck, v) VALUES (4, 0, 'b-4-0') USING TIMESTAMP $T_B"
  flush_ks
}

major_compact() {
  local table="$1"
  log "=== Major-compacting $KEYSPACE.$table (two inputs -> one output) ==="
  run $ENGINE exec "$CONTAINER_NAME" nodetool compact "$KEYSPACE" "$table"
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
  done < <(find "$sstables_dir" -type f -name "*-Data.db" -not -name "._*" -print0 \
            | grep -z "$KEYSPACE" 2>/dev/null || true)
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

log "Starting $KEYSPACE generation (issue #1017)"
log "Output directory: $OUT_DIR"
log "Fixed writetimes: T_A=$T_A T_B=$T_B"

SSTABLES_DIR="$OUT_DIR/sstables"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-compactionparity \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$ROOT/schemas/compaction-parity.cql"

# Disable autocompaction so the two freshly-flushed generations are NOT compacted
# before the single explicit major compaction below.
log "Disabling autocompaction for $KEYSPACE..."
run $ENGINE exec "$CONTAINER_NAME" nodetool disableautocompaction "$KEYSPACE" || true

insert_no_clustering
insert_clustering

major_compact live_no_clustering
major_compact live_clustering

log "=== Exporting $KEYSPACE SSTables from container ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE into $SSTABLES_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.ccp_export_tmp"
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

  # A major compaction leaves EXACTLY ONE Data.db per table. If more than one
  # survives, the explicit compaction did not run (or autocompaction interfered):
  # fail loudly rather than commit an ambiguous, non-compacted fixture.
  for table in live_no_clustering live_clustering; do
    tdir="$SSTABLES_DIR/$KEYSPACE/$table"*
    cnt=$(find $tdir -name "*-Data.db" -not -name "._*" 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$cnt" -ne 1 ]]; then
      fail "$table: expected exactly ONE compacted Data.db, found $cnt. \
Major compaction did not collapse the two inputs into one output."
    fi
    log "  $table: exactly one compacted Data.db (OK)"
  done

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
