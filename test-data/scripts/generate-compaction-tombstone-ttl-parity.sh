#!/usr/bin/env bash
# generate-compaction-tombstone-ttl-parity.sh — Cassandra 5.0.2 TOMBSTONE / TTL
# COMPACTION byte-parity fixtures (issue #1387, epic #973).
#
# This is the tombstone/TTL analogue of generate-compaction-parity.sh (issue
# #1017, which pinned live-cell compaction byte parity). For each table it writes
# TWO (or more) overlapping SSTables, flushes each independently, disables
# autocompaction, then runs a single explicit MAJOR compaction (`nodetool
# compact`) so the inputs merge into exactly one output SSTable. The COMPACTED
# output's components are exported and committed.
#
# CQLite's `compact_sstables` re-produces the same merge over the same inputs; the
# byte-comparison test (issue_1387_tombstone_ttl_compaction_byte_parity.rs) diffs
# the two compacted outputs for Data.db / Index.db / Summary.db / Digest.crc32.
#
# ============================================================================
# DETERMINISM CONTRACT (see also schemas/compaction-tombstone-ttl-parity.cql)
#
#   T_A = 1000   older generation writetime (micros)
#   T_B = 2000   newer generation writetime (micros; wins/shadows overlaps)
#   T_DEL = 3000 explicit DELETE writetime (micros) so markedForDeleteAt is pinned
#
# CRITICAL — localDeletionTime (LDT) is NOT pinned by USING TIMESTAMP. Cassandra
# derives a tombstone/TTL cell's LDT from the coordinator WALL CLOCK at write
# time. USING TIMESTAMP only fixes markedForDeleteAt / writetime. Therefore:
#   * The committed golden captures whatever wall-clock LDT occurred at generation
#     time; the sstabledump JSONL golden records `local_delete_time` (ISO-8601).
#   * The CQLite byte-comparison test READS that LDT out of the committed golden
#     and stamps CQLite's compaction inputs with the SAME LDT (authoritative, not
#     guessed) via Mutation::with_local_deletion_time / Delete { local_deletion_time }.
#     This is how two independent compactors byte-match on tombstone LDT.
#
# PURGE determinism:
#   * gc_grace_seconds = 864000 tables (shadow_row_delete, ttl_expired_live,
#     rt_cross_gen): a recent tombstone LDT is FAR below gcBefore's grace window so
#     it is NOT purgeable → the tombstone/marker SURVIVES the major compaction.
#   * gc_grace_seconds = 0 table (gc_purge_grace0): gcBefore = nowInSeconds, so any
#     tombstone with LDT <= now is immediately purgeable. With no live data to
#     shadow, the major compaction PURGES the tombstone entirely → deterministic
#     and wall-clock independent (LDT is always <= now at compaction time).
#
# Tables are UNCOMPRESSED (no CompressionInfo.db); PKs are int/(int,int).
#
# Usage:
#   bash test-data/scripts/generate-compaction-tombstone-ttl-parity.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~4 GB RAM for the container.
#
# ============================================================================
# MANDATORY: committing the .db binaries
#
# The *.db binaries produced here are gitignored and will NOT be picked up by a
# bare `git add`. They MUST be force-added with `git add -f`, otherwise the
# committed Digest.crc32 sidecars point to a stale Data.db and the byte-parity
# test FAILS with a "committed Digest.crc32 does not match CRC32 of committed
# Data.db" error. The script prints the exact commands at exit.
# ============================================================================
#
# Backs: issue #1387 (epic #973).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="${CONTAINER_NAME:-cqlite-cttlparity}"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_compaction_tombstone_ttl"

# Fixed writetimes (micros). MUST match the constants in
# issue_1387_tombstone_ttl_compaction_byte_parity.rs.
T_A=1000     # older generation
T_B=2000     # newer generation (wins/shadows overlaps)
T_DEL=3000   # explicit DELETE writetime (markedForDeleteAt)

# All four tables under test.
TABLES=(shadow_row_delete ttl_expired_live gc_purge_grace0 rt_cross_gen)

# gc_purge_grace0 is the ONLY table expected to lose its output entirely (the
# purged tombstone leaves an empty SSTable). Cassandra emits NO Data.db when a
# major compaction produces zero unfiltered output, so that table is validated by
# the "no surviving Data.db" contract rather than an exported .db.
PURGE_EMPTY_TABLE="gc_purge_grace0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[cttl] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[cttl] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[cttl][ERROR] $*" >&2; exit 1; }

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
    echo "[cttl] (dry-run) no container engine found; using placeholder 'docker'"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

# The container may be pre-started by the caller (spike / CI warmup). Only refuse
# a pre-existing container in a fresh run when the caller did NOT set REUSE=1.
REUSE="${REUSE:-0}"
if [[ "$DRY_RUN" -eq 0 && "$REUSE" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME  (or re-run with REUSE=1 to reuse a warm container)"
fi

cleanup() {
  if [[ "$DRY_RUN" -eq 0 && "$REUSE" -eq 0 ]]; then
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
# issue_1387_tombstone_ttl_compaction_byte_parity.rs.
# ----------------------------------------------------------------------------

# (a) shadow_row_delete: newer-gen ROW tombstone shadows older-gen live row.
#   A (T_A): (1,1,'a-1-1'),(1,2,'a-1-2'),(2,0,'a-2-0')
#   B: DELETE (1,1) USING TIMESTAMP T_DEL  (row tombstone shadows a-1-1)
#      INSERT (1,3,'b-1-3') USING TIMESTAMP T_B (new live row)
# Surviving after merge: (1,1)=row tombstone, (1,2)='a-1-2', (1,3)='b-1-3', (2,0)='a-2-0'.
insert_shadow_row_delete() {
  log "=== shadow_row_delete: group A (USING TIMESTAMP $T_A) ==="
  cql "INSERT INTO shadow_row_delete (id, ck, v) VALUES (1, 1, 'a-1-1') USING TIMESTAMP $T_A"
  cql "INSERT INTO shadow_row_delete (id, ck, v) VALUES (1, 2, 'a-1-2') USING TIMESTAMP $T_A"
  cql "INSERT INTO shadow_row_delete (id, ck, v) VALUES (2, 0, 'a-2-0') USING TIMESTAMP $T_A"
  flush_ks
  log "=== shadow_row_delete: group B (DELETE + new row) ==="
  cql "DELETE FROM shadow_row_delete USING TIMESTAMP $T_DEL WHERE id = 1 AND ck = 1"
  cql "INSERT INTO shadow_row_delete (id, ck, v) VALUES (1, 3, 'b-1-3') USING TIMESTAMP $T_B"
  flush_ks
}

# (b) ttl_expired_live: an already-expired TTL cell + a live cell across gens.
#   A (T_A, TTL 1s): (1,1,'a-1-1') expires ~1s after write (LDT ~ now+1)
#   B (T_B): (1,2,'b-1-2') live
# By the time the compaction runs the TTL cell is expired; the merge converts it
# to a cell/row tombstone (kept because gc_grace is default). The exact LDT is
# wall-clock; the test reads it from the golden.
insert_ttl_expired_live() {
  log "=== ttl_expired_live: group A (USING TIMESTAMP $T_A AND TTL 1) ==="
  cql "INSERT INTO ttl_expired_live (id, ck, v) VALUES (1, 1, 'a-1-1') USING TIMESTAMP $T_A AND TTL 1"
  flush_ks
  log "=== ttl_expired_live: group B (USING TIMESTAMP $T_B, live) ==="
  cql "INSERT INTO ttl_expired_live (id, ck, v) VALUES (1, 2, 'b-1-2') USING TIMESTAMP $T_B"
  flush_ks
  log "Waiting 3s for the TTL cell to expire before compaction..."
  run sleep 3
}

# (c) gc_purge_grace0: a row tombstone with nothing to shadow, gc_grace=0.
#   A: DELETE (1,1) USING TIMESTAMP T_DEL  (row tombstone, no live data anywhere)
#   B: DELETE (2,0) USING TIMESTAMP T_DEL  (row tombstone in a second flushed gen)
# gcBefore = now (gc_grace 0), so both tombstones are purgeable → major compaction
# produces ZERO output (no Data.db). Deterministic (LDT always <= now).
insert_gc_purge_grace0() {
  log "=== gc_purge_grace0: group A (row tombstone, gc_grace=0) ==="
  cql "DELETE FROM gc_purge_grace0 USING TIMESTAMP $T_DEL WHERE id = 1 AND ck = 1"
  flush_ks
  log "=== gc_purge_grace0: group B (second row tombstone) ==="
  cql "DELETE FROM gc_purge_grace0 USING TIMESTAMP $T_DEL WHERE id = 2 AND ck = 0"
  flush_ks
}

# (d) rt_cross_gen: cross-generation range-tombstone open/close.
#   A: DELETE range (1, ck in [10,20]) USING TIMESTAMP T_DEL ; INSERT (1,5,'a-1-5')
#   B: DELETE range (1, ck in [15,25]) USING TIMESTAMP (T_DEL+1) ; INSERT (1,30,'b-1-30')
# The two overlapping range tombstones + live rows exercise cross-generation RT
# boundary synthesis (open/close bound markers) in the merged output.
insert_rt_cross_gen() {
  log "=== rt_cross_gen: group A (range [10,20] + live row) ==="
  cql "INSERT INTO rt_cross_gen (id, ck, v) VALUES (1, 5, 'a-1-5') USING TIMESTAMP $T_A"
  cql "DELETE FROM rt_cross_gen USING TIMESTAMP $T_DEL WHERE id = 1 AND ck >= 10 AND ck <= 20"
  flush_ks
  # localDeletionTime is second-granularity WALL CLOCK (not pinned by USING
  # TIMESTAMP). The cross-generation RT-merge byte-parity test reads the two
  # range-tombstone LDTs out of the golden and stamps CQLite's two inputs with
  # DISTINCT LDTs (ldts[0] != ldts[1]); the strict fixture guard likewise asserts
  # two distinct LDTs. Without this pause the two back-to-back DELETEs can land in
  # the SAME wall-clock second, collapsing to a single LDT and failing a valid
  # regeneration. Sleep >1s so the group-B RT LDT is deterministically a later
  # second than group A's (issue #1387 wall-clock-race fix).
  run sleep 2
  log "=== rt_cross_gen: group B (overlapping range [15,25] + live row) ==="
  cql "INSERT INTO rt_cross_gen (id, ck, v) VALUES (1, 30, 'b-1-30') USING TIMESTAMP $T_B"
  cql "DELETE FROM rt_cross_gen USING TIMESTAMP $((T_DEL + 1)) WHERE id = 1 AND ck >= 15 AND ck <= 25"
  flush_ks
}

major_compact() {
  local table="$1"
  log "=== Major-compacting $KEYSPACE.$table ==="
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

log "Starting $KEYSPACE generation (issue #1387)"
log "Output directory: $OUT_DIR"
log "Fixed writetimes: T_A=$T_A T_B=$T_B T_DEL=$T_DEL"

SSTABLES_DIR="$OUT_DIR/sstables"

if [[ "$REUSE" -eq 0 ]]; then
  log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
  run $ENGINE run -d \
    --name "$CONTAINER_NAME" \
    -e MAX_HEAP_SIZE=1G \
    -e HEAP_NEWSIZE=256m \
    -e CASSANDRA_CLUSTER_NAME=cqlite-cttlparity \
    "$CASSANDRA_IMAGE"
else
  log "Reusing pre-started container $CONTAINER_NAME (REUSE=1)."
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$ROOT/schemas/compaction-tombstone-ttl-parity.cql"

log "Disabling autocompaction for $KEYSPACE..."
run $ENGINE exec "$CONTAINER_NAME" nodetool disableautocompaction "$KEYSPACE"

insert_shadow_row_delete
insert_ttl_expired_live
insert_gc_purge_grace0
insert_rt_cross_gen

for table in "${TABLES[@]}"; do
  major_compact "$table"
done

log "=== Exporting $KEYSPACE SSTables from container ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE into $SSTABLES_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.cttl_export_tmp"
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

  # Contract per table:
  #   * gc_purge_grace0 → EXACTLY ZERO Data.db (the purge produced empty output).
  #   * every other table → EXACTLY ONE compacted Data.db.
  for table in "${TABLES[@]}"; do
    tdirs=( "$SSTABLES_DIR/$KEYSPACE/$table"* )
    if [[ ! -d "${tdirs[0]}" ]]; then
      fail "$table: no table directory matched under $SSTABLES_DIR/$KEYSPACE/; export failed"
    fi
    cnt=$(find "${tdirs[@]}" -name "*-Data.db" -not -name "._*" 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$table" == "$PURGE_EMPTY_TABLE" ]]; then
      if [[ "$cnt" -ne 0 ]]; then
        fail "$table: expected ZERO Data.db (gc_grace=0 purge), found $cnt. \
The purgeable tombstone was NOT dropped — check gc_grace_seconds / LDT vs now."
      fi
      log "  $table: zero surviving Data.db after gc-purge (OK)"
    else
      if [[ "$cnt" -ne 1 ]]; then
        fail "$table: expected exactly ONE compacted Data.db, found $cnt. \
Major compaction did not collapse inputs into one output."
      fi
      log "  $table: exactly one compacted Data.db (OK)"
    fi
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
  done < <(find "$SSTABLES_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" -print0)

  find "$SSTABLES_DIR/$KEYSPACE" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

  log "=== $KEYSPACE generation COMPLETE ==="
  log "SSTables: $SSTABLES_DIR/$KEYSPACE"

  echo ""
  echo "=============================================================="
  echo "  NEXT: commit the generated fixtures"
  echo "=============================================================="
  echo ""
  echo "  # Force-add the .db binaries (gitignored — MUST use -f):"
  echo "  git -C '$REPO_ROOT' add -f \\"
  echo "    test-data/datasets/sstables/$KEYSPACE/*/*.db"
  echo ""
  echo "  # Add the sidecars normally (not gitignored):"
  echo "  git -C '$REPO_ROOT' add \\"
  echo "    test-data/datasets/sstables/$KEYSPACE/*/*.jsonl \\"
  echo "    test-data/datasets/sstables/$KEYSPACE/*/TOC.txt \\"
  echo "    test-data/datasets/sstables/$KEYSPACE/*/Digest.crc32"
  echo ""
  echo "  git -C '$REPO_ROOT' commit -m 'fixtures(#1387): regenerate tombstone/TTL compaction golden SSTables'"
  echo "=============================================================="
fi
