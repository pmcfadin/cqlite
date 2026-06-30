#!/usr/bin/env bash
# generate-compaction-parity-udt.sh — Cassandra 5.0.2 UDT / frozen-value
# COMPACTION byte-parity fixtures (issue #1020, epic #973).
#
# The UDT / frozen-value extension of generate-compaction-parity.sh (#1017).
# Produces committed Cassandra-COMPACTED reference SSTables for the three
# UDT/frozen scenarios in cassandra-parity-manifest.yml:
#   * cqlite.compaction_parity.udt.frozen_person       (udt_frozen_person)
#   * cqlite.compaction_parity.udt.nested_udt          (udt_nested)
#   * cqlite.compaction_parity.udt.collections_with_udts (udt_collections)
#
# For each table the generator writes TWO overlapping SSTables (group A at
# USING TIMESTAMP T_A, group B at the newer T_B that wins overlaps), flushes each
# independently, then runs a single explicit MAJOR compaction
# (`nodetool compact`) so exactly two inputs merge into exactly one output
# SSTable. autocompaction is disabled so the two flushed generations are not
# compacted before the explicit major compaction.
#
# The COMPACTED output SSTable's components are exported and committed. CQLite's
# `compact_sstables` re-produces the same merge over the same two inputs (built
# via its public WriteEngine API with a UdtRegistry built from the SAME UDT
# definitions in schemas/compaction-parity-udt.cql); the byte-comparison test
# diffs the two compacted outputs.
#
# Every complex value here is FROZEN, so it serializes as a SINGLE value cell —
# the smallest deterministic surface two independent compactors can byte-match.
#
# ============================================================================
# FIXED TIMESTAMPS (also documented in schemas/compaction-parity-udt.cql)
#   T_A = 1000   (older generation), micros
#   T_B = 2000   (newer generation, wins overlaps), micros
# All INSERTs use an explicit USING TIMESTAMP. No TTL, no DELETE: purge never
# fires for live cells, so the compaction output is independent of
# gcBefore / nowInSeconds and reproducible by CQLite's compactor.
# ============================================================================
#
# Usage:
#   bash test-data/scripts/generate-compaction-parity-udt.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~4 GB RAM for the container.
#
# ============================================================================
# MANDATORY: committing the .db binaries
#
# The *.db binary files produced by this script are gitignored and will NOT be
# included by a bare `git add`. They MUST be force-added with `git add -f` after
# every regeneration, otherwise the committed Digest.crc32 sidecars will point to
# a stale Data.db and the byte-parity test will FAIL. The script prints the exact
# commands at exit.
# ============================================================================
#
# Backs: issue #1020 (epic #973).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-compactionparityudt"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_compactionparityudt"
TABLES=(udt_frozen_person udt_nested udt_collections)

# Fixed writetimes (micros). MUST match the constants the byte-comparison test
# feeds CQLite's compactor (issue_1020_udt_frozen_compaction_byte_parity.rs).
T_A=1000   # older generation
T_B=2000   # newer generation (wins overlaps)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[ccpu] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[ccpu] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[ccpu][ERROR] $*" >&2; exit 1; }

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
    echo "[ccpu] (dry-run) no container engine found; using placeholder 'docker'"
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
# issue_1020_udt_frozen_compaction_byte_parity.rs. Each table is written as TWO
# overlapping flushed SSTables, then explicitly major-compacted into one.
# ----------------------------------------------------------------------------

# udt_frozen_person: frozen<person> LWW overlap.
#   A (T_A): id 1 full, id 2 null-middle, id 3 empty-first, id 5 null-middle (NEVER overridden)
#   B (T_B): id 2 override (full), id 3 unchanged-shape override, id 4 new
# Surviving: 1=A, 2=B, 3=B, 4=B, 5=A.
# id 5 is written ONLY in group A and never overwritten, so a SURVIVING value
# carrying a null middle field (last_name:null) is verified in the COMPACTED
# output — exercising the `-1` absent-field encoding on the winning side
# (roborev #1020 Finding 2: the prior null-field rows were all overwritten).
insert_frozen_person() {
  log "=== udt_frozen_person: group A (USING TIMESTAMP $T_A) ==="
  cql "INSERT INTO udt_frozen_person (id, p) VALUES (1, {first_name:'Ada', last_name:'Lovelace', age:36}) USING TIMESTAMP $T_A"
  cql "INSERT INTO udt_frozen_person (id, p) VALUES (2, {first_name:'Grace', last_name:null, age:85}) USING TIMESTAMP $T_A"
  cql "INSERT INTO udt_frozen_person (id, p) VALUES (3, {first_name:'', last_name:'Turing', age:41}) USING TIMESTAMP $T_A"
  cql "INSERT INTO udt_frozen_person (id, p) VALUES (5, {first_name:'Edsger', last_name:null, age:75}) USING TIMESTAMP $T_A"
  flush_ks
  log "=== udt_frozen_person: group B (USING TIMESTAMP $T_B) ==="
  cql "INSERT INTO udt_frozen_person (id, p) VALUES (2, {first_name:'Grace', last_name:'Hopper', age:85}) USING TIMESTAMP $T_B"
  cql "INSERT INTO udt_frozen_person (id, p) VALUES (3, {first_name:'Alan', last_name:'Turing', age:41}) USING TIMESTAMP $T_B"
  cql "INSERT INTO udt_frozen_person (id, p) VALUES (4, {first_name:'Katherine', last_name:'Johnson', age:101}) USING TIMESTAMP $T_B"
  flush_ks
}

# udt_nested: frozen<employee> (contains frozen<address>) LWW overlap.
#   A (T_A): id 1 full nested, id 2 inner-null-city
#   B (T_B): id 2 override, id 3 new
# Surviving: 1=A, 2=B, 3=B.
insert_nested() {
  log "=== udt_nested: group A (USING TIMESTAMP $T_A) ==="
  cql "INSERT INTO udt_nested (id, e) VALUES (1, {name:'Grace', home:{street:'1 Navy Way', city:'Arlington', zip:'22201'}, level:9}) USING TIMESTAMP $T_A"
  cql "INSERT INTO udt_nested (id, e) VALUES (2, {name:'NoCity', home:{street:'5 Elm', city:null, zip:'00000'}, level:0}) USING TIMESTAMP $T_A"
  flush_ks
  log "=== udt_nested: group B (USING TIMESTAMP $T_B) ==="
  cql "INSERT INTO udt_nested (id, e) VALUES (2, {name:'WithCity', home:{street:'5 Elm', city:'Dover', zip:'00000'}, level:2}) USING TIMESTAMP $T_B"
  cql "INSERT INTO udt_nested (id, e) VALUES (3, {name:'Katherine', home:{street:'9 Apollo', city:'Hampton', zip:'23666'}, level:11}) USING TIMESTAMP $T_B"
  flush_ks
}

# udt_collections: frozen collections incl. collections-of-UDT LWW overlap.
#   A (T_A): id 1 full, id 2 partial
#   B (T_B): id 2 override, id 3 new
# Surviving: 1=A, 2=B, 3=B.
insert_collections() {
  log "=== udt_collections: group A (USING TIMESTAMP $T_A) ==="
  cql "INSERT INTO udt_collections (id, fl, fm, lp, ma) VALUES (1, [1,2,3], {'x':10,'y':20}, [{first_name:'Ada', last_name:'Lovelace', age:36}], {'home':{street:'1 Navy Way', city:'Arlington', zip:'22201'}}) USING TIMESTAMP $T_A"
  cql "INSERT INTO udt_collections (id, fl, fm, lp, ma) VALUES (2, [9], {'z':99}, [{first_name:'Old', last_name:'Val', age:1}], {'k':{street:'old', city:'old', zip:'0'}}) USING TIMESTAMP $T_A"
  flush_ks
  log "=== udt_collections: group B (USING TIMESTAMP $T_B) ==="
  cql "INSERT INTO udt_collections (id, fl, fm, lp, ma) VALUES (2, [4,5], {'a':1,'b':2}, [{first_name:'Grace', last_name:'Hopper', age:85},{first_name:'Alan', last_name:'Turing', age:41}], {'office':{street:'9 Apollo', city:'Hampton', zip:'23666'}}) USING TIMESTAMP $T_B"
  cql "INSERT INTO udt_collections (id, fl, fm, lp, ma) VALUES (3, [7,8,9], {'q':1}, [{first_name:'Katherine', last_name:'Johnson', age:101}], {'h':{street:'9 Apollo', city:'Hampton', zip:'23666'}}) USING TIMESTAMP $T_B"
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

log "Starting $KEYSPACE generation (issue #1020)"
log "Output directory: $OUT_DIR"
log "Fixed writetimes: T_A=$T_A T_B=$T_B"

SSTABLES_DIR="$OUT_DIR/sstables"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-compactionparityudt \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$ROOT/schemas/compaction-parity-udt.cql"

# Disable autocompaction so the two freshly-flushed generations are NOT compacted
# before the single explicit major compaction below.
log "Disabling autocompaction for $KEYSPACE..."
run $ENGINE exec "$CONTAINER_NAME" nodetool disableautocompaction "$KEYSPACE"

insert_frozen_person
insert_nested
insert_collections

for table in "${TABLES[@]}"; do
  major_compact "$table"
done

log "=== Exporting $KEYSPACE SSTables from container ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE into $SSTABLES_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.ccpu_export_tmp"
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

  # A major compaction leaves EXACTLY ONE Data.db per table.
  for table in "${TABLES[@]}"; do
    tdirs=( "$SSTABLES_DIR/$KEYSPACE/$table"* )
    if [[ ! -d "${tdirs[0]}" ]]; then
      fail "$table: no table directory matched under $SSTABLES_DIR/$KEYSPACE/ \
(glob '$SSTABLES_DIR/$KEYSPACE/$table*' did not expand); export failed"
    fi
    cnt=$(find "${tdirs[@]}" -name "*-Data.db" -not -name "._*" 2>/dev/null | wc -l | tr -d ' ')
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
  echo "    test-data/datasets/sstables/$KEYSPACE/*/Digest.crc32 \\"
  echo "    test-data/datasets/sstables/$KEYSPACE/*/Statistics.db.txt"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'fixtures(#1020): regenerate UDT/frozen compaction-parity golden SSTables'"
  echo "=============================================================="
fi
