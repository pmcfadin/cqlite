#!/usr/bin/env bash
# generate-nested-udt-keys.sh — Cassandra 5.0.2 fixtures for NESTED UDTs reached
# through a HASHABLE position (set element / map key) — issue #3500.
#
# WHY
# The Python binding reduces every CQL set element and map key to a HASHABLE
# Python object. That reduction was not TOTAL over the cqlite_core::Value tree:
# a UDT reached through a tuple, or through a nested collection, fell through to
# the generic converter and raised `TypeError: unhashable type: 'dict'` /
# `'list'`. NO committed fixture in this repository declared any of those
# shapes, so the defect was UNREACHABLE from the corpus. This script generates a
# real Cassandra-written fixture that declares them, per issue #3500's
# "Generating one is part of the fix."
#
# Shapes generated (see test-data/schemas/nested-udt-keys.cql for the full
# rationale, per column):
#   s_tuple_udt     set<frozen<tuple<frozen<key_part>, int>>>
#   s_set_udt       set<frozen<set<frozen<key_part>>>>
#   m_tuple_udt     map<frozen<tuple<frozen<key_part>, int>>, int>
#   s_list_udt      set<frozen<list<frozen<key_part>>>>          (AC5 control)
#   f_set_tuple_udt frozen<set<frozen<tuple<frozen<key_part>, int>>>>
#
# This is a READ-fidelity fixture, not a compaction byte-parity fixture: there
# is ONE flush and therefore ONE SSTable generation per table, no explicit
# compaction, and no pinned USING TIMESTAMP. The oracles are the sstabledump
# JSONL golden committed beside the binaries and a real `SELECT`.
#
# Usage:
#   bash test-data/scripts/generate-nested-udt-keys.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~4 GB RAM for the container.
#
# ============================================================================
# MANDATORY: committing the .db binaries
#
# The *.db binary files produced by this script are gitignored (`*.db` is
# ignored globally) and will NOT be included by a bare `git add`. They MUST be
# force-added with `git add -f`, otherwise the committed JSONL/Digest sidecars
# point at a Data.db that is not in the tree and every consumer of the fixture
# silently reads ZERO rows. The script prints the exact commands at exit.
# ============================================================================
#
# Backs: issue #3500.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
KEEP_CONTAINER="${KEEP_CONTAINER:-0}"
CONTAINER_NAME="cqlite-nestedudtkeys"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_nested_udt_keys"
TABLES=(nested_udt_keys)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)            OUT_DIR="$2"; shift 2 ;;
    --dry-run)        DRY_RUN=1; shift ;;
    --keep-container) KEEP_CONTAINER=1; shift ;;
    *) echo "[nuk] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[nuk] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[nuk][ERROR] $*" >&2; exit 1; }

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
    echo "[nuk] (dry-run) no container engine found; using placeholder 'docker'"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

STARTED_CONTAINER=0
cleanup() {
  if [[ "$DRY_RUN" -eq 0 && "$KEEP_CONTAINER" -eq 0 && "$STARTED_CONTAINER" -eq 1 ]]; then
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
# Rows.
#
# Set/map literals are written deliberately OUT of sorted order so Cassandra's
# own writer performs the element ordering + de-duplication; the committed bytes
# therefore pin CASSANDRA's ordering of these composite elements, not ours.
# ----------------------------------------------------------------------------

# id 1 — fully populated, MULTIPLE distinct elements per collection.
#   * s_tuple_udt holds two tuples sharing the SAME udt component but different
#     trailing ints, so the whole tuple (not just the udt) is the sort/uniqueness
#     key.
#   * s_list_udt holds two lists with the same elements in DIFFERENT order, which
#     are DISTINCT list values — the case a set-of-lists exists to test.
insert_full() {
  log "=== nested_udt_keys id=1 (fully populated, multi-element) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt) VALUES (
    1,
    { ({label:'zulu', rank:26}, 7), ({label:'alpha', rank:1}, 2), ({label:'alpha', rank:1}, 1) },
    { { {label:'beta', rank:2}, {label:'alpha', rank:1} }, { {label:'gamma', rank:3} } },
    { ({label:'delta', rank:4}, 9): 90, ({label:'charlie', rank:3}, 8): 80 },
    { [ {label:'one', rank:1}, {label:'two', rank:2} ], [ {label:'two', rank:2}, {label:'one', rank:1} ] },
    { ({label:'frozen-b', rank:12}, 2), ({label:'frozen-a', rank:11}, 1) }
  )"
}

# id 2 — NULL UDT FIELDS inside every hashable position, plus an EMPTY-string
# field (distinct from null). value_to_hashable_key's Udt arm has a
# `None => py.None()` path that no committed fixture previously reached.
insert_null_fields() {
  log "=== nested_udt_keys id=2 (null UDT fields + empty-string field) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt) VALUES (
    2,
    { ({label:'nullrank', rank:null}, 1), ({label:null, rank:5}, 2) },
    { { {label:'nullrank2', rank:null}, {label:null, rank:null} } },
    { ({label:null, rank:null}, 0): 1, ({label:'', rank:0}, 0): 2 },
    { [ {label:'', rank:0}, {label:null, rank:9} ] },
    { ({label:null, rank:7}, 3) }
  )"
}

# id 3 — minimal: exactly ONE element in every collection, same udt value in all
# five columns, so a decoder that confuses two columns is visible.
insert_minimal() {
  log "=== nested_udt_keys id=3 (single element per collection) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt) VALUES (
    3,
    { ({label:'solo', rank:99}, 42) },
    { { {label:'solo', rank:99} } },
    { ({label:'solo', rank:99}, 42): 7 },
    { [ {label:'solo', rank:99} ] },
    { ({label:'solo', rank:99}, 42) }
  )"
}

# id 4 — ABSENT columns: only the tuple-borne set is written. The other four
# columns have no cells at all, so the row exercises the missing-column path
# alongside a populated hashable-position column in the same partition.
insert_partial() {
  log "=== nested_udt_keys id=4 (only s_tuple_udt present) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt) VALUES (
    4,
    { ({label:'partial', rank:4}, 4) }
  )"
}

verify_select() {
  log "=== SELECT * (evidence that Cassandra itself round-trips these shapes) ==="
  cql "SELECT * FROM nested_udt_keys"
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
        fail "JSONL golden is EMPTY: $jsonl_file (sstabledump produced nothing)"
      fi
      local lines
      lines=$(wc -l < "$jsonl_file" | tr -d ' ')
      log "  OK: $jsonl_file ($lines partitions)"
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

log "Starting $KEYSPACE generation (issue #3500)"
log "Output directory: $OUT_DIR"

SSTABLES_DIR="$OUT_DIR/sstables"

# Reuse an already-running container if one is present (the premise-validation
# workflow starts one by hand); otherwise start a fresh one.
if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  log "Reusing existing container '$CONTAINER_NAME'."
else
  log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
  run $ENGINE run -d \
    --name "$CONTAINER_NAME" \
    -e MAX_HEAP_SIZE=1G \
    -e HEAP_NEWSIZE=256m \
    -e CASSANDRA_CLUSTER_NAME=cqlite-nestedudtkeys \
    "$CASSANDRA_IMAGE"
  STARTED_CONTAINER=1
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$ROOT/schemas/nested-udt-keys.cql"

insert_full
insert_null_fields
insert_minimal
insert_partial
verify_select
flush_ks

log "=== Exporting $KEYSPACE SSTables from container ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE into $SSTABLES_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$SSTABLES_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.nuk_export_tmp"
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

  # ONE flush => exactly ONE Data.db per table.
  for table in "${TABLES[@]}"; do
    tdirs=( "$SSTABLES_DIR/$KEYSPACE/$table"* )
    if [[ ! -d "${tdirs[0]}" ]]; then
      fail "$table: no table directory matched under $SSTABLES_DIR/$KEYSPACE/ \
(glob '$SSTABLES_DIR/$KEYSPACE/$table*' did not expand); export failed"
    fi
    cnt=$(find "${tdirs[@]}" -name "*-Data.db" -not -name "._*" 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$cnt" -ne 1 ]]; then
      fail "$table: expected exactly ONE flushed Data.db, found $cnt."
    fi
    log "  $table: exactly one Data.db (OK)"
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
  echo "    test-data/datasets/sstables/$KEYSPACE/*/*-TOC.txt \\"
  echo "    test-data/datasets/sstables/$KEYSPACE/*/*-Digest.crc32 \\"
  echo "    test-data/datasets/sstables/$KEYSPACE/*/*-Statistics.db.txt"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'test(#3500): nested-UDT hashable-position fixture SSTables'"
  echo "=============================================================="
fi
