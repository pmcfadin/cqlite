#!/usr/bin/env bash
# generate-signed-collection-parity.sh — Cassandra 5.0.2 reference SSTables for
# the SIGNED numeric collection-element / map-key ordering golden (issue #1295,
# follow-up to #1275).
#
# Produces committed Cassandra-written reference SSTables for the
# test_signed_coll keyspace (schemas/signed-collection-parity.cql) whose
# collections MIX NEGATIVE AND POSITIVE numeric elements/keys. The on-disk
# element/key ORDER (Cassandra's signed SetType/MapType comparator) is the
# authoritative oracle the read-path parity test asserts against.
#
# Tables:
#   signed_int_collections    — non-frozen set<int> / map<int,text> w/ negatives
#   frozen_int_collections    — frozen<set<int>> / frozen<map<int,text>> w/ negatives
#   signed_width_collections  — set<bigint> / set<smallint> / set<tinyint>
#   signed_special_collections — set<decimal> (large unscaled) + set<double> (NaN/±0.0)
#
# Lifecycle / export / sstabledump-JSONL steps mirror
# generate-write-load-parity.sh exactly so committed reference files
# (`*-Data.db.jsonl`, `*-Statistics.db.txt`) are consistent with the corpus.
#
# Usage:
#   bash test-data/scripts/generate-signed-collection-parity.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~4 GB RAM for the container.
#
# ROUTING: fixture generation is SLOW under emulation and belongs on the
# nightly/exhaustive lane, NOT the required gate. The byte-for-byte read-path
# parity test is skip-on-absence so the required gate stays green when the
# local-only binaries are absent in CI.
#
# Backs: issue #1295 (epic #968 type/comparator parity).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-signedcoll"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_signed_coll"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[scp] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[scp] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[scp][ERROR] $*" >&2; exit 1; }

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
    echo "[scp] (dry-run) no container engine found; using placeholder 'docker'"
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
# Insert collection data with NEGATIVE + POSITIVE numeric elements/keys, fed in
# DELIBERATELY UNSORTED order so the on-disk persisted order reflects Cassandra's
# SIGNED comparator (not insertion order).
# ----------------------------------------------------------------------------
insert_data() {
  log "=== Inserting signed-collection fixture data ==="
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] signed_int_collections id=1 s={3,-1,1,0,-2} m fed unsorted negative keys"
    echo "[dry-run] frozen_int_collections id=1 fs/fm same negative shapes (frozen)"
    echo "[dry-run] signed_width_collections id=1 bigint/smallint/tinyint negatives"
    echo "[dry-run] signed_special_collections id=1 decimal(large unscaled)/double(NaN,±0.0)"
    return 0
  fi

  # set<int> / map<int,text>: elements/keys fed unsorted, mix of neg/pos.
  # Signed order -> set {-2,-1,0,1,3}; map keys {-5,-1,0,2}.
  cql "INSERT INTO signed_int_collections (id, s, m) VALUES (1, {3, -1, 1, 0, -2}, {2: 'two', -1: 'neg-one', 0: 'zero', -5: 'neg-five'})"

  # frozen<set<int>> / frozen<map<int,text>>: same negative shapes (single cell).
  cql "INSERT INTO frozen_int_collections (id, fs, fm) VALUES (1, {3, -1, 1, 0, -2}, {2: 'two', -1: 'neg-one', 0: 'zero', -5: 'neg-five'})"

  # set<bigint> / set<smallint> / set<tinyint>: negatives + positives, unsorted.
  cql "INSERT INTO signed_width_collections (id, sb, ss, st) VALUES (1, {100, -100, 0, -1, 1}, {300, -300, 0, -1, 1}, {120, -120, 0, -1, 1})"

  # set<decimal>: a LARGE-unscaled value (beyond i128) plus negatives, unsorted.
  # set<double>: NaN, -0.0, +0.0, negatives, positives — locks the double
  # comparator branch (NaN sorts LAST, -0.0 < +0.0 in Cassandra DoubleType).
  cql "INSERT INTO signed_special_collections (id, sd, sf) VALUES (1, {123456789012345678901234567890.123, -1.5, 0, -999999999999999999999999999999.999}, {NaN, -0.0, 0.0, -1.5, 2.5, Infinity, -Infinity})"

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

log "Starting $KEYSPACE generation (issue #1295)"
log "Output directory: $OUT_DIR"

SSTABLES_DIR="$OUT_DIR/sstables"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-signedcoll \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$ROOT/schemas/signed-collection-parity.cql"

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

  TMPDIR_EXPORT="$OUT_DIR/.scp_export_tmp"
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
