#!/usr/bin/env bash
# generate-issue-3504-udt-collision.sh — Cassandra 5.0.2 UDT FIELD-NAME /
# TYPE-IDENTITY COLLISION fixture (issue #3504).
#
# WHAT THIS PRODUCES AND WHY
# The bindings used to render a UDT as ONE FLAT namespace holding both the
# injected type identity (`_type` / `_keyspace`) and the UDT's own declared
# fields, so a UDT that DECLARES a field literally named `_type`/`_keyspace`
# silently overwrote the marker. NO fixture in the corpus declares such a field,
# so the defect had no test subject. This script generates one.
#
# CASSANDRA-WRITTEN, NOT CQLITE-WRITTEN. Deliberate, for two reasons beyond the
# usual round-trip warning: (1) a Cassandra-written fixture additionally proves
# the DECODER can produce such a UDT at all, which a hand-constructed in-memory
# `Value::Udt` cannot; (2) CQLite's write path could not supply it anyway —
# nothing in cqlite-core/src/cql/ ever constructs a `CqlLiteral::Udt`, so an
# INSERT cannot produce a UDT value.
#
# Schema: test-data/schemas/issue-3504-udt-collision.cql (committed alongside;
# read it for the type/column rationale). Rows are described at insert_rows().
#
# ============================================================================
# OUTPUT LOCATION IS LOAD-BEARING
#
# The fixture is written CHECKOUT-RELATIVE to
#   test-data/fixtures/issue_3504/<keyspace>/<table>-<uuid>/
# and NOT under test-data/datasets/sstables/. Reason: both binding test suites
# (bindings/python/tests/conftest.py:42-48, bindings/node/__test__/setup.js:23)
# resolve the dataset corpus as an EITHER/OR on CQLITE_DATASETS_ROOT: unset, they
# DO fall back to the checkout's test-data/datasets. But when it IS set -- which
# every gate run does -- the checkout copy is never consulted, so a corpus-rooted
# fixture is INVISIBLE exactly where these suites run. A checkout-relative path
# cannot be hidden by an env var. Precedent: cqlite-core/tests/fixtures/issue_2225/.
#
# The fixture root is itself an "sstables root" — it directly contains the
# KEYSPACE directory — so a consumer opens it exactly the way the dataset tests
# open CQLITE_DATASETS_ROOT/sstables, and queries `test_udt_collision.udt_collide`.
# ============================================================================
#
# ============================================================================
# MANDATORY: committing the .db binaries
#
# The *.db binary files produced by this script are gitignored (`*.db` in
# .gitignore) and will NOT be included by a bare `git add`. They MUST be
# force-added with `git add -f`. Force-adding tiny parity/reference binaries is
# mandated doctrine (CLAUDE.md, "Gitignored reference binaries"). The script
# prints the exact commands at exit.
# ============================================================================
#
# Usage:
#   bash test-data/scripts/generate-issue-3504-udt-collision.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~2 GB RAM for the container.
#
# Backs: issue #3504.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_3504}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue3504-udtcollision"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_udt_collision"
TABLE="udt_collide"
SCHEMA_FILE="$ROOT/schemas/issue-3504-udt-collision.cql"

# Every INSERT carries an explicit writetime so the committed sstabledump golden
# is reproducible across regenerations rather than carrying a wall clock. NOT
# fully deterministic: a non-frozen map INSERT also emits a collection tombstone
# whose local_delete_time comes from nowInSeconds, which no CQL clause pins. The
# three rows are separate partitions, so one shared timestamp implies no LWW.
T_FIXED=1000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[i3504] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[i3504] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i3504][ERROR] $*" >&2; exit 1; }

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
    echo "[i3504] (dry-run) no container engine found; using placeholder 'docker'"
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
  [[ -f "$schema_file" ]] || fail "Schema file not found: $schema_file"
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
# Rows. Every value is DISTINCT and self-describing so an overwrite is visible
# in the output rather than merely absent: the colliding `"_type"` field carries
# 'user-supplied-type', which is not a type name anything would inject, and
# `"_keyspace"` carries 'user-supplied-keyspace'.
#
# All INSERTs use an explicit USING TIMESTAMP 1000 (see T_FIXED above).
#
#   id 1 — the collision subject, everything populated:
#           c   = collide with BOTH colliding fields + real_field (the rendered
#                 UDT value — site 3)
#           p   = plain (NO colliding field) — the contrast value
#           cm  = map<frozen<collide>, int>, NON-FROZEN. MEASURED: CQLite decodes
#                 a multicell map's cell-path key as `Value::Blob`, never
#                 `Value::Udt` (`parse_cell_path_key` in
#                 row_decoder/complex_column.rs matches a closed set of PRIMITIVE
#                 cell-path types and blob-falls-back for a frozen UDT), so this
#                 column does NOT reach the Python hashable projection. Kept
#                 because it is the shape a user would naturally write, and it
#                 documents the gap.
#           tm  = the same, one type over.
#           fcm = frozen<map<frozen<collide>, int>> — SITE 4's ACTUAL SUBJECT. A
#                 frozen map is a single value cell decoded by
#                 `parse_map_with_types`, which resolves the key type through the
#                 UdtRegistry, so the key really is a `Value::Udt`.
#           ftm = frozen<map<frozen<collide_twin>, int>> — SAME field values as
#                 fcm's key under a DIFFERENT type name, so a projection that has
#                 dropped type identity collapses the two keys and one that keeps
#                 it does not.
#           fs  = frozen<set<frozen<collide>>> — the set path into the same
#                 projection (`set_to_py` shares `value_to_hashable_key`).
#   id 2 — contrast row: `p` only. Every other column is absent, so a consumer sees
#           a row where a UDT is NULL and the plain UDT has no `_type` field at
#           all (reading `_type` out of the field namespace must fail here).
#   id 3 — null-field row: `c` with a NULL `"_type"` field but a populated
#           `"_keyspace"` field, pinning that the absent-field encoding of a
#           frozen UDT is orthogonal to the collision.
# ----------------------------------------------------------------------------
insert_rows() {
  log "=== $TABLE: inserting rows (USING TIMESTAMP $T_FIXED) ==="
  cql "INSERT INTO $TABLE (id, c, p, cm, tm, fcm, ftm, fs) VALUES (
         1,
         {\"_type\": 'user-supplied-type', \"_keyspace\": 'user-supplied-keyspace', real_field: 42},
         {label: 'no-colliding-field', real_field: 7},
         {{\"_type\": 'key-type-marker', \"_keyspace\": 'key-keyspace-marker', real_field: 100}: 1},
         {{\"_type\": 'key-type-marker', \"_keyspace\": 'key-keyspace-marker', real_field: 100}: 2},
         {{\"_type\": 'key-type-marker', \"_keyspace\": 'key-keyspace-marker', real_field: 100}: 3},
         {{\"_type\": 'key-type-marker', \"_keyspace\": 'key-keyspace-marker', real_field: 100}: 4},
         {{\"_type\": 'set-member-type', \"_keyspace\": 'set-member-keyspace', real_field: 200}}
       ) USING TIMESTAMP $T_FIXED"
  cql "INSERT INTO $TABLE (id, p) VALUES (
         2,
         {label: 'contrast-row', real_field: 8}
       ) USING TIMESTAMP $T_FIXED"
  cql "INSERT INTO $TABLE (id, c) VALUES (
         3,
         {\"_type\": null, \"_keyspace\": 'keyspace-field-only', real_field: 0}
       ) USING TIMESTAMP $T_FIXED"
}

generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  log "Generating sstabledump JSONL golden files..."
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
        fail "sstabledump JSONL golden is EMPTY: $jsonl_file"
      fi
      local lines
      lines=$(wc -l < "$jsonl_file" | tr -d ' ')
      log "  OK: $jsonl_file ($lines partitions)"
    fi
  done < <(find "$sstables_dir/$KEYSPACE" -type f -name "*-Data.db" -not -name "._*" -print0 \
            2>/dev/null || true)
}

# ----------------------------------------------------------------------------
# OUT_DIR safety (same guards as generate-compaction-parity-udt.sh: the export
# step rm -rf's the keyspace subtree).
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

log "Starting $KEYSPACE generation (issue #3504)"
log "Output directory: $OUT_DIR"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue3504 \
  "$CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$SCHEMA_FILE"
insert_rows
flush_ks

log "=== Exporting $KEYSPACE SSTables from container ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE into $OUT_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$OUT_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.i3504_export_tmp"
  rm -rf "$TMPDIR_EXPORT"
  mkdir -p "$TMPDIR_EXPORT"

  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$TMPDIR_EXPORT" -xf -; then
    if [[ -d "$TMPDIR_EXPORT/data/$KEYSPACE" ]]; then
      rm -rf "$OUT_DIR/$KEYSPACE"
      mkdir -p "$OUT_DIR/$KEYSPACE"
      cp -r "$TMPDIR_EXPORT/data/$KEYSPACE/." "$OUT_DIR/$KEYSPACE/"
      log "$KEYSPACE SSTables placed in $OUT_DIR/$KEYSPACE"
    else
      fail "Expected $TMPDIR_EXPORT/data/$KEYSPACE but it was not found. Export failed."
    fi
    rm -rf "$TMPDIR_EXPORT"
  else
    fail "tar export from container failed."
  fi

  # ONE flush of ONE table => exactly one Data.db. More than one means the
  # inserts did not land in a single memtable flush and the fixture is not the
  # single-SSTable subject the tests assume.
  tdirs=( "$OUT_DIR/$KEYSPACE/$TABLE"* )
  if [[ ! -d "${tdirs[0]}" ]]; then
    fail "$TABLE: no table directory matched under $OUT_DIR/$KEYSPACE/ \
(glob '$OUT_DIR/$KEYSPACE/$TABLE*' did not expand); export failed"
  fi
  cnt=$(find "${tdirs[@]}" -name "*-Data.db" -not -name "._*" 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$cnt" -ne 1 ]]; then
    fail "$TABLE: expected exactly ONE Data.db, found $cnt."
  fi
  log "  $TABLE: exactly one Data.db (OK)"

  generate_sstabledump_jsonl "$OUT_DIR"

  log "Generating Statistics.db.txt..."
  while IFS= read -r -d '' data_file; do
    rel="${data_file#"$OUT_DIR"/}"
    stats_base="${data_file%Data.db}Statistics.db.txt"
    log "  sstablemetadata: $rel"
    $ENGINE run --rm \
      -v "$OUT_DIR:/data" \
      "$CASSANDRA_IMAGE" \
      bash -lc "/opt/cassandra/tools/bin/sstablemetadata /data/${rel}" \
      > "$stats_base" 2>/dev/null || true
    if [[ -s "$stats_base" ]]; then
      log "  OK: $stats_base"
    else
      log "  WARNING: Empty statistics for $rel"
    fi
  done < <(find "$OUT_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" -print0)

  find "$OUT_DIR/$KEYSPACE" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

  # The colliding field name must actually be present in the sstabledump golden;
  # otherwise the fixture does not carry the subject of issue #3504.
  golden=$(find "$OUT_DIR/$KEYSPACE" -name "*-Data.db.jsonl" | head -1)
  if ! grep -q '"_type"' "$golden"; then
    fail "sstabledump golden $golden does not mention a \"_type\" column/field; \
the colliding UDT field did not survive into the fixture."
  fi
  log "  golden mentions the colliding \"_type\" field (OK)"

  log "=== $KEYSPACE generation COMPLETE ==="
  log "Fixture root (an sstables root): $OUT_DIR"

  echo ""
  echo "=============================================================="
  echo "  NEXT: commit the generated fixture"
  echo "=============================================================="
  echo ""
  # THE PRINTED PATHS ARE ABSOLUTE, AND BOTH HALVES OF THAT MATTER.
  #
  # (1) The GLOBS are expanded by the pasting user's SHELL, not by git, so a
  #     `$REPO_ROOT`-relative glob silently matches nothing unless their cwd
  #     happens to be the repo root — which `git -C` otherwise makes
  #     unnecessary. Absolute globs work from any cwd.
  # (2) The SIDECAR NAMES: Cassandra names these `<generation>-<format>-TOC.txt`
  #     and `<...>-Digest.crc32` (e.g. `nb-1-big-TOC.txt`), so a bare `TOC.txt` /
  #     `Digest.crc32` pathspec matches NOTHING — and `git add` aborts on an
  #     unmatched pathspec, staging NONE of the other sidecars on the same
  #     command line. The previous text printed exactly that: pasting the block
  #     force-added the `.db` binaries (a separate, working command) and then
  #     staged no JSONL golden, no `Statistics.db.txt`, no schema and no script.
  #     Measured before the fix: 6 binaries staged, sidecar command exit 128, 0
  #     sidecars. Leading-`*` globs, so an unprefixed name would still match.
  echo "  # Force-add the .db binaries (gitignored — MUST use -f):"
  echo "  git -C '$REPO_ROOT' add -f \\"
  echo "    '$OUT_DIR'/$KEYSPACE/*/*.db"
  echo ""
  echo "  # Add the sidecars normally (not gitignored):"
  echo "  git -C '$REPO_ROOT' add \\"
  echo "    '$OUT_DIR'/$KEYSPACE/*/*.jsonl \\"
  echo "    '$OUT_DIR'/$KEYSPACE/*/*TOC.txt \\"
  echo "    '$OUT_DIR'/$KEYSPACE/*/*.crc32 \\"
  echo "    '$OUT_DIR'/$KEYSPACE/*/*.db.txt \\"
  echo "    '$REPO_ROOT'/test-data/schemas/issue-3504-udt-collision.cql \\"
  echo "    '$REPO_ROOT'/test-data/scripts/generate-issue-3504-udt-collision.sh"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'fixtures(#3504): Cassandra-written colliding-UDT-field fixture'"
  echo "=============================================================="
fi
