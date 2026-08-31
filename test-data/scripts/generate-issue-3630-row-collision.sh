#!/usr/bin/env bash
# generate-issue-3630-row-collision.sh — Cassandra 5.0.2 ROW-LEVEL
# Object.prototype COLLISION fixture (issue #3630).
#
# WHAT THIS PRODUCES AND WHY
# The Node binding builds a result row by writing each column name onto a plain
# JavaScript object with an ordinary property assignment. That is a JavaScript
# `[[Set]]`, which CONSULTS THE PROTOTYPE CHAIN, so a column name matching an
# inherited accessor reaches that accessor's SETTER instead of creating an own
# property — and the column is silently lost with no error anywhere. #3504 fixed
# this mechanism for the UDT FIELD bag and deliberately scoped the ROW and
# JSON-object paths out; #3630 is that scoped-out half.
#
# NO corpus fixture declares a row COLUMN with such a name, so the row-level
# defect had no test subject. This script generates one.
#
# THE FOUR COLLISION COLUMNS EXERCISE THREE DIFFERENT MECHANISMS. This is the
# fixture's whole point and the schema states it at length; in brief:
#   * "__proto__"   — the ONLY inherited ACCESSOR on Object.prototype. A string
#                     value is silently DISCARDED; a NULL value REPLACES the
#                     object's prototype. Two failure modes, both the defect.
#   * "constructor" — inherited WRITABLE DATA properties, not accessors: a
#   * "toString"      `[[Set]]` of these already works. They are the AC7
#                     discriminator (a literal-`__proto__` special case passes
#                     every `__proto__` case and fails these) AND the regression
#                     control for the new write mechanism.
#   * "prototype"   — NOT on Object.prototype at all (it lives on functions).
#                     Exercises NO interception; present because the ruling is to
#                     enumerate the class, not to special-case one name. It must
#                     never be described as a second accessor case.
#
# CASSANDRA-WRITTEN, NOT CQLITE-WRITTEN. A Cassandra-written fixture
# additionally proves the DECODER can carry such a column name at all — a
# separate claim from the binding's rendering, and the one a CQLite-written
# subject cannot make (a CQLite-written + CQLite-read subject is invariant to a
# uniform error on both sides; CLAUDE.md, #3042).
#
# Schema: test-data/schemas/issue-3630-row-collision.cql (committed alongside;
# read it for the per-column rationale). Rows are described at insert_rows().
#
# ============================================================================
# OUTPUT LOCATION IS LOAD-BEARING
#
# The fixture is written CHECKOUT-RELATIVE to
#   test-data/fixtures/issue_3630/<keyspace>/<table>-<uuid>/
# and NOT under test-data/datasets/sstables/. Both binding suites
# (bindings/python/tests/conftest.py, bindings/node/__test__/setup.js) resolve
# the corpus as an EITHER/OR on CQLITE_DATASETS_ROOT: unset, they fall back to
# the checkout's test-data/datasets; but when it IS set — which every gate run
# does — the checkout copy is never consulted, so a corpus-rooted fixture is
# INVISIBLE exactly where these suites run. A checkout-relative path cannot be
# hidden by an env var. Precedent: test-data/fixtures/issue_3504/.
#
# The fixture root is itself an "sstables root" — it directly contains the
# KEYSPACE directory — so a consumer opens it exactly the way the dataset tests
# open CQLITE_DATASETS_ROOT/sstables, and queries
# `test_row_collision.row_collide`.
# ============================================================================
#
# ============================================================================
# MANDATORY: committing the .db binaries
#
# The *.db files this produces are gitignored (`*.db`) and will NOT be picked up
# by a bare `git add`. They MUST be force-added with `git add -f` — mandated
# doctrine (CLAUDE.md, "Gitignored reference binaries"). The exact commands are
# printed at exit. Verify them from a fresh `git worktree add --detach HEAD`,
# never from the dirty tree that produced them.
# ============================================================================
#
# Usage:
#   bash test-data/scripts/generate-issue-3630-row-collision.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~2 GB RAM for the container.
#
# NOTE ON A FOREIGN CONTAINER: another lane's `cassandra:5.0.2` container may be
# running on the same box. Every container operation here targets the exact
# CONTAINER_NAME below — there is no `ancestor=`/image-wide filter anywhere in
# this script, the pre-flight FAILS CLOSED if that name already exists rather
# than reclaiming it, and `nodetool flush` is keyspace-scoped. So a concurrent
# foreign Cassandra cannot be touched by this run.
#
# Backs: issue #3630.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_3630}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue3630-rowcollision"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_row_collision"
TABLE="row_collide"

SCHEMA_FILE="$ROOT/schemas/issue-3630-row-collision.cql"

# Every INSERT pins this timestamp, which stabilises liveness_info.tstamp in the
# committed golden. It does NOT make the golden fully reproducible: row 2's
# explicit CQL NULL writes a CELL TOMBSTONE whose local_delete_time is a wall
# clock no CQL clause can pin (MEASURED — see
# test-data/fixtures/issue_3630/README.md). Do not byte-compare the golden
# across regenerations.
T_FIXED=1000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[i3630] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[i3630] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i3630][ERROR] $*" >&2; exit 1; }

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
    echo "[i3630] (dry-run) no container engine found; using placeholder 'docker'"
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
# Rows. Every value is DISTINCT and SELF-DESCRIBING so a lost or displaced cell
# is visible in the output rather than merely absent — the same reason #3504's
# rows carry 'user-supplied-*' values rather than 'a'/'b'/'c'.
#
# All INSERTs use an explicit USING TIMESTAMP 1000 (T_FIXED above).
#
#   id 1 — THE STRING CASE. All four collision columns populated. Before the fix
#          the `"__proto__"` cell is the one that VANISHES: absent from
#          Object.keys, not an own property, and reading the name back yields
#          Object.prototype. The other three arrive normally even unfixed, which
#          is exactly why they are here (see the header).
#
#   id 2 — THE NULL CASE, and the harsher half. `"__proto__"` is written as an
#          EXPLICIT NULL; the other three are populated. Assigning null to
#          `__proto__` is the one case the inherited accessor HONOURS, so before
#          the fix this row's object has its PROTOTYPE REPLACED with null — the
#          row silently stops being a normal object. This is what distinguishes
#          "the column vanished" from "the object was mutated".
#
#          MEASUREMENT NOTE, and it decides how this case can be asserted: an
#          explicit CQL NULL writes a CELL TOMBSTONE, so whether CQLite surfaces
#          it as a `Value::Null` PRESENT in the row's value map or as an ABSENT
#          cell is a property of the decoder that this script does NOT assume.
#          It matters because `row_to_object` SKIPS a metadata column with no
#          matching value — so if the cell arrives absent, no assignment happens,
#          no prototype replacement occurs, and the unfixed code shows no defect
#          on this row. In that case the null-valued oracle must be a Rust-level
#          unit test over `row_to_object` with a value map explicitly containing
#          `Value::Null`, and the Node case is characterized as "absent cell,
#          column skipped" instead. The test that consumes this fixture MUST
#          record which of the two it observed rather than asserting the
#          expectation blind.
#
#   id 3 — THE CONTRAST CASE. `real_col` only; no collision column set. Pins
#          that the fix is a property of the CONSTRUCTION and not of the data:
#          this row must be shaped identically before and after.
# ----------------------------------------------------------------------------
insert_rows() {
  log "=== $TABLE: inserting rows (USING TIMESTAMP $T_FIXED) ==="

  cql "INSERT INTO $TABLE (id, \"__proto__\", \"constructor\", \"toString\", \"prototype\", real_col)
       VALUES (1, 'user-supplied-proto', 'user-supplied-constructor',
               'user-supplied-tostring', 'user-supplied-prototype', 42)
       USING TIMESTAMP $T_FIXED"

  cql "INSERT INTO $TABLE (id, \"__proto__\", \"constructor\", \"toString\", \"prototype\", real_col)
       VALUES (2, null, 'user-supplied-constructor-2',
               'user-supplied-tostring-2', 'user-supplied-prototype-2', 43)
       USING TIMESTAMP $T_FIXED"

  cql "INSERT INTO $TABLE (id, real_col)
       VALUES (3, 44)
       USING TIMESTAMP $T_FIXED"
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
# OUT_DIR safety: the export step rm -rf's the keyspace subtree.
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

log "Starting $KEYSPACE generation (issue #3630)"
log "Output directory: $OUT_DIR"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue3630 \
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

  TMPDIR_EXPORT="$OUT_DIR/.i3630_export_tmp"
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

  # ONE flush => exactly one Data.db. More than one means the inserts did not
  # land in a single memtable flush and the fixture is not the single-SSTable
  # subject the tests assume.
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

  # Every collision column name must actually appear in the golden, or the
  # fixture does not carry the subject of issue #3630. Checked per NAME rather
  # than once, because a schema edit that drops one column would otherwise leave
  # the fixture silently weaker while the assert still passed on the others.
  golden=$(find "$OUT_DIR/$KEYSPACE/$TABLE"* -name "*-Data.db.jsonl" | head -1)
  [[ -n "$golden" ]] || fail "no $TABLE golden found under $OUT_DIR/$KEYSPACE/$TABLE*"
  for collide_col in '__proto__' 'constructor' 'toString' 'prototype'; do
    if ! grep -q -- "$collide_col" "$golden"; then
      fail "sstabledump golden $golden does not mention the '$collide_col' \
column; that colliding column did not survive into the fixture."
    fi
    log "  golden mentions the colliding '$collide_col' column (OK)"
  done

  log "=== $KEYSPACE generation COMPLETE ==="
  log "Fixture root (an sstables root): $OUT_DIR"

  echo ""
  echo "=============================================================="
  echo "  NEXT: commit the generated fixture"
  echo "=============================================================="
  echo ""
  # THE PRINTED PATHS ARE ABSOLUTE, AND BOTH HALVES OF THAT MATTER (#3504's
  # measured trap, kept verbatim because it cost a broken commit there):
  # (1) the GLOBS are expanded by the pasting user's SHELL, not by git, so a
  #     $REPO_ROOT-relative glob silently matches nothing unless their cwd
  #     happens to be the repo root;
  # (2) the SIDECAR NAMES are `<generation>-<format>-TOC.txt` /
  #     `<...>-Digest.crc32` (e.g. `nb-1-big-TOC.txt`), so a bare `TOC.txt` /
  #     `Digest.crc32` pathspec matches NOTHING — and `git add` aborts on an
  #     unmatched pathspec, staging NONE of the other sidecars on the same
  #     command line. Hence the leading-`*` globs.
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
  echo "    '$REPO_ROOT'/test-data/schemas/issue-3630-row-collision.cql \\"
  echo "    '$REPO_ROOT'/test-data/scripts/generate-issue-3630-row-collision.sh"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'fixtures(#3630): Cassandra-written row-column collision fixture'"
  echo "=============================================================="
fi
