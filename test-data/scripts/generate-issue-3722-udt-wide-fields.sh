#!/usr/bin/env bash
# generate-issue-3722-udt-wide-fields.sh — Cassandra 5.0.2 UDT FIELD-TYPE
# COVERAGE fixture (issue #3722).
#
# WHAT THIS PRODUCES AND WHY
# UDT *field* values of many CQL types decode as an opaque `Value::Blob`. Two
# SEPARATE shared UDT-field decoders live in
# cqlite-core/src/storage/sstable/reader/parsing/row_decoder/udt.rs —
# `parse_udt_field_value` and `parse_simple_udt_field_value` — with DIVERGENT arm
# sets, and BOTH end in `_ => Value::Blob`, so a field type neither names
# silently becomes bytes.
#
# The defect is UNREACHABLE FROM THE CORPUS today. A census of every `CREATE
# TYPE` in test-data/schemas/** (7 files: `key_part`, `collide`, `collide_twin`,
# `plain`, `unhashable_fields`, `person`/`address`/`employee`,
# `person_type`/`address_type`/`employee_type`) found NOT ONE declaring
# smallint / tinyint / decimal / varint / time / timeuuid / duration, and only
# `unhashable_fields` declares a collection field — the same position #3500 and
# #3504 were in before their fixtures existed. This script generates the missing
# subject.
#
# CASSANDRA-WRITTEN, NOT CQLITE-WRITTEN. Mandated by CLAUDE.md: for an ON-DISK
# decode property the oracle must be Cassandra-written bytes (or Cassandra
# source), never CQLite's own output. A CQLite-write/CQLite-read round trip is
# INVARIANT to a uniform decode error — both sides make the identical mistake and
# the round trip closes green while real Cassandra data reads wrong. CQLite's
# write path could not supply it anyway: nothing in cqlite-core/src/cql/ ever
# constructs a `CqlLiteral::Udt`, so an INSERT cannot produce a UDT value.
#
# TWO FIELDS ARE DELIBERATE CONTROLS:
#   * `bl blob` — the ONE field that MUST STILL decode to `Value::Blob`. A fix
#     that blanket-stops emitting Blob is caught by this field.
#   * `i int`   — already decodes correctly today, so a regression in the
#     working path is caught by this field.
#
# EVERY SCALAR VALUE IS SELF-DESCRIBING AND SIGN-BEARING, so a wrong decode is
# VISIBLE rather than merely absent: `s: -300` = 0xfed4 (a wrong-width read
# cannot produce -300), `t: -1` = 0xff, `d: 123.45` = scale 2 / unscaled 12345,
# `vi` EXCEEDS i64, `du` has all three components non-zero. PRESERVE that
# property in any row added later.
#
# MEASURED REFUSAL — DO NOT WORK AROUND IT: `counter` is refused as a UDT field
# by Cassandra 5.0.2, verbatim —
#   InvalidRequest: code=2200 [Invalid query] message="A user type cannot contain counters"
# Issue #3722's AC1 names `counter`, so that AC is unsatisfiable by construction
# and no fixture can carry it; the `counter` arm is pinned at the `CqlType` level
# elsewhere instead.
#
# Schema: test-data/schemas/issue-3722-udt-wide-fields.cql (committed alongside;
# read it for the full per-field/per-column rationale). Rows are described at
# insert_rows().
#
# ============================================================================
# OUTPUT LOCATION IS LOAD-BEARING
#
# The fixture is written CHECKOUT-RELATIVE to
#   test-data/fixtures/issue_3722/<keyspace>/<table>-<uuid>/
# and NOT under test-data/datasets/sstables/. Reason: consumers resolve the
# corpus from CQLITE_DATASETS_ROOT, which every gate run sets, so a corpus-rooted
# fixture is INVISIBLE on every gate run. A checkout-relative path cannot be
# hidden by an env var. Precedent: test-data/fixtures/issue_3504/.
#
# The fixture root is itself an "sstables root" — it directly contains the
# KEYSPACE directory — so a consumer opens it exactly the way the dataset tests
# open CQLITE_DATASETS_ROOT/sstables, and queries
# `test_udt_wide_fields.udt_wide_fields`.
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
#   bash test-data/scripts/generate-issue-3722-udt-wide-fields.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~2 GB RAM for the container.
#
# Backs: issue #3722.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_3722}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue3722-widefields"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_udt_wide_fields"
TABLE="udt_wide_fields"
SCHEMA_FILE="$ROOT/schemas/issue-3722-udt-wide-fields.cql"

# Every INSERT carries an explicit writetime so the committed sstabledump golden
# is reproducible across regenerations rather than carrying a wall clock. NOT
# fully deterministic: a MULTICELL collection INSERT (`mw`, `sw`) also emits a
# collection tombstone whose local_delete_time comes from nowInSeconds, which no
# CQL clause pins. The three rows are separate partitions, so one shared
# timestamp implies no LWW.
T_FIXED=1000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)     OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[i3722] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[i3722] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i3722][ERROR] $*" >&2; exit 1; }

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
    echo "[i3722] (dry-run) no container engine found; using placeholder 'docker'"
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
# The two `wide` literals. VALUE CHOICE IS THE POINT: every scalar is
# self-describing and sign-bearing so a wrong decode is VISIBLE rather than
# merely absent (see the file header). Both literals were verified to INSERT
# against cassandra:5.0.2 in every key position, not merely to parse.
#
# WIDE_A — the primary value, used for row 1's `w` and for row 1's container
#          columns.
# WIDE_B — a SECOND, DISTINCT value (different scalar values, all still
#          sign-bearing) used by row 3's container columns so multi-element
#          ordering and uniqueness are exercised by Cassandra's OWN writer.
# ----------------------------------------------------------------------------
WIDE_A="{s: -300, t: -1, d: 123.45, vi: 90071992547409910000, tm: '13:30:54.234000000',
 tu: 8ac6d580-6d4d-11ee-b962-0242ac120002, du: 2mo3d4h5m, dt: '2026-03-14',
 ip: '192.168.1.42', fl: [1,-2,3], fs: {'a','b'}, fm: {'k1': 10, 'k2': -20},
 tp: (7, 'seven'), nu: {a: 5, b: 'nested'}, bl: 0xdeadbeef, i: 7}"

WIDE_B="{s: 32767, t: 127, d: -0.001, vi: -170141183460469231731687303715884105728,
 tm: '00:00:00.000000001', tu: 8ac6d581-6d4d-11ee-b962-0242ac120002,
 du: -1y2mo3d4h5m6s7ms8us9ns, dt: '1970-01-02', ip: '2001:db8::dead:beef',
 fl: [-2147483648, 2147483647], fs: {'zzz'}, fm: {'neg': -1},
 tp: (-1, 'minus'), nu: {a: -5, b: 'nested-b'}, bl: 0x00ff, i: -7}"

# ----------------------------------------------------------------------------
# Rows. Separate rows are used wherever a decode failure in one column could
# MASK another.
#
# All INSERTs use an explicit USING TIMESTAMP 1000 (see T_FIXED above).
#
#   id 1 — EVERY column populated with WIDE_A: `w` (top-level frozen UDT),
#          `mw` (multicell map key => cell path), `fmw` (frozen map key =>
#          single value cell), `fsw` (frozen set element), `sw` (multicell set
#          element => cell path). This is the row that drives all five of
#          #3722's AC3 container routes with the full 16-field value.
#
#   id 2 — NULL UDT FIELDS. `w` only, with SIX fields NULL spanning both
#          categories the ACs distinguish: three scalars (`t` tinyint,
#          `vi` varint, `du` duration) and two collections (`fs` set,
#          `fm` map) plus the nested UDT (`nu`). The remaining fields are
#          POPULATED, so the row proves the absent-field encoding is ORTHOGONAL
#          to field-type decoding: a decoder that mis-handles the null bitmap
#          would shift every later field and the populated ones would read
#          wrong. Both CONTROL fields (`bl`, `i`) are populated so the controls
#          are still asserted in the presence of nulls.
#
#   id 3 — a SECOND, DISTINCT `wide` value (WIDE_B) in mw/fmw/fsw/sw, so
#          multi-element ordering and uniqueness are exercised by Cassandra's
#          own writer: each container holds BOTH WIDE_A and WIDE_B and Cassandra
#          sorts them by its own serialized-UDT comparator. `w` is left NULL
#          here so a failure decoding the top-level UDT cannot mask the
#          container routes.
#          INSERTED OUT OF SORTED ORDER: this statement runs BEFORE id 2's, and
#          within it WIDE_B is written BEFORE WIDE_A in every collection
#          literal, so the committed bytes reflect Cassandra's ordering rather
#          than the insertion order.
# ----------------------------------------------------------------------------
insert_rows() {
  log "=== $TABLE: inserting rows (USING TIMESTAMP $T_FIXED) ==="

  cql "INSERT INTO $TABLE (id, w, mw, fmw, fsw, sw) VALUES (
         1,
         $WIDE_A,
         {$WIDE_A: 1},
         {$WIDE_A: 2},
         {$WIDE_A},
         {$WIDE_A}
       ) USING TIMESTAMP $T_FIXED"

  # id 3 BEFORE id 2 — out of sorted order, deliberately (see above).
  cql "INSERT INTO $TABLE (id, mw, fmw, fsw, sw) VALUES (
         3,
         {$WIDE_B: 30, $WIDE_A: 31},
         {$WIDE_B: 32, $WIDE_A: 33},
         {$WIDE_B, $WIDE_A},
         {$WIDE_B, $WIDE_A}
       ) USING TIMESTAMP $T_FIXED"

  cql "INSERT INTO $TABLE (id, w) VALUES (
         2,
         {s: -300, t: null, d: 123.45, vi: null, tm: '13:30:54.234000000',
          tu: 8ac6d580-6d4d-11ee-b962-0242ac120002, du: null, dt: '2026-03-14',
          ip: '192.168.1.42', fl: [1,-2,3], fs: null, fm: null,
          tp: (7, 'seven'), nu: null, bl: 0xdeadbeef, i: 7}
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
# OUT_DIR safety (same guards as generate-issue-3504-udt-collision.sh: the
# export step rm -rf's the keyspace subtree).
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

log "Starting $KEYSPACE generation (issue #3722)"
log "Output directory: $OUT_DIR"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue3722 \
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

  TMPDIR_EXPORT="$OUT_DIR/.i3722_export_tmp"
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

  # No CompressionInfo.db: the table is declared with compression disabled, and a
  # compressed fixture would put the read under test behind a decompressor.
  if find "${tdirs[@]}" -name "*-CompressionInfo.db" -not -name "._*" | grep -q .; then
    fail "$TABLE: a CompressionInfo.db was written; the fixture must be UNCOMPRESSED."
  fi
  log "  $TABLE: no CompressionInfo.db (uncompressed, OK)"

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

  # The fixture must actually CARRY the subject of issue #3722: the wide UDT's
  # marshal spelling must name every affected field type. Asserted against the
  # generated Statistics.db.txt (the SerializationHeader's UserType(...) form),
  # because that is the exact string #3722's AC2 requires a test to drive the
  # decoder through. A missing type here means a field was silently dropped or
  # the schema drifted.
  stats_txt=$(find "${tdirs[@]}" -name "*-Statistics.db.txt" | head -1)
  [[ -s "$stats_txt" ]] || fail "no non-empty Statistics.db.txt under ${tdirs[0]}"
  for marshal in ShortType ByteType DecimalType IntegerType TimeType TimeUUIDType \
                 DurationType SimpleDateType InetAddressType ListType SetType MapType \
                 TupleType UserType BytesType Int32Type; do
    if ! grep -q -- "$marshal" "$stats_txt"; then
      fail "Statistics.db.txt $stats_txt does not mention $marshal; \
a UDT field type did not survive into the fixture."
    fi
  done
  log "  Statistics.db.txt names all 16 field marshal types (OK)"

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
  #     $REPO_ROOT-relative glob silently matches nothing unless their cwd
  #     happens to be the repo root — which `git -C` otherwise makes
  #     unnecessary. Absolute globs work from any cwd.
  # (2) The SIDECAR NAMES: Cassandra names these `<generation>-<format>-TOC.txt`
  #     and `<...>-Digest.crc32` (e.g. `nb-1-big-TOC.txt`), so a bare `TOC.txt` /
  #     `Digest.crc32` pathspec matches NOTHING — and `git add` aborts on an
  #     unmatched pathspec, staging NONE of the other sidecars on the same
  #     command line. Leading-`*` globs, so an unprefixed name would still match.
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
  echo "    '$REPO_ROOT'/test-data/schemas/issue-3722-udt-wide-fields.cql \\"
  echo "    '$REPO_ROOT'/test-data/scripts/generate-issue-3722-udt-wide-fields.sh"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'fixtures(#3722): Cassandra-written wide-UDT-field-type fixture'"
  echo "=============================================================="
fi
