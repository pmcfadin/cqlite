#!/usr/bin/env bash
# generate-issue-4114-vector-float.sh — Cassandra 5.0-written `vector<float, n>`
# fixture (issue #4114, epic #3479 "Cassandra 5 read-completeness audit").
#
# WHAT THIS PRODUCES AND WHY
# `vector<float, n>` is the Cassandra 5 flagship type. CQLite has no decoder for
# it, no CQL type variant, and — before this run — NO FIXTURE ANYWHERE in the
# corpus (measured: the single pre-existing `vector` hit under test-data/schemas
# is `legacy/hardened_validator_test_schema.cql:146`, a `LIST<DOUBLE>` column,
# not a vector). Nothing exercised the type, which is exactly why its failure
# mode was unknown.
#
# AC1 of #4114 is a MEASUREMENT, not a decoder: does a `vector<float,n>` column
# fail CLOSED with a named error, or does it silently mis-decode into
# plausible-looking wrong numbers? The first is a missing feature; the second is
# a correctness defect. This script produces the bytes that settle it.
#
# THE ORACLE IS CASSANDRA-WRITTEN BYTES, NOT A CQLITE ROUND-TRIP. A
# CQLite-written + CQLite-read subject is invariant to a uniform framing error on
# both sides — both halves make the identical mistake, the round-trip closes, the
# test stays green, and real Cassandra data still reads wrong (CLAUDE.md, #3042).
# That trap is acute here because there is no pre-existing oracle to disagree
# with, so a synthesized fixture could not settle the encoding at all.
#
# Schema: test-data/schemas/issue-4114-vector-float.cql (committed alongside).
# READ IT — it carries the on-disk encoding derived from pinned Cassandra source
# (`vector<float,n>` = exactly `4*n` contiguous big-endian binary32 elements, NO
# per-element framing, FIXED-length overall), the per-column rationale, and why
# the `a_before`/`z_after` sentinels are load-bearing rather than decorative.
#
# ============================================================================
# OUTPUT LOCATION IS LOAD-BEARING
#
# The fixture is written CHECKOUT-RELATIVE to
#   test-data/fixtures/issue_4114/<keyspace>/<table>-<uuid>/
# and NOT under test-data/datasets/sstables/. Consumers resolve the corpus as an
# EITHER/OR on CQLITE_DATASETS_ROOT: when it IS set — which every gate run does,
# to a machine-local root like /data/datasets — the checkout copy is never
# consulted, so a corpus-rooted fixture is INVISIBLE exactly where the tests run.
# A checkout-relative path cannot be hidden by an env var. Precedent:
# test-data/fixtures/issue_3504/, issue_3630/, issue_3722/, issue_3747/.
#
# The fixture root is itself an "sstables root" — it directly contains the
# KEYSPACE directory — so a consumer opens it exactly the way the dataset tests
# open CQLITE_DATASETS_ROOT/sstables. Per-TABLE resolution is still mandatory in
# the Rust consumer (`sstables_root_for_table`, never a keyspace-level root):
# a root holding one of these two tables but not the other must not let the
# missing case skip silently behind its sibling (#3220, AC2 of #4114).
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
# ============================================================================
# REGENERATION REPLACES EVERY TABLE DIRECTORY, INCLUDING ALREADY-COMMITTED ONES
#
# The export step `rm -rf`s $OUT_DIR/$KEYSPACE and Cassandra mints a FRESH table
# UUID per run, so a plain re-run of this script does not "add" a table — it
# replaces all of them with new directories and new bytes. That matters because
# committed analysis cites byte OFFSETS inside the already-committed
# vector_clustered/vector_pk_only Data.db files
# (.drive-issue-4114/format-authority.md), and those citations would silently
# stop describing the committed bytes.
#
# So when the goal is to ADD a table to an existing committed fixture, generate
# to a scratch directory and copy only the NEW table directory across:
#
#   bash test-data/scripts/generate-issue-4114-vector-float.sh --out /tmp/i4114-run
#   cp -r /tmp/i4114-run/test_vector/vector_last-* \
#         test-data/fixtures/issue_4114/test_vector/
#
# All three tables are still generated and verified in the scratch run; only the
# new directory is promoted. A full regeneration (pointing --out straight at the
# fixture root) is legitimate too, but then re-derive any byte-offset analysis.
# ============================================================================
#
# Usage:
#   bash test-data/scripts/generate-issue-4114-vector-float.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; python3; ~2 GB RAM for the container.
#
# NOTE ON A FOREIGN CONTAINER: another lane's Cassandra container may be running
# on the same box. Every container operation here targets the exact
# CONTAINER_NAME below — there is no `ancestor=`/image-wide filter anywhere in
# this script, the pre-flight FAILS CLOSED if that name already exists rather
# than reclaiming it, and `nodetool flush` is keyspace-scoped. So a concurrent
# foreign Cassandra cannot be touched by this run.
#
# Backs: issue #4114.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4114}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue4114-vectorfloat"

# THE IMAGE TAG MATCHES THE FORMAT-AUTHORITY PIN.
# CLAUDE.md pins Cassandra format authority at the `cassandra-5.0.8` tag, so the
# fixture is written by 5.0.8 and the bytes and the source agree by construction.
# Override only with a deliberate reason, and record which tag actually wrote a
# committed fixture — a fixture generated by a different minor is still valid
# bytes, but the "source and bytes are the same release" property is lost.
CASSANDRA_IMAGE="${CASSANDRA_IMAGE:-cassandra:5.0.8}"

KEYSPACE="test_vector"
TABLE_PK="vector_pk_only"
TABLE_CK="vector_clustered"
TABLE_LAST="vector_last"
TABLE_EXACT="vector_exact"

# vector_last's first elements are chosen for their LEADING BIG-ENDIAN BYTE, not
# for their magnitude — see the schema's vector_last block and build_inserts()
# below. 2^-111 encodes as exactly 0x08000000 (exponent field 127-111 == 16).
V_LAST_SMALL_EXP="-111"

# vector_exact defeats the row-body ACCOUNTING guard instead of the bounds
# check: the misreader consumes `1 + len`, so a 12-byte vector balances exactly
# when len == 11, i.e. leading byte 0x0b. 2^-105 encodes as exactly 0x0b000000.
V_EXACT_SMALL_EXP="-105"

SCHEMA_FILE="$ROOT/schemas/issue-4114-vector-float.cql"

# Every INSERT pins this timestamp, which stabilises `liveness_info.tstamp` in
# the committed golden.
#
# IT DOES NOT MAKE THE GOLDEN BYTE-REPRODUCIBLE. Row 2 writes explicit NULLs for
# both vector columns, and a CQL NULL emits a tombstone carrying a
# `local_delete_time` that is a WALL CLOCK no CQL clause can pin. Consequences,
# both load-bearing: do NOT byte-compare this golden across regenerations, and
# the test that consumes it MUST NOT assert on `local_delete_time`. Same caveat
# as test-data/fixtures/issue_3630 and issue_3747.
T_FIXED=1000

# The 384-dim pattern: element i = i * 0.5, so v384[0]=0.0, [1]=0.5 ... [383]=191.5.
#
# WHY THIS PATTERN. Every value is EXACTLY representable in binary32 (a multiple
# of 0.5 well inside the mantissa), so there is no float rounding anywhere and an
# expectation can be written as an exact equality rather than an epsilon compare.
# It is also strictly increasing and position-derived, so a mis-decode is legible
# BY INSPECTION rather than only by diff: a byte-offset slip shows up as a value
# that does not equal index/2, an endianness flip yields absurd magnitudes, and a
# LIST<FLOAT>-style framing misread (reading a 4-byte length prefix where there
# is none) desynchronises immediately and visibly. A constant or random fill
# would hide all three.
V384_STEP="0.5"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[i4114] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[i4114] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4114][ERROR] $*" >&2; exit 1; }

run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

command -v python3 >/dev/null 2>&1 || fail "python3 is required (builds the 384-element literal and verifies the golden)."

if command -v docker >/dev/null 2>&1; then
  ENGINE="docker"
elif command -v podman >/dev/null 2>&1; then
  ENGINE="podman"
else
  if [[ "$DRY_RUN" -eq 1 ]]; then
    ENGINE="docker"
    echo "[i4114] (dry-run) no container engine found; using placeholder 'docker'"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"
log "Cassandra image: $CASSANDRA_IMAGE"

if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME"
fi

# CLEANUP REMOVES THE CONTAINER ONLY IF *THIS* INVOCATION CREATED IT.
#
# The pre-flight above refuses when the fixed CONTAINER_NAME already exists, but
# a name check is not a lock: two concurrent invocations can BOTH pass it, and
# then the loser's `docker run` fails on the duplicate name — at which point an
# unconditional `rm -f "$CONTAINER_NAME"` in its EXIT trap would delete the
# WINNER's container out from under a live generation. The victim sees its
# Cassandra vanish mid-run, which reads as an infrastructure flake rather than as
# another process killing it. So ownership is tracked explicitly: the flag is set
# ONLY after `docker run` returns 0, and cleanup is a no-op otherwise.
CONTAINER_CREATED=0
cleanup() {
  if [[ "$DRY_RUN" -eq 0 && "$CONTAINER_CREATED" -eq 1 ]]; then
    log "Cleaning up container (created by this invocation)..."
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

# Inserts are driven from a FILE rather than `cqlsh -e`, because the 384-element
# vector literal is ~2.5 KB and a `-e` argument that long is awkward to quote
# safely through two shells.
apply_cql_file() {
  local f="$1" label="$2"
  log "Applying CQL: $label"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $ENGINE cp $f $CONTAINER_NAME:/tmp/$label && cqlsh -k $KEYSPACE -f /tmp/$label"
    return 0
  fi
  $ENGINE cp "$f" "$CONTAINER_NAME:/tmp/$label"
  $ENGINE exec "$CONTAINER_NAME" cqlsh -k "$KEYSPACE" -f "/tmp/$label"
}

flush_ks() {
  log "Flushing $KEYSPACE..."
  run $ENGINE exec "$CONTAINER_NAME" nodetool flush "$KEYSPACE"
}

# ----------------------------------------------------------------------------
# Rows. Every value is distinct and position-derived so a lost or displaced
# element is visible in the output rather than merely absent.
#
# vector_pk_only:
#   id 1 — THE SUBJECT. Both vector columns populated, both sentinels populated.
#          v1=[1.5]; v384[i]=i*0.5. This is the row every positive decode
#          expectation is written against.
#   id 2 — THE NULL CASE (AC3). Both vector columns explicitly NULL, both
#          sentinels populated. Cassandra forbids an EMPTY vector outright
#          ("we don't allow empty vectors" — VectorType.java:412-417), so
#          null/unset is the ONLY absent-value shape this type has, and it must
#          stay distinguishable from a zero-filled vector.
#   id 3 — THE CONTRAST. Distinct non-zero values including a NEGATIVE element
#          (v1=[-2.25]) and a v384 offset by +1000, so a decoder that returns a
#          cached or constant-folded answer for every row is caught, and so the
#          sign bit is exercised at all.
#
# vector_clustered:
#   (pk 1, ck 10) and (pk 1, ck 20) — two rows in ONE partition, so the fixture
#          covers a vector column reached through a clustering-prefixed row body
#          and not only through a static/pk-only row. Values are exactly
#          representable and per-row distinct.
# ----------------------------------------------------------------------------
build_inserts() {
  local out="$1"
  python3 - "$out" "$T_FIXED" "$TABLE_PK" "$TABLE_CK" "$V384_STEP" \
           "$TABLE_LAST" "$V_LAST_SMALL_EXP" \
           "$TABLE_EXACT" "$V_EXACT_SMALL_EXP" <<'PY'
import struct, sys
(out, tfixed, table_pk, table_ck, step, table_last, small_exp,
 table_exact, exact_exp) = sys.argv[1:10]
step = float(step)
n = 384

def vec(vals):
    # Render with repr-stable formatting: these are all exact binary32 values,
    # so a short decimal form round-trips without introducing a rounding step.
    return "[" + ", ".join(("%g" % v) for v in vals) + "]"

base = [i * step for i in range(n)]
alt  = [1000.0 + i * step for i in range(n)]

lines = []
lines.append("-- GENERATED by test-data/scripts/generate-issue-4114-vector-float.sh")
lines.append("-- Do not edit; regenerate instead.")
lines.append("")
lines.append(
    "INSERT INTO %s (id, a_before, v1, v384, z_after) VALUES "
    "(1, 'before-1', [1.5], %s, 'after-1') USING TIMESTAMP %s;"
    % (table_pk, vec(base), tfixed)
)
# Explicit NULLs, not an omitted column list: an omitted column is simply unset
# and writes nothing, whereas an explicit CQL NULL writes a tombstone. Both are
# "absent" to a reader, and pinning the tombstone form is the stricter subject.
lines.append(
    "INSERT INTO %s (id, a_before, v1, v384, z_after) VALUES "
    "(2, 'before-2', null, null, 'after-2') USING TIMESTAMP %s;"
    % (table_pk, tfixed)
)
lines.append(
    "INSERT INTO %s (id, a_before, v1, v384, z_after) VALUES "
    "(3, 'before-3', [-2.25], %s, 'after-3') USING TIMESTAMP %s;"
    % (table_pk, vec(alt), tfixed)
)
lines.append("")
lines.append(
    "INSERT INTO %s (pk, ck, v3, z_after) VALUES "
    "(1, 10, [1, 2.5, -3.75], 'ck-after-10') USING TIMESTAMP %s;"
    % (table_ck, tfixed)
)
lines.append(
    "INSERT INTO %s (pk, ck, v3, z_after) VALUES "
    "(1, 20, [4.5, -5, 6.25], 'ck-after-20') USING TIMESTAMP %s;"
    % (table_ck, tfixed)
)
lines.append("")

# ---- vector_last: the silent-mis-decode reachability subject ----------------
# The literals are chosen for the LEADING BYTE of the FIRST element's big-endian
# binary32 encoding, because that byte is what a vint-length-prefix misreader
# consumes as a length:
#
#   0.0        -> 00 00 00 00  -> vint 0x00 = 0 bytes  (empty value, no bounds hit)
#   2^-111     -> 08 00 00 00  -> vint 0x08 = 8 bytes  (satisfiable, wrong bytes)
#
# repr() of the float32-exact double round-trips through cqlsh's decimal parse
# without a rounding step, so the on-disk bytes are exactly 0x08000000. That is
# ASSERTED here rather than assumed: a literal whose leading byte is not the one
# this fixture is built around would make the whole subject vacuous.
small = 2.0 ** int(small_exp)
packed = struct.pack(">f", float(repr(small)))
if packed != b"\x08\x00\x00\x00":
    print("[i4114][ERROR] 2^%s must encode as 08000000, got %s"
          % (small_exp, packed.hex()), file=sys.stderr)
    sys.exit(1)
if struct.pack(">f", 0.0) != b"\x00\x00\x00\x00":
    print("[i4114][ERROR] 0.0 must encode as 00000000", file=sys.stderr)
    sys.exit(1)

lines.append(
    "INSERT INTO %s (id, v3) VALUES (1, [0.0, 1.0, 2.0]) USING TIMESTAMP %s;"
    % (table_last, tfixed)
)
lines.append(
    "INSERT INTO %s (id, v3) VALUES (2, [%s, 1.0, 2.0]) USING TIMESTAMP %s;"
    % (table_last, repr(small), tfixed)
)
lines.append("")

# ---- vector_exact: the EXACT-CONSUMPTION (fully silent) subject -------------
# 2^-105 -> 0b 00 00 00 -> bogus vint 0x0b = 11 -> the misreader consumes
# 1 + 11 == 12 == the full vector width, so the row-body accounting balances and
# nothing fails closed. Asserted, not assumed: the whole table is vacuous if the
# leading byte is not 0x0b, and vacuous in the OPPOSITE direction (it would just
# fail closed like vector_last) rather than loudly.
exact = 2.0 ** int(exact_exp)
exact_packed = struct.pack(">f", float(repr(exact)))
if exact_packed != b"\x0b\x00\x00\x00":
    print("[i4114][ERROR] 2^%s must encode as 0b000000, got %s"
          % (exact_exp, exact_packed.hex()), file=sys.stderr)
    sys.exit(1)

for pid, tail in ((1, "1.0, 2.0"), (2, "4.5, -5.0")):
    lines.append(
        "INSERT INTO %s (id, v3) VALUES (%d, [%s, %s]) USING TIMESTAMP %s;"
        % (table_exact, pid, repr(exact), tail, tfixed)
    )
lines.append("")
with open(out, "w") as fh:
    fh.write("\n".join(lines) + "\n")
print("[i4114]   built %d INSERT statements (v384 dim=%d, step=%g); "
      "vector_last v3[0] literals 0.0 -> 00000000 and %s -> %s; "
      "vector_exact v3[0] literal %s -> %s"
      % (9, n, step, repr(small), packed.hex(), repr(exact), exact_packed.hex()))
PY
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
#
# CANONICALIZE FIRST. The prefix tests below are LEXICAL string comparisons, so a
# path containing `..` segments or a symlinked component can SATISFY
# `$OUT_DIR == $REPO_ROOT/*` while resolving somewhere else entirely — and what
# follows is `rm -rf "$OUT_DIR/$KEYSPACE"`. Resolving the path BEFORE validating
# it closes the class rather than blacklisting `..`. (Inherited verbatim from
# generate-issue-3630-row-collision.sh, where it was roborev job 287.)
if ! command -v realpath >/dev/null 2>&1; then
  fail "realpath(1) is required to canonicalize OUT_DIR before destructive operations."
fi
OUT_DIR="$(realpath -m "$OUT_DIR")"
OUT_DIR="${OUT_DIR%/}"

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

log "Starting $KEYSPACE generation (issue #4114)"
log "Output directory: $OUT_DIR"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
if ! run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue4114 \
  "$CASSANDRA_IMAGE"; then
  # No ownership flag is set, so the EXIT trap will NOT remove a container that
  # a concurrent invocation may legitimately own.
  fail "container '$CONTAINER_NAME' could not be started (a concurrent invocation \
may already own that name); refusing to remove it."
fi
# Ownership established: from here the EXIT trap is allowed to remove it.
#
# An explicit `if`, NOT `[[ ... ]] && CONTAINER_CREATED=1`. That one-liner
# returns non-zero whenever the test is false, and under `set -e` it survives
# only via the &&-list exemption — a subtlety no reader of a script that runs
# `rm -rf` should have to re-derive. Spell it out.
if [[ "$DRY_RUN" -eq 0 ]]; then
  CONTAINER_CREATED=1
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$SCHEMA_FILE"

INSERT_FILE="$(mktemp -t i4114-inserts.XXXXXX.cql)"
trap 'rm -f "$INSERT_FILE"; cleanup' EXIT
build_inserts "$INSERT_FILE"
apply_cql_file "$INSERT_FILE" "i4114-inserts.cql"

flush_ks

log "=== Exporting $KEYSPACE SSTables from container ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE into $OUT_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$OUT_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.i4114_export_tmp"
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

  # ONE flush => exactly one Data.db PER TABLE. More than one means the inserts
  # did not land in a single memtable flush and the fixture is not the
  # single-SSTable subject the tests assume.
  for t in "$TABLE_PK" "$TABLE_CK" "$TABLE_LAST" "$TABLE_EXACT"; do
    tdirs=( "$OUT_DIR/$KEYSPACE/$t"* )
    if [[ ! -d "${tdirs[0]}" ]]; then
      fail "$t: no table directory matched under $OUT_DIR/$KEYSPACE/ \
(glob '$OUT_DIR/$KEYSPACE/$t*' did not expand); export failed"
    fi
    cnt=$(find "${tdirs[@]}" -name "*-Data.db" -not -name "._*" 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$cnt" -ne 1 ]]; then
      fail "$t: expected exactly ONE Data.db, found $cnt."
    fi
    log "  $t: exactly one Data.db (OK)"
  done

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

  # ==========================================================================
  # THE FIXTURE MUST CARRY ITS OWN SUBJECT, AND THIS IS WHERE THAT IS PROVED.
  #
  # A generation can succeed in every mechanical respect — container up, schema
  # applied, one Data.db per table, non-empty goldens — and still not contain a
  # readable `vector<float,n>` value, if (say) cqlsh coerced a literal, a schema
  # edit dropped a column, or the 384-element literal was truncated. The fixture
  # would then be a green subject for a test that can no longer fail, which is
  # worse than no fixture.
  #
  # So the VECTOR VALUES are asserted POSITIVELY in the golden, per table and per
  # dimension, against the values this script actually inserted — not merely
  # "some vector-looking thing is present". The 384-dim check verifies ALL 384
  # elements equal i*step, so a truncated or displaced literal cannot pass.
  #
  # The check reads sstabledump's rendering, which is format authority tier 2.
  # Note it does NOT assert the on-disk byte length here — that is the Rust
  # test's job against the Data.db itself; sstabledump has already decoded by
  # the time this sees it.
  # ==========================================================================
  log "Verifying the goldens actually carry the inserted vector values..."
  python3 - "$OUT_DIR/$KEYSPACE" "$TABLE_PK" "$TABLE_CK" "$V384_STEP" \
           "$TABLE_LAST" "$V_LAST_SMALL_EXP" \
           "$TABLE_EXACT" "$V_EXACT_SMALL_EXP" <<'PY' || fail "golden verification FAILED (see above)"
import glob, json, os, struct, sys

(ks_dir, table_pk, table_ck, step, table_last, small_exp,
 table_exact, exact_exp) = sys.argv[1:9]
step = float(step)
N = 384
V_LAST_SMALL  = float(repr(2.0 ** int(small_exp)))
V_EXACT_SMALL = float(repr(2.0 ** int(exact_exp)))

def load(table):
    hits = glob.glob(os.path.join(ks_dir, table + "-*", "*-Data.db.jsonl"))
    if len(hits) != 1:
        print("[i4114][ERROR] expected exactly 1 golden for %s, found %d"
              % (table, len(hits)), file=sys.stderr)
        sys.exit(1)
    parts = []
    with open(hits[0]) as fh:
        for line in fh:
            line = line.strip()
            if line:
                parts.append(json.loads(line))
    return hits[0], parts

def cells(part):
    """Yield (row, {name: value}) for every row in a partition."""
    for row in part.get("rows", []):
        m = {}
        for c in row.get("cells", []):
            if "name" in c:
                m[c["name"]] = c.get("value")
        yield row, m

errors = []

# ---- vector_pk_only -------------------------------------------------------
path_pk, parts_pk = load(table_pk)
seen_pk = {}
for part in parts_pk:
    key = part.get("partition", {}).get("key")
    pid = int(key[0]) if isinstance(key, list) and key else None
    for row, m in cells(part):
        seen_pk[pid] = m

for pid in (1, 2, 3):
    if pid not in seen_pk:
        errors.append("%s: partition id=%d absent from golden" % (table_pk, pid))

# id 1 and 3 must carry fully-populated vectors; id 2 must carry NONE.
if 1 in seen_pk:
    m = seen_pk[1]
    v1 = m.get("v1")
    if v1 != [1.5]:
        errors.append("%s id=1: v1 expected [1.5], golden has %r" % (table_pk, v1))
    v384 = m.get("v384")
    if not isinstance(v384, list) or len(v384) != N:
        errors.append("%s id=1: v384 expected a %d-element list, golden has %r"
                      % (table_pk, N, type(v384).__name__ if not isinstance(v384, list) else "len=%d" % len(v384)))
    else:
        bad = [(i, v384[i]) for i in range(N) if float(v384[i]) != i * step]
        if bad:
            errors.append("%s id=1: v384 has %d element(s) != i*%g; first 3: %r"
                          % (table_pk, len(bad), step, bad[:3]))

if 3 in seen_pk:
    m = seen_pk[3]
    v1 = m.get("v1")
    if v1 != [-2.25]:
        errors.append("%s id=3: v1 expected [-2.25], golden has %r" % (table_pk, v1))
    v384 = m.get("v384")
    if not isinstance(v384, list) or len(v384) != N:
        errors.append("%s id=3: v384 expected a %d-element list, golden has %r" % (table_pk, N, v384))
    else:
        bad = [(i, v384[i]) for i in range(N) if float(v384[i]) != 1000.0 + i * step]
        if bad:
            errors.append("%s id=3: v384 has %d element(s) != 1000+i*%g; first 3: %r"
                          % (table_pk, len(bad), step, bad[:3]))

# The NULL row: the golden must show NO live value for either vector column.
# A tombstoned cell is rendered with "deletion_info", never with a "value", so
# a present non-None value here would mean the NULL did not take.
if 2 in seen_pk:
    m = seen_pk[2]
    for col in ("v1", "v384"):
        if m.get(col) is not None:
            errors.append("%s id=2: %s expected NO live value (explicit NULL), golden has %r"
                          % (table_pk, col, m.get(col)))
    # The sentinels around the NULL vectors must still be live.
    for col, want in (("a_before", "before-2"), ("z_after", "after-2")):
        if m.get(col) != want:
            errors.append("%s id=2: %s expected %r, golden has %r" % (table_pk, col, want, m.get(col)))

# The sentinels must be live on the populated rows too — they are the witnesses
# the AC1 measurement depends on, so an absent one invalidates the fixture.
for pid, pre, post in ((1, "before-1", "after-1"), (3, "before-3", "after-3")):
    if pid in seen_pk:
        m = seen_pk[pid]
        if m.get("a_before") != pre:
            errors.append("%s id=%d: a_before expected %r, golden has %r" % (table_pk, pid, pre, m.get("a_before")))
        if m.get("z_after") != post:
            errors.append("%s id=%d: z_after expected %r, golden has %r" % (table_pk, pid, post, m.get("z_after")))

# ---- vector_clustered -----------------------------------------------------
path_ck, parts_ck = load(table_ck)
seen_ck = {}
for part in parts_ck:
    for row, m in cells(part):
        ckv = row.get("clustering")
        c = int(ckv[0]) if isinstance(ckv, list) and ckv else None
        seen_ck[c] = m

for c, want_v3, want_z in ((10, [1.0, 2.5, -3.75], "ck-after-10"),
                           (20, [4.5, -5.0, 6.25], "ck-after-20")):
    if c not in seen_ck:
        errors.append("%s: clustering ck=%d absent from golden" % (table_ck, c))
        continue
    m = seen_ck[c]
    got = m.get("v3")
    if not isinstance(got, list) or [float(x) for x in got] != want_v3:
        errors.append("%s ck=%d: v3 expected %r, golden has %r" % (table_ck, c, want_v3, got))
    if m.get("z_after") != want_z:
        errors.append("%s ck=%d: z_after expected %r, golden has %r" % (table_ck, c, want_z, m.get("z_after")))

# ---- vector_last ----------------------------------------------------------
# The subject here is the FIRST ELEMENT'S LEADING BYTE, so the check asserts the
# reconstructed big-endian encoding and not merely the decoded number: a value
# that compares equal but re-encodes to a different leading byte would leave the
# fixture unable to reach the mis-decode it exists to demonstrate.
path_last, parts_last = load(table_last)
seen_last = {}
for part in parts_last:
    key = part.get("partition", {}).get("key")
    pid = int(key[0]) if isinstance(key, list) and key else None
    for row, m in cells(part):
        seen_last[pid] = m

for pid, want_v3, want_lead in ((1, [0.0, 1.0, 2.0], 0x00),
                                (2, [V_LAST_SMALL, 1.0, 2.0], 0x08)):
    if pid not in seen_last:
        errors.append("%s: partition id=%d absent from golden" % (table_last, pid))
        continue
    got = seen_last[pid].get("v3")
    if not isinstance(got, list) or len(got) != 3:
        errors.append("%s id=%d: v3 expected a 3-element list, golden has %r"
                      % (table_last, pid, got))
        continue
    # COMPARE IN BINARY32 SPACE, NOT AS DOUBLES. sstabledump renders a float
    # with Java's Float.toString, i.e. the shortest decimal that round-trips
    # through a FLOAT ("3.85186E-34"), which is NOT the shortest decimal that
    # round-trips through a DOUBLE ("3.851859888774472e-34"). Read as doubles
    # those two differ, so a `==` on floats would fail on a golden that is
    # perfectly correct. Re-encoding both sides to big-endian binary32 compares
    # exactly the bytes on disk, and re-uses the encoding the leading-byte
    # assertion below already depends on.
    got_be = [struct.pack(">f", float(x)) for x in got]
    want_be = [struct.pack(">f", v) for v in want_v3]
    if got_be != want_be:
        errors.append("%s id=%d: v3 expected binary32 %s, golden has %s (%r)"
                      % (table_last, pid,
                         " ".join(b.hex() for b in want_be),
                         " ".join(b.hex() for b in got_be), got))
        continue
    lead = got_be[0][0]
    if lead != want_lead:
        errors.append("%s id=%d: v3[0]=%r must encode with leading byte 0x%02x, got 0x%02x"
                      % (table_last, pid, got[0], want_lead, lead))
    # vector_last must have NO other regular column: a column after the vector
    # would restore the desync escape hatch this table exists to remove.
    extra = sorted(k for k in seen_last[pid] if k != "v3")
    if extra:
        errors.append("%s id=%d: expected v3 as the ONLY cell, golden also has %r"
                      % (table_last, pid, extra))

# ---- vector_exact ---------------------------------------------------------
# Same binary32-space comparison and leading-byte assertion as vector_last, but
# the required leading byte is 0x0b (the EXACT-consumption value) and it is
# required on EVERY row: one fail-closed row aborts the scan and hides the rest.
path_exact, parts_exact = load(table_exact)
seen_exact = {}
for part in parts_exact:
    key = part.get("partition", {}).get("key")
    pid = int(key[0]) if isinstance(key, list) and key else None
    for row, m in cells(part):
        seen_exact[pid] = m

for pid, want_v3 in ((1, [V_EXACT_SMALL, 1.0, 2.0]),
                     (2, [V_EXACT_SMALL, 4.5, -5.0])):
    if pid not in seen_exact:
        errors.append("%s: partition id=%d absent from golden" % (table_exact, pid))
        continue
    got = seen_exact[pid].get("v3")
    if not isinstance(got, list) or len(got) != 3:
        errors.append("%s id=%d: v3 expected a 3-element list, golden has %r"
                      % (table_exact, pid, got))
        continue
    got_be = [struct.pack(">f", float(x)) for x in got]
    want_be = [struct.pack(">f", v) for v in want_v3]
    if got_be != want_be:
        errors.append("%s id=%d: v3 expected binary32 %s, golden has %s (%r)"
                      % (table_exact, pid,
                         " ".join(b.hex() for b in want_be),
                         " ".join(b.hex() for b in got_be), got))
        continue
    if got_be[0][0] != 0x0b:
        errors.append("%s id=%d: v3[0]=%r must encode with leading byte 0x0b "
                      "(the exact-consumption length), got 0x%02x"
                      % (table_exact, pid, got[0], got_be[0][0]))
    extra = sorted(k for k in seen_exact[pid] if k != "v3")
    if extra:
        errors.append("%s id=%d: expected v3 as the ONLY cell, golden also has %r"
                      % (table_exact, pid, extra))

# The two rows must differ in the bytes the MIS-DECODE hands back, otherwise the
# fixture cannot distinguish a data-derived wrong value from a constant.
if 1 in seen_exact and 2 in seen_exact:
    def misread(m):
        v = m.get("v3")
        if not isinstance(v, list) or len(v) != 3:
            return None
        pay = b"".join(struct.pack(">f", float(x)) for x in v)
        return pay[1:1 + pay[0]]
    b1, b2 = misread(seen_exact[1]), misread(seen_exact[2])
    if b1 is None or b2 is None or b1 == b2 or len(b1) != 11 or len(b2) != 11:
        errors.append("%s: the two rows must yield DISTINCT 11-byte mis-read "
                      "blobs; got %r and %r" % (table_exact, b1, b2))

if errors:
    print("[i4114][ERROR] the goldens do NOT carry the subject of issue #4114:", file=sys.stderr)
    for e in errors:
        print("               - %s" % e, file=sys.stderr)
    sys.exit(1)

print("[i4114]   %s: id=1 v1=[1.5] and all %d v384 elements == i*%g (OK)" % (table_pk, N, step))
print("[i4114]   %s: id=3 v1=[-2.25] and all %d v384 elements == 1000+i*%g (OK)" % (table_pk, N, step))
print("[i4114]   %s: id=2 both vector columns have NO live value, sentinels live (OK)" % table_pk)
print("[i4114]   %s: ck=10 and ck=20 v3 exact, sentinels live (OK)" % table_ck)
print("[i4114]   %s: id=1 v3=[0,1,2] (lead 0x00) and id=2 v3[0]=%r (lead 0x08), "
      "v3 the only cell (OK)" % (table_last, V_LAST_SMALL))
print("[i4114]   %s: id=1/id=2 v3[0]=%r (lead 0x0b, exact consumption), "
      "distinct 11-byte mis-read blobs, v3 the only cell (OK)"
      % (table_exact, V_EXACT_SMALL))
print("[i4114]   goldens: %s" % os.path.basename(path_pk))
print("[i4114]            %s" % os.path.basename(path_ck))
print("[i4114]            %s" % os.path.basename(path_last))
print("[i4114]            %s" % os.path.basename(path_exact))
PY

  log "=== $KEYSPACE generation COMPLETE ==="
  log "Fixture root (an sstables root): $OUT_DIR"
  log "Written by image: $CASSANDRA_IMAGE"

  echo ""
  echo "=============================================================="
  echo "  NEXT: commit the generated fixture"
  echo "=============================================================="
  echo ""
  # THE PRINTED PATHS ARE ABSOLUTE, AND BOTH HALVES OF THAT MATTER (#3504's
  # measured trap, inherited verbatim because it cost a broken commit there):
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
  echo "    '$REPO_ROOT'/test-data/schemas/issue-4114-vector-float.cql \\"
  echo "    '$REPO_ROOT'/test-data/scripts/generate-issue-4114-vector-float.sh"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'fixtures(#4114): Cassandra-5.0.8-written vector<float,n> fixture'"
  echo "=============================================================="
fi
