#!/usr/bin/env bash
# generate-issue-4106-empty-fixedwidth-set-member.sh — Cassandra 5.0.2 EMPTY
# FIXED-WIDTH MULTICELL SET MEMBER fixture (issue #4106).
#
# THE SET HALF of generate-issue-3805-empty-fixedwidth-map-key.sh, from which
# this script is derived structurally (container ownership, OUT_DIR
# canonicalisation, single-Data.db assertion, golden self-verification, the
# printed `git add -f`). Read that script and
# test-data/schemas/issue-4106-empty-fixedwidth-set-member.cql for the WHY; this
# header records only what is DIFFERENT.
#
# WHAT THIS PRODUCES
# A non-frozen `set<T>` is multicell and its MEMBER travels in the cell PATH —
# `cql3/Sets.java:407` at cassandra-5.0.8 writes the element as
# `CellPath.create(bb)` with `ByteBufferUtil.EMPTY_BYTE_BUFFER` as the cell
# value. So a ZERO-LENGTH cell path is the EMPTY MEMBER, exactly as it is the
# empty KEY for a map, and Cassandra validates both with the one
# `validateCellPath` line. CQLite's reader instead carried a
# `!path_bytes.is_empty()` guard on the set branch and DROPPED such a member: a
# `SELECT` returned the set short one member with no error and no log line.
#
# NO fixture in this repository carried an empty fixed-width SET member before
# this one. #3805's map fixture cannot stand in: a map short one ENTRY and a set
# short one MEMBER are different code paths in `complex_column.rs`.
#
# A CQLite-written + CQLite-read round trip could not settle this (CLAUDE.md,
# #3042) — and #4106 adds a WRITE path too, so a symmetric test would be
# invariant to a uniform framing error made by both sides.
#
# THE `blobAsX(0x)` ACCEPTANCE PROBE IS MEASURED PER RUN, NOT INHERITED.
# #3805 measured which families cqlsh accepts as a MAP KEY. This script probes
# the same terms as SET MEMBERS and prints the verdict per family, then FAILS
# CLOSED if a family this fixture's schema depends on turns out to be refused.
# An inherited measurement is an assumption; a printed one is evidence.
#
# Usage:
#   bash test-data/scripts/generate-issue-4106-empty-fixedwidth-set-member.sh [--out <dir>] [--dry-run]
#
# ============================================================================
# WHY THIS SCRIPT REJECTS EVERY UNRECOGNIZED ARGUMENT, AND WHY THERE IS A
# GUARD BLOCK BEFORE THE DOCKER WORK: **IT RUNS `rm -rf` ON A KEYSPACE SUBTREE.**
#
# The export step replaces `$OUT_DIR/$KEYSPACE` wholesale, so the destructive
# statement is literally `rm -rf "$OUT_DIR/$KEYSPACE"`. A reader who finds that
# line must be able to find the reasoning without inferring it, so it is stated
# here:
#
#  * the argument loop's `*)` arm exits NON-ZERO on anything it does not
#    recognise, rather than ignoring it — a mistyped `--out` would otherwise
#    leave OUT_DIR at its default and delete a subtree the caller never named.
#    Same posture, and the same reason, as `test-data/scripts/fetch-datasets.sh`
#    (see its header at `:28-33`, whose default path is also a destructive
#    `rm -rf` on the dataset root);
#  * the GUARD BLOCK below refuses an empty/unset `OUT_DIR` or `KEYSPACE` BY
#    NAME, canonicalizes `OUT_DIR` with `realpath -m` BEFORE validating it,
#    asserts it is absolute, refuses `/`, bare `/tmp` and anything shorter than
#    4 characters, and refuses any target that is not under the repo root or
#    `/tmp/`. It runs BEFORE the container starts, so a bad invocation fails in
#    a second rather than after a Cassandra boot.
#
# The canonicalize-then-validate ORDERING is load-bearing, not stylistic: the
# prefix tests are LEXICAL string comparisons, so a path containing `..`
# segments or a symlinked component can SATISFY `$OUT_DIR == $REPO_ROOT/*` while
# resolving somewhere else entirely — and what follows is an `rm -rf`. Resolving
# first closes that class instead of blacklisting `..`.
# ============================================================================
#
# Prerequisites: Docker (or podman) in PATH; ~2 GB RAM for the container.
#
# NOTE ON A FOREIGN CONTAINER: other lanes on this box run cassandra:5.0.2
# containers. Every container operation here targets the exact CONTAINER_NAME
# below — no `ancestor=`/image-wide filter anywhere — the pre-flight FAILS
# CLOSED if that name exists rather than reclaiming it, cleanup removes the
# container ONLY if this invocation created it, and `nodetool flush` is
# keyspace-scoped.
#
# Backs: issue #4106.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_4106}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue4106-emptyfixedwidthset"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_empty_fixedwidth_set"
TABLE="empty_fixedwidth_set_member"
PROBE_TABLE="probe_blobas_set"

SCHEMA_FILE="$ROOT/schemas/issue-4106-empty-fixedwidth-set-member.cql"

# Pins `liveness_info.tstamp` and the collection tombstones' `marked_deleted`.
# It does NOT make the golden byte-reproducible — see the schema header.
T_FIXED=1000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    # EVERY unrecognized argument is rejected NON-ZERO, never ignored: this
    # script's default path runs `rm -rf "$OUT_DIR/$KEYSPACE"`, so a mistyped
    # `--out` must not silently fall back to the default target. Same posture
    # and same reason as `test-data/scripts/fetch-datasets.sh`.
    *) echo "[i4106] Unknown argument: $1" >&2; exit 2 ;;
  esac
done

log()  { echo "[i4106] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i4106][ERROR] $*" >&2; exit 1; }

# COERCE relative -> absolute FIRST (canonicalization and validation both come
# later, in the guard block, and the ordering there is load-bearing).
if [[ -n "${OUT_DIR:-}" && "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

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
    echo "[i4106] (dry-run) no container engine found; using placeholder 'docker'"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME"
fi

# Cleanup removes the container ONLY if THIS invocation created it: a name
# pre-flight is not a lock, so an unconditional `rm -f` in the EXIT trap of a
# loser could delete a concurrent winner's live container. Inherited verbatim
# from the #3805/#3747 generators.
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
# THE ACCEPTANCE PROBE — MEASURED, per run.
#
# For each family, ask cqlsh to evaluate `blobAsX(0x)` AS A SET ELEMENT in a
# throwaway table, and record ACCEPTED / REFUSED. "cqlsh refuses exactly
# tinyint/smallint/date/time" is #3805's MAP-KEY measurement; this re-measures
# it for SET MEMBERS rather than inheriting it, and FAILS CLOSED if any family
# this fixture's schema DEPENDS ON is refused (which would mean the fixture
# cannot carry its own subject).
#
# The throwaway table lives in the same keyspace and is dropped afterwards; the
# probe runs BEFORE insert_rows so a refusal is reported before any fixture
# bytes exist.
# ----------------------------------------------------------------------------
# Families the FIXTURE depends on: a REFUSAL here is fatal.
PROBE_REQUIRED=(int bigint uuid boolean inet varint decimal)
# Families PREDICTED refused by the bare `size != N` validate. A refusal is the
# expected outcome and is RECORDED, not fatal; an ACCEPTANCE would mean the
# schema's NOT-COVERED bound is wrong, so it is reported loudly.
PROBE_STRICT=(tinyint smallint date time)
# Extra families probed for the record only.
PROBE_EXTRA=(float double timestamp timeuuid)

# `blobAsX` is spelled with the CQL type name capitalised exactly as cqlsh's
# function table has it. Derived from `BytesConversionFcts.java`, which builds
# one function per `CQL3Type.Native` as "blobAs" + name.toLowerCase() — cqlsh
# resolves function names case-INSENSITIVELY, so `${fam^}` (capitalise the
# first letter) is sufficient and no per-family table is needed.
probe_blobas() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would probe blobAsX(0x) as a set element"
    return 0
  fi

  log "=== MEASURING: is blobAsX(0x) accepted as a SET ELEMENT? ==="
  local accepted=() refused=()
  local fam
  for fam in "${PROBE_REQUIRED[@]}" "${PROBE_STRICT[@]}" "${PROBE_EXTRA[@]}"; do
    if ! $ENGINE exec "$CONTAINER_NAME" cqlsh -k "$KEYSPACE" \
        -e "DROP TABLE IF EXISTS ${PROBE_TABLE}; CREATE TABLE ${PROBE_TABLE} (id int PRIMARY KEY, s set<${fam}>);" \
        >/dev/null 2>&1; then
      log "  $fam: SET ELEMENT TYPE NOT ALLOWED (table creation refused)"
      refused+=("${fam}(type)")
      continue
    fi
    if $ENGINE exec "$CONTAINER_NAME" cqlsh -k "$KEYSPACE" \
         -e "INSERT INTO ${PROBE_TABLE} (id, s) VALUES (1, {blobAs${fam^}(0x)});" >/dev/null 2>&1; then
      log "  $fam: blobAs${fam^}(0x) ACCEPTED as a set element"
      accepted+=("$fam")
    else
      log "  $fam: blobAs${fam^}(0x) REFUSED as a set element"
      refused+=("$fam")
    fi
  done
  $ENGINE exec "$CONTAINER_NAME" cqlsh -k "$KEYSPACE" \
    -e "DROP TABLE IF EXISTS ${PROBE_TABLE};" >/dev/null 2>&1 || true

  log "MEASURED ACCEPTED: ${accepted[*]:-<none>}"
  log "MEASURED REFUSED:  ${refused[*]:-<none>}"

  # FAIL CLOSED on a required family: the fixture's schema declares a column for
  # each, so a refusal means the fixture cannot carry its own subject.
  local missing=()
  for fam in "${PROBE_REQUIRED[@]}"; do
    [[ " ${accepted[*]} " == *" $fam "* ]] || missing+=("$fam")
  done
  if [[ "${#missing[@]}" -gt 0 ]]; then
    fail "blobAsX(0x) was REFUSED as a set element for a family this fixture DEPENDS ON: \
${missing[*]}. The fixture cannot carry its own subject; re-derive the schema \
rather than committing bytes that do not hold it."
  fi

  # A STRICT family turning out ACCEPTED is a real finding — the schema's
  # NOT-COVERED bound would be wrong and the fixture should widen. Loud, not
  # fatal (widening is a schema decision, not this script's).
  for fam in "${PROBE_STRICT[@]}"; do
    if [[ " ${accepted[*]} " == *" $fam "* ]]; then
      log "  *** FINDING: '$fam' was ACCEPTED, but the schema's NOT-COVERED section \
predicts a REFUSAL (bare 'size != N' validate). Re-derive that bound and \
consider adding a column."
    fi
  done
}

# ----------------------------------------------------------------------------
# Rows. See the schema for the per-column rationale.
#
#   id 1 — THE SUBJECT: an empty member in every set column, each beside a
#          NON-EMPTY sibling so a failure is legible (set SHORT ONE MEMBER, not
#          absent).
#   id 2 — THE CONTRAST: no empty member anywhere.
#
# THE EMPTY MEMBER SORTS FIRST and that is asserted in the golden verification
# below, not merely observed: `Int32Type.compareCustom:61-71` gives the empty
# buffer a UNIQUE sort position strictly before every non-empty value, and
# Cassandra writes a multicell collection's cells in comparator order.
# ----------------------------------------------------------------------------
insert_rows() {
  log "=== $TABLE: inserting rows (USING TIMESTAMP $T_FIXED) ==="

  # id 1 — the subject: an empty member in every set column, each beside a sibling.
  cql "INSERT INTO $TABLE (id, s_int, s_bigint, s_uuid, s_bool, s_inet, s_varint, s_dec, s_text, s_frozen)
       VALUES (1,
               {blobAsInt(0x), 42},
               {blobAsBigint(0x), 99},
               {blobAsUuid(0x), 123e4567-e89b-12d3-a456-426614174000},
               {blobAsBoolean(0x), true},
               {blobAsInet(0x), '10.0.0.1'},
               {blobAsVarint(0x), 7},
               {blobAsDecimal(0x), 1.5},
               {'', 'k'},
               {blobAsInt(0x), 7})
       USING TIMESTAMP $T_FIXED"

  # id 2 — the contrast: no empty member anywhere.
  cql "INSERT INTO $TABLE (id, s_int, s_bigint, s_uuid, s_bool, s_inet, s_varint, s_dec, s_text, s_frozen)
       VALUES (2,
               {5},
               {6},
               {223e4567-e89b-12d3-a456-426614174111},
               {false},
               {'10.0.0.2'},
               {8},
               {2.5},
               {'w'},
               {9})
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

# ============================================================================
# THE GUARD BLOCK. It exists because the export step below runs
#
#     rm -rf "$OUT_DIR/$KEYSPACE"
#
# and it runs HERE — before the container starts — so a bad invocation fails in
# a second rather than after a Cassandra boot. Structure inherited from
# generate-issue-3805-empty-fixedwidth-map-key.sh:302-330 (originally roborev
# job 287 on generate-issue-3630-row-collision.sh), with the three additions the
# lead asked for on #4106 marked below.
#
# ORDERING IS LOAD-BEARING: canonicalize BEFORE validating. The prefix tests are
# LEXICAL string comparisons, so a path containing `..` segments or a symlinked
# component can SATISFY `$OUT_DIR == $REPO_ROOT/*` while resolving somewhere
# else entirely — and what follows is an `rm -rf`. Resolving first closes that
# class instead of blacklisting `..`.
# ============================================================================

# ADDITION 1 — EXPLICIT NON-EMPTY REFUSAL FOR BOTH VARIABLES THE `rm -rf` PATH
# IS BUILT FROM, each naming the offending variable.
#
# The precedent only length-checks OUT_DIR and treats KEYSPACE as a literal
# constant. Checking KEYSPACE anyway is the point: it IS a constant today, so
# this costs nothing, and a later edit that makes it derived (from an argument,
# a schema parse, a `basename`) would otherwise silently turn the destructive
# statement into `rm -rf "$OUT_DIR/"` — the parent, not the subtree. Under
# `set -u` an UNSET variable already aborts; an EMPTY one does not, which is
# exactly the case that reads as success and deletes the wrong tree.
if [[ -z "${OUT_DIR:-}" ]]; then
  fail "OUT_DIR is EMPTY or unset. Refusing: the export step runs \
'rm -rf \$OUT_DIR/\$KEYSPACE', and an empty OUT_DIR makes that a deletion of a \
parent directory."
fi
if [[ -z "${KEYSPACE:-}" ]]; then
  fail "KEYSPACE is EMPTY or unset. Refusing: the export step runs \
'rm -rf \$OUT_DIR/\$KEYSPACE', and an empty KEYSPACE makes that a deletion of \
OUT_DIR itself rather than of the keyspace subtree."
fi

if ! command -v realpath >/dev/null 2>&1; then
  fail "realpath(1) is required to canonicalize OUT_DIR before destructive operations."
fi
OUT_DIR="$(realpath -m "$OUT_DIR")"
OUT_DIR="${OUT_DIR%/}"

# ADDITION 2 — ASSERT absoluteness, do not merely COERCE it. The coercion above
# runs before this canonicalization (that ordering is the precedent's); this is
# the fail-closed VERIFICATION that it took, so a future edit that reorders or
# drops the coercion cannot leave a relative path reaching the `rm -rf`.
if [[ "$OUT_DIR" != /* ]]; then
  fail "OUT_DIR '$OUT_DIR' is NOT ABSOLUTE after coercion and canonicalization. \
Refusing: a relative target would resolve against whatever cwd the caller \
happened to have when the 'rm -rf' runs."
fi

# `realpath -m` collapses `foo/..` and can therefore SHORTEN the path; the
# length and prefix tests below are applied to the CANONICAL form for that
# reason.
if [[ "${#OUT_DIR}" -lt 4 ]]; then
  fail "OUT_DIR '$OUT_DIR' is suspiciously short (< 4 chars). Refusing."
fi
case "$OUT_DIR" in
  /) fail "Refusing to operate on '/'." ;;
  /tmp) fail "Refusing to use '/tmp' directly. Use a subdirectory." ;;
esac

# ADDITION 3 — the target must be under the REPO ROOT or under an
# sstables/dataset root. `test-data/fixtures/issue_4106` (the default) is under
# the repo root; `/tmp/...` is the scratch route used by `--out` for a trial
# run; `CQLITE_DATASETS_ROOT`, when exported, is the machine-local sstables root
# a caller may legitimately target. Anything else is refused BY NAME.
_allowed_root=""
[[ "$OUT_DIR" == "$REPO_ROOT/"* ]] && _allowed_root="the repo root ($REPO_ROOT)"
[[ -z "$_allowed_root" && "$OUT_DIR" == /tmp/* ]] && _allowed_root="/tmp/"
if [[ -z "$_allowed_root" && -n "${CQLITE_DATASETS_ROOT:-}" ]]; then
  _ds_root="$(realpath -m "$CQLITE_DATASETS_ROOT")"
  _ds_root="${_ds_root%/}"
  # Length-guarded so an empty/degenerate CQLITE_DATASETS_ROOT cannot widen the
  # allowlist to everything.
  if [[ "${#_ds_root}" -ge 4 && "$OUT_DIR" == "$_ds_root/"* ]]; then
    _allowed_root="the dataset root ($_ds_root)"
  fi
fi
if [[ -z "$_allowed_root" ]]; then
  fail "OUT_DIR '$OUT_DIR' is not under the repo root ($REPO_ROOT), /tmp/, or \
\$CQLITE_DATASETS_ROOT. Refusing: the export step runs 'rm -rf \
\$OUT_DIR/$KEYSPACE'."
fi
log "OUT_DIR guard: '$OUT_DIR' accepted (under $_allowed_root)"

log "Starting $KEYSPACE generation (issue #4106)"
log "Output directory: $OUT_DIR"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
if ! run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue4106 \
  "$CASSANDRA_IMAGE"; then
  # No ownership flag set, so the EXIT trap will NOT remove a container a
  # concurrent invocation may legitimately own.
  fail "container '$CONTAINER_NAME' could not be started (a concurrent invocation \
may already own that name); refusing to remove it."
fi
# An explicit `if`, NOT `[[ ... ]] && CONTAINER_CREATED=1`: that one-liner
# returns non-zero when the test is false and survives `set -e` only via the
# &&-list exemption — not a subtlety a reader of a script that runs `rm -rf`
# should have to re-derive.
if [[ "$DRY_RUN" -eq 0 ]]; then
  CONTAINER_CREATED=1
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  wait_cassandra
fi

apply_schema "$SCHEMA_FILE"
probe_blobas
insert_rows
flush_ks

log "=== Exporting $KEYSPACE SSTables from container ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] would tar-stream /var/lib/cassandra/data/$KEYSPACE into $OUT_DIR/$KEYSPACE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$OUT_DIR"

  TMPDIR_EXPORT="$OUT_DIR/.i4106_export_tmp"
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

  # The probe table is dropped in-container, but its (empty, unflushed)
  # directory can still be exported. Remove it so the fixture tree holds ONLY
  # the subject table.
  rm -rf "$OUT_DIR/$KEYSPACE/${PROBE_TABLE}"*

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

  # ==========================================================================
  # THE FIXTURE MUST CARRY ITS OWN SUBJECT, AND THIS IS WHERE THAT IS PROVED.
  #
  # A generation can succeed in every mechanical respect and still not contain
  # an empty fixed-width set member, if cqlsh coerced a `blobAsX(0x)` term or a
  # schema edit dropped a column. The fixture would then be a green subject for
  # a test that can no longer fail, which is worse than no fixture. So the EMPTY
  # MEMBER is asserted POSITIVELY in the golden, PER COLUMN.
  #
  # sstabledump renders a multicell set cell's member in the cell's "path"
  # field. An EMPTY member renders as an empty string, so this is a python json
  # walk, not a `grep` for `""` — which would match any empty string anywhere in
  # the document and pass for the wrong reason.
  # ==========================================================================
  golden=$(find "$OUT_DIR/$KEYSPACE/$TABLE"* -name "*-Data.db.jsonl" | head -1)
  [[ -n "$golden" ]] || fail "no $TABLE golden found under $OUT_DIR/$KEYSPACE/$TABLE*"

  log "Verifying the golden actually carries an empty FIXED-WIDTH set member, per column..."
  python3 - "$golden" <<'PY' || fail "golden verification FAILED (see above)"
import json, sys

golden = sys.argv[1]

# Columns that MUST show an empty member in the golden.
#
# s_frozen is excluded ON PURPOSE and structurally: a frozen set is ONE inline
# cell whose whole value is the serialized set, so it has no per-element cell
# "path" — its empty member lives INSIDE the cell value blob, not in the dump's
# cell paths. Asserting it here would assert a shape sstabledump does not
# expose. It stays in the fixture as a DIFFERENT-CODE-PATH control.
#
# s_text is included although it is the already-working variable-width case: it
# keeps "this fixture's fixed-width families decode" distinguishable from "empty
# members decode at all".
REQUIRED = ["s_int", "s_bigint", "s_uuid", "s_bool", "s_inet", "s_varint",
            "s_dec", "s_text"]

found = {c: False for c in REQUIRED}
# Cell paths observed per column, IN ORDER, so the ordering claim below is
# checked against what the file says rather than assumed.
order = {c: [] for c in REQUIRED}
# Every multicell set cell's VALUE, so the "member is the PATH" framing claim is
# checked too rather than only "a path exists".
non_empty_values = []
rows_seen = 0

with open(golden) as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        part = json.loads(line)
        for row in part.get("rows", []):
            rows_seen += 1
            for cell in row.get("cells", []):
                nm = cell.get("name")
                if nm not in found:
                    continue
                path = cell.get("path")
                if path is None:
                    continue          # the collection tombstone, not a cell
                order[nm].append(path)
                if path == [""]:
                    found[nm] = True
                value = cell.get("value")
                if value not in (None, ""):
                    non_empty_values.append((nm, path, value))

if rows_seen == 0:
    print("[i4106][ERROR] golden has ZERO rows: %s" % golden, file=sys.stderr)
    sys.exit(1)

missing = [c for c, ok in found.items() if not ok]
if missing:
    print(
        "[i4106][ERROR] golden %s has NO empty-member cell for column(s): %s\n"
        "               The fixture does not carry the subject of issue #4106."
        % (golden, ", ".join(missing)),
        file=sys.stderr,
    )
    for c in REQUIRED:
        print("               observed %s: %r" % (c, order[c]), file=sys.stderr)
    sys.exit(1)

# THE EMPTY MEMBER MUST SORT FIRST — asserted, not merely observed.
#
# `Int32Type.compareCustom:61-71` (cassandra-5.0.8) gives the empty buffer a
# UNIQUE sort position, strictly before every non-empty value, and Cassandra
# writes a multicell collection's cells in comparator order. This is one of the
# three independent grounds on which #3805's oracle concludes an empty
# component is DISTINCT from a null one rather than merely present, so it is
# worth failing on: if it stops holding, that argument needs re-deriving — do
# not just delete the check.
#
# Only checked where the column actually has a non-empty sibling in this row.
bad_order = []
for c in REQUIRED:
    paths = order[c]
    if len(paths) >= 2 and [""] in paths and paths[0] != [""]:
        bad_order.append((c, paths))
if bad_order:
    print(
        "[i4106][ERROR] the EMPTY member did not sort FIRST in: %s"
        % ", ".join(c for c, _ in bad_order),
        file=sys.stderr,
    )
    for c, paths in bad_order:
        print("               observed %s: %r" % (c, paths), file=sys.stderr)
    sys.exit(1)

# A live SET cell carries HAS_EMPTY_VALUE: the member IS the path and the cell
# VALUE is the empty buffer (`cql3/Sets.java:407` passes
# `ByteBufferUtil.EMPTY_BYTE_BUFFER`). Asserted so the fixture is evidence for
# that framing and not merely for "a path exists".
if non_empty_values:
    print(
        "[i4106][ERROR] a multicell SET cell carried a NON-EMPTY value; the member must\n"
        "               live in the PATH with an empty value (cql3/Sets.java:407): %r"
        % (non_empty_values[:5],),
        file=sys.stderr,
    )
    sys.exit(1)

for c in REQUIRED:
    print("[i4106]   golden carries an EMPTY member for column '%s' (OK)" % c)
print("[i4106]   empty member sorts FIRST wherever a non-empty sibling exists (OK)")
print("[i4106]   every multicell set cell has an EMPTY value (member is the PATH) (OK)")
print("[i4106]   golden rows inspected: %d" % rows_seen)
PY

  log "=== $KEYSPACE generation COMPLETE ==="
  log "Fixture root (an sstables root): $OUT_DIR"

  echo ""
  echo "=============================================================="
  echo "  NEXT: commit the generated fixture"
  echo "=============================================================="
  echo ""
  # ABSOLUTE paths, and both halves matter (#3504's measured trap): the GLOBS
  # are expanded by the pasting user's SHELL not by git, so a $REPO_ROOT-relative
  # glob silently matches nothing unless their cwd is the repo root; and the
  # SIDECAR NAMES are `<generation>-<format>-TOC.txt` / `-Digest.crc32`, so a
  # bare `TOC.txt` pathspec matches NOTHING and `git add` then aborts, staging
  # none of the others on the same command line. Hence the leading-`*` globs.
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
  echo "    '$REPO_ROOT'/test-data/schemas/issue-4106-empty-fixedwidth-set-member.cql \\"
  echo "    '$REPO_ROOT'/test-data/scripts/generate-issue-4106-empty-fixedwidth-set-member.sh"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'fixtures(#4106): Cassandra-written empty fixed-width set-member fixture'"
  echo "=============================================================="
fi
