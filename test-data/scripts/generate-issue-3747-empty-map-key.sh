#!/usr/bin/env bash
# generate-issue-3747-empty-map-key.sh — Cassandra 5.0.2 EMPTY MULTICELL MAP KEY
# fixture (issue #3747).
#
# WHAT THIS PRODUCES AND WHY
# CQLite's multicell map decoder guards its key decode on a NON-EMPTY cell path,
# so a map entry whose key is the EMPTY value is silently DROPPED from the
# reconstructed `Value::Map` — a `SELECT` returns a map short one entry, with no
# error and no warning. `{'': 1}` is legal CQL for `map<text,int>`, and empty is
# a distinct value from null.
#
# NO fixture in the corpus carried an empty map key before this one — measured,
# not assumed: zero goldens under test-data/** contain an empty `"key"`, and no
# committed .cql writes one. That absence is exactly why the defect survived, and
# closing it is AC1 of the issue. The issue itself records "I could not reproduce
# end-to-end ... generating one needs a real Cassandra"; this script is that.
#
# THE ORACLE IS CASSANDRA-WRITTEN BYTES, NOT A CQLITE ROUND-TRIP. A CQLite-written
# + CQLite-read subject is invariant to a uniform error on both sides (CLAUDE.md,
# #3042), so it could never settle what the on-disk encoding of an empty key IS.
# The sstabledump golden generated here is the authority the Rust test asserts
# against — never CQLite's prior output.
#
# Schema: test-data/schemas/issue-3747-empty-map-key.cql (committed alongside;
# read it for the per-column rationale, including why `m_frozen` is a CONTROL and
# not a second instance of the defect, and why `inet` is deliberately absent).
#
# ============================================================================
# OUTPUT LOCATION IS LOAD-BEARING
#
# The fixture is written CHECKOUT-RELATIVE to
#   test-data/fixtures/issue_3747/<keyspace>/<table>-<uuid>/
# and NOT under test-data/datasets/sstables/. Consumers resolve the corpus as an
# EITHER/OR on CQLITE_DATASETS_ROOT: when it IS set — which every gate run does,
# to a machine-local root like /data/datasets — the checkout copy is never
# consulted, so a corpus-rooted fixture is INVISIBLE exactly where the tests run.
# A checkout-relative path cannot be hidden by an env var. Precedent:
# test-data/fixtures/issue_3504/, test-data/fixtures/issue_3630/.
#
# The fixture root is itself an "sstables root" — it directly contains the
# KEYSPACE directory — so a consumer opens it exactly the way the dataset tests
# open CQLITE_DATASETS_ROOT/sstables.
# ============================================================================
#
# ============================================================================
# MANDATORY: committing the .db binaries
#
# The *.db files this produces are gitignored (`*.db`, .gitignore:74) and will
# NOT be picked up by a bare `git add`. They MUST be force-added with
# `git add -f` — mandated doctrine (CLAUDE.md, "Gitignored reference binaries").
# The exact commands are printed at exit. Verify them from a fresh
# `git worktree add --detach HEAD`, never from the dirty tree that produced them.
# ============================================================================
#
# Usage:
#   bash test-data/scripts/generate-issue-3747-empty-map-key.sh [--out <dir>] [--dry-run]
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
# Backs: issue #3747.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/fixtures/issue_3747}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue3747-emptymapkey"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_empty_map_key"
TABLE="empty_key_map"

SCHEMA_FILE="$ROOT/schemas/issue-3747-empty-map-key.cql"

# Every INSERT pins this timestamp, which stabilises `liveness_info.tstamp` and
# the collection tombstones' `marked_deleted` (T_FIXED-1 microseconds) in the
# committed golden.
#
# IT DOES NOT MAKE THE GOLDEN BYTE-REPRODUCIBLE, and an earlier draft of this
# comment claimed it did — MEASURED WRONG on the first generation, and corrected
# here rather than left standing. An `INSERT` of a whole NON-FROZEN collection
# REPLACES it, so Cassandra emits a collection tombstone for each of m_text,
# m_blob and m_ascii even though this fixture writes no nulls at all. That
# tombstone carries a `local_delete_time` which is a WALL CLOCK no CQL clause can
# pin (observed: "2026-09-01T05:26:32Z"). Only the frozen column, being a single
# inline cell, escapes it.
#
# Consequences, both load-bearing: do NOT byte-compare this golden across
# regenerations, and the test that consumes it MUST NOT assert on
# `local_delete_time`. Same caveat as test-data/fixtures/issue_3630, reached by a
# different route (a collection replace, not an explicit CQL NULL).
T_FIXED=1000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[i3747] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi
OUT_DIR="${OUT_DIR%/}"

log()  { echo "[i3747] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i3747][ERROR] $*" >&2; exit 1; }

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
    echo "[i3747] (dry-run) no container engine found; using placeholder 'docker'"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME"
fi

# CLEANUP REMOVES THE CONTAINER ONLY IF *THIS* INVOCATION CREATED IT.
#
# The pre-flight above refuses when the fixed CONTAINER_NAME already exists, but a
# name check is not a lock: two concurrent invocations can BOTH pass it, and then
# the loser's `docker run` fails on the duplicate name — at which point an
# unconditional `rm -f "$CONTAINER_NAME"` in its EXIT trap would delete the
# WINNER's container out from under a live generation. The victim sees its
# Cassandra vanish mid-run, which reads as an infrastructure flake rather than as
# another process killing it.
#
# So ownership is tracked explicitly: the flag is set ONLY after `docker run`
# returns 0, and cleanup is a no-op otherwise. A container this script did not
# create is never touched — the same rule the pre-flight already follows by
# refusing instead of reclaiming.
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
# Rows. Every VALUE is distinct and self-describing so a lost or displaced entry
# is visible in the output rather than merely absent.
#
#   id 1 — THE SUBJECT. Every map column carries an EMPTY key alongside a
#          NON-EMPTY one. The non-empty sibling is what makes the failure
#          legible: pre-fix the map comes back short exactly one entry rather
#          than absent, so "entry dropped" is distinguishable from "column
#          missing" — the two have completely different causes and only one of
#          them is this issue.
#
#          Cassandra sorts a multicell map's cells by the key comparator, and
#          empty sorts FIRST for every variable-width type here, so the empty
#          key is cell 0 in each collection. That is an observation about the
#          fixture, not an assumption the test relies on — the test asserts
#          SET membership, not cell order.
#
#   id 2 — THE CONTRAST. No empty key anywhere. Pins that the fix is a property
#          of the guard and not of the data: this row must decode identically
#          before and after the fix.
# ----------------------------------------------------------------------------
insert_rows() {
  log "=== $TABLE: inserting rows (USING TIMESTAMP $T_FIXED) ==="

  # id 1 — the subject: an empty key in every map column.
  cql "INSERT INTO $TABLE (id, m_text, m_blob, m_ascii, m_frozen)
       VALUES (1,
               {'': 1, 'a': 2},
               {0x: 10, 0xab: 20},
               {'': 100, 'z': 200},
               {'': 1000, 'k': 2000})
       USING TIMESTAMP $T_FIXED"

  # id 2 — the contrast: no empty key anywhere.
  cql "INSERT INTO $TABLE (id, m_text, m_blob, m_ascii, m_frozen)
       VALUES (2,
               {'x': 3, 'y': 4},
               {0xcd: 30},
               {'w': 300},
               {'j': 3000})
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

log "Starting $KEYSPACE generation (issue #3747)"
log "Output directory: $OUT_DIR"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
if ! run $ENGINE run -d \
  --name "$CONTAINER_NAME" \
  -e MAX_HEAP_SIZE=1G \
  -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-issue3747 \
  "$CASSANDRA_IMAGE"; then
  # No ownership flag is set, so the EXIT trap will NOT remove a container that
  # a concurrent invocation may legitimately own.
  fail "container '$CONTAINER_NAME' could not be started (a concurrent invocation \
may already own that name); refusing to remove it."
fi
# Ownership established: from here the EXIT trap is allowed to remove it.
#
# An explicit `if`, NOT `[[ ... ]] && CONTAINER_CREATED=1`. That one-liner returns
# non-zero whenever the test is false, and under `set -e` it survives only via the
# &&-list exemption — a subtlety no reader of a script that runs `rm -rf` should
# have to re-derive. Spell it out.
if [[ "$DRY_RUN" -eq 0 ]]; then
  CONTAINER_CREATED=1
fi

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

  TMPDIR_EXPORT="$OUT_DIR/.i3747_export_tmp"
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

  # ==========================================================================
  # THE FIXTURE MUST CARRY ITS OWN SUBJECT, AND THIS IS WHERE THAT IS PROVED.
  #
  # A generation can succeed in every mechanical respect — container up, schema
  # applied, one Data.db, non-empty golden — and still not contain an empty map
  # key, if (say) cqlsh silently coerced `{'': 1}` or a schema edit dropped a
  # column. The fixture would then be a green subject for a test that can no
  # longer fail, which is worse than no fixture. So the EMPTY KEY is asserted
  # POSITIVELY in the golden, per COLUMN, rather than once for the whole file.
  #
  # sstabledump renders a multicell map cell's key in the cell's "name" field.
  # An EMPTY key renders as an empty string, so the assertion is made with a
  # python json walk rather than a `grep` for `""` — which would match any empty
  # string anywhere in the document and pass for the wrong reason.
  # ==========================================================================
  golden=$(find "$OUT_DIR/$KEYSPACE/$TABLE"* -name "*-Data.db.jsonl" | head -1)
  [[ -n "$golden" ]] || fail "no $TABLE golden found under $OUT_DIR/$KEYSPACE/$TABLE*"

  log "Verifying the golden actually carries an empty map key, per column..."
  python3 - "$golden" <<'PY' || fail "golden verification FAILED (see above)"
import json, sys

golden = sys.argv[1]

# Columns that MUST show an empty key in the golden. m_frozen is excluded on
# purpose: a frozen map is ONE inline cell whose whole value is the serialized
# map, so it has no per-entry cell "name" to inspect — its empty key lives
# inside the cell value blob, not in the dump's cell names. Asserting it here
# would be asserting a shape sstabledump does not expose.
REQUIRED = ["m_text", "m_blob", "m_ascii"]

found = {c: False for c in REQUIRED}
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
                if nm in found and cell.get("path") == [""]:
                    found[nm] = True

if rows_seen == 0:
    print("[i3747][ERROR] golden has ZERO rows: %s" % golden, file=sys.stderr)
    sys.exit(1)

missing = [c for c, ok in found.items() if not ok]
if missing:
    print(
        "[i3747][ERROR] golden %s has NO empty-key cell for column(s): %s\n"
        "               The fixture does not carry the subject of issue #3747."
        % (golden, ", ".join(missing)),
        file=sys.stderr,
    )
    # Dump the observed cell paths so the failure is diagnosable rather than bare.
    with open(golden) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            part = json.loads(line)
            for row in part.get("rows", []):
                for cell in row.get("cells", []):
                    if cell.get("name") in found:
                        print(
                            "               observed: name=%r path=%r"
                            % (cell.get("name"), cell.get("path")),
                            file=sys.stderr,
                        )
    sys.exit(1)

for c in REQUIRED:
    print("[i3747]   golden carries an EMPTY key for column '%s' (OK)" % c)
print("[i3747]   golden rows inspected: %d" % rows_seen)
PY

  log "=== $KEYSPACE generation COMPLETE ==="
  log "Fixture root (an sstables root): $OUT_DIR"

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
  echo "    '$REPO_ROOT'/test-data/schemas/issue-3747-empty-map-key.cql \\"
  echo "    '$REPO_ROOT'/test-data/scripts/generate-issue-3747-empty-map-key.sh"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'fixtures(#3747): Cassandra-written empty-map-key fixture'"
  echo "=============================================================="
fi
