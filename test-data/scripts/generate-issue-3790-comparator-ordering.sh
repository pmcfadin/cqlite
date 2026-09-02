#!/usr/bin/env bash
# generate-issue-3790-comparator-ordering.sh — Cassandra 5.0.2 INET/TIME
# multicell-collection ORDERING fixture (issue #3790).
#
# WHAT THIS PRODUCES AND WHY
# HISTORICALLY (the defect this fixture was built to falsify),
# `ComparatorType::compare` compared `Custom("inet")` and `Custom("time")` by
# their FORMATTED STRING (a `compare_custom` helper doing `format!("{}", value)`)
# instead of by serialized bytes. #3790 removed that helper: `inet` and `time` now
# compare BY VALUE in `cqlite-core/src/types/comparator/custom.rs`, and the
# formatted-string comparison survives only as the residual path for an
# unresolved-UDT / unknown `Custom(name)`.
# Cassandra 5.0.8 constructs both `InetAddressType` and `TimeType` with
# `ComparisonType.BYTE_ORDER`, so the correct order is unsigned byte-wise over the
# serialized value. In a NON-FROZEN collection the element /
# map key IS the cell path, so that comparator decides the ON-DISK order — which
# is what makes the divergence observable in Cassandra-written bytes.
#
# NO committed schema could serve this: collections.cql has `MAP<TEXT, INET>`
# (inet as the VALUE, never an ordering position) and there is no `SET<INET>`,
# no `SET<TIME>` and no MAP keyed by either type under test-data/schemas/.
#
# CASSANDRA-WRITTEN, NOT CQLITE-WRITTEN — #3790 AC3, and the whole point. A
# CQLite-written + CQLite-read round-trip is INVARIANT to a uniform ordering
# error: both sides make the identical mistake, the round-trip closes, and the
# test stays green while real Cassandra data reads misordered (CLAUDE.md, #3042).
#
# Schema: test-data/schemas/issue-3790-comparator-ordering.cql — read it for the
# per-column rationale and for the stated ORACLE, including the honest scope note
# that `time` cannot falsify CQLite's current fixed-width `fmt_time` rendering
# and is a value-order PIN (plus a falsifier for a decimal-nanosecond string
# comparison), not a falsifying case for the string rendering.
#
# ============================================================================
# OUTPUT LOCATION IS LOAD-BEARING
#
# Written CHECKOUT-RELATIVE into the committed corpus:
#   test-data/datasets/sstables/<keyspace>/<table>-<uuid>/
# That is the SECOND built-in candidate root of the TABLE-granular resolver
# cqlite-core/tests/support/datasets_root.rs::sstables_root_for_table, so a
# consumer resolves it with no extra candidate root and still finds it on a fleet
# box whose CQLITE_DATASETS_ROOT (e.g. /data/datasets) does not carry it — the
# #3220 defect being a resolver that selected by KEYSPACE and then declared the
# table absent.
#
# THE COST, WHICH IS NOT OPTIONAL TO PAY: per test-data/corpus-coverage-policy.md
# every committed keyspace under that directory must be CLASSIFIED, and an
# unclassified one REDS the enumeration guard in all three comprehensive
# harnesses. `test_comparator_order` is classified as a SKIP-SET
# (parity-fixture) keyspace — validated by the dedicated #3790 Rust ordering
# test, not by the comprehensive read-parity corpus, and deliberately not
# enforced because the ordering it pins is the very thing that was wrong. If you
# rename the keyspace or add another, update the policy row AND all three harness
# skip sets in the same change; the README beside the fixture tracks which are
# done.
# ============================================================================
#
# ============================================================================
# MANDATORY: committing the .db binaries
#
# The *.db files are gitignored (`*.db`) and will NOT be picked up by a bare
# `git add`. They MUST be force-added with `git add -f` — mandated doctrine
# (CLAUDE.md, "Gitignored reference binaries"). The exact commands are printed at
# exit. Verify them from a fresh `git worktree add --detach HEAD`, never from the
# dirty tree that produced them.
# ============================================================================
#
# Usage:
#   bash test-data/scripts/generate-issue-3790-comparator-ordering.sh [--out <dir>] [--dry-run]
#
# Prerequisites: Docker (or podman) in PATH; ~2 GB RAM for the container.
#
# NOTE ON A FOREIGN CONTAINER: another lane's cassandra:5.0.2 container may be
# running on the same box. Every container operation targets the exact
# CONTAINER_NAME below — no `ancestor=`/image-wide filter anywhere — the
# pre-flight FAILS CLOSED if that name already exists rather than reclaiming it,
# and `nodetool flush` is keyspace-scoped. A concurrent foreign Cassandra cannot
# be touched by this run.
#
# Backs: issue #3790.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets/sstables}"
DRY_RUN="${DRY_RUN:-0}"
CONTAINER_NAME="cqlite-issue3790-cmporder"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_comparator_order"
TABLE="collection_order"

SCHEMA_FILE="$ROOT/schemas/issue-3790-comparator-ordering.cql"

# Every INSERT pins this timestamp, which stabilises liveness_info.tstamp and
# every complex deletion's marked_deleted in the committed golden.
#
# WHAT IS REPRODUCIBLE, AND WHAT IS NOT (issue #3790, roborev finding 1):
# This golden is NOT byte-reproducible, and an earlier version of this comment
# wrongly claimed it was. Assigning a whole NON-FROZEN collection (`inet_set =
# {...}`) makes Cassandra emit a COLLECTION TOMBSTONE — a complex deletion — ahead
# of the cells, and its `local_delete_time` is WALL CLOCK at generation time, not
# derived from the pinned writetime. Every one of the five complex columns carries
# one, so regeneration changes those fields even with T_FIXED unchanged.
#
# Reproducible: the pinned timestamps (liveness_info.tstamp, marked_deleted) and —
# the only thing this fixture is an oracle for — the ORDER of the cell paths.
# NOT reproducible: each complex deletion's `local_delete_time`. Normalise or
# ignore that field before any byte/JSON comparison of the golden.
T_FIXED=1000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1;   shift   ;;
    *) echo "[i3790] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$PWD/$OUT_DIR"
fi

log()  { echo "[i3790] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[i3790][ERROR] $*" >&2; exit 1; }

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
    echo "[i3790] (dry-run) no container engine found; using placeholder 'docker'"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

if [[ "$DRY_RUN" -eq 0 ]] && $ENGINE inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  fail "Container '$CONTAINER_NAME' already exists. Remove it first:
  $ENGINE rm -f $CONTAINER_NAME"
fi

# CLEAN UP BY OWNED ID, NEVER BY NAME (roborev job 51, finding 2).
# This trap is installed BEFORE the container exists, so cleaning up by NAME races
# a concurrent invocation: both can pass the `inspect` check above while the name is
# absent, one wins `run --name`, and the LOSER's failed run fires this trap and
# deletes the WINNER's container. Only ever remove a container this process
# successfully started, identified by the id `run -d` returned.
OWNED_CID=""
cleanup() {
  if [[ "$DRY_RUN" -eq 0 && -n "$OWNED_CID" ]]; then
    log "Cleaning up container $OWNED_CID..."
    $ENGINE rm -f "$OWNED_CID" >/dev/null 2>&1 || true
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
# Rows. The INSERT literals are deliberately written in an order that is NEITHER
# the byte order NOR the string order, so nothing about the observed on-disk
# order can be an artefact of insertion sequence.
#
#   id 1 — THE FULL CASE. Six inet values spanning IPv4 and IPv6 (the 4-vs-16
#          byte length case) and five time values. Three inet pairs invert
#          between byte order and string order; see the schema header for the
#          side-by-side oracle.
#
#   id 2 — THE MINIMAL FALSIFYING PAIR: {10.0.0.2, 9.0.0.1} and
#          {00:00:09, 00:00:10}, two elements per collection. Pins that the
#          divergence is a property of the COMPARATOR, not of collection size or
#          of the surrounding partition.
#
# Map/tuple text values are self-describing ('v4-nine', 'pair-ten', ...) so a
# displaced or lost key is visible in the output rather than merely absent.
# ----------------------------------------------------------------------------
insert_rows() {
  log "=== $TABLE: inserting rows (USING TIMESTAMP $T_FIXED) ==="

  cql "INSERT INTO $TABLE (id, inet_set, inet_map, time_set, time_map, pair_set) VALUES (
         1,
         {'192.168.0.1', '9.0.0.1', 'fe80::1', '10.0.0.2', '::1', '2001:db8::1'},
         {'192.168.0.1': 'v4-private', '9.0.0.1': 'v4-nine', 'fe80::1': 'v6-linklocal',
          '10.0.0.2': 'v4-ten', '::1': 'v6-loopback', '2001:db8::1': 'v6-doc'},
         {'12:00:00.000000000', '00:00:09.000000000', '23:59:59.999999999',
          '00:00:00.000000000', '00:00:10.000000000'},
         {'12:00:00.000000000': 't-noon', '00:00:09.000000000': 't-nine-sec',
          '23:59:59.999999999': 't-max', '00:00:00.000000000': 't-midnight',
          '00:00:10.000000000': 't-ten-sec'},
         {('192.168.0.1', '12:00:00.000000000'), ('9.0.0.1', '23:59:59.999999999'),
          ('10.0.0.2', '00:00:09.000000000'), ('2001:db8::1', '00:00:10.000000000'),
          ('10.0.0.2', '00:00:00.000000000')}
       ) USING TIMESTAMP $T_FIXED"

  cql "INSERT INTO $TABLE (id, inet_set, inet_map, time_set, time_map, pair_set) VALUES (
         2,
         {'10.0.0.2', '9.0.0.1'},
         {'10.0.0.2': 'pair-ten', '9.0.0.1': 'pair-nine'},
         {'00:00:10.000000000', '00:00:09.000000000'},
         {'00:00:10.000000000': 'pair-ten-sec', '00:00:09.000000000': 'pair-nine-sec'},
         {('10.0.0.2', '00:00:09.000000000'), ('9.0.0.1', '00:00:10.000000000')}
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
  # SCOPED TO THE ONE CAPTURED GENERATION DIRECTORY (roborev jobs 53 + 63): the
  # generator only owns what it just wrote, and `$GEN_DIR` is the exact
  # <TABLE>-<32-hex> path validated at export — not a re-glob with a looser rule.
  done < <(find "$GEN_DIR" -type f -name "*-Data.db" -not -name "._*" -print0 \
            2>/dev/null || true)
}

# ----------------------------------------------------------------------------
# OUT_DIR safety: the export step rm -rf's the keyspace subtree.
#
# CANONICALIZE BEFORE VALIDATING (roborev job 287 on the #3630 generator, kept
# verbatim). The prefix tests below are LEXICAL string comparisons, so a path
# containing `..` segments or a symlinked component can SATISFY
# `$OUT_DIR == $REPO_ROOT/*` while resolving somewhere else entirely — and what
# follows is `rm -rf "$OUT_DIR/$KEYSPACE"`. Resolving first closes the class
# rather than blacklisting `..`. `realpath -m` is REQUIRED, not optional:
# silently skipping canonicalization would leave the destructive path
# unvalidated, the permissive branch of a two-valued test.
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

log "Starting $KEYSPACE generation (issue #3790)"
log "Output directory: $OUT_DIR"

log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] $ENGINE run -d --name $CONTAINER_NAME ... $CASSANDRA_IMAGE"
else
  # Capture the id so `cleanup` removes only OUR container (see the trap above).
  # A failure here leaves OWNED_CID empty, so the trap is a no-op and a concurrent
  # run's container is left alone.
  OWNED_CID="$($ENGINE run -d \
    --name "$CONTAINER_NAME" \
    -e MAX_HEAP_SIZE=1G \
    -e HEAP_NEWSIZE=256m \
    -e CASSANDRA_CLUSTER_NAME=cqlite-issue3790 \
    "$CASSANDRA_IMAGE")" \
    || fail "Failed to start $CASSANDRA_IMAGE as '$CONTAINER_NAME'. If a concurrent
  run of this script owns that name, wait for it to finish rather than removing it."
  [[ -n "$OWNED_CID" ]] || fail "container started but no id was returned by $ENGINE"
  log "Started container id $OWNED_CID"
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

  TMPDIR_EXPORT="$OUT_DIR/.i3790_export_tmp"
  rm -rf "$TMPDIR_EXPORT"
  mkdir -p "$TMPDIR_EXPORT"

  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$TMPDIR_EXPORT" -xf -; then
    if [[ -d "$TMPDIR_EXPORT/data/$KEYSPACE" ]]; then
      # REPLACE ONLY THE TABLE GENERATIONS, NEVER THE KEYSPACE DIRECTORY
      # (roborev job 42, finding 2). `rm -rf "$OUT_DIR/$KEYSPACE"` used to be
      # here, and it deleted the COMMITTED `README.md` beside the fixture --
      # which is THE ORACLE for this issue (it records the observed on-disk
      # ordering the tests assert against) -- and never restored it. Scope the
      # removal to the `<TABLE>-<uuid>/` generations so keyspace-level
      # documentation survives regeneration.
      mkdir -p "$OUT_DIR/$KEYSPACE"

      # REFUSE A SYMLINKED KEYSPACE PATH BEFORE DELETING ANYTHING
      # (roborev job 43, finding 2). Only OUT_DIR is canonicalised above; the
      # table-scoped `rm -rf` loop below would FOLLOW a symlinked
      # "$OUT_DIR/$KEYSPACE" and delete `collection_order-*` directories OUTSIDE
      # the validated output root. Fail closed rather than canonicalise-and-hope:
      # a symlink here is never legitimate for a committed fixture path.
      if [[ -L "$OUT_DIR/$KEYSPACE" ]]; then
        fail "$OUT_DIR/$KEYSPACE is a SYMLINK. Refusing to delete generations through it (a
      symlinked keyspace path would let this script remove directories outside the
      validated output root). Replace it with a real directory and re-run."
      fi
      # Belt: even for a real directory, confirm it is still beneath OUT_DIR after
      # canonicalisation before any removal.
      _ks_real="$(cd "$OUT_DIR/$KEYSPACE" && pwd -P)" || fail "cannot canonicalise $OUT_DIR/$KEYSPACE"
      _out_real="$(cd "$OUT_DIR" && pwd -P)" || fail "cannot canonicalise $OUT_DIR"
      case "$_ks_real/" in
        "$_out_real"/*) : ;;
        *) fail "$OUT_DIR/$KEYSPACE canonicalises to $_ks_real, which is OUTSIDE $_out_real. Refusing to delete." ;;
      esac

      # DELETE ONLY WHAT CASSANDRA ITSELF WOULD HAVE WRITTEN
      # (roborev job 47, finding 2). `"$TABLE"-*/` is too loose for an `rm -rf`:
      # the trailing slash makes the glob follow a SYMLINK-to-directory, and the
      # `*` matches any suffix, so an unrelated `collection_order-scratch/` or a
      # symlink planted beside the fixture would be destroyed. Deleting is the one
      # irreversible thing this script does, so each candidate must earn it:
      #   * iterate WITHOUT the trailing slash, so a symlink is seen as a symlink
      #     rather than silently dereferenced by the glob;
      #   * require Cassandra's exact table-directory basename, `<TABLE>-<32 hex>`;
      #   * refuse a symlink;
      #   * require a real directory; and
      #   * require the canonical target to be a DIRECT CHILD of the already
      #     validated keyspace directory, so nothing outside it can be reached.
      # Anything not matching is LEFT ALONE and reported, never deleted.
      shopt -s nullglob
      for _stale in "$OUT_DIR/$KEYSPACE/$TABLE"-*; do
        _base="$(basename "$_stale")"
        if [[ ! "$_base" =~ ^${TABLE}-[0-9a-f]{32}$ ]]; then
          log "SKIP (not a <table>-<32-hex> generation dir, leaving alone): $_stale"
          continue
        fi
        if [[ -L "$_stale" ]]; then
          log "SKIP (symlink, refusing to delete through it): $_stale"
          continue
        fi
        if [[ ! -d "$_stale" ]]; then
          log "SKIP (not a directory): $_stale"
          continue
        fi
        _stale_real="$(cd "$_stale" && pwd -P)" || fail "cannot canonicalise $_stale"
        if [[ "$(dirname "$_stale_real")" != "$_ks_real" ]]; then
          fail "$_stale canonicalises to $_stale_real, which is NOT a direct child of $_ks_real. Refusing to delete."
        fi
        log "removing previous generation $_stale_real"
        rm -rf "$_stale_real"
      done
      shopt -u nullglob
      cp -r "$TMPDIR_EXPORT/data/$KEYSPACE/." "$OUT_DIR/$KEYSPACE/"
      log "$KEYSPACE SSTables placed in $OUT_DIR/$KEYSPACE"

      # CAPTURE THE ONE EXPORTED GENERATION DIRECTORY, ONCE (roborev job 63).
      # The deletion loop above requires an exact <TABLE>-<32-hex> basename, but the
      # post-export passes used to re-glob `${TABLE}-*`, which is LOOSER: a
      # preserved unrelated directory (say `collection_order-scratch`) that the
      # deletion deliberately leaves alone could still be counted, cleaned,
      # selected as the golden, or staged. The two halves disagreed about what a
      # generation directory IS. Resolve it by naming the directory once, here,
      # from the same exact shape — then every later pass uses THAT path instead of
      # re-deriving it with a different rule.
      shopt -s nullglob
      _gens=()
      for _cand in "$OUT_DIR/$KEYSPACE/$TABLE"-*; do
        [[ -d "$_cand" && ! -L "$_cand" ]] || continue
        [[ "$(basename "$_cand")" =~ ^${TABLE}-[0-9a-f]{32}$ ]] || continue
        _gens+=("$_cand")
      done
      shopt -u nullglob
      case "${#_gens[@]}" in
        1) GEN_DIR="${_gens[0]}" ;;
        0) fail "no <$TABLE>-<32-hex> generation directory under $OUT_DIR/$KEYSPACE after export" ;;
        *) fail "expected ONE generation directory after export, found ${#_gens[@]}: ${_gens[*]}" ;;
      esac
      log "generation directory: $GEN_DIR"
    else
      fail "Expected $TMPDIR_EXPORT/data/$KEYSPACE but it was not found. Export failed."
    fi
    rm -rf "$TMPDIR_EXPORT"
  else
    fail "tar export from container failed."
  fi

  # ONE flush => exactly one Data.db. More than one means the inserts did not
  # land in a single memtable flush and the fixture is not the single-SSTable
  # subject the ordering test assumes.
  # $GEN_DIR, not a re-glob (roborev jobs 63/65): the directory was already
  # resolved and validated at export. A `$TABLE*` glob is looser even than
  # `$TABLE-*` and would let a preserved `collection_order-scratch` change this
  # count, making the single-SSTable assertion nondeterministic.
  cnt=$(find "$GEN_DIR" -name "*-Data.db" -not -name "._*" 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$cnt" -ne 1 ]]; then
    fail "$TABLE: expected exactly ONE Data.db, found $cnt."
  fi
  log "  $TABLE: exactly one Data.db (OK)"

  # UNCOMPRESSED BY CONSTRUCTION. A CompressionInfo.db here would mean the
  # `compression = {'enabled': 'false'}` clause did not take, and a zero-length
  # or absent CompressionInfo.db on a table that HAS one makes SELECT return 0
  # rows SILENTLY — the "0-rows-when-present" failure this repo says must never
  # pass. Asserted rather than assumed.
  # $GEN_DIR, not "${tdirs[@]}" — that array's assignment was removed with the
  # loose glob above, and an UNSET array here would make `find` search the CWD
  # instead of the fixture, so this check would pass or fail on unrelated files.
  if find "$GEN_DIR" -name "*CompressionInfo.db" -not -name "._*" | grep -q .; then
    fail "$TABLE: a CompressionInfo.db is present; the table was written COMPRESSED."
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
  # Both scoped to the ONE captured generation directory (roborev jobs 53 + 63):
  # the statistics pass WRITES a *-Statistics.db.txt beside every match and the
  # cleanup pass DELETES, so anything broader than what this run exported could
  # truncate or remove a file the generator does not own.
  done < <(find "$GEN_DIR" -name "*-Data.db" -not -name "._*" -print0)

  find "$GEN_DIR" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

  # Every ordering-bearing value must actually appear in the golden, or the
  # fixture does not carry the subject of issue #3790. Checked per VALUE rather
  # than once, because a schema or literal edit that drops one would otherwise
  # leave the fixture silently weaker while the assert still passed on the rest.
  # EXACTLY ONE golden in $GEN_DIR (roborev jobs 63/65). This was
  # `find "$OUT_DIR/$KEYSPACE/$TABLE"* ... | head -1`, which both re-globbed
  # loosely AND silently accepted the first of several — so an unrelated preserved
  # directory could supply the file these value assertions then "verified",
  # masking a bad generated oracle. `head -1` on a multi-match set is the
  # three-valued-signal-read-two-valued shape: "one" and "several" must not look
  # the same to a check whose job is to validate the one.
  mapfile -t _goldens < <(find "$GEN_DIR" -name "*-Data.db.jsonl" | sort)
  case "${#_goldens[@]}" in
    1) golden="${_goldens[0]}" ;;
    0) fail "no $TABLE golden (*-Data.db.jsonl) in $GEN_DIR" ;;
    *) fail "expected ONE $TABLE golden in $GEN_DIR, found ${#_goldens[@]}: ${_goldens[*]}" ;;
  esac
  for col in inet_set inet_map time_set time_map pair_set; do
    grep -q -- "$col" "$golden" \
      || fail "golden $golden does not mention the '$col' column."
    log "  golden mentions '$col' (OK)"
  done
  for val in '9.0.0.1' '10.0.0.2' '192.168.0.1' '2001:db8' 'fe80' '00:00:09' '00:00:10' '23:59:59'; do
    grep -q -- "$val" "$golden" \
      || fail "golden $golden does not mention the ordering-bearing value '$val'."
    log "  golden mentions '$val' (OK)"
  done

  log "=== $KEYSPACE generation COMPLETE ==="
  log "Fixture root (an sstables root): $OUT_DIR"

  echo ""
  echo "=============================================================="
  echo "  NEXT: commit the generated fixture"
  echo "=============================================================="
  echo ""
  # THE PRINTED PATHS ARE ABSOLUTE, AND BOTH HALVES MATTER (#3504's measured
  # trap, kept verbatim because it cost a broken commit there):
  # (1) the GLOBS are expanded by the pasting user's SHELL, not by git, so a
  #     $REPO_ROOT-relative glob silently matches nothing unless their cwd
  #     happens to be the repo root;
  # (2) the SIDECAR NAMES are `<generation>-<format>-TOC.txt` /
  #     `<...>-Digest.crc32` (e.g. `nb-1-big-TOC.txt`), so a bare `TOC.txt` /
  #     `Digest.crc32` pathspec matches NOTHING — and `git add` aborts on an
  #     unmatched pathspec, staging NONE of the other sidecars on the same
  #     command line. Hence the leading-`*` globs.
  # STAGE THE PREVIOUS GENERATION'S DELETION FIRST (roborev job 42, finding 1).
  # `git add` only ever ADDS. Regeneration produces a NEW `<uuid>` directory, so
  # adding just the new files leaves the OLD directory still tracked and still
  # committed -- two generations of the same table in the corpus, after which the
  # test's fixture lookup would be resolved by filesystem order. `git add -A --`
  # on the keyspace path stages deletions of tracked files (including the
  # force-added, otherwise-ignored .db binaries), which is exactly what is needed
  # and what a bare `git add` cannot do.
  echo "  # FIRST: stage the removal of any previous generation (git add only ADDS):"
  echo "  git -C '$REPO_ROOT' add -A -- \\"
  echo "    '$OUT_DIR'/$KEYSPACE"
  echo ""
  # The ADD paths name the ONE generation directory this run exported (roborev job
  # 63), not `$KEYSPACE/*/`: a keyspace-wide glob would force-add files from any
  # other directory that happens to sit there, including one the deletion pass
  # deliberately preserved. The `add -A` above is deliberately keyspace-scoped
  # instead, because its whole job is to stage the PREVIOUS generation's deletion,
  # and that directory no longer exists to be named.
  echo "  # Force-add the .db binaries (gitignored — MUST use -f):"
  echo "  git -C '$REPO_ROOT' add -f \\"
  echo "    '$GEN_DIR'/*.db"
  echo ""
  echo "  # Add the sidecars normally (not gitignored):"
  echo "  git -C '$REPO_ROOT' add \\"
  echo "    '$GEN_DIR'/*.jsonl \\"
  echo "    '$GEN_DIR'/*TOC.txt \\"
  echo "    '$GEN_DIR'/*.crc32 \\"
  echo "    '$GEN_DIR'/*.db.txt \\"
  echo "    '$REPO_ROOT'/test-data/schemas/issue-3790-comparator-ordering.cql \\"
  echo "    '$REPO_ROOT'/test-data/scripts/generate-issue-3790-comparator-ordering.sh"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'test(#3790): Cassandra-written inet/time collection-ordering fixture'"
  echo "=============================================================="
fi
