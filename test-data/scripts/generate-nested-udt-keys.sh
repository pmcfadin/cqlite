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
#   f_map_tuple_udt frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>
#   f_map_set_udt   frozen<map<frozen<set<frozen<key_part>>>, int>>
#
# The two FROZEN MAPS are the only columns whose values reach the Python
# binding's `value_to_hashable_key` at all: a frozen map's keys are decoded
# STRUCTURALLY (parse_frozen_map_value -> read_frozen_element ->
# parse_value_from_raw_bytes) and handed to `map_to_py`, which projects every key
# through that function. Every `set` column instead takes `set_to_py`'s
# UDT list fallback, and a MULTICELL map's keys arrive as opaque `Value::Blob`
# from the scalar-only `parse_cell_path_key` (#3612). See the schema header.
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

# NOTE: OUT_DIR is deliberately NOT normalized here. It is resolved to a
# canonical PHYSICAL path and validated in the "Destructive-path safety"
# section below, BEFORE any destructive operation runs.

log()  { echo "[nuk] $(date '+%Y-%m-%dT%H:%M:%S') $*"; }
fail() { echo "[nuk][ERROR] $*" >&2; exit 1; }

# ----------------------------------------------------------------------------
# Destructive-path safety (roborev job 240, F1)
#
# Every destructive operation in this script targets a path DERIVED from
# $OUT_DIR. A LEXICAL check on that string is not sufficient: `/tmp/work/../..`
# matches a `/tmp/*` glob yet resolves to `/`, and a symlinked component
# (`/tmp/x/escape -> /`) escapes the same way while looking local. So OUT_DIR is
# resolved to a canonical PHYSICAL path FIRST, and every destructive target is
# then re-checked against that resolution at its point of use.
#
# Audit of every destructive / truncating target in this script:
#   * rm -rf "$TMPDIR_EXPORT"            -> rm_rf_guarded (2 call sites)
#   * rm -rf "$SSTABLES_DIR/$KEYSPACE"   -> rm_rf_guarded
#   * find "$SSTABLES_DIR/$KEYSPACE" -delete -> dir validated immediately before
#   * > "$jsonl_file" / > "$stats_base"  -> both are `find` results from WITHIN
#     the already-validated $SSTABLES_DIR subtree, so they inherit its guarantee
#   * $ENGINE rm -f "$CONTAINER_NAME"    -> a container, fixed literal name
#     (2 call sites: `cleanup`, and `ensure_container`'s unconditional remove)
#   * tar -C "$TMPDIR_EXPORT" -xf -      -> extraction into a validated dir
# There is no `mv` in this script.
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Output-suppression audit (roborev job 245, F2)
#
# RULE: no step whose output becomes a COMMITTED ARTIFACT may have its failure
# suppressed. A golden that is silently empty, or silently never written, is
# worse than a failed run — the run reports COMPLETE, references.yml declares
# the sidecar present, and the next reader of the fixture finds nothing.
#
# Every `2>/dev/null`, `>/dev/null` and `|| true` in this file, and what it is:
#
#  FIXED (each produced or gated a committed artifact):
#   * sstablemetadata > *-Statistics.db.txt   was `2>/dev/null || true` +
#     "WARNING: Empty statistics" and carried on. Now FAILS on a non-zero exit
#     AND on an empty output file; stderr is no longer discarded.
#   * find driving the JSONL loop             was `2>/dev/null || true`, so a
#     failed/empty find wrote ZERO goldens silently. Suppression removed and
#     the number of goldens written is asserted non-zero.
#   * find driving the statistics loop        never suppressed, but had no
#     count; the number written is now asserted non-zero too.
#   * find for the exactly-one-Data.db check  `2>/dev/null` removed: `cnt=0`
#     failed the check but the reason was discarded.
#   * find -delete junk sweep                 was `2>/dev/null || true`; a
#     silent failure leaves `._*` files inside the committed fixture directory.
#
#  KEPT, each a PROBE whose non-zero exit is an ANSWER rather than a fault, and
#  none of which produces a committed artifact:
#   * realpath / readlink / `cd -P` in resolve_physical — tried in order, each
#     with an explicit fallback; the failure is handled, not ignored.
#   * `command -v docker|podman` — engine detection.
#   * `$ENGINE inspect` in container_exists — the whole point is the boolean.
#   * the cqlsh readiness poll in wait_cassandra — "not ready yet" is expected;
#     the LAST attempt is re-run UNSUPPRESSED before failing, so a timeout still
#     reports a cause. (That diagnostic re-run's own `|| true` exists so the
#     unconditional `fail` on the next line is what ends the run.)
#   * `$ENGINE rm -f` in cleanup / ensure_container — stdout only (the container
#     id); stderr flows. ensure_container's verdict comes from an affirmative
#     re-probe, and cleanup's runs in the EXIT trap where failing would rewrite
#     this run's exit status, so it WARNS loudly instead.
# ----------------------------------------------------------------------------

# Resolve a path to a canonical PHYSICAL path (symlinks in existing components
# followed, `.`/`..` collapsed). The path need NOT exist: OUT_DIR is routinely a
# directory this run is about to create. Echoes the resolved path; returns 1 if
# it cannot be resolved at all.
resolve_physical() {
  local p="$1" out=""
  [[ "$p" == /* ]] || p="$PWD/$p"
  if out=$(realpath -m -- "$p" 2>/dev/null) && [[ -n "$out" ]]; then
    :
  elif out=$(readlink -m -- "$p" 2>/dev/null) && [[ -n "$out" ]]; then
    :
  else
    # Portable fallback: physically resolve the deepest EXISTING ancestor with
    # `cd -P`, then re-append the not-yet-existing tail. `..` inside that tail is
    # NOT collapsed here, which is why validate_destructive_target rejects any
    # relative component surviving in the result (fail closed).
    local head="$p" tail="" resolved
    while [[ -n "$head" && "$head" != "/" && ! -d "$head" ]]; do
      tail="$(basename -- "$head")${tail:+/$tail}"
      head="$(dirname -- "$head")"
    done
    resolved="$(cd -P -- "$head" 2>/dev/null && pwd -P)" || return 1
    if [[ -n "$tail" ]]; then
      out="$resolved/$tail"
    else
      out="$resolved"
    fi
  fi
  # Collapse repeated separators. Not cosmetic: bash's own `cd -P; pwd -P` emits
  # a DOUBLED leading slash when the resolved ancestor is the root — measured,
  # with `escape -> /`, `cd -P /tmp/x/escape/etc; pwd -P` prints `//etc` — and an
  # empty component is semantically identical to a single separator on POSIX. If
  # it is not collapsed, the caller rejects the path on its empty-component
  # branch and the refusal message names `//etc` instead of the real `/etc`
  # target, i.e. the right verdict for the wrong stated reason.
  while [[ "$out" == *//* ]]; do out="${out//\/\//\/}"; done
  [[ -n "$out" ]] || return 1
  printf '%s\n' "$out"
}

# True iff $1 is a STRICT descendant of $2. Trailing-separator aware on both
# sides, so `/tmpfoo` is NOT beneath `/tmp` and `/tmp` is not beneath itself.
is_strictly_beneath() {
  local cand="${1%/}" root="${2%/}"
  [[ -n "$cand" && -n "$root" ]] || return 1   # never approve `/` as a root
  [[ "$cand" != "$root" ]] || return 1
  [[ "$cand" == "$root"/* ]]
}

# Approved roots for destructive operations, themselves resolved physically
# (`/tmp` is a symlink to `/private/tmp` on macOS, so comparing the raw strings
# would reject every legitimate macOS temp path).
APPROVED_DESTRUCTIVE_ROOTS=()
for _root in "$REPO_ROOT" "/tmp"; do
  if _resolved_root="$(resolve_physical "$_root")" && [[ "$_resolved_root" != "/" ]]; then
    APPROVED_DESTRUCTIVE_ROOTS+=("$_resolved_root")
  fi
done
if [[ "${#APPROVED_DESTRUCTIVE_ROOTS[@]}" -eq 0 ]]; then
  fail "No approved destructive root could be resolved (tried '$REPO_ROOT' and '/tmp'). Refusing."
fi

# Resolve $2 and assert it is strictly beneath an approved root; echo the
# RESOLVED path on success, or a diagnostic on stderr and RETURN 1 on rejection.
#
# It deliberately does NOT call `fail` (which exits): every caller invokes this
# through a command substitution, which runs in a SUBSHELL, so an `exit` here
# would kill only that subshell and the caller would carry on with an empty
# path. Measured — an earlier version printed its refusal and the caller still
# reached its `rm -rf`. Each caller must therefore check the status explicitly
# (`|| exit 1` at top level, or the `if !` form inside a function).
validate_destructive_target() {
  local label="$1" raw="$2" resolved root
  if ! resolved="$(resolve_physical "$raw")"; then
    echo "[nuk][ERROR] $label '$raw' could not be resolved to a physical path. Refusing." >&2
    return 1
  fi
  if [[ "$resolved" == "/" ]]; then
    echo "[nuk][ERROR] $label '$raw' resolves to '/'. Refusing to operate on the filesystem root." >&2
    return 1
  fi
  # `$resolved` always begins with `/`, so appending one trailing separator makes
  # every component `/`-delimited on BOTH sides: `*/../*` and `*/./*` then match a
  # surviving relative component in any position, and `*//*` an empty one. Do NOT
  # prepend a second `/` — `//tmpfoo/x/` matches `*//*` and every path would be
  # rejected on this branch (caught in RED-verify: all four cases failed here
  # rather than on the beneath check they were written for). The `*//*` arm is
  # belt — resolve_physical collapses empty components — and is kept because a
  # destructive guard should fail closed if that ever stops holding.
  case "$resolved/" in
    */../* | */./* | *//*)
      echo "[nuk][ERROR] $label '$raw' resolved to '$resolved', which still contains a relative or empty path component. Refusing." >&2
      return 1 ;;
  esac
  for root in "${APPROVED_DESTRUCTIVE_ROOTS[@]}"; do
    if is_strictly_beneath "$resolved" "$root"; then
      printf '%s\n' "$resolved"
      return 0
    fi
  done
  echo "[nuk][ERROR] $label '$raw' resolves to '$resolved', which is not strictly beneath an approved root (${APPROVED_DESTRUCTIVE_ROOTS[*]}). Refusing destructive operation." >&2
  return 1
}

# `rm -rf` that re-validates its target immediately before deleting. The status
# of the substitution is checked explicitly rather than left to `set -e`, which
# is suppressed for any command in a `&&`/`||`/`!` list.
rm_rf_guarded() {
  local target
  if ! target="$(validate_destructive_target "rm -rf target" "$1")" || [[ -z "$target" ]]; then
    fail "Refusing to 'rm -rf' '$1': rejected by the destructive-path guard (see above)."
  fi
  rm -rf -- "$target"
}

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
    # An EXIT-trap failure must not change this run's exit status, so the
    # non-zero case WARNS rather than failing — but stderr is not discarded and
    # the warning is loud, because a container left behind is what the next run
    # has to remove. It cannot corrupt a later run's goldens (there is no reuse
    # path), so it is genuinely a warning and not a fail-closed condition.
    if ! $ENGINE rm -f "$CONTAINER_NAME" >/dev/null; then
      echo "[nuk][WARNING] could not remove container '$CONTAINER_NAME'; remove it by hand: $ENGINE rm -f $CONTAINER_NAME" >&2
    fi
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
  # The per-attempt probe above suppresses output BY DESIGN — it is a readiness
  # poll and "not ready yet" is its expected answer, not a fault. But the LAST
  # answer is a fault, and discarding it leaves a timeout with no cause, so run
  # the probe once more UNSUPPRESSED before failing.
  echo "[nuk] final readiness probe (unsuppressed), for the diagnostic:" >&2
  $ENGINE exec "$CONTAINER_NAME" cqlsh -e "SELECT cluster_name FROM system.local;" >&2 || true
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
#   * f_map_tuple_udt / f_map_set_udt hold two entries each, so the frozen-map
#     KEY decode (the only route to value_to_hashable_key) is exercised with
#     more than one key and with Cassandra's own key ordering.
insert_full() {
  log "=== nested_udt_keys id=1 (fully populated, multi-element) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt, f_map_tuple_udt, f_map_set_udt) VALUES (
    1,
    { ({label:'zulu', rank:26}, 7), ({label:'alpha', rank:1}, 2), ({label:'alpha', rank:1}, 1) },
    { { {label:'beta', rank:2}, {label:'alpha', rank:1} }, { {label:'gamma', rank:3} } },
    { ({label:'delta', rank:4}, 9): 90, ({label:'charlie', rank:3}, 8): 80 },
    { [ {label:'one', rank:1}, {label:'two', rank:2} ], [ {label:'two', rank:2}, {label:'one', rank:1} ] },
    { ({label:'frozen-b', rank:12}, 2), ({label:'frozen-a', rank:11}, 1) },
    { ({label:'mkey-b', rank:22}, 2): 220, ({label:'mkey-a', rank:21}, 1): 210 },
    { { {label:'mset-b', rank:32}, {label:'mset-a', rank:31} }: 310, { {label:'mset-c', rank:33} }: 330 }
  )"
}

# id 2 — NULL UDT FIELDS inside every hashable position, plus an EMPTY-string
# field (distinct from null). value_to_hashable_key's Udt arm has a
# `None => py.None()` path that no committed fixture previously reached.
# id 2 — NULL UDT FIELDS inside every hashable position, plus an EMPTY-string
# field (distinct from null).
#
# The FROZEN-MAP keys here are what make `value_to_hashable_key`'s Udt-arm
# `None => py.None()` branch reachable: those keys are the only values in this
# repository that arrive at that function as a structured `Value::Udt` carrying
# a `None` field. (The set columns' null fields go through `udt_to_py`'s own
# `None` branch — a different function.)
insert_null_fields() {
  log "=== nested_udt_keys id=2 (null UDT fields + empty-string field) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt, f_map_tuple_udt, f_map_set_udt) VALUES (
    2,
    { ({label:'nullrank', rank:null}, 1), ({label:null, rank:5}, 2) },
    { { {label:'nullrank2', rank:null}, {label:null, rank:null} } },
    { ({label:null, rank:null}, 0): 1, ({label:'', rank:0}, 0): 2 },
    { [ {label:'', rank:0}, {label:null, rank:9} ] },
    { ({label:null, rank:7}, 3) },
    { ({label:'nullrank3', rank:null}, 1): 51, ({label:null, rank:5}, 2): 52 },
    { { {label:null, rank:null} }: 61, { {label:'', rank:0} }: 62 }
  )"
}

# id 3 — minimal: exactly ONE element in every collection, same udt value in all
# five columns, so a decoder that confuses two columns is visible.
insert_minimal() {
  log "=== nested_udt_keys id=3 (single element per collection) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt, f_map_tuple_udt, f_map_set_udt) VALUES (
    3,
    { ({label:'solo', rank:99}, 42) },
    { { {label:'solo', rank:99} } },
    { ({label:'solo', rank:99}, 42): 7 },
    { [ {label:'solo', rank:99} ] },
    { ({label:'solo', rank:99}, 42) },
    { ({label:'solo', rank:99}, 42): 7 },
    { { {label:'solo', rank:99} }: 7 }
  )"
}

# id 4 — ABSENT columns: only the tuple-borne set is written. The other six
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

# Write one `*-Data.db.jsonl` sstabledump golden beside every exported Data.db.
#
# The `find` that DRIVES this loop is not suppressed and the number of goldens
# actually written is asserted to be non-zero (roborev job 245, F2). It used to
# read `find ... -print0 2>/dev/null || true`, which meant a failed or
# empty-result `find` produced NO goldens and NO error: the run reported
# COMPLETE while references.yml declared a `data_jsonl` that was never written.
# A golden that is silently absent (or silently empty) is worse than a failed
# run, so every step whose output becomes a committed artifact fails closed here.
generate_sstabledump_jsonl() {
  local sstables_dir="$1"
  local goldens_written=0
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
    goldens_written=$((goldens_written + 1))
  done < <(find "$sstables_dir/$KEYSPACE" -type f -name "*-Data.db" -not -name "._*" -print0)

  # Affirmative measurement: a `find` that failed, or a keyspace directory with
  # no Data.db in it, is indistinguishable from a clean pass by the loop alone.
  if [[ "$goldens_written" -eq 0 ]]; then
    fail "no *-Data.db found under $sstables_dir/$KEYSPACE — ZERO JSONL goldens were written. \
Refusing to report a complete generation with no golden."
  fi
  log "  $goldens_written JSONL golden(s) written"
}

# ----------------------------------------------------------------------------
# OUT_DIR safety
#
# Canonicalize BEFORE validating, and validate BEFORE any destructive operation:
# a lexical check accepts `..` and symlinked components that resolve outside the
# approved roots. OUT_DIR is REBOUND to the resolved path, so every path derived
# from it below (SSTABLES_DIR, TMPDIR_EXPORT) is derived from the resolved form.
# ----------------------------------------------------------------------------
OUT_DIR="$(validate_destructive_target "OUT_DIR" "$OUT_DIR")" || exit 1
if [[ -z "$OUT_DIR" ]]; then
  fail "OUT_DIR resolved to an empty path. Refusing."
fi

log "Starting $KEYSPACE generation (issue #3500)"
log "Output directory: $OUT_DIR"

SSTABLES_DIR="$OUT_DIR/sstables"

# ----------------------------------------------------------------------------
# Container lifecycle: ALWAYS RECREATE. There is deliberately NO reuse path.
#
# Rounds 3 and 4 of review each found a defect in a DIFFERENT branch of a
# three-way "reuse / start / replace" preflight (a stopped container treated as
# usable because `inspect` merely SUCCEEDS for it; then a RUNNING container
# reused, which preserves the SSTables of the previous run so a second
# generation either trips the exactly-one-Data.db check or exports STALE data).
# Two findings in one small function is a SHAPE problem, not two coincidences:
# the optional behaviour IS the bug surface. So the optional behaviour is gone
# rather than hardened a third time.
#
# The reason round 3 already gave for the stopped case applies verbatim to the
# running one — a fixture generator needs a clean slate, or a half-applied
# schema / leftover generation silently changes the goldens — and a generator's
# job is determinism, not convenience. Absent => create; EXISTS IN ANY STATE =>
# remove and create.
#
# `--keep-container` survives with a strictly narrowed meaning: leave the
# container in place at EXIT so it can be inspected. It can no longer influence
# what the NEXT run does, because the next run removes whatever it finds. (It is
# honoured on failure as well as on success: an inspectable container is most
# useful precisely when the run failed, and since reuse is impossible either way
# that costs nothing.)
# ----------------------------------------------------------------------------

# True iff a container named $CONTAINER_NAME EXISTS, in any state (created,
# running, exited, dead). `inspect` succeeding is exactly this fact — the
# question round 4 needs — and NOT "is it usable", which is what keying reuse on
# it got wrong.
container_exists() {
  $ENGINE inspect --type container "$CONTAINER_NAME" >/dev/null 2>&1
}

ensure_container() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $ENGINE rm -f $CONTAINER_NAME   # unconditional: there is no reuse path"
    start_container
    return 0
  fi

  if container_exists; then
    log "Container '$CONTAINER_NAME' EXISTS; removing it (this generator never reuses a container)."
    # stdout is only the container id; stderr is deliberately NOT suppressed so a
    # real removal failure is diagnosable. The verdict comes from the affirmative
    # re-probe below, not from this exit status.
    if ! $ENGINE rm -f "$CONTAINER_NAME" >/dev/null; then
      log "  '$ENGINE rm -f' exited non-zero; re-probing whether the container is gone."
    fi
    if container_exists; then
      fail "Container '$CONTAINER_NAME' still exists after '$ENGINE rm -f'. Refusing to generate \
against an unknown container state (its data directory may hold a previous run's SSTables). \
Remove it by hand and re-run."
    fi
    log "  removed."
  fi

  start_container
}

start_container() {
  log "Starting $CASSANDRA_IMAGE container ($CONTAINER_NAME)..."
  run $ENGINE run -d \
    --name "$CONTAINER_NAME" \
    -e MAX_HEAP_SIZE=1G \
    -e HEAP_NEWSIZE=256m \
    -e CASSANDRA_CLUSTER_NAME=cqlite-nestedudtkeys \
    "$CASSANDRA_IMAGE"
  STARTED_CONTAINER=1
}

ensure_container

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
  rm_rf_guarded "$TMPDIR_EXPORT"
  mkdir -p "$TMPDIR_EXPORT"

  if $ENGINE exec "$CONTAINER_NAME" bash -lc 'tar -C /var/lib/cassandra -cf - data' \
      | tar -C "$TMPDIR_EXPORT" -xf -; then
    if [[ -d "$TMPDIR_EXPORT/data/$KEYSPACE" ]]; then
      rm_rf_guarded "$SSTABLES_DIR/$KEYSPACE"
      mkdir -p "$SSTABLES_DIR/$KEYSPACE"
      cp -r "$TMPDIR_EXPORT/data/$KEYSPACE/." "$SSTABLES_DIR/$KEYSPACE/"
      log "$KEYSPACE SSTables placed in $SSTABLES_DIR/$KEYSPACE"
    else
      fail "Expected $TMPDIR_EXPORT/data/$KEYSPACE but it was not found. Export failed."
    fi
    rm_rf_guarded "$TMPDIR_EXPORT"
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
    # stderr NOT suppressed: a `find` failure here shows up as `cnt=0` and fails
    # the check below, but only its stderr says WHY (roborev job 245, F2).
    cnt=$(find "${tdirs[@]}" -name "*-Data.db" -not -name "._*" | wc -l | tr -d ' ')
    if [[ "$cnt" -ne 1 ]]; then
      fail "$table: expected exactly ONE flushed Data.db, found $cnt."
    fi
    log "  $table: exactly one Data.db (OK)"
  done

  generate_sstabledump_jsonl "$SSTABLES_DIR"

  # `sstablemetadata`'s stdout IS a committed golden
  # (`*-Statistics.db.txt`, declared `present: true` in
  # test-data/datasets/references.yml). It used to run as
  # `... > "$stats_base" 2>/dev/null || true`, which turned a tool failure into a
  # log line reading "WARNING: Empty statistics" while the run went on to report
  # COMPLETE — an empty or missing golden with a manifest that says it is there
  # (roborev job 245, F2). Both halves now fail the generation: a non-zero exit,
  # and an empty output file. stderr is no longer discarded.
  log "Generating Statistics.db.txt for $KEYSPACE tables..."
  stats_written=0
  while IFS= read -r -d '' data_file; do
    rel="${data_file#"$SSTABLES_DIR"/}"
    stats_base="${data_file%Data.db}Statistics.db.txt"
    log "  sstablemetadata: $rel"
    if ! $ENGINE run --rm \
      -v "$SSTABLES_DIR:/data" \
      "$CASSANDRA_IMAGE" \
      bash -lc "/opt/cassandra/tools/bin/sstablemetadata /data/${rel}" \
      > "$stats_base"; then
      fail "sstablemetadata FAILED (non-zero exit) for $rel. The committed \
$stats_base golden would be empty or partial while references.yml declares it present."
    fi
    if [[ ! -s "$stats_base" ]]; then
      fail "sstablemetadata produced an EMPTY golden: $stats_base (for $rel)."
    fi
    log "  OK: $stats_base"
    stats_written=$((stats_written + 1))
  done < <(find "$SSTABLES_DIR/$KEYSPACE" -name "*-Data.db" -not -name "._*" -print0)
  if [[ "$stats_written" -eq 0 ]]; then
    fail "ZERO Statistics.db.txt goldens were written for $KEYSPACE. \
Refusing to report a complete generation with no statistics golden."
  fi
  log "  $stats_written statistics golden(s) written"


  # `find -delete` is destructive: re-validate the root it walks first.
  _cleanup_root="$(validate_destructive_target "find -delete root" "$SSTABLES_DIR/$KEYSPACE")" \
    || exit 1
  [[ -n "$_cleanup_root" ]] || fail "find -delete root resolved to an empty path. Refusing."
  # Not suppressed either: this sweep removes macOS junk that would otherwise be
  # picked up by the commit globs the script prints, so a silent failure here
  # ends up in the committed fixture directory (roborev job 245, F2).
  find "$_cleanup_root" \( -name '._*' -o -name '.DS_Store' \) -delete

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
