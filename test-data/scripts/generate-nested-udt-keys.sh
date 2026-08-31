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
#   s_map_udt_key   set<frozen<map<frozen<key_part>, int>>>      (contains_udt k)
#   s_map_udt_val   set<frozen<map<int, frozen<key_part>>>>      (contains_udt v)
#
# The last two exist for `contains_udt`'s MAP arm, whose two halves
# (`contains_udt(k) || contains_udt(v)`) were covered by NOTHING. They are
# SEPARATE columns because `||` short-circuits, so one column with a UDT in both
# halves cannot distinguish a k-only from a v-only regression (roborev job 245).
#
# WHICH of these columns reach the Python binding's `value_to_hashable_key`, and
# on WHICH arm, is stated in exactly ONE place and is NOT restated here:
# the ROUTING section of bindings/python/src/value_hashable.rs. This header
# asserts nothing about it. (It was restated in four places in the test file and
# in both fixture headers, and drifted in all but one — the version that lived
# here claimed the two frozen maps were the only columns reaching that function
# "at all", which is false: five of the nine columns do.)
#
# This is a READ-fidelity fixture, not a compaction byte-parity fixture: there
# is ONE flush and therefore ONE SSTable generation per table, no explicit
# compaction, and no pinned USING TIMESTAMP. The oracles are the sstabledump
# JSONL golden committed beside the binaries and a real `SELECT`.
#
# Usage:
#   bash test-data/scripts/generate-nested-udt-keys.sh [--out <dir>] [--dry-run]
#   bash test-data/scripts/generate-nested-udt-keys.sh --verify-only [--out <dir>]
#
# `--verify-only` runs ONLY the outcome-based post-condition
# (verify_generated_artifacts) against an ALREADY-generated fixture tree and
# exits. It starts no container and needs no engine, so the post-condition can be
# exercised — including RED-verified against a deliberately damaged copy of the
# tree — without a 5-minute Cassandra run.
#
# Prerequisites: Docker (or podman) in PATH; ~4 GB RAM for the container.
# (`--dry-run` needs neither. `--verify-only` needs neither either, but DOES need
# `python3`, which verifies the JSONL golden's CONTENT; its absence is a FAILURE,
# not a skip — an unverified golden is not a verified one.)
#
# ============================================================================
# MANDATORY: committing the fixture
#
# TWO independent traps, both of which have bitten this fixture:
#
#  1. The *.db binaries are gitignored (`*.db` is ignored globally) and will NOT
#     be included by a bare `git add`. Without `-f`, the committed
#     JSONL/Digest sidecars point at a Data.db that is not in the tree and every
#     consumer of the fixture silently reads ZERO rows.
#
#  2. Every regeneration mints a NEW Cassandra table UUID, so the fixture
#     DIRECTORY NAME changes and the previous one is deleted. A staging command
#     that only adds CURRENT files leaves the deletion UNSTAGED, so the commit
#     carries BOTH generations — and the manifest, if it is not updated too,
#     keeps selecting the stale directory (roborev job 254, F2).
#
# So the staging command is `git add -f -A -- <keyspace path>` (`-A` stages
# deletions; `-f` forces the ignored binaries), and this script rewrites the
# `references.yml` entry ITSELF rather than printing a reminder. The exact
# commands are printed at exit.
# ============================================================================
#
# Backs: issue #3500.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
DRY_RUN="${DRY_RUN:-0}"
VERIFY_ONLY="${VERIFY_ONLY:-0}"
KEEP_CONTAINER="${KEEP_CONTAINER:-0}"
CONTAINER_NAME="cqlite-nestedudtkeys"
CASSANDRA_IMAGE="cassandra:5.0.2"
KEYSPACE="test_nested_udt_keys"
TABLES=(nested_udt_keys)

SCHEMA_FILE="$ROOT/schemas/nested-udt-keys.cql"

# ----------------------------------------------------------------------------
# THE REQUIRED ARTIFACT SET HAS TWO SOURCES, AND THEY MUST NOT BE CONFUSED
# (roborev job 257).
#
#  1. CASSANDRA'S OWN COMPONENTS ARE NOT LISTED HERE. They are DERIVED at
#     verification time from the fixture's `*-TOC.txt` — Cassandra's own manifest
#     of the components its writer emitted (see toc_components). A curated list
#     here is wrong in BOTH directions, and was: it MISSED components this
#     Cassandra wrote (`Index.db`, `Statistics.db`, `Summary.db`, `Filter.db`,
#     `CRC.db` were all unchecked, so `--verify-only` passed with them deleted),
#     and it would DEMAND one a legitimate configuration does not write
#     (`CompressionInfo.db`: these tables are deliberately uncompressed, so it is
#     absent from TOC.txt and must NOT be required). Deriving makes the
#     requirement a fact declared by the WRITER rather than a list we maintain.
#     TOC.txt names itself, which is why `-TOC.txt` is not special-cased.
#
#  2. OUR OWN DERIVED GOLDENS, which Cassandra's writer knows nothing about and
#     which therefore appear in no TOC.txt. They are enumerated here because
#     nothing else declares them — this script is what produces them.
DERIVED_ARTIFACT_SUFFIXES=(-Data.db.jsonl -Statistics.db.txt)

#  1b. A MANDATORY-COMPONENT FLOOR, because a TRUNCATED TOC.txt UNDER-DEMANDS
#      (roborev job 260).
#
# Deriving the requirement from TOC.txt makes it a fact declared by the WRITER —
# but it also makes the MANIFEST the only thing consulted, so a TOC.txt that is
# ITSELF damaged (truncated by a partial copy, an interrupted write, a bad
# staging step) requires LESS, and this post-condition then certifies an
# INCOMPLETE SSTable as complete. Any nonempty TOC naming `Data.db` was accepted,
# so a TOC cut down to that one line demanded exactly one component.
#
# MEASURED, including the part that makes it easy to miss: a TOC truncated to the
# single line `Data.db` DID red before this floor existed — but via the NEIGHBOURING
# references.yml cross-check ("advertises 'nb-1-big-TOC.txt', which is neither a
# TOC.txt component nor ..."), i.e. the right verdict for a stated reason that has
# nothing to do with an incomplete SSTable. Trim the TOC to
# `Data.db / Digest.crc32 / TOC.txt` instead — keeping exactly what
# $REFERENCED_SIDECAR_SUFFIXES advertises — and the neighbour falls silent: that
# tree reported `POST-CONDITION OK ... 3 declared by TOC.txt` with
# `Statistics.db`, `CRC.db`, `Index.db` and `Summary.db` unrequired. A check whose
# failure is delegated to a neighbour is a latent false pass, because the coupling
# evaporates the moment the neighbour's subject changes.
#
# The floor is the components Cassandra 5.0 ALWAYS writes for an UNCOMPRESSED BIG
# (`nb`) table, and the required set is TOC UNION FLOOR. It is a MINIMUM, never
# an expectation of equality: a legitimately richer TOC (this fixture's, which
# also names `Filter.db`) still has every entry required, because the union keeps
# everything the manifest declares.
#
# AUTHORITY — CASSANDRA'S OWN WRITER AT THE PINNED TAG. A CQLite file:line is
# never format authority, so nothing here is derived from our reader. Read it:
#   git show cassandra-5.0.8:src/java/org/apache/cassandra/io/sstable/format/SSTableWriter.java
#   git show cassandra-5.0.8:src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java
#   git show cassandra-5.0.8:src/java/org/apache/cassandra/io/sstable/format/SortedTableWriter.java
# Component name -> on-disk repr strings come from `SSTableFormat.Components.Types`
# (SSTableFormat.java) and `BigFormat.Components.Types` (BigFormat.java);
# cross-checked against docs/sstables-definitive-guide/ Ch.5 (Data.db) and Ch.6
# (Index.db / Summary.db), which agree.
#
#   * `SSTableWriter.Builder#addDefaultComponents` — UNCONDITIONAL:
#     DATA "Data.db", STATS "Statistics.db", DIGEST "Digest.crc32", TOC "TOC.txt".
#     Then it branches on `params.compression.isEnabled()`: COMPRESSION_INFO
#     "CompressionInfo.db" in the `if`, CRC "CRC.db" in the `else`. These tables
#     are DELIBERATELY UNCOMPRESSED, so `CRC.db` is IN the floor and
#     `CompressionInfo.db` is NOT — requiring it would fail a correct fixture,
#     which is the same error the old curated list made.
#   * `BigTableWriter.Builder#addDefaultComponents` — UNCONDITIONAL for BIG:
#     PRIMARY_INDEX "Index.db", SUMMARY "Summary.db".
#   * `SortedTableWriter.Builder#addDefaultComponents` adds FILTER "Filter.db"
#     ONLY under `FilterComponent.shouldUseBloomFilter(...bloomFilterFpChance)`,
#     so it is CONDITIONAL (a table at fp_chance 1.0 writes none) and is
#     deliberately OUT of the floor. This fixture DOES write it, and the union
#     with TOC.txt is what requires it — no authority disagrees with the
#     committed TOC.txt here: it names all 7 floor components plus `Filter.db`.
MANDATORY_TOC_COMPONENTS=(Data.db Statistics.db Digest.crc32 CRC.db TOC.txt Index.db Summary.db)

# What references.yml advertises to fixture CONSUMERS. Every entry must be part
# of the required set above (a derived-artifact suffix, or `-<TOC component>`);
# verify_generated_artifacts asserts exactly that, so the manifest cannot come to
# advertise an artifact the post-condition does not require.
REFERENCED_SIDECAR_SUFFIXES=(-Data.db.jsonl -Statistics.db.txt -TOC.txt -Digest.crc32)

# The partitions the four insert_* functions below write, and the columns each
# carries: `ALL` = every non-PK column declared in $SCHEMA_FILE; otherwise a
# space-free comma list. verify_jsonl_content asserts this against the JSONL
# golden's CONTENT — a golden that parses, and is the right SIZE, but holds a
# PARTIAL or WRONG dataset is the other half of the round-8 finding, and
# non-emptiness cannot see it. `4:s_tuple_udt` IS insert_partial's deliberate
# absent-column row, so this declaration is also what stops that row's nine
# missing columns reading as a defect.
EXPECTED_PARTITIONS=(1:ALL 2:ALL 3:ALL 4:s_tuple_udt)

REFERENCES_YML="$ROOT/datasets/references.yml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)            OUT_DIR="$2"; shift 2 ;;
    --dry-run)        DRY_RUN=1; shift ;;
    --verify-only)    VERIFY_ONLY=1; shift ;;
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
#     failed/empty find wrote ZERO goldens silently. Suppression removed.
#   * find driving the statistics loop        never suppressed, but had no
#     count.
#   * find for the exactly-one-Data.db check  `2>/dev/null` removed: `cnt=0`
#     failed the check but the reason was discarded.
#   * find -delete junk sweep                 was `2>/dev/null || true`; a
#     silent failure leaves `._*` files inside the committed fixture directory.
#
#  FIXED AGAIN (roborev job 254, F1): removing the suppression was not enough.
#  All four of those `find`s ran inside a PROCESS SUBSTITUTION or behind an
#  ASSIGNMENT, both of which DISCARD the exit status — so a find that emitted
#  some paths and then failed drove a partial loop and the run still reported
#  success, and the non-zero COUNTs added above cannot tell "3 of 3" from "3 of
#  10, then failed". Every one of them now goes through collect_matching_files /
#  an explicit `if !`, and the junk sweep's `-delete` status is checked by name.
#
#  AND THE AUDIT ITSELF IS NOT THE CONTRACT. An enumeration of suppression sites
#  closes only the mechanisms someone has already thought of; six findings across
#  four rounds were six different mechanisms. The contract is the outcome-based
#  post-condition, verify_generated_artifacts — see its header.
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

# Scratch directory for NUL-delimited file lists (see collect_matching_files).
# Created lazily, removed by the EXIT trap.
WORK_TMP=""
#
# Deliberately sets a GLOBAL rather than echoing the path: `fail` inside a
# command substitution exits only that SUBSHELL, so an echoing variant would
# leave the caller running with an empty path (the same defect documented on
# validate_destructive_target below). Callers run it, then read $WORK_TMP.
ensure_work_tmp() {
  if [[ -n "$WORK_TMP" && -d "$WORK_TMP" ]]; then
    return 0
  fi
  WORK_TMP="$(mktemp -d "${TMPDIR:-/tmp}/nuk-lists.XXXXXX")" \
    || fail "could not create a scratch directory for file lists."
  [[ -n "$WORK_TMP" && -d "$WORK_TMP" ]] \
    || fail "scratch directory for file lists was not created."
}

# Materialize a `find` result into the NUL-delimited list file $2, with find's
# EXIT STATUS EXPLICITLY CHECKED (roborev job 254, F1). Remaining arguments are
# the find predicates.
#
# WHY THIS EXISTS. `while IFS= read -r -d '' f; do … done < <(find …)` DISCARDS
# find's exit status: the `while` reports the status of the last `read`, and a
# process substitution's status is not propagated anywhere at all. So a `find`
# that emitted three paths and THEN failed — an unreadable subdirectory, a tree
# that changed under it — drove the loop over a PARTIAL set and the run carried
# on to report a COMPLETE generation with incomplete artifacts.
#
# A non-zero COUNT does not close that hole, which is what round 4 of review got
# wrong: "found 3 of 3" and "found 3 of 10, then failed" are the SAME count. A
# count is not an affirmative measurement of COMPLETENESS. `mapfile -t x <
# <(find …)` has the identical defect — the status is still the substitution's.
#
# So find runs as the sole command of an `if !` (its status is checked directly,
# not left to `set -e`, which is suppressed inside `&&`/`||`/`!` lists), its
# output goes to a file, and callers iterate the FILE.
#
# `set -o pipefail` is in effect (see `set -euo pipefail` at the top of this
# file), so the pipelines elsewhere in this script fail on any stage's failure;
# there is deliberately no pipeline here.
collect_matching_files() {
  local search_root="$1" dest="$2"
  shift 2
  if [[ ! -d "$search_root" ]]; then
    fail "collect_matching_files: '$search_root' is not a directory."
  fi
  if ! find "$search_root" "$@" -print0 > "$dest"; then
    fail "find under '$search_root' exited NON-ZERO. Its output may be a PARTIAL file \
list, so this generation is aborted rather than continued against an incomplete set."
  fi
}

# Populate the global array TABLE_DIRS with the DIRECTORIES matching
# `<keyspace-dir>/<table>-*`, and nothing else (an unmatched glob leaves it
# empty; a non-directory sharing the prefix is not counted).
#
# A GLOBAL, not an echoed value, because callers need the COUNT as well as the
# path, and because `fail` inside a command substitution would exit only the
# subshell (see validate_destructive_target's note).
#
# Exactly ONE match is what a clean generation produces. Two means a previous
# generation is still sitting beside this one — each regeneration mints a new
# Cassandra table UUID, so the directory NAME changes — which is precisely the
# state the old staging instructions would have committed (roborev job 254, F2).
TABLE_DIRS=()
list_table_dirs() {
  local table="$1" ks_dir="$SSTABLES_DIR/$KEYSPACE" d
  TABLE_DIRS=()
  for d in "$ks_dir/$table"-*; do
    if [[ -d "$d" ]]; then
      TABLE_DIRS+=("$d")
    fi
  done
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
  if [[ "$DRY_RUN" -eq 1 || "$VERIFY_ONLY" -eq 1 ]]; then
    ENGINE="docker"
    echo "[nuk] no container engine found; using placeholder 'docker' (no container is started in --dry-run/--verify-only)"
  else
    fail "Neither docker nor podman found in PATH."
  fi
fi
log "Using container engine: $ENGINE"

STARTED_CONTAINER=0
cleanup() {
  # Scratch file lists only; never a committed artifact, so a removal failure
  # WARNS rather than rewriting this run's exit status from inside the trap.
  if [[ -n "$WORK_TMP" && -d "$WORK_TMP" ]]; then
    rm -rf -- "$WORK_TMP" \
      || echo "[nuk][WARNING] could not remove scratch dir '$WORK_TMP'; remove it by hand." >&2
  fi
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
#     KEY decode is exercised with more than one key and with Cassandra's own
#     key ordering.
#   * s_map_udt_key / s_map_udt_val hold TWO set elements, the first a two-entry
#     frozen map written out of key order: so the element is a real multi-pair
#     `Value::Map` when `contains_udt`'s Map arm walks it, and `any(...)` is not
#     trivially answered by a single pair.
insert_full() {
  log "=== nested_udt_keys id=1 (fully populated, multi-element) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt, f_map_tuple_udt, f_map_set_udt, s_map_udt_key, s_map_udt_val) VALUES (
    1,
    { ({label:'zulu', rank:26}, 7), ({label:'alpha', rank:1}, 2), ({label:'alpha', rank:1}, 1) },
    { { {label:'beta', rank:2}, {label:'alpha', rank:1} }, { {label:'gamma', rank:3} } },
    { ({label:'delta', rank:4}, 9): 90, ({label:'charlie', rank:3}, 8): 80 },
    { [ {label:'one', rank:1}, {label:'two', rank:2} ], [ {label:'two', rank:2}, {label:'one', rank:1} ] },
    { ({label:'frozen-b', rank:12}, 2), ({label:'frozen-a', rank:11}, 1) },
    { ({label:'mkey-b', rank:22}, 2): 220, ({label:'mkey-a', rank:21}, 1): 210 },
    { { {label:'mset-b', rank:32}, {label:'mset-a', rank:31} }: 310, { {label:'mset-c', rank:33} }: 330 },
    { { {label:'kb', rank:2}: 20, {label:'ka', rank:1}: 10 }, { {label:'kc', rank:3}: 30 } },
    { { 2: {label:'vb', rank:12}, 1: {label:'va', rank:11} }, { 3: {label:'vc', rank:13} } }
  )"
}

# id 2 — NULL UDT FIELDS inside every hashable position, plus an EMPTY-string
# field (distinct from null).
#
# EXCEPT s_map_udt_key, which carries an empty-string label instead of a null
# one: a null UDT field in a `set<frozen<map<frozen<udt>, …>>>` element makes the
# python driver's SortedSet FORMATTER raise while PRINTING (`'<' not supported
# between instances of 'str' and 'NoneType'`). Measured against 5.0.2 — the
# INSERT and the SELECT both exit 0 and the bytes are stored correctly, it is a
# client-side display failure — but it would put a spurious `Failed to format
# value` line in this generator's own log. Null-field coverage for the new pair
# lives in s_map_udt_val's map VALUES, where the formatter is fine.
#
# The null fields written here are why id=2 exists: a `None` UDT field is what
# `build_udt`'s `None => py.None()` path handles, and no committed fixture
# reached that path before this one. WHICH columns get there through the hashable
# projection's own arms, and which get there with `convert = value_to_py`, is
# stated only in the ROUTING section of bindings/python/src/value_hashable.rs;
# this generator asserts neither.
insert_null_fields() {
  log "=== nested_udt_keys id=2 (null UDT fields + empty-string field) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt, f_map_tuple_udt, f_map_set_udt, s_map_udt_key, s_map_udt_val) VALUES (
    2,
    { ({label:'nullrank', rank:null}, 1), ({label:null, rank:5}, 2) },
    { { {label:'nullrank2', rank:null}, {label:null, rank:null} } },
    { ({label:null, rank:null}, 0): 1, ({label:'', rank:0}, 0): 2 },
    { [ {label:'', rank:0}, {label:null, rank:9} ] },
    { ({label:null, rank:7}, 3) },
    { ({label:'nullrank3', rank:null}, 1): 51, ({label:null, rank:5}, 2): 52 },
    { { {label:null, rank:null} }: 61, { {label:'', rank:0} }: 62 },
    { { {label:'', rank:0}: 1, {label:'zz', rank:9}: 2 } },
    { { 1: {label:null, rank:null}, 2: {label:'', rank:0} } }
  )"
}

# id 3 — minimal: exactly ONE element in every collection, same udt value in all
# five columns, so a decoder that confuses two columns is visible.
insert_minimal() {
  log "=== nested_udt_keys id=3 (single element per collection) ==="
  cql "INSERT INTO nested_udt_keys (id, s_tuple_udt, s_set_udt, m_tuple_udt, s_list_udt, f_set_tuple_udt, f_map_tuple_udt, f_map_set_udt, s_map_udt_key, s_map_udt_val) VALUES (
    3,
    { ({label:'solo', rank:99}, 42) },
    { { {label:'solo', rank:99} } },
    { ({label:'solo', rank:99}, 42): 7 },
    { [ {label:'solo', rank:99} ] },
    { ({label:'solo', rank:99}, 42) },
    { ({label:'solo', rank:99}, 42): 7 },
    { { {label:'solo', rank:99} }: 7 },
    { { {label:'solo', rank:99}: 42 } },
    { { 42: {label:'solo', rank:99} } }
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
  # The driving `find` is MATERIALIZED with its exit status checked before the
  # loop runs (roborev job 254, F1) — a process substitution would discard it and
  # the loop would silently walk a partial set.
  ensure_work_tmp
  local data_list="$WORK_TMP/jsonl-data-files.list"
  collect_matching_files "$sstables_dir/$KEYSPACE" "$data_list" \
    -type f -name "*-Data.db" -not -name "._*"
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
  done < "$data_list"

  # Affirmative measurement that the loop had a SUBJECT. It is NOT a completeness
  # check — a count cannot distinguish a complete find from a truncated one, which
  # is why the find above is status-checked and why the outcome-based
  # post-condition (verify_generated_artifacts) is the actual contract.
  if [[ "$goldens_written" -eq 0 ]]; then
    fail "no *-Data.db found under $sstables_dir/$KEYSPACE — ZERO JSONL goldens were written. \
Refusing to report a complete generation with no golden."
  fi
  log "  $goldens_written JSONL golden(s) written"
}

# ----------------------------------------------------------------------------
# Populate the global TOC_COMPONENTS with the component names CASSANDRA'S OWN
# WRITER recorded in $1 (a `*-TOC.txt`), one per line, CR and surrounding
# whitespace stripped.
#
# THIS IS THE AUTHORITY for "which components must be present". TOC.txt is the
# writer's own declaration of what it emitted, so the required set is a FACT
# DECLARED BY CASSANDRA rather than a list this script maintains — the same
# derive-never-curate move the gate's executing feature lanes make. A component
# a legitimate configuration does not write (`CompressionInfo.db` here) is
# absent from TOC.txt and is therefore not demanded, with no exception list.
#
# A GLOBAL rather than an echoed value, for the reason documented on
# list_table_dirs. Returns 1 with a diagnostic on stderr rather than calling
# `fail`, so the caller can collect it alongside the other problems.
TOC_COMPONENTS=()
toc_components() {
  local toc="$1" line
  TOC_COMPONENTS=()
  if [[ ! -s "$toc" ]]; then
    echo "[nuk][ERROR]   - TOC.txt '$toc' is missing or EMPTY, so the required component set cannot be derived." >&2
    return 1
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    if [[ -n "$line" ]]; then
      TOC_COMPONENTS+=("$line")
    fi
  done < "$toc"
  # Fail CLOSED on a TOC this does not recognise AS one. An empty or unrecognised
  # derivation would EXCUSE every component — a vacuous pass, which is the exact
  # shape being fixed — so it is an error, never a permissive fallback.
  if [[ "${#TOC_COMPONENTS[@]}" -eq 0 ]]; then
    echo "[nuk][ERROR]   - TOC.txt '$toc' named ZERO components." >&2
    return 1
  fi
  # THE MANDATORY-COMPONENT FLOOR (roborev job 260). A TOC.txt is the writer's
  # declaration, and a DAMAGED one declares LESS — so a manifest missing a
  # component Cassandra 5.0 unconditionally writes is not a smaller valid
  # configuration, it is an UNTRUSTWORTHY MANIFEST, and deriving from it would
  # require less than a complete SSTable has. See $MANDATORY_TOC_COMPONENTS for
  # the floor and its pinned-Cassandra citation. This subsumes the former
  # `Data.db`-only check, which any one-line truncation satisfied.
  local want have absent=()
  for want in "${MANDATORY_TOC_COMPONENTS[@]}"; do
    local found=0
    for have in "${TOC_COMPONENTS[@]}"; do
      if [[ "$have" == "$want" ]]; then
        found=1
      fi
    done
    if [[ "$found" -ne 1 ]]; then
      absent+=("$want")
    fi
  done
  if [[ "${#absent[@]}" -ne 0 ]]; then
    echo "[nuk][ERROR]   - TOC.txt '$toc' is MISSING ${#absent[@]} mandatory component(s) \
${absent[*]} that Cassandra 5.0 ALWAYS writes for an uncompressed BIG table (it names: \
${TOC_COMPONENTS[*]}). The MANIFEST is damaged or truncated, so refusing to derive a component set \
from it — a shorter TOC would require LESS and certify an INCOMPLETE SSTable." >&2
    return 1
  fi
  return 0
}

# Assert the CONTENT of the JSONL golden $1 for table $2: exactly the partitions
# $EXPECTED_PARTITIONS declares, each carrying exactly the columns it declares,
# with the column NAMES derived from $SCHEMA_FILE's own CREATE TABLE.
#
# WHY, and why it is not just a size check: `[[ -s ]]` cannot tell this fixture's
# four-partition, nine-column golden from a three-partition one, from one missing
# a column across every row, or from a golden dumped from a DIFFERENT table
# (roborev job 257). All three are valid JSON of a plausible size.
#
# SCOPE, AND WHY VALUES ARE DELIBERATELY OUT OF IT (roborev job 260 asked for an
# expected-value oracle here; this is the recorded decision NOT to build one, so
# it is read as a scoping argument rather than re-raised as a gap).
#
# This post-condition's job is STRUCTURAL COMPLETENESS — the artifact set, the
# partition ids, the column coverage. It answers "did this generation produce a
# complete set of the RIGHT SHAPE". It deliberately does not compare normalized
# cell paths or VALUES against an expected fixture, because:
#
#   * VALUE correctness is ALREADY ORACLED, TWICE, AND BETTER. The 54 tests
#     (117 assertions) in bindings/python/tests/test_nested_udt_hashable.py
#     compare exact Python structures against THIS fixture, and the sstabledump
#     JSONL golden is itself the physical oracle that suite and the parity suite
#     read. A wrong value fails pytest — which runs in `python-bindings` on every
#     full gate, and in `--lite`'s `python-tier:` on any bindings/python diff.
#     Nothing added here would catch it sooner or more precisely.
#   * Re-deriving the expected values in bash/python here would be a SECOND
#     IMPLEMENTATION OF THE ORACLE IN A WEAKER LANGUAGE, and by this repo's own
#     recorded lesson (#3229) a port's correctness is only knowable by
#     differential testing against the original. Two oracles that must agree is a
#     DRIFT SURFACE, not extra safety — and the one in this script is the one
#     nobody updates when the fixture changes, so it would fail on correct input,
#     which is the shape of a check people learn to waive.
#
# So the division of labour is: THIS checks that generation produced a complete
# artifact set of the declared shape; PYTEST checks that the values in it are
# right. Widening this to values would not add a property, it would duplicate one.
#
# Returns 1 and lists EVERY problem on stderr; the caller collects the verdict.
verify_jsonl_content() {
  local jsonl="$1" table="$2"
  if ! command -v python3 >/dev/null 2>&1; then
    echo "[nuk][ERROR]   - python3 is required to verify the JSONL golden's CONTENT and was not found. \
The content of the golden is UNVERIFIED, which is not a pass." >&2
    return 1
  fi
  NUK_JSONL="$jsonl" \
  NUK_SCHEMA="$SCHEMA_FILE" \
  NUK_TABLE="$table" \
  NUK_EXPECTED_PARTITIONS="${EXPECTED_PARTITIONS[*]}" \
  python3 - <<'PY'
import json
import os
import re
import sys

jsonl = os.environ["NUK_JSONL"]
schema = os.environ["NUK_SCHEMA"]
table = os.environ["NUK_TABLE"]
spec_raw = os.environ["NUK_EXPECTED_PARTITIONS"].split()

problems = []


def die(msg):
    sys.exit("[nuk][ERROR]   - " + msg)


# ---- EXPECTED COLUMNS: derived from the committed CQL schema ----------------
# Derived, not hard-coded: the schema file is the declaration of what this table
# HAS, so adding a column there cannot leave the golden silently unchecked.
#
# Parsing it is sound for a reason stated IN that file's own header: "NO `--`
# COMMENT MAY APPEAR BETWEEN `CREATE TABLE (` AND ITS CLOSING `)`" (CQLite's nom
# schema parser cannot strip them, and a comment there takes the whole read path
# down), so the column list holds column declarations and nothing else. Every
# failure to parse is a HARD ERROR — never a silently empty expectation, which
# would make the checks below vacuous.
try:
    with open(schema, encoding="utf-8") as fh:
        src = fh.read()
except OSError as exc:
    die("cannot read the schema file %s: %s" % (schema, exc))

m = re.search(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?%s\s*\(" % re.escape(table),
    src,
    re.IGNORECASE,
)
if not m:
    die("schema %s declares no CREATE TABLE for '%s'" % (schema, table))

depth, i = 1, m.end()
while i < len(src) and depth:
    if src[i] == "(":
        depth += 1
    elif src[i] == ")":
        depth -= 1
    i += 1
if depth:
    die("unbalanced parentheses in the CREATE TABLE for '%s' in %s" % (table, schema))

# Split the column list on TOP-LEVEL commas only. Depth counts BOTH `(...)` and
# the CQL type parameter brackets `<...>`: every column in this table is a nested
# generic (`map<frozen<tuple<frozen<key_part>, int>>, int>`), so a paren-only
# depth splits inside a type and yields fragments like `int>>` as column names —
# measured, and it made the check demand five columns that do not exist while
# never asserting the nine that do.
entries, buf, depth = [], "", 0
for ch in src[m.end():i - 1]:
    if ch in "(<":
        depth += 1
    elif ch in ")>":
        depth -= 1
    if ch == "," and depth == 0:
        entries.append(buf)
        buf = ""
    else:
        buf += ch
entries.append(buf)

schema_columns = []
for entry in entries:
    entry = entry.strip()
    # Skips `id int PRIMARY KEY` and any standalone `PRIMARY KEY (...)` clause:
    # the expectation is over the NON-key columns, since the partition key is
    # asserted separately as the partition id.
    if not entry or "PRIMARY KEY" in entry.upper():
        continue
    schema_columns.append(entry.split()[0])

if len(schema_columns) < 2:
    die(
        "derived only %d non-PK column(s) %s from the CREATE TABLE for '%s' in %s; refusing to "
        "check the golden against an empty or near-empty expectation"
        % (len(schema_columns), schema_columns, table, schema)
    )
all_columns = set(schema_columns)

# ---- EXPECTED PARTITIONS: the generator's own insert_* declaration ----------
expected = {}
for item in spec_raw:
    if ":" not in item:
        die("malformed EXPECTED_PARTITIONS entry %r (want '<id>:ALL' or '<id>:<col>,<col>')" % item)
    pid, cols = item.split(":", 1)
    if cols == "ALL":
        expected[pid] = set(all_columns)
    else:
        want = set(c for c in cols.split(",") if c)
        unknown = sorted(want - all_columns)
        if unknown:
            die(
                "EXPECTED_PARTITIONS entry %r names column(s) the schema does not declare: %s"
                % (item, unknown)
            )
        expected[pid] = want
if not expected:
    die("EXPECTED_PARTITIONS is empty, so nothing would be asserted about the golden")

# ---- OBSERVED --------------------------------------------------------------
observed = {}
observed_ids = []
lines_read = 0
try:
    fh = open(jsonl, encoding="utf-8")
except OSError as exc:
    die("cannot read the JSONL golden %s: %s" % (jsonl, exc))
with fh:
    for raw in fh:
        lines_read += 1
        raw = raw.strip()
        if not raw:
            continue
        try:
            partition = json.loads(raw)
        except ValueError as exc:
            problems.append("%s line %d is not valid JSON: %s" % (jsonl, lines_read, exc))
            continue
        if not isinstance(partition, dict) or not isinstance(partition.get("partition"), dict):
            problems.append(
                "%s line %d is not an sstabledump partition object" % (jsonl, lines_read)
            )
            continue
        key = partition["partition"].get("key")
        if not isinstance(key, list) or not key:
            problems.append("%s line %d carries no partition key" % (jsonl, lines_read))
            continue
        pid = str(key[0])
        # The ORDERED list, kept alongside the set-valued map: reducing each
        # partition to a set of column names is what the column checks need, and
        # it is exactly what makes two lines carrying the SAME id merge into one
        # silently (roborev job 260). A duplicated partition is then invisible.
        observed_ids.append(pid)
        cells = observed.setdefault(pid, set())
        for row in partition.get("rows", []):
            for cell in row.get("cells", []):
                name = cell.get("name")
                if name:
                    cells.add(name)

if not observed:
    problems.append("%s holds ZERO partitions (%d line(s) read)" % (jsonl, lines_read))

# DUPLICATE partition ids. sstabledump emits one line per partition, so a repeat
# is a duplicated or spliced golden — and the set reduction above cannot see it,
# because the second occurrence merges into the first and the id/column checks
# below still pass. Asserted as "the observed id LIST is exactly the expected set
# with NO repeats", which is strictly stronger than comparing the two as sets.
duplicated = sorted(
    set(pid for pid in observed_ids if observed_ids.count(pid) > 1),
    key=lambda pid: (len(pid), pid),
)
if duplicated:
    problems.append(
        "%s holds DUPLICATE partition id(s) %s (%d partition line(s) for %d distinct id(s); "
        "counts: %s) — the golden repeats a partition, which merging by id hides"
        % (
            jsonl,
            duplicated,
            len(observed_ids),
            len(observed),
            ", ".join("%s x%d" % (pid, observed_ids.count(pid)) for pid in duplicated),
        )
    )

missing_ids = sorted(set(expected) - set(observed))
extra_ids = sorted(set(observed) - set(expected))
if missing_ids:
    problems.append(
        "%s is MISSING partition id(s) %s (expected exactly %s, found %s) — the golden holds a "
        "PARTIAL or WRONG dataset" % (jsonl, missing_ids, sorted(expected), sorted(observed))
    )
if extra_ids:
    problems.append(
        "%s holds UNEXPECTED partition id(s) %s (expected exactly %s) — the golden is not this "
        "fixture's dataset" % (jsonl, extra_ids, sorted(expected))
    )
for pid in sorted(set(expected) & set(observed)):
    absent = sorted(expected[pid] - observed[pid])
    unexpected = sorted(observed[pid] - expected[pid])
    if absent:
        problems.append(
            "%s partition id=%s is MISSING column(s) %s" % (jsonl, pid, absent)
        )
    if unexpected:
        problems.append(
            "%s partition id=%s carries UNEXPECTED column(s) %s (not declared by %s)"
            % (jsonl, pid, unexpected, schema)
        )

covered = set()
for names in observed.values():
    covered |= names
uncovered = sorted(all_columns - covered)
if uncovered:
    problems.append(
        "%s covers %d of %d schema column(s); NO partition carries %s"
        % (jsonl, len(covered & all_columns), len(all_columns), uncovered)
    )

if problems:
    for problem in problems:
        sys.stderr.write("[nuk][ERROR]   - %s\n" % problem)
    sys.exit(1)

print(
    "[nuk]   JSONL content OK: %d partition line(s), no duplicate id(s), partitions %s; "
    "all %d schema column(s) covered"
    % (len(observed_ids), sorted(observed), len(all_columns))
)
PY
}

# ----------------------------------------------------------------------------
# OUTCOME-BASED POST-CONDITION — the PRIMARY correctness contract of this script
# (roborev job 254)
#
# Six review findings across four rounds of this one script were the SAME SHAPE:
# a step could fail silently and the run still reported SUCCESS. The MECHANISMS
# all differed — `|| true`, `2>/dev/null`, a process substitution's discarded
# exit status, and printed staging commands that omitted deletions — and each
# round fixed the mechanism in front of it. That enumeration does not close:
# there is always another way for a step to fail quietly.
#
# So this function checks the OUTCOME instead of the failure modes: is the
# artifact set on disk EXACTLY what a complete generation produces? A silent
# failure by a mechanism nobody has thought of is then caught by THE ARTIFACTS
# BEING WRONG, rather than by someone having predicted the mechanism. It is the
# same move as replacing an enumerated blocklist with an allowlist.
#
# EVERY per-step check in this script — the removed `|| true`s, the unsuppressed
# stderr, the status-checked `find`s, the non-zero counters — is DEFENCE IN
# DEPTH, NOT the contract. Their value is naming the step that broke early and
# close to the cause; their absence would not let an incomplete artifact set pass,
# because this does.
#
# The expected set is DERIVED from $TABLES and from the Data.db actually present
# (never hard-coded to a prefix or a directory id); every artifact must exist AND
# be non-empty; and EVERY problem found is reported, not just the first.
#
# WHERE THIS BOTTOMS OUT, AND WHY IT IS HERE (roborev job 257)
#
# This is the THIRD iteration of one question — "may this run report COMPLETE?" —
# and each answer was too weak in the same way, so it is worth saying where the
# regress stops rather than leaving a fourth layer to be invented or this one to
# be removed as arbitrary:
#   * per-step status        — a step can fail by a mechanism nobody enumerated
#   * a non-zero COUNT       — "3 of 3" and "3 of 10, then failed" are one number
#   * a NON-EMPTY artifact   — non-emptiness is not completeness: the check
#                              passed with `Index.db`/`Statistics.db`/`Summary.db`
#                              deleted and with a partial golden
#   * COMPLETENESS AGAINST AN AUTHORITATIVE MANIFEST  <- here
#
# It bottoms out here because both halves now compare against a declaration made
# OUTSIDE this check:
#   * the component set comes from `*-TOC.txt`, which is CASSANDRA'S WRITER
#     declaring what it wrote (toc_components);
#   * the golden's content is compared against the fixture's known shape — the
#     schema file's own column list and this script's own insert_* partitions —
#     which is the SAME shape the pytest suite asserts (verify_jsonl_content).
# There is no further oracle to appeal to. The next level up would be
# re-deriving Cassandra's output FROM Cassandra, and that is precisely what the
# committed fixture IS. So a fourth layer would have nothing new to consult.
#
# Exercisable on its own with `--verify-only`, which is how it is RED-verified.
# ----------------------------------------------------------------------------
verify_generated_artifacts() {
  local sstables_dir="$1"
  local ks_dir="$sstables_dir/$KEYSPACE"
  local -a problems=()
  local tables_checked=0
  local table

  log "=== POST-CONDITION: verifying the artifact set under $ks_dir ==="

  if [[ ! -d "$ks_dir" ]]; then
    fail "POST-CONDITION FAILED: keyspace directory '$ks_dir' does not exist — nothing was generated."
  fi

  for table in "${TABLES[@]}"; do
    list_table_dirs "$table"
    if [[ "${#TABLE_DIRS[@]}" -ne 1 ]]; then
      local found_desc="none"
      if [[ "${#TABLE_DIRS[@]}" -gt 0 ]]; then
        found_desc="${TABLE_DIRS[*]}"
      fi
      problems+=("$table: expected exactly ONE table directory matching '$table-*' under \
$ks_dir, found ${#TABLE_DIRS[@]} ($found_desc)")
      continue
    fi
    local tdir="${TABLE_DIRS[0]}"

    ensure_work_tmp
    local data_list="$WORK_TMP/postcond-data-$table.list"
    collect_matching_files "$tdir" "$data_list" \
      -type f -name "*-Data.db" -not -name "._*"
    local -a data_files=()
    local f
    while IFS= read -r -d '' f; do data_files+=("$f"); done < "$data_list"
    if [[ "${#data_files[@]}" -ne 1 ]]; then
      problems+=("$table: expected exactly ONE *-Data.db in $tdir, found ${#data_files[@]}")
      continue
    fi
    local data_file="${data_files[0]}"
    if [[ ! -s "$data_file" ]]; then
      problems+=("$table: EMPTY artifact $data_file")
    fi

    local prefix
    prefix="$(basename -- "$data_file")"
    prefix="${prefix%-Data.db}"
    local problems_before="${#problems[@]}"

    # --- CASSANDRA'S components: TOC.txt manifest UNION the mandatory FLOOR ---
    # The floor is seeded FIRST and unconditionally, so it is required even when
    # the derivation fails: a damaged manifest must not be able to shrink the
    # required set (roborev job 260). This is a MINIMUM, not the curated list the
    # round-8 finding removed — the union keeps every component TOC.txt declares,
    # including ones the floor deliberately omits as configuration-dependent
    # (`Filter.db`), and still demands nothing a correct uncompressed fixture
    # lacks (`CompressionInfo.db` is in neither).
    local -a required_suffixes=()
    local component floor_component toc_ok=0 already seen_suffix
    for floor_component in "${MANDATORY_TOC_COMPONENTS[@]}"; do
      required_suffixes+=("-$floor_component")
    done
    local floor_count="${#required_suffixes[@]}"
    if toc_components "$tdir/$prefix-TOC.txt"; then
      toc_ok=1
      for component in "${TOC_COMPONENTS[@]}"; do
        already=0
        for seen_suffix in "${required_suffixes[@]}"; do
          if [[ "$seen_suffix" == "-$component" ]]; then
            already=1
          fi
        done
        if [[ "$already" -ne 1 ]]; then
          required_suffixes+=("-$component")
        fi
      done
    else
      problems+=("$table: could not derive the required component set from \
$tdir/$prefix-TOC.txt (see above). Refusing to fall back to a curated list, which is what let \
missing components pass; the ${floor_count}-component mandatory FLOOR is still required below, but a \
manifest this script cannot trust is itself the failure.")
    fi
    # --- OUR OWN derived goldens: in no TOC.txt, so declared by this script --
    required_suffixes+=("${DERIVED_ARTIFACT_SUFFIXES[@]}")

    local suffix artifact
    for suffix in "${required_suffixes[@]}"; do
      artifact="$tdir/$prefix$suffix"
      if [[ ! -f "$artifact" ]]; then
        problems+=("$table: MISSING artifact $artifact")
      elif [[ ! -s "$artifact" ]]; then
        problems+=("$table: EMPTY artifact $artifact")
      fi
    done

    # references.yml may not advertise an artifact this post-condition does not
    # REQUIRE, or the manifest and the contract disagree about the set. Checked
    # here rather than in update_references_yml because this is where the
    # TOC-derived half of the required set is known.
    # Only when the TOC-derived half of the required set is actually KNOWN: with a
    # failed derivation this check has no subject, and running it anyway blamed
    # references.yml for a damaged TOC.txt — the right verdict for the wrong
    # stated reason. The derivation failure above is already a problem.
    local ref_suffix ref_ok
    if [[ "$toc_ok" -eq 1 ]]; then
      for ref_suffix in "${REFERENCED_SIDECAR_SUFFIXES[@]}"; do
        ref_ok=0
        for suffix in "${required_suffixes[@]}"; do
          if [[ "$ref_suffix" == "$suffix" ]]; then
            ref_ok=1
          fi
        done
        if [[ "$ref_ok" -ne 1 ]]; then
          problems+=("$table: references.yml advertises '$prefix$ref_suffix', which is neither a \
TOC.txt component nor one of this script's derived goldens (${DERIVED_ARTIFACT_SUFFIXES[*]}) — the \
manifest and this post-condition disagree about the artifact set")
        fi
      done
    fi

    # --- CONTENT of the JSONL golden, not just its size ---------------------
    # Skipped only when the golden is absent or empty, which the artifact loop
    # above has already recorded as a problem — never silently.
    local jsonl="$tdir/$prefix-Data.db.jsonl"
    if [[ -s "$jsonl" ]]; then
      if ! verify_jsonl_content "$jsonl" "$table"; then
        problems+=("$table: the JSONL golden $jsonl does not hold this fixture's dataset \
(itemized above) — it parses and is non-empty, which is exactly what a size check cannot see")
      fi
    fi

    # Junk the staging command would otherwise force-add into the fixture.
    local junk_list="$WORK_TMP/postcond-junk-$table.list"
    collect_matching_files "$tdir" "$junk_list" \
      \( -name '._*' -o -name '.DS_Store' \)
    local -a junk=()
    while IFS= read -r -d '' f; do junk+=("$f"); done < "$junk_list"
    if [[ "${#junk[@]}" -ne 0 ]]; then
      problems+=("$table: ${#junk[@]} junk file(s) present that the staging command \
would commit: ${junk[*]}")
    fi

    tables_checked=$((tables_checked + 1))
    if [[ "${#problems[@]}" -eq "$problems_before" ]]; then
      log "  $table: $tdir (prefix $prefix) — ${#required_suffixes[@]} required artifact(s) \
($(( ${#required_suffixes[@]} - ${#DERIVED_ARTIFACT_SUFFIXES[@]} )) components = \
${#TOC_COMPONENTS[@]} declared by TOC.txt UNION $floor_count mandatory floor, \
+ ${#DERIVED_ARTIFACT_SUFFIXES[@]} derived by this script) present and non-empty; JSONL content \
matches"
    else
      log "  $table: $tdir (prefix $prefix) — $(( ${#problems[@]} - problems_before )) problem(s) \
found (itemized below)"
    fi
  done

  # The post-condition must have had a SUBJECT. An empty $TABLES, or every table
  # short-circuiting above, must never read as a pass.
  if [[ "$tables_checked" -eq 0 && "${#problems[@]}" -eq 0 ]]; then
    problems+=("the post-condition verified ZERO tables (TABLES=${TABLES[*]}) — nothing \
was measured, so nothing may be reported complete")
  fi

  if [[ "${#problems[@]}" -ne 0 ]]; then
    echo "[nuk][ERROR] POST-CONDITION FAILED: the artifact set under $ks_dir is not what a \
COMPLETE generation produces." >&2
    local problem
    for problem in "${problems[@]}"; do
      echo "[nuk][ERROR]   - $problem" >&2
    done
    fail "${#problems[@]} artifact problem(s) listed above. Commit NOTHING from this run."
  fi

  log "POST-CONDITION OK: $tables_checked table(s) verified; every component TOC.txt declares \
UNION the ${#MANDATORY_TOC_COMPONENTS[@]} Cassandra 5.0 mandatory ones is present and non-empty, \
and the JSONL golden holds this fixture's dataset with no duplicated partition."
}

# ----------------------------------------------------------------------------
# references.yml rewrite (roborev job 254, F2)
#
# WHY THE SCRIPT DOES THIS instead of printing a reminder: every regeneration
# mints a NEW Cassandra table UUID, so the fixture DIRECTORY NAME changes and the
# previous directory is deleted. A manifest left untouched keeps selecting the
# PREVIOUS generation, which no longer exists, and the fixture reads as absent.
# Issue #3500's fixture directory id changed THREE times and stayed consistent
# only because a human remembered to hand-edit the manifest each time — an
# instruction a human must not forget is not a mechanism.
#
# Rewrites the single `- keyspace: $KEYSPACE` entry in place (appending it if it
# is absent), then AFFIRMATIVELY re-reads the file to assert the new directory is
# named and that no line still references a different directory under this
# keyspace.
# ----------------------------------------------------------------------------
update_references_yml() {
  local table_dir="$1" prefix="$2"
  local dirname
  dirname="$(basename -- "$table_dir")"

  local canonical_datasets
  if ! canonical_datasets="$(resolve_physical "$ROOT/datasets")"; then
    fail "could not resolve '$ROOT/datasets'; refusing to update references.yml blind."
  fi
  if [[ "$OUT_DIR" != "$canonical_datasets" ]]; then
    log "references.yml NOT updated: output went to '$OUT_DIR', not this repository's own \
'$canonical_datasets', so this run's fixture is not the committed one."
    return 0
  fi
  if [[ ! -f "$REFERENCES_YML" ]]; then
    fail "references.yml not found at '$REFERENCES_YML'; refusing to report a complete \
generation with an un-updated manifest."
  fi

  log "Updating $REFERENCES_YML -> $dirname (prefix $prefix)"
  if ! NUK_YML="$REFERENCES_YML" \
       NUK_KS="$KEYSPACE" \
       NUK_TABLE="${TABLES[0]}" \
       NUK_DIRNAME="$dirname" \
       NUK_PREFIX="$prefix" \
       NUK_SIDECARS="${REFERENCED_SIDECAR_SUFFIXES[*]}" \
       python3 - <<'PY'
import datetime
import os
import sys

yml = os.environ["NUK_YML"]
ks = os.environ["NUK_KS"]
table = os.environ["NUK_TABLE"]
dirname = os.environ["NUK_DIRNAME"]
prefix = os.environ["NUK_PREFIX"]
required = os.environ["NUK_SIDECARS"].split()

rel = f"test-data/datasets/sstables/{ks}/{dirname}"
ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# The manifest field name for each artifact suffix. Every suffix in
# REFERENCED_SIDECAR_SUFFIXES must appear here, so advertising one without
# teaching the manifest a field name for it is an ERROR rather than a silent
# omission. (The reverse coupling — that everything the manifest advertises is
# also REQUIRED by the post-condition — is asserted in
# verify_generated_artifacts, which is where the derived TOC set is known.)
FIELD_FOR_SUFFIX = {
    "-Data.db.jsonl": "data_jsonl",
    "-Statistics.db.txt": "statistics_txt",
    "-TOC.txt": "toc_txt",
    "-Digest.crc32": "digest_crc32",
}
unknown = [s for s in required if s not in FIELD_FOR_SUFFIX]
if unknown:
    sys.exit(
        "references.yml rewrite does not know a manifest field for required "
        f"artifact suffix(es): {unknown}. Teach FIELD_FOR_SUFFIX about them."
    )

# `present: true` for everything the post-condition requires, plus the one
# artifact this fixture deliberately does NOT ship.
entries = [(FIELD_FOR_SUFFIX[s], s, "true") for s in required]
entries.append(("summary_txt", "-Summary.db.txt", "false"))

# Paths are REPO-RELATIVE here, unlike the generator-machine absolute paths of
# the older entries: the only consumer,
# dataset_helpers::resolve_table_dir_via_manifest, takes file_name() of
# sstable_dir and re-roots it under the caller's datasets root, and the
# references.*.path fields are not deserialized at all. Relative keeps the entry
# correct after the generating worktree is merged and gone (issue #3500).
block = [
    f"  - keyspace: {ks}\n",
    f"    table: {table}\n",
    f'    sstable_dir: "{rel}"\n',
    f"    prefix: {prefix}\n",
    "    references:\n",
]
for field, suffix, present in entries:
    block += [
        f"      {field}:\n",
        f"        present: {present}\n",
        f'        path: "{rel}/{prefix}{suffix}"\n',
    ]
block.append(f"    generated_at: {ts}\n")

with open(yml, encoding="utf-8") as fh:
    lines = fh.readlines()

start = None
for i, line in enumerate(lines):
    if line.rstrip("\n") == f"  - keyspace: {ks}":
        start = i
        break

if start is None:
    if lines and not lines[-1].endswith("\n"):
        lines[-1] += "\n"
    lines.extend(block)
    action = "appended"
else:
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("  - "):
            end = j
            break
    lines[start:end] = block
    action = "replaced"

with open(yml, "w", encoding="utf-8") as fh:
    fh.writelines(lines)

# AFFIRMATIVE re-read: the new directory must be named, and NO surviving line may
# reference a DIFFERENT directory under this keyspace — that is exactly the
# two-generations state this fix is about.
after = open(yml, encoding="utf-8").read()
if f'sstable_dir: "{rel}"' not in after:
    sys.exit(f"references.yml does not name the new sstable_dir after the {action}: {rel}")
stale = [ln for ln in after.splitlines() if f"sstables/{ks}/" in ln and dirname not in ln]
if stale:
    sys.exit(
        f"references.yml still references a DIFFERENT directory under {ks}:\n  "
        + "\n  ".join(stale)
    )
print(f"[nuk] references.yml entry {action}: {ks}.{table} -> {dirname} (prefix {prefix})")
PY
  then
    fail "failed to update '$REFERENCES_YML' for $dirname (see the error above). The \
manifest would keep selecting a directory this run deleted."
  fi
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

# `--verify-only`: run ONLY the outcome-based post-condition against an existing
# fixture tree, then exit. It starts no container, needs no engine and mutates
# nothing — which is what makes the post-condition independently exercisable, and
# RED-verifiable against a deliberately damaged COPY of the tree
# (`--out <copy>/datasets --verify-only`).
if [[ "$VERIFY_ONLY" -eq 1 ]]; then
  log "--verify-only: running the post-condition only (no container, no generation)"
  verify_generated_artifacts "$SSTABLES_DIR"
  log "=== --verify-only COMPLETE ==="
  exit 0
fi

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

apply_schema "$SCHEMA_FILE"

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

  # ONE flush => exactly ONE table directory and exactly ONE Data.db per table.
  # This is an EARLY locator (it fails next to the export that produced the
  # problem); the authoritative check is verify_generated_artifacts at the end.
  for table in "${TABLES[@]}"; do
    list_table_dirs "$table"
    if [[ "${#TABLE_DIRS[@]}" -ne 1 ]]; then
      fail "$table: expected exactly ONE table directory under \
$SSTABLES_DIR/$KEYSPACE/ matching '$table-*', found ${#TABLE_DIRS[@]}; export failed"
    fi
    # Materialized with find's status CHECKED, for the same reason as the two
    # loops below (roborev job 254, F1): the previous `find … | wc -l` form put
    # find's status behind an assignment, which always succeeds.
    ensure_work_tmp
    one_data_list="$WORK_TMP/one-data-$table.list"
    collect_matching_files "${TABLE_DIRS[0]}" "$one_data_list" \
      -type f -name "*-Data.db" -not -name "._*"
    cnt=0
    while IFS= read -r -d '' _f; do cnt=$((cnt + 1)); done < "$one_data_list"
    if [[ "$cnt" -ne 1 ]]; then
      fail "$table: expected exactly ONE flushed Data.db, found $cnt."
    fi
    log "  $table: exactly one table directory and one Data.db (OK)"
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
  # Same materialization as the JSONL loop: find's status is checked BEFORE the
  # loop, because a process substitution discards it (roborev job 254, F1).
  ensure_work_tmp
  stats_data_list="$WORK_TMP/stats-data-files.list"
  collect_matching_files "$SSTABLES_DIR/$KEYSPACE" "$stats_data_list" \
    -type f -name "*-Data.db" -not -name "._*"
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
  done < "$stats_data_list"
  # Subject check, not a completeness check — see the JSONL loop's note.
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
  if ! find "$_cleanup_root" \( -name '._*' -o -name '.DS_Store' \) -delete; then
    fail "junk sweep under '$_cleanup_root' exited NON-ZERO; junk files may remain \
inside the fixture directory and would be picked up by the staging command below."
  fi

  # THE CONTRACT. Everything above is defence in depth; this is what decides
  # whether the run may report COMPLETE (roborev job 254).
  verify_generated_artifacts "$SSTABLES_DIR"

  # The post-condition just guaranteed exactly one table directory holding exactly
  # one Data.db, so the fixture directory and SSTable prefix are unambiguous.
  list_table_dirs "${TABLES[0]}"
  FIXTURE_DIR="${TABLE_DIRS[0]}"
  ensure_work_tmp
  collect_matching_files "$FIXTURE_DIR" "$WORK_TMP/manifest-data.list" \
    -type f -name "*-Data.db" -not -name "._*"
  FIXTURE_PREFIX=""
  while IFS= read -r -d '' _data_file; do
    FIXTURE_PREFIX="$(basename -- "$_data_file")"
    FIXTURE_PREFIX="${FIXTURE_PREFIX%-Data.db}"
  done < "$WORK_TMP/manifest-data.list"
  [[ -n "$FIXTURE_PREFIX" ]] \
    || fail "could not derive the SSTable prefix from '$FIXTURE_DIR'."

  update_references_yml "$FIXTURE_DIR" "$FIXTURE_PREFIX"

  log "=== $KEYSPACE generation COMPLETE ==="
  log "SSTables: $FIXTURE_DIR"

  echo ""
  echo "=============================================================="
  echo "  NEXT: commit the generated fixture"
  echo "=============================================================="
  echo ""
  echo "  # Stage the WHOLE keyspace path, INCLUDING DELETIONS:"
  echo "  #   -A  stages the REMOVAL of the previous generation's directory."
  echo "  #       Every regeneration mints a new Cassandra table UUID, so the old"
  echo "  #       directory is gone; a command that adds only CURRENT files leaves"
  echo "  #       that deletion unstaged and commits BOTH generations."
  echo "  #   -f  forces the gitignored *.db binaries."
  echo "  git -C '$REPO_ROOT' add -f -A -- test-data/datasets/sstables/$KEYSPACE"
  echo ""
  echo "  # references.yml has ALREADY been rewritten in place by this script to"
  echo "  # name $(basename -- "$FIXTURE_DIR") / $FIXTURE_PREFIX — stage it in the SAME commit,"
  echo "  # or the manifest will keep selecting a directory that no longer exists:"
  echo "  git -C '$REPO_ROOT' add -- test-data/datasets/references.yml"
  echo ""
  echo "  # Confirm what is staged: the NEW directory added, any PREVIOUS"
  echo "  # generation's files deleted, references.yml modified, nothing else."
  echo "  git -C '$REPO_ROOT' status --short -- test-data/datasets"
  echo ""
  echo "  # Commit:"
  echo "  git -C '$REPO_ROOT' commit -m 'test(#3500): nested-UDT hashable-position fixture SSTables'"
  echo "=============================================================="
fi
