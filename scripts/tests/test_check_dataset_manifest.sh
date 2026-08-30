#!/usr/bin/env bash
# Self-test for test-data/scripts/check-dataset-manifest.sh (issue #3493).
#
# THE QUESTION THIS CHECKER ANSWERS, and why #3522's census does not answer it:
# that census asks "does every test FILE run"; this asks "is the CORPUS those files read
# actually complete". The Node parity cases DERIVE their table set FROM DISK, so a partial
# extraction is green BY OMISSION — every suite runs, every suite does real work, and the
# missing tables are simply never enumerated. #3522's per-suite guard cannot see it.
#
# Measured against the real node binding, on an otherwise intact generation:
#   * a zero-length CompressionInfo.db or Statistics.db -> SELECT returns 0 ROWS, silently;
#   * a second generation whose Data.db is well-formed garbage -> the reader THROWS.
#
# Dataset-free and network-free: every case builds its own synthetic corpus. The few that
# need a real one guard on CQLITE_DATASETS_ROOT and report INFO when it is absent.
#
# Run standalone:   bash scripts/tests/test_check_dataset_manifest.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

# #2751 defense-in-depth: this self-test drives nested `agent-gate.sh --only
# node-bindings` runs. Each case pins its own AGENT_GATE_SUMMARY_FILE, but scrub
# any inherited value up front so a standalone run can never clobber the caller's.
unset AGENT_GATE_SUMMARY_FILE
# The component's strictness is decided from these, so a value inherited from the
# invoking shell must not steer the case.
unset CQLITE_REQUIRE_FIXTURES CQLITE_PARITY_REQUIRE_DATASETS

# NEVER pipe a gate invocation straight into `grep -q` under `set -o pipefail`.
# `grep -q` exits on its FIRST match, the gate then dies of SIGPIPE, and pipefail
# propagates that -- so a MATCHING pattern yields a FAILING `if`. It is timing
# dependent (small outputs flush before grep exits, so it passes standalone and fails
# under load), which is exactly the kind of false verdict this suite exists to catch.
# Capture into a variable first, then match.
# EVERY synthetic Data.db below is created with its `.jsonl` golden sibling. Since round
# 21 a Data.db without a golden does not satisfy a table, so a fixture missing one would
# be rejected for THAT reason -- silently turning cases about globs, UUIDs, OA format,
# symlinks or tracking into cases about goldens, several of which would still have
# "passed". Case 32 is the exception and manages its golden explicitly, because the
# golden IS its subject.
PASS=0; FAIL=0
ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

# This script deliberately runs WITHOUT `set -e` (every case must report its own verdict
# rather than abort the suite), so a failed `mktemp -d` would leave WORK empty and every
# `mkdir -p "$WORK/..."` below would write to ABSOLUTE paths at the filesystem root --
# `/datasets`, `/stub-bin` -- and the EXIT trap would then `rm -rf ""`. Checked
# explicitly, before the trap is installed (roborev #3493 round 14, Medium).
WORK=$(mktemp -d) || { echo "FAIL: mktemp -d failed; refusing to run with an unset work dir" >&2; exit 1; }
[ -n "$WORK" ] && [ -d "$WORK" ] || {
  echo "FAIL: mktemp -d produced no usable directory (got '${WORK:-<empty>}')" >&2; exit 1; }
# A scratch git worktree is REGISTERED IN REPOSITORY METADATA, so deleting its directory
# is not cleanup -- an interrupt between `worktree add` and `worktree remove` leaves a
# registered-but-missing worktree behind in the real repo (roborev #3493 round 25).
# Tracked here and unregistered from the trap, which also covers INT/TERM.
SCRATCH_WORKTREE=""
SCRATCH_WORKTREE_REPO=""
_cleanup() {
  # NO repository-wide `worktree prune` (roborev #3493 round 27). Pruning unregisters
  # every worktree git currently considers stale -- including a PEER LANE's whose
  # directory is temporarily unavailable -- so a cleanup for one scratch tree could
  # deregister someone else's work. This machine runs several lanes concurrently, so that
  # is a live hazard, not a theoretical one. Remove only what this script registered, and
  # SAY SO if that fails rather than reaching for a broader hammer.
  # IDEMPOTENT (roborev #3493 round 29): the INT/TERM handlers call _cleanup and then
  # re-raise, which runs the EXIT trap and calls _cleanup a SECOND time. Without clearing
  # the tracking on success, that second pass tried to remove an already-removed worktree
  # and printed a false "could not remove" warning -- a cleanup path inventing a problem
  # it had just fixed.
  if [ -n "$SCRATCH_WORKTREE" ] && [ -n "$SCRATCH_WORKTREE_REPO" ]; then
    if git -C "$SCRATCH_WORKTREE_REPO" worktree remove --force "$SCRATCH_WORKTREE" >/dev/null 2>&1; then
      SCRATCH_WORKTREE=""; SCRATCH_WORKTREE_REPO=""
    else
      echo "WARN: could not remove scratch worktree $SCRATCH_WORKTREE (left registered; run 'git worktree prune' by hand if it is stale)" >&2
    fi
  fi
  rm -rf "$WORK"
}
# INT/TERM get their OWN traps that clean up and then EXIT with the conventional
# 128+signal status. Sharing the EXIT handler was wrong: a trap handler RETURNS, so on
# Ctrl-C the script deleted $WORK and then carried on running cases against a directory
# that no longer existed -- cascading failures, and expensive nested gate invocations
# continuing after the operator cancelled (roborev #3493 round 26).
trap _cleanup EXIT
trap '_cleanup; trap - INT;  kill -INT  $$' INT
trap '_cleanup; trap - TERM; kill -TERM $$' TERM

# A corpus root that is NON-EMPTY but lacks the canonical `test_basic` keyspace.
ROOT="$WORK/datasets"
mkdir -p "$ROOT/sstables/test_other"
printf 'x\n' > "$ROOT/sstables/test_other/aa-1-big-Data.db"
printf 'row\n' > "$ROOT/sstables/test_other/aa-1-big-Data.db.jsonl"

# Fast stubs so the negative case does not build the napi module. Only reached
# when the component does NOT take the opt-out branch -- which is the assertion.
STUB="$WORK/stub-bin"
mkdir -p "$STUB"
for tool in node npx; do
  printf '#!/bin/sh\nexit 0\n' > "$STUB/$tool"
  chmod +x "$STUB/$tool"
done
# The npm stub fails `npm run build` iff STUB_FAIL_BUILD=1, so case 3 can prove the
# corpus-free half is genuinely ENFORCED under the opt-out rather than merely run.
cat > "$STUB/npm" <<'STUBEOF'
#!/bin/sh
# Record the environment the component actually handed us, so the schemas-root
# plumbing (#3493) is asserted from what was PASSED, not from reading the source.
[ -n "${STUB_ENV_DUMP:-}" ] && env > "$STUB_ENV_DUMP"
if [ "${STUB_FAIL_BUILD:-0}" = 1 ] && [ "$1" = "run" ] && [ "$2" = "build" ]; then
  echo "stub npm: simulated build failure" >&2
  exit 1
fi
exit 0
STUBEOF
chmod +x "$STUB/npm"

# A copy of the manifest OUTSIDE any git work tree. The committed-table-dir rule
# (round 18) correctly rejects synthetic UUIDs, which would make every OTHER manifest
# case below fail for the WRONG reason -- passing or failing on a property it is not
# testing. Running those through the git-less copy triggers the documented fallback
# ("nothing tracked -> everything counts"), isolating each case to its own property.
# Case 28 uses the REAL script, because the committed rule IS its subject.
node_pkg="$(cd "$(dirname "$GATE")/.." && pwd)/bindings/node"

# _autotoc <corpus-root> -- give every REGULAR-FILE `*-Data.db` under <corpus-root> a
# minimal self-consistent sibling `TOC.txt`, unless one already exists.
#
# Round 47 made the manifest validate the accepted generation's TOC-listed components,
# because a partial extraction that drops a `CompressionInfo.db` otherwise reports the
# corpus complete. Real Cassandra output always carries a TOC (measured: 144/144
# generations across both corpus roots), but this suite's SYNTHETIC fixtures were written
# before the rule existed and 26 accept-cases had none -- they would now fail for a reason
# the case is not about.
#
# So the ACCEPT-path fixtures get a TOC generated here rather than 26 hand-edits, keeping
# each case's assertion about the property it was written for. Cases that are ABOUT the
# TOC opt out with `NO_AUTO_TOC=1` and build their own.
#
# Deliberately minimal (`Data.db` + `TOC.txt`): a TOC lists only what the generation has,
# so a fixture with no companions has a two-line one. Never overwrites an existing TOC.
_autotoc() {
  [ "${NO_AUTO_TOC:-0}" = 1 ] && return 0
  [ -d "$1" ] || return 0
  # NEVER mutate a root outside this suite's scratch dir. Several cases pass the REAL
  # shared corpus root ($CQLITE_DATASETS_ROOT, often a machine-local /data/datasets used
  # concurrently by other lanes); generating files there would be a cross-lane side
  # effect. Real Cassandra output already has a TOC, so those roots need nothing.
  case "$1" in "$WORK"/*|"$WORK") : ;; *) return 0 ;; esac
  local _d
  while IFS= read -r _d; do
    [ -f "$_d" ] || continue                     # a dir/dangling-symlink fixture gets none
    [ -e "${_d%Data.db}TOC.txt" ] && continue
    printf 'Data.db\nTOC.txt\n' > "${_d%Data.db}TOC.txt"
  done < <(find "$1" -name '*-Data.db' 2>/dev/null)
  return 0    # the loop's rc is the last `[ -e ]` test; never leak it as failure
}

# mrun <corpus-root> [manifest-args...] -- _autotoc the root, then run the manifest on it.
# Every call site goes through this so the TOC rule cannot silently invalidate a case.
mrun() { local _r=$1; shift; _autotoc "$_r"; bash "$MANIFEST_NOGIT" "$_r" "$@"; }

UUID=4b892c6064e711f1bd3ac7dbf655c673
MANIFEST_SRC="$(cd "$(dirname "$GATE")/.." && pwd)/test-data/scripts/check-dataset-manifest.sh"
MANIFEST_NOGIT=""
if [ -f "$MANIFEST_SRC" ]; then
  mkdir -p "$WORK/nogit-manifest"
  cp "$MANIFEST_SRC" "$WORK/nogit-manifest/check-dataset-manifest.sh"
  MANIFEST_NOGIT="$WORK/nogit-manifest/check-dataset-manifest.sh"
fi


# Hidden-hook helpers, defined HERE rather than beside their first use: bash resolves
# functions at CALL time, so a definition inside a later case is simply not available to
# an earlier one -- which is how three retargeted cases came to invoke an undefined `pf`
# and report the opposite verdict.

# ---------------------------------------------------------------------------
manifest="$(cd "$(dirname "$GATE")/.." && pwd)/test-data/scripts/check-dataset-manifest.sh"
if [ -f "$manifest" ]; then
  # A DIRECTORY named *-Data.db must NOT satisfy the completeness check.
  #
  # Asserted on the PER-TABLE message, not the exit code: the script fails on ANY
  # incomplete corpus, so a synthetic root's non-zero exit proves nothing about the
  # predicate -- a bare red is not evidence. Under the old name-only match the
  # directory COUNTED, so `counters` was NOT reported missing; under the shared
  # predicate it does not count, so it IS.
  fake="$WORK/manifest-fake"
  mkdir -p "$fake/sstables/test_basic/counters-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db"
  fake_out=$(mrun "$fake" 2>&1 || true)
  if printf '%s' "$fake_out" | grep -q 'missing Data.db for expected table: test_basic/counters'; then
    ok "manifest check rejects a DIRECTORY named *-Data.db (matches the shared predicate)"
  else
    bad "manifest check accepted a DIRECTORY named *-Data.db as a fixture for test_basic/counters"
  fi
  # Control: the SAME shape as a regular file must be accepted, so the assertion above
  # is about the file TYPE and not about the path pattern.
  real_fake="$WORK/manifest-real"
  mkdir -p "$real_fake/sstables/test_basic/counters-4b892c6064e711f1bd3ac7dbf655c673"
  printf 'x\n' > "$real_fake/sstables/test_basic/counters-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db"
  printf 'row\n' > "$real_fake/sstables/test_basic/counters-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db.jsonl"
  real_out=$(mrun "$real_fake" 2>&1 || true)
  if printf '%s' "$real_out" | grep -q 'missing Data.db for expected table: test_basic/counters'; then
    bad "manifest check rejected a REGULAR FILE fixture for test_basic/counters"
  else
    ok "manifest check accepts the same path as a REGULAR FILE (type, not pattern)"
  fi
  # Positive control on the REAL corpus, so the check above is not rejecting everything.
  if [ -n "${CQLITE_DATASETS_ROOT:-}" ] && [ -d "${CQLITE_DATASETS_ROOT:-}/sstables" ]; then
    if bash "$manifest" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1; then
      ok "manifest check still passes on the real corpus (predicate not over-tightened)"
    else
      bad "manifest check now fails on the real corpus"
    fi
  else
    echo "INFO: no CQLITE_DATASETS_ROOT corpus; skipping the manifest positive control"
  fi
else
  echo "INFO: $manifest absent; skipping the manifest predicate cases"
fi

# ---------------------------------------------------------------------------
if [ -f "$manifest" ]; then
  # The colliding sibling must be in the SAME KEYSPACE as the expected table, because
  # the search is rooted at the keyspace directory. My first cut of this case used
  # `counters` (test_basic) against `time_bucketed_counters` (test_timeseries) -- which
  # is where the finding's example pointed -- and it passed against the UNANCHORED glob,
  # proving nothing: different roots can never collide. `collection_table` is expected in
  # test_collections, so a `frozen_collection_table-*` sibling THERE is the real shape.
  overlap="$WORK/manifest-overlap"
  mkdir -p "$overlap/sstables/test_collections/frozen_collection_table-4b892c6064e711f1bd3ac7dbf655c673"
  printf 'x\n' > "$overlap/sstables/test_collections/frozen_collection_table-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db"
  printf 'row\n' > "$overlap/sstables/test_collections/frozen_collection_table-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db.jsonl"
  ov_out=$(mrun "$overlap" 2>&1 || true)
  if printf '%s' "$ov_out" | grep -q 'missing Data.db for expected table: test_collections/collection_table'; then
    ok "a same-keyspace sibling whose name CONTAINS the table does not satisfy it (glob anchored)"
  else
    bad "frozen_collection_table satisfied the expectation for the distinct table collection_table"
  fi
  # Control: the correctly-named directory DOES satisfy it, so the anchoring is not
  # simply rejecting everything.
  mkdir -p "$overlap/sstables/test_collections/collection_table-4b892c6064e711f1bd3ac7dbf655c673"
  printf 'x\n' > "$overlap/sstables/test_collections/collection_table-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db"
  printf 'row\n' > "$overlap/sstables/test_collections/collection_table-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db.jsonl"
  ov_out2=$(mrun "$overlap" 2>&1 || true)
  if printf '%s' "$ov_out2" | grep -q 'missing Data.db for expected table: test_collections/collection_table'; then
    bad "the correctly-named collection_table- directory did not satisfy its expectation"
  else
    ok "the correctly-named table directory still satisfies its expectation"
  fi
fi

# ---------------------------------------------------------------------------
if [ -f "$manifest" ]; then
  nested="$WORK/manifest-nested"
  mkdir -p "$nested/sstables/test_basic/other/counters-4b892c6064e711f1bd3ac7dbf655c673"
  printf 'x\n' > "$nested/sstables/test_basic/other/counters-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db"
  printf 'row\n' > "$nested/sstables/test_basic/other/counters-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db.jsonl"
  ns_out=$(mrun "$nested" 2>&1 || true)
  if printf '%s' "$ns_out" | grep -q 'missing Data.db for expected table: test_basic/counters'; then
    ok "a table directory nested below the keyspace does not satisfy it (depth anchored)"
  else
    bad "a nested other/counters-* directory satisfied the expectation for test_basic/counters"
  fi
fi

# ---------------------------------------------------------------------------
if [ -f "$manifest" ]; then
  oafake="$WORK/manifest-oa"
  mkdir -p "$oafake/sstables/test_oa/simple_table-4b892c6064e711f1bd3ac7dbf655c673"
  printf 'x\n' > "$oafake/sstables/test_oa/simple_table-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db"   # generic, NOT oa-
  printf 'row\n' > "$oafake/sstables/test_oa/simple_table-4b892c6064e711f1bd3ac7dbf655c673/nb-1-big-Data.db.jsonl"
  oa_out=$(mrun "$oafake" 2>&1 || true)
  if printf '%s' "$oa_out" | grep -q 'no OA-format Data.db.*test_oa/simple_table'; then
    ok "a generic *-Data.db does not satisfy a test_oa table (OA format required)"
  else
    bad "a generic *-Data.db satisfied test_oa/simple_table"
  fi
  # Control: the OA-named file DOES satisfy it.
  printf 'x\n' > "$oafake/sstables/test_oa/simple_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-Data.db"
  printf 'row\n' > "$oafake/sstables/test_oa/simple_table-4b892c6064e711f1bd3ac7dbf655c673/oa-2-big-Data.db.jsonl"
  oa_out2=$(mrun "$oafake" 2>&1 || true)
  if printf '%s' "$oa_out2" | grep -q 'test_oa/simple_table'; then
    bad "an oa-<n>-big-Data.db did not satisfy test_oa/simple_table"
  else
    ok "an oa-<n>-big-Data.db satisfies the same table (check is format, not blanket)"
  fi
fi

# ---------------------------------------------------------------------------
UUID=4b892c6064e711f1bd3ac7dbf655c673
if [ -f "$manifest" ]; then
  badu="$WORK/manifest-baduuid"
  mkdir -p "$badu/sstables/test_collections/collection_table-abc"
  printf 'x\n' > "$badu/sstables/test_collections/collection_table-abc/nb-1-big-Data.db"
  printf 'row\n' > "$badu/sstables/test_collections/collection_table-abc/nb-1-big-Data.db.jsonl"
  bu_out=$(mrun "$badu" 2>&1 || true)
  if printf '%s' "$bu_out" | grep -q 'missing Data.db for expected table: test_collections/collection_table'; then
    ok "a non-UUID table-directory suffix does not satisfy the table (matches TABLE_DIR_RE)"
  else
    bad "collection_table-abc satisfied the expectation Jest would never discover"
  fi
  mkdir -p "$badu/sstables/test_collections/collection_table-$UUID"
  printf 'x\n' > "$badu/sstables/test_collections/collection_table-$UUID/nb-1-big-Data.db"
  printf 'row\n' > "$badu/sstables/test_collections/collection_table-$UUID/nb-1-big-Data.db.jsonl"
  bu_out2=$(mrun "$badu" 2>&1 || true)
  if printf '%s' "$bu_out2" | grep -q 'missing Data.db for expected table: test_collections/collection_table'; then
    bad "a valid 32-hex UUID suffix did not satisfy the table"
  else
    ok "a valid 32-hex UUID suffix does satisfy it (check is the shape, not a blanket reject)"
  fi
fi

# ---------------------------------------------------------------------------
if [ -f "$manifest" ]; then
  oagen="$WORK/manifest-oagen"
  mkdir -p "$oagen/sstables/test_oa/simple_table-$UUID"
  printf 'x\n' > "$oagen/sstables/test_oa/simple_table-$UUID/oa-invalid-big-Data.db"
  printf 'row\n' > "$oagen/sstables/test_oa/simple_table-$UUID/oa-invalid-big-Data.db.jsonl"
  og_out=$(mrun "$oagen" 2>&1 || true)
  if printf '%s' "$og_out" | grep -q 'no OA-format Data.db.*test_oa/simple_table'; then
    ok "a non-numeric OA generation is rejected (matches oaBinariesPresent's regex)"
  else
    bad "oa-invalid-big-Data.db satisfied test_oa/simple_table"
  fi
  printf 'x\n' > "$oagen/sstables/test_oa/simple_table-$UUID/oa-2-big-Data.db"
  printf 'row\n' > "$oagen/sstables/test_oa/simple_table-$UUID/oa-2-big-Data.db.jsonl"
  og_out2=$(mrun "$oagen" 2>&1 || true)
  if printf '%s' "$og_out2" | grep -q 'test_oa/simple_table'; then
    bad "a numeric OA generation did not satisfy test_oa/simple_table"
  else
    ok "a numeric OA generation does satisfy it (check is the shape, not a blanket reject)"
  fi
fi

# ---------------------------------------------------------------------------
if [ -f "$manifest" ]; then
  UNTRACKED_UUID=ffffffffffffffffffffffffffffffff
  untr="$WORK/manifest-untracked"
  mkdir -p "$untr/sstables/test_collections/collection_table-$UNTRACKED_UUID"
  printf 'x\n' > "$untr/sstables/test_collections/collection_table-$UNTRACKED_UUID/nb-1-big-Data.db"
  printf 'row\n' > "$untr/sstables/test_collections/collection_table-$UNTRACKED_UUID/nb-1-big-Data.db.jsonl"
  ut_out=$(bash "$manifest" "$untr" 2>&1 || true)
  if printf '%s' "$ut_out" | grep -q 'missing Data.db for expected table: test_collections/collection_table'; then
    ok "a valid-UUID but UNTRACKED table dir does not satisfy the table (matches isCommittedTableDir)"
  else
    bad "an untracked table dir satisfied a table Jest would never enforce"
  fi
  # Fallback: the same corpus, with the script copied OUTSIDE any work tree, must ACCEPT
  # -- git reports nothing tracked, so every discovered dir counts (Jest's rule).
  ng_out=$(mrun "$untr" 2>&1 || true)
  if printf '%s' "$ng_out" | grep -q 'missing Data.db for expected table: test_collections/collection_table'; then
    bad "the git-less fallback rejected an untracked dir; it must treat all as committed"
  else
    ok "outside a work tree, every discovered dir counts (Jest's graceful fallback)"
  fi

  # ---------------------------------------------------------------------------
  symd="$WORK/manifest-symlink"
  mkdir -p "$symd/sstables/test_collections/real-$UUID"
  printf 'x\n' > "$symd/sstables/test_collections/real-$UUID/nb-1-big-Data.db"
  printf 'row\n' > "$symd/sstables/test_collections/real-$UUID/nb-1-big-Data.db.jsonl"
  ln -s "$symd/sstables/test_collections/real-$UUID" \
        "$symd/sstables/test_collections/collection_table-$UUID"
  sy_out=$(mrun "$symd" 2>&1 || true)
  if printf '%s' "$sy_out" | grep -q 'missing Data.db for expected table: test_collections/collection_table'; then
    ok "a SYMLINKED table directory does not satisfy the table (matches Dirent.isDirectory)"
  else
    bad "a symlinked table directory satisfied the expectation Jest would skip"
  fi
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  gold="$WORK/manifest-golden"
  gdir="$gold/sstables/test_basic/counters-$UUID"
  mkdir -p "$gdir"
  printf 'x\n' > "$gdir/nb-1-big-Data.db"
  g_out=$(mrun "$gold" 2>&1 || true)
  if printf '%s' "$g_out" | grep -q 'not the JSONL golden Jest reads.*test_basic/counters'; then
    ok "a Data.db without its .jsonl golden does not satisfy the table"
  else
    bad "a Data.db with no JSONL golden satisfied test_basic/counters"
  fi
  printf 'row\n' > "$gdir/nb-1-big-Data.db.jsonl"
  g_out2=$(mrun "$gold" 2>&1 || true)
  if printf '%s' "$g_out2" | grep -q 'not the JSONL golden Jest reads.*test_basic/counters'; then
    bad "the table still reported a missing golden after one was created"
  else
    ok "adding the .jsonl golden satisfies it (check is the golden, not a blanket reject)"
  fi
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  gen="$WORK/manifest-generation"
  gdir2="$gen/sstables/test_basic/counters-$UUID"
  mkdir -p "$gdir2"
  printf 'x\n' > "$gdir2/nb-2-big-Data.db"
  printf 'row\n' > "$gdir2/nb-2-big-Data.db.jsonl"
  gen_out=$(mrun "$gen" 2>&1 || true)
  if printf '%s' "$gen_out" | grep -q 'not the JSONL golden Jest reads.*test_basic/counters'; then
    ok "an alternate generation with its OWN golden does not satisfy a non-OA table"
  else
    bad "nb-2-big-Data.db.jsonl satisfied a table whose golden Jest reads as nb-1"
  fi
  # The nb-1 BINARY goes in alongside the nb-1 golden (round 33 made it required). Adding
  # only the golden would leave the table rejected for the BINARY's sake, and this case
  # would then be asserting the binary rule while claiming to assert the golden NAME.
  printf 'row\n' > "$gdir2/nb-1-big-Data.db.jsonl"
  printf 'x\n' > "$gdir2/nb-1-big-Data.db"
  gen_out2=$(mrun "$gen" 2>&1 || true)
  if printf '%s' "$gen_out2" | grep -q 'not the JSONL golden Jest reads.*test_basic/counters'; then
    bad "adding nb-1-big-Data.db.jsonl (and its binary) did not satisfy the table"
  else
    ok "adding the golden Jest actually reads satisfies it (name, not a blanket reject)"
  fi
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  multi="$WORK/manifest-multigen"
  mdir="$multi/sstables/test_oa/simple_table-$UUID"
  mkdir -p "$mdir"
  printf 'x\n' > "$mdir/oa-1-big-Data.db"          # earlier generation, NO golden
  printf 'x\n' > "$mdir/oa-2-big-Data.db"          # later generation, WITH golden -- Jest finds this
  printf 'row\n' > "$mdir/oa-2-big-Data.db.jsonl"
  mg_out=$(mrun "$multi" 2>&1 || true)
  if printf '%s' "$mg_out" | grep -q 'test_oa/simple_table'; then
    bad "a golden-less earlier OA generation masked a complete later one"
  else
    ok "a complete later OA generation satisfies the table despite a golden-less earlier one"
  fi
  # Control: with NO generation complete, it must still report the golden as missing --
  # the fix must not have turned the golden check off.
  rm -f "$mdir/oa-2-big-Data.db.jsonl"
  mg_out2=$(mrun "$multi" 2>&1 || true)
  if printf '%s' "$mg_out2" | grep -q 'not the JSONL golden Jest reads.*test_oa/simple_table'; then
    ok "with no complete generation, the missing golden is still reported"
  else
    bad "no generation had a golden, yet the table was accepted"
  fi
  # And the non-OA family keeps its DIRECTORY-scoped golden: Jest reads
  # nb-1-big-Data.db.jsonl whichever generation's Data.db is present.
  ngen="$WORK/manifest-nongen"
  ndir="$ngen/sstables/test_basic/counters-$UUID"
  mkdir -p "$ndir"
  printf 'x\n' > "$ndir/nb-2-big-Data.db"
  printf 'row\n' > "$ndir/nb-1-big-Data.db.jsonl"
  # ROUND 22's RULE STANDS, after round 33 briefly overturned it and round 34 restored it.
  # The GOLDEN is directory-scoped (`nb-1-big-Data.db.jsonl` whatever the binary) AND the
  # binary generation is free -- for every non-OA table EXCEPT test_basic/simple_table,
  # whose binary name corrupt-fixture.js hard-codes. `counters` is not that table, so an
  # nb-2 binary with the nb-1 golden is satisfied. The simple_table exception is asserted
  # separately below; keeping both is what stops the rule being re-generalised a third
  # time.
  ng_out2=$(mrun "$ngen" 2>&1 || true)
  if printf '%s' "$ng_out2" | grep -q 'test_basic/counters'; then
    bad "an nb-2 binary with the nb-1 golden was rejected for a table that does not pin the generation"
  else
    ok "a non-OA table other than simple_table accepts any generation with the nb-1 golden"
  fi
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  cross="$WORK/manifest-crossgen"
  cdir="$cross/sstables/test_oa/simple_table-$UUID"
  mkdir -p "$cdir"
  printf 'x\n' > "$cdir/oa-1-big-Data.db"           # binary of one generation ...
  printf 'row\n' > "$cdir/oa-2-big-Data.db.jsonl"     # ... golden of ANOTHER; Jest accepts this
  cx_out=$(mrun "$cross" 2>&1 || true)
  if printf '%s' "$cx_out" | grep -q 'test_oa/simple_table'; then
    bad "an OA binary and a DIFFERENT generation's golden were rejected, but Jest accepts them"
  else
    ok "an OA binary and a different generation's golden satisfy the table (matches Jest)"
  fi
  # Control: a golden of NO generation must still be rejected, so the decoupling did not
  # simply stop checking.
  rm -f "$cdir/oa-2-big-Data.db.jsonl"
  cx_out2=$(mrun "$cross" 2>&1 || true)
  if printf '%s' "$cx_out2" | grep -q 'not the JSONL golden Jest reads.*test_oa/simple_table'; then
    ok "with no OA golden at all the table is still rejected (decoupling is not disabling)"
  else
    bad "a table with no OA golden was accepted"
  fi
fi

# ---------------------------------------------------------------------------
if [ -f "$MANIFEST_SRC" ]; then
  toolless="$WORK/toolless"; mkdir -p "$toolless/sstables"
  # A FAILING HELPER, not an absent interpreter. `PATH=/nonexistent` was the first cut and
  # proved nothing: bash then cannot run anything, so the script dies whatever it does.
  # Shadowing ONE helper leaves the script running and reaching its verdict path, which is
  # the situation `|| true` used to turn into a false judged-9.
  failbin="$WORK/failbin"; mkdir -p "$failbin"
  printf '#!/bin/sh\nexit 3\n' > "$failbin/sort"; chmod +x "$failbin/sort"
  rc_ok=0
  env PATH="$failbin:$PATH" bash "$MANIFEST_SRC" "$toolless" >/dev/null 2>&1 || rc_ok=$?
  if [ "$rc_ok" -ne 9 ]; then
    ok "a failing helper does NOT reach the reserved verdict (rc=$rc_ok, not 9)"
  else
    bad "a failing helper still produced the reserved verdict 9; the code is not reserved"
  fi
  # Control: with tools available the same empty corpus IS a judged 9, so the case above
  # is about reachability of the code and not about the corpus.
  rc_v=0
  bash "$MANIFEST_SRC" "$toolless" >/dev/null 2>&1 || rc_v=$?
  if [ "$rc_v" -eq 9 ]; then
    ok "the same empty corpus with tools present emits the reserved verdict (9)"
  else
    bad "empty corpus with tools present returned $rc_v, expected the reserved 9"
  fi
fi

# ---------------------------------------------------------------------------
if [ -f "$MANIFEST_SRC" ]; then
  gbin="$WORK/failgrep"; mkdir -p "$gbin"
  printf '#!/bin/sh\nexit 3\n' > "$gbin/grep"; chmod +x "$gbin/grep"
  gcorpus="$WORK/grep-corpus"
  mkdir -p "$gcorpus/sstables/test_basic/counters-$UUID"
  printf 'x\n' > "$gcorpus/sstables/test_basic/counters-$UUID/nb-1-big-Data.db"
  printf 'row\n' > "$gcorpus/sstables/test_basic/counters-$UUID/nb-1-big-Data.db.jsonl"
  rc_g=0
  env PATH="$gbin:$PATH" bash "$MANIFEST_SRC" "$gcorpus" >/dev/null 2>&1 || rc_g=$?
  if [ "$rc_g" -ne 9 ]; then
    ok "a failing grep does NOT reach the reserved verdict (rc=$rc_g, not 9)"
  else
    bad "a failing grep produced the reserved verdict 9; operational failures are being read as non-matches"
  fi
fi

# ---------------------------------------------------------------------------
if [ -f "$MANIFEST_SRC" ] && [ -n "${CQLITE_DATASETS_ROOT:-}" ] \
   && [ -d "${CQLITE_DATASETS_ROOT:-}/sstables" ]; then
  fbin="$WORK/failgrepF"; mkdir -p "$fbin"
  real_grep=$(command -v grep)
  {
    printf '#!/bin/sh\n'
    printf 'for a in "$@"; do case "$a" in -*F*) exit 3;; esac; done\n'
    printf 'exec %s "$@"\n' "$real_grep"
  } > "$fbin/grep"
  chmod +x "$fbin/grep"
  rc_f=0
  env PATH="$fbin:$PATH" bash "$MANIFEST_SRC" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1 || rc_f=$?
  if [ "$rc_f" -ne 9 ] && [ "$rc_f" -ne 0 ]; then
    ok "a grep that fails only for -F does NOT reach the reserved verdict (rc=$rc_f)"
  elif [ "$rc_f" -eq 0 ]; then
    bad "the -F-failing grep run reported SUCCESS; the committed-set check cannot have run"
  else
    bad "a grep failing only for -F produced the reserved verdict 9; that site still collapses 1 and >1"
  fi
fi

# ---------------------------------------------------------------------------
if [ -f "$MANIFEST_SRC" ]; then
  piped=$(grep -nE '^[[:space:]]*printf .*\|[[:space:]]*grep ' "$MANIFEST_SRC" || true)
  if [ -z "$piped" ]; then
    ok "no grep predicate is fed by a pipeline (SIGPIPE cannot be read as a malfunction)"
  else
    bad "a grep predicate is fed by a pipeline; under pipefail an early match returns 141: $piped"
  fi
  # Positive control: the predicates still EXIST and still answer, so the check above is
  # not passing merely because the greps were deleted.
  if grep -qE '<<<' "$MANIFEST_SRC" && grep -qE 'grep -(Eq|Fxq)' "$MANIFEST_SRC"; then
    ok "the predicates are still present, fed by here-strings"
  else
    bad "the grep predicates are missing entirely; the pipeline check above proves nothing"
  fi
fi

# ---------------------------------------------------------------------------
gnu_class="[$(printf 's')$(printf 'd')$(printf 'w')$(printf 'b')SWDB]"
gnu_hits=""
for f in "$0" "$MANIFEST_SRC"; do
  [ -f "$f" ] || continue
  # Only lines that actually invoke grep/sed/awk with a pattern; a backslash-letter in
  # prose is not a portability problem.
  # COMMENTS ARE STRIPPED FIRST. The prose above names `\s` and friends while explaining
  # why the code must not use them, and a scan that cannot tell code from commentary
  # fails on its own documentation -- the identical trap this suite already hit in round
  # 13's portability check. `grep -v` on a leading-# line is enough here: these are shell
  # scripts, and a pattern is never introduced inside a comment.
  h=$(grep -nE "(grep|sed|awk)[^|]*\\\\${gnu_class}" "$f" 2>/dev/null \
        | grep -vE '^[0-9]+:[[:space:]]*#' \
        | grep -v 'gnu_class=' || true)
  [ -n "$h" ] && gnu_hits="$gnu_hits
$f: $h"
done
if [ -z "$gnu_hits" ]; then
  ok "no GNU-only regex escape in the scripts this change owns (BSD/macOS-safe)"
else
  bad "GNU-only regex escape(s) found; BSD/macOS grep reads them as literals:$gnu_hits"
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  for shape in nb2only nb1empty nb1ok; do
    nb="$WORK/manifest-$shape"
    nbdir="$nb/sstables/test_basic/simple_table-$UUID"
    mkdir -p "$nbdir"
    printf 'row\n' > "$nbdir/nb-1-big-Data.db.jsonl"
    case "$shape" in
      # NONEMPTY, or this shape is rejected by the emptiness rule and never reaches the
      # generation pin -- which is exactly what happened: the case passed with the pin
      # REMOVED, testing size while claiming to test the binary name.
      nb2only)  printf 'x\n' > "$nbdir/nb-2-big-Data.db" ;;
      nb1empty) : > "$nbdir/nb-1-big-Data.db" ;;   # deliberately ZERO-LENGTH
      nb1ok)    echo x > "$nbdir/nb-1-big-Data.db" ;;
    esac
    nb_out=$(mrun "$nb" 2>&1 || true)
    if printf '%s' "$nb_out" | grep -q 'test_basic/simple_table'; then hit=rejected; else hit=accepted; fi
    case "$shape" in
      nb2only)
        [ "$hit" = rejected ] \
          && ok "an alternate-generation binary alone does not satisfy a non-OA table" \
          || bad "nb-2-big-Data.db alone satisfied test_basic/simple_table" ;;
      nb1empty)
        [ "$hit" = rejected ] \
          && ok "a ZERO-LENGTH nb-1 binary does not satisfy it (a truncated fetch is not a corpus)" \
          || bad "an empty nb-1-big-Data.db satisfied test_basic/simple_table" ;;
      nb1ok)
        [ "$hit" = accepted ] \
          && ok "a nonempty nb-1 binary with its golden does satisfy it (rule, not blanket reject)" \
          || bad "a nonempty nb-1-big-Data.db with its golden was rejected" ;;
    esac
  done
  # SCOPE CONTROL: the nb-1 BINARY pin is specific to test_basic/simple_table, whose name
  # corrupt-fixture.js hard-codes. Round 33 generalised it to every non-OA table and
  # rejected usable alternate-generation corpora. Asserting the same shape on a DIFFERENT
  # table is what keeps the exception an exception.
  other="$WORK/manifest-othertable"
  odir="$other/sstables/test_basic/counters-$UUID"
  mkdir -p "$odir"
  printf 'x\n' > "$odir/nb-2-big-Data.db"
  printf 'row\n' > "$odir/nb-1-big-Data.db.jsonl"
  o_out=$(mrun "$other" 2>&1 || true)
  if printf '%s' "$o_out" | grep -q 'test_basic/counters'; then
    bad "the nb-1 binary pin leaked to test_basic/counters, which does not require it"
  else
    ok "the nb-1 binary pin applies ONLY to test_basic/simple_table"
  fi
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  # zero-length OA binary
  oz="$WORK/manifest-oa-empty"
  ozdir="$oz/sstables/test_oa/simple_table-$UUID"
  mkdir -p "$ozdir"
  : > "$ozdir/oa-2-big-Data.db"          # deliberately ZERO-LENGTH
  printf 'row\n' > "$ozdir/oa-2-big-Data.db.jsonl"
  oz_out=$(mrun "$oz" 2>&1 || true)
  if printf '%s' "$oz_out" | grep -q 'test_oa/simple_table'; then
    ok "a ZERO-LENGTH OA binary does not satisfy the table (size rule is not non-OA-only)"
  else
    bad "an empty oa-2-big-Data.db satisfied test_oa/simple_table"
  fi
  # unrecognised non-OA descriptor name
  jn="$WORK/manifest-junkname"
  jndir="$jn/sstables/test_basic/counters-$UUID"
  mkdir -p "$jndir"
  printf 'x\n' > "$jndir/junk-Data.db"
  printf 'row\n' > "$jndir/nb-1-big-Data.db.jsonl"
  jn_out=$(mrun "$jn" 2>&1 || true)
  if printf '%s' "$jn_out" | grep -q 'test_basic/counters'; then
    ok "a name the reader would not recognise (junk-Data.db) does not satisfy a table"
  else
    bad "junk-Data.db satisfied test_basic/counters"
  fi
  # Control: a real alternate descriptor (da-<n>-bti) IS recognised, so the pattern is a
  # format rule and not a whitelist of the one name this corpus happens to use most.
  bti="$WORK/manifest-bti"
  btidir="$bti/sstables/test_basic/counters-$UUID"
  mkdir -p "$btidir"
  printf 'x\n' > "$btidir/da-2-bti-Data.db"
  printf 'row\n' > "$btidir/nb-1-big-Data.db.jsonl"
  bti_out=$(mrun "$bti" 2>&1 || true)
  if printf '%s' "$bti_out" | grep -q 'test_basic/counters'; then
    bad "a da-<n>-bti descriptor was rejected; it is a real shape in this corpus"
  else
    ok "a da-<n>-bti descriptor IS recognised (format rule, not a single-name whitelist)"
  fi
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  #    name                      expected (accept|reject)   why
  for spec in "na-1-big-Data.db:accept" \
              "nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db:accept" \
              "nb-6aa08200-a251-11f0-a3fe-f1a551383fb9-big-Data.db:accept" \
              "nb-1-big-Data.db:accept" \
              "oa-1-big-Data.db:accept" \
              "da-2-bti-Data.db:accept" \
              "zz-9-big-Data.db:reject" \
              "nc-1-big-Data.db:reject" \
              "nb-1-bti-Data.db:reject" \
              "ma-1-big-Data.db:reject" \
              "da-1-big-Data.db:reject"; do
    dname=${spec%%:*}; want=${spec##*:}
    dcase="$WORK/manifest-desc-${dname%%-*}-$want-$RANDOM"
    ddir="$dcase/sstables/test_basic/counters-$UUID"
    mkdir -p "$ddir"
    printf 'x\n' > "$ddir/$dname"
    printf 'row\n' > "$ddir/nb-1-big-Data.db.jsonl"
    d_out=$(mrun "$dcase" 2>&1 || true)
    if printf '%s' "$d_out" | grep -q 'test_basic/counters'; then got=reject; else got=accept; fi
    if [ "$got" = "$want" ]; then
      ok "descriptor $dname -> $want (matches the version gates)"
    else
      bad "descriptor $dname -> $got, expected $want"
    fi
  done
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  #    fixture shape                      expected substring of the diagnostic
  for spec in "empty:ZERO-LENGTH" "junk:none the reader would open" "nogolden:not the JSONL golden"; do
    shape=${spec%%:*}; want=${spec#*:}
    dg="$WORK/manifest-diag-$shape"
    dgdir="$dg/sstables/test_basic/counters-$UUID"
    mkdir -p "$dgdir"
    case "$shape" in
      empty)    : > "$dgdir/nb-1-big-Data.db"; : > "$dgdir/nb-1-big-Data.db.jsonl" ;;
      junk)     printf 'x\n' > "$dgdir/junk-Data.db"; : > "$dgdir/nb-1-big-Data.db.jsonl" ;;
      nogolden) printf 'x\n' > "$dgdir/nb-1-big-Data.db" ;;
    esac
    dg_out=$(mrun "$dg" 2>&1 || true)
    if printf '%s' "$dg_out" | grep -q "$want"; then
      ok "the '$shape' shape reports its OWN cause ($want)"
    else
      bad "the '$shape' shape did not report '$want'; got: $(printf '%s' "$dg_out" | grep 'test_basic/counters' | head -1)"
    fi
  done
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  for shape in golden-dir binary-dir symlink-ok plain-ok; do
    uf="$WORK/manifest-usable-$shape"
    ufdir="$uf/sstables/test_basic/counters-$UUID"
    mkdir -p "$ufdir"
    case "$shape" in
      golden-dir)
        printf 'x\n' > "$ufdir/nb-1-big-Data.db"
        mkdir -p "$ufdir/nb-1-big-Data.db.jsonl/sub"
        printf 'y\n' > "$ufdir/nb-1-big-Data.db.jsonl/sub/f" ;;
      binary-dir)
        mkdir -p "$ufdir/nb-1-big-Data.db/sub"
        printf 'y\n' > "$ufdir/nb-1-big-Data.db/sub/f"
        printf 'row\n' > "$ufdir/nb-1-big-Data.db.jsonl" ;;
      symlink-ok)
        printf 'x\n' > "$ufdir/real-binary.db"
        ln -s "$ufdir/real-binary.db" "$ufdir/nb-1-big-Data.db"
        printf 'row\n' > "$ufdir/nb-1-big-Data.db.jsonl" ;;
      plain-ok)
        printf 'x\n' > "$ufdir/nb-1-big-Data.db"
        printf 'row\n' > "$ufdir/nb-1-big-Data.db.jsonl" ;;
    esac
    uf_out=$(mrun "$uf" 2>&1 || true)
    if printf '%s' "$uf_out" | grep -q 'test_basic/counters'; then got=reject; else got=accept; fi
    case "$shape" in
      golden-dir|binary-dir)
        [ "$got" = reject ] \
          && ok "a nonempty DIRECTORY as the ${shape%-dir} does not satisfy the table" \
          || bad "a directory named like the ${shape%-dir} satisfied test_basic/counters" ;;
      symlink-ok)
        [ "$got" = accept ] \
          && ok "a symlink to a nonempty regular file still counts (type test, not a symlink ban)" \
          || bad "a symlink to a nonempty regular binary was rejected" ;;
      plain-ok)
        [ "$got" = accept ] \
          && ok "plain nonempty regular files still satisfy the table" \
          || bad "plain nonempty regular files were rejected" ;;
    esac
  done
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ] && [ "$(id -u)" -ne 0 ]; then
  for target in binary golden; do
    ur="$WORK/manifest-unreadable-$target"
    urdir="$ur/sstables/test_basic/counters-$UUID"
    mkdir -p "$urdir"
    printf 'x\n'   > "$urdir/nb-1-big-Data.db"
    printf 'row\n' > "$urdir/nb-1-big-Data.db.jsonl"
    case "$target" in
      binary) chmod 000 "$urdir/nb-1-big-Data.db" ;;
      golden) chmod 000 "$urdir/nb-1-big-Data.db.jsonl" ;;
    esac
    ur_out=$(mrun "$ur" 2>&1 || true)
    if printf '%s' "$ur_out" | grep -q 'test_basic/counters'; then
      ok "an UNREADABLE $target does not satisfy the table"
    else
      bad "an unreadable $target satisfied test_basic/counters"
    fi
    chmod -R u+rwX "$ur" 2>/dev/null || true
  done
  # Control: the same shapes READABLE must be accepted, so this is a permission test and
  # not a second copy of the existence test.
  urok="$WORK/manifest-unreadable-control"
  urokdir="$urok/sstables/test_basic/counters-$UUID"
  mkdir -p "$urokdir"
  printf 'x\n'   > "$urokdir/nb-1-big-Data.db"
  printf 'row\n' > "$urokdir/nb-1-big-Data.db.jsonl"
  urok_out=$(mrun "$urok" 2>&1 || true)
  if printf '%s' "$urok_out" | grep -q 'test_basic/counters'; then
    bad "readable fixtures were rejected; the readability rule is over-tightened"
  else
    ok "the same fixtures READABLE are accepted (permission test, not existence)"
  fi
elif [ -n "$MANIFEST_NOGIT" ]; then
  echo "INFO: running as root, which can read mode-000 files; skipping the readability case"
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  for shape in one-valid empty-sibling dir-sibling two-valid; do
    og="$WORK/manifest-oagolden-$shape"
    ogdir="$og/sstables/test_oa/simple_table-$UUID"
    mkdir -p "$ogdir"
    printf 'x\n'   > "$ogdir/oa-2-big-Data.db"
    printf 'row\n' > "$ogdir/oa-2-big-Data.db.jsonl"
    case "$shape" in
      empty-sibling) : > "$ogdir/oa-1-big-Data.db.jsonl" ;;
      dir-sibling)   mkdir -p "$ogdir/oa-1-big-Data.db.jsonl/x"
                     printf 'y\n' > "$ogdir/oa-1-big-Data.db.jsonl/x/f" ;;
      two-valid)     printf 'row\n' > "$ogdir/oa-1-big-Data.db.jsonl" ;;
    esac
    og_out=$(mrun "$og" 2>&1 || true)
    if printf '%s' "$og_out" | grep -q 'test_oa/simple_table'; then got=reject; else got=accept; fi
    case "$shape" in
      one-valid|two-valid)
        [ "$got" = accept ] \
          && ok "OA goldens all usable ($shape) -> table satisfied" \
          || bad "$shape was rejected though every OA golden is usable" ;;
      *)
        [ "$got" = reject ] \
          && ok "an UNUSABLE OA golden sibling ($shape) makes the table incomplete" \
          || bad "$shape satisfied test_oa/simple_table though Jest could select the broken golden" ;;
    esac
    chmod -R u+rwX "$og" 2>/dev/null || true
  done
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  UUID2=aa11bb22cc33dd44ee55ff6677889900
  for shape in both-good one-broken; do
    tc="$WORK/manifest-twodir-$shape"
    tck="$tc/sstables/test_basic"
    mkdir -p "$tck/counters-$UUID" "$tck/counters-$UUID2"
    printf 'x\n'   > "$tck/counters-$UUID/nb-1-big-Data.db"
    printf 'row\n' > "$tck/counters-$UUID/nb-1-big-Data.db.jsonl"
    printf 'x\n'   > "$tck/counters-$UUID2/nb-1-big-Data.db"
    case "$shape" in
      both-good)  printf 'row\n' > "$tck/counters-$UUID2/nb-1-big-Data.db.jsonl" ;;
      one-broken) : > "$tck/counters-$UUID2/nb-1-big-Data.db.jsonl" ;;   # ZERO-LENGTH
    esac
    tc_out=$(mrun "$tc" 2>&1 || true)
    if printf '%s' "$tc_out" | grep -q 'test_basic/counters'; then got=reject; else got=accept; fi
    case "$shape" in
      both-good)
        [ "$got" = accept ] \
          && ok "two good candidate directories satisfy the table" \
          || bad "two good candidate directories were rejected" ;;
      one-broken)
        [ "$got" = reject ] \
          && ok "one broken candidate directory disqualifies the table (the consumer picks blind)" \
          || bad "a broken candidate directory was masked by a good sibling" ;;
    esac
  done
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  # toc_case <shape> -> "accept" | "reject"
  toc_case() {
    local shape=$1 tc tck d
    tc="$WORK/manifest-toc-$shape"; tck="$tc/sstables/test_basic"
    d="$tck/counters-$UUID"; mkdir -p "$d"
    printf 'x\n'   > "$d/nb-1-big-Data.db"
    printf 'row\n' > "$d/nb-1-big-Data.db.jsonl"
    case "$shape" in
      complete)
        printf 'Data.db\nTOC.txt\n' > "$d/nb-1-big-TOC.txt" ;;
      zero-length-companion)   # a LEGITIMATE shape: 3 real Rows.db are zero-length
        : > "$d/nb-1-big-Rows.db"
        printf 'Data.db\nRows.db\nTOC.txt\n' > "$d/nb-1-big-TOC.txt" ;;
      missing-companion)       # the partial-extraction shape
        printf 'Data.db\nCompressionInfo.db\nTOC.txt\n' > "$d/nb-1-big-TOC.txt" ;;
      companion-is-a-dir)      # present to `-e`, unusable to a reader
        mkdir -p "$d/nb-1-big-Filter.db"
        printf 'Data.db\nFilter.db\nTOC.txt\n' > "$d/nb-1-big-TOC.txt" ;;
      no-toc)                  # measured 144/144 real generations have one
        : ;;
      toc-escapes-dir)         # a TOC entry must be a component NAME, never a path
        printf 'Data.db\n../elsewhere.db\nTOC.txt\n' > "$d/nb-1-big-TOC.txt" ;;
    esac
    # CAPTURE, then grep. `mrun ... | grep -q` closes the pipe on the first match while the
    # manifest is still writing its other 38 lines, so the manifest dies of SIGPIPE and
    # `pipefail` makes the pipeline rc 141 -- the `if` takes the else branch and every
    # REJECT case reads as "accepted". This suite has now been bitten by that four times.
    local _o; _o=$(NO_AUTO_TOC=1 mrun "$tc" 2>&1 || true)
    if printf '%s' "$_o" | grep -q 'test_basic/counters'; then
      echo reject
    else
      echo accept
    fi
  }

  # Both directions. The accept cases are the control: without them a manifest that
  # rejected EVERY generation would pass the reject cases and look correct.
  for shape in complete zero-length-companion; do
    [ "$(toc_case "$shape")" = accept ] \
      && ok "TOC shape '$shape' satisfies the table" \
      || bad "TOC shape '$shape' was rejected, but it is a shape the real corpus has"
  done
  for shape in missing-companion companion-is-a-dir no-toc toc-escapes-dir; do
    [ "$(toc_case "$shape")" = reject ] \
      && ok "TOC shape '$shape' disqualifies the table" \
      || bad "TOC shape '$shape' was accepted; an incomplete generation reads as complete"
  done

  # The diagnostic must NAME the cause: an operator told only "missing Data.db" for a
  # Data.db that is present and fine goes looking in the wrong place (the same
  # misattribution round 35 fixed for the truncated/misnamed binary).
  tcd="$WORK/manifest-toc-diag"; mkdir -p "$tcd/sstables/test_basic/counters-$UUID"
  printf 'x\n'   > "$tcd/sstables/test_basic/counters-$UUID/nb-1-big-Data.db"
  printf 'row\n' > "$tcd/sstables/test_basic/counters-$UUID/nb-1-big-Data.db.jsonl"
  printf 'Data.db\nCompressionInfo.db\nTOC.txt\n' > "$tcd/sstables/test_basic/counters-$UUID/nb-1-big-TOC.txt"
  tcd_out=$(NO_AUTO_TOC=1 mrun "$tcd" 2>&1 || true)
  if printf '%s' "$tcd_out" | grep -q 'TOC.txt is absent, or lists a component'; then
    ok "the incomplete-generation diagnostic names the TOC as the cause"
  else
    bad "the incomplete-generation diagnostic did not name the TOC"
  fi
fi

# ---------------------------------------------------------------------------
PU_SRC=$(cd "$SCRIPT_DIR/../.." && pwd)/bindings/node/__test__/parity-utils.js
if command -v node >/dev/null 2>&1 && [ -f "$PU_SRC" ]; then
  pu_tree="$WORK/pu-tree"; mkdir -p "$pu_tree/bindings/node/__test__"
  cp "$PU_SRC" "$pu_tree/bindings/node/__test__/parity-utils.js"
  puk="$pu_tree/corpus/test_basic"
  # ONLY a non-UUID committed sibling exists, and its golden is a DIRECTORY -- present to
  # `fs.existsSync`, unusable to the reader. No UUID sibling, so the answer cannot depend
  # on readdir order (which is filesystem-determined and not a property to test on).
  mkdir -p "$puk/orders-wip/nb-1-big-Data.db.jsonl" "$puk/oatab-wip/oa-1-big-Data.db.jsonl"

  pu_probe() {   # $1 = parity-utils.js path -> two lines, "fn -> path|(null)"
    node -e "
      global.testPaths = { SSTABLES_DIR: '$puk/..' };
      const u = require('$1');
      for (const [f, t] of [['findJsonlFile','orders'], ['findOaJsonlFile','oatab']]) {
        const r = u[f]('test_basic', t);
        console.log(f + ' ' + (r ? 'SELECTED' : 'null'));
      }" 2>/dev/null
  }

  pu_out=$(pu_probe "$pu_tree/bindings/node/__test__/parity-utils.js")
  if [ "$(printf '%s' "$pu_out" | grep -c 'null')" = 2 ]; then
    ok "both golden lookups ignore a committed non-UUID sibling"
  else
    bad "a golden lookup selected a committed non-UUID sibling the manifest never validates"
    printf '  %s\n' "$pu_out"
  fi

  # The predicate itself: equality on the CAPTURED table name, not a prefix test.
  # `orders-extra-<uuid>` is table `orders-extra`, never table `orders`.
  pu_pred=$(node -e "
    global.testPaths = { SSTABLES_DIR: '$puk/..' };
    const u = require('$pu_tree/bindings/node/__test__/parity-utils.js');
    const cases = [
      ['orders-$UUID', 'orders', true],
      ['orders-wip', 'orders', false],
      ['orders-extra-$UUID', 'orders', false],
      ['orders-extra-$UUID', 'orders-extra', true],
      ['orders-$(printf '%s' "$UUID" | tr 'a-f' 'A-F')', 'orders', false]
    ];
    let n = 0;
    for (const [d, t, e] of cases) if (u.isTableDirFor(d, t) === e) n++;
    console.log(n + '/' + cases.length);" 2>/dev/null)
  [ "$pu_pred" = "5/5" ] \
    && ok "isTableDirFor matches the manifest's rule exactly (5/5)" \
    || bad "isTableDirFor disagrees with the manifest's rule ($pu_pred)"

  # RED CONTROL: with the pre-fix predicate restored in the scratch copy, the lookups MUST
  # select the broken sibling. Without this the case above could pass for the wrong reason
  # (a typo'd probe, a module that failed to load) -- a bare "null" proves nothing.
  pu_red="$WORK/pu-red"; mkdir -p "$pu_red/bindings/node/__test__"
  sed 's/      isTableDirFor(entry\.name, table) \&\&/      entry.name.startsWith(`${table}-`) \&\&/' \
    "$PU_SRC" > "$pu_red/bindings/node/__test__/parity-utils.js"
  if grep -q 'startsWith(`${table}-`)' "$pu_red/bindings/node/__test__/parity-utils.js"; then
    red_out=$(pu_probe "$pu_red/bindings/node/__test__/parity-utils.js")
    if [ "$(printf '%s' "$red_out" | grep -c 'SELECTED')" = 2 ]; then
      ok "RED control: the pre-fix startsWith predicate does select the broken sibling"
    else
      bad "RED control did not reproduce the defect, so the case above proves nothing"
      printf '  %s\n' "$red_out"
    fi
  else
    bad "RED control could not plant the pre-fix predicate (the fix's shape moved)"
  fi
else
  echo "info - node or parity-utils.js unavailable; skipping the Jest-lookup cases"
fi

# ---------------------------------------------------------------------------
# Case 63 (round 48, Medium): corrupt-fixture.js is the THIRD consumer of a table
# directory and must apply the same rule as the manifest and the golden lookups.
#
# `sourceTableDir()` took any `simple_table-*` name, sorted LEXICOGRAPHICALLY and returned
# the first with a `nb-1-big-Data.db` entry -- so an earlier-sorting non-canonical or
# uncommitted sibling WON over the valid directory, and `existsSync` accepted a Data.db
# that is a directory or zero-length. The manifest validates only `<table>-<32 hex>`
# directories, so it reports the corpus complete while abort-safety corrupts the wrong
# fixture.
#
# Driven directly (no native module needed) from a scratch copy of the tree, which puts
# `isCommittedTableDir` on its git-unavailable fallback -- the only way a synthetic dir is
# "committed".
# ---------------------------------------------------------------------------
CF_SRC=$(cd "$SCRIPT_DIR/../.." && pwd)/bindings/node/__test__/corrupt-fixture.js
if command -v node >/dev/null 2>&1 && [ -f "$CF_SRC" ] && [ -f "$PU_SRC" ]; then
  cf_tree="$WORK/cf-tree"; mkdir -p "$cf_tree/bindings/node/__test__"
  cp "$CF_SRC" "$PU_SRC" "$cf_tree/bindings/node/__test__/"
  cfk="$cf_tree/corpus/test_basic"; cf_good="simple_table-$UUID"
  mkdir -p "$cfk/$cf_good"; printf 'x\n' > "$cfk/$cf_good/nb-1-big-Data.db"
  # Three decoys, each sorting BEFORE the hex UUID ('0'/'1'/'2' < '4'), so a lexicographic
  # pick without the predicates lands on one of them.
  mkdir -p "$cfk/simple_table-0wip";  printf 'x\n' > "$cfk/simple_table-0wip/nb-1-big-Data.db"
  mkdir -p "$cfk/simple_table-1zero"; : >          "$cfk/simple_table-1zero/nb-1-big-Data.db"
  mkdir -p "$cfk/simple_table-2dir/nb-1-big-Data.db"

  cf_probe() {   # $1 = tree root -> the selected directory's basename, or "(null)"
    node -e "
      global.testPaths = { SSTABLES_DIR: '$cf_tree/corpus' };
      const cf = require('$1/bindings/node/__test__/corrupt-fixture.js');
      const d = cf.sourceTableDir('$cf_tree/corpus');
      console.log(d ? require('path').basename(d) : '(null)');" 2>/dev/null
  }

  cf_got=$(cf_probe "$cf_tree")
  [ "$cf_got" = "$cf_good" ] \
    && ok "sourceTableDir selects the canonical committed directory, not an earlier-sorting decoy" \
    || bad "sourceTableDir selected '$cf_got', expected '$cf_good'"

  # A SYMLINKED table directory must be invisible -- and this one is not a consistency
  # point but a DESTRUCTIVE-WRITE guard (round 49). `fs.cpSync` preserves a symlink by
  # default, so a symlinked table dir was copied AS A SYMLINK and the harness's
  # truncate/bitflip then wrote THROUGH it into the real fixture. On a shared machine-local
  # corpus that damages every other lane on the box.
  cf_sym="$WORK/cf-symlink"; mkdir -p "$cf_sym/bindings/node/__test__"
  cp "$CF_SRC" "$PU_SRC" "$cf_sym/bindings/node/__test__/"
  symk="$cf_sym/corpus/test_basic"; mkdir -p "$symk"
  # The REAL directory lives outside the keyspace; only a canonical, committed-looking
  # SYMLINK to it appears inside. Nothing else can be selected.
  mkdir -p "$cf_sym/elsewhere/simple_table-$UUID"
  printf 'ORIGINALDATA' > "$cf_sym/elsewhere/simple_table-$UUID/nb-1-big-Data.db"
  ln -s "$cf_sym/elsewhere/simple_table-$UUID" "$symk/simple_table-$UUID"

  sym_got=$(node -e "
    global.testPaths = { SSTABLES_DIR: '$cf_sym/corpus' };
    const cf = require('$cf_sym/bindings/node/__test__/corrupt-fixture.js');
    const d = cf.sourceTableDir('$cf_sym/corpus');
    console.log(d ? require('path').basename(d) : '(null)');" 2>/dev/null)
  [ "$sym_got" = "(null)" ] \
    && ok "sourceTableDir ignores a symlinked table directory" \
    || bad "sourceTableDir selected the symlinked table directory '$sym_got'"

  # The consequence, asserted directly: build the fixture and confirm the SOURCE is
  # untouched. Without this the case above tests a predicate; this tests the damage.
  #
  # It covers the DIRECTORY guard only. `sourceTableDir` rejects the symlinked directory,
  # so `makeCorruptFixture` throws before it ever calls `cpSync` -- deleting
  # `dereference: true` leaves this assert green. The COMPONENT-symlink half is a separate
  # case below, with its own RED control.
  mkdir -p "$symk/simple_table-real"   # not canonical; forces the symlink to be the only
  rm -rf "$symk/simple_table-real"     # plausible candidate. (created+removed to be explicit)
  node -e "
    global.testPaths = { SSTABLES_DIR: '$cf_sym/corpus' };
    const cf = require('$cf_sym/bindings/node/__test__/corrupt-fixture.js');
    try { cf.makeCorruptFixture('$WORK/cf-sym-dest', '$cf_sym/corpus', 'truncate'); } catch (_e) { /* expected: no selectable source */ }
    " >/dev/null 2>&1 || true
  sym_after=$(cat "$cf_sym/elsewhere/simple_table-$UUID/nb-1-big-Data.db" 2>/dev/null)
  [ "$sym_after" = "ORIGINALDATA" ] \
    && ok "the source corpus is intact after a fixture build over a symlinked table dir" \
    || bad "the source fixture was MODIFIED through the symlink (now '$sym_after')"

  # RED CONTROL: the pre-fix selection must land on a decoy, or the case above proves nothing.
  cf_red="$WORK/cf-red"; mkdir -p "$cf_red/bindings/node/__test__"
  cp "$PU_SRC" "$cf_red/bindings/node/__test__/"
  sed -e 's|\.readdirSync(ksDir, { withFileTypes: true })|.readdirSync(ksDir)|' \
      -e 's|\.filter((entry) => entry\.isDirectory())||' \
      -e 's|\.map((entry) => entry\.name)||' \
      -e 's|\.filter((name) => isTableDirFor(name, TABLE) && isCommittedTableDir(KEYSPACE, name))|.filter((name) => name.startsWith(`${TABLE}-`))|' \
      -e 's|\.filter((dir) => isNonemptyFile(path\.join(dir, DATA_COMPONENT)))|.filter((dir) => fs.existsSync(path.join(dir, DATA_COMPONENT)))|' \
      -e 's|^    dereference: true,$||' \
      "$CF_SRC" > "$cf_red/bindings/node/__test__/corrupt-fixture.js"
  if grep -q 'name.startsWith(`${TABLE}-`)' "$cf_red/bindings/node/__test__/corrupt-fixture.js"; then
    cf_red_got=$(cf_probe "$cf_red")
    [ "$cf_red_got" != "$cf_good" ] && [ "$cf_red_got" != "(null)" ] \
      && ok "RED control: the pre-fix selection lands on the decoy '$cf_red_got'" \
      || bad "RED control did not reproduce the defect (got '$cf_red_got'); the case above proves nothing"

    # RED control for the symlink half, against its OWN scratch corpus so the assertion
    # above cannot be satisfied by a corpus this run already mutated.
    sym_red="$WORK/cf-sym-red-corpus"; mkdir -p "$sym_red/test_basic" "$sym_red/elsewhere/simple_table-$UUID"
    printf 'ORIGINALDATA' > "$sym_red/elsewhere/simple_table-$UUID/nb-1-big-Data.db"
    ln -s "$sym_red/elsewhere/simple_table-$UUID" "$sym_red/test_basic/simple_table-$UUID"
    node -e "
      global.testPaths = { SSTABLES_DIR: '$sym_red' };
      const cf = require('$cf_red/bindings/node/__test__/corrupt-fixture.js');
      try { cf.makeCorruptFixture('$WORK/cf-sym-red-dest', '$sym_red', 'truncate'); } catch (_e) {}
      " >/dev/null 2>&1 || true
    sym_red_after=$(cat "$sym_red/elsewhere/simple_table-$UUID/nb-1-big-Data.db" 2>/dev/null)
    [ "$sym_red_after" != "ORIGINALDATA" ] \
      && ok "RED control: the pre-fix harness DOES write through the symlink (source now '$sym_red_after')" \
      || bad "RED control did not reproduce the destructive write; the intactness assert proves nothing"
  else
    bad "RED control could not plant the pre-fix selection (the fix's shape moved)"
  fi
else
  echo "info - node or corrupt-fixture.js unavailable; skipping the fixture-selection case"
fi

# ---------------------------------------------------------------------------
# Case 65 (round 50, Low): the COMPONENT-symlink half -- `cpSync(..., { dereference: true })`.
#
# Case 64 covers a symlinked table DIRECTORY, which `sourceTableDir` now rejects outright,
# so `makeCorruptFixture` throws before reaching `cpSync` and that case cannot see the
# dereference flag at all. A symlinked COMPONENT inside an otherwise REAL, canonical,
# committed directory is the case that reaches it: the directory passes every predicate
# (`isNonemptyFile` uses `statSync`, which follows symlinks -- deliberately, since a
# symlink to a real fixture file is a legitimate corpus layout), the copy proceeds, and
# without `dereference` the destination Data.db is a LINK whose truncation destroys the
# external source.
#
# Asserts BOTH directions, which is what makes it a real test of the flag: the destination
# IS mutated (the harness still does its job) and the external source is NOT.
# ---------------------------------------------------------------------------
# Case 67 (round 52, Low): `broken` must stay distinguishable from `absent`.
#
# Round 48 gave `sourceTableDir` a nonempty-regular-file filter, which silently converted
# "present but UNUSABLE" into "ABSENT". `abort-safety.test.js` gates on exactly that
# distinction -- `broken` is a HARD FAILURE, a non-strict `absent` is a real `test.skip` --
# so a TRUNCATED fixture stopped hard-failing and started SKIPPING, inverting #1437's stated
# design and making that test's `broken` branch unreachable. A regression introduced by an
# earlier round of this same PR.
#
# Drives the real `classifyTableDir` over every shape, including the two forms of damage the
# old size-only check could not see (a directory, a dangling symlink).
# ---------------------------------------------------------------------------
if command -v node >/dev/null 2>&1 && [ -f "$CF_SRC" ] && [ -f "$PU_SRC" ]; then
  cls_probe() {   # <shape> -> "<status>"
    local shape=$1 root d
    root="$WORK/cf-cls-$shape"; rm -rf "$root"
    d="$root/test_basic/simple_table-$UUID"
    case "$shape" in
      ok)             mkdir -p "$d"; printf 'DATA' > "$d/nb-1-big-Data.db" ;;
      zero-length)    mkdir -p "$d"; :             > "$d/nb-1-big-Data.db" ;;
      is-a-directory) mkdir -p "$d/nb-1-big-Data.db" ;;
      dangling-link)  mkdir -p "$d"; ln -s "$root/nowhere" "$d/nb-1-big-Data.db" ;;
      no-data-db)     mkdir -p "$d" ;;                       # fetched nothing: absent
      no-keyspace)    mkdir -p "$root" ;;
      good-beats-bad) mkdir -p "$d" "$root/test_basic/simple_table-0000000000000000000000000000000a"
                      printf 'DATA' > "$d/nb-1-big-Data.db"
                      :              > "$root/test_basic/simple_table-0000000000000000000000000000000a/nb-1-big-Data.db" ;;
    esac
    node -e "
      global.testPaths = { SSTABLES_DIR: '$root' };
      const cf = require('$cf_tree/bindings/node/__test__/corrupt-fixture.js');
      console.log(cf.classifyTableDir('$root').status);" 2>/dev/null
  }

  cls_bad=""
  for pair in ok:ok zero-length:broken is-a-directory:broken dangling-link:broken \
              no-data-db:absent no-keyspace:absent good-beats-bad:ok; do
    cls_shape=${pair%%:*}; cls_want=${pair#*:}
    cls_got=$(cls_probe "$cls_shape")
    [ "$cls_got" = "$cls_want" ] || cls_bad="$cls_bad $cls_shape(got=$cls_got,want=$cls_want)"
  done
  [ -z "$cls_bad" ] \
    && ok "classifyTableDir separates ok / broken / absent over all seven shapes" \
    || bad "classifyTableDir misclassified:$cls_bad"

  # The CONSEQUENCE, which is the whole reason the distinction exists: abort-safety turns
  # `broken` into a hard failure and a non-strict `absent` into a skip. Asserting the status
  # alone would not catch a future caller that stopped acting on it.
  cls_gate=$(node -e "
    global.testPaths = { SSTABLES_DIR: '$WORK/cf-cls-zero-length' };
    const cf = require('$cf_tree/bindings/node/__test__/corrupt-fixture.js');
    const r = cf.classifyTableDir('$WORK/cf-cls-zero-length');
    // Mirrors abort-safety.test.js: HARD_FAIL = broken || (absent && strict)
    console.log(r.status === 'broken' ? 'hard-fail' : 'skip');" 2>/dev/null)
  [ "$cls_gate" = "hard-fail" ] \
    && ok "a truncated fixture hard-fails abort-safety rather than silently skipping" \
    || bad "a truncated fixture would SKIP abort-safety (#1437 inverted)"

  # RED CONTROL: with the round-48 shape restored (filter to usable, return null otherwise),
  # a zero-length Data.db must classify as `absent`.
  cls_red="$WORK/cf-cls-red"; mkdir -p "$cls_red/bindings/node/__test__"
  cp "$PU_SRC" "$cls_red/bindings/node/__test__/"
  sed -e "s|  const damaged = candidates.find((dir) => entryExists(path.join(dir, DATA_COMPONENT)));|  const damaged = undefined;|" \
      "$CF_SRC" > "$cls_red/bindings/node/__test__/corrupt-fixture.js"
  if grep -q 'const damaged = undefined;' "$cls_red/bindings/node/__test__/corrupt-fixture.js"; then
    cls_red_got=$(node -e "
      global.testPaths = { SSTABLES_DIR: '$WORK/cf-cls-zero-length' };
      const cf = require('$cls_red/bindings/node/__test__/corrupt-fixture.js');
      console.log(cf.classifyTableDir('$WORK/cf-cls-zero-length').status);" 2>/dev/null)
    [ "$cls_red_got" = "absent" ] \
      && ok "RED control: without the damaged branch a truncated fixture reads as 'absent'" \
      || bad "RED control did not reproduce the collapse (got '$cls_red_got'); the case above proves nothing"
  else
    bad "RED control could not plant the round-48 shape (the fix's shape moved)"
  fi
else
  echo "info - node or corrupt-fixture.js unavailable; skipping the classification case"
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  gen_case() {   # <shape> -> "accept" | "reject"
    local shape=$1 gc d
    gc="$WORK/manifest-gen-$shape"; rm -rf "$gc"
    d="$gc/sstables/test_basic/counters-$UUID"; mkdir -p "$d"
    printf 'x\n'   > "$d/nb-1-big-Data.db"
    printf 'row\n' > "$d/nb-1-big-Data.db.jsonl"
    printf 'Data.db\nTOC.txt\n' > "$d/nb-1-big-TOC.txt"
    case "$shape" in
      one-good)        : ;;
      two-good)        printf 'x\n' > "$d/nb-2-big-Data.db"
                       printf 'Data.db\nTOC.txt\n' > "$d/nb-2-big-TOC.txt" ;;
      second-zero)     : > "$d/nb-2-big-Data.db"
                       printf 'Data.db\nTOC.txt\n' > "$d/nb-2-big-TOC.txt" ;;
      second-no-toc)   printf 'x\n' > "$d/nb-2-big-Data.db" ;;
      second-partial)  printf 'x\n' > "$d/nb-2-big-Data.db"
                       printf 'Data.db\nCompressionInfo.db\nTOC.txt\n' > "$d/nb-2-big-TOC.txt" ;;
      # A file that is NOT a descriptor the reader opens is not a generation at all, and
      # must not disqualify the table -- the round-24 over-rejection, one level down.
      junk-sibling)    printf 'x\n' > "$d/junk-Data.db" ;;
    esac
    local _o; _o=$(NO_AUTO_TOC=1 mrun "$gc" 2>&1 || true)
    if printf '%s' "$_o" | grep -q 'test_basic/counters'; then echo reject; else echo accept; fi
  }

  for shape in one-good two-good junk-sibling; do
    [ "$(gen_case "$shape")" = accept ] \
      && ok "generation shape '$shape' satisfies the table" \
      || bad "generation shape '$shape' was rejected, but it is a shape the reader handles"
  done
  for shape in second-zero second-no-toc second-partial; do
    [ "$(gen_case "$shape")" = reject ] \
      && ok "generation shape '$shape' disqualifies the table (the reader reads them all)" \
      || bad "generation shape '$shape' was masked by a good FIRST generation"
  done
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  # 69: the pinned-binary table (test_basic/simple_table names nb-1-big-Data.db).
  pin_case() {
    local shape=$1 pc d
    pc="$WORK/manifest-pin-$shape"; rm -rf "$pc"
    d="$pc/sstables/test_basic/simple_table-$UUID"; mkdir -p "$d"
    printf 'x\n'   > "$d/nb-1-big-Data.db"
    printf 'row\n' > "$d/nb-1-big-Data.db.jsonl"
    printf 'Data.db\nTOC.txt\n' > "$d/nb-1-big-TOC.txt"
    case "$shape" in
      pinned-only)   : ;;
      second-good)   printf 'x\n' > "$d/nb-2-big-Data.db"
                     printf 'Data.db\nTOC.txt\n' > "$d/nb-2-big-TOC.txt" ;;
      second-zero)   : > "$d/nb-2-big-Data.db"
                     printf 'Data.db\nTOC.txt\n' > "$d/nb-2-big-TOC.txt" ;;
      second-partial) printf 'x\n' > "$d/nb-2-big-Data.db"
                      printf 'Data.db\nFilter.db\nTOC.txt\n' > "$d/nb-2-big-TOC.txt" ;;
    esac
    local _o; _o=$(NO_AUTO_TOC=1 mrun "$pc" 2>&1 || true)
    if printf '%s' "$_o" | grep -q 'test_basic/simple_table'; then echo reject; else echo accept; fi
  }
  for pair in pinned-only:accept second-good:accept second-zero:reject second-partial:reject; do
    pin_shape=${pair%%:*}; pin_want=${pair#*:}
    [ "$(pin_case "$pin_shape")" = "$pin_want" ] \
      && ok "pinned-binary table, '$pin_shape' -> $pin_want" \
      || bad "pinned-binary table, '$pin_shape': expected $pin_want (a damaged sibling was masked by the pinned nb-1)"
  done

  # 70: the OA branch. Jest's guard needs a NUMERIC oa-<n>; a non-numeric OA name is
  # reader-supported (so pass 1 validates it) but does NOT satisfy the guard.
  oa_case() {
    local shape=$1 oc d
    oc="$WORK/manifest-oagen-$shape"; rm -rf "$oc"
    d="$oc/sstables/test_oa/simple_table-$UUID"; mkdir -p "$d"
    printf 'x\n'   > "$d/oa-1-big-Data.db"
    printf 'row\n' > "$d/oa-1-big-Data.db.jsonl"
    printf 'Data.db\nTOC.txt\n' > "$d/oa-1-big-TOC.txt"
    case "$shape" in
      one-oa)        : ;;
      second-oa-zero) : > "$d/oa-2-big-Data.db"
                      printf 'Data.db\nTOC.txt\n' > "$d/oa-2-big-TOC.txt" ;;
      nb-sibling-zero) : > "$d/nb-9-big-Data.db"      # reader-supported, not OA-named
                       printf 'Data.db\nTOC.txt\n' > "$d/nb-9-big-TOC.txt" ;;
      uuid-oa-zero)  : > "$d/oa-$UUID-big-Data.db"    # reader-supported, NOT numeric
                     printf 'Data.db\nTOC.txt\n' > "$d/oa-$UUID-big-TOC.txt" ;;
    esac
    local _o; _o=$(NO_AUTO_TOC=1 mrun "$oc" 2>&1 || true)
    if printf '%s' "$_o" | grep -q 'test_oa/simple_table'; then echo reject; else echo accept; fi
  }
  for pair in one-oa:accept second-oa-zero:reject nb-sibling-zero:reject uuid-oa-zero:reject; do
    oa_shape=${pair%%:*}; oa_want=${pair#*:}
    [ "$(oa_case "$oa_shape")" = "$oa_want" ] \
      && ok "OA table, '$oa_shape' -> $oa_want" \
      || bad "OA table, '$oa_shape': expected $oa_want (an unusable sibling generation was ignored)"
  done
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  # dg_case <keyspace/table> <table-dir-prefix> <shape> -> the FIRST diagnostic, trimmed
  dg_case() {
    local entry=$1 pfx=$2 shape=$3 root d ks tbl
    ks=${entry%%/*}; tbl=${entry#*/}
    root="$WORK/manifest-dg-$ks-$tbl-$shape"; rm -rf "$root"
    d="$root/sstables/$ks/$tbl-$UUID"; mkdir -p "$d"
    # A VALID generation is always present, so every reject below is caused by the SIBLING
    # and never by the table being empty.
    printf 'x\n' > "$d/$pfx-1-big-Data.db"
    printf 'Data.db\nTOC.txt\n' > "$d/$pfx-1-big-TOC.txt"
    case "$ks" in
      test_oa) printf 'row\n' > "$d/oa-1-big-Data.db.jsonl" ;;
      *)       printf 'row\n' > "$d/nb-1-big-Data.db.jsonl" ;;
    esac
    case "$shape" in
      valid-only)  : ;;
      dangling)    ln -s "$d/nowhere" "$d/$pfx-2-big-Data.db" ;;
      directory)   mkdir -p "$d/$pfx-2-big-Data.db" ;;
      zero-length) : > "$d/$pfx-2-big-Data.db"
                   printf 'Data.db\nTOC.txt\n' > "$d/$pfx-2-big-TOC.txt" ;;
    esac
    local _o; _o=$(NO_AUTO_TOC=1 mrun "$root" 2>&1 || true)
    printf '%s\n' "$_o" | grep -F "$entry" | head -1
  }

  # 71: each shape names ITS OWN cause.
  dg_out=$(dg_case test_basic/counters nb directory)
  printf '%s' "$dg_out" | grep -q 'NOT A REGULAR FILE' \
    && ok "a directory-valued Data.db is reported as NOT A REGULAR FILE, not as zero-length" \
    || bad "directory-valued Data.db misattributed: $dg_out"
  dg_out=$(dg_case test_basic/counters nb zero-length)
  printf '%s' "$dg_out" | grep -q 'ZERO-LENGTH' \
    && ok "a zero-length Data.db is still reported as ZERO-LENGTH (truncated fetch)" \
    || bad "zero-length Data.db misattributed: $dg_out"

  # 72: a dangling sibling rejects in EVERY branch -- general, pinned, OA.
  for spec in test_basic/counters:nb test_basic/simple_table:nb test_oa/simple_table:oa; do
    dg_entry=${spec%%:*}; dg_pfx=${spec#*:}
    # Control first: without the sibling the table is ACCEPTED, so the reject below is
    # attributable to the dangling sibling and not to a fixture this case builds wrong.
    if [ -z "$(dg_case "$dg_entry" "$dg_pfx" valid-only)" ]; then
      dg_out=$(dg_case "$dg_entry" "$dg_pfx" dangling)
      if printf '%s' "$dg_out" | grep -q 'NOT A REGULAR FILE'; then
        ok "$dg_entry: a DANGLING sibling generation disqualifies the table, and is named"
      else
        bad "$dg_entry: a dangling sibling was skipped or misnamed: ${dg_out:-<accepted>}"
      fi
    else
      bad "$dg_entry: the valid-only control was REJECTED, so the dangling case proves nothing"
    fi
  done
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  cause_msg() {   # <cause> -> the diagnostic's first 60 chars, or "" if none
    local cause=$1 root t ks=test_basic tb=counters
    case "$cause" in oa_bad) ks=test_oa; tb=simple_table ;; esac
    root="$WORK/manifest-cause-$cause"; rm -rf "$root"
    t="$root/sstables/$ks/$tb-$UUID"; mkdir -p "$t"
    case "$cause" in
      type_bad)   printf 'row\n' > "$t/nb-1-big-Data.db.jsonl"; mkdir -p "$t/nb-1-big-Data.db" ;;
      unread_bad) printf 'row\n' > "$t/nb-1-big-Data.db.jsonl"; printf 'x\n' > "$t/nb-1-big-Data.db"
                  chmod 000 "$t/nb-1-big-Data.db"; printf 'Data.db\nTOC.txt\n' > "$t/nb-1-big-TOC.txt" ;;
      empty_bad)  printf 'row\n' > "$t/nb-1-big-Data.db.jsonl"; : > "$t/nb-1-big-Data.db" ;;
      oa_bad)     printf 'row\n' > "$t/oa-1-big-Data.db.jsonl"; printf 'x\n' > "$t/nb-1-big-Data.db"
                  printf 'Data.db\nTOC.txt\n' > "$t/nb-1-big-TOC.txt" ;;
      name_bad)   printf 'row\n' > "$t/nb-1-big-Data.db.jsonl"; printf 'x\n' > "$t/junk-Data.db" ;;
      golden_bad) printf 'x\n' > "$t/nb-1-big-Data.db"; printf 'Data.db\nTOC.txt\n' > "$t/nb-1-big-TOC.txt" ;;
      toc_bad)    printf 'row\n' > "$t/nb-1-big-Data.db.jsonl"; printf 'x\n' > "$t/nb-1-big-Data.db"
                  printf 'Data.db\nFilter.db\nTOC.txt\n' > "$t/nb-1-big-TOC.txt" ;;
    esac
    local _o; _o=$(NO_AUTO_TOC=1 mrun "$root" 2>&1 || true)
    # The table-specific line, never the generic "missing Data.db" summary line.
    printf '%s\n' "$_o" | grep -F "$ks/$tb" | grep -v 'missing Data.db for expected table' \
      | head -1 | cut -c1-60
    chmod 0644 "$t/nb-1-big-Data.db" 2>/dev/null || true   # so _cleanup can remove it
  }

  cause_unreached=""; cause_msgs=""
  for cause in type_bad unread_bad empty_bad oa_bad name_bad golden_bad toc_bad; do
    cm=$(cause_msg "$cause")
    if [ -z "$cm" ]; then
      cause_unreached="$cause_unreached $cause"
    else
      cause_msgs="$cause_msgs$cm"$'\n'
    fi
  done
  [ -z "$cause_unreached" ] \
    && ok "all seven manifest failure causes are reachable" \
    || bad "manifest cause(s) went dark (no diagnostic emitted):$cause_unreached"

  cause_n=$(printf '%s' "$cause_msgs" | grep -c . || true)
  cause_u=$(printf '%s' "$cause_msgs" | sort -u | grep -c . || true)
  [ "$cause_n" -eq 7 ] && [ "$cause_u" -eq 7 ] \
    && ok "all seven causes emit DISTINCT diagnostics (no cause borrows another's message)" \
    || bad "manifest diagnostics are not pairwise distinct ($cause_u distinct of $cause_n emitted)"
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  empty_comp_case() {   # <component> -> "accept" | "reject"
    local comp=$1 ec d
    ec="$WORK/manifest-emptycomp-${comp%.db}"; rm -rf "$ec"
    d="$ec/sstables/test_basic/counters-$UUID"; mkdir -p "$d"
    printf 'x\n'   > "$d/nb-1-big-Data.db"
    printf 'row\n' > "$d/nb-1-big-Data.db.jsonl"
    printf 'Data.db\n%s\nTOC.txt\n' "$comp" > "$d/nb-1-big-TOC.txt"
    : > "$d/nb-1-big-$comp"                      # the component exists but is ZERO-LENGTH
    local _o; _o=$(NO_AUTO_TOC=1 mrun "$ec" 2>&1 || true)
    if printf '%s' "$_o" | grep -q 'test_basic/counters'; then echo reject; else echo accept; fi
  }

  # The two measured to produce a silently-wrong read, plus three that are merely tolerated
  # -- all rejected, because the rule is "nonempty unless allowlisted" and none of these was
  # ever observed empty in real data.
  for comp in CompressionInfo.db Statistics.db Filter.db Index.db Summary.db; do
    [ "$(empty_comp_case "$comp")" = reject ] \
      && ok "a zero-length $comp disqualifies the generation" \
      || bad "a zero-length $comp was accepted; the reader can return 0 rows over it"
  done
  # The ACCEPT control, and the reason the rule is an allowlist rather than a blanket:
  # without this a rule that rejected EVERY empty companion would pass the cases above while
  # red-lining three real BTI generations.
  [ "$(empty_comp_case Rows.db)" = accept ] \
    && ok "a zero-length Rows.db is still accepted (a real, measured BTI state)" \
    || bad "a zero-length Rows.db was rejected; that reds 3 generations of the real corpus"
fi

# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  # (a) the predicate itself, against the JS consumer's answer as the ORACLE.
  if command -v node >/dev/null 2>&1; then
    nl_re='^oa-[0-9]+-big-Data\.db$'
    nl_bad=""
    # label:value -- a plain name, the two-line attack, and a leading-newline variant.
    nl_plain='oa-1-big-Data.db'
    nl_two=$(printf 'oa-x\noa-1-big-Data.db')
    nl_lead=$(printf '\noa-1-big-Data.db')
    for nl_pair in "plain:$nl_plain" "embedded-newline:$nl_two" "leading-newline:$nl_lead"; do
      nl_lbl=${nl_pair%%:*}; nl_val=${nl_pair#*:}
      nl_js=$(node -e "process.stdout.write(/^oa-\d+-big-Data\.db$/.test(process.argv[1]) ? 'yes' : 'no')" "$nl_val" 2>/dev/null)
      nl_sh=$(NO_AUTO_TOC=1 bash -c '
        eval "$(sed -n "/^_re_match() {/,/^}/p" "$1")"
        _re_match "$2" "$3" && printf yes || printf no' _ "$MANIFEST_NOGIT" "$nl_re" "$nl_val")
      [ "$nl_js" = "$nl_sh" ] || nl_bad="$nl_bad $nl_lbl(js=$nl_js,sh=$nl_sh)"
    done
    [ -z "$nl_bad" ] \
      && ok "_re_match agrees with the JS consumer on newline-bearing values" \
      || bad "_re_match disagrees with the JS consumer:$nl_bad"
  else
    echo "info - node unavailable; skipping the whole-string match differential"
  fi

  # (b) a helper exiting with the RESERVED 9 must surface as a MALFUNCTION, never as the
  #     corpus verdict. Shadow `sort` so the committed-set derivation fails with exactly 9.
  #
  #     Uses MANIFEST_SRC, the REAL in-repo script, not the nogit copy: the derivation only
  #     runs when the script sits inside a git work tree, so the copy skips the block
  #     entirely and the case would pass while exercising nothing. That mistake was made
  #     first -- an out-of-git copy "proved" the guard worked when the guarded code had not
  #     run at all.
  #
  #     And the corpus must be one that would otherwise SUCCEED. Against an empty corpus a 9
  #     is the CORRECT verdict, so the assertion could not tell a right answer from a wrong
  #     one -- also got wrong on the first attempt.
  if [ -n "${CQLITE_DATASETS_ROOT:-}" ] && [ -d "${CQLITE_DATASETS_ROOT:-}" ] \
     && bash "$MANIFEST_SRC" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1; then
    nine_bin="$WORK/nine-bin"; mkdir -p "$nine_bin"
    printf '#!/bin/sh\nexit 9\n' > "$nine_bin/sort"; chmod +x "$nine_bin/sort"
    nine_rc=0
    PATH="$nine_bin:$PATH" bash "$MANIFEST_SRC" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1 || nine_rc=$?
    [ "$nine_rc" = 2 ] \
      && ok "a helper exiting 9 is reported as a MALFUNCTION (exit 2), not the corpus verdict" \
      || bad "a helper exiting 9 gave exit $nine_rc; 9 would be read as a judged verdict and suppressed by the opt-out"
  else
    echo "info - no succeeding real corpus available; skipping the reserved-exit case"
  fi
fi

# ---------------------------------------------------------------------------
# Cases 77-78 (post-rebase review): prefix collision, and a TRUNCATED TOC.
#
# 77. Production discovery is a bare `filename.ends_with("-Data.db")`, and
#     `SSTableComponent::from_filename` maps ANY such name to the Data component -- so a
#     file sharing a REAL generation's prefix is read as that generation's Data component.
#     Measured with garbage bytes beside a healthy nb-1:
#       nb-1-big-Foo-Data.db -> the query THROWS   (shares the prefix)
#       junk-Data.db         -> 100 rows           (discovered, open fails, SKIPPED)
#       nb-9-big-Data.db     -> 100 rows           (valid descriptor, other generation)
#       xx-1-big / nb-foo-big -> 100 rows
#     So the fatal shape is NARROW. Rejecting every odd `*-Data.db` would red on input the
#     reader demonstrably tolerates, which is why the accept controls are here.
#
# 78. TOC validation used the TOC as the required set, trusting it as a COMPLETE inventory.
#     A truncated-but-nonempty TOC listing only `Data.db` then passed, and the components
#     this check exists to catch went missing unopposed.
# ---------------------------------------------------------------------------
if [ -n "$MANIFEST_NOGIT" ]; then
  pc_case() {   # <shape> -> accept | reject
    local shape=$1 root t
    root="$WORK/manifest-pc-$shape"; rm -rf "$root"
    t="$root/sstables/test_basic/counters-$UUID"; mkdir -p "$t"
    printf 'x\n'   > "$t/nb-1-big-Data.db"
    printf 'row\n' > "$t/nb-1-big-Data.db.jsonl"
    printf 'Data.db\nStatistics.db\nFilter.db\nDigest.crc32\nTOC.txt\n' > "$t/nb-1-big-TOC.txt"
    for _m in Statistics.db Filter.db Digest.crc32; do printf 'x\n' > "$t/nb-1-big-$_m"; done
    case "$shape" in
      clean)            : ;;
      junk-sibling)     printf 'x\n' > "$t/junk-Data.db" ;;
      other-generation) printf 'x\n' > "$t/nb-9-big-Data.db"
                        printf 'Data.db\nStatistics.db\nFilter.db\nDigest.crc32\nTOC.txt\n' > "$t/nb-9-big-TOC.txt"
                        for _m in Statistics.db Filter.db Digest.crc32; do printf 'x\n' > "$t/nb-9-big-$_m"; done ;;
      prefix-collision) printf 'x\n' > "$t/nb-1-big-Foo-Data.db" ;;
      truncated-toc)    printf 'Data.db\n' > "$t/nb-1-big-TOC.txt" ;;
      toc-omits-stats)  printf 'Data.db\nTOC.txt\n' > "$t/nb-1-big-TOC.txt"
                        rm -f "$t/nb-1-big-Statistics.db" ;;
    esac
    local _o; _o=$(NO_AUTO_TOC=1 mrun "$root" 2>&1 || true)
    if printf '%s' "$_o" | grep -q 'test_basic/counters'; then echo reject; else echo accept; fi
  }

  # ACCEPT controls first: without them a manifest that rejected everything would pass the
  # reject cases and look correct, and these three are shapes the reader TOLERATES.
  for shape in clean junk-sibling other-generation; do
    [ "$(pc_case "$shape")" = accept ] \
      && ok "generation shape '$shape' is accepted (the reader tolerates it)" \
      || bad "generation shape '$shape' was rejected, but the reader tolerates it"
  done
  # NOTE ON WHAT THIS PAIR PROVES. The reject verdict alone is NOT discriminating: the
  # bidirectional TOC check independently disqualifies a colliding file (it shares the
  # prefix and is not TOC-listed), so deleting the dedicated collision branch leaves this
  # assertion GREEN. Verified by planting exactly that. The DIAGNOSTIC assert below is the
  # one that discriminates, and it is why the branch is kept -- without it the operator is
  # told "no Data.db the reader would open" about a directory whose Data.db is fine.
  [ "$(pc_case prefix-collision)" = reject ] \
    && ok "a *-Data.db sharing a real generation's prefix disqualifies the table" \
    || bad "a prefix-colliding *-Data.db was accepted; the reader maps it to that generation and FAILS"
  # The collision must be NAMED, not reported as some other cause.
  pc_out=$(NO_AUTO_TOC=1 mrun "$WORK/manifest-pc-prefix-collision" 2>&1 || true)
  printf '%s' "$pc_out" | grep -q "SHARES a real generation's prefix" \
    && ok "the prefix-collision diagnostic names its own cause" \
    || bad "the prefix collision was reported as something else: $(printf '%s' "$pc_out" | grep 'test_basic/counters' | head -1)"

  for shape in truncated-toc toc-omits-stats; do
    [ "$(pc_case "$shape")" = reject ] \
      && ok "TOC shape '$shape' disqualifies the table (the TOC is not a trusted inventory)" \
      || bad "TOC shape '$shape' was accepted; a truncated TOC shrank the required set"
  done
fi

# ---------------------------------------------------------------------------
# Case 79 (post-rebase round 2, High): a COHERENTLY truncated TOC.
#
# Both derived directions — every listed component exists, every prefix-sharing file is
# listed — are computed FROM THE CORPUS, so a TOC shortened IN STEP with the files it stopped
# listing satisfies both and greens an incomplete generation.
#
# The trusted inventory is the GIT-TRACKED `*-TOC.txt` (164 committed). It is not subject to
# the truncated fetch that damages the gitignored binaries, which is exactly what a
# corpus-derived check cannot be. Measured: all 144 generations in the machine-local corpus
# have a committed twin and all 144 match byte-for-byte.
#
# Driven against a COPY OF THE REAL CORPUS, because the property is about agreement with the
# committed tree — a synthetic table has no committed twin and so cannot exercise it.
# ---------------------------------------------------------------------------
if [ -n "${CQLITE_DATASETS_ROOT:-}" ] && [ -d "${CQLITE_DATASETS_ROOT:-}/sstables" ] \
   && bash "$MANIFEST_SRC" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1; then
  ct_root="$WORK/coherent-toc"; rm -rf "$ct_root"; mkdir -p "$ct_root"
  cp -r "$CQLITE_DATASETS_ROOT/sstables" "$ct_root/" 2>/dev/null
  ct_dir=$(find "$ct_root/sstables/test_basic" -maxdepth 1 -type d -name 'simple_table-*' | head -1)
  if [ -n "$ct_dir" ] && [ -f "$ct_dir/nb-1-big-TOC.txt" ]; then
    # Control: the untouched copy still passes, so a reject below is caused by the edit.
    if bash "$MANIFEST_SRC" "$ct_root" >/dev/null 2>&1; then
      ok "the corpus copy is a valid control (passes before the TOC is truncated)"
    else
      bad "the corpus copy did not pass unmodified; the coherent-truncation case proves nothing"
    fi
    # Shorten the TOC *and* delete exactly the components it stopped listing.
    printf 'Data.db\nStatistics.db\nDigest.crc32\nTOC.txt\n' > "$ct_dir/nb-1-big-TOC.txt"
    rm -f "$ct_dir"/nb-1-big-CompressionInfo.db "$ct_dir"/nb-1-big-Filter.db \
          "$ct_dir"/nb-1-big-Index.db "$ct_dir"/nb-1-big-Summary.db
    ct_out=$(bash "$MANIFEST_SRC" "$ct_root" 2>&1 || true)
    if printf '%s' "$ct_out" | grep -q 'test_basic/simple_table'; then
      ok "a COHERENTLY truncated TOC disqualifies the generation"
    else
      bad "a coherently truncated TOC was ACCEPTED; both derived directions are satisfied by it"
    fi
    # And it must NAME the trusted-inventory mismatch, not the generic listed-component cause.
    if printf '%s' "$ct_out" | grep -q 'does NOT match the git-tracked committed twin'; then
      ok "the coherent-truncation diagnostic names the committed-twin mismatch"
    else
      bad "coherent truncation was reported as some other cause: $(printf '%s' "$ct_out" | grep 'test_basic/simple_table' | head -1 | cut -c1-90)"
    fi
  else
    echo "info - no simple_table generation with a TOC in the corpus copy; skipping"
  fi
else
  echo "info - no passing real corpus available; skipping the coherent-truncation case"
fi

# ---------------------------------------------------------------------------
# Case 80 (post-rebase round 2, Medium): a broken grep in the TOC reconciliation must be a
# MALFUNCTION, not an incomplete corpus.
#
# `grep -qxF ... || return 1` collapsed an OPERATIONAL failure (>1) onto "not listed", and
# because the function is called on the left of `||`, that walked out to the script's
# RESERVED exit 9 — a judged corpus verdict the #2078 opt-out suppresses. A broken grep must
# never be readable as a judged corpus.
#
# The shadow delegates every other grep to the real one and fails ONLY the `-qxF` form the
# reconciliation uses: shadowing grep wholesale would break the script's own tool check and
# prove nothing about this path.
# ---------------------------------------------------------------------------
if [ -n "${CQLITE_DATASETS_ROOT:-}" ] && [ -d "${CQLITE_DATASETS_ROOT:-}/sstables" ] \
   && bash "$MANIFEST_SRC" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1; then
  gb_bin="$WORK/grepfail-bin"; mkdir -p "$gb_bin"
  gb_real=$(command -v grep)
  cat >"$gb_bin/grep" <<GREPFAIL
#!/bin/sh
# Fail ONLY the reconciliation's exact invocation; delegate everything else.
if [ "\$1" = "-qxF" ]; then exit 2; fi
exec "$gb_real" "\$@"
GREPFAIL
  chmod +x "$gb_bin/grep"
  gb_rc=0
  PATH="$gb_bin:$PATH" bash "$MANIFEST_SRC" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1 || gb_rc=$?
  if [ "$gb_rc" = 2 ]; then
    ok "a grep malfunction in the TOC reconciliation exits 2, not the reserved corpus verdict"
  elif [ "$gb_rc" = 9 ]; then
    bad "a grep malfunction surfaced AS exit 9; the opt-out would suppress a broken checker"
  else
    bad "a grep malfunction gave exit $gb_rc; expected 2 (malfunction)"
  fi
else
  echo "info - no passing real corpus available; skipping the grep-malfunction case"
fi

# ---------------------------------------------------------------------------
# Case 81 (post-rebase round 3, High): the trusted inventory must come from HEAD, not from
# the working tree.
#
# The first version compared the corpus TOC against the working-tree file at the mapped
# path. Under the DEFAULT dataset root — the checkout's own `test-data/datasets`, which is
# the documented fetch target — those are THE SAME FILE, so `cmp` compared a file to itself,
# always succeeded, and the whole trusted-inventory check was VACUOUS in exactly the
# configuration most likely to be used.
#
# Asserted on the function directly, because that isolates the property: modify the WORKING
# TREE copy of a tracked TOC and require a mismatch. A whole-corpus case cannot reach it
# without populating the checkout with gitignored binaries.
#
# The working-tree file is restored, and the case FAILS LOUDLY rather than leaving the
# checkout dirty if it cannot be.
# ---------------------------------------------------------------------------
tw_repo=$(cd "$(dirname "$GATE")/.." && pwd)
tw_toc=$(git -C "$tw_repo" ls-files test-data/datasets/sstables 2>/dev/null | grep 'TOC\.txt$' | head -1)
if [ -n "$tw_toc" ] && [ -f "$tw_repo/$tw_toc" ] && git -C "$tw_repo" diff --quiet -- "$tw_toc" 2>/dev/null; then
  tw_probe() {   # -> "match" | "mismatch"
    _SCRIPT_REPO="$tw_repo" bash -c '
      _SCRIPT_REPO=$1
      eval "$(sed -n "/^_toc_matches_head() {/,/^}/p" "$2")"
      _toc_matches_head "$3" "$4" && printf match || printf mismatch' \
      _ "$tw_repo" "$MANIFEST_SRC" "$tw_repo/$tw_toc" "$tw_toc"
  }
  tw_save="$WORK/tw-save"; cp "$tw_repo/$tw_toc" "$tw_save"
  # Control first: unmodified, it must MATCH — otherwise the mismatch below proves nothing.
  tw_before=$(tw_probe)
  printf 'Data.db\nTOC.txt\n' > "$tw_repo/$tw_toc"
  tw_after=$(tw_probe)
  cp "$tw_save" "$tw_repo/$tw_toc"
  if ! git -C "$tw_repo" diff --quiet -- "$tw_toc" 2>/dev/null; then
    bad "case 81 could not restore $tw_toc; the checkout is DIRTY and must be fixed by hand"
  elif [ "$tw_before" = match ] && [ "$tw_after" = mismatch ]; then
    ok "the trusted inventory is read from HEAD, so a working-tree truncation is detected"
  elif [ "$tw_before" != match ]; then
    bad "case 81's control failed: an unmodified tracked TOC did not match HEAD ($tw_before)"
  else
    bad "a working-tree TOC truncation was NOT detected — the twin comparison is vacuous when the corpus root IS the checkout"
  fi
else
  echo "info - no clean tracked TOC.txt available; skipping the HEAD-inventory case"
fi

# ---------------------------------------------------------------------------
# Case 82 (post-rebase round 3, Medium): a broken `cmp` is a MALFUNCTION, not a TOC mismatch.
#
# `cmp -s ... || return 1` collapsed status >1 (unreadable file, cmp absent) onto "the TOCs
# differ", which walks out to the reserved exit 9 — a judged corpus verdict the #2078 opt-out
# suppresses. Exactly the class case 80 covers for `grep`; this one was added because the
# first RED for it did not fire, which is how the gap showed.
# ---------------------------------------------------------------------------
if [ -n "${CQLITE_DATASETS_ROOT:-}" ] && [ -d "${CQLITE_DATASETS_ROOT:-}/sstables" ] \
   && bash "$MANIFEST_SRC" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1; then
  cb_bin="$WORK/cmpfail-bin"; mkdir -p "$cb_bin"
  # `cmp` is used ONLY by the twin comparison here, so a blanket shadow is precise enough --
  # unlike grep, which the script's own tool check also calls.
  printf '#!/bin/sh\nexit 2\n' > "$cb_bin/cmp"; chmod +x "$cb_bin/cmp"
  cb_rc=0
  PATH="$cb_bin:$PATH" bash "$MANIFEST_SRC" "$CQLITE_DATASETS_ROOT" >/dev/null 2>&1 || cb_rc=$?
  if [ "$cb_rc" = 2 ]; then
    ok "a cmp malfunction exits 2, not the reserved corpus verdict"
  elif [ "$cb_rc" = 9 ]; then
    bad "a cmp malfunction surfaced AS exit 9; the opt-out would suppress a broken checker"
  else
    bad "a cmp malfunction gave exit $cb_rc; expected 2 (malfunction)"
  fi
else
  echo "info - no passing real corpus available; skipping the cmp-malfunction case"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
