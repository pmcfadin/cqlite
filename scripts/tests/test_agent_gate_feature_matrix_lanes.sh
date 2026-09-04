#!/usr/bin/env bash
#
# test_agent_gate_feature_matrix_lanes.sh — planted-break observation harness for the
# four feature-matrix gate lanes added by issue #1699 (design decision D5).
#
# WHAT THIS ANSWERS, and why nothing else does.
# `--list` proves a lane is REGISTERED. A green SUMMARY line proves a lane RAN and
# found nothing. Neither proves the lane CAN FAIL. `feature-iso-parquet` reporting
# `PASS (0s)` is indistinguishable, from the SUMMARY alone, from a lane that compiles
# nothing and always exits 0. This harness is the only affirmative evidence that each
# lane fires on the incident class it was built for — issue #1699's AC2 deliverable.
#
# BOTH DIRECTIONS, ALWAYS. Per lane it runs the real component on a CLEAN tree
# (expecting the lane's PASS) and then on a tree carrying ONE planted break
# (expecting the lane's FAIL). A harness that only ever plants breaks passes just as
# happily against a lane that fails unconditionally — the vacuous-guard shape of
# #3229 — so a lane that reds in BOTH directions is reported as a HARNESS FAILURE,
# never as a successful observation.
#
# ATTRIBUTED, NOT MERELY RED. A lane that broke for an unrelated reason produces the
# same exit code and the same SUMMARY line as one that detected the plant, so a bare
# red is not evidence either. Each planted run's output must NAME the planted symbol;
# a red that does not is reported as FIRED-UNATTRIBUTED and fails the harness.
#
# THE REAL COMPONENT, NEVER A RETYPED COMMAND. Each direction runs
# `bash scripts/agent-gate.sh --only <lane>`. Retyping the lane's cargo invocation
# here would prove that a cargo command works; it would prove nothing about the gate
# component, which is the subject. `--only` is a PARTIAL run (it can never be a
# verdict — it exits 3 on success for exactly that reason) and is deliberately
# lenient about fixtures, which is what makes it usable as a probe.
#
# ISOLATION. Every mutation happens in a throwaway `git worktree add --detach` copy,
# never the live checkout. #2926 makes a mid-run tree mutation a gate FAIL, so a
# harness that edited the tree its own gate was running in would be the very defect
# it exists to catch. Plants are applied and reverted BETWEEN runs, never during one.
# That isolation is VERIFIED, not asserted: the live checkout's `git status --porcelain`
# is captured at start and re-compared before the summary and again in cleanup, and any
# difference is a HARNESS FAILURE. It also REFUSES to run from a dirty checkout — the
# worktree is created from committed HEAD, so uncommitted changes would be silently
# excluded and a PASS would describe code other than the code in front of you.
# The copy gets its own CARGO_TARGET_DIR so the clean and planted runs of a lane
# share compilation.
#
# OPT-IN, NOT A GATE COMPONENT (D5). It performs real compiles; taxing every full
# gate to re-prove a static property is disproportionate. It is deliberately absent
# from COMPONENTS / LITE_COMPONENTS / DELTA_COMPONENTS. Nightly `gate.yml`
# enrollment is out of scope (a workflow change needs #2910 registry enrollment).
#
# ONE LANE CAN NEED SEVERAL CASES (#3725). feature-iso-delta-scan stopped being
# compile-only, so its single #1978 coupling plant no longer covers it: that plant fires on
# a lane that merely COMPILES, while EXECUTION, a DERIVED target set, a NON-ZERO test count
# per target and a STRICT fixture posture are each invisible to it. Cases are declared per
# lane in `CASES_<lane_fn>` and counted against `CASE_FLOOR`, so a case list that silently
# empties reds instead of reporting "every lane fired" over an observation nobody made.
#
# Usage:
#   bash scripts/tests/test_agent_gate_feature_matrix_lanes.sh              # all four lanes
#   bash scripts/tests/test_agent_gate_feature_matrix_lanes.sh <lane> ...   # subset
#
# Exit: 0 = every selected lane observed to fire AND to stay clean; 1 = a lane did not
# fire, the harness could not establish a clean baseline, or the live checkout was
# mutated during the run; 2 = usage / precondition refusal (unknown lane, no absolute
# CQLITE_DATASETS_ROOT, DIRTY live checkout); 3 = a SUBSET run in which everything
# selected fired (a partial observation, never the full AC2 evidence).

set -uo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel) || { echo "not a git checkout" >&2; exit 1; }
cd "$REPO_ROOT" || exit 1

ALL_LANES=(feature-iso-parquet feature-iso-delta-scan legacy-heuristics flight-tests)

SUBSET=0
if [ "$#" -gt 0 ]; then
  SUBSET=1
  LANES=("$@")
  for l in "${LANES[@]}"; do
    case " ${ALL_LANES[*]} " in
      *" $l "*) ;;
      *) echo "unknown lane: $l (known: ${ALL_LANES[*]})" >&2; exit 2 ;;
    esac
  done
else
  LANES=("${ALL_LANES[@]}")
fi

# The corpus root — required ONLY BY THE LANES THAT CONSUME FIXTURES (roborev round-3
# finding, Low). flight-tests and legacy-heuristics are DATASET_COMPONENTS, and the
# throwaway worktree has none of the gitignored Data.db binaries, so for those the root
# must be an absolute path to a real corpus. The two feature-isolation lanes are
# COMPILE-ONLY (`cargo test --lib --no-run`) and are not in DATASET_COMPONENTS, so
# demanding a corpus for a subset naming only those made the documented subset mode
# unusable in a fixture-less checkout — a precondition unrelated to what was selected.
DATASETS="${CQLITE_DATASETS_ROOT:-}"
_needs_datasets=0
# feature-iso-delta-scan JOINED THIS LIST on #3725: it stopped being compile-only and now
# EXECUTES 13 dataset-consuming parity targets, and its corpus_absent case additionally
# needs the REAL corpus's canonical keyspace to build a canonical-only one from.
for _l in ${LANES[@]+"${LANES[@]}"}; do
  case "$_l" in flight-tests|legacy-heuristics|feature-iso-delta-scan) _needs_datasets=1 ;; esac
done
if [ "$_needs_datasets" -eq 1 ]; then
  if [ -z "$DATASETS" ]; then
    echo "CQLITE_DATASETS_ROOT is not set, and the selected lanes include a" >&2
    echo "dataset-consuming lane (flight-tests / legacy-heuristics /" >&2
    echo "feature-iso-delta-scan). Export the absolute root printed by:" >&2
    echo "  bash test-data/scripts/fetch-datasets.sh" >&2
    echo "Or select only feature-iso-parquet, the ONE remaining compile-only" >&2
    echo "lane, which needs no corpus. feature-iso-delta-scan stopped being" >&2
    echo "compile-only on #3725 -- it EXECUTES dataset-consuming parity" >&2
    echo "targets." >&2
    exit 2
  fi
  case "$DATASETS" in
    /*) ;;
    *) echo "CQLITE_DATASETS_ROOT must be ABSOLUTE (got: $DATASETS)" >&2; exit 2 ;;
  esac
else
  # A root is still VALIDATED if one is present — an exported relative root is a
  # fail-closed error everywhere else in this repo (#3148) and must not become
  # acceptable just because this run does not read it.
  case "${DATASETS:-/}" in
    /*) ;;
    *) echo "CQLITE_DATASETS_ROOT must be ABSOLUTE (got: $DATASETS)" >&2; exit 2 ;;
  esac
  echo "note: no dataset-consuming lane selected; CQLITE_DATASETS_ROOT not required" >&2
fi

# THE SUBJECT MUST BE THE CODE IN FRONT OF YOU (roborev round-2 finding 1). The
# throwaway worktree is created from committed HEAD, so any UNCOMMITTED lane change in
# the live checkout is silently EXCLUDED from every run — the harness would then report
# a successful observation of code that is not the code being reviewed, which is worse
# than no observation. Refuse rather than mislead; the remedy is a commit, not a flag.
# The COMMIT is captured alongside the status, and both are re-verified (roborev round-6
# finding, Medium). `git status --porcelain` alone cannot see a HEAD move: a clean commit,
# `checkout`, `reset --hard` or branch switch during this multi-minute harness leaves the
# status IDENTICAL while the code under observation changes. The harness would then have
# observed the OLD tree and reported the NEW one as successfully observed — a stale
# certification that reads exactly like a fresh one, which is the #2926 tree-integrity
# hazard reproduced inside the tool that exists to prove the lanes work.
LIVE_HEAD_BEFORE=$(git -C "$REPO_ROOT" rev-parse HEAD) || {
  echo "HARNESS FAILURE: could not read the live checkout's HEAD, so the" >&2
  echo "  never-mutate-the-live-checkout invariant is unverifiable from the start." >&2
  exit 2
}
LIVE_STATUS_BEFORE=$(git -C "$REPO_ROOT" status --porcelain) || {
  echo "FATAL: could not read the live checkout's git status" >&2; exit 1; }
if [ -n "$LIVE_STATUS_BEFORE" ]; then
  {
    echo "REFUSING TO RUN: the live checkout is DIRTY."
    echo
    echo "  This harness observes the lanes against a throwaway worktree created from"
    echo "  committed HEAD ($(git -C "$REPO_ROOT" rev-parse --short HEAD)), so uncommitted"
    echo "  changes are NOT part of any run. A PASS here would describe HEAD, not the"
    echo "  working tree you are looking at — an observation about the wrong code."
    echo
    echo "  Commit (or stash) first, then re-run. There is deliberately no override:"
    echo "  the only thing an override could buy is a green about code nobody changed."
    echo
    echo "  dirty paths:"
    printf '%s\n' "$LIVE_STATUS_BEFORE" | sed 's/^/    /'
  } >&2
  exit 2
fi

# assert_live_checkout_untouched <phase>: re-verify the harness's OWN stated invariant
# — "the live checkout is never mutated" — rather than merely asserting it in prose
# (roborev round-2 finding 1; delta spec Requirement 5). Called before reporting
# success AND from cleanup, so a mutation is caught on every exit path. A difference is
# a HARNESS FAILURE: every plant is supposed to land in the throwaway worktree, so a
# changed live tree means a plant (or a gate run) escaped its isolation, and #2926
# makes exactly that a gate FAIL for everyone else in this checkout.
LIVE_TREE_VIOLATED=0
assert_live_checkout_untouched() {
  local phase="$1" now now_head
  now=$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null) || {
    echo "HARNESS FAILURE ($phase): could not re-read the live checkout's git status," >&2
    echo "  so the never-mutate-the-live-checkout invariant is UNVERIFIABLE here." >&2
    LIVE_TREE_VIOLATED=1
    return 1
  }
  now_head=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null) || {
    echo "HARNESS FAILURE ($phase): could not re-read the live checkout's HEAD," >&2
    echo "  so the invariant is UNVERIFIABLE here (unreadable is never treated as unchanged)." >&2
    LIVE_TREE_VIOLATED=1
    return 1
  }
  if [ "$now_head" != "$LIVE_HEAD_BEFORE" ]; then
    {
      echo "HARNESS FAILURE ($phase): the live checkout's HEAD MOVED during the run"
      echo "  ($LIVE_HEAD_BEFORE -> $now_head). git status alone cannot see this: a clean"
      echo "  commit, checkout, reset or branch switch leaves it identical. The observation"
      echo "  above was made against the OLD commit, so reporting it as this HEAD's would be"
      echo "  a STALE certification indistinguishable from a fresh one."
    } >&2
    LIVE_TREE_VIOLATED=1
    return 1
  fi
  if [ "$now" != "$LIVE_STATUS_BEFORE" ]; then
    {
      echo "HARNESS FAILURE ($phase): the LIVE CHECKOUT changed during the run."
      echo "  Every plant must land in the throwaway worktree; a mutated live tree means"
      echo "  one escaped isolation (and #2926 makes a mid-run tree mutation a gate FAIL)."
      echo "  live git status now:"
      printf '%s\n' "$now" | sed 's/^/    /'
    } >&2
    LIVE_TREE_VIOLATED=1
    return 1
  fi
  return 0
}

WORK=$(mktemp -d "${TMPDIR:-/tmp}/ah6-1699-XXXXXX") || exit 1
TREE="$WORK/tree"
# Stable, OUTSIDE the throwaway tree: the clean and planted runs of a lane must share
# compilation (otherwise the planted run pays a full cold build), and a target dir
# inside the copy would be swept by the revert.
#
# PRIVATE, NOT A PREDICTABLE SHARED PATH (roborev round-30, Medium). This used to be a fixed
# `${TMPDIR:-/tmp}/ah6-1699-target`, which had two problems: two harnesses running at once
# corrupted each other's incremental state, and on a multi-user host another user could
# pre-create that directory and the harness would then EXECUTE compiled artifacts out of a
# directory they controlled. It lives under $WORK (itself an `mktemp -d`), so it is unique per
# invocation and removed with everything else by `cleanup` — while still being SHARED between the
# clean and planted runs of one invocation, which is the property that keeps the observation
# affordable (each lane compiles once rather than twice).
TARGET="$WORK/target"

cleanup() {
  local rc=$?
  # A SIGNAL FORCES A NON-ZERO VERDICT (roborev round-29, Medium). `cleanup` inherits `$?`, and on a
  # signal delivered BETWEEN commands — or delivered to the harness alone while its child exited 0 —
  # `$?` is ZERO, so an INTERRUPTED observation exited SUCCESSFULLY. That is this issue's own defect
  # class inside its own harness: a run that did not finish reporting the verdict of one that did.
  # The signal traps therefore pass an explicit status (128+signo, the shell convention) and only
  # EXIT relies on `$?`.
  local sig="${1:-}"
  case "$sig" in
    INT)  rc=130 ;;
    TERM) rc=143 ;;
  esac
  [ -n "$sig" ] && echo "FATAL: observation ABORTED by SIG$sig — this run is NOT evidence (rc=$rc)" >&2
  # Verify the invariant on EVERY exit path, including an interrupted run: a plant that
  # escaped into the live checkout is exactly what an aborted run is most likely to
  # leave behind, and it must never be discovered later by somebody else's gate.
  assert_live_checkout_untouched "cleanup" || rc=1
  if [ -d "$TREE" ]; then
    git -C "$REPO_ROOT" worktree remove --force "$TREE" >/dev/null 2>&1 \
      || rm -rf "$TREE"
  fi
  git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1
  rm -rf "$WORK"
  exit "$rc"
}
trap 'cleanup' EXIT
trap 'cleanup INT' INT
trap 'cleanup TERM' TERM

echo "==== AH6 FEATURE-MATRIX LANE OBSERVATION (issue #1699, design D5) ===="
echo "repo:      $REPO_ROOT"
echo "head:      $(git rev-parse HEAD)"
echo "lanes:     ${LANES[*]}"
[ "$SUBSET" -eq 1 ] && echo "mode:      SUBSET (partial observation — not the full AC2 evidence)"
echo "worktree:  $TREE (throwaway; the live checkout is never mutated)"
echo "target:    $TARGET"
echo "datasets:  $DATASETS"
echo

# `$LIVE_HEAD_BEFORE`, not `HEAD`: the copy must be the exact commit whose observation
# this run reports. `HEAD` is re-resolved at this moment and would silently follow a
# concurrent commit (roborev round-6 finding).
git worktree add --detach "$TREE" "$LIVE_HEAD_BEFORE" >/dev/null 2>&1 || {
  echo "FATAL: could not create the throwaway worktree" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Plants. Each is the lane's INCIDENT CLASS, not a syntax error: a syntax error
# fires any lane that compiles anything and so distinguishes nothing.
# ---------------------------------------------------------------------------

# The two isolation plants are mirror images. Each adds a pair of crate-root items to
# cqlite-core/src/lib.rs: one gated on feature A referencing one gated on feature B.
# With BOTH features on (clippy's cqlite-core arm, which enables ~30 features at once)
# the pair compiles — which is precisely what MASKS this class today. With only A on,
# B's item does not exist and the reference fails to resolve. This is #1978's class.
# They live in cqlite-core/src because the isolation lanes run `cargo test --lib
# --no-run`, which pulls in no integration target.
#
# BOTH ITEMS ARE ALSO GATED ON `test` (roborev round-19, and the same gap the C re-audit
# scored against R4). Without that, the plants are ordinary library items, so a bare
# `cargo check --lib` would ALSO catch them — and then the observation, while red, would
# not DISCRIMINATE the shipped instrument (`cargo test --lib --no-run`) from the one the
# spec forbids. It would have been a true observation of a claim nobody doubted, standing
# in for the claim that actually matters. `cfg(test)` code is compiled only under
# `--test`, so with the `test` gate the plant is INVISIBLE to `cargo check --lib` and
# visible to the lane — which is #1978's shape exactly (an ungated `#[cfg(test)]` module
# referencing a feature-gated item), not merely its neighbourhood.
#
# MEASURED both directions on the planted tree before this text was written; the figures
# are in docs/reports/ah6-1699-feature-matrix-lanes.md.
plant_feature_iso_parquet_coupling() {
  cat >> "$TREE/cqlite-core/src/lib.rs" <<'EOF'

// AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[cfg(all(test, feature = "parquet"))]
pub fn ah6_planted_parquet_probe() -> bool {
    crate::ah6_planted_delta_scan_marker()
}
#[cfg(all(test, feature = "delta-scan"))]
pub fn ah6_planted_delta_scan_marker() -> bool {
    true
}
EOF
}
plant_marker_feature_iso_parquet_coupling='ah6_planted_delta_scan_marker'
plant_desc_feature_iso_parquet_coupling='a #[cfg(all(test, feature = "parquet"))] fn in cqlite-core/src/lib.rs calling a #[cfg(all(test, feature = "delta-scan"))] fn (compiles with both features on; unresolved with parquet alone; INVISIBLE to cargo check --lib because it is cfg(test)) — #1978 class exactly'

plant_feature_iso_delta_scan_coupling() {
  cat >> "$TREE/cqlite-core/src/lib.rs" <<'EOF'

// AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[cfg(all(test, feature = "delta-scan"))]
pub fn ah6_planted_delta_scan_probe() -> bool {
    crate::ah6_planted_parquet_marker()
}
#[cfg(all(test, feature = "parquet"))]
pub fn ah6_planted_parquet_marker() -> bool {
    true
}
EOF
}
plant_marker_feature_iso_delta_scan_coupling='ah6_planted_parquet_marker'
plant_desc_feature_iso_delta_scan_coupling='the mirror: a #[cfg(all(test, feature = "delta-scan"))] fn in cqlite-core/src/lib.rs calling a #[cfg(all(test, feature = "parquet"))] fn — likewise cfg(test), so likewise invisible to cargo check --lib'

# A NEW file, deliberately. It does double duty: it proves the lane EXECUTES rather
# than merely compiles (a compile-only lane stays green on a failing assertion), and
# it proves the lane's DERIVED target set picks up a sixth gated file with no gate
# edit — a hard-coded list would ignore it and the lane would stay green.
plant_legacy_heuristics_gatedtest() {
  cat > "$TREE/cqlite-core/tests/ah6_planted_legacy.rs" <<'EOF'
//! AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[cfg(feature = "legacy-heuristics")]
#[test]
fn ah6_planted_legacy_heuristics_break() {
    assert_eq!(1 + 1, 3, "AH6 planted break for the legacy-heuristics lane");
}
EOF
}
plant_marker_legacy_heuristics_gatedtest='ah6_planted_legacy_heuristics_break'
plant_desc_legacy_heuristics_gatedtest='a NEW cqlite-core/tests/ah6_planted_legacy.rs carrying a #[cfg(feature = "legacy-heuristics")] #[test] with an inverted assertion (also proves the DERIVED target set picks up a sixth gated file with no gate edit)'

# A failing UNIT test in a NEW cqlite-flight/src module — appended to src/lib.rs, so it
# lands in the `--lib` binary the lane now executes.
#
# WHY THIS PLANT CHANGED (issue #3384). It used to be a NEW cqlite-flight/tests/*.rs
# integration target, chosen to prove the lane reached past the three targets already
# covered elsewhere (query_semantics_flight_parity, issue_3095_flight_static_columns,
# memory-budget's dhat target). The lane no longer executes integration targets at all:
# that half of the package is ~50% non-deterministic under intra-package parallelism, so
# the lane narrowed to `--lib --bins` and DECLARES the integration half as an un-run gap
# in a census it prints on every run. The old plant would therefore no longer fire, and
# a plant that cannot fire turns this harness into the vacuous green it exists to
# prevent — so the plant moved to the lane's ACTUAL subject. This expectation was
# updated deliberately, not because the lane got weaker at its own job: it still proves
# EXECUTION rather than compilation (a compile-only lane stays green on a failing
# assertion), which is the incident class this lane owns.
#
# The gate names neither the module nor the test, so firing still proves reach beyond
# anything hard-coded — and it additionally exercises check_unittest_targets_ran's
# subject, the `--lib` unittest binary.
plant_flight_tests_unittest() {
  cat > "$TREE/cqlite-flight/src/ah6_planted_flight.rs" <<'EOF'
//! AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[test]
fn ah6_planted_flight_break() {
    assert_eq!(1 + 1, 3, "AH6 planted break for the flight-tests lane");
}
EOF
  cat >> "$TREE/cqlite-flight/src/lib.rs" <<'EOF'

// AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[cfg(test)]
mod ah6_planted_flight;
EOF
}
plant_marker_flight_tests_unittest='ah6_planted_flight_break'
plant_desc_flight_tests_unittest='a NEW cqlite-flight/src/ah6_planted_flight.rs unit-test module wired into src/lib.rs with a failing #[test] — the lane runs --lib --bins after the #3384 narrowing, and the gate names neither the module nor the test, so firing proves EXECUTION (not compilation) of a subject nothing hard-codes'

# ---------------------------------------------------------------------------
# #3725: the THREE additional cases for the WIDENED feature-iso-delta-scan lane.
#
# The lane stopped being compile-only, so the one #1978 coupling plant above can no
# longer stand in for it: that plant fires on a lane that merely COMPILES, and every
# property #3725 added (execution, a derived target set, a non-zero test count per
# target, a strict fixture posture) is invisible to it. One plant per new property.
# ---------------------------------------------------------------------------

# (a) EXECUTION + the DERIVED target set. A NEW crate-level-gated file with an inverted
# assertion: a compile-only lane stays green on it, and a lane with a hard-coded target
# list never names it. The gate names neither the file nor the test.
plant_feature_iso_delta_scan_assertion() {
  cat > "$TREE/cqlite-core/tests/ah6_planted_delta_scan.rs" <<'EOF'
//! AH6 PLANTED BREAK (issue #3725 observation harness) — reverted by the harness.
#![cfg(feature = "delta-scan")]
#[test]
fn ah6_planted_delta_scan_break() {
    assert_eq!(1 + 1, 3, "AH6 planted break for the widened feature-iso-delta-scan lane");
}
EOF
}
plant_marker_feature_iso_delta_scan_assertion='ah6_planted_delta_scan_break'
plant_desc_feature_iso_delta_scan_assertion='a NEW cqlite-core/tests/ah6_planted_delta_scan.rs carrying a crate-level #![cfg(feature = "delta-scan")] and an inverted assertion — fires ONLY if the lane EXECUTES (the pre-#3725 `--lib --no-run` shape stays green) and only if its target set is DERIVED (a hard-coded list never names a 14th file)'

# (b) THE ZERO-TESTS HALF, and the only plant that can reach it. A target that COMPILES,
# RUNS and reports `0 tests`, exiting 0 — so cargo is green and only
# check_no_unexpected_zero_tests can fail the lane. Every failing-assertion plant exits
# non-zero and therefore proves nothing about the non-zero-count guard; this is the
# established idiom from scripts/tests/test_agent_gate_binding_rust_lanes.sh.
#
# The construction: the crate gate is the BARE recognised form (so _ds_inner_cfg_gate
# reports no co-required feature and the target is NOT allowed-zero), while the only test
# inside is gated on `parquet` — which this lane deliberately does not enable. Nothing in
# the gate names this file, so firing also proves the derived set reached it.
plant_feature_iso_delta_scan_zerotests() {
  cat > "$TREE/cqlite-core/tests/ah6_planted_delta_scan_empty.rs" <<'EOF'
//! AH6 PLANTED BREAK (issue #3725 observation harness) — reverted by the harness.
#![cfg(feature = "delta-scan")]
#[cfg(feature = "parquet")]
#[test]
fn ah6_planted_delta_scan_zero_tests() {
    // Never compiled in this lane: parquet is the feature it isolates AGAINST.
}
EOF
}
plant_marker_feature_iso_delta_scan_zerotests='ah6_planted_delta_scan_empty'
plant_desc_feature_iso_delta_scan_zerotests='a NEW crate-level-gated target whose ONLY test is gated on `parquet` — it compiles, runs, reports 0 tests and EXITS 0, so cargo is green and only the non-zero-count guard can red the lane (the one plant a failing assertion cannot substitute for)'

# (c) THE FIXTURE POSTURE, exercised in FULL-GATE MODE — the anti-vacuity half.
#
# MEASURED, and the reason this case exists: with the corpus absent,
# issue_1007_complex_type_parity reports `6 passed` in 0.00s having compared NOTHING (its
# own skip_or_fail prints `[SKIP] …` and returns). So an executor added for #3725's AC2
# satisfies AC3's letter — it prints 6 — while closing the gap VACUOUSLY. Only the FULL
# gate exports CQLITE_REQUIRE_FIXTURES=1, and the mode is derived from the gate's own
# $ONLY/$LITE state on purpose (deriving it from the environment would let the ambient
# shell redefine what the gate certifies), so NO flag or variable can force it from here.
#
# THE INSTRUMENT: narrow the SCRATCH COPY's own COMPONENTS array to this one lane,
# regenerate its committed manifest, and COMMIT both — then run the gate with NO flags.
# Committing is load-bearing, not tidiness: #3544's component-set pre-flight reports a
# fail-closed UNCOMMITTED verdict for a removal present only in the working copy, and the
# non-fatal DECLARED verdict once the removal is committed and origin/main is an ancestor.
# Substituting the artifact in the harness's own scratch copy is the sanctioned idiom
# (CLAUDE.md #3312 job 27: a test-only seam is one more thing a real invoker can set).
#
# TWO CORPORA, because the outer #2078 preflight would otherwise mask the lane:
#   * clean    — the real corpus: the narrowed full gate must PASS. This is the positive
#                control; without it a red below could be the narrowing's own doing.
#   * planted  — a corpus holding ONLY the canonical keyspace (test_basic), so the #2078
#                canonical-corpus preflight is SATISFIED and the run reaches the lane,
#                whose parity fixtures (test_types, …) are then absent. Strict mode must
#                turn those absences into a NAMED failure, never a skip-pass.
probe_feature_iso_delta_scan_corpus_absent() {
  local sfx="$WORK/summary-fx" lgx="$WORK/gate-fx" rc=0 partial
  # A corpus with the canonical keyspace and nothing else. Symlinked, never copied: the
  # real corpus is large, and the property under test is which keyspaces are REACHABLE.
  partial="$WORK/corpus-canonical-only"
  if [ ! -d "$partial" ]; then
    if [ ! -d "$DATASETS/sstables/test_basic" ]; then
      echo "  => HARNESS FAILURE: \$CQLITE_DATASETS_ROOT/sstables/test_basic is absent, so a"
      echo "     canonical-only corpus cannot be built and the #2078 preflight would fail"
      echo "     BEFORE the lane runs — the observation would be about the preflight, not the lane."
      return 1
    fi
    # A FILE-level symlink farm, not one symlink for the keyspace directory. MEASURED:
    # the gate's canonical-corpus probe is
    # `find "$root/sstables/test_basic" -name "*-Data.db"`, and `find` does NOT descend
    # through a symlinked directory unless given -H/-L — so the tidier single symlink made
    # the run die at the #2078 preflight (missing-fixtures: FAIL-CLOSED) BEFORE the lane
    # ever ran, and the observation would have been about the preflight, not the posture.
    # Real directories + symlinked FILES, so every corpus reader sees an ordinary tree.
    mkdir -p "$partial/sstables/test_basic" || return 1
    local rel
    while IFS= read -r rel; do
      [ -n "$rel" ] || continue
      mkdir -p "$partial/sstables/test_basic/$(dirname "$rel")" || return 1
      ln -sf "$DATASETS/sstables/test_basic/$rel" "$partial/sstables/test_basic/$rel" || return 1
    done <<EOF
$( cd "$DATASETS/sstables/test_basic" && find . -type f | sed 's|^\./||' )
EOF
    if [ "$(find "$partial/sstables/test_basic" -name '*-Data.db' | wc -l | tr -d ' ')" -eq 0 ]; then
      echo "  => HARNESS FAILURE: the canonical-only corpus holds no *-Data.db reachable by find,"
      echo "     so the #2078 preflight would fail before the lane runs. Refusing to report an"
      echo "     observation about the preflight as one about the lane."
      return 1
    fi
    # Root-level metadata, if the corpus carries any: absent files are not an error here
    # (the property is keyspace reachability), so this is best-effort BY DESIGN.
    local meta
    for meta in metadata.yml references.yml; do
      [ -e "$DATASETS/$meta" ] && ln -s "$DATASETS/$meta" "$partial/$meta"
    done
  fi

  # Narrow COMPONENTS to this lane, in the SCRATCH COPY only.
  local gate="$TREE/scripts/agent-gate.sh"
  if [ "$(grep -c '^COMPONENTS=(' "$gate")" -ne 1 ]; then
    echo "  => HARNESS FAILURE: expected exactly one top-level COMPONENTS=( line in the scratch"
    echo "     copy's gate; found $(grep -c '^COMPONENTS=(' "$gate"). Refusing to guess."
    return 1
  fi
  # A python3 rewrite, not `sed -i` (BSD/GNU incompatible) and not an in-place shell
  # redirect (which truncates the file before the read).
  python3 - "$gate" <<'PYEOF' || return 1
import sys
p = sys.argv[1]
lines = open(p).read().split('\n')
n = 0
for i, l in enumerate(lines):
    if l.startswith('COMPONENTS=('):
        lines[i] = 'COMPONENTS=(feature-iso-delta-scan)'
        n += 1
assert n == 1, n
open(p, 'w').write('\n'.join(lines))
PYEOF
  # The LOCAL manifest is asserted against the running array on every run
  # (manifest-stale is fail-closed), so it must be regenerated with the documented
  # one-liner — the comment header is preserved and the names come from --list.
  ( cd "$TREE" && { sed -n -e '/^[^#]/q' -e p scripts/agent-gate.components; \
      bash scripts/agent-gate.sh --list; } > "$WORK/components.new" ) || return 1
  [ -s "$WORK/components.new" ] || { echo "  => HARNESS FAILURE: manifest regeneration produced nothing"; return 1; }
  cp "$WORK/components.new" "$TREE/scripts/agent-gate.components" || return 1
  git -C "$TREE" add -A >/dev/null 2>&1 || return 1
  git -C "$TREE" -c user.email=ah6@local -c user.name=ah6 \
    commit -q -m "AH6 #3725: narrow COMPONENTS to feature-iso-delta-scan (throwaway worktree)" \
    >/dev/null 2>&1 || return 1

  local dir tag root exp_rc
  local clean_ok=0 planted_ok=0 attributed=0
  for tag in clean planted; do
    if [ "$tag" = clean ]; then root="$DATASETS"; else root="$partial"; fi
    local t0 t1; t0=$(date +%s)
    ( cd "$TREE" && env \
        AGENT_GATE_SUMMARY_FILE="$sfx-$tag.txt" \
        CARGO_TARGET_DIR="$TARGET" \
        CQLITE_DATASETS_ROOT="$root" \
        bash scripts/agent-gate.sh >"$lgx-$tag.log" 2>&1 )
    rc=$?
    t1=$(date +%s)
    local st; st=$(sed -n "s/^feature-iso-delta-scan:[[:space:]]*\([A-Z][A-Z-]*\).*/\1/p" "$sfx-$tag.txt" 2>/dev/null | tail -1)
    [ -n "$st" ] || st="<no lane line in the summary>"
    printf '  %-7s FULL gate exit=%s  summary says "feature-iso-delta-scan: %s"  corpus=%s  (%ss)\n' \
      "$tag" "$rc" "$st" "$(basename "$root")" "$((t1 - t0))"
    if [ "$tag" = clean ]; then
      [ "$rc" = 0 ] && [ "$st" = PASS ] && clean_ok=1
    else
      [ "$rc" != 0 ] && [ "$st" = FAIL ] && planted_ok=1
      # ATTRIBUTION: the red must name the STRICT-MODE failure, not just any failure. The
      # marker is the string the target itself panics with under CQLITE_REQUIRE_FIXTURES=1.
      dir=$(sed -n 's/^logs: //p' "$sfx-$tag.txt" 2>/dev/null | tail -1)
      if { [ -n "$dir" ] && [ -f "$dir/feature-iso-delta-scan.log" ] \
             && grep -qF 'CQLITE_REQUIRE_FIXTURES=1 but' "$dir/feature-iso-delta-scan.log"; } \
         || grep -qF 'CQLITE_REQUIRE_FIXTURES=1 but' "$lgx-$tag.log"; then
        attributed=1
      fi
    fi
  done

  if [ "$clean_ok" = 1 ] && [ "$planted_ok" = 1 ] && [ "$attributed" = 1 ]; then
    echo "  => FIRED: the FULL gate PASSes on the real corpus and FAILs on a canonical-only one,"
    echo "     and the red NAMES the strict-fixture panic (CQLITE_REQUIRE_FIXTURES=1 but …) — so an"
    echo "     absent parity fixture cannot skip-pass in the gate of record."
    return 0
  fi
  if [ "$clean_ok" != 1 ]; then
    echo "  => HARNESS FAILURE: the narrowed FULL gate did not pass on the REAL corpus, so its red on"
    echo "     the canonical-only corpus is not evidence of anything. Fix the baseline, not the plant."
    tail -25 "$lgx-clean.log" | sed 's/^/       /'
    return 1
  fi
  if [ "$planted_ok" != 1 ]; then
    echo "  => DID NOT FIRE: with the parity fixtures ABSENT the lane did not red — i.e. it SKIP-PASSED,"
    echo "     which is exactly the vacuous close #3725 forbids. A REAL FINDING about the lane."
    tail -25 "$lgx-planted.log" | sed 's/^/       /'
    return 1
  fi
  echo "  => FIRED, BUT UNATTRIBUTED: the lane red'd without naming the strict-fixture panic, so the"
  echo "     red cannot be shown to be the absent corpus's. Not an observation."
  return 1
}
case_kind_feature_iso_delta_scan_corpus_absent=probe
plant_desc_feature_iso_delta_scan_corpus_absent='a FULL-gate run (COMPONENTS narrowed + committed in the scratch copy) against a corpus holding ONLY the canonical keyspace — the lane must FAIL by name under CQLITE_REQUIRE_FIXTURES=1 instead of printing `6 passed` having compared nothing'

# ---------------------------------------------------------------------------
# The CASE LISTS. One case = one observation. DECLARED per lane, and the loop below
# refuses a lane with none — a lane whose case list silently emptied would report
# "all lanes fired" having observed nothing, which is #3544's shape inside this harness.
# ---------------------------------------------------------------------------
CASES_feature_iso_parquet="coupling"
CASES_feature_iso_delta_scan="coupling assertion zerotests corpus_absent"
CASES_legacy_heuristics="gatedtest"
CASES_flight_tests="unittest"

# A CASE FLOOR, in the idiom test_agent_gate_summary.sh uses. A green tally over a
# SHRUNKEN suite is its own defect (#3544): four cases were deleted from that file by a
# span-replacing edit and it reported `failed: 0` at 102 instead of 105 for a whole round.
# The floor is the full-observation count; a SUBSET run is exempt because it deliberately
# observes fewer.
CASE_FLOOR=7

# One uniform revert for every plant: restore tracked files, delete untracked ones.
# `clean -fd` without -x deliberately spares the gitignored build output.
# `reset --hard` to the OBSERVED commit, not `checkout -- .`: the #3725 corpus_absent
# probe COMMITS its narrowed COMPONENTS array in the throwaway worktree (it has to — a
# removal present only in the working copy is a fail-closed UNCOMMITTED verdict from
# #3544's component-set pre-flight), and `checkout -- .` cannot undo a commit. The reset
# targets $LIVE_HEAD_BEFORE, the sha every observation in this run is reported against, so
# a stray commit can never carry into the next case. The worktree has its OWN HEAD, so
# this touches the live checkout not at all — verified independently by
# assert_live_checkout_untouched.
unplant() {
  git -C "$TREE" reset --hard "$LIVE_HEAD_BEFORE" >/dev/null 2>&1
  git -C "$TREE" clean -fdq >/dev/null 2>&1
  local residue head
  residue=$(git -C "$TREE" status --porcelain)
  if [ -n "$residue" ]; then
    echo "  HARNESS ERROR: the throwaway tree did not revert cleanly:" >&2
    printf '%s\n' "$residue" >&2
    return 1
  fi
  # A clean STATUS says nothing about HEAD, and the corpus_absent probe moves it. An
  # observation made against an unnoticed extra commit would be reported against
  # $LIVE_HEAD_BEFORE — a stale certification that reads exactly like a fresh one, the
  # same #2926 hazard this harness's live-checkout assert exists for.
  head=$(git -C "$TREE" rev-parse HEAD 2>/dev/null)
  if [ "$head" != "$LIVE_HEAD_BEFORE" ]; then
    echo "  HARNESS ERROR: the throwaway tree's HEAD is $head, not the observed commit $LIVE_HEAD_BEFORE" >&2
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# One direction of one lane. Returns via RC / STATUS.
#
# `--only` exit codes are load-bearing and NOT the usual 0/1: a PARTIAL run that
# found nothing exits 3 (the gate refuses to let a partial run be scripted into a
# green claim), and a PARTIAL run with a failed component exits 1. So the CLEAN
# expectation is 3, not 0. The exit code alone is not trusted either — the
# component's own SUMMARY line is parsed and both must agree, so a gate that
# mis-reported one of them could not be mistaken for an observation.
# ---------------------------------------------------------------------------
RC=0
STATUS=""
run_direction() { # <lane> <tag>
  local lane=$1 tag=$2
  local sf="$WORK/summary-$lane-$tag.txt" lg="$WORK/gate-$lane-$tag.log"
  local t0 t1
  t0=$(date +%s)
  ( cd "$TREE" && env \
      AGENT_GATE_SUMMARY_FILE="$sf" \
      CARGO_TARGET_DIR="$TARGET" \
      CQLITE_DATASETS_ROOT="$DATASETS" \
      bash scripts/agent-gate.sh --only "$lane" >"$lg" 2>&1 )
  RC=$?
  t1=$(date +%s)
  STATUS=$(sed -n "s/^${lane}:[[:space:]]*\([A-Z][A-Z-]*\).*/\1/p" "$sf" 2>/dev/null | tail -1)
  # The gate's per-component log, named by the SUMMARY's own `logs:` line. Used for
  # ATTRIBUTION (below); the harness's captured stdout is the fallback, since the FAIL
  # branch tails only 40 lines of the component log into it.
  COMPONENT_LOG=""
  local logdir
  logdir=$(sed -n 's/^logs: //p' "$sf" 2>/dev/null | tail -1)
  [ -n "$logdir" ] && [ -f "$logdir/$lane.log" ] && COMPONENT_LOG="$logdir/$lane.log"
  [ -n "$STATUS" ] || STATUS="<no ${lane} line in the summary>"
  printf '  %-7s exit=%s  summary says "%s: %s"  (%ss)\n' "$tag" "$RC" "$lane" "$STATUS" "$((t1 - t0))"
  LAST_LOG="$lg"
}

# ---------------------------------------------------------------------------
RESULTS=()
FAILED=0
START=$(date +%s)

CASES_OBSERVED=0

# ONE case of ONE lane. Split out of the loop so the `probe` kind (a case that manages its
# own runs, e.g. #3725's FULL-gate corpus_absent observation) and the `plant` kind
# (clean-PASS then planted-FAIL, the original shape) can be dispatched without either
# becoming a special case inside the other.
observe_case() { # <lane> <case>
  local lane=$1 case=$2
  local fn="${lane//-/_}" cfn kind desc_var marker_var marker
  cfn="${fn}_${case}"
  kind_var="case_kind_$cfn"; kind="${!kind_var:-plant}"
  desc_var="plant_desc_$cfn"
  if [ -z "${!desc_var:-}" ]; then
    RESULTS+=("$lane [$case]|HARNESS-ERROR|no plant_desc_$cfn declared — an undescribed case is an unreviewable observation")
    return 1
  fi
  echo "---- $lane [$case] ($kind) ----"
  echo "  plant: ${!desc_var}"

  unplant || { RESULTS+=("$lane [$case]|HARNESS-ERROR|tree would not revert before the case"); return 1; }

  if [ "$kind" = probe ]; then
    # A probe OWNS both directions and its own attribution; it returns 0 only when it has
    # made a complete observation. Its tree is reverted after it either way.
    if "probe_$cfn"; then
      RESULTS+=("$lane [$case]|FIRED|probe observed both directions and attributed the red")
      unplant || { RESULTS+=("$lane [$case]|HARNESS-ERROR|tree would not revert after the probe"); return 1; }
      return 0
    fi
    unplant || RESULTS+=("$lane [$case]|HARNESS-ERROR|tree would not revert after the probe")
    RESULTS+=("$lane [$case]|DID-NOT-FIRE|the probe did not make a complete, attributed observation")
    return 1
  fi

  local clean_rc clean_status planted_rc planted_status planted_log planted_component_log
  run_direction "$lane" "clean-$case"
  clean_rc=$RC clean_status=$STATUS

  "plant_$cfn"
  run_direction "$lane" "planted-$case"
  planted_rc=$RC planted_status=$STATUS
  planted_log=$LAST_LOG
  planted_component_log=$COMPONENT_LOG

  unplant || { RESULTS+=("$lane [$case]|HARNESS-ERROR|tree would not revert after the planted run"); return 1; }

  local clean_ok=0 planted_ok=0 attributed=0
  [ "$clean_rc" = 3 ]   && [ "$clean_status" = PASS ] && clean_ok=1
  [ "$planted_rc" = 1 ] && [ "$planted_status" = FAIL ] && planted_ok=1

  # ATTRIBUTION. A red is only evidence if it is THIS plant's red: a lane that happened
  # to break for an unrelated reason would produce an identical exit code and an
  # identical SUMMARY line. So the planted run's output must NAME the planted symbol.
  # Unattributed is treated as a failure of the observation, never as a fire.
  marker_var="plant_marker_$cfn"
  marker="${!marker_var:-}"
  if [ -z "$marker" ]; then
    RESULTS+=("$lane [$case]|HARNESS-ERROR|no plant_marker_$cfn declared — a red could not be attributed")
    return 1
  fi
  if [ "$planted_ok" = 1 ]; then
    if { [ -n "$planted_component_log" ] && grep -qF "$marker" "$planted_component_log"; } \
       || grep -qF "$marker" "$planted_log"; then
      attributed=1
    fi
  fi

  if [ "$clean_ok" = 1 ] && [ "$planted_ok" = 1 ] && [ "$attributed" = 1 ]; then
    RESULTS+=("$lane [$case]|FIRED|clean=PASS(exit 3), planted=FAIL(exit 1) naming $marker")
    echo "  => FIRED: silent on the clean tree, red on the planted break, and the red NAMES"
    echo "     the planted symbol ($marker) — so the red is this plant's, not an unrelated one."
    return 0
  elif [ "$clean_ok" = 1 ] && [ "$planted_ok" = 1 ]; then
    RESULTS+=("$lane [$case]|FIRED-UNATTRIBUTED|the lane red'd but its output never names $marker")
    echo "  => FIRED, BUT UNATTRIBUTED: the lane went red and never named the planted symbol"
    echo "     ($marker), so the red cannot be shown to be this plant's. Not an observation."
    return 1
  elif [ "$clean_ok" = 0 ] && [ "$planted_ok" = 1 ]; then
    # Red in BOTH directions is NOT an observation: a lane that fails unconditionally
    # would look identical. Report it as the harness's own failure (#3229).
    RESULTS+=("$lane [$case]|HARNESS-FAILURE|clean direction did not pass (exit $clean_rc, status $clean_status) — red in both directions proves nothing")
    echo "  => HARNESS FAILURE: the lane was already red on the CLEAN tree, so its red on the"
    echo "     planted break is not evidence of anything. Fix the baseline, do not adjust the plant."
    return 1
  elif [ "$clean_ok" = 1 ]; then
    RESULTS+=("$lane [$case]|DID-NOT-FIRE|clean=PASS but the planted break produced exit $planted_rc, status $planted_status")
    echo "  => DID NOT FIRE: the planted break did not red the lane. This is a REAL FINDING about"
    echo "     the lane, not a harness knob — do not adjust the plant until it fires."
    echo "     planted-run log: $planted_log (last 30 lines)"
    tail -30 "$planted_log" | sed 's/^/       /'
    return 1
  fi
  RESULTS+=("$lane [$case]|HARNESS-FAILURE|clean exit $clean_rc/$clean_status, planted exit $planted_rc/$planted_status — neither direction behaved as specified")
  echo "  => HARNESS FAILURE: neither direction behaved as specified."
  return 1
}

for lane in "${LANES[@]}"; do
  fn="${lane//-/_}"
  cases_var="CASES_$fn"
  cases="${!cases_var:-}"
  # A lane with NO declared case would sail through the loop and be reported nowhere, so
  # "every selected lane fired" would be true of a lane nobody observed — #3544's
  # shrunken-subject-set shape inside this harness. Refuse instead.
  if [ -z "$cases" ]; then
    RESULTS+=("$lane||HARNESS-ERROR|no CASES_$fn declared — the lane would be silently unobserved")
    FAILED=1
    echo "---- $lane ----"
    echo "  => HARNESS ERROR: no CASES_$fn declared"
    echo
    continue
  fi
  for case in $cases; do
    observe_case "$lane" "$case" || FAILED=1
    CASES_OBSERVED=$((CASES_OBSERVED + 1))
    echo
  done
done

# The harness's own invariant, MEASURED before any verdict is printed: a success that
# was accompanied by a mutated live checkout is not a success (roborev round-2 finding
# 1). Checked here rather than only in cleanup so the failure is attributable to the
# run and appears above the summary.
assert_live_checkout_untouched "pre-summary" || FAILED=1

END=$(date +%s)
echo "==== AH6 OBSERVATION SUMMARY ===="
# The CAPTURED commit, never a fresh `git rev-parse HEAD` at emit time: the observation
# was made against the tree copied from $LIVE_HEAD_BEFORE, so re-reading HEAD here would
# report whatever the checkout happens to be NOW and attribute this run's evidence to a
# commit it never examined (roborev round-6 finding; the same rule as the gate's #2926
# block, whose commit: line is derived from its verified capture).
echo "observed-commit: $LIVE_HEAD_BEFORE (captured at start; the throwaway copy was made from THIS sha)"
echo "live-tree: $([ "$LIVE_TREE_VIOLATED" -eq 0 ] && echo "UNCHANGED (verified: git status --porcelain AND HEAD identical to start)" || echo "MUTATED — HARNESS FAILURE")"
echo "elapsed:  $((END - START))s"
[ "$SUBSET" -eq 1 ] && echo "mode:     SUBSET (${LANES[*]}) — a partial observation, NOT the full AC2 evidence"
echo "cases:    $CASES_OBSERVED observed (floor $CASE_FLOOR on a full run)"
for r in "${RESULTS[@]}"; do
  printf '%-40s %-18s %s\n' "${r%%|*}" "$(echo "$r" | cut -d'|' -f2)" "$(echo "$r" | cut -d'|' -f3-)"
done
# THE CASE FLOOR (#3544's lesson, in test_agent_gate_summary.sh's idiom). A span-replacing
# edit that deletes cases leaves the tally green over a SHRUNKEN subject set, and
# "everything fired" is then true of an observation nobody made. Exempt on a SUBSET run,
# which deliberately observes fewer.
if [ "$SUBSET" -eq 0 ] && [ "$CASES_OBSERVED" -lt "$CASE_FLOOR" ]; then
  echo "RESULT: FAIL (only $CASES_OBSERVED cases were observed, floor $CASE_FLOOR — cases are being"
  echo "        skipped or silently dropped, and 'every lane fired' over a shrunken set certifies nothing)"
  exit 1
fi
if [ "$FAILED" -ne 0 ]; then
  echo "RESULT: FAIL (a case did not fire, its clean baseline was not established, or the live checkout was mutated)"
  exit 1
fi
if [ "$SUBSET" -eq 1 ]; then
  echo "RESULT: PARTIAL (every SELECTED case fired; run with no arguments for the full observation)"
  exit 3
fi
echo "RESULT: PASS ($CASES_OBSERVED cases across ${#LANES[@]} lanes observed to fire on a planted break and stay silent on a clean tree)"
exit 0
