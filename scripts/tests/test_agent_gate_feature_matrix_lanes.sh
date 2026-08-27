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
# The copy gets its own CARGO_TARGET_DIR so the clean and planted runs of a lane
# share compilation.
#
# OPT-IN, NOT A GATE COMPONENT (D5). It performs real compiles; taxing every full
# gate to re-prove a static property is disproportionate. It is deliberately absent
# from COMPONENTS / LITE_COMPONENTS / DELTA_COMPONENTS. Nightly `gate.yml`
# enrollment is out of scope (a workflow change needs #2910 registry enrollment).
#
# Usage:
#   bash scripts/tests/test_agent_gate_feature_matrix_lanes.sh              # all four
#   bash scripts/tests/test_agent_gate_feature_matrix_lanes.sh <lane> ...   # subset
#
# Exit: 0 = every selected lane observed to fire AND to stay clean; 1 = a lane did not
# fire, or the harness could not establish a clean baseline; 3 = a SUBSET run in which
# everything selected fired (a partial observation, never the full AC2 evidence).

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

# The corpus root. flight-tests and legacy-heuristics are DATASET_COMPONENTS, and the
# throwaway worktree has none of the gitignored Data.db binaries, so the root must be
# an absolute path to a real corpus — inherited if the caller already exported one.
DATASETS="${CQLITE_DATASETS_ROOT:-}"
if [ -z "$DATASETS" ]; then
  echo "CQLITE_DATASETS_ROOT is not set; export the absolute root printed by" >&2
  echo "  bash test-data/scripts/fetch-datasets.sh" >&2
  exit 2
fi
case "$DATASETS" in
  /*) ;;
  *) echo "CQLITE_DATASETS_ROOT must be ABSOLUTE (got: $DATASETS)" >&2; exit 2 ;;
esac

WORK=$(mktemp -d "${TMPDIR:-/tmp}/ah6-1699-XXXXXX") || exit 1
TREE="$WORK/tree"
# Stable, OUTSIDE the throwaway tree: the clean and planted runs of a lane must share
# compilation (otherwise the planted run pays a full cold build), and a target dir
# inside the copy would be swept by the revert.
TARGET="${TMPDIR:-/tmp}/ah6-1699-target"

cleanup() {
  local rc=$?
  if [ -d "$TREE" ]; then
    git -C "$REPO_ROOT" worktree remove --force "$TREE" >/dev/null 2>&1 \
      || rm -rf "$TREE"
  fi
  git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1
  rm -rf "$WORK"
  exit "$rc"
}
trap cleanup EXIT INT TERM

echo "==== AH6 FEATURE-MATRIX LANE OBSERVATION (issue #1699, design D5) ===="
echo "repo:      $REPO_ROOT"
echo "head:      $(git rev-parse HEAD)"
echo "lanes:     ${LANES[*]}"
[ "$SUBSET" -eq 1 ] && echo "mode:      SUBSET (partial observation — not the full AC2 evidence)"
echo "worktree:  $TREE (throwaway; the live checkout is never mutated)"
echo "target:    $TARGET"
echo "datasets:  $DATASETS"
echo

git worktree add --detach "$TREE" HEAD >/dev/null 2>&1 || {
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
plant_feature_iso_parquet() {
  cat >> "$TREE/cqlite-core/src/lib.rs" <<'EOF'

// AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[cfg(feature = "parquet")]
pub fn ah6_planted_parquet_probe() -> bool {
    crate::ah6_planted_delta_scan_marker()
}
#[cfg(feature = "delta-scan")]
pub fn ah6_planted_delta_scan_marker() -> bool {
    true
}
EOF
}
plant_marker_feature_iso_parquet='ah6_planted_delta_scan_marker'
plant_desc_feature_iso_parquet='a #[cfg(feature = "parquet")] fn in cqlite-core/src/lib.rs calling a #[cfg(feature = "delta-scan")] fn (compiles with both features on; unresolved with parquet alone) — #1978 class'

plant_feature_iso_delta_scan() {
  cat >> "$TREE/cqlite-core/src/lib.rs" <<'EOF'

// AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[cfg(feature = "delta-scan")]
pub fn ah6_planted_delta_scan_probe() -> bool {
    crate::ah6_planted_parquet_marker()
}
#[cfg(feature = "parquet")]
pub fn ah6_planted_parquet_marker() -> bool {
    true
}
EOF
}
plant_marker_feature_iso_delta_scan='ah6_planted_parquet_marker'
plant_desc_feature_iso_delta_scan='the mirror: a #[cfg(feature = "delta-scan")] fn in cqlite-core/src/lib.rs calling a #[cfg(feature = "parquet")] fn'

# A NEW file, deliberately. It does double duty: it proves the lane EXECUTES rather
# than merely compiles (a compile-only lane stays green on a failing assertion), and
# it proves the lane's DERIVED target set picks up a sixth gated file with no gate
# edit — a hard-coded list would ignore it and the lane would stay green.
plant_legacy_heuristics() {
  cat > "$TREE/cqlite-core/tests/ah6_planted_legacy.rs" <<'EOF'
//! AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[cfg(feature = "legacy-heuristics")]
#[test]
fn ah6_planted_legacy_heuristics_break() {
    assert_eq!(1 + 1, 3, "AH6 planted break for the legacy-heuristics lane");
}
EOF
}
plant_marker_legacy_heuristics='ah6_planted_legacy_heuristics_break'
plant_desc_legacy_heuristics='a NEW cqlite-core/tests/ah6_planted_legacy.rs carrying a #[cfg(feature = "legacy-heuristics")] #[test] with an inverted assertion (also proves the DERIVED target set picks up a sixth gated file with no gate edit)'

# A NEW cqlite-flight integration target, named by nothing in the gate — so a green
# here would mean the lane reaches past the three targets already covered
# (query_semantics_flight_parity, issue_3095_flight_static_columns, memory-budget's
# dhat target).
plant_flight_tests() {
  cat > "$TREE/cqlite-flight/tests/ah6_planted_flight.rs" <<'EOF'
//! AH6 PLANTED BREAK (issue #1699 observation harness) — reverted by the harness.
#[test]
fn ah6_planted_flight_break() {
    assert_eq!(1 + 1, 3, "AH6 planted break for the flight-tests lane");
}
EOF
}
plant_marker_flight_tests='ah6_planted_flight_break'
plant_desc_flight_tests='a NEW cqlite-flight/tests/ah6_planted_flight.rs with a failing #[test] — a target the gate names nowhere, so firing proves reach beyond query_semantics_flight_parity / issue_3095_flight_static_columns / the dhat target'

# One uniform revert for every plant: restore tracked files, delete untracked ones.
# `clean -fd` without -x deliberately spares the gitignored build output.
unplant() {
  git -C "$TREE" checkout -- . >/dev/null 2>&1
  git -C "$TREE" clean -fdq >/dev/null 2>&1
  local residue
  residue=$(git -C "$TREE" status --porcelain)
  if [ -n "$residue" ]; then
    echo "  HARNESS ERROR: the throwaway tree did not revert cleanly:" >&2
    printf '%s\n' "$residue" >&2
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

for lane in "${LANES[@]}"; do
  fn="${lane//-/_}"
  desc_var="plant_desc_${fn}"
  echo "---- $lane ----"
  echo "  plant: ${!desc_var}"

  unplant || { RESULTS+=("$lane|HARNESS-ERROR|tree would not revert before the clean run"); FAILED=1; continue; }

  run_direction "$lane" clean
  clean_rc=$RC clean_status=$STATUS

  "plant_${fn}"
  run_direction "$lane" planted
  planted_rc=$RC planted_status=$STATUS
  planted_log=$LAST_LOG
  planted_component_log=$COMPONENT_LOG

  unplant || { RESULTS+=("$lane|HARNESS-ERROR|tree would not revert after the planted run"); FAILED=1; continue; }

  clean_ok=0;   [ "$clean_rc" = 3 ]   && [ "$clean_status" = PASS ] && clean_ok=1
  planted_ok=0; [ "$planted_rc" = 1 ] && [ "$planted_status" = FAIL ] && planted_ok=1

  # ATTRIBUTION. A red is only evidence if it is THIS plant's red: a lane that happened
  # to break for an unrelated reason would produce an identical exit code and an
  # identical SUMMARY line. So the planted run's output must NAME the planted symbol.
  # Unattributed is treated as a failure of the observation, never as a fire.
  marker_var="plant_marker_${fn}"
  marker="${!marker_var}"
  attributed=0
  if [ "$planted_ok" = 1 ]; then
    if { [ -n "$planted_component_log" ] && grep -qF "$marker" "$planted_component_log"; } \
       || grep -qF "$marker" "$planted_log"; then
      attributed=1
    fi
  fi

  if [ "$clean_ok" = 1 ] && [ "$planted_ok" = 1 ] && [ "$attributed" = 1 ]; then
    RESULTS+=("$lane|FIRED|clean=PASS(exit 3), planted=FAIL(exit 1) naming $marker")
    echo "  => FIRED: silent on the clean tree, red on the planted break, and the red NAMES"
    echo "     the planted symbol ($marker) — so the red is this plant's, not an unrelated one."
  elif [ "$clean_ok" = 1 ] && [ "$planted_ok" = 1 ]; then
    RESULTS+=("$lane|FIRED-UNATTRIBUTED|the lane red'd but its output never names $marker")
    echo "  => FIRED, BUT UNATTRIBUTED: the lane went red and never named the planted symbol"
    echo "     ($marker), so the red cannot be shown to be this plant's. Not an observation."
    FAILED=1
  elif [ "$clean_ok" = 0 ] && [ "$planted_ok" = 1 ]; then
    # Red in BOTH directions is NOT an observation: a lane that fails unconditionally
    # would look identical. Report it as the harness's own failure (#3229).
    RESULTS+=("$lane|HARNESS-FAILURE|clean direction did not pass (exit $clean_rc, status $clean_status) — red in both directions proves nothing")
    echo "  => HARNESS FAILURE: the lane was already red on the CLEAN tree, so its red on the"
    echo "     planted break is not evidence of anything. Fix the baseline, do not adjust the plant."
    FAILED=1
  elif [ "$clean_ok" = 1 ]; then
    RESULTS+=("$lane|DID-NOT-FIRE|clean=PASS but the planted break produced exit $planted_rc, status $planted_status")
    echo "  => DID NOT FIRE: the planted break did not red the lane. This is a REAL FINDING about"
    echo "     the lane, not a harness knob — do not adjust the plant until it fires."
    echo "     planted-run log: $planted_log (last 30 lines)"
    tail -30 "$planted_log" | sed 's/^/       /'
    FAILED=1
  else
    RESULTS+=("$lane|HARNESS-FAILURE|clean exit $clean_rc/$clean_status, planted exit $planted_rc/$planted_status — neither direction behaved as specified")
    echo "  => HARNESS FAILURE: neither direction behaved as specified."
    FAILED=1
  fi
  echo
done

END=$(date +%s)
echo "==== AH6 OBSERVATION SUMMARY ===="
echo "head:     $(git rev-parse HEAD)"
echo "elapsed:  $((END - START))s"
[ "$SUBSET" -eq 1 ] && echo "mode:     SUBSET (${LANES[*]}) — a partial observation, NOT the full AC2 evidence"
for r in "${RESULTS[@]}"; do
  printf '%-24s %-16s %s\n' "${r%%|*}" "$(echo "$r" | cut -d'|' -f2)" "$(echo "$r" | cut -d'|' -f3-)"
done
if [ "$FAILED" -ne 0 ]; then
  echo "RESULT: FAIL (a lane did not fire, or its clean baseline was not established)"
  exit 1
fi
if [ "$SUBSET" -eq 1 ]; then
  echo "RESULT: PARTIAL (every SELECTED lane fired; run with no arguments for the full observation)"
  exit 3
fi
echo "RESULT: PASS (all four lanes observed to fire on a planted break and stay silent on a clean tree)"
exit 0
