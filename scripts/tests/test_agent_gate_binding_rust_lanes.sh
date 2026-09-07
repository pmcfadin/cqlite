#!/usr/bin/env bash
#
# test_agent_gate_binding_rust_lanes.sh — planted-break observation harness for the
# binding-side gate lanes of issue #3522: the NEW `binding-rust-tests` component and
# the WIDENED `node-bindings` component.
#
# WHAT THIS ANSWERS, and why nothing else does.
# `--list` proves a lane is REGISTERED. A green SUMMARY line proves a lane RAN and
# found nothing. Neither proves the lane CAN FAIL — and #3522 exists precisely because
# two crates were compiled by every gate run while executing nothing, a state that is
# indistinguishable from coverage unless you make the lane red on demand. This harness
# is the affirmative evidence that each lane fires on the incident class it was built
# for (issue #3522 AC3).
#
# BOTH DIRECTIONS, ALWAYS. Per case it runs the real component on a CLEAN tree
# (expecting the lane's PASS) and then on a tree carrying ONE planted break (expecting
# the lane's FAIL). A harness that only ever plants breaks passes just as happily
# against a lane that fails unconditionally, so a lane that reds in BOTH directions is
# reported as a HARNESS FAILURE, never as a successful observation.
#
# ATTRIBUTED, NOT MERELY RED. A lane that broke for an unrelated reason produces the
# same exit code and the same SUMMARY line as one that detected the plant, so a bare
# red is not evidence either. Each planted run's output must NAME the planted symbol.
#
# THE ZERO-TEST CASE IS THE ONE THE OTHERS CANNOT COVER. Three of the plants are
# failing assertions; those prove the lane EXECUTES rather than merely compiles. The
# fourth (`zero-tests`) cfg's a unit suite OUT, so it compiles clean, runs zero tests
# and exits 0 — the vacuous green this whole issue is about. Only that case exercises
# check_unittest_targets_ran's non-zero-count half, and a plain failing assertion would
# pass over it silently.
#
# THE REAL COMPONENT, NEVER A RETYPED COMMAND. Each direction runs
# `bash scripts/agent-gate.sh --only <lane>`. Retyping the lane's cargo invocation here
# would prove that a cargo command works; it would prove nothing about the gate
# component, which is the subject. `--only` is a PARTIAL run (it can never be a verdict
# — it exits 3 on success for exactly that reason) and is deliberately lenient about
# fixtures, which is what makes it usable as a probe.
#
# ISOLATION. Every mutation happens in a throwaway `git worktree add --detach` copy,
# never the live checkout. #2926 makes a mid-run tree mutation a gate FAIL, so a harness
# that edited the tree its own gate was running in would be the very defect it exists to
# catch. That isolation is VERIFIED, not asserted: the live checkout's status AND HEAD
# are captured at start and re-compared before the summary and again in cleanup. It also
# REFUSES to run from a dirty checkout — the worktree is created from committed HEAD, so
# uncommitted changes would be silently excluded and a PASS would describe code other
# than the code in front of you.
#
# OPT-IN, NOT A GATE COMPONENT. It performs real compiles (and, for the node case, a
# release-unwind napi build plus `npm ci`); taxing every full gate to re-prove a static
# property is disproportionate. It is deliberately absent from COMPONENTS /
# LITE_COMPONENTS / DELTA_COMPONENTS.
#
# Usage:
#   bash scripts/tests/test_agent_gate_binding_rust_lanes.sh              # all cases
#   bash scripts/tests/test_agent_gate_binding_rust_lanes.sh <case> ...   # subset
#
# Cases: ffi-integration ffi-error-contract node-rust-unit zero-tests node-jest
#
# Exit: 0 = every selected case observed to fire AND to stay clean; 1 = a case did not
# fire, a clean baseline could not be established, or the live checkout was mutated;
# 2 = usage / precondition refusal (unknown case, non-absolute CQLITE_DATASETS_ROOT,
# DIRTY live checkout); 3 = a SUBSET run in which everything selected fired (a partial
# observation, never the full AC3 evidence).

set -uo pipefail

# #3637: the gate now REMOVES its per-run log dir on a terminal PASS and on ANY
# verdict when it is nested (which it is here — this script runs inside the gate's
# tooling-tests component, so the child gates below inherit AGENT_GATE_PARENT_RUN_ID).
# These cases READ `<logdir>/<component>.log` AFTER the child exits, so they opt out.
export AGENT_GATE_KEEP_LOGS=1

REPO_ROOT=$(git rev-parse --show-toplevel) || { echo "not a git checkout" >&2; exit 1; }
cd "$REPO_ROOT" || exit 1

ALL_CASES=(ffi-integration ffi-error-contract node-rust-unit zero-tests node-jest)

SUBSET=0
if [ "$#" -gt 0 ]; then
  SUBSET=1
  CASES=("$@")
  for c in "${CASES[@]}"; do
    case " ${ALL_CASES[*]} " in
      *" $c "*) ;;
      *) echo "unknown case: $c (known: ${ALL_CASES[*]})" >&2; exit 2 ;;
    esac
  done
else
  CASES=("${ALL_CASES[@]}")
fi

# The corpus root. binding-rust-tests needs NONE (neither crate's sources reference
# CQLITE_DATASETS_ROOT — that is why it is not in DATASET_COMPONENTS), but node-bindings
# IS a DATASET_COMPONENT now, and although `--only` runs it leniently its corpus-reading
# suites would then skip and the observation would be over less code than the full gate
# runs. So the node case requires a real corpus; the Rust cases do not.
DATASETS="${CQLITE_DATASETS_ROOT:-}"
_needs_datasets=0
for _c in ${CASES[@]+"${CASES[@]}"}; do
  case "$_c" in node-jest) _needs_datasets=1 ;; esac
done
if [ "$_needs_datasets" -eq 1 ] && [ -z "$DATASETS" ]; then
  echo "CQLITE_DATASETS_ROOT is not set, and the selected cases include node-jest," >&2
  echo "whose suite reads the corpus. Export the absolute root printed by:" >&2
  echo "  bash test-data/scripts/fetch-datasets.sh" >&2
  echo "Or select only the Rust cases, which need no corpus." >&2
  exit 2
fi
# A root is VALIDATED whenever one is present, even by a run that does not read it: an
# exported relative root is a fail-closed error everywhere else in this repo (#3148) and
# must not become acceptable just because this run ignores it.
case "${DATASETS:-/}" in
  /*) ;;
  *) echo "CQLITE_DATASETS_ROOT must be ABSOLUTE (got: $DATASETS)" >&2; exit 2 ;;
esac

# THE SUBJECT MUST BE THE CODE IN FRONT OF YOU. The throwaway worktree is created from
# committed HEAD, so any UNCOMMITTED change is silently EXCLUDED from every run — the
# harness would then report a successful observation of code that is not the code being
# reviewed. Refuse rather than mislead; the remedy is a commit, not a flag. The COMMIT is
# captured alongside the status because `git status --porcelain` alone cannot see a HEAD
# move: a clean commit, checkout, reset or branch switch during this multi-minute run
# leaves the status IDENTICAL while the code under observation changes.
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

LIVE_TREE_VIOLATED=0
assert_live_checkout_untouched() {
  local phase="$1" now now_head
  now=$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null) || {
    echo "HARNESS FAILURE ($phase): could not re-read the live checkout's git status," >&2
    echo "  so the never-mutate-the-live-checkout invariant is UNVERIFIABLE here." >&2
    LIVE_TREE_VIOLATED=1; return 1; }
  now_head=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null) || {
    echo "HARNESS FAILURE ($phase): could not re-read the live checkout's HEAD," >&2
    echo "  so the invariant is UNVERIFIABLE here (unreadable is never treated as unchanged)." >&2
    LIVE_TREE_VIOLATED=1; return 1; }
  if [ "$now_head" != "$LIVE_HEAD_BEFORE" ]; then
    {
      echo "HARNESS FAILURE ($phase): the live checkout's HEAD MOVED during the run"
      echo "  ($LIVE_HEAD_BEFORE -> $now_head). The observation above was made against the"
      echo "  OLD commit, so reporting it as this HEAD's would be a STALE certification"
      echo "  indistinguishable from a fresh one."
    } >&2
    LIVE_TREE_VIOLATED=1; return 1
  fi
  if [ "$now" != "$LIVE_STATUS_BEFORE" ]; then
    {
      echo "HARNESS FAILURE ($phase): the LIVE CHECKOUT changed during the run."
      echo "  Every plant must land in the throwaway worktree; a mutated live tree means"
      echo "  one escaped isolation (and #2926 makes a mid-run tree mutation a gate FAIL)."
      printf '%s\n' "$now" | sed 's/^/    /'
    } >&2
    LIVE_TREE_VIOLATED=1; return 1
  fi
  return 0
}

WORK=$(mktemp -d "${TMPDIR:-/tmp}/brl-3522-XXXXXX") || exit 1
TREE="$WORK/tree"
# PRIVATE (under $WORK, itself an mktemp -d), never a predictable shared path: two
# harnesses at once would corrupt each other's incremental state, and on a multi-user
# host a pre-created fixed path would let another user supply the compiled artifacts
# this harness then EXECUTES. Still SHARED between the clean and planted runs of one
# invocation, which is what keeps the observation affordable.
TARGET="$WORK/target"

cleanup() {
  local rc=$?
  # A SIGNAL FORCES A NON-ZERO VERDICT. `cleanup` inherits `$?`, and on a signal
  # delivered BETWEEN commands `$?` is ZERO — so an INTERRUPTED observation would exit
  # successfully, which is this issue's own defect class inside its own harness.
  local sig="${1:-}"
  case "$sig" in
    INT)  rc=130 ;;
    TERM) rc=143 ;;
  esac
  [ -n "$sig" ] && echo "FATAL: observation ABORTED by SIG$sig — this run is NOT evidence (rc=$rc)" >&2
  assert_live_checkout_untouched "cleanup" || rc=1
  if [ -d "$TREE" ]; then
    git -C "$REPO_ROOT" worktree remove --force "$TREE" >/dev/null 2>&1 || rm -rf "$TREE"
  fi
  git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1
  rm -rf "$WORK"
  exit "$rc"
}
trap 'cleanup' EXIT

# CONTAIN the child gates' per-run LOG_DIRs (#3637). This harness sets
# AGENT_GATE_KEEP_LOGS=1 because its cases READ `<logdir>/<component>.log` after the
# child exits — a necessary opt-out, but an opt-out that, left pointing at the shared
# /tmp, leaks one directory per child gate into exactly the population this issue
# exists to drain. Redirecting TMPDIR under $WORK (which `cleanup` removes on EXIT,
# INT and TERM alike) makes the opt-out unable to leak, instead of trading one
# correctness property for another. The same idiom as
# scripts/tests/test_agent_gate_file_size_log.sh.
GATE_TMPDIR="$WORK/tmp"
mkdir -p "$GATE_TMPDIR" || exit 1
export TMPDIR="$GATE_TMPDIR"
trap 'cleanup INT' INT
trap 'cleanup TERM' TERM

echo "==== #3522 BINDING-LANE OBSERVATION ===="
echo "repo:      $REPO_ROOT"
echo "head:      $LIVE_HEAD_BEFORE"
echo "cases:     ${CASES[*]}"
[ "$SUBSET" -eq 1 ] && echo "mode:      SUBSET (partial observation — not the full AC3 evidence)"
echo "worktree:  $TREE (throwaway; the live checkout is never mutated)"
echo "target:    $TARGET"
echo "datasets:  ${DATASETS:-<unset; no dataset-consuming case selected>}"
echo

# `$LIVE_HEAD_BEFORE`, not `HEAD`: the copy must be the exact commit whose observation
# this run reports. `HEAD` is re-resolved at this moment and would silently follow a
# concurrent commit.
git worktree add --detach "$TREE" "$LIVE_HEAD_BEFORE" >/dev/null 2>&1 || {
  echo "FATAL: could not create the throwaway worktree" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Plants. Each is a lane's INCIDENT CLASS, not a syntax error: a syntax error fires any
# lane that compiles anything and so distinguishes nothing.
# ---------------------------------------------------------------------------

# The lane each case observes.
lane_ffi_integration=binding-rust-tests
lane_ffi_error_contract=binding-rust-tests
lane_node_rust_unit=binding-rust-tests
lane_zero_tests=binding-rust-tests
lane_node_jest=node-bindings

# (a) An inverted assertion in cqlite-ffi-common's dependency_boundary integration
# target — the specific target #3522's AC names, because it is the one that measures the
# binding crates' dependency closure and it had never executed anywhere.
plant_ffi_integration() {
  cat >> "$TREE/cqlite-ffi-common/tests/dependency_boundary.rs" <<'EOF'

// #3522 PLANTED BREAK (binding-lane observation harness) — reverted by the harness.
#[test]
fn brl_3522_planted_dependency_boundary_break() {
    assert_eq!(1 + 1, 3, "#3522 planted break in the dependency_boundary target");
}
EOF
}
plant_marker_ffi_integration='brl_3522_planted_dependency_boundary_break'
plant_desc_ffi_integration='a failing #[test] appended to cqlite-ffi-common/tests/dependency_boundary.rs — the integration target #3522 AC names; firing proves the lane EXECUTES it (before #3522 nothing did, anywhere)'

# (b) The same for the OTHER integration target, so the observation covers the derived
# target SET rather than one member of it.
plant_ffi_error_contract() {
  cat >> "$TREE/cqlite-ffi-common/tests/error_contract_table.rs" <<'EOF'

// #3522 PLANTED BREAK (binding-lane observation harness) — reverted by the harness.
#[test]
fn brl_3522_planted_error_contract_break() {
    assert_eq!(1 + 1, 3, "#3522 planted break in the error_contract_table target");
}
EOF
}
plant_marker_ffi_error_contract='brl_3522_planted_error_contract_break'
plant_desc_ffi_error_contract='a failing #[test] appended to cqlite-ffi-common/tests/error_contract_table.rs — the second derived integration target, so the observation covers the SET and not one member of it'

# (c) A failing unit test in cqlite-node's Rust half. The gate names neither the module
# nor the test, so firing proves reach beyond anything hard-coded — and it lands in the
# `--lib` binary of a `crate-type = ["cdylib"]` package, which is the linkability
# question this component's design turned on.
plant_node_rust_unit() {
  cat >> "$TREE/bindings/node/src/value_tests.rs" <<'EOF'

// #3522 PLANTED BREAK (binding-lane observation harness) — reverted by the harness.
#[test]
fn brl_3522_planted_node_value_break() {
    assert_eq!(1 + 1, 3, "#3522 planted break in cqlite-node's Rust unit suite");
}
EOF
}
plant_marker_node_rust_unit='brl_3522_planted_node_value_break'
plant_desc_node_rust_unit='a failing #[test] appended to bindings/node/src/value_tests.rs — cqlite-node is crate-type = ["cdylib"], so this also observes that the --lib harness genuinely links and runs'

# (d) THE ZERO-TEST CASE. cfg the whole cqlite-node unit suite OUT behind a feature that
# does not exist. The crate still COMPILES, the `--lib` harness still runs, it executes
# ZERO tests and cargo exits 0 — the vacuous green. Only check_unittest_targets_ran's
# non-zero-count half can see this; the three failing-assertion plants above cannot, so
# without this case that half of the guard would be unobserved.
#
# The marker is the unittest target PATH, because that is what the guard names in its
# diagnostic ("src/lib.rs(ran 0 tests: ...)"). A `#[cfg]` on the mod declarations is used
# rather than deleting the files, so the plant is a realistic cfg accident rather than a
# structural edit.
plant_zero_tests() {
  python3 - "$TREE/bindings/node/src" <<'ZEROPLANT'
import os, re, sys

src = sys.argv[1]

# SWEEP THE CLASS, NOT THE INSTANCE (roborev round 1, B3). The first cut of this plant
# edited `bindings/node/src/lib.rs`, which contains NO `#[cfg(test)]` at all -- the only
# occurrence of that string there is PROSE inside a comment -- so the plant could never
# apply and the case reported HARNESS-ERROR. The obvious repair (name the sibling files
# that DO carry it) is the SAME staleness bug one level down, and it was already wrong on
# arrival: two independent reviews produced two DIFFERENT file lists, one of them
# incomplete. So the set is DERIVED here, at plant time, from the committed source.
targets = []
for name in sorted(os.listdir(src)):
    if not name.endswith(".rs"):
        continue
    path = os.path.join(src, name)
    with open(path) as fh:
        text = fh.read()
    # The ATTRIBUTE, at the start of a line (modulo indentation) -- not the bare string,
    # which appears in prose. Every real `#[cfg(test)]` in this crate is on its own line.
    if re.search(r'^[ \t]*#\[cfg\(test\)\]', text, re.M):
        targets.append(path)

# FAIL LOUDLY on an empty derivation -- never a silent no-op plant. An empty subject set
# here means the crate stopped using `#[cfg(test)]` (or the pattern stopped matching), and
# a plant that quietly applies to nothing turns this case into the vacuous green the whole
# harness exists to detect. The harness treats a failed plant as HARNESS-ERROR, not as a
# lane finding, which is the correct attribution.
assert targets, "no file under bindings/node/src carries a line-initial #[cfg(test)] to neutralise"

# `#[cfg(any())]`, NOT `#[cfg(feature = "does-not-exist")]`. An unknown feature name trips
# rustc's `unexpected_cfgs` lint, and under `-D warnings` that reds the case via a COMPILE
# FAILURE instead of via the zero-test guard -- the right colour by the wrong mechanism,
# proving nothing about the guard this case exists to observe. `any()` is unconditionally
# false and lint-clean.
n = 0
for path in targets:
    with open(path) as fh:
        text = fh.read()
    new, k = re.subn(r'(?m)^([ \t]*)#\[cfg\(test\)\]', r'\1#[cfg(any())]', text)
    assert k, path
    with open(path, "w") as fh:
        fh.write(new)
    n += k
print("zero-tests plant: neutralised %d #[cfg(test)] attribute(s) across %d file(s): %s"
      % (n, len(targets), " ".join(os.path.basename(t) for t in targets)))
ZEROPLANT
}
plant_marker_zero_tests='ran 0 tests'
plant_desc_zero_tests='every line-initial #[cfg(test)] under bindings/node/src (file set DERIVED at plant time, never listed) rewritten to #[cfg(any())] — the crate COMPILES clean, the --lib harness RUNS, it executes ZERO tests and cargo exits 0. The vacuous green; only check_unittest_targets_ran'"'"'s non-zero-COUNT half sees it, which no failing-assertion plant exercises. #[cfg(any())] rather than an unknown feature name, so it cannot red via the unexpected_cfgs lint under -D warnings instead of via the guard'

# (e) A failing assertion in a jest suite the OLD node-bindings scope did not run.
# shared-vectors.test.js is chosen deliberately: it carries the cross-binding SHA-256
# exact oracles, whose entire value is that Python, Node and Rust agree byte-for-byte,
# and before #3522 it executed in no merge-gating lane. Firing proves the widening is
# real and not just a changed comment.
plant_node_jest() {
  cat >> "$TREE/bindings/node/__test__/shared-vectors.test.js" <<'EOF'

// #3522 PLANTED BREAK (binding-lane observation harness) — reverted by the harness.
test('brl_3522_planted_shared_vectors_break', () => {
  expect(1 + 1).toBe(3);
});
EOF
}
plant_marker_node_jest='brl_3522_planted_shared_vectors_break'
plant_desc_node_jest='a failing test appended to bindings/node/__test__/shared-vectors.test.js — one of the 26 suites the pre-#3522 `npx jest write-readback-content` scope never ran, and the one carrying the cross-binding SHA-256 exact oracles'

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
# One direction of one case.
#
# `--only` exit codes are load-bearing and NOT the usual 0/1: a PARTIAL run that found
# nothing exits 3 (the gate refuses to let a partial run be scripted into a green
# claim), and a PARTIAL run with a failed component exits 1. So the CLEAN expectation is
# 3, not 0. The exit code alone is not trusted either — the component's own SUMMARY line
# is parsed and both must agree.
# ---------------------------------------------------------------------------
# _marker_in_sibling_logs <marker> <component-log> — search the component's SIBLING logs
# (binding-rust-tests writes each package's cargo output to `<component>.<pkg>.log` and its
# guard verdicts to `<component>.guards.log`; they cannot share one file, because both
# packages print `Running unittests src/lib.rs` and the unittest guard keys on that path).
#
# ITERATED, NOT PASSED AS AN UNQUOTED GLOB (roborev round 1, B6). This used to be
# `grep -qF -- "$marker" "${log%.log}".*.log 2>/dev/null`, which relies on bash's DEFAULT
# no-`nullglob` failure mode: bash honours `BASHOPTS` from the environment at startup, so
# `BASHOPTS=nullglob bash ...` expands the pattern to NOTHING, grep then reads STDIN, and the
# harness either BLOCKS INDEFINITELY or -- at EOF -- silently reports FIRED-UNATTRIBUTED. A
# failure that depends on an ambient shell option this harness never sets and cannot control
# is the same class agent-gate.sh's own round-34 "`find`, not a glob" comment records.
# THE FIRST FIX OF THIS WAS INCOMPLETE, AND THAT IS THE POINT (roborev round 4, E2). B6
# replaced the INLINE unquoted glob at the call site with this function — and left the SAME
# glob inside the function. `[ -f "$f" ]` per candidate handles `nullglob` (the pattern
# expands to nothing, the body is skipped) but CANNOT handle `failglob`: with that option the
# shell ABORTS AT THE `for` LINE, before any guard in the body can run, so an unattributed
# Node failure would terminate the harness instead of producing its diagnostic summary.
# "Fixed one site, missed its sibling" is the pattern agent-gate.sh's own round 35->38 comment
# records as its fifth instance; this is ours.
#
# A bounded `find` instead, which is what the round-34 comment prescribes: its meaning does
# not depend on ambient shell options this harness never sets and cannot control
# (`nullglob`/`failglob`, both reachable through BASHOPTS). `-maxdepth 1` because the subject
# is SIBLING files in one directory, not a tree. The find's status is deliberately not fatal:
# no sibling logs is a legitimate state (node-bindings writes only one log), and the caller
# already treats "marker not found" as UNATTRIBUTED, which is the fail-closed direction.
_marker_in_sibling_logs() {
  local marker="$1" complog="${2:-}" dir base f
  [ -n "$complog" ] || return 1
  dir=$(dirname -- "$complog") || return 1
  base=$(basename -- "${complog%.log}") || return 1
  [ -d "$dir" ] || return 1
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    [ -f "$f" ] || continue
    if grep -qF -- "$marker" "$f"; then return 0; fi
  done <<EOF
$(find -H "$dir" -maxdepth 1 -type f -name "$base.*.log" -print 2>/dev/null)
EOF
  return 1
}

RC=0
STATUS=""
COMPONENT_LOG=""
LAST_LOG=""
run_direction() { # <case> <lane> <tag>
  local cse=$1 lane=$2 tag=$3
  local sf="$WORK/summary-$cse-$tag.txt" lg="$WORK/gate-$cse-$tag.log"
  local t0 t1
  t0=$(date +%s)
  ( cd "$TREE" && env \
      AGENT_GATE_SUMMARY_FILE="$sf" \
      CARGO_TARGET_DIR="$TARGET" \
      CQLITE_DATASETS_ROOT="${DATASETS:-}" \
      bash scripts/agent-gate.sh --only "$lane" >"$lg" 2>&1 )
  RC=$?
  t1=$(date +%s)
  STATUS=$(sed -n "s/^${lane}:[[:space:]]*\([A-Z][A-Z-]*\).*/\1/p" "$sf" 2>/dev/null | tail -1)
  # The gate's per-component log, named by the SUMMARY's own `logs:` line. Used for
  # ATTRIBUTION; the harness's captured stdout is the fallback, since the FAIL branch
  # tails only 40 lines of the component log into it.
  COMPONENT_LOG=""
  local logdir
  logdir=$(sed -n 's/^logs: //p' "$sf" 2>/dev/null | tail -1)
  [ -n "$logdir" ] && [ -f "$logdir/$lane.log" ] && COMPONENT_LOG="$logdir/$lane.log"
  [ -n "$STATUS" ] || STATUS="<no ${lane} line in the summary>"
  printf '  %-7s exit=%s  summary says "%s: %s"  (%ss)\n' "$tag" "$RC" "$lane" "$STATUS" "$((t1 - t0))"
  LAST_LOG="$lg"
}

RESULTS=()
FAILED=0
START=$(date +%s)

for cse in "${CASES[@]}"; do
  fn="${cse//-/_}"
  lane_var="lane_${fn}"; lane="${!lane_var}"
  desc_var="plant_desc_${fn}"
  echo "---- $cse (lane: $lane) ----"
  echo "  plant: ${!desc_var}"

  unplant || { RESULTS+=("$cse|HARNESS-ERROR|tree would not revert before the clean run"); FAILED=1; continue; }

  run_direction "$cse" "$lane" clean
  clean_rc=$RC clean_status=$STATUS

  "plant_${fn}" || { RESULTS+=("$cse|HARNESS-ERROR|the plant itself failed to apply"); FAILED=1; unplant; continue; }
  run_direction "$cse" "$lane" planted
  planted_rc=$RC planted_status=$STATUS
  planted_log=$LAST_LOG
  planted_component_log=$COMPONENT_LOG

  unplant || { RESULTS+=("$cse|HARNESS-ERROR|tree would not revert after the planted run"); FAILED=1; continue; }

  clean_ok=0;   [ "$clean_rc" = 3 ]   && [ "$clean_status" = PASS ] && clean_ok=1
  planted_ok=0; [ "$planted_rc" = 1 ] && [ "$planted_status" = FAIL ] && planted_ok=1

  marker_var="plant_marker_${fn}"
  marker="${!marker_var}"
  attributed=0
  if [ "$planted_ok" = 1 ]; then
    # The per-package sibling logs too: binding-rust-tests writes each package's cargo
    # output to `<component>.<package>.log` (they cannot share one file — both print
    # `Running unittests src/lib.rs`, which the unittest guard keys on).
    if { [ -n "$planted_component_log" ] && grep -qF "$marker" "$planted_component_log"; } \
       || grep -qF "$marker" "$planted_log" \
       || _marker_in_sibling_logs "$marker" "$planted_component_log"; then
      attributed=1
    fi
  fi

  if [ "$clean_ok" = 1 ] && [ "$planted_ok" = 1 ] && [ "$attributed" = 1 ]; then
    RESULTS+=("$cse|FIRED|clean=PASS(exit 3), planted=FAIL(exit 1) naming '$marker'")
    echo "  => FIRED: silent on the clean tree, red on the planted break, and the red NAMES"
    echo "     the planted symbol ('$marker') — so the red is this plant's, not an unrelated one."
  elif [ "$clean_ok" = 1 ] && [ "$planted_ok" = 1 ]; then
    RESULTS+=("$cse|FIRED-UNATTRIBUTED|the lane red'd but its output never names '$marker'")
    echo "  => FIRED, BUT UNATTRIBUTED: the lane went red and never named the planted symbol"
    echo "     ('$marker'), so the red cannot be shown to be this plant's. Not an observation."
    FAILED=1
  elif [ "$clean_ok" = 0 ] && [ "$planted_ok" = 1 ]; then
    RESULTS+=("$cse|HARNESS-FAILURE|clean direction did not pass (exit $clean_rc, status $clean_status) — red in both directions proves nothing")
    echo "  => HARNESS FAILURE: the lane was already red on the CLEAN tree, so its red on the"
    echo "     planted break is not evidence of anything. Fix the baseline, do not adjust the plant."
    FAILED=1
  elif [ "$clean_ok" = 1 ]; then
    RESULTS+=("$cse|DID-NOT-FIRE|clean=PASS but the planted break produced exit $planted_rc, status $planted_status")
    echo "  => DID NOT FIRE: the planted break did not red the lane. This is a REAL FINDING about"
    echo "     the lane, not a harness knob — do not adjust the plant until it fires."
    echo "     planted-run log: $planted_log (last 30 lines)"
    tail -30 "$planted_log" | sed 's/^/       /'
    FAILED=1
  else
    RESULTS+=("$cse|HARNESS-FAILURE|clean exit $clean_rc/$clean_status, planted exit $planted_rc/$planted_status — neither direction behaved as specified")
    echo "  => HARNESS FAILURE: neither direction behaved as specified."
    FAILED=1
  fi
  echo
done

# MEASURED before any verdict is printed: a success accompanied by a mutated live
# checkout is not a success.
assert_live_checkout_untouched "pre-summary" || FAILED=1

END=$(date +%s)
echo "==== #3522 OBSERVATION SUMMARY ===="
# The CAPTURED commit, never a fresh `git rev-parse HEAD` at emit time: re-reading HEAD
# here would attribute this run's evidence to a commit it never examined.
echo "observed-commit: $LIVE_HEAD_BEFORE (captured at start; the throwaway copy was made from THIS sha)"
echo "live-tree: $([ "$LIVE_TREE_VIOLATED" -eq 0 ] && echo "UNCHANGED (verified: git status --porcelain AND HEAD identical to start)" || echo "MUTATED — HARNESS FAILURE")"
echo "elapsed:  $((END - START))s"
[ "$SUBSET" -eq 1 ] && echo "mode:     SUBSET (${CASES[*]}) — a partial observation, NOT the full AC3 evidence"
for r in "${RESULTS[@]}"; do
  printf '%-22s %-18s %s\n' "${r%%|*}" "$(echo "$r" | cut -d'|' -f2)" "$(echo "$r" | cut -d'|' -f3-)"
done
if [ "$FAILED" -ne 0 ]; then
  echo "RESULT: FAIL (a case did not fire, its clean baseline was not established, or the live checkout was mutated)"
  exit 1
fi
if [ "$SUBSET" -eq 1 ]; then
  echo "RESULT: PARTIAL (every SELECTED case fired; run with no arguments for the full observation)"
  exit 3
fi
echo "RESULT: PASS (all ${#ALL_CASES[@]} cases observed to fire on a planted break and stay silent on a clean tree)"
exit 0
