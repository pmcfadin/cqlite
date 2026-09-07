#!/usr/bin/env bash
#
# test_agent_gate_all_features_lane.sh — planted-break observation harness for the
# `all-features-check` gate lane added by issue #3453.
#
# WHAT THIS ANSWERS, and why nothing else does.
# `--list` proves the lane is REGISTERED (that is section 53 of
# scripts/tests/test_agent_gate_summary.sh, and it is cheap enough to run everywhere).
# A green SUMMARY line proves the lane RAN and found nothing. NEITHER proves the lane
# CAN FAIL: `feature-iso-parquet` reports `PASS (0s)` warm, so presence proves nothing
# at all. This harness is the only affirmative evidence that `all-features-check` fires
# on the incident class it was built for.
#
# AND IT PROVES THE THESIS, NOT JUST THE LANE. Issue #3453's claim is not merely "a new
# lane can go red"; it is that the EXISTING components CANNOT go red on this class. The
# gap is MEASURED, not anecdotal: `cargo test -p cqlite-core --lib -- --list` discovers
# 3562 tests at the gate's `--features cli-helpers` and 3782 at pr-gate's
# `--all-features`, so 220 lib tests execute in CI and NOWHERE in the gate of record —
# #3382's own fix pin among them, unlistable at the gate's feature set. That is how a
# 31/31 gate PASS on PR #3382 never executed the test pinning that PR's own fix. This
# harness cannot observe those 220 EXECUTING (nothing local does; the lane compiles and
# lints only), which is exactly why the CONTROL below matters: it demonstrates the
# COMPILE/LINT half was blind too. So every planted run here is a THREE-WAY observation:
#   * all-features-check on the PLANTED tree  -> must FAIL, and must NAME the plant
#   * all-features-check on the CLEAN tree    -> must PASS (a lane red in both
#                                               directions proves nothing — #3229)
#   * clippy + core-tests on the SAME PLANT   -> must PASS (the CONTROL; without it
#                                               the "nothing else covers this" claim is
#                                               asserted rather than demonstrated)
# The control is the half that cannot be replaced by argument. run_clippy's exclusion of
# observability/observability-testing/metrics is #1844 doctrine, and core-tests runs
# `--features cli-helpers` — but doctrine drifts from code, and if either ever starts
# enabling the OTel stack the control turns red and tells us this lane is redundant.
#
# THE PLANTS ARE THE INCIDENT CLASS, not a syntax error. A syntax error fires every lane
# that compiles anything, so it discriminates nothing. Each plant is an item that
# compiles fine with the OTel features OFF (it does not exist) and is broken with them
# ON — which is exactly why no existing component sees it. Two plants, one per half of
# the lane:
#   observability-type   a type error inside a #[cfg(feature = "observability")] item
#                        -> caught by pass 1 (cargo check). NOT by pass 2: the component
#                           SKIPS clippy once an earlier pass has failed, so neither of the
#                           first two plants ever exercised the clippy pass in the FAILING
#                           direction. That gap was real and is why observability-clippy
#                           exists (roborev job 281) — a header claiming coverage the
#                           harness does not have is worse than no claim.
#   observability-lint   a `-D warnings`-class lint (an unused variable) inside a
#                        #[cfg(feature = "observability-testing")] item -> caught only
#                        because -D warnings is in force. It is gated on
#                        `observability-testing`, DELIBERATELY: the #3382 tests are
#                        gated on that feature and not on `observability`, so a lane
#                        that only ever enabled the latter would have missed them.
#
# ATTRIBUTED, NEVER MERELY RED. A lane broken for an unrelated reason produces an
# identical exit code and an identical SUMMARY line, so a bare red is not evidence: the
# planted run's output must NAME the planted symbol, or it is reported as
# FIRED-UNATTRIBUTED and the harness fails.
#
# ISOLATION. Every mutation happens in a throwaway `git worktree add --detach` copy,
# never the live checkout — #2926 makes a mid-run tree mutation a gate FAIL, so a harness
# that edited the tree its own gate was running in would be the defect it exists to
# catch. The live checkout's HEAD and `git status --porcelain` are captured at start and
# re-verified before the summary and again in cleanup; any difference is a HARNESS
# FAILURE. It also REFUSES to run from a dirty checkout: the worktree is made from
# committed HEAD, so uncommitted changes are silently excluded and a PASS would describe
# code other than the code in front of you.
#
# OPT-IN, NOT A GATE COMPONENT — AND THE DECIDING NUMBER IS DISK, NOT TIME. It performs
# real `--all-features` compiles (the OTel stack) several times over: MEASURED 2617s and a
# **47-56G peak** throwaway target dir (OBSERVED: 47G on a two-plant run, 56G on a one-plant
# subset — the peak is per-BUILD, not cumulative). Slow is recoverable; filling a
# shared box is not — on this fleet a lane's target dir has reached 89G and filled the host,
# and the symptom is a confusing unrelated-looking red that costs a diagnosis cycle before
# anyone greps for ENOSPC. ~50G+ per run is the strongest single argument that this must never
# join `tooling-tests`, so it is recorded as a measurement rather than left to be assumed.
# Same convention as scripts/tests/test_agent_gate_feature_matrix_lanes.sh (issue #1699
# design D5): deliberately absent from COMPONENTS / LITE_COMPONENTS / DELTA_COMPONENTS.
#
# WHERE THAT ~50G LANDS IS CHOSEN, NOT INHERITED. `mktemp -d` would default to $TMPDIR
# (i.e. /tmp, which on the worker boxes lives on the 145G root filesystem alongside every
# other lane), so this harness instead defaults its scratch to $REPO_ROOT/target/ — the
# same filesystem as the checkout, which is the volume with the headroom (295G) and the one
# a lane's own cleanup already targets. `target/` is gitignored, so a throwaway worktree
# there leaves `git status --porcelain` untouched, which this harness asserts. Override with
# an ABSOLUTE CQLITE_HARNESS_SCRATCH. It also PREFLIGHTS free space and REFUSES rather than
# starting a run it has measured cause to believe cannot finish: an ENOSPC halfway through
# is indistinguishable from a real lane failure, and it takes the whole box with it.
#
# ONE TREE, ONE TARGET DIR, SERIAL BY CONSTRUCTION. The plants share a single throwaway
# worktree and a single target dir, reverted between plants — never two concurrent trees, so
# the peak above is the peak and not a per-plant multiple. Removal happens in a `trap` on
# EXIT/INT/TERM, i.e. on the FAILURE paths too (verified: a run that ended
# `RESULT: FAIL` on an unattributed plant left no `afc-3453-*` directory behind).
#
# Usage:
#   bash scripts/tests/test_agent_gate_all_features_lane.sh                  # both plants
#   bash scripts/tests/test_agent_gate_all_features_lane.sh <plant> ...      # subset
#     plants: observability-type observability-lint observability-clippy
#   CQLITE_DATASETS_ROOT=<abs>  required ONLY for the control run's core-tests half;
#                               without it the control degrades to clippy alone and SAYS SO.
#
# Exit: 0 = every selected plant observed to fire, with its control green; 1 = a plant
# did not fire, a clean baseline could not be established, a control went red, or the
# live checkout was mutated; 2 = usage / precondition refusal; 3 = a SUBSET run in which
# everything selected fired (a partial observation).

set -uo pipefail

# #3637: the gate now REMOVES its per-run log dir on a terminal PASS and on ANY
# verdict when it is nested (which it is here — this script runs inside the gate's
# tooling-tests component, so the child gates below inherit AGENT_GATE_PARENT_RUN_ID).
# These cases READ `<logdir>/<component>.log` AFTER the child exits, so they opt out.
export AGENT_GATE_KEEP_LOGS=1

REPO_ROOT=$(git rev-parse --show-toplevel) || { echo "not a git checkout" >&2; exit 1; }
cd "$REPO_ROOT" || exit 1

LANE=all-features-check
ALL_PLANTS=(observability-type observability-lint observability-clippy)

SUBSET=0
if [ "$#" -gt 0 ]; then
  SUBSET=1
  PLANTS=("$@")
  for p in "${PLANTS[@]}"; do
    case " ${ALL_PLANTS[*]} " in
      *" $p "*) ;;
      *) echo "unknown plant: $p (known: ${ALL_PLANTS[*]})" >&2; exit 2 ;;
    esac
  done
else
  PLANTS=("${ALL_PLANTS[@]}")
fi

# The corpus root. This lane needs NONE (it is absent from DATASET_COMPONENTS and opens
# no fixture), but the CONTROL runs core-tests, which does. A missing root therefore
# NARROWS the control rather than failing the run — and the narrowing is REPORTED, since
# a control that silently shrank would be the vacuous half of the observation.
DATASETS="${CQLITE_DATASETS_ROOT:-}"
case "${DATASETS:-/}" in
  /*) ;;
  *) echo "CQLITE_DATASETS_ROOT must be ABSOLUTE (got: $DATASETS)" >&2; exit 2 ;;
esac
CONTROL_COMPONENTS="clippy,core-tests"
CONTROL_NOTE="clippy + core-tests"
if [ -z "$DATASETS" ]; then
  CONTROL_COMPONENTS="clippy"
  CONTROL_NOTE="clippy ONLY (no CQLITE_DATASETS_ROOT, so core-tests was excluded from the control)"
fi

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
    echo "  This harness observes the lane against a throwaway worktree created from"
    echo "  committed HEAD ($(git -C "$REPO_ROOT" rev-parse --short HEAD)), so uncommitted"
    echo "  changes are NOT part of any run. A PASS here would describe HEAD, not the"
    echo "  working tree you are looking at. Commit (or stash) first; there is deliberately"
    echo "  no override, since the only thing one could buy is a green about code nobody"
    echo "  changed."
    echo
    echo "  dirty paths:"
    printf '%s\n' "$LIVE_STATUS_BEFORE" | sed 's/^/    /'
  } >&2
  exit 2
fi

LIVE_TREE_VIOLATED=0
assert_live_checkout_untouched() { # <phase>
  local phase="$1" now now_head
  now=$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null) || {
    echo "HARNESS FAILURE ($phase): could not re-read the live checkout's git status," >&2
    echo "  so the never-mutate-the-live-checkout invariant is UNVERIFIABLE here." >&2
    LIVE_TREE_VIOLATED=1; return 1; }
  now_head=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null) || {
    echo "HARNESS FAILURE ($phase): could not re-read the live checkout's HEAD," >&2
    echo "  so the invariant is UNVERIFIABLE here (unreadable is never 'unchanged')." >&2
    LIVE_TREE_VIOLATED=1; return 1; }
  if [ "$now_head" != "$LIVE_HEAD_BEFORE" ]; then
    echo "HARNESS FAILURE ($phase): the live checkout's HEAD MOVED during the run" >&2
    echo "  ($LIVE_HEAD_BEFORE -> $now_head). git status alone cannot see this, and the" >&2
    echo "  observation above was made against the OLD commit — a stale certification." >&2
    LIVE_TREE_VIOLATED=1; return 1; fi
  if [ "$now" != "$LIVE_STATUS_BEFORE" ]; then
    echo "HARNESS FAILURE ($phase): the LIVE CHECKOUT changed during the run." >&2
    printf '%s\n' "$now" | sed 's/^/    /' >&2
    LIVE_TREE_VIOLATED=1; return 1; fi
  return 0
}

# The scratch root — CHOSEN, not inherited from $TMPDIR (see the header). Default is this
# checkout's own gitignored target/, which is on the big volume; an absolute
# CQLITE_HARNESS_SCRATCH overrides it.
SCRATCH_ROOT="${CQLITE_HARNESS_SCRATCH:-$REPO_ROOT/target/afc-3453-harness}"
case "$SCRATCH_ROOT" in
  /*) ;;
  *) echo "CQLITE_HARNESS_SCRATCH must be ABSOLUTE (got: $SCRATCH_ROOT)" >&2; exit 2 ;;
esac
mkdir -p "$SCRATCH_ROOT" || { echo "FATAL: could not create scratch root $SCRATCH_ROOT" >&2; exit 1; }

# PREFLIGHT THE SPACE, and refuse rather than start. The measured peak for a full run is
# 47G on a two-plant run and 56G on a ONE-plant subset, so peak is NOT cumulative in plant
# count: each plant reverts and rebuilds, so a single --all-features build tree drives it and a
# subset run can exceed a full one. The floor below is ~4G above the HIGHEST OBSERVATION, not
# 13G above a bound -- do NOT tighten it toward 47G. The failure being
# avoided is not this run's but the BOX's — three other lanes share this filesystem, and an
# ENOSPC surfaces as an unrelated-looking red in somebody else's gate. `df -Pk` for the
# POSIX-portable single-line form; an unreadable df is treated as UNKNOWN and REFUSED, never
# as enough room (an unmeasured resource is not a measured one).
AFC_MIN_FREE_GB=60
free_kb=$(df -Pk "$SCRATCH_ROOT" 2>/dev/null | awk 'NR==2 {print $4}')
case "${free_kb:-}" in
  ''|*[!0-9]*)
    echo "REFUSING TO RUN: could not measure free space on $SCRATCH_ROOT (df unreadable)." >&2
    echo "  A run peaks at an observed 47-56G of throwaway build output; starting one against an" >&2
    echo "  unmeasured filesystem risks an ENOSPC that reds every other lane on this box." >&2
    exit 2 ;;
esac
free_gb=$((free_kb / 1024 / 1024))
if [ "$free_gb" -lt "$AFC_MIN_FREE_GB" ]; then
  {
    echo "REFUSING TO RUN: only ${free_gb}G free on $SCRATCH_ROOT (need >= ${AFC_MIN_FREE_GB}G)."
    echo
    echo "  A run peaks at a MEASURED 47-56G of throwaway --all-features build"
    echo "  output. Finishing is not the concern; an ENOSPC mid-run is, because it surfaces"
    echo "  as a confusing unrelated red in whatever else shares this filesystem."
    echo
    echo "  Free some space, or point CQLITE_HARNESS_SCRATCH at an absolute path on a volume"
    echo "  that has room."
  } >&2
  exit 2
fi

WORK=$(mktemp -d "$SCRATCH_ROOT/run-XXXXXX") || exit 1
TREE="$WORK/tree"
# OUTSIDE the throwaway tree so the clean and planted runs share compilation (otherwise
# each planted run pays a full cold --all-features build), and PRIVATE (under an
# `mktemp -d`) rather than a predictable shared path: a fixed path lets two concurrent
# harnesses corrupt each other's incremental state, and on a multi-user host lets another
# user pre-create a directory whose compiled artifacts this script would then execute.
TARGET="$WORK/target"

cleanup() {
  local rc=$?
  # A SIGNAL MUST FORCE A NON-ZERO VERDICT: `cleanup` inherits `$?`, which is ZERO for a
  # signal delivered between commands — so an INTERRUPTED observation would exit
  # successfully, which is this issue's own defect class inside its own harness.
  local sig="${1:-}"
  case "$sig" in INT) rc=130 ;; TERM) rc=143 ;; esac
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

echo "==== #3453 ALL-FEATURES LANE OBSERVATION ===="
echo "repo:     $REPO_ROOT"
echo "head:     $LIVE_HEAD_BEFORE"
echo "lane:     $LANE"
echo "plants:   ${PLANTS[*]}"
[ "$SUBSET" -eq 1 ] && echo "mode:     SUBSET (partial observation)"
echo "control:  $CONTROL_NOTE"
echo "worktree: $TREE (throwaway; the live checkout is never mutated)"
echo "target:   $TARGET"
echo "scratch:  $SCRATCH_ROOT (${free_gb}G free; runs peak at a measured 47-56G)"
echo

git worktree add --detach "$TREE" "$LIVE_HEAD_BEFORE" >/dev/null 2>&1 || {
  echo "FATAL: could not create the throwaway worktree" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Plants. Appended to cqlite-core/src/lib.rs, so they are reached by a `--lib`
# compile as well as `--all-targets` — the point is the FEATURE GATE, not the target.
# ---------------------------------------------------------------------------

# A TYPE ERROR under `observability`. Invisible with the feature off (the item does not
# exist), so core-tests (--features cli-helpers) and run_clippy (which EXCLUDES
# observability by #1844 design) compile straight past it.
plant_observability_type() {
  cat >> "$TREE/cqlite-core/src/lib.rs" <<'EOF'

// #3453 PLANTED BREAK (all-features lane observation harness) — reverted by the harness.
#[cfg(feature = "observability")]
pub fn afc3453_planted_observability_type_break() -> u32 {
    let s: &str = "not a u32";
    s
}
EOF
}
plant_marker_observability_type='afc3453_planted_observability_type_break'
plant_desc_observability_type='a TYPE ERROR inside a #[cfg(feature = "observability")] fn in cqlite-core/src/lib.rs — the item does not exist with the feature off, which is exactly why run_clippy (excludes observability per #1844) and core-tests (--features cli-helpers) compile past it'

# A `-D warnings`-CLASS LINT under `observability-testing`. Gated on that feature and not
# on `observability` deliberately: the #3382 tests are gated on `observability-testing`,
# so a lane enabling only `observability` would have missed them. It also discriminates
# the two halves of the lane — this fires ONLY because -D warnings is in force, so it is
# evidence about the guard, not merely about the compile.
#
# THE MARKER IS THE VARIABLE NAME, NOT THE FUNCTION NAME — measured, not guessed. The
# first version of this plant named the enclosing fn `afc3453_planted_..._break` and used
# that as the marker; the lane duly went red and the harness reported FIRED-UNATTRIBUTED,
# because rustc's diagnostic is `unused variable: \`<binding>\`` and names the BINDING and
# a file:line, never the item it sits in. So the binding carries the marker. That round
# is worth recording: the harness refused to accept a red it could not attribute, which
# is exactly the discrimination it exists for.
plant_observability_lint() {
  cat >> "$TREE/cqlite-core/src/lib.rs" <<'EOF'

// #3453 PLANTED BREAK (all-features lane observation harness) — reverted by the harness.
#[cfg(feature = "observability-testing")]
pub fn afc3453_planted_observability_lint_fn() -> bool {
    let afc3453_planted_observability_lint_break = 42;
    true
}
EOF
}
plant_marker_observability_lint='afc3453_planted_observability_lint_break'

# A CLIPPY-ONLY lint: it must PASS `cargo check -D warnings` and FAIL `cargo clippy -D
# warnings`. Without it the clippy half of the lane was never observed failing, because the
# component runs check FIRST and skips clippy once it fails — so both earlier plants proved
# only pass 1. That matters here more than usual: clippy at --all-features is precisely what
# run_clippy EXCLUDES by #1844 design, i.e. the half of this component with no other cover.
#
# MEASURED, not chosen from a list: `clippy::needless_return` produces NO rustc diagnostic
#   cargo check  -p cqlite-core --all-features --all-targets (RUSTFLAGS=-D warnings) -> rc=0
#   cargo clippy -p cqlite-core --all-features --all-targets -- -D warnings           -> rc=101
#                                                          "error: unneeded `return` statement"
#
# AND THE MARKER IS THE BINDING NAME, for the reason the lint plant above records: clippy
# points at the STATEMENT (`return afc3453_planted_clippy_only_v + 1;`) and a file:line, never
# the enclosing item. Verified by reading the real diagnostic, not assumed.
plant_observability_clippy() {
  cat >> "$TREE/cqlite-core/src/lib.rs" <<'PLANTEOF'

// #3453 PLANTED BREAK (clippy-only; all-features lane observation harness) — reverted by the harness.
#[cfg(feature = "observability")]
pub fn afc3453_planted_clippy_only() -> u32 {
    let afc3453_planted_clippy_only_v = 41u32;
    return afc3453_planted_clippy_only_v + 1;
}
PLANTEOF
}
plant_marker_observability_clippy='afc3453_planted_clippy_only_v'
plant_desc_observability_clippy='a CLIPPY-ONLY lint (clippy::needless_return) inside a #[cfg(feature = "observability")] fn — it PASSES cargo check -D warnings and fails ONLY the clippy pass, so it is the only plant that observes pass 2 in the failing direction'
# Which passes this plant must exercise, asserted from the component log below. The other two
# plants stop at pass 1, so only this one can pin pass 2.
plant_passes_observability_clippy='1=OK,2=FAIL'
plant_desc_observability_lint='an UNUSED-VARIABLE (-D warnings class) lint inside a #[cfg(feature = "observability-testing")] fn — gated on observability-TESTING because that, not `observability`, is what gates the #3382 tests; fires only because -D warnings is in force'

# One uniform revert: restore tracked files, delete untracked ones. `clean -fd` without
# -x deliberately spares the gitignored build output.
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
# One gate run against the throwaway tree.
#
# `--only` exit codes are load-bearing and NOT the usual 0/1: a PARTIAL run that found
# nothing exits 3 (the gate refuses to let a partial run be scripted into a green
# claim), and a PARTIAL run with a failed component exits 1. So the CLEAN expectation is
# 3, not 0. The exit code alone is not trusted: each named component's own SUMMARY line
# is parsed too, and both must agree.
# ---------------------------------------------------------------------------
RC=0
STATUSES=""
COMPONENT_LOGS=""
LAST_LOG=""
run_only() { # <components-csv> <tag>
  local comps=$1 tag=$2
  local sf="$WORK/summary-$tag.txt" lg="$WORK/gate-$tag.log"
  local t0 t1 c logdir
  t0=$(date +%s)
  # The corpus root is passed as an ARRAY element, not an unquoted `${VAR:+NAME=$VAR}`
  # expansion: the latter word-splits a root containing spaces and would hand `env` a
  # truncated assignment plus a bogus command name — a run that fails for a reason
  # entirely unrelated to the plant.
  local -a env_args=(AGENT_GATE_SUMMARY_FILE="$sf" CARGO_TARGET_DIR="$TARGET")
  [ -n "$DATASETS" ] && env_args+=(CQLITE_DATASETS_ROOT="$DATASETS")
  ( cd "$TREE" && env "${env_args[@]}" \
      bash scripts/agent-gate.sh --only "$comps" >"$lg" 2>&1 )
  RC=$?
  t1=$(date +%s)
  STATUSES=""
  COMPONENT_LOGS=""
  logdir=$(sed -n 's/^logs: //p' "$sf" 2>/dev/null | tail -1)
  for c in ${comps//,/ }; do
    local st
    st=$(sed -n "s/^${c}:[[:space:]]*\([A-Z][A-Z-]*\).*/\1/p" "$sf" 2>/dev/null | tail -1)
    [ -n "$st" ] || st="<no ${c} line in the summary>"
    STATUSES="$STATUSES $c=$st"
    [ -n "$logdir" ] && [ -f "$logdir/$c.log" ] && COMPONENT_LOGS="$COMPONENT_LOGS $logdir/$c.log"
  done
  printf '  %-24s exit=%s  summary:%s  (%ss)\n' "$tag" "$RC" "$STATUSES" "$((t1 - t0))"
  LAST_LOG="$lg"
}

# _status_of <component>: read one component's status out of the STATUSES string built
# by the last run_only. Parsed from the string rather than re-read from the file so a
# caller can never accidentally describe a different run.
_status_of() { # <component>
  local c=$1 tok
  for tok in $STATUSES; do
    case "$tok" in "$c="*) printf '%s' "${tok#*=}"; return 0 ;; esac
  done
  printf 'MISSING'
}

# ---------------------------------------------------------------------------
RESULTS=()
FAILED=0
START=$(date +%s)

# The CLEAN baseline, established ONCE (the plants are independent and each is reverted,
# so re-establishing it per plant would pay for the same measurement twice).
echo "---- clean baseline ----"
unplant || { echo "HARNESS ERROR: tree would not revert before the clean run" >&2; exit 1; }
run_only "$LANE" "clean"
clean_rc=$RC
clean_status=$(_status_of "$LANE")
clean_ok=0; [ "$clean_rc" = 3 ] && [ "$clean_status" = PASS ] && clean_ok=1
if [ "$clean_ok" = 1 ]; then
  echo "  => baseline OK: the lane is silent on a clean tree (PASS, exit 3)"
else
  echo "  => HARNESS FAILURE: no clean baseline (exit $clean_rc, status $clean_status). A lane"
  echo "     red in BOTH directions is not evidence of anything (#3229). Fix the baseline first."
  FAILED=1
fi
echo

for plant in "${PLANTS[@]}"; do
  fn="${plant//-/_}"
  desc_var="plant_desc_${fn}"
  marker_var="plant_marker_${fn}"
  marker="${!marker_var}"
  echo "---- $plant ----"
  echo "  plant: ${!desc_var}"

  unplant || { RESULTS+=("$plant|HARNESS-ERROR|tree would not revert before this plant"); FAILED=1; continue; }
  "plant_${fn}"

  run_only "$LANE" "planted-$plant"
  planted_rc=$RC
  planted_status=$(_status_of "$LANE")
  planted_log=$LAST_LOG
  planted_component_logs=$COMPONENT_LOGS

  # THE CONTROL, on the SAME PLANTED TREE. This is the issue's thesis: the components
  # that already existed cannot see this class.
  run_only "$CONTROL_COMPONENTS" "control-$plant"
  control_rc=$RC
  control_statuses=$STATUSES

  unplant || { RESULTS+=("$plant|HARNESS-ERROR|tree would not revert after this plant"); FAILED=1; continue; }

  planted_ok=0; [ "$planted_rc" = 1 ] && [ "$planted_status" = FAIL ] && planted_ok=1

  # ATTRIBUTION: the red must NAME the planted symbol, or it could be anything.
  attributed=0
  if [ "$planted_ok" = 1 ]; then
    local_hit=0
    for l in $planted_component_logs; do
      grep -qF "$marker" "$l" && local_hit=1
    done
    [ "$local_hit" = 1 ] && attributed=1
    [ "$attributed" = 0 ] && grep -qF "$marker" "$planted_log" && attributed=1
  fi

  # PASS DISCRIMINATION (roborev job 281). A plant may declare WHICH of the component's two
  # passes it must exercise, e.g. `1=OK,2=FAIL`. Without this, a plant that reds the lane
  # proves only that SOMETHING failed — and both original plants stop at pass 1, because the
  # component skips clippy once check has failed, so the clippy pass had no failing-direction
  # coverage at all. Read from the component's own log lines, which state each pass verdict.
  passes_var="plant_passes_${fn}"
  passes_spec="${!passes_var:-}"
  passes_ok=1
  passes_detail=""
  if [ -n "$passes_spec" ]; then
    for claim in ${passes_spec//,/ }; do
      want_n="${claim%%=*}"; want_v="${claim##*=}"
      hit=0
      for l in $planted_component_logs; do
        # Redirection, not a pipe: `grep -q` exits on first match and this file runs
        # `pipefail`, so a piped producer can take SIGPIPE and invert the verdict (#3685).
        # Portable boundary, not GNU `\b` (undefined in POSIX ERE, not honoured by BSD grep
        # on macOS). This one was introduced by the previous round's fix — the same defect it
        # was warning about, one file over.
        grep -qE "pass ${want_n}/2 .*: ${want_v}([^[:alnum:]_]|$)" "$l" && hit=1
      done
      [ "$hit" = 1 ] || { passes_ok=0; passes_detail="$passes_detail pass${want_n}!=${want_v}"; }
    done
    if [ "$passes_ok" = 1 ]; then
      echo "  pass-discrimination: OK — the component log shows $passes_spec, so this plant"
      echo "     exercises the pass it claims to (not merely 'the lane went red')"
    fi
  fi

  # The control passes IFF every control component reports PASS (a SKIP is NOT a pass —
  # a skipped control measures nothing, and reporting it as green would manufacture the
  # very evidence this half exists to provide).
  control_ok=1
  control_detail=""
  for tok in $control_statuses; do
    case "${tok#*=}" in
      PASS) ;;
      *) control_ok=0; control_detail="$control_detail ${tok}" ;;
    esac
  done
  [ "$control_rc" = 3 ] || { control_ok=0; control_detail="$control_detail exit=$control_rc"; }

  if [ "$clean_ok" = 1 ] && [ "$planted_ok" = 1 ] && [ "$attributed" = 1 ] && [ "$control_ok" = 1 ] && [ "$passes_ok" = 1 ]; then
    RESULTS+=("$plant|FIRED|lane FAIL naming $marker; control ($CONTROL_NOTE) stayed GREEN on the same plant")
    echo "  => FIRED: $LANE went red and NAMED $marker, while$control_statuses stayed green on the"
    echo "     SAME planted tree — which is issue #3453's thesis, demonstrated rather than asserted."
  elif [ "$planted_ok" = 1 ] && [ "$attributed" = 1 ] && [ "$control_ok" = 1 ] && [ "$passes_ok" = 0 ]; then
    RESULTS+=("$plant|WRONG-PASS|lane fired and control stayed green, but the component log does not show $passes_spec:$passes_detail")
    echo "  => WRONG PASS: the lane went red for the wrong reason —$passes_detail. The plant"
    echo "     was chosen to exercise a specific pass; a red from a different pass proves"
    echo "     nothing about the one it was written for."
    FAILED=1
  elif [ "$planted_ok" = 1 ] && [ "$attributed" = 1 ] && [ "$control_ok" = 0 ]; then
    RESULTS+=("$plant|CONTROL-RED|the lane fired, but the control also went red:$control_detail")
    echo "  => CONTROL RED: the lane fired, but$control_detail — so an EXISTING component already"
    echo "     covers this class and the plant does not discriminate. This is a REAL FINDING about"
    echo "     the component set (has run_clippy started enabling the OTel stack, contra #1844?),"
    echo "     not a harness knob. Do not weaken the control."
    FAILED=1
  elif [ "$planted_ok" = 1 ]; then
    RESULTS+=("$plant|FIRED-UNATTRIBUTED|the lane red'd but its output never names $marker")
    echo "  => FIRED, BUT UNATTRIBUTED: the red never names $marker, so it cannot be shown to be"
    echo "     this plant's. An unrelated breakage produces an identical exit code."
    FAILED=1
  else
    RESULTS+=("$plant|DID-NOT-FIRE|planted run gave exit $planted_rc, status $planted_status")
    echo "  => DID NOT FIRE: the planted break did not red the lane (exit $planted_rc, status"
    echo "     $planted_status). This is a REAL FINDING about the lane — do not adjust the plant"
    echo "     until it fires. planted-run log: $planted_log (last 30 lines)"
    tail -30 "$planted_log" | sed 's/^/       /'
    FAILED=1
  fi
  echo
done

assert_live_checkout_untouched "pre-summary" || FAILED=1

END=$(date +%s)
echo "==== #3453 OBSERVATION SUMMARY ===="
# The CAPTURED commit, never a fresh `git rev-parse HEAD`: the observation was made
# against the copy taken from $LIVE_HEAD_BEFORE, so re-reading HEAD here would attribute
# this run's evidence to a commit it never examined.
echo "observed-commit: $LIVE_HEAD_BEFORE (captured at start; the throwaway copy was made from THIS sha)"
echo "live-tree: $([ "$LIVE_TREE_VIOLATED" -eq 0 ] && echo "UNCHANGED (verified: git status --porcelain AND HEAD identical to start)" || echo "MUTATED — HARNESS FAILURE")"
echo "control:   $CONTROL_NOTE"
echo "elapsed:   $((END - START))s"
# The PEAK is not measurable after cleanup, so report what is: the size still on disk at
# summary time. Stated as what it is rather than labelled a peak it cannot observe.
echo "disk:      $(du -sh "$WORK" 2>/dev/null | awk '{print $1}') in $WORK at summary time (observed peaks: 47G two-plant, 56G one-plant subset -- per-BUILD, not cumulative; removed by the EXIT trap)"
[ "$SUBSET" -eq 1 ] && echo "mode:      SUBSET (${PLANTS[*]}) — a partial observation"
for r in "${RESULTS[@]}"; do
  printf '%-22s %-20s %s\n' "${r%%|*}" "$(echo "$r" | cut -d'|' -f2)" "$(echo "$r" | cut -d'|' -f3-)"
done
if [ "$FAILED" -ne 0 ]; then
  echo "RESULT: FAIL (a plant did not fire, a control went red, the clean baseline failed, or the live checkout was mutated)"
  exit 1
fi
if [ "$SUBSET" -eq 1 ]; then
  echo "RESULT: PARTIAL (every SELECTED plant fired with a green control; run with no arguments for the full observation)"
  exit 3
fi
echo "RESULT: PASS (all-features-check fires on both observability-gated plants, stays silent on a clean tree, and the pre-existing components stay green on the same plants)"
exit 0
