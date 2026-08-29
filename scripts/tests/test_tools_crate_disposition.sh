#!/usr/bin/env bash
# tools/ crate disposition census (issue #1716, epic #1688 finding AK5).
#
# ONE property, at CRATE granularity: every crate under tools/ is EXPLICITLY
# classified, and every crate carrying unwired targets carries a README that says
# so.
#
#   WIRED   something runs it.
#   UNWIRED nothing runs it and nothing depends on it.
#   MIXED   some targets are live and others are orphaned.
#
# Why it exists. AK5 was that three tools/ crates were invoked by no workflow, no
# script and no doc, and nothing said so — so they read as live tooling for months.
# #1716 labeled them. Without this guard the labeling silently decays: the root
# manifest's `members` globs `tools/*`, so a NEW crate joins the workspace with no
# statement of whether anything runs it, and a deleted README is invisible. Both
# are caught here, deterministically, with no false positives: the rule is a list.
#
# ============================ SCOPE, AND WHAT IT IS NOT ======================
# This is a DOCUMENTATION-COMPLETENESS guard. It verifies that a disposition has
# been RECORDED and LABELED. It does NOT verify that the recorded disposition is
# TRUE, and it makes no such claim anywhere.
#
# An earlier version did try to verify truth, by deriving workspace dependents from
# `cargo tree --workspace --invert` and cross-checking them. That was REMOVED, and
# the reason is worth recording because it is the more useful lesson:
#
#   * Eleven review findings over eight rounds landed in that machinery and NONE in
#     the list-and-README part below. The derivation was where all the complexity
#     and all the defects lived.
#   * It required cargo inside a component documented as fast and network-free, and
#     its self-tests built scratch workspaces OUTSIDE the repository — which do not
#     inherit rust-toolchain.toml, so a MANDATORY gate component's behaviour became
#     dependent on the host's default toolchain (roborev job 86). A flaky mandatory
#     gate is a fleet-wide liability, and that is a bad trade for a P3 labeling task.
#   * The property it verified is genuinely valuable, but verifying it properly is a
#     larger mechanism than this issue warrants. It belongs to epic #1688 as its own
#     issue, not as a rider here.
#
# Two consequences, stated rather than left to be discovered:
#   * A MIXED crate's "live half" claim is DOCUMENTED, not verified. The README must
#     carry the label; nothing here proves which half is live.
#   * Granularity is per-CRATE. Adding an orphaned binary to an already-WIRED crate
#     needs no census update and passes unchanged.
# Both are accepted limitations of a small guard, not oversights. A guard that
# overpromises is worse than a small one.
#
# Deliberately NOT grep-inferred: wiredness is RECORDED by a human and reviewed in
# the diff, never guessed from invocations. A grep for those gets both directions
# wrong — tools/format-validator is referenced twice under scripts/ purely as a PATH
# FIXTURE that runs nothing, and tools/ws0-corpus-gen is wired via a binary name
# that differs from its package name. A guard whose FAIL an agent learns to waive is
# worse than no guard (CLAUDE.md).
#
# Needs NO cargo, NO python3, NO Docker and NO network: filesystem and lists only,
# so it always runs and cannot be environment-dependent.
# FAILS CLOSED: an unclassifiable or unmeasurable tree is a FAIL, never a pass.
# Self-test / negative controls: scripts/tests/test_tools_crate_disposition_selftest.sh
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# Resolved from this script's OWN location, with no env override: an override is
# settable by the party the guard constrains (CLAUDE.md #3312 — "the constrained
# party must not choose its own enforcer"). A self-test needing a different tree
# copies this script into that tree; it never redirects this variable.
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

# --- The recorded disposition. Hard-coded ON PURPOSE: one visible location, inside
# --- the diff a reviewer already reads. Moving a crate between these lists is a
# --- deliberate, reviewable act.

# Invoked by at least one CI workflow or script. No README required by this guard.
# NOTE the crate-level granularity (see SCOPE above): a crate here may later gain an
# orphaned binary without tripping anything. If you know that has happened, move it
# to MIXED_TOOLS and label it — the guard will not tell you to.
WIRED_TOOLS="cassandra-parity
flight-loadgen
sstabledump-validator
ws0-corpus-gen"

# Nothing runs these AND nothing depends on them (census: issue #1716). Each MUST
# carry a README.md containing LABEL_MARKER.
UNWIRED_TOOLS="cqlite-validator
memory-safety-runner"

# Some targets live, some orphaned. Each MUST carry a README.md containing
# LABEL_MARKER, and that README is where the split is documented.
#   format-validator: its 4 binaries are orphaned; its LIBRARY is a path dependency
#   of tests/format-compatibility (the gate's `format-compat` component), and it is
#   also used as a fixture by scripts/tests/test_agent_gate_summary.sh (owners
#   resolution) and asserted on by xtask/src/oom_audit/scope.rs. So it must stay a
#   workspace MEMBER — never `exclude` it, only its bins are dead.
MIXED_TOOLS="format-validator"

# The marker every README with orphaned targets must contain, so the label states
# the actual fact rather than merely existing as a file.
LABEL_MARKER="NOT CI-wired"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok: $*"; }

[ -d "$ROOT/tools" ] || fail "no tools/ directory under $ROOT — this guard's subject is missing; refusing to pass vacuously"

# `grep -c` exits 1 for a count of ZERO while still PRINTING "0", so its status is
# deliberately not used as a signal — the printed count is the datum, and the floors
# below are what reject zero. count_lines guards only the thing that would otherwise
# slip through: a value that is not a number at all, which would reach
# `[ "$n" -gt 0 ]` as a bash syntax error rather than a named cause.
#
# RETURNS non-zero rather than calling fail(): fail() ends in `exit`, and an `exit`
# inside a command substitution leaves only the SUBSHELL — the parent would carry on
# with an empty value. So the status is checked at each call site with `|| fail`.
count_lines() {
  local n
  n=$(printf '%s\n' "$1" | grep -c .)
  case "$n" in
    ''|*[!0-9]*) return 1 ;;
  esac
  printf '%s\n' "$n"
}

wired_sorted=$(printf '%s\n' "$WIRED_TOOLS" | grep . | sort -u)
unwired_sorted=$(printf '%s\n' "$UNWIRED_TOOLS" | grep . | sort -u)
mixed_sorted=$(printf '%s\n' "$MIXED_TOOLS" | grep . | sort -u)
n_wired=$(count_lines "$wired_sorted")     || fail "cannot count WIRED_TOOLS — unmeasurable is not a pass"
n_unwired=$(count_lines "$unwired_sorted") || fail "cannot count UNWIRED_TOOLS — unmeasurable is not a pass"
n_mixed=$(count_lines "$mixed_sorted")     || fail "cannot count MIXED_TOOLS — unmeasurable is not a pass"

# Crates that carry orphaned targets: UNWIRED (wholly) + MIXED (partly). Both owe a
# README label.
labelled_sorted=$(printf '%s\n%s\n' "$unwired_sorted" "$mixed_sorted" | grep . | sort -u)
n_labelled=$(count_lines "$labelled_sorted") || fail "cannot count the label-bearing crate set — unmeasurable is not a pass"

# --- affirmative-measurement floor: the label-bearing set must be non-empty, or the
# --- label loop below would enforce nothing while still reporting PASS.
[ "$n_labelled" -gt 0 ] || fail "UNWIRED_TOOLS and MIXED_TOOLS are both empty — this guard would then enforce no label at all; refusing to pass vacuously"
[ "$n_wired" -gt 0 ]    || fail "the recorded WIRED_TOOLS list is empty — refusing to pass vacuously"

# --- 1. the three lists must be pairwise DISJOINT. A crate in two of them would
# ---    satisfy the accounted-for loop below while making its label requirement
# ---    ambiguous.
#
# `comm`'s exit status is CHECKED, not discarded. An unchecked `both=$(comm ...)`
# yields an EMPTY string when comm FAILS, and "empty intersection" is exactly this
# check's PASS condition — so a comm failure would silently certify the lists as
# disjoint. Never let a permissive verdict rest on the absence of output that a
# failure also produces.
check_disjoint() {
  local a="$1" b="$2" name_a="$3" name_b="$4" both rc
  both=$(comm -12 <(printf '%s\n' "$a") <(printf '%s\n' "$b"))
  rc=$?
  [ "$rc" -eq 0 ] || fail "cannot compare $name_a against $name_b (comm exited $rc) — an unmeasurable comparison is not a pass; a comm failure and an EMPTY intersection both produce no output, so this status must be checked"
  [ -z "$both" ] || fail "crate(s) recorded in BOTH $name_a and $name_b: $(printf '%s ' $both)- a crate has exactly ONE disposition"
}
check_disjoint "$wired_sorted"   "$unwired_sorted" WIRED_TOOLS   UNWIRED_TOOLS
check_disjoint "$wired_sorted"   "$mixed_sorted"   WIRED_TOOLS   MIXED_TOOLS
check_disjoint "$unwired_sorted" "$mixed_sorted"   UNWIRED_TOOLS MIXED_TOOLS
ok "recorded disposition is self-consistent ($n_wired wired, $n_unwired unwired, $n_mixed mixed; pairwise disjoint)"

# --- 2. every crate ON DISK under tools/ must appear in exactly one list. This is
# ---    the half that catches a NEW crate arriving via the `tools/*` members glob
# ---    with no statement of whether anything runs it. It reads the FILESYSTEM, so
# ---    a manifest glob that silently failed to match cannot hide one.
n_disk=0
for manifest in "$ROOT"/tools/*/Cargo.toml; do
  [ -f "$manifest" ] || continue
  n_disk=$((n_disk + 1))
  crate=$(basename "$(dirname "$manifest")")
  if printf '%s\n' "$wired_sorted"   | grep -qxF "$crate"; then continue; fi
  if printf '%s\n' "$unwired_sorted" | grep -qxF "$crate"; then continue; fi
  if printf '%s\n' "$mixed_sorted"   | grep -qxF "$crate"; then continue; fi
  fail "tools/$crate exists on disk but is in NONE of the three recorded lists (issue #1716). Classify it in $SCRIPT_DIR/$(basename "$0"): WIRED_TOOLS if a CI workflow or script runs it, UNWIRED_TOOLS if nothing runs it and nothing depends on it, or MIXED_TOOLS if only SOME of its targets are live. The latter two also need a README.md labeling it '$LABEL_MARKER'."
done
[ "$n_disk" -gt 0 ] || fail "found no tools/*/Cargo.toml on disk — the filesystem half of this guard measured nothing; refusing to pass vacuously"
[ "$n_disk" -eq $((n_wired + n_unwired + n_mixed)) ] || fail "the recorded lists name $((n_wired + n_unwired + n_mixed)) crate(s) but $n_disk exist on disk — a recorded crate was renamed or removed without updating $(basename "$0") (issue #1716)"
ok "all $n_disk tools/ crate(s) on disk are classified by the recorded disposition"

# --- 3. every recorded crate must actually EXIST. Without this, deleting a crate
# ---    while leaving it listed would keep the count check above satisfiable by a
# ---    compensating addition.
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  [ -f "$ROOT/tools/$crate/Cargo.toml" ] \
    || fail "tools/$crate is recorded in $(basename "$0") but has no Cargo.toml on disk — remove it from the list if the crate is gone (issue #1716)"
done <<< "$wired_sorted
$unwired_sorted
$mixed_sorted"
ok "every recorded crate exists on disk"

# --- 4. the actual acceptance criterion of #1716: every crate carrying orphaned
# ---    targets carries a README that SAYS so. A present-but-silent README does not
# ---    label anything, so the marker is required, not just the file.
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  readme="$ROOT/tools/$crate/README.md"
  [ -f "$readme" ] \
    || fail "tools/$crate carries orphaned targets but has no README.md — issue #1716 requires it be LABELED (what it is, that its unwired parts are not CI-wired, how to build/run it)"
  [ -r "$readme" ] \
    || fail "tools/$crate/README.md is not readable — cannot verify its label; unmeasurable is not a pass"
  grep -qF "$LABEL_MARKER" "$readme" \
    || fail "tools/$crate/README.md exists but does not contain '$LABEL_MARKER' — the README must STATE that no CI workflow or script invokes it (or its orphaned targets), not merely exist (issue #1716)"
done <<< "$labelled_sorted"
ok "all $n_labelled crate(s) with orphaned targets carry a README.md stating '$LABEL_MARKER'"

echo "PASS: tools/ crate disposition census (#1716)"
