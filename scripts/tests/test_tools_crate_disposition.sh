#!/usr/bin/env bash
# tools/ crate disposition census (issue #1716, epic #1688 finding AK5).
#
# ONE property: every crate under tools/ is EXPLICITLY classified into one of
# THREE dispositions, and every crate carrying unwired targets carries the README
# label that says so:
#
#   WIRED   — something runs it; nothing in it is orphaned.
#   UNWIRED — nothing runs it, and nothing depends on it either. Needs a label.
#   MIXED   — some targets are live and others are orphaned. Needs a label that
#             states BOTH halves, because calling such a crate simply "unwired"
#             is a FALSE census and invites deleting the live half.
#
# The three-way split exists because a two-way one was wrong (roborev job 75 on
# #1716): tools/format-validator's four BINARIES are invoked by nothing, while its
# LIBRARY is a path dependency of tests/format-compatibility — the gate's own
# `format-compat` component. Filing that crate under a crate-level "unwired" made
# a mixed-status crate satisfy a mutually-exclusive classification, so the census
# asserted something untrue about it.
#
# Why. Finding AK5 was that three tools/ crates were invoked by no workflow, no
# script and no doc, and nothing said so — so they read as live tooling for
# months. #1716 labeled them. Without this guard that labeling silently decays:
# the root manifest's `members` globs `tools/*`, so a NEW crate joins the
# workspace with no statement of whether anything runs it, and a deleted README
# is invisible. Both are caught here, at zero cost, with no false positives:
# the rule is a list, not an inference.
#
# Deliberately NOT what this guard does: it does not try to DERIVE wiredness by
# grepping for invocations. A grep is a proxy that gets both directions wrong —
# tools/format-validator is referenced twice under scripts/ purely as a PATH
# FIXTURE (neither reference runs it), and a crate can be wired via a binary
# name that differs from its package name (tools/ws0-corpus-gen ships
# `ws0-scan-bench`). A guard whose FAIL an agent learns to waive is worse than
# no guard (CLAUDE.md), so wiredness is RECORDED here by a human and reviewed in
# the diff, never guessed.
#
# FAILS CLOSED: an unclassifiable or unmeasurable tree is a FAIL, never a pass.
# Self-test / negative controls: scripts/tests/test_tools_crate_disposition_selftest.sh
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# Resolved from this script's OWN location, with no env override: an override is
# settable by the party the guard constrains (CLAUDE.md #3312 — "the constrained
# party must not choose its own enforcer"). A self-test needing a different tree
# copies this script into that tree; it never redirects this variable.
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

# --- The recorded disposition. Hard-coded ON PURPOSE: one visible location,
# --- inside the diff a reviewer already reads. Moving a crate between these
# --- lists is a deliberate, reviewable act.

# Invoked by at least one CI workflow or script. No README required by this guard.
WIRED_TOOLS="cassandra-parity
flight-loadgen
sstabledump-validator
ws0-corpus-gen"

# Nothing runs these AND nothing depends on them (census: issue #1716). Each MUST
# carry a README.md containing LABEL_MARKER.
UNWIRED_TOOLS="cqlite-validator
memory-safety-runner"

# Some targets live, some orphaned. Each MUST carry a README.md containing BOTH
# LABEL_MARKER (for the orphaned targets) and MIXED_WIRED_MARKER (naming the live
# ones), so the label cannot describe the crate as wholly unwired.
#   format-validator: 4 binaries orphaned; LIBRARY wired into
#   tests/format-compatibility (gate component `format-compat`), and also used as
#   a fixture by scripts/tests/test_agent_gate_summary.sh (owners resolution) and
#   asserted on by xtask/src/oom_audit/scope.rs. NEVER workspace-`exclude` it.
MIXED_TOOLS="format-validator"

# The marker every README with orphaned targets must contain, so the label states
# the actual fact rather than merely existing as a file.
LABEL_MARKER="NOT CI-wired"
# Additionally required of a MIXED crate's README: it must name the LIVE half, so
# a partly-live crate can never be labeled as though it were wholly dead.
MIXED_WIRED_MARKER="WIRED"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok: $*"; }

[ -d "$ROOT/tools" ] || fail "no tools/ directory under $ROOT — this guard's subject is missing; refusing to pass vacuously"

wired_sorted=$(printf '%s\n' "$WIRED_TOOLS" | grep . | sort -u)
unwired_sorted=$(printf '%s\n' "$UNWIRED_TOOLS" | grep . | sort -u)
mixed_sorted=$(printf '%s\n' "$MIXED_TOOLS" | grep . | sort -u)
n_wired=$(printf '%s\n' "$wired_sorted" | grep -c .)
n_unwired=$(printf '%s\n' "$unwired_sorted" | grep -c .)
n_mixed=$(printf '%s\n' "$mixed_sorted" | grep -c .)
# Crates that carry orphaned targets: UNWIRED (wholly) + MIXED (partly). Both owe
# a label; only MIXED additionally owes a statement of its live half.
labelled_sorted=$(printf '%s\n%s\n' "$unwired_sorted" "$mixed_sorted" | grep . | sort -u)
n_labelled=$(printf '%s\n' "$labelled_sorted" | grep -c .)

# --- affirmative-measurement floor: the label-bearing set must be non-empty, or
# --- the label loop below would enforce nothing while still reporting PASS.
[ "$n_labelled" -gt 0 ] || fail "UNWIRED_TOOLS and MIXED_TOOLS are both empty — this guard would then enforce no label at all; refusing to pass vacuously"
[ "$n_wired" -gt 0 ]    || fail "the recorded WIRED_TOOLS list is empty — refusing to pass vacuously"

# --- 1. the three lists must be pairwise DISJOINT. A crate in two of them would
# ---    satisfy the accounted-for loop below while making its label requirement
# ---    ambiguous — which is the exact defect the third category exists to fix
# ---    (roborev job 75), so it is asserted rather than assumed.
check_disjoint() {
  local a="$1" b="$2" name_a="$3" name_b="$4" both
  both=$(comm -12 <(printf '%s\n' "$a") <(printf '%s\n' "$b"))
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
# ---    targets carries a README that SAYS so. A present-but-silent README does
# ---    not label anything, so the marker is required, not just the file.
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

# --- 5. a MIXED crate's README must ALSO name its LIVE half. Without this, a
# ---    mixed crate's label is indistinguishable from a wholly-unwired one — the
# ---    false census roborev job 75 caught — and a future reader could delete or
# ---    workspace-`exclude` a dependency the gate needs.
if [ "$n_mixed" -gt 0 ]; then
  while IFS= read -r crate; do
    [ -n "$crate" ] || continue
    readme="$ROOT/tools/$crate/README.md"
    grep -qF "$MIXED_WIRED_MARKER" "$readme" \
      || fail "tools/$crate is recorded MIXED (some targets live, some orphaned) but its README.md never says '$MIXED_WIRED_MARKER' — a mixed crate labeled only '$LABEL_MARKER' reads as wholly dead, which invites deleting or workspace-excluding the half the gate depends on (issue #1716)"
  done <<< "$mixed_sorted"
  ok "all $n_mixed mixed crate(s) name their live half ('$MIXED_WIRED_MARKER') in the README"
fi

echo "PASS: tools/ crate disposition census (#1716)"
