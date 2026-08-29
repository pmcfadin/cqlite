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
# TWO HALVES, and the split matters — one is DERIVED, the other RECORDED:
#
#   "something DEPENDS on it" is MECHANICALLY CHECKABLE from the manifests, so it
#   is DERIVED (via `cargo tree --workspace --invert`, cargo as its own authority)
#   and CROSS-CHECKED against the recorded disposition. UNWIRED must have ZERO
#   workspace dependents and MIXED must have AT LEAST ONE — so neither category
#   can be asserted falsely, in either direction.
#
#   "something RUNS it" is NOT mechanically checkable — invocations live in
#   workflows, scripts and prose — so it is RECORDED by a human and reviewed in
#   the diff. It is deliberately NOT grep-inferred: a grep for invocations gets
#   both directions wrong (tools/format-validator is referenced twice under
#   scripts/ purely as a PATH FIXTURE that runs nothing, and tools/ws0-corpus-gen
#   is wired via a binary name that differs from its package name), and a guard
#   whose FAIL an agent learns to waive is worse than no guard (CLAUDE.md).
#
# This split is the fix for roborev job 78: the MIXED label had been verified by
# grepping the README for the generic word "WIRED", which a README could satisfy
# while saying the opposite ("previously WIRED, now entirely NOT CI-wired").
# Verifying a claim by pattern-matching prose is unbounded — the prose author
# chooses the wording — so the load-bearing check moved OFF the prose and onto the
# manifests, and the prose requirement is now to name the crate's ACTUAL,
# DERIVED dependents rather than any fixed word.
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
# LABEL_MARKER (for the orphaned targets) and the names of its DERIVED dependents (the live
# ones, named below from the DERIVED dependent set), so the label cannot describe
# the crate as wholly unwired.
#   format-validator: 4 binaries orphaned; LIBRARY wired into
#   tests/format-compatibility (gate component `format-compat`), and also used as
#   a fixture by scripts/tests/test_agent_gate_summary.sh (owners resolution) and
#   asserted on by xtask/src/oom_audit/scope.rs. NEVER workspace-`exclude` it.
MIXED_TOOLS="format-validator"

# The marker every README with orphaned targets must contain, so the label states
# the actual fact rather than merely existing as a file.
LABEL_MARKER="NOT CI-wired"
# A MIXED crate's README must additionally name each of its ACTUAL workspace
# dependents — the package names are DERIVED below, never hard-coded here, so this
# requirement cannot be satisfied by a generic word (roborev job 78).

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok: $*"; }

# The PACKAGE name declared by tools/<dir>/Cargo.toml. The recorded lists and the
# README live under the DIRECTORY name, but cargo is queried by PACKAGE name, and
# the two are not the same thing — assuming they were made every scratch-tree
# self-test case report "cargo could not answer". All seven of this repo's tools/
# crates happen to match, which is exactly why the assumption would have stayed
# invisible here until someone added one that did not.
#
# Takes the FIRST `name =` line in the manifest: that is the `[package]` one, since
# `[package]` precedes any `[lib]`/`[[bin]]` table that also carries a `name`
# (tools/cassandra-parity has both). Fails closed if none is found.
package_name_of() {
  local dir="$1" name
  name=$(sed -n 's/^name[[:space:]]*=[[:space:]]*"\([^"]\{1,\}\)".*/\1/p' "$ROOT/tools/$dir/Cargo.toml" 2>/dev/null | head -1)
  [ -n "$name" ] || return 1
  printf '%s\n' "$name"
}

# Workspace packages that DIRECTLY depend on $1 (a PACKAGE name), derived from cargo's own
# resolution — never a hand-parse of Cargo.toml, and never a grep for prose.
#
# `--workspace` is REQUIRED: without it `cargo tree` operates on the DEFAULT
# member set, which in this workspace is the root package alone (see the
# `default-members` note in the root Cargo.toml), and every crate would appear to
# have zero dependents — a silent false negative that would let a MIXED crate be
# recorded UNWIRED. Measured: `cargo tree --invert format-validator` reports no
# dependents; adding `--workspace` reports format-compatibility-tests.
#
# Direct dependents suffice: if nothing depends on a crate directly, nothing
# depends on it transitively either.
#
# Prints one package name per line (the subject itself excluded). Returns
# non-zero if cargo could not answer, so the caller can fail CLOSED.
# NOTE the deliberate separation of the two failure modes below. Under
# `pipefail`, a `grep -v` that matches nothing exits 1, so folding the cargo call
# and the filtering into ONE pipeline made "cargo could not answer" and "the crate
# has ZERO dependents" return the SAME status — and zero dependents is the NORMAL,
# EXPECTED case for an UNWIRED crate. That is the two-valued-predicate trap: a
# probe that cannot distinguish "no result" from "could not measure" must collapse
# them, and it collapsed the legitimate case onto the error branch (every UNWIRED
# crate reported "unmeasurable"). So cargo's exit status is captured ALONE, and the
# filtering — whose emptiness is meaningful, not an error — is `|| true`.
workspace_dependents() {
  local crate="$1" out rc
  out=$(cd "$ROOT" && cargo tree --workspace --invert "$crate" --depth 1 2>/dev/null)
  rc=$?
  [ "$rc" -eq 0 ] || return 1
  # cargo always echoes the subject itself as the tree root, so empty output here
  # means cargo answered nothing at all — a measurement failure, not "no deps".
  [ -n "$out" ] || return 1
  printf '%s\n' "$out" \
    | sed -n 's/^[^A-Za-z0-9_-]*\([A-Za-z0-9_-]\{1,\}\) v[0-9].*/\1/p' \
    | { grep -vxF "$crate" || true; } \
    | sort -u
  return 0
}

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

# --- 5. CROSS-CHECK the recorded disposition against the MANIFESTS. This is the
# ---    half that cannot be talked around: "something depends on it" is derived
# ---    from cargo's own resolution, so UNWIRED cannot be claimed for a crate the
# ---    workspace depends on, and MIXED cannot be claimed for one it does not.
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  pkg=$(package_name_of "$crate") \
    || fail "cannot read the package name from tools/$crate/Cargo.toml — unmeasurable is not a pass"
  deps=$(workspace_dependents "$pkg") \
    || fail "cannot derive workspace dependents of tools/$crate (package '$pkg'; cargo tree --workspace --invert failed) — unmeasurable is not a pass"
  if [ -n "$deps" ]; then
    fail "tools/$crate is recorded UNWIRED (nothing runs it AND nothing depends on it) but these workspace package(s) DEPEND on it: $(printf '%s ' $deps)- that is a FALSE census. Move it to MIXED_TOOLS (and label its README accordingly), because deleting or workspace-excluding it would break them (issue #1716)."
  fi
done <<< "$unwired_sorted"
ok "every UNWIRED crate has zero workspace dependents (derived from cargo, not asserted)"

if [ "$n_mixed" -gt 0 ]; then
  while IFS= read -r crate; do
    [ -n "$crate" ] || continue
    readme="$ROOT/tools/$crate/README.md"
    pkg=$(package_name_of "$crate") \
      || fail "cannot read the package name from tools/$crate/Cargo.toml — unmeasurable is not a pass"
    deps=$(workspace_dependents "$pkg") \
      || fail "cannot derive workspace dependents of tools/$crate (package '$pkg'; cargo tree --workspace --invert failed) — unmeasurable is not a pass"
    [ -n "$deps" ] \
      || fail "tools/$crate is recorded MIXED (some targets live, some orphaned) but NO workspace package depends on it — the 'live half' claim is unsupported. If nothing runs it either, it belongs in UNWIRED_TOOLS (issue #1716)."
    # --- and its README must NAME each real dependent. The required strings are
    # --- DERIVED above, so this cannot be satisfied by a generic word like
    # --- "WIRED" that a README could carry while saying the opposite
    # --- ("previously WIRED, now entirely NOT CI-wired") — roborev job 78.
    while IFS= read -r dep; do
      [ -n "$dep" ] || continue
      grep -qF "$dep" "$readme" \
        || fail "tools/$crate is recorded MIXED and '$dep' really does depend on it, but its README.md never mentions '$dep'. A mixed crate's label must NAME the live half — otherwise it reads as wholly dead and invites deleting or workspace-excluding the part '$dep' needs (issue #1716)."
    done <<< "$deps"
    ok "tools/$crate (MIXED) names its real dependent(s) in the README: $(printf '%s ' $deps)"
  done <<< "$mixed_sorted"
fi

echo "PASS: tools/ crate disposition census (#1716)"
