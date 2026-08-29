#!/usr/bin/env bash
# tools/ crate disposition census (issue #1716, epic #1688 finding AK5).
#
# ONE property: every crate under tools/ is EXPLICITLY classified as either
# CI-wired or an unwired manual dev tool, and every unwired one carries the
# README label that says so.
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
# --- inside the diff a reviewer already reads. Moving a crate between these two
# --- lists is a deliberate, reviewable act.

# Invoked by at least one CI workflow or script. No README required by this guard.
WIRED_TOOLS="cassandra-parity
flight-loadgen
sstabledump-validator
ws0-corpus-gen"

# Invoked by NO workflow, NO script and NO live doc (census: issue #1716).
# Each MUST carry a README.md carrying the label marker below.
UNWIRED_TOOLS="cqlite-validator
format-validator
memory-safety-runner"

# The marker every unwired tool's README must contain, so the label states the
# actual fact rather than merely existing as a file.
LABEL_MARKER="NOT CI-wired"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok: $*"; }

[ -d "$ROOT/tools" ] || fail "no tools/ directory under $ROOT — this guard's subject is missing; refusing to pass vacuously"

wired_sorted=$(printf '%s\n' "$WIRED_TOOLS" | grep . | sort -u)
unwired_sorted=$(printf '%s\n' "$UNWIRED_TOOLS" | grep . | sort -u)
n_wired=$(printf '%s\n' "$wired_sorted" | grep -c .)
n_unwired=$(printf '%s\n' "$unwired_sorted" | grep -c .)

# --- affirmative-measurement floor: both recorded lists must be non-empty, or a
# --- later "every crate is accounted for" loop could be satisfied by an empty
# --- subject set.
[ "$n_unwired" -gt 0 ] || fail "the recorded UNWIRED_TOOLS list is empty — this guard would then enforce no label at all; refusing to pass vacuously"
[ "$n_wired" -gt 0 ]   || fail "the recorded WIRED_TOOLS list is empty — refusing to pass vacuously"

# --- 1. the two lists must be disjoint. A crate in both would satisfy the
# ---    accounted-for loop below while making its label requirement ambiguous.
both=$(comm -12 <(printf '%s\n' "$wired_sorted") <(printf '%s\n' "$unwired_sorted"))
[ -z "$both" ] || fail "crate(s) recorded as BOTH wired and unwired: $(printf '%s ' $both)- a crate has exactly one disposition"
ok "recorded disposition is self-consistent ($n_wired wired, $n_unwired unwired, disjoint)"

# --- 2. every crate ON DISK under tools/ must appear in exactly one list. This
# ---    is the half that catches a NEW crate arriving via the `tools/*` members
# ---    glob with no statement of whether anything runs it. It reads the
# ---    FILESYSTEM, so a manifest glob that silently failed to match cannot hide
# ---    one.
n_disk=0
for manifest in "$ROOT"/tools/*/Cargo.toml; do
  [ -f "$manifest" ] || continue
  n_disk=$((n_disk + 1))
  crate=$(basename "$(dirname "$manifest")")
  if printf '%s\n' "$wired_sorted"   | grep -qxF "$crate"; then continue; fi
  if printf '%s\n' "$unwired_sorted" | grep -qxF "$crate"; then continue; fi
  fail "tools/$crate exists on disk but is in NEITHER recorded list (issue #1716). Classify it in $SCRIPT_DIR/$(basename "$0"): add it to WIRED_TOOLS if a CI workflow or script invokes it, or to UNWIRED_TOOLS plus a README.md labeling it '$LABEL_MARKER' if it is a manual dev tool."
done
[ "$n_disk" -gt 0 ] || fail "found no tools/*/Cargo.toml on disk — the filesystem half of this guard measured nothing; refusing to pass vacuously"
[ "$n_disk" -eq $((n_wired + n_unwired)) ] || fail "the recorded lists name $((n_wired + n_unwired)) crate(s) but $n_disk exist on disk — a recorded crate was renamed or removed without updating $(basename "$0") (issue #1716)"
ok "all $n_disk tools/ crate(s) on disk are classified by the recorded disposition"

# --- 3. every recorded crate must actually EXIST. Without this, deleting a crate
# ---    while leaving it listed would keep the count check above satisfiable by a
# ---    compensating addition.
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  [ -f "$ROOT/tools/$crate/Cargo.toml" ] \
    || fail "tools/$crate is recorded in $(basename "$0") but has no Cargo.toml on disk — remove it from the list if the crate is gone (issue #1716)"
done <<< "$wired_sorted
$unwired_sorted"
ok "every recorded crate exists on disk"

# --- 4. the actual acceptance criterion of #1716: each unwired crate carries a
# ---    README that SAYS it is not CI-wired. A present-but-silent README does
# ---    not label anything, so the marker is required, not just the file.
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  readme="$ROOT/tools/$crate/README.md"
  [ -f "$readme" ] \
    || fail "tools/$crate is recorded as unwired but has no README.md — issue #1716 requires each unwired tool be LABELED (what it is, that it is not CI-wired, how to build/run it)"
  [ -r "$readme" ] \
    || fail "tools/$crate/README.md is not readable — cannot verify its label; unmeasurable is not a pass"
  grep -qF "$LABEL_MARKER" "$readme" \
    || fail "tools/$crate/README.md exists but does not contain the label '$LABEL_MARKER' — the README must STATE that no CI workflow or script invokes this crate, not merely exist (issue #1716)"
done <<< "$unwired_sorted"
ok "all $n_unwired unwired crate(s) carry a README.md stating '$LABEL_MARKER'"

echo "PASS: tools/ crate disposition census (#1716)"
