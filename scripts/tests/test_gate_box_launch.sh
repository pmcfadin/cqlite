#!/usr/bin/env bash
# test_gate_box_launch.sh — self-test for scripts/flow/gate-box-launch.sh (issue #4267).
#
# #4267's acceptance criterion 1 names three things this suite must cover:
#   (a) env scrubbing: a LANE_ID in the calling environment, and an npx-bearing PATH in
#       the box profile, are both rejected;
#   (b) stale-base refusal: a head whose ancestry does not include current origin/main
#       is refused, mirroring CLAUDE.md's "a gate script behind origin/main cannot
#       certify" rule;
#   (c) profile loading: a missing profile and a profile missing a required variable are
#       both refused by name, and a complete profile resolves and loads.
#
# Everything here runs under --dry-run against a throwaway bare "origin" + a throwaway
# "canonical clone" this script creates and destroys itself — no network, no real box, no
# systemd, no cargo. Two positive controls (a rebased branch, a complete profile) prove
# the harness can pass at all, since a suite that only ever refuses cannot tell "correctly
# rejected" from "cannot succeed at anything" (the case-floor lesson, #3544).
#
# Run standalone:   bash scripts/tests/test_gate_box_launch.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && cd .. && pwd)"
LAUNCHER="$REPO_ROOT/scripts/flow/gate-box-launch.sh"
BOXES_DIR="$REPO_ROOT/scripts/flow/boxes"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

[ -x "$LAUNCHER" ] || bad "gate-box-launch.sh must be executable ($LAUNCHER)"
[ -f "$LAUNCHER" ] || { echo "test_gate_box_launch: cannot find $LAUNCHER" >&2; exit 1; }

gg() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/gate-box-launch-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

ORIGIN="$T/origin.git"
SEED="$T/seed"
CLONE="$T/canonical"
LANES="$T/lanes"
TMPDIR_FIXTURE="$T/tmp"
DATASETS="$T/datasets"
LOGDIR="$T/log"

gg init --bare -q "$ORIGIN"
gg clone -q "$ORIGIN" "$SEED" 2>/dev/null
( cd "$SEED" && echo one >f.txt && gg add f.txt && gg commit -qm seed >/dev/null && gg push -q -u origin main )
gg clone -q "$ORIGIN" "$CLONE" 2>/dev/null

mkdir -p "$LANES" "$TMPDIR_FIXTURE" "$DATASETS" "$LOGDIR"

# A profile that resolves and loads cleanly, used by every case that is not itself
# testing profile loading. BOX_MIN_FREE_GB=0 so the (fixture) tmpfs's real free space
# never fails an unrelated case.
_write_good_profile() {  # <name>
  cat >"$BOXES_DIR/$1.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="/usr/bin:/bin"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
}

# Every fixture profile is written under a name this suite owns and removes on exit —
# never edits a real box profile such as astro-processor.env.
FIXTURE_BOXES=()
_cleanup_boxes() {
  local b
  for b in "${FIXTURE_BOXES[@]}"; do
    rm -f "$BOXES_DIR/$b.env"
  done
}
trap '_cleanup_boxes; rm -rf "$T"' EXIT

_new_box() {  # <name> -> writes a good profile under scripts/flow/boxes/<name>.env
  FIXTURE_BOXES+=("$1")
  _write_good_profile "$1"
}

# ---------------------------------------------------------------------------------
# (c) profile loading
# ---------------------------------------------------------------------------------
_out=$(bash "$LAUNCHER" main --box nosuchtestbox4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "no committed profile at"; then
  ok "missing profile is refused by name"
else
  bad "missing profile should be refused (rc=$_rc): $_out"
fi

FIXTURE_BOXES+=("incomplete4267")
cat >"$BOXES_DIR/incomplete4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
EOF
_out=$(bash "$LAUNCHER" main --box incomplete4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "does not set required variable"; then
  ok "profile missing a required variable is refused by name"
else
  bad "incomplete profile should be refused (rc=$_rc): $_out"
fi

_new_box good4267
_out=$(bash "$LAUNCHER" main --box good4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 0 ] && printf '%s' "$_out" | grep -q "^GATE-BOX-LAUNCH: dry-run "; then
  ok "a complete profile loads and resolves cleanly (positive control)"
else
  bad "a complete, correct profile should dry-run clean (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# (a) env scrubbing — LANE_ID
# ---------------------------------------------------------------------------------
_out=$(LANE_ID=peer-lane bash "$LAUNCHER" main --box good4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "LANE_ID='peer-lane' is set"; then
  ok "an inherited LANE_ID is refused, naming the value"
else
  bad "LANE_ID in the calling environment should be refused (rc=$_rc): $_out"
fi
# A LANE_ID that is exported but EMPTY is still "set" (env -u is the only way to remove
# it) — the check must not treat empty-string as absent.
_out=$(LANE_ID= bash "$LAUNCHER" main --box good4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "LANE_ID="; then
  ok "an inherited empty-but-set LANE_ID is still refused"
else
  bad "empty LANE_ID should still be refused (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# (a) env scrubbing — npx on PATH
# ---------------------------------------------------------------------------------
NPXBIN="$T/npxbin"
mkdir -p "$NPXBIN"
cat >"$NPXBIN/npx" <<'EOF'
#!/bin/sh
echo fake-npx
EOF
chmod +x "$NPXBIN/npx"
FIXTURE_BOXES+=("npxbox4267")
cat >"$BOXES_DIR/npxbox4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="$NPXBIN:/usr/bin:/bin"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(bash "$LAUNCHER" main --box npxbox4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "'npx' resolves on the profile's PATH"; then
  ok "an npx-bearing PATH is refused, naming the resolved path"
else
  bad "npx on PATH should be refused (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# (b) stale-base refusal
# ---------------------------------------------------------------------------------
( cd "$CLONE" && gg checkout -q -b feat4267 && echo two >g.txt && gg add g.txt \
    && gg commit -qm feat >/dev/null && gg push -q -u origin feat4267 )
# advance origin/main so feat4267's merge-base is now BEHIND the tip
( cd "$SEED" && echo three >h.txt && gg add h.txt && gg commit -qm advance >/dev/null && gg push -q origin main )

_out=$(bash "$LAUNCHER" feat4267 --box good4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "does not have current" \
   && printf '%s' "$_out" | grep -qi "rebase before the gate of record"; then
  ok "a branch behind origin/main is refused (stale base), naming the CLAUDE.md rule"
else
  bad "a stale-based branch should be refused (rc=$_rc): $_out"
fi

# Positive control: rebase the branch onto the new main tip, and it must now pass.
( cd "$CLONE" && gg fetch -q origin && gg checkout -q feat4267 && gg rebase -q origin/main \
    && gg push -q -f origin feat4267 )
_out=$(bash "$LAUNCHER" feat4267 --box good4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 0 ] && printf '%s' "$_out" | grep -q "a descendant of origin/main"; then
  ok "a branch rebased onto current origin/main is accepted (positive control)"
else
  bad "a freshly rebased branch should dry-run clean (rc=$_rc): $_out"
fi

# A PR-number argument goes through the identical ancestry check (issue #4267 says the
# launcher takes a PR number OR a branch) — simulate a GitHub PR ref locally.
FEAT_SHA=$( (cd "$CLONE" && gg rev-parse origin/feat4267) )
( cd "$ORIGIN" && gg update-ref refs/pull/99/head "$FEAT_SHA" )
_out=$(bash "$LAUNCHER" 99 --box good4267 --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 0 ] && printf '%s' "$_out" | grep -q "^GATE-BOX-LAUNCH: dry-run "; then
  ok "a PR-number argument resolves refs/pull/<n>/head through the same ancestry check"
else
  bad "PR-number resolution should dry-run clean (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# usage errors
# ---------------------------------------------------------------------------------
_out=$(bash "$LAUNCHER" 2>&1); _rc=$?
if [ "$_rc" -eq 2 ]; then
  ok "no PR/branch argument is a usage error (exit 2)"
else
  bad "missing argument should exit 2 (rc=$_rc): $_out"
fi

echo
echo "test_gate_box_launch: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
