#!/usr/bin/env bash
# test_gate_box_launch.sh — self-test for scripts/flow/gate-box-launch.sh (issue #4267).
#
# #4267's acceptance criterion 1 names three things this suite must cover:
#   (a) env scrubbing: a LANE_ID in the calling environment, and an npx-bearing PATH in
#       the box profile, are both rejected — and the ALLOWLIST claim itself is checked:
#       an ambient RUSTFLAGS/CQLITE_ALLOW_FILE_GROWTH/arbitrary variable must NOT reach
#       the printed (and, in a real launch, the actual) gate environment;
#   (b) stale-base refusal: a head whose ancestry does not include current origin/main
#       is refused, mirroring CLAUDE.md's "a gate script behind origin/main cannot
#       certify" rule;
#   (c) profile loading: a missing profile and a profile missing a required variable are
#       both refused by name, and a complete profile resolves and loads.
#
# Everything here runs under --dry-run against a throwaway bare "origin" + a throwaway
# "canonical clone" this script creates and destroys itself — no network, no real box, no
# systemd, no cargo. Fixture box profiles live under `--box-dir "$T/boxes"`, a scratch
# directory this script owns and the EXIT trap removes with everything else — NEVER under
# the tracked scripts/flow/boxes/, which would enter the #2926 tree-integrity digest as an
# untracked (not gitignored) file and could FAIL a concurrently-running gate component
# (roborev finding, #4267 round 2). Two positive controls (a rebased branch, a complete
# profile) prove the harness can pass at all, since a suite that only ever refuses cannot
# tell "correctly rejected" from "cannot succeed at anything" (the case-floor lesson,
# #3544).
#
# Every invocation below runs under `env -u LANE_ID` except the two cases that ARE
# testing LANE_ID: without that scrub, a shell that happens to export LANE_ID (exactly
# the #4252 situation this launcher exists to prevent) would fail five unrelated cases,
# including both positive controls, for an environmental reason (roborev finding, #4267
# round 2).
#
# Run standalone:   bash scripts/tests/test_gate_box_launch.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && cd .. && pwd)"
LAUNCHER="$REPO_ROOT/scripts/flow/gate-box-launch.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

[ -x "$LAUNCHER" ] || bad "gate-box-launch.sh must be executable ($LAUNCHER)"
[ -f "$LAUNCHER" ] || { echo "test_gate_box_launch: cannot find $LAUNCHER" >&2; exit 1; }

# run <box> <pr-or-branch> [extra launcher args...] — every case goes through this, so the
# LANE_ID scrub and --box-dir pointing at the scratch fixtures are never forgotten at a
# call site.
run() {
  local box="$1" arg="$2"; shift 2
  env -u LANE_ID bash "$LAUNCHER" "$arg" --box "$box" --box-dir "$BOXES_DIR" --dry-run "$@" 2>&1
}

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
BOXES_DIR="$T/boxes"

gg init --bare -q "$ORIGIN"
gg clone -q "$ORIGIN" "$SEED" 2>/dev/null
( cd "$SEED" && echo one >f.txt && gg add f.txt && gg commit -qm seed >/dev/null && gg push -q -u origin main )
gg clone -q "$ORIGIN" "$CLONE" 2>/dev/null

mkdir -p "$LANES" "$TMPDIR_FIXTURE" "$DATASETS" "$LOGDIR" "$BOXES_DIR"

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

# ---------------------------------------------------------------------------------
# (c) profile loading
# ---------------------------------------------------------------------------------
_out=$(run nosuchtestbox4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "no committed profile at"; then
  ok "missing profile is refused by name"
else
  bad "missing profile should be refused (rc=$_rc): $_out"
fi

cat >"$BOXES_DIR/incomplete4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
EOF
_out=$(run incomplete4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "does not set required variable"; then
  ok "profile missing a required variable is refused by name"
else
  bad "incomplete profile should be refused (rc=$_rc): $_out"
fi

cat >"$BOXES_DIR/badnumber4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="/usr/bin:/bin"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=150G
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(run badnumber4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "is not a plain"; then
  ok "a non-numeric BOX_MIN_FREE_GB is refused rather than silently admitted"
else
  bad "non-numeric BOX_MIN_FREE_GB should be refused (rc=$_rc): $_out"
fi

_write_good_profile good4267
_out=$(run good4267 main)
_rc=$?
if [ "$_rc" -eq 0 ] && printf '%s' "$_out" | grep -q "^GATE-BOX-LAUNCH: dry-run "; then
  ok "a complete profile loads and resolves cleanly (positive control)"
else
  bad "a complete, correct profile should dry-run clean (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# (a) env scrubbing — LANE_ID
# ---------------------------------------------------------------------------------
_out=$(LANE_ID=peer-lane bash "$LAUNCHER" main --box good4267 --box-dir "$BOXES_DIR" --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "LANE_ID='peer-lane' is set"; then
  ok "an inherited LANE_ID is refused, naming the value"
else
  bad "LANE_ID in the calling environment should be refused (rc=$_rc): $_out"
fi
# A LANE_ID that is exported but EMPTY is still "set" (env -u is the only way to remove
# it) — the check must not treat empty-string as absent.
_out=$(LANE_ID= bash "$LAUNCHER" main --box good4267 --box-dir "$BOXES_DIR" --dry-run 2>&1)
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
_out=$(run npxbox4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && printf '%s' "$_out" | grep -q "'npx' resolves on the profile's PATH"; then
  ok "an npx-bearing PATH is refused, naming the resolved path"
else
  bad "npx on PATH should be refused (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# (a) env scrubbing — the allowlist claim itself: an ambient hazard variable must not
# reach the printed (and therefore the real) gate environment.
# ---------------------------------------------------------------------------------
_out=$(env -u LANE_ID RUSTFLAGS='-D warnings' CQLITE_ALLOW_FILE_GROWTH=1 GATE_BOX_LAUNCH_TEST_FOO=bar \
  bash "$LAUNCHER" main --box good4267 --box-dir "$BOXES_DIR" --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 0 ] \
  && ! printf '%s' "$_out" | grep -q "RUSTFLAGS" \
  && ! printf '%s' "$_out" | grep -q "CQLITE_ALLOW_FILE_GROWTH" \
  && ! printf '%s' "$_out" | grep -q "GATE_BOX_LAUNCH_TEST_FOO"; then
  ok "ambient RUSTFLAGS/CQLITE_ALLOW_FILE_GROWTH/an arbitrary variable do not reach the built env"
else
  bad "ambient hazard variables must not leak into the built env (rc=$_rc): $_out"
fi
# The converse: --allow-file-growth is the ONLY way CQLITE_ALLOW_FILE_GROWTH=1 reaches
# the built env — assert it actually does when the flag is passed.
_out=$(run good4267 main --allow-file-growth)
_rc=$?
if [ "$_rc" -eq 0 ] && printf '%s' "$_out" | grep -q "CQLITE_ALLOW_FILE_GROWTH=1"; then
  ok "--allow-file-growth does add CQLITE_ALLOW_FILE_GROWTH=1 to the built env"
else
  bad "--allow-file-growth should add CQLITE_ALLOW_FILE_GROWTH=1 (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# (b) stale-base refusal
# ---------------------------------------------------------------------------------
( cd "$CLONE" && gg checkout -q -b feat4267 && echo two >g.txt && gg add g.txt \
    && gg commit -qm feat >/dev/null && gg push -q -u origin feat4267 )
# advance origin/main so feat4267's merge-base is now BEHIND the tip
( cd "$SEED" && echo three >h.txt && gg add h.txt && gg commit -qm advance >/dev/null && gg push -q origin main )

_out=$(run good4267 feat4267)
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
_out=$(run good4267 feat4267)
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
_out=$(run good4267 99)
_rc=$?
if [ "$_rc" -eq 0 ] && printf '%s' "$_out" | grep -q "^GATE-BOX-LAUNCH: dry-run "; then
  ok "a PR-number argument resolves refs/pull/<n>/head through the same ancestry check"
else
  bad "PR-number resolution should dry-run clean (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# usage errors and other named refusals
# ---------------------------------------------------------------------------------
_out=$(env -u LANE_ID bash "$LAUNCHER" 2>&1); _rc=$?
if [ "$_rc" -eq 2 ]; then
  ok "no PR/branch argument is a usage error (exit 2)"
else
  bad "missing argument should exit 2 (rc=$_rc): $_out"
fi

_out=$(run good4267 main -- --lite)
_rc=$?
if [ "$_rc" -eq 2 ] && printf '%s' "$_out" | grep -q "unexpected arguments after"; then
  ok "extra arguments after '--' are refused rather than silently passed through"
else
  bad "'-- --lite' should be refused (rc=$_rc): $_out"
fi

_out=$(run "../../../etc/passwd" main)
_rc=$?
if [ "$_rc" -eq 2 ] && printf '%s' "$_out" | grep -q "refusing box name"; then
  ok "a box name containing '..' is refused before it is used as a path"
else
  bad "a path-traversal box name should be refused (rc=$_rc): $_out"
fi

echo
echo "test_gate_box_launch: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
