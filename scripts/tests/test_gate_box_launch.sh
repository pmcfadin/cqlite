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

# A curated, npx-free bin directory built from THIS host's real tool locations — never the
# literal "/usr/bin:/bin" this fixture used to hardcode, which is npx-free only by accident
# and breaks on any host where node/npm's npx lands there (found the hard way: this box's
# own /usr/bin/npx -> ../lib/node_modules/npm/bin/npx-cli.js, #4267 endgame review). Every
# "good"/non-npx fixture below points BOX_PATH at this directory instead.
CLEANBIN="$T/cleanbin"
mkdir -p "$CLEANBIN"
for _tool in git hostname df date awk dirname basename sed tr mkdir readlink id cat env bash; do
  _tool_path=$(command -v "$_tool" 2>/dev/null) || continue
  ln -sf "$_tool_path" "$CLEANBIN/$_tool"
done
# systemctl is optional (the busy-lane check degrades to a NOTE without it) — link it only
# when this host actually has one, so the fixture also matches a real no-systemctl host.
if _systemctl_path=$(command -v systemctl 2>/dev/null); then
  ln -sf "$_systemctl_path" "$CLEANBIN/systemctl"
fi

# A profile that resolves and loads cleanly, used by every case that is not itself
# testing profile loading. Both admission bars are 0 so the fixture tree's real free
# space never fails an unrelated case.
_write_good_profile() {  # <name>
  cat >"$BOXES_DIR/$1.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="$CLEANBIN"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=0
BOX_TMP_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
}

# ---------------------------------------------------------------------------------
# (c) profile loading
# ---------------------------------------------------------------------------------
_out=$(run nosuchtestbox4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -q "no committed profile at" <<<"$_out"; then
  ok "missing profile is refused by name"
else
  bad "missing profile should be refused (rc=$_rc): $_out"
fi

cat >"$BOXES_DIR/incomplete4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
EOF
_out=$(run incomplete4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -q "does not set required variable" <<<"$_out"; then
  ok "profile missing a required variable is refused by name"
else
  bad "incomplete profile should be refused (rc=$_rc): $_out"
fi

cat >"$BOXES_DIR/badnumber4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="$CLEANBIN"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=150G
BOX_TMP_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(run badnumber4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -q "is not a plain" <<<"$_out"; then
  ok "a non-numeric BOX_MIN_FREE_GB is refused rather than silently admitted"
else
  bad "non-numeric BOX_MIN_FREE_GB should be refused (rc=$_rc): $_out"
fi

_write_good_profile good4267
_out=$(run good4267 main)
_rc=$?
if [ "$_rc" -eq 0 ] && grep -q "^GATE-BOX-LAUNCH: dry-run " <<<"$_out"; then
  ok "a complete profile loads and resolves cleanly (positive control)"
else
  bad "a complete, correct profile should dry-run clean (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# (c) profile loading — BOX_CANONICAL_CLONE pointed at a WORKTREE, not a primary clone.
# `git worktree add` writes `.git` as a regular file, so `[ -d "$CLONE/.git" ]` alone
# is false for a worktree too — the same shape as a missing checkout — and used to
# misreport this as "not a git checkout" instead of naming the actual problem (roborev
# finding, #4267 endgame review).
# ---------------------------------------------------------------------------------
WORKTREE_CLONE="$T/canonical-worktree"
gg -C "$CLONE" worktree add -q --detach "$WORKTREE_CLONE" main >/dev/null 2>&1
cat >"$BOXES_DIR/worktreebox4267.env" <<EOF
BOX_CANONICAL_CLONE="$WORKTREE_CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="$CLEANBIN"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=0
BOX_TMP_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(run worktreebox4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -qi "is a linked git" <<<"$_out" \
  && grep -qi "WORKTREE" <<<"$_out" \
  && ! grep -q "not a git.*checkout" <<<"$_out"; then
  ok "BOX_CANONICAL_CLONE pointed at a worktree is refused as a worktree, not misreported as 'not a git checkout'"
else
  bad "a worktree BOX_CANONICAL_CLONE should be refused by its real cause (rc=$_rc): $_out"
fi
gg -C "$CLONE" worktree remove --force "$WORKTREE_CLONE" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------------
# (a) env scrubbing — LANE_ID
# ---------------------------------------------------------------------------------
_out=$(LANE_ID=peer-lane bash "$LAUNCHER" main --box good4267 --box-dir "$BOXES_DIR" --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -q "LANE_ID='peer-lane' is set" <<<"$_out"; then
  ok "an inherited LANE_ID is refused, naming the value"
else
  bad "LANE_ID in the calling environment should be refused (rc=$_rc): $_out"
fi
# A LANE_ID that is exported but EMPTY is still "set" (env -u is the only way to remove
# it) — the check must not treat empty-string as absent.
_out=$(LANE_ID= bash "$LAUNCHER" main --box good4267 --box-dir "$BOXES_DIR" --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -q "LANE_ID=" <<<"$_out"; then
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
BOX_PATH="$NPXBIN:$CLEANBIN"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=0
BOX_TMP_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(run npxbox4267 main)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -q "'npx' resolves on the profile's PATH" <<<"$_out"; then
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
  && ! grep -q "RUSTFLAGS" <<<"$_out" \
  && ! grep -q "CQLITE_ALLOW_FILE_GROWTH" <<<"$_out" \
  && ! grep -q "GATE_BOX_LAUNCH_TEST_FOO" <<<"$_out"; then
  ok "ambient RUSTFLAGS/CQLITE_ALLOW_FILE_GROWTH/an arbitrary variable do not reach the built env"
else
  bad "ambient hazard variables must not leak into the built env (rc=$_rc): $_out"
fi
# The converse: --allow-file-growth is the ONLY way CQLITE_ALLOW_FILE_GROWTH=1 reaches
# the built env — assert it actually does when the flag is passed.
_out=$(run good4267 main --allow-file-growth)
_rc=$?
if [ "$_rc" -eq 0 ] && grep -q "CQLITE_ALLOW_FILE_GROWTH=1" <<<"$_out"; then
  ok "--allow-file-growth does add CQLITE_ALLOW_FILE_GROWTH=1 to the built env"
else
  bad "--allow-file-growth should add CQLITE_ALLOW_FILE_GROWTH=1 (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# (a) env scrubbing — webhook/token values must never appear IN THE CLEAR in --dry-run's
# printed env. This output is routinely pasted into PR/issue comments by closers
# (flow-closer.md), so a webhook URL leaking through the very check meant to prove
# things "resolve cleanly" hands a credential to a public log (roborev finding, #4267
# endgame review, CRITICAL). The name must still be disclosed as wired — dropping it
# silently would be the #4267-round-2 notify-disclosure regression all over again.
# ---------------------------------------------------------------------------------
_secret='https://hooks.example.test/T00/deadbeefSECRET1234'
_out=$(env -u LANE_ID CQLITE_NOTIFY_WEBHOOK="$_secret" CODEX_NOTIFY_WEBHOOK="$_secret" \
  bash "$LAUNCHER" main --box good4267 --box-dir "$BOXES_DIR" --dry-run 2>&1)
_rc=$?
if [ "$_rc" -eq 0 ] \
  && ! grep -qF "$_secret" <<<"$_out" \
  && grep -q "CQLITE_NOTIFY_WEBHOOK=<redacted>" <<<"$_out" \
  && grep -q "CODEX_NOTIFY_WEBHOOK=<redacted>" <<<"$_out" \
  && grep -q "notifications wired: CQLITE_NOTIFY_WEBHOOK, CODEX_NOTIFY_WEBHOOK" <<<"$_out"; then
  ok "webhook secret values are redacted in --dry-run output, but their names are still disclosed as wired"
else
  bad "webhook secrets must never appear verbatim in --dry-run output (rc=$_rc): $_out"
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
if [ "$_rc" -eq 1 ] && grep -q "does not have current" <<<"$_out" \
   && grep -qi "rebase before the gate of record" <<<"$_out"; then
  ok "a branch behind origin/main is refused (stale base), naming the CLAUDE.md rule"
else
  bad "a stale-based branch should be refused (rc=$_rc): $_out"
fi

# Positive control: rebase the branch onto the new main tip, and it must now pass.
( cd "$CLONE" && gg fetch -q origin && gg checkout -q feat4267 && gg rebase -q origin/main \
    && gg push -q -f origin feat4267 )
_out=$(run good4267 feat4267)
_rc=$?
if [ "$_rc" -eq 0 ] && grep -q "a descendant of origin/main" <<<"$_out"; then
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
if [ "$_rc" -eq 0 ] && grep -q "^GATE-BOX-LAUNCH: dry-run " <<<"$_out"; then
  ok "a PR-number argument resolves refs/pull/<n>/head through the same ancestry check"
else
  bad "PR-number resolution should dry-run clean (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# disk admission — a real REFUSAL (both bars are 0 everywhere else, making the check
# vacuous), the nearest-existing-ancestor walk for a not-yet-created lanes dir (roborev
# Medium finding, #4267 round 2), and that the lanes/tmpdir bars are INDEPENDENT (found
# running --dry-run against the real astro-processor box: a single shared bar refused
# every launch there, since /data/tmp's tmpfs total size is far below a sane lanes bar).
# ---------------------------------------------------------------------------------
cat >"$BOXES_DIR/hugebar4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="$CLEANBIN"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=999999999
BOX_TMP_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(run hugebar4267 feat4267)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -q "below the" <<<"$_out" \
  && grep -q "admission bar" <<<"$_out"; then
  ok "an absurdly high BOX_MIN_FREE_GB actually REFUSES the launch (disk check is not vacuous)"
else
  bad "disk admission should refuse below its bar (rc=$_rc): $_out"
fi

# The two bars must be INDEPENDENT — an astro-processor production bug, found by running
# --dry-run against the real box, where a single BOX_MIN_FREE_GB shared by BOX_LANES_DIR
# (hundreds of GB) and BOX_TMPDIR (a 48G tmpfs, fixed total size) refused every launch
# unconditionally at the tmpdir check. Here: a permissive lanes bar (0) alongside an
# absurdly high tmp bar must refuse ON THE TMPDIR specifically.
cat >"$BOXES_DIR/hugetmpbar4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="$CLEANBIN"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=0
BOX_TMP_MIN_FREE_GB=999999999
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(run hugetmpbar4267 feat4267)
_rc=$?
if [ "$_rc" -eq 1 ] && grep -q "REFUSING — tmpdir has only" <<<"$_out" \
  && ! grep -q "REFUSING — lanes has only" <<<"$_out"; then
  ok "the tmpdir admission bar is independent of the lanes bar (astro-processor bug, fixed)"
else
  bad "a high BOX_TMP_MIN_FREE_GB alone should refuse only the tmpdir check (rc=$_rc): $_out"
fi

ANCESTOR_LANES="$LANES/notyet/deeper"
cat >"$BOXES_DIR/ancestorwalk4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$ANCESTOR_LANES"
BOX_TMPDIR="$TMPDIR_FIXTURE"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="$CLEANBIN"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=0
BOX_TMP_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(run ancestorwalk4267 feat4267)
_rc=$?
if [ "$_rc" -eq 0 ] && grep -q "nearest existing ancestor '$LANES'" <<<"$_out" \
  && grep -q "disk(lanes):" <<<"$_out"; then
  ok "a not-yet-created lanes dir still runs the disk check, against its nearest existing ancestor"
else
  bad "the ancestor walk should measure and name '$LANES' (rc=$_rc): $_out"
fi

# A missing BOX_TMPDIR must REFUSE, not print "(will be created)": unlike BOX_LANES_DIR
# (mkdir'd later, :486) and BOX_LOG_DIR (mkdir'd later, :640), nothing in this script ever
# creates the tmpdir — it is expected to be a pre-mounted tmpfs, so mkdir'ing it would
# silently produce a plain directory over a missing mount instead of surfacing the problem
# (roborev finding, #4267 endgame review).
MISSING_TMPDIR="$T/tmp-not-mounted"
cat >"$BOXES_DIR/missingtmp4267.env" <<EOF
BOX_CANONICAL_CLONE="$CLONE"
BOX_LANES_DIR="$LANES"
BOX_TMPDIR="$MISSING_TMPDIR"
BOX_DATASETS_ROOT="$DATASETS"
BOX_PATH="$CLEANBIN"
BOX_JOBS=4
BOX_RUST_TEST_THREADS=1
BOX_MAX_CONCURRENCY=1
BOX_MIN_FREE_GB=0
BOX_TMP_MIN_FREE_GB=0
BOX_LOG_DIR="$LOGDIR"
EOF
_out=$(run missingtmp4267 feat4267)
_rc=$?
if [ "$_rc" -ne 0 ] && grep -q "REFUSING.*tmpdir path" <<<"$_out" \
  && ! grep -q "will be created" <<<"$_out"; then
  ok "a missing BOX_TMPDIR is REFUSED, never claimed as '(will be created)'"
else
  bad "a missing tmpdir should refuse, not claim it will be created (rc=$_rc): $_out"
fi

# ---------------------------------------------------------------------------------
# lane worktree create THEN refresh, non-dry-run — the direct pin for the roborev High
# finding (`[ -d "$LANE_DIR/.git" ]` is always false for a real worktree, since
# `git worktree add` writes `.git` as a regular file, so a refresh fell into the
# create branch and failed against an already-registered worktree, round 2).
#
# The dataset-verify step (section 7) runs for real once DRY_RUN is off, against
# BOX_CANONICAL_CLONE's OWN working tree (currently checked out on feat4267, from the
# rebase above) — so feat4267 needs a stub test-data/scripts/fetch-datasets.sh that
# --verify-only can call. It is committed ONTO feat4267 itself, so both the "create" and
# the "refresh" head below carry it. Beyond that stub, the fixture canonical clone still
# has no scripts/flow/gate-detached.sh, so the launch step fails after the worktree is
# ready — exactly the point this test needs to observe, so a non-zero exit is EXPECTED.
( cd "$CLONE" && gg checkout -q feat4267 \
    && mkdir -p test-data/scripts \
    && printf '#!/bin/sh\ncase "$1" in --verify-only) exit 0 ;; esac\nexit 0\n' >test-data/scripts/fetch-datasets.sh \
    && chmod +x test-data/scripts/fetch-datasets.sh \
    && gg add test-data/scripts/fetch-datasets.sh \
    && gg commit -qm stub-fetch-datasets >/dev/null \
    && gg push -q origin feat4267 )

FEAT_LANE="$LANES/feat4267"
_out=$(env -u LANE_ID bash "$LAUNCHER" feat4267 --box good4267 --box-dir "$BOXES_DIR" 2>&1)
_created_head=$(git -C "$FEAT_LANE" rev-parse HEAD 2>/dev/null || echo "")
_expected_head=$( (cd "$CLONE" && gg rev-parse origin/feat4267) )
if grep -q "lane worktree ready:" <<<"$_out" && [ "$_created_head" = "$_expected_head" ]; then
  ok "first launch for a lane CREATES the worktree at the resolved head"
else
  bad "lane worktree creation should succeed and land on $_expected_head (got '$_created_head'): $_out"
fi

# Advance feat4267 with a new commit (still a descendant of origin/main, still carrying
# the stub) and relaunch for the SAME lane: this must take the REFRESH path, not fail
# with "already exists".
( cd "$CLONE" && gg checkout -q feat4267 && echo four >i.txt && gg add i.txt \
    && gg commit -qm advance-feat >/dev/null && gg push -q origin feat4267 )
_new_head=$( (cd "$CLONE" && gg rev-parse origin/feat4267) )
_out=$(env -u LANE_ID bash "$LAUNCHER" feat4267 --box good4267 --box-dir "$BOXES_DIR" 2>&1)
_refreshed_head=$(git -C "$FEAT_LANE" rev-parse HEAD 2>/dev/null || echo "")
if grep -q "lane worktree ready:" <<<"$_out" \
  && ! grep -qi "worktree add' failed" <<<"$_out" \
  && ! grep -qi "already exists" <<<"$_out" \
  && [ "$_refreshed_head" = "$_new_head" ]; then
  ok "relaunching for the same lane REFRESHES it to the new head (pins the round-2 High finding)"
else
  bad "relaunch should refresh, not fail as 'already exists' (want $_new_head, got '$_refreshed_head'): $_out"
fi

# ---------------------------------------------------------------------------------
# A non-ignored UNTRACKED leftover in the lane (a stopped fix round's scratch file) must
# not survive a refresh: `checkout --force` discards only TRACKED changes, and the
# gate's own dirty check counts untracked files too, so a survivor would stamp
# `dirty: yes` and refuse to certify — the exact loss this launcher exists to prevent
# (roborev finding, #4267 endgame review, Medium).
# ---------------------------------------------------------------------------------
echo leftover >"$FEAT_LANE/stray-scratch-file.txt"
_out=$(env -u LANE_ID bash "$LAUNCHER" feat4267 --box good4267 --box-dir "$BOXES_DIR" 2>&1)
if grep -q "lane worktree ready:" <<<"$_out" && [ ! -e "$FEAT_LANE/stray-scratch-file.txt" ]; then
  ok "a non-ignored untracked leftover in the lane is cleaned by a refresh, not left to stamp the gate dirty"
else
  bad "a stray untracked file should be cleaned by a refresh: $_out"
fi

# ---------------------------------------------------------------------------------
# BOX_LANES_DIR/<lane> pointed at a PRIMARY clone (not a linked worktree at all) of the
# SAME origin must not be silently refreshed and certified from. This is the case that
# actually differs pre/post-fix: a primary clone's fetch+checkout SUCCEED (shared
# history, so $HEAD_SHA resolves fine) and its `.git` is a real directory, so the old
# `[ -e "$1/.git" ] && rev-parse --is-inside-work-tree` check could not tell it apart
# from a genuine linked worktree — the inverse of the BOX_CANONICAL_CLONE-is-a-worktree
# check earlier in this file, now also enforced on the lane side (roborev finding, #4267
# endgame review, Medium).
# ---------------------------------------------------------------------------------
FOREIGN_LANE="$LANES/main"
gg clone -q "$ORIGIN" "$FOREIGN_LANE" 2>/dev/null
_out=$(env -u LANE_ID bash "$LAUNCHER" main --box good4267 --box-dir "$BOXES_DIR" 2>&1)
_rc=$?
if [ "$_rc" -ne 0 ] && ! grep -q "lane worktree ready:" <<<"$_out"; then
  ok "a lane dir that is a PRIMARY clone (not a linked worktree) is refused, not silently refreshed"
else
  bad "a primary-clone lane dir should be refused, not treated as our worktree (rc=$_rc): $_out"
fi
rm -rf "$FOREIGN_LANE"

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
if [ "$_rc" -eq 2 ] && grep -q "unexpected arguments after" <<<"$_out"; then
  ok "extra arguments after '--' are refused rather than silently passed through"
else
  bad "'-- --lite' should be refused (rc=$_rc): $_out"
fi

# `--` as ordinary end-of-options, with the PR/branch positional STILL to come, must reach
# the same positional handling as omitting it — not be swallowed as "unexpected arguments"
# (roborev finding, #4267 endgame review: $# used to still count '--' itself, rejecting
# this case unconditionally and leaving '--' with no reachable use at all).
_out=$(env -u LANE_ID bash "$LAUNCHER" --box good4267 --box-dir "$BOXES_DIR" --dry-run -- main 2>&1)
_rc=$?
if [ "$_rc" -eq 0 ] && ! grep -q "unexpected arguments after" <<<"$_out"; then
  ok "'-- <pr-or-branch>' as ordinary end-of-options is accepted, not refused"
else
  bad "'-- main' should resolve 'main' as the positional (rc=$_rc): $_out"
fi

_out=$(run "../../../etc/passwd" main)
_rc=$?
if [ "$_rc" -eq 2 ] && grep -q "refusing box name" <<<"$_out"; then
  ok "a box name containing '..' is refused before it is used as a path"
else
  bad "a path-traversal box name should be refused (rc=$_rc): $_out"
fi

echo
echo "test_gate_box_launch: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
