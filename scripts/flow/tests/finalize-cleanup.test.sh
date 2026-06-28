#!/usr/bin/env bash
#
# Regression tests for scripts/flow/finalize-cleanup.sh (issue #1162).
#
# Each test builds an isolated sandbox: a bare "origin" remote + a working clone,
# feature branches, and worktrees — then runs the cleanup script and asserts the
# guardrails. No network, no GitHub; the merged-branch name stands in for the
# `gh pr view --json headRefName` the SKILL resolves in production.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLEANUP="$SCRIPT_DIR/../finalize-cleanup.sh"

PASS=0
FAIL=0
fail() { echo "  ✗ $*"; FAIL=$((FAIL+1)); }
ok()   { echo "  ✓ $*"; PASS=$((PASS+1)); }

# git in a throwaway identity so commits work in CI sandboxes
g() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

# build_sandbox <dir> : a bare origin + a clone on `main` with one commit.
build_sandbox() {
  local root="$1"
  mkdir -p "$root"
  g init --bare -q "$root/origin.git"
  g clone -q "$root/origin.git" "$root/work" 2>/dev/null
  ( cd "$root/work" || exit 1
    echo seed > seed.txt
    g add seed.txt
    g commit -qm "seed"
    g push -q -u origin main
  )
}

# add_branch_worktree <work> <branch> <wtdir> [dirty]
# creates <branch> off main, pushes it, and checks it out in a worktree.
add_branch_worktree() {
  local work="$1" branch="$2" wt="$3" dirty="${4:-}"
  ( cd "$work" || exit 1
    g worktree add -q -b "$branch" "$wt" main
    ( cd "$wt" || exit 1; echo "$branch" > f.txt; g add f.txt; g commit -qm "$branch work"; g push -q -u origin "$branch" )
    if [ "$dirty" = "dirty" ]; then ( cd "$wt" || exit 1; echo extra >> f.txt ); fi
  )
}

remote_branches() { g -C "$1" ls-remote --heads origin "issue-*" | awk '{print $2}' | sed 's,^refs/heads/,,' | sort; }

# ===========================================================================
echo "TEST 1: #1143 regression — merged branch's sibling active claim survives"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"
WORK="$T/work"
# active, unmerged, DIRTY effort that must survive
add_branch_worktree "$WORK" "issue-1143-scan-window-offload" "$T/wt-active" dirty
# the merged branch: simulate gh pr merge --delete-branch already removed it from
# origin, and finalize already... no: finalize is what we're testing. Create it,
# push, then delete from origin to mimic --delete-branch, leaving only the sibling.
add_branch_worktree "$WORK" "issue-1143-read-p99-regression" "$T/wt-merged" ""
g -C "$WORK" push -q origin --delete issue-1143-read-p99-regression
g -C "$WORK" worktree remove "$T/wt-merged" --force 2>/dev/null
# Now origin has ONLY issue-1143-scan-window-offload (the active one).
bash "$CLEANUP" --issue 1143 --merged-branch issue-1143-read-p99-regression \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1
rc=$?
[ "$rc" -eq 0 ] && ok "exit 0 (clean no-op for already-deleted merged branch)" || fail "expected exit 0, got $rc"
if remote_branches "$WORK" | grep -qx "issue-1143-scan-window-offload"; then
  ok "active sibling 'issue-1143-scan-window-offload' SURVIVES on origin"
else
  fail "active sibling was deleted from origin — REGRESSION"
fi
[ -d "$T/wt-active" ] && ok "active worktree survives" || fail "active worktree was removed"
rm -rf "$T"

# ===========================================================================
echo "TEST 2: >1 lock for the issue → refuse (exit 2), delete nothing"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1200-alpha" "$T/wt-a" ""
add_branch_worktree "$WORK" "issue-1200-beta"  "$T/wt-b" ""
out=$(bash "$CLEANUP" --issue 1200 --merged-branch issue-1200-alpha \
  --repo-root "$WORK" --worktrees-dir "$T" 2>&1); rc=$?
[ "$rc" -eq 2 ] && ok "exit 2 on multi-lock" || fail "expected exit 2, got $rc"
echo "$out" | grep -q "1:1:1:1 violation" && ok "surfaces 1:1:1:1 violation" || fail "no 1:1:1:1 message"
n=$(remote_branches "$WORK" | grep -c "issue-1200-")
[ "$n" -eq 2 ] && ok "both branches still on origin (nothing deleted)" || fail "expected 2 branches, got $n"
rm -rf "$T"

# ===========================================================================
echo "TEST 3: dirty worktree → refuse (exit 3), no deletion"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1201-feature" "$T/wt" dirty
out=$(bash "$CLEANUP" --issue 1201 --merged-branch issue-1201-feature \
  --repo-root "$WORK" --worktrees-dir "$T" 2>&1); rc=$?
[ "$rc" -eq 3 ] && ok "exit 3 on dirty worktree" || fail "expected exit 3, got $rc"
[ -d "$T/wt" ] && ok "dirty worktree survives" || fail "dirty worktree removed"
remote_branches "$WORK" | grep -qx "issue-1201-feature" && ok "origin branch survives" || fail "origin branch deleted"
rm -rf "$T"

# ===========================================================================
echo "TEST 4: unpushed commits → refuse (exit 3)"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1202-feature" "$T/wt" ""
( cd "$T/wt" || exit 1; echo more > g.txt; g add g.txt; g commit -qm "unpushed" )  # ahead of upstream
rc=0
bash "$CLEANUP" --issue 1202 --merged-branch issue-1202-feature \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 3 ] && ok "exit 3 on unpushed commits" || fail "expected exit 3, got $rc"
[ -d "$T/wt" ] && ok "worktree with unpushed work survives" || fail "worktree removed"
rm -rf "$T"

# ===========================================================================
echo "TEST 5: happy path (squash-merge, --confirm-unmerged) → worktree + origin lock removed"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1203-feature" "$T/wt" ""
# clean worktree, branch pushed; tip NOT in main (squash). flow-finalize passes
# --confirm-unmerged after verifying PR state=MERGED.
bash "$CLEANUP" --issue 1203 --merged-branch issue-1203-feature --confirm-unmerged \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1; rc=$?
[ "$rc" -eq 0 ] && ok "exit 0 on happy path" || fail "expected exit 0, got $rc"
[ ! -d "$T/wt" ] && ok "clean worktree removed" || fail "worktree not removed"
remote_branches "$WORK" | grep -qx "issue-1203-feature" && fail "origin lock NOT deleted" || ok "origin lock deleted"
rm -rf "$T"

# ===========================================================================
echo "TEST 6: glob safety — only the merged branch is targeted, sibling untouched"
# ===========================================================================
# Here the merged branch IS still on origin (gh did not --delete-branch), so two
# locks exist → guard 2 refuses. This proves the script never silently picks one
# of an ambiguous pair. (The clean-resolution path is TEST 1.)
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1204-merged" "$T/wt-m" ""
add_branch_worktree "$WORK" "issue-1204-active" "$T/wt-x" dirty
rc=0
bash "$CLEANUP" --issue 1204 --merged-branch issue-1204-merged \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 2 ] && ok "exit 2 — ambiguous pair refused, not guessed" || fail "expected exit 2, got $rc"
[ -d "$T/wt-x" ] && ok "active sibling worktree survives" || fail "sibling worktree removed"
rm -rf "$T"

# ===========================================================================
echo "TEST 7: no upstream + no origin branch + HEAD ahead of main → refuse (exit 3)"
# ===========================================================================
# The blind-spot case: a branch created locally, committed, but never pushed
# (no @{u}, no origin branch). The script must NOT silently remove it.
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
( cd "$WORK" || exit 1
  g worktree add -q -b issue-1205-local-only "$T/wt" main
  ( cd "$T/wt" || exit 1; echo x > f.txt; g add f.txt; g commit -qm "local only, never pushed" )
)
rc=0
bash "$CLEANUP" --issue 1205 --merged-branch issue-1205-local-only \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 3 ] && ok "exit 3 — no-upstream unpushed work refused" || fail "expected exit 3, got $rc"
[ -d "$T/wt" ] && ok "local-only worktree survives" || fail "local-only worktree removed — REGRESSION"
rm -rf "$T"

# ===========================================================================
echo "TEST 8: origin branch, unmerged tip, no --confirm-unmerged → refuse (exit 4)"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1206-feature" "$T/wt" ""
rc=0
bash "$CLEANUP" --issue 1206 --merged-branch issue-1206-feature \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 4 ] && ok "exit 4 — unmerged origin tip refused without confirmation" || fail "expected exit 4, got $rc"
remote_branches "$WORK" | grep -qx "issue-1206-feature" && ok "origin branch survives (Guard 4)" || fail "origin branch deleted without confirmation"
# validate-before-mutate: a refused (exit 4) run must NOT have removed the worktree
[ -d "$T/wt" ] && ok "worktree intact on refused path (no half-deletion)" || fail "worktree removed on exit-4 path — non-atomic"
rm -rf "$T"

# ===========================================================================
echo "TEST 9: same as 8 but with --confirm-unmerged → deletes (exit 0)"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1207-feature" "$T/wt" ""
bash "$CLEANUP" --issue 1207 --merged-branch issue-1207-feature --confirm-unmerged \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1; rc=$?
[ "$rc" -eq 0 ] && ok "exit 0 with --confirm-unmerged" || fail "expected exit 0, got $rc"
remote_branches "$WORK" | grep -qx "issue-1207-feature" && fail "origin branch NOT deleted" || ok "origin branch deleted with confirmation"
rm -rf "$T"

# ===========================================================================
echo "TEST 10: worktree outside --worktrees-dir → refuse (exit 3), no removal"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1208-feature" "$T/wt" ""
rc=0
# point --worktrees-dir at a sibling dir that does NOT contain the worktree
bash "$CLEANUP" --issue 1208 --merged-branch issue-1208-feature --confirm-unmerged \
  --repo-root "$WORK" --worktrees-dir "$T/elsewhere" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 3 ] && ok "exit 3 — worktree not under --worktrees-dir" || fail "expected exit 3, got $rc"
[ -d "$T/wt" ] && ok "out-of-dir worktree survives" || fail "out-of-dir worktree removed"
rm -rf "$T"

# ===========================================================================
echo "TEST 11: --dry-run previews without mutating (happy path)"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1209-feature" "$T/wt" ""
out=$(bash "$CLEANUP" --issue 1209 --merged-branch issue-1209-feature --confirm-unmerged --dry-run \
  --repo-root "$WORK" --worktrees-dir "$T" 2>&1); rc=$?
[ "$rc" -eq 0 ] && ok "exit 0 on dry-run" || fail "expected exit 0, got $rc"
[ -d "$T/wt" ] && ok "dry-run did not remove worktree" || fail "dry-run removed worktree"
remote_branches "$WORK" | grep -qx "issue-1209-feature" && ok "dry-run did not delete origin branch" || fail "dry-run deleted branch"
echo "$out" | grep -q "DRY-RUN" && ok "dry-run prints planned actions" || fail "no DRY-RUN output"
rm -rf "$T"

# ===========================================================================
echo "TEST 12: --dry-run still honors a refusal guard (dirty → exit 3)"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1211-feature" "$T/wt" dirty
rc=0
bash "$CLEANUP" --issue 1211 --merged-branch issue-1211-feature --dry-run \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 3 ] && ok "dry-run aborts on dirty worktree (exit 3)" || fail "expected exit 3, got $rc"
[ -d "$T/wt" ] && ok "worktree intact" || fail "worktree removed in dry-run"
rm -rf "$T"

# ===========================================================================
echo "TEST 13: stale upstream must not mask unpushed commits (exit 3)"
# ===========================================================================
# Round-3 regression: @{u} stays *configured* after the origin branch is deleted,
# so a name-only lookup + `|| echo 0` silently reported 0 unpushed. The SHA-verify
# fallback must catch the real unpushed commit.
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1210-feature" "$T/wt" ""    # pushed, upstream set
( cd "$T/wt" || exit 1; echo more > h.txt; g add h.txt; g commit -qm "unpushed after push" )
g -C "$WORK" push -q origin --delete issue-1210-feature        # origin branch gone; @{u} now stale
rc=0
bash "$CLEANUP" --issue 1210 --merged-branch issue-1210-feature \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 3 ] && ok "exit 3 — unpushed work not masked by stale upstream" || fail "expected exit 3, got $rc"
[ -d "$T/wt" ] && ok "worktree with unpushed work survives" || fail "worktree removed — REGRESSION"
rm -rf "$T"

# ===========================================================================
echo "TEST 14: --merged-branch not matching --issue → usage error (exit 64)"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
rc=0
bash "$CLEANUP" --issue 1212 --merged-branch issue-9999-wrong-issue \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 64 ] && ok "exit 64 — issue/branch identity mismatch rejected" || fail "expected exit 64, got $rc"
rm -rf "$T"

# ===========================================================================
echo "TEST 15: remote query error → fail closed (exit 5), no mutation"
# ===========================================================================
# A transient ls-remote failure must NOT read as '0 locks / branch deleted' and
# bypass Guard 2. Point at a non-existent remote to force the error.
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1213-feature" "$T/wt" ""
rc=0
bash "$CLEANUP" --issue 1213 --merged-branch issue-1213-feature --remote no-such-remote \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 5 ] && ok "exit 5 — fail closed on remote query error" || fail "expected exit 5, got $rc"
[ -d "$T/wt" ] && ok "worktree intact (no fail-open mutation)" || fail "worktree removed on remote error"
remote_branches "$WORK" | grep -qx "issue-1213-feature" && ok "origin branch intact" || fail "branch deleted on remote error"
rm -rf "$T"

# ===========================================================================
echo "TEST 16: ff/merge-commit path (tip in main) → cleanup WITHOUT --confirm-unmerged"
# ===========================================================================
# Exercises the tip_in_main=1 / local -d branch that the squash tests never hit.
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1214-feature" "$T/wt" ""
( cd "$WORK" || exit 1
  g fetch -q origin issue-1214-feature
  g merge -q --no-ff -m "merge 1214" origin/issue-1214-feature
  g push -q origin main )   # origin tip now contained in main
rc=0
bash "$CLEANUP" --issue 1214 --merged-branch issue-1214-feature \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 0 ] && ok "exit 0 on merged tip without --confirm-unmerged" || fail "expected exit 0, got $rc"
[ ! -d "$T/wt" ] && ok "worktree removed" || fail "worktree not removed"
remote_branches "$WORK" | grep -qx "issue-1214-feature" && fail "origin branch not deleted" || ok "origin branch deleted"
g -C "$WORK" show-ref --verify --quiet refs/heads/issue-1214-feature && fail "local branch not deleted" || ok "local branch deleted"
rm -rf "$T"

# ===========================================================================
echo "TEST 17: local main lagging origin/main (tip in origin/main) → still exit 0"
# ===========================================================================
# Realistic production state: the worktree repo's LOCAL main lags origin/main.
# `git branch -d` judges against local main and would refuse + 128-abort
# post-mutation; the script proves containment against origin/main and uses -D.
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1217-feature" "$T/wt" ""
g clone -q "$T/origin.git" "$T/work2" 2>/dev/null
( cd "$T/work2" || exit 1
  g fetch -q origin issue-1217-feature
  g merge -q --no-ff -m "merge 1217" origin/issue-1217-feature
  g push -q origin main )                 # origin/main now contains the feature
g -C "$WORK" fetch -q origin              # WORK: remote-tracking current; LOCAL main still behind
rc=0
bash "$CLEANUP" --issue 1217 --merged-branch issue-1217-feature \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 0 ] && ok "exit 0 with local main lagging origin/main" || fail "expected exit 0, got $rc"
g -C "$WORK" show-ref --verify --quiet refs/heads/issue-1217-feature && fail "local branch not deleted" || ok "local branch deleted despite lagging local main"
rm -rf "$T"

# ===========================================================================
echo "TEST 18: unresolvable --main-ref in indeterminate path → fail closed (exit 3)"
# ===========================================================================
# No upstream, no origin branch → indeterminate path falls back to MAIN_REF; if
# that is unresolvable we must refuse, not silently treat as '0 ahead' and delete.
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
( cd "$WORK" || exit 1
  g worktree add -q -b issue-1215-local "$T/wt" main
  ( cd "$T/wt" || exit 1; echo x > f.txt; g add f.txt; g commit -qm "local only" ) )
rc=0
bash "$CLEANUP" --issue 1215 --merged-branch issue-1215-local --main-ref refs/heads/does-not-exist \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 3 ] && ok "exit 3 — unresolvable --main-ref fails closed" || fail "expected exit 3, got $rc"
[ -d "$T/wt" ] && ok "worktree survives" || fail "worktree removed on unresolvable main-ref"
rm -rf "$T"

# ===========================================================================
echo "TEST 19: stale worktree dir (removed out-of-band) → prune, not a 128 abort"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
add_branch_worktree "$WORK" "issue-1216-feature" "$T/wt" ""
rm -rf "$T/wt"   # delete the worktree dir out-of-band; git's admin entry lingers
rc=0
bash "$CLEANUP" --issue 1216 --merged-branch issue-1216-feature --confirm-unmerged \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 0 ] && ok "exit 0 — stale entry handled (no set -e 128 abort)" || fail "expected exit 0, got $rc"
remote_branches "$WORK" | grep -qx "issue-1216-feature" && fail "origin branch not deleted" || ok "origin branch deleted"
n=$(g -C "$WORK" worktree list --porcelain | grep -c '^worktree ')
[ "$n" -eq 1 ] && ok "stale worktree entry pruned (only root remains)" || fail "stale entry remains ($n worktrees)"
rm -rf "$T"

# ===========================================================================
echo "TEST 20: non-numeric --issue rejected (exit 64) — keeps Guard 2 glob tight"
# ===========================================================================
T=$(mktemp -d); build_sandbox "$T"; WORK="$T/work"
rc=0
bash "$CLEANUP" --issue 'abc' --merged-branch issue-abc-x \
  --repo-root "$WORK" --worktrees-dir "$T" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 64 ] && ok "exit 64 — non-numeric --issue rejected" || fail "expected exit 64, got $rc"
rm -rf "$T"

# ===========================================================================
echo "TEST 21: --help exits 0 and renders the usage block"
# ===========================================================================
out="$(bash "$CLEANUP" --help 2>&1)"; rc=$?
[ "$rc" -eq 0 ] && ok "--help exits 0" || fail "expected exit 0, got $rc"
echo "$out" | grep -q "USAGE" && ok "--help shows USAGE" || fail "--help missing USAGE"
echo "$out" | grep -q "EXIT CODES" && ok "--help shows EXIT CODES" || fail "--help missing EXIT CODES"
echo "$out" | grep -q "END-HELP" && fail "--help leaked the sentinel" || ok "--help stops before sentinel"

echo ""
echo "================  finalize-cleanup: $PASS passed, $FAIL failed  ================"
[ "$FAIL" -eq 0 ]
