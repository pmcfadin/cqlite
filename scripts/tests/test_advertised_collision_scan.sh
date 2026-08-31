#!/usr/bin/env bash
#
# Regression tests for scripts/flow/advertised-collision-scan.sh (issue #3436,
# lead deliverable 2, epic #2664).
#
# HERMETIC BY CONSTRUCTION: a mktemp BARE repo stands in for origin (so
# `git ls-remote` is REAL, not stubbed), a PATH-shimmed fake `gh` stands in for
# the board read, and LANE_ROOT points into the sandbox. NO network, NO GitHub,
# NO python3, NO cargo — this suite runs in the gate's `tooling-tests` component
# BEFORE its python3 gate, so it must need nothing beyond bash + git + coreutils.
#
# Run standalone:   bash scripts/tests/test_advertised_collision_scan.sh
#
# THE PROPERTY UNDER TEST IS THREE-FACTS-ANDED PLUS POSITIVE-DETECTION-ONLY:
#   * all three facts true                 -> the row is reported, exit 3
#   * ANY ONE fact false                   -> nothing reported, exit 1
#   * ANY input unmeasurable               -> exit 1 AND a line NAMING the input
#   * nothing reported                     -> exit 1, NEVER 0
#   * the scan MUTATES NOTHING
# The one-fact-false cases are three separate cases on purpose: a detector that
# fires on two of the three facts passes a combined case and fails here.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCAN="$SCRIPT_DIR/../flow/advertised-collision-scan.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

gg() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/collision-scan-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

ORIGIN="$T/origin.git"
W="$T/w"
export LANE_ROOT="$T/lanes"
mkdir -p "$LANE_ROOT"

gg init --bare -q "$ORIGIN"
gg clone -q "$ORIGIN" "$W" 2>/dev/null
(
  cd "$W" || exit 1
  echo seed >seed.txt
  gg add seed.txt
  gg commit -qm seed
  gg push -q -u origin main
)

push_branch() {   # <issue> — a pushed lane branch, no claim ref
  (
    cd "$W" || exit 1
    gg checkout -q -b "issue-$1-slug" main
    gg commit -q --allow-empty -m "work on issue $1"
    gg push -q origin "issue-$1-slug"
    gg checkout -q main
    gg branch -q -D "issue-$1-slug"
  )
}
push_claim() {    # <issue> — hold refs/claims/issue-<N>
  (
    cd "$W" || exit 1
    gg commit -q --allow-empty -m "claim issue=$1"
    gg push -q origin "HEAD:refs/claims/issue-$1"
    gg reset -q --hard HEAD~1
  )
}

# mk_gh <dir> <issue...> — a fake `gh` whose board read answers with those issue
# numbers, one per line, exactly as the real `--jq '.items[]|.content.number'`
# does. It also RECORDS its argv, so a case can assert HOW the board was read.
mk_gh() {
  local dir="$1"; shift
  mkdir -p "$dir"
  {
    printf '#!/usr/bin/env bash\n'
    printf 'printf "%%s\\n" "$@" >>"%s/gh-args.txt"\n' "$dir"
    local n
    for n in "$@"; do printf 'printf "%%s\\n" %s\n' "$n"; done
    printf 'exit 0\n'
  } >"$dir/gh"
  chmod +x "$dir/gh"
}

run_scan() {   # <ghdir> [args...]
  local ghdir="$1"; shift
  ( PATH="$ghdir:$PATH" CLAIM_REMOTE="$ORIGIN" bash "$SCAN" "$@" )
}

refs_snapshot() { gg ls-remote "$ORIGIN" | sort; }
tree_snapshot() { find "$LANE_ROOT" 2>/dev/null | sort; }

# ===========================================================================
echo "TEST 1: all three facts true -> the row is REPORTED, exit 3"
# ===========================================================================
push_branch 600
GH1="$T/gh-600"; mk_gh "$GH1" 600
out=$(run_scan "$GH1"); rc=$?
if [ "$rc" -eq 3 ] && printf '%s\n' "$out" | grep -q '^COLLISION: issue=600 ' \
   && printf '%s\n' "$out" | grep -q 'branches=refs/heads/issue-600-slug' \
   && printf '%s\n' "$out" | grep -q 'claim-ref=absent' \
   && printf '%s\n' "$out" | grep -q 'RESULT=FOUND'; then
  ok "board Ready + pushed branch + no claim ref => one COLLISION row, exit 3"
else
  bad "expected a COLLISION row for issue 600 and exit 3; got rc=$rc
$out"
fi

# The row composes the two locks the issue says know nothing about each other.
if printf '%s\n' "$out" | grep -q 'lane-lock='; then
  ok "the row carries the machine-local lane-lock state"
else
  bad "the row carried no lane-lock= field:
$out"
fi

# HOW the board was read is part of the contract: an UNFILTERED item-list
# silently truncates this 900+ item board and has produced wrong 'nothing is
# Ready' reads, so the filter must be server-side and the limit explicit.
ghargs=$(cat "$GH1/gh-args.txt" 2>/dev/null)
if printf '%s\n' "$ghargs" | grep -qx 'status:Ready' \
   && printf '%s\n' "$ghargs" | grep -qx 'item-list' \
   && printf '%s\n' "$ghargs" | grep -qx -- '--query' \
   && printf '%s\n' "$ghargs" | grep -qx -- '-L' \
   && ! printf '%s\n' "$ghargs" | grep -qx 'api'; then
  ok "the board is read with a SERVER-SIDE filtered item-list (--query status:Ready, explicit -L), not GraphQL"
else
  bad "board read was not a filtered item-list; gh argv was:
$ghargs"
fi

# ===========================================================================
echo "TEST 2: fact (3) false — a HELD claim ref closes the window (exit 1)"
# ===========================================================================
push_branch 601
push_claim 601
GH2="$T/gh-601"; mk_gh "$GH2" 601
out=$(run_scan "$GH2"); rc=$?
if [ "$rc" -eq 1 ] && ! printf '%s\n' "$out" | grep -q 'issue=601' \
   && printf '%s\n' "$out" | grep -q 'RESULT=NONE-REPORTED'; then
  ok "a held refs/claims/issue-601 is NOT reported (exit 1)"
else
  bad "expected issue 601 unreported with exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 3: fact (2) false — Ready with NO pushed branch (exit 1)"
# ===========================================================================
GH3="$T/gh-602"; mk_gh "$GH3" 602    # 602 is Ready, and has no branch at all
out=$(run_scan "$GH3"); rc=$?
if [ "$rc" -eq 1 ] && ! printf '%s\n' "$out" | grep -q 'issue=602'; then
  ok "a Ready issue with no pushed issue-602-* branch is NOT reported (exit 1)"
else
  bad "expected issue 602 unreported with exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 4: fact (1) false — a pushed branch whose board Status is NOT Ready (exit 1)"
# ===========================================================================
push_branch 603
GH4="$T/gh-nonready"; mk_gh "$GH4" 999999   # 603 absent from the Ready column
out=$(run_scan "$GH4"); rc=$?
if [ "$rc" -eq 1 ] && ! printf '%s\n' "$out" | grep -q 'issue=603'; then
  ok "a pushed branch for an issue the board does NOT offer as Ready is NOT reported (exit 1)"
else
  bad "expected issue 603 unreported with exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 5: NONE-REPORTED is exit 1 and NEVER exit 0"
# ===========================================================================
# The whole point of positive-detection-only: an implementation returning 0 for
# 'nothing found' would otherwise look correct, and a cron reading 0 as a clean
# bill of health is #3393's fail-open family.
GH5="$T/gh-empty"; mk_gh "$GH5"
out=$(run_scan "$GH5"); rc=$?
if [ "$rc" -eq 1 ] && [ "$rc" -ne 0 ] && printf '%s\n' "$out" | grep -q 'RESULT=NONE-REPORTED'; then
  ok "an empty Ready column yields exit 1 (never 0) and says it is not a clean bill of health"
else
  bad "expected exit 1 with RESULT=NONE-REPORTED; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 6: unmeasurable board — gh EXITS NON-ZERO (exit 1, input NAMED)"
# ===========================================================================
GHFAIL="$T/gh-fail"; mkdir -p "$GHFAIL"
printf '#!/usr/bin/env bash\necho "gh: HTTP 502" >&2\nexit 1\n' >"$GHFAIL/gh"
chmod +x "$GHFAIL/gh"
out=$(run_scan "$GHFAIL" 2>/dev/null); rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$out" | grep -q 'UNMEASURABLE' \
   && printf '%s\n' "$out" | grep -q 'what=board-status' \
   && ! printf '%s\n' "$out" | grep -q 'RESULT=NONE-REPORTED'; then
  ok "a failing gh is UNMEASURABLE what=board-status (exit 1), NOT a 'none found'"
else
  bad "expected UNMEASURABLE what=board-status exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 7: unmeasurable board — gh NOT ON PATH AT ALL (exit 1, input NAMED)"
# ===========================================================================
# A minimal PATH holding only the tools the scan needs, so `gh` is genuinely
# absent rather than shadowed by a stub that pretends to be absent.
MINBIN="$T/minbin"; mkdir -p "$MINBIN"
REALBASH="$(command -v bash)"
for tool in bash git awk grep head sort tr cut basename dirname timeout cat find; do
  p=$(command -v "$tool" 2>/dev/null) && ln -sf "$p" "$MINBIN/$tool"
done
out=$( PATH="$MINBIN" CLAIM_REMOTE="$ORIGIN" "$REALBASH" "$SCAN" 2>/dev/null ); rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$out" | grep -q 'UNMEASURABLE' \
   && printf '%s\n' "$out" | grep -q 'what=board-status' \
   && printf '%s\n' "$out" | grep -q 'not on PATH'; then
  ok "gh absent from PATH is UNMEASURABLE what=board-status naming the missing tool (exit 1)"
else
  bad "expected UNMEASURABLE naming gh-not-on-PATH exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 8: unmeasurable branches — ls-remote against an unreachable remote (exit 1, input NAMED)"
# ===========================================================================
GH8="$T/gh-600b"; mk_gh "$GH8" 600
out=$( PATH="$GH8:$PATH" CLAIM_REMOTE="$T/does-not-exist.git" bash "$SCAN" 2>/dev/null ); rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$out" | grep -q 'UNMEASURABLE' \
   && printf '%s\n' "$out" | grep -q 'what=issue-branches' \
   && printf '%s\n' "$out" | grep -q 'ls-remote' \
   && ! printf '%s\n' "$out" | grep -q 'RESULT=NONE-REPORTED'; then
  ok "an unreachable remote is UNMEASURABLE what=issue-branches naming ls-remote (exit 1)"
else
  bad "expected UNMEASURABLE what=issue-branches exit 1; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 9: the scan MUTATES NOTHING — refs and lane tree byte-identical"
# ===========================================================================
# It reports and never acts, because only the session on that box knows whether
# it owns the branch: from here, 'the lane is yours' and 'a peer abandoned it'
# look identical and have OPPOSITE remedies.
# Give lane 600 a REAL, live lane lock first, so the no-mutation claim is tested
# against an EXISTING record (a lane with no record can be left unchanged by a
# tool that writes only when it finds one) and so the row's lane-lock field is
# something other than FREE.
LANELOCK="$SCRIPT_DIR/../flow/lane-lock.sh"
sleep 900 &
SLEEPER=$!
LANE_LOCK_PID=$SLEEPER bash "$LANELOCK" acquire 600 >/dev/null 2>&1
# The lock record lives in the sibling LOCK ROOT, not in the lane directory (#3436:
# `git worktree add` refuses a target that exists at all, so a lock inside the lane
# would forbid acquire-before-worktree-add).
LANE_LOCK_RECORD_600="$LANE_ROOT/.lane-locks/lane-600.lock"
recordBefore=$(cat "$LANE_LOCK_RECORD_600" 2>/dev/null)
refsBefore=$(refs_snapshot)
treeBefore=$(tree_snapshot)
GH9="$T/gh-mutate"; mk_gh "$GH9" 600 601 602 603
out=$(run_scan "$GH9"); rc=$?
refsAfter=$(refs_snapshot)
treeAfter=$(tree_snapshot)
recordAfter=$(cat "$LANE_LOCK_RECORD_600" 2>/dev/null)
if [ "$refsBefore" = "$refsAfter" ] && [ "$treeBefore" = "$treeAfter" ] \
   && [ -n "$recordBefore" ] && [ "$recordBefore" = "$recordAfter" ] && [ "$rc" -eq 3 ]; then
  ok "a FOUND run left every ref, the whole lane tree AND an existing lane-lock record byte-identical"
else
  bad "the scan mutated something (rc=$rc)
refs before:
$refsBefore
refs after:
$refsAfter
tree before:
$treeBefore
tree after:
$treeAfter
record before: $recordBefore
record after:  $recordAfter"
fi

# The lane-lock field is READ, not invented: with a live holder it must report the
# probe's own HELD/ALIVE words rather than the FREE it reported when the lane was
# empty in TEST 1.
if printf '%s\n' "$out" | grep -q 'issue=600 .*lane-lock=HELD/ALIVE'; then
  ok "the row reports the probe's own verdict for a live holder (lane-lock=HELD/ALIVE), not a re-derived one"
else
  bad "expected lane-lock=HELD/ALIVE for issue 600 with a live holder:
$out"
fi
kill "$SLEEPER" 2>/dev/null || true
wait "$SLEEPER" 2>/dev/null || true

# ===========================================================================
echo "TEST 10: --issue narrows the scan; the three facts are unchanged"
# ===========================================================================
out=$(run_scan "$GH9" --issue 600); rc=$?
outOther=$(run_scan "$GH9" --issue 601); rcOther=$?
if [ "$rc" -eq 3 ] && printf '%s\n' "$out" | grep -q 'issue=600' \
   && [ "$rcOther" -eq 1 ] && ! printf '%s\n' "$outOther" | grep -q '^COLLISION:'; then
  ok "--issue 600 reports only 600 (exit 3); --issue 601 (claim held) reports nothing (exit 1)"
else
  bad "--issue filtering wrong: rc=$rc rcOther=$rcOther
600: $out
601: $outOther"
fi

# ===========================================================================
echo "TEST 11: --json emits one object per row plus a summary, same exit codes"
# ===========================================================================
out=$(run_scan "$GH9" --json --issue 600); rc=$?
if [ "$rc" -eq 3 ] && printf '%s\n' "$out" | grep -q '"issue":600' \
   && printf '%s\n' "$out" | grep -q '"result":"FOUND"' \
   && ! printf '%s\n' "$out" | grep -q '^COLLISION:'; then
  ok "--json emits a row object and a FOUND summary object, exit 3, with no text rows mixed in"
else
  bad "expected JSON row + summary with exit 3; got rc=$rc
$out"
fi

# ===========================================================================
echo "TEST 12: --help exits 0 and DOCUMENTS the exit codes; a bad flag is refused"
# ===========================================================================
outHelp=$(bash "$SCAN" --help); rcHelp=$?
rcBad=0; outBad=$(bash "$SCAN" --bogus 2>&1) || rcBad=$?
if [ "$rcHelp" -eq 0 ] \
   && printf '%s\n' "$outHelp" | grep -q 'POSITIVE-DETECTION ONLY' \
   && printf '%s\n' "$outHelp" | grep -qE '^ *3 +at least one row' \
   && printf '%s\n' "$outHelp" | grep -qE '^ *1 +no row was reported' \
   && printf '%s\n' "$outHelp" | grep -qE '^ *64 +usage error' \
   && [ "$rcBad" -eq 64 ] && printf '%s\n' "$outBad" | grep -q 'unknown argument'; then
  ok "--help exits 0 documenting exit 3/1/64 and never-exit-0; an unknown flag is REFUSED (exit 64), not ignored"
else
  bad "help/usage contract wrong: rcHelp=$rcHelp rcBad=$rcBad
help: $outHelp
bad:  $outBad"
fi

# ===========================================================================
echo
echo "==== ADVERTISED-COLLISION-SCAN TEST SUMMARY: PASS=$PASS FAIL=$FAIL ===="
if [ "$FAIL" -eq 0 ]; then echo "RESULT: PASS"; exit 0; else echo "RESULT: FAIL"; exit 1; fi
