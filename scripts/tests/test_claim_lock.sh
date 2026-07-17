#!/usr/bin/env bash
#
# Regression tests for scripts/flow/claim.sh (issue #2665, epic #2664 FM2).
#
# Fast + hermetic: a mktemp BARE repo stands in for origin, plus TWO separate
# clones playing claimant A and claimant B (each overriding CLAIM_MACHINE). No
# network, no GitHub — the claim is a pure git-ref mechanism. The one subcommand
# that needs gh (`release`, open-PR guard) gets a PATH-shimmed fake `gh`.
#
# Run standalone:   bash scripts/tests/test_claim_lock.sh
#
# No wall-clock timing assertions — every verdict is a git-ref state or exit code.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLAIM="$SCRIPT_DIR/../flow/claim.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# git in a throwaway identity so commits/pushes work in any sandbox.
gg() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/claim-lock-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

ORIGIN="$T/origin.git"
A="$T/A"
B="$T/B"
gg init --bare -q "$ORIGIN"
gg clone -q "$ORIGIN" "$A" 2>/dev/null
gg clone -q "$ORIGIN" "$B" 2>/dev/null
(
  cd "$A" || exit 1
  echo seed >seed.txt
  gg add seed.txt
  gg commit -qm seed
  gg push -q -u origin main
)
( cd "$B" && gg fetch -q origin )

# runA/runB — run claim.sh from clone A/B as a distinct machine. The function
# EXIT CODE is claim.sh's exit code, so callers use `out=$(runA ...); rc=$?`
# (a command-substitution subshell would otherwise swallow any global we set).
runA() { ( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$CLAIM" "$@" ); }
runB() { ( cd "$B" && CLAIM_MACHINE=machineB CLAIM_REMOTE=origin bash "$CLAIM" "$@" ); }

ref_sha() { gg -C "$A" ls-remote origin "refs/claims/issue-$1" | awk '{print $1}' | head -1; }

# ===========================================================================
echo "TEST 1: A claims, B claims same issue -> exactly one HELD, other LOST(2)"
# ===========================================================================
outA=$(runA claim 1); rcA=$?
outB=$(runB claim 1); rcB=$?
held=0; lost=0
printf '%s\n' "$outA" | grep -q 'CLAIM: HELD' && held=$((held+1))
printf '%s\n' "$outB" | grep -q 'CLAIM: HELD' && held=$((held+1))
printf '%s\n' "$outA" | grep -q 'CLAIM: LOST' && lost=$((lost+1))
printf '%s\n' "$outB" | grep -q 'CLAIM: LOST' && lost=$((lost+1))
if [ "$held" -eq 1 ] && [ "$lost" -eq 1 ] && [ "$rcA" -eq 0 ] && [ "$rcB" -eq 2 ]; then
  ok "exactly one HELD (rcA=0) and one LOST (rcB=2)"
else
  bad "expected 1 HELD/1 LOST rcA=0 rcB=2; got held=$held lost=$lost rcA=$rcA rcB=$rcB
A: $outA
B: $outB"
fi

# ===========================================================================
echo "TEST 2: identical-base hazard — both fresh from the same tip, one loses"
# ===========================================================================
# A and B both fetch the same origin/main and race a claim for a fresh issue.
( cd "$A" && gg fetch -q origin ) ; ( cd "$B" && gg fetch -q origin )
outA=$(runA claim 2); rcA=$?
outB=$(runB claim 2); rcB=$?
winners=0
[ "$rcA" -eq 0 ] && winners=$((winners+1))
[ "$rcB" -eq 0 ] && winners=$((winners+1))
# Exactly one ref, and exactly one winner — a bare identical-SHA no-op push would
# have let BOTH report success; the unique root commit prevents that.
refcount=$(gg -C "$A" ls-remote origin "refs/claims/issue-2" | wc -l | tr -d ' ')
if [ "$winners" -eq 1 ] && [ "$refcount" = "1" ]; then
  ok "same-base race yields exactly one winner and one claim ref (no identical-SHA double-win)"
else
  bad "expected 1 winner + 1 ref; got winners=$winners refcount=$refcount rcA=$rcA rcB=$rcB
A: $outA
B: $outB"
fi

# ===========================================================================
echo "TEST 3: legacy-guard — pre-existing issue-7-oldslug branch blocks claim 7"
# ===========================================================================
# Simulate an OLD-fleet worker that still branch-locks with issue-<N>-<slug>.
(
  cd "$A" || exit 1
  gg checkout -q -b issue-7-oldslug main
  gg commit -q --allow-empty -m "claim issue-7 someoldbox-1234-5678"
  gg push -q origin issue-7-oldslug
  gg checkout -q main
  gg branch -q -D issue-7-oldslug
)
( cd "$B" && gg fetch -q origin )
outB=$(runB claim 7); rcB=$?
if [ "$rcB" -eq 2 ] && printf '%s\n' "$outB" | grep -q 'legacy-branch-lock'; then
  ok "legacy issue-7-oldslug branch blocks a fresh claim 7 (exit 2)"
else
  bad "expected legacy-branch-lock refusal exit 2; got rc=$rcB
B: $outB"
fi

# ===========================================================================
echo "TEST 4: adopt CAS — correct --expect wins; original's re-push then loses; stale --expect refused"
# ===========================================================================
runA claim 4 >/dev/null; rcClaim=$?          # A holds issue 4
oldsha=$(ref_sha 4)
outB=$(runB adopt 4 --expect "$oldsha"); rcAdopt=$?   # B adopts with correct expect
newsha=$(ref_sha 4)
# A, the resurrected original, now finds the ref is no longer its own commit.
outAver=$(runA verify 4); rcAver=$?
# A tries to adopt back with the STALE expected sha it remembers -> refused.
outAstale=$(runA adopt 4 --expect "$oldsha"); rcStale=$?
if [ "$rcClaim" -eq 0 ] && [ "$rcAdopt" -eq 0 ] && [ "$newsha" != "$oldsha" ] \
   && printf '%s\n' "$outB" | grep -q 'CLAIM: ADOPTED' \
   && [ "$rcAver" -eq 2 ] \
   && [ "$rcStale" -eq 2 ] && printf '%s\n' "$outAstale" | grep -q 'CLAIM: ADOPT-LOST'; then
  ok "adopt CAS: correct-expect wins, original detects loss, stale-expect refused (exit 2)"
else
  bad "adopt CAS chain unexpected: rcClaim=$rcClaim rcAdopt=$rcAdopt old=$oldsha new=$newsha rcAver=$rcAver rcStale=$rcStale
adopt: $outB
verify: $outAver
stale: $outAstale"
fi

# ===========================================================================
echo "TEST 5: release refuses under an open PR (gh shim), succeeds with --force"
# ===========================================================================
runA claim 5 >/dev/null   # A holds issue 5

# gh shim that reports ONE open PR for the issue.
SHIMDIR="$T/shim-open"
mkdir -p "$SHIMDIR"
cat >"$SHIMDIR/gh" <<'SHIM'
#!/usr/bin/env bash
# Fake gh: `pr list ... --jq length` -> 1 (an open PR exists).
printf '1\n'
SHIM
chmod +x "$SHIMDIR/gh"

rc=0; outRef=$( cd "$A" && PATH="$SHIMDIR:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 5 ) || rc=$?
rcRefuse=$rc
still=$(ref_sha 5)
if [ "$rcRefuse" -eq 2 ] && printf '%s\n' "$outRef" | grep -q 'RELEASE-REFUSED' && [ -n "$still" ]; then
  ok "release refused under an open PR (exit 2, ref intact)"
else
  bad "expected RELEASE-REFUSED exit 2 with ref intact; got rc=$rcRefuse still=$still
$outRef"
fi

rc=0; outForce=$( cd "$A" && PATH="$SHIMDIR:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 5 --force ) || rc=$?
rcForce=$rc
gone=$(ref_sha 5)
if [ "$rcForce" -eq 0 ] && printf '%s\n' "$outForce" | grep -q 'RELEASED' && [ -z "$gone" ]; then
  ok "release --force deletes the ref even under an open PR (exit 0)"
else
  bad "expected RELEASED exit 0 with ref gone; got rc=$rcForce gone='$gone'
$outForce"
fi

# ===========================================================================
echo "TEST 6: re-entrancy — same machine+actor re-claiming its own ref exits 0"
# ===========================================================================
runA claim 6 >/dev/null; rc1=$?
sha1=$(ref_sha 6)
outRe=$(runA claim 6); rc2=$?
sha2=$(ref_sha 6)
if [ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ] && [ "$sha1" = "$sha2" ] \
   && printf '%s\n' "$outRe" | grep -q 're-entrant'; then
  ok "re-entrant claim by the same holder exits 0 and leaves the ref unchanged"
else
  bad "expected re-entrant exit 0, ref unchanged; got rc1=$rc1 rc2=$rc2 sha1=$sha1 sha2=$sha2
$outRe"
fi

# ===========================================================================
echo "TEST 7: verify holder identity; status renders the claim ref"
# ===========================================================================
runA verify 6 >/dev/null; rcVok=$?         # A holds 6
runB verify 6 >/dev/null; rcVbad=$?        # B does not
outStat=$( cd "$A" && CLAIM_MACHINE=machineA bash "$CLAIM" status 6 )
if [ "$rcVok" -eq 0 ] && [ "$rcVbad" -eq 2 ] \
   && printf '%s\n' "$outStat" | grep -q 'CLAIM: STATUS issue=6' \
   && printf '%s\n' "$outStat" | grep -q 'machine=machineA'; then
  ok "verify true for holder / false for other; status renders holder + machine"
else
  bad "verify/status unexpected: rcVok=$rcVok rcVbad=$rcVbad
status: $outStat"
fi

# ===========================================================================
echo "TEST 8: release fails loud when the open-PR check is unavailable (gh errors), no --force"
# ===========================================================================
# A gh whose `pr list` fails stands in for gh being absent/broken — the release
# guard must fail CLOSED (never silently treat it as "0 open PRs").
runA claim 8 >/dev/null
FAILDIR="$T/shim-fail"
mkdir -p "$FAILDIR"
cat >"$FAILDIR/gh" <<'SHIM'
#!/usr/bin/env bash
echo "gh: simulated failure" >&2
exit 1
SHIM
chmod +x "$FAILDIR/gh"
rc=0; outNoGh=$( cd "$A" && PATH="$FAILDIR:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 8 ) || rc=$?
rcNoGh=$rc
intact=$(ref_sha 8)
if [ "$rcNoGh" -eq 2 ] && printf '%s\n' "$outNoGh" | grep -q 'open-pr-check-unavailable' && [ -n "$intact" ]; then
  ok "release fails loud (exit 2) when the gh PR check errors and no --force (ref intact)"
else
  bad "expected fail-loud refusal exit 2 with ref intact; got rc=$rcNoGh intact=$intact
$outNoGh"
fi

# ===========================================================================
echo
echo "==== CLAIM-LOCK TEST SUMMARY: PASS=$PASS FAIL=$FAIL ===="
if [ "$FAIL" -eq 0 ]; then echo "RESULT: PASS"; exit 0; else echo "RESULT: FAIL"; exit 1; fi
