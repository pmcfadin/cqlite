#!/usr/bin/env bash
#
# Regression tests for the claim.sh RESUME path (issue #2945).
#
# The legacy-branch guard used to refuse a claim whenever ANY refs/heads/issue-<N>-*
# branch existed on origin, with no reachable escape hatch: its documented
# "all-ours -> no block" re-entrancy compared the BRANCH TIP against claim-commit
# trailers a real work branch can never carry, so it was dead code. `adopt` could
# not substitute either (it demanded a --expect <old-sha> CAS against an EXISTING
# ref). Workers hand-crafted claim commits to get past it — twice in one day.
#
# These tests pin the sanctioned replacement: `adopt <N> --expect none --reason <why>`
# (git's EMPTY LEASE = "the ref must not exist"), the safety direction (a HELD ref
# still refuses), a REAL two-machine race on that path (git arbitrates, exactly one
# winner), and the guard's fail-CLOSED behaviour on an enumeration outage
# (#2677 item 2 — an outage must never read as "no legacy branch").
#
# Fast + hermetic: a mktemp BARE repo stands in for origin plus two clones playing
# machines A and B (each overriding CLAIM_MACHINE). No network, no GitHub, no gh
# (neither `claim` nor `adopt` uses it). No wall-clock assertions — every verdict is
# a git-ref state or an exit code.
#
# Run standalone:  bash scripts/flow/tests/claim-resume.test.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLAIM="$SCRIPT_DIR/../claim.sh"
REALGIT="$(command -v git)"   # absolute git, for the ls-remote shim in TEST 6

PASS=0
FAIL=0
fail() { echo "  ✗ $*"; FAIL=$((FAIL+1)); }
ok()   { echo "  ✓ $*"; PASS=$((PASS+1)); }

# git in a throwaway identity so commits/pushes work in any sandbox.
g() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/claim-resume-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

ORIGIN="$T/origin.git"
A="$T/A"
B="$T/B"
g init --bare -q "$ORIGIN"
g clone -q "$ORIGIN" "$A" 2>/dev/null
g clone -q "$ORIGIN" "$B" 2>/dev/null
(
  cd "$A" || exit 1
  echo seed >seed.txt
  g add seed.txt
  g commit -qm seed
  g push -q -u origin main
)
( cd "$B" && g fetch -q origin )

# runA/runB — claim.sh from clone A/B as a distinct machine. The function EXIT CODE
# is claim.sh's, so callers use `out=$(runA ...); rc=$?`.
runA() { ( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$CLAIM" "$@" ); }
runB() { ( cd "$B" && CLAIM_MACHINE=machineB CLAIM_REMOTE=origin bash "$CLAIM" "$@" ); }

ref_sha()      { g -C "$A" ls-remote origin "refs/claims/issue-$1" | awk '{print $1}' | head -1; }
ref_count()    { g -C "$A" ls-remote origin "refs/claims/issue-$1" | wc -l | tr -d ' '; }
branch_exists() { [ -n "$(g -C "$A" ls-remote --heads origin "$1" | awk '{print $1}')" ]; }

# push_work_branch <branch> [msg] — an ORDINARY work branch on origin: cut from
# main, an ordinary commit on top. Its tip carries NO claim trailers, which is
# exactly why the old tip-based re-entrancy could never fire.
push_work_branch() {
  local branch="$1" msg="${2:-ordinary work commit}"
  (
    cd "$A" || exit 1
    g checkout -q -b "$branch" main
    g commit -q --allow-empty -m "$msg"
    g push -q origin "$branch"
    g checkout -q main
    g branch -q -D "$branch"
  )
}

# ===========================================================================
echo "TEST 1: FREE claim ref + foreign-tip issue-<N>-* branch — claim refuses WITH the remediation, adopt --expect none SUCCEEDS"
# ===========================================================================
push_work_branch "issue-2001-owner-approved-spec" "docs(#2001): OpenSpec change approved by owner"
( cd "$B" && g fetch -q origin )
[ -z "$(ref_sha 2001)" ] && ok "precondition: refs/claims/issue-2001 is FREE" \
  || fail "precondition broken: a claim ref already exists for 2001"

outClaim=$(runB claim 2001); rcClaim=$?
if [ "$rcClaim" -eq 2 ] && printf '%s\n' "$outClaim" | grep -q 'reason=legacy-branch-lock'; then
  ok "claim still refuses (exit 2) while the branch stands"
else
  fail "expected legacy-branch-lock refusal exit 2; got rc=$rcClaim
$outClaim"
fi
# Option 3 of the issue: the refusal must be ACTIONABLE, not a bare LOST — a bare
# LOST is what sent two workers into hand-crafted claim-commit pushes.
if printf '%s\n' "$outClaim" | grep -q 'adopt 2001 --expect none --reason' \
   && printf '%s\n' "$outClaim" | grep -q 'issue-2001-owner-approved-spec' \
   && printf '%s\n' "$outClaim" | grep -q 'claim-ref=free'; then
  ok "refusal names the exact remediation command, the blocking branch, and claim-ref=free"
else
  fail "refusal is not actionable (missing remediation/branch/claim-ref=free):
$outClaim"
fi
[ "$(printf '%s\n' "$outClaim" | grep -c 'CLAIM:')" = "1" ] \
  && ok "refusal stays ONE CLAIM: line (single-line output contract)" \
  || fail "refusal spans multiple CLAIM: lines:
$outClaim"

outAdopt=$(runB adopt 2001 --expect none --reason "resume of #1883: owner-approved OpenSpec change lives on the branch"); rcAdopt=$?
adoptSha=$(ref_sha 2001)
if [ "$rcAdopt" -eq 0 ] && printf '%s\n' "$outAdopt" | grep -q 'CLAIM: ADOPTED' \
   && printf '%s\n' "$outAdopt" | grep -q 'from=none' && [ -n "$adoptSha" ]; then
  ok "adopt --expect none acquires the FREE ref (exit 0, ADOPTED, ref created)"
else
  fail "expected ADOPTED exit 0 with a ref; got rc=$rcAdopt ref='$adoptSha'
$outAdopt"
fi
runB verify 2001 >/dev/null 2>&1; rcVerify=$?
[ "$rcVerify" -eq 0 ] && ok "the adopter now VERIFIES as holder (exit 0)" \
  || fail "expected verify exit 0 for the adopter, got $rcVerify"
runA verify 2001 >/dev/null 2>&1; rcVerifyOther=$?
[ "$rcVerifyOther" -eq 2 ] && ok "the other machine does NOT verify as holder (exit 2)" \
  || fail "expected verify exit 2 for a non-holder, got $rcVerifyOther"
branch_exists "issue-2001-owner-approved-spec" \
  && ok "the resumed work branch is left untouched on origin" \
  || fail "the resume path deleted/moved the work branch"

# Criterion 1: the command records WHO took it and WHY.
statusOut=$( cd "$B" && CLAIM_MACHINE=machineB bash "$CLAIM" status 2001 )
if printf '%s\n' "$statusOut" | grep -q 'machine=machineB' \
   && printf '%s\n' "$statusOut" | grep -q 'reason=resume-of-#1883:-owner-approved-OpenSpec-change-lives-on-the-branch'; then
  ok "the claim record carries who (machine=machineB) AND why (sanitized one-token reason)"
else
  fail "claim record missing holder and/or reason:
$statusOut"
fi
[ "$(printf '%s\n' "$statusOut" | grep -c 'CLAIM: STATUS')" = "1" ] \
  && ok "a multi-word reason stays ONE parseable status line" \
  || fail "reason sanitization leaked whitespace into the record:
$statusOut"

# ===========================================================================
echo "TEST 2: HELD claim ref + branch present — the resume path is still REFUSED (exit 2, holder keeps it)"
# ===========================================================================
push_work_branch "issue-2002-active-effort"
( cd "$B" && g fetch -q origin )
runA claim 2002 >/dev/null 2>&1   # blocked by the branch guard, so take it via the resume path
runA adopt 2002 --expect none --reason "machineA is actively working this" >/dev/null 2>&1; rcHold=$?
heldSha=$(ref_sha 2002)
[ "$rcHold" -eq 0 ] && [ -n "$heldSha" ] && ok "machineA holds refs/claims/issue-2002" \
  || fail "could not set up a HELD ref (rc=$rcHold ref='$heldSha')"

outSteal=$(runB adopt 2002 --expect none --reason "second machine tries the resume path"); rcSteal=$?
stillSha=$(ref_sha 2002)
if [ "$rcSteal" -eq 2 ] && printf '%s\n' "$outSteal" | grep -q 'CLAIM: ADOPT-LOST' \
   && printf '%s\n' "$outSteal" | grep -q 'holder-machine=machineA' \
   && [ "$stillSha" = "$heldSha" ]; then
  ok "a second machine's empty-lease adopt is REFUSED (exit 2) and the holder's ref is unchanged"
else
  fail "expected ADOPT-LOST exit 2 with the ref intact; got rc=$rcSteal held=$heldSha now=$stillSha
$outSteal"
fi
runA verify 2002 >/dev/null 2>&1; rcHolderStill=$?
[ "$rcHolderStill" -eq 0 ] && ok "the original holder still verifies after the refused attempt" \
  || fail "holder lost its claim to a refused resume (verify rc=$rcHolderStill)"

# ===========================================================================
echo "TEST 3: two machines RACE the resume path — exactly one winner (git arbitrates)"
# ===========================================================================
push_work_branch "issue-2003-resumable"
( cd "$B" && g fetch -q origin )
# Real competing pushes: both machines are released from a FIFO barrier and run the
# empty-lease adopt concurrently against the same origin. Nothing is mocked — the
# remote's ref update is the arbiter.
mkfifo "$T/gate-a" "$T/gate-b"
( head -1 <"$T/gate-a" >/dev/null 2>&1
  runA adopt 2003 --expect none --reason "racing machineA" >"$T/race-a.out" 2>&1
  echo "$?" >"$T/race-a.rc" ) &
pidA=$!
( head -1 <"$T/gate-b" >/dev/null 2>&1
  runB adopt 2003 --expect none --reason "racing machineB" >"$T/race-b.out" 2>&1
  echo "$?" >"$T/race-b.rc" ) &
pidB=$!
( echo go >"$T/gate-a" ) &
writerA=$!
( echo go >"$T/gate-b" ) &
writerB=$!
wait "$pidA" "$pidB" 2>/dev/null
# Never `wait` on the fifo writers: a writer blocks until its reader opens, so a
# child that died before opening would hang the suite. The racers are done here,
# so any still-blocked writer is simply killed.
kill "$writerA" "$writerB" 2>/dev/null || true
rcRaceA="$(cat "$T/race-a.rc" 2>/dev/null || echo missing)"
rcRaceB="$(cat "$T/race-b.rc" 2>/dev/null || echo missing)"
winners=0
[ "$rcRaceA" = "0" ] && winners=$((winners+1))
[ "$rcRaceB" = "0" ] && winners=$((winners+1))
raceRef=$(ref_sha 2003)
raceRefs=$(ref_count 2003)
adoptedLines=$(cat "$T/race-a.out" "$T/race-b.out" 2>/dev/null | grep -c 'CLAIM: ADOPTED')
if [ "$winners" -eq 1 ] && [ "$raceRefs" = "1" ] && [ "$adoptedLines" = "1" ] && [ -n "$raceRef" ]; then
  ok "concurrent empty-lease adopts: exactly one exit-0 ADOPTED winner, exactly one claim ref"
else
  fail "expected exactly one winner and one ref; got winners=$winners refs=$raceRefs adopted-lines=$adoptedLines rcA=$rcRaceA rcB=$rcRaceB
A: $(cat "$T/race-a.out" 2>/dev/null)
B: $(cat "$T/race-b.out" 2>/dev/null)"
fi
# The loser must never report success. (It reports ADOPT-LOST when it reads the
# winner's ref, or a RETRYABLE infra ERROR if its push was rejected before the
# winner's create landed — never ADOPTED, and never a bogus "nobody holds it" win.)
if [ "$rcRaceA" = "0" ]; then loserOut="$T/race-b.out"; else loserOut="$T/race-a.out"; fi
if ! grep -q 'CLAIM: ADOPTED' "$loserOut" 2>/dev/null; then
  ok "the losing machine never printed ADOPTED"
else
  fail "the losing machine reported ADOPTED — double-claim:
$(cat "$loserOut")"
fi
# And the surviving ref really belongs to the winner, not a torn state.
winnerVerify=2
if [ "$rcRaceA" = "0" ]; then runA verify 2003 >/dev/null 2>&1; winnerVerify=$?
elif [ "$rcRaceB" = "0" ]; then runB verify 2003 >/dev/null 2>&1; winnerVerify=$?; fi
[ "$winnerVerify" -eq 0 ] && ok "the winner verifies as the holder of the single surviving ref" \
  || fail "the race winner does not verify as holder (rc=$winnerVerify)"

# ===========================================================================
echo "TEST 4: --expect fail-closed — empty '' is a usage error; only the literal 'none' opts into the empty lease"
# ===========================================================================
# An unset shell variable (`--expect \"\$sha\"` with sha unset) must NEVER silently
# turn a compare-and-swap into a create.
outEmpty=$(runB adopt 2004 --expect "" 2>&1); rcEmpty=$?
noRef=$(ref_sha 2004)
if [ "$rcEmpty" -eq 64 ] && [ -z "$noRef" ]; then
  ok "adopt --expect '' is a usage error (exit 64) and creates nothing"
else
  fail "expected exit 64 and no ref for an empty --expect; got rc=$rcEmpty ref='$noRef'
$outEmpty"
fi
outJunk=$(runB adopt 2004 --expect "HEAD~1" 2>&1); rcJunk=$?
[ "$rcJunk" -eq 64 ] && ok "a non-hex, non-'none' --expect is a usage error (exit 64)" \
  || fail "expected exit 64 for --expect HEAD~1; got rc=$rcJunk
$outJunk"

# ===========================================================================
echo "TEST 5: adopt --expect none REQUIRES --reason (the record must say why)"
# ===========================================================================
outNoWhy=$(runB adopt 2005 --expect none 2>&1); rcNoWhy=$?
noRef5=$(ref_sha 2005)
if [ "$rcNoWhy" -eq 64 ] && [ -z "$noRef5" ] && printf '%s\n' "$outNoWhy" | grep -q -- '--reason'; then
  ok "adopt --expect none without --reason is a usage error (exit 64), nothing acquired"
else
  fail "expected exit 64 demanding --reason with no ref created; got rc=$rcNoWhy ref='$noRef5'
$outNoWhy"
fi
# An ALL-ZERO --expect is git's own "must not exist": same intent as `none`, so it
# takes the same AUDITED route rather than a quiet unrecorded create. (Verified on
# the real origin: an all-zero lease DOES create the ref.)
ZERO=0000000000000000000000000000000000000000
outZeroNoWhy=$(runB adopt 2005 --expect "$ZERO" 2>&1); rcZeroNoWhy=$?
zeroRef=$(ref_sha 2005)
if [ "$rcZeroNoWhy" -eq 64 ] && [ -z "$zeroRef" ]; then
  ok "an all-zero --expect also demands --reason (exit 64) — no unaudited create-with-no-record"
else
  fail "expected exit 64 and no ref for an all-zero --expect without --reason; got rc=$rcZeroNoWhy ref='$zeroRef'
$outZeroNoWhy"
fi
outZero=$(runB adopt 2005 --expect "$ZERO" --reason "all-zero lease with a recorded why"); rcZero=$?
if [ "$rcZero" -eq 0 ] && printf '%s\n' "$outZero" | grep -q 'CLAIM: ADOPTED' && [ -n "$(ref_sha 2005)" ]; then
  ok "an all-zero --expect WITH --reason acquires the free ref (exit 0, recorded)"
else
  fail "expected ADOPTED exit 0 for an all-zero --expect with --reason; got rc=$rcZero
$outZero"
fi

# ===========================================================================
echo "TEST 6: #2677 item 2 — a legacy-branch enumeration OUTAGE fails CLOSED (ERROR infra, exit 1), never an all-clear"
# ===========================================================================
# A git shim fails ONLY `ls-remote --heads` (the guard's enumeration) and passes
# everything else through, so the claim push WOULD succeed. The old code mapped that
# failure to "no legacy branches" and granted the claim; it must now be UNKNOWN.
push_work_branch "issue-2006-invisible-to-the-guard"
SHIMH="$T/shim-heads-fail"
mkdir -p "$SHIMH"
cat >"$SHIMH/git" <<SHIM
#!/usr/bin/env bash
saw_ls=0; saw_heads=0
for a in "\$@"; do
  [ "\$a" = "ls-remote" ] && saw_ls=1
  [ "\$a" = "--heads" ]   && saw_heads=1
done
if [ "\$saw_ls" = 1 ] && [ "\$saw_heads" = 1 ]; then exit 128; fi
exec "$REALGIT" "\$@"
SHIM
chmod +x "$SHIMH/git"
outOutage=$( cd "$B" && PATH="$SHIMH:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin bash "$CLAIM" claim 2006 2>&1 ); rcOutage=$?
grantedRef=$(ref_sha 2006)
if [ "$rcOutage" -eq 1 ] && printf '%s\n' "$outOutage" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outOutage" | grep -q 'infra' \
   && ! printf '%s\n' "$outOutage" | grep -q 'CLAIM: LOST' \
   && [ -z "$grantedRef" ]; then
  ok "guard enumeration outage → ERROR infra exit 1, no claim ref granted (not a bogus LOST either)"
else
  fail "expected ERROR infra exit 1 with NO ref created; got rc=$rcOutage ref='$grantedRef'
$outOutage"
fi

# ===========================================================================
echo "TEST 7: no tip-based re-entrancy survives — even a claim-shaped branch tip blocks (the dead 'all-ours' hatch is gone)"
# ===========================================================================
# The old guard tried to exempt a branch whose TIP looked like our own claim commit.
# That exemption is deliberately gone: branch tips are not the lock, so a tip that
# happens to carry claim trailers must NOT grant a claim. The resume is explicit.
push_work_branch "issue-2007-tip-looks-like-a-claim" \
  "claim issue=2007 machine=machineB pid=1 actor=flow ts=2026-07-26T00:00:00Z nonce=x"
( cd "$B" && g fetch -q origin )
outTip=$(runB claim 2007); rcTip=$?
tipRef=$(ref_sha 2007)
if [ "$rcTip" -eq 2 ] && printf '%s\n' "$outTip" | grep -q 'reason=legacy-branch-lock' && [ -z "$tipRef" ]; then
  ok "a claim-shaped branch tip does not grant a claim — the guard blocks and points at the resume command"
else
  fail "expected legacy-branch-lock refusal exit 2 with no ref; got rc=$rcTip ref='$tipRef'
$outTip"
fi

# ===========================================================================
echo "TEST 8: unchanged happy path — free ref, no legacy branch → plain claim still wins"
# ===========================================================================
outPlain=$(runA claim 2008); rcPlain=$?
plainRef=$(ref_sha 2008)
if [ "$rcPlain" -eq 0 ] && printf '%s\n' "$outPlain" | grep -q 'CLAIM: HELD' && [ -n "$plainRef" ]; then
  ok "a normal claim (no legacy branch) is unaffected by the guard rework"
else
  fail "expected CLAIM: HELD exit 0; got rc=$rcPlain ref='$plainRef'
$outPlain"
fi

echo ""
echo "================  claim-resume (#2945): $PASS passed, $FAIL failed  ================"
[ "$FAIL" -eq 0 ]
