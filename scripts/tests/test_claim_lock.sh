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
REALGIT="$(command -v git)"   # absolute git, for the ls-remote shim in TEST 10

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# git in a throwaway identity so commits/pushes work in any sandbox.
gg() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/claim-lock-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

# claim.sh now reports the machine-local lane-directory lock's state on every HELD
# line (#3436 AC5), reading ${LANE_ROOT:-/data/lanes}/lane-<N>. Pin LANE_ROOT into
# the sandbox for the WHOLE suite so no case reads a real fleet lane directory:
# the report would otherwise depend on which lanes happen to exist on the host.
export LANE_ROOT="$T/lanes"
mkdir -p "$LANE_ROOT"

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

# gh shim that reports an open PR whose HEAD BRANCH is this issue's (issue-5-*),
# matching the head-branch guard (never free-text search).
SHIMDIR="$T/shim-open"
mkdir -p "$SHIMDIR"
cat >"$SHIMDIR/gh" <<'SHIM'
#!/usr/bin/env bash
# Fake gh: `pr list ... --json headRefName --jq '.[].headRefName'` -> one head.
printf 'issue-5-some-slug\n'
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
echo "TEST 9: infra failure (origin unreachable) reports ERROR infra (exit 1), never LOST"
# ===========================================================================
# Rename origin so BOTH the push AND the follow-up ls-remote fail — a genuine
# infra outage, NOT a race-loss. claim.sh must exit 1 with CLAIM: ERROR infra,
# and must NOT claim someone else holds a ref that nobody holds.
mv "$ORIGIN" "$ORIGIN.bak"
rc=0; outInfra=$( cd "$A" && CLAIM_MACHINE=machineA bash "$CLAIM" claim 99 ) || rc=$?
rcInfra=$rc
mv "$ORIGIN.bak" "$ORIGIN"
if [ "$rcInfra" -eq 1 ] && printf '%s\n' "$outInfra" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outInfra" | grep -q 'infra' \
   && ! printf '%s\n' "$outInfra" | grep -q 'CLAIM: LOST'; then
  ok "unreachable origin → CLAIM ERROR infra exit 1 (not a bogus LOST)"
else
  bad "expected CLAIM ERROR infra exit 1, no LOST; got rc=$rcInfra
$outInfra"
fi

# ===========================================================================
echo "TEST 10: a LANDED push whose confirm read comes back EMPTY is infra (exit 1), and names NO holder"
# ===========================================================================
# A git shim makes every ls-remote return EMPTY while push passes through, so the
# push CREATES the ref and the post-push confirm read comes back absent.
# This assertion used to pin `LOST … holder=unknown` (exit 2) — its point being that
# the verdict must never fabricate a holder out of OUR OWN sha. The no-holder half of
# that verdict was itself the bug (#2945): nobody holds the ref in this state, so exit 2
# ("you did not win, take the next item") walked a worker away from a FREE lane, and the
# rejected-push sibling already called the identical state infra. The original intent is
# preserved and strengthened — the verdict now names NO holder at all, unknown or
# otherwise, and is retryable (exit 1).
SHIMG="$T/shim-git"
mkdir -p "$SHIMG"
cat >"$SHIMG/git" <<SHIM
#!/usr/bin/env bash
for a in "\$@"; do
  if [ "\$a" = "ls-remote" ]; then exit 0; fi   # simulate: ls-remote returns nothing
done
exec "$REALGIT" "\$@"
SHIM
chmod +x "$SHIMG/git"
rc=0; outUnk=$( cd "$A" && PATH="$SHIMG:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" claim 10 ) || rc=$?
rcUnk=$rc
if [ "$rcUnk" -eq 1 ] && printf '%s\n' "$outUnk" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outUnk" | grep -q 'reason=infra' \
   && printf '%s\n' "$outUnk" | grep -q 'detail=push-accepted-but-ref-absent-on-confirm' \
   && ! printf '%s\n' "$outUnk" | grep -q 'CLAIM: LOST' \
   && ! printf '%s\n' "$outUnk" | grep -q 'holder=' \
   && [ -n "$(ref_sha 10)" ]; then
  ok "an empty confirm read over a LANDED push → ERROR infra exit 1, naming no holder (never our own commit, never LOST)"
else
  bad "expected ERROR infra push-accepted-but-ref-absent-on-confirm exit 1 with no holder named; got rc=$rcUnk ref='$(ref_sha 10)'
$outUnk"
fi

# ===========================================================================
echo "TEST 11: release PR guard matches the HEAD BRANCH exactly, not free-text"
# ===========================================================================
# gh shim returns head-branch names. A PR on a DIFFERENT issue's branch (issue-266,
# issue-99) must NOT block release of issue 11; a real issue-11-* branch must.
runA claim 11 >/dev/null
OTHERDIR="$T/shim-other-pr"
mkdir -p "$OTHERDIR"
cat >"$OTHERDIR/gh" <<'SHIM'
#!/usr/bin/env bash
# Only ever answers `pr list ... --json headRefName --jq ...` — echo unrelated heads.
printf 'issue-266-substring-trap\nissue-99-unrelated\n'
SHIM
chmod +x "$OTHERDIR/gh"
rc=0; outOther=$( cd "$A" && PATH="$OTHERDIR:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 11 ) || rc=$?
rcOther=$rc
goneOther=$(ref_sha 11)
if [ "$rcOther" -eq 0 ] && printf '%s\n' "$outOther" | grep -q 'RELEASED' && [ -z "$goneOther" ]; then
  ok "release ignores non-matching head branches (issue-266/issue-99 do not block issue 11)"
else
  bad "expected RELEASED exit 0 (unrelated PRs ignored); got rc=$rcOther gone='$goneOther'
$outOther"
fi

runA claim 11 >/dev/null   # re-claim to test the blocking case
MATCHDIR="$T/shim-match-pr"
mkdir -p "$MATCHDIR"
cat >"$MATCHDIR/gh" <<'SHIM'
#!/usr/bin/env bash
printf 'issue-11-real-work\n'
SHIM
chmod +x "$MATCHDIR/gh"
rc=0; outMatch=$( cd "$A" && PATH="$MATCHDIR:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 11 ) || rc=$?
rcMatch=$rc
intactMatch=$(ref_sha 11)
if [ "$rcMatch" -eq 2 ] && printf '%s\n' "$outMatch" | grep -q 'RELEASE-REFUSED' \
   && printf '%s\n' "$outMatch" | grep -q 'reason=open-pr' && [ -n "$intactMatch" ]; then
  ok "release refuses when a matching issue-11-* head branch has an open PR (exit 2)"
else
  bad "expected RELEASE-REFUSED open-pr exit 2 (matching head branch); got rc=$rcMatch intact=$intactMatch
$outMatch"
fi

# ===========================================================================
echo "TEST 12: verify under an unreachable origin reports ERROR infra (exit 1), not VERIFY-FAIL"
# ===========================================================================
# A holds issue 13; then origin goes away. verify must NOT conclude "you don't
# hold it" (VERIFY-FAIL exit 2) on a network blip — it must ERROR infra (exit 1).
runA claim 13 >/dev/null
mv "$ORIGIN" "$ORIGIN.bak"
rc=0; outVinfra=$( cd "$A" && CLAIM_MACHINE=machineA bash "$CLAIM" verify 13 ) || rc=$?
rcVinfra=$rc
mv "$ORIGIN.bak" "$ORIGIN"
if [ "$rcVinfra" -eq 1 ] && printf '%s\n' "$outVinfra" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outVinfra" | grep -q 'infra' \
   && ! printf '%s\n' "$outVinfra" | grep -q 'VERIFY-FAIL'; then
  ok "verify on unreachable origin → ERROR infra exit 1 (not a bogus VERIFY-FAIL)"
else
  bad "expected verify ERROR infra exit 1, no VERIFY-FAIL; got rc=$rcVinfra
$outVinfra"
fi

# ===========================================================================
echo "TEST 13: open_pr_count passes --limit 1000 to gh (no 30-PR-page under-count)"
# ===========================================================================
# gh's default page is 30; an under-count would delete a claim under an open PR.
# The shim records its args; the test asserts the release path invoked gh with
# --limit 1000.
runA claim 12 >/dev/null
ARGDIR="$T/shim-argcap"
mkdir -p "$ARGDIR"
cat >"$ARGDIR/gh" <<SHIM
#!/usr/bin/env bash
echo "\$@" >> "$T/gh-argcap.txt"
printf 'issue-12-paging-check\n'
SHIM
chmod +x "$ARGDIR/gh"
: >"$T/gh-argcap.txt"
( cd "$A" && PATH="$ARGDIR:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 12 >/dev/null 2>&1 ) || true
if grep -q -- '--limit 1000' "$T/gh-argcap.txt"; then
  ok "release invoked gh pr list with --limit 1000"
else
  bad "expected gh invoked with --limit 1000; captured args:
$(cat "$T/gh-argcap.txt")"
fi

# ===========================================================================
echo "TEST 14: push WINS but the confirm ls-remote fails → ERROR infra (exit 1), never a false LOST"
# ===========================================================================
# A git shim passes `push` through (the claim really lands) but fails every
# ls-remote of a CLAIM ref — so the post-push WIN confirmation cannot read the ref.
# That must be treated as infra (retryable, exit 1), NOT a bogus LOST on a claim we
# hold. The shim deliberately leaves the legacy-branch enumeration
# (`ls-remote --heads issue-<N>-*`) readable: since #2945 that guard fails CLOSED on
# an unreadable enumeration and returns BEFORE any push, so a blanket ls-remote
# failure would never reach the push at all (that whole-remote outage case is
# TEST 9's / claim-resume.test.sh TEST 6's).
SHIMF="$T/shim-git-lsfail"
mkdir -p "$SHIMF"
cat >"$SHIMF/git" <<SHIM
#!/usr/bin/env bash
is_ls=0; is_claims=0
for a in "\$@"; do
  [ "\$a" = "ls-remote" ] && is_ls=1
  case "\$a" in refs/claims/*) is_claims=1 ;; esac
done
# simulate: reading a CLAIM ref is unreachable (branch enumeration still works)
if [ "\$is_ls" = 1 ] && [ "\$is_claims" = 1 ]; then exit 1; fi
exec "$REALGIT" "\$@"
SHIM
chmod +x "$SHIMF/git"
rc=0; outWin=$( cd "$A" && PATH="$SHIMF:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" claim 14 ) || rc=$?
rcWin=$rc
# Sanity: the push really landed — the ref exists when read with real git.
wonRef=$(ref_sha 14)
if [ "$rcWin" -eq 1 ] && printf '%s\n' "$outWin" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outWin" | grep -q 'infra' \
   && ! printf '%s\n' "$outWin" | grep -q 'CLAIM: LOST' \
   && [ -n "$wonRef" ]; then
  ok "won push + confirm-read failure → ERROR infra exit 1 (not a false LOST; ref actually landed)"
else
  bad "expected ERROR infra exit 1 (no LOST) with ref landed; got rc=$rcWin wonRef=$wonRef
$outWin"
fi

# ===========================================================================
echo "TEST 15: adopt CAS lands but the confirm ls-remote fails → ERROR infra (exit 1), never ADOPT-LOST"
# ===========================================================================
# A holds issue 15; B adopts with the correct --expect under the ls-remote-fail
# shim (reused from TEST 14). force-with-lease carries the expected sha, so the
# CAS push lands WITHOUT an ls-remote; the post-CAS confirm read then fails →
# infra (exit 1), never a false ADOPT-LOST on a ref B actually adopted.
runA claim 15 >/dev/null
oldsha15=$(ref_sha 15)
rc=0; outAdoptInfra=$( cd "$B" && PATH="$SHIMF:$PATH" CLAIM_MACHINE=machineB bash "$CLAIM" adopt 15 --expect "$oldsha15" ) || rc=$?
rcAdoptInfra=$rc
newsha15=$(ref_sha 15)
if [ "$rcAdoptInfra" -eq 1 ] && printf '%s\n' "$outAdoptInfra" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outAdoptInfra" | grep -q 'infra' \
   && ! printf '%s\n' "$outAdoptInfra" | grep -q 'ADOPT-LOST' \
   && [ -n "$newsha15" ] && [ "$newsha15" != "$oldsha15" ]; then
  ok "adopt CAS lands + confirm-read failure → ERROR infra exit 1 (not ADOPT-LOST; ref actually adopted)"
else
  bad "expected adopt ERROR infra exit 1 (no ADOPT-LOST) with ref changed; got rc=$rcAdoptInfra old=$oldsha15 new=$newsha15
$outAdoptInfra"
fi

# ===========================================================================
echo "TEST 16: release without --force is holder-gated + CAS; --force overrides identity"
# ===========================================================================
runA claim 16 >/dev/null   # A (machineA) holds issue 16
# (a) a non-holder (machineB) releasing without --force is refused (ref intact).
rc=0; outNH=$( cd "$B" && CLAIM_MACHINE=machineB bash "$CLAIM" release 16 ) || rc=$?
rcNH=$rc; ref16a=$(ref_sha 16)
if [ "$rcNH" -eq 2 ] && printf '%s\n' "$outNH" | grep -q 'RELEASE-REFUSED' \
   && printf '%s\n' "$outNH" | grep -q 'reason=not-holder' && [ -n "$ref16a" ]; then
  ok "(a) non-holder release without --force refused (exit 2, ref intact)"
else
  bad "(a) expected not-holder refusal exit 2, ref intact; got rc=$rcNH intact=$ref16a
$outNH"
fi
# (b) the holder (machineA), no open PR, releases via CAS → RELEASED (ref gone).
NOPR="$T/shim-nopr"
mkdir -p "$NOPR"
cat >"$NOPR/gh" <<'SHIM'
#!/usr/bin/env bash
# No open PRs at all.
printf ''
SHIM
chmod +x "$NOPR/gh"
rc=0; outH=$( cd "$A" && PATH="$NOPR:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 16 ) || rc=$?
rcH=$rc; ref16b=$(ref_sha 16)
if [ "$rcH" -eq 0 ] && printf '%s\n' "$outH" | grep -q 'RELEASED' && [ -z "$ref16b" ]; then
  ok "(b) holder release (no open PR) via CAS → RELEASED exit 0 (ref gone)"
else
  bad "(b) expected RELEASED exit 0, ref gone; got rc=$rcH gone='$ref16b'
$outH"
fi
# (c) --force lets a NON-holder (machineB) delete unconditionally (reaper).
runA claim 16 >/dev/null
rc=0; outF=$( cd "$B" && CLAIM_MACHINE=machineB bash "$CLAIM" release 16 --force ) || rc=$?
rcF=$rc; ref16c=$(ref_sha 16)
if [ "$rcF" -eq 0 ] && printf '%s\n' "$outF" | grep -q 'RELEASED' && [ -z "$ref16c" ]; then
  ok "(c) --force overrides identity — non-holder reaper delete succeeds (exit 0, ref gone)"
else
  bad "(c) expected --force RELEASED exit 0, ref gone; got rc=$rcF gone='$ref16c'
$outF"
fi

# ===========================================================================
echo "TEST 17: a malicious ref name never executes a command substitution"
# ===========================================================================
# A git shim emits an ls-remote line whose refname contains \$(touch ...). If any
# code path expanded remote output unquoted, the file would appear. It must NOT.
PWNMARK="$T/claimpwn"
rm -f "$PWNMARK"
FAKESHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
PWNDIR="$T/shim-pwn"
mkdir -p "$PWNDIR"
cat >"$PWNDIR/git" <<SHIM
#!/usr/bin/env bash
for a in "\$@"; do
  if [ "\$a" = "ls-remote" ]; then
    printf '%s\trefs/claims/issue-99\$(touch $PWNMARK)-evil\n' "$FAKESHA"
    exit 0
  fi
done
exec "$REALGIT" "\$@"
SHIM
chmod +x "$PWNDIR/git"
( cd "$A" && PATH="$PWNDIR:$PATH" bash "$CLAIM" status >/dev/null 2>&1 ) || true
if [ ! -e "$PWNMARK" ]; then
  ok "status over a \$(...)-laden refname did not execute the payload"
else
  bad "SECURITY: refname command substitution EXECUTED — $PWNMARK was created"
  rm -f "$PWNMARK"
fi

# ===========================================================================
echo "TEST 18: CLAIM_MACHINE overrides holder identity (not the clone) for release"
# ===========================================================================
# A (machineA) holds issue 18. Holder identity is CLAIM_MACHINE, not the checkout:
# clone B releasing as machineB is a non-holder (refused), but clone B releasing
# as machineA IS the holder identity → it may release.
runA claim 18 >/dev/null
NOPR2="$T/shim-nopr"   # reuse the empty-PR gh shim from TEST 16 (no open PRs)
# (a) B with its OWN identity (machineB) is refused as non-holder.
rc=0; outMisId=$( cd "$B" && PATH="$NOPR2:$PATH" CLAIM_MACHINE=machineB bash "$CLAIM" release 18 ) || rc=$?
rcMisId=$rc; ref18a=$(ref_sha 18)
# (b) B impersonating A's CLAIM_MACHINE (machineA) matches the holder → releases.
rc=0; outSameId=$( cd "$B" && PATH="$NOPR2:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 18 ) || rc=$?
rcSameId=$rc; ref18b=$(ref_sha 18)
if [ "$rcMisId" -eq 2 ] && printf '%s\n' "$outMisId" | grep -q 'reason=not-holder' && [ -n "$ref18a" ] \
   && [ "$rcSameId" -eq 0 ] && printf '%s\n' "$outSameId" | grep -q 'RELEASED' && [ -z "$ref18b" ]; then
  ok "CLAIM_MACHINE drives holder identity: machineB refused, CLAIM_MACHINE=machineA released"
else
  bad "expected machineB refused (exit2, intact) then machineA released (exit0, gone); got rcMisId=$rcMisId intact=$ref18a rcSameId=$rcSameId gone='$ref18b'
mis: $outMisId
same: $outSameId"
fi

# ===========================================================================
echo "TEST 19: status skips a stray non-issue ref under refs/claims/* (smoke leftover)"
# ===========================================================================
# A leftover refs/claims/smoke-<x> (e.g. an interrupted preflight) must NOT be
# rendered as an issue row; a real issue claim alongside it still is.
runA claim 19 >/dev/null
( cd "$A" && gg push -q origin HEAD:refs/claims/smoke-stray )
statusOut=$( cd "$A" && CLAIM_MACHINE=machineA bash "$CLAIM" status )
if printf '%s\n' "$statusOut" | grep -q 'CLAIM: STATUS issue=19' \
   && ! printf '%s\n' "$statusOut" | grep -q 'smoke-stray'; then
  ok "status renders issue-19 and skips the stray refs/claims/smoke-stray ref"
else
  bad "expected issue=19 rendered and smoke-stray skipped; got:
$statusOut"
fi

# ===========================================================================
echo "TEST 20: an AUTH failure is reason=auth (never 'transient — retry'); a real transient still is"
# ===========================================================================
# Issue #2942. A box whose `gh` is authenticated but whose GIT has no credential
# helper fails every claim push with `fatal: could not read Username`. That is a
# machine-configuration fault that CANNOT self-clear, yet it was reported as
#   CLAIM: ERROR reason=infra detail=push-rejected-but-ref-absent (transient — retry)
# telling the worker to retry the one thing guaranteed never to work. The auth
# signature must produce a DISTINCT non-retryable verdict — and, per the #2665
# contract, a genuine transient must still report as the retryable infra error.
#
# Both shims fail only `push` (ls-remote passes through, as it does for a public
# repo readable anonymously) and differ ONLY in the stderr git emits.
mk_push_fail_shim() {
  # mk_push_fail_shim <dir> <stderr-line>
  mkdir -p "$1"
  cat >"$1/git" <<SHIM
#!/usr/bin/env bash
for a in "\$@"; do
  if [ "\$a" = "push" ]; then
    echo "$2" >&2
    exit 128
  fi
done
exec "$REALGIT" "\$@"
SHIM
  chmod +x "$1/git"
}

# (a) credential failure -> reason=auth, no "transient", no retry advice.
AUTHSHIM="$T/shim-git-noauth"
mk_push_fail_shim "$AUTHSHIM" "fatal: could not read Username for 'https://github.com': terminal prompts disabled"
rc=0; outAuth=$( cd "$A" && PATH="$AUTHSHIM:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" claim 20 ) || rc=$?
rcAuth=$rc
if [ "$rcAuth" -eq 1 ] && printf '%s\n' "$outAuth" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outAuth" | grep -q 'reason=auth' \
   && printf '%s\n' "$outAuth" | grep -q 'NOT retryable' \
   && ! printf '%s\n' "$outAuth" | grep -q 'transient' \
   && ! printf '%s\n' "$outAuth" | grep -q -- '— retry' \
   && ! printf '%s\n' "$outAuth" | grep -q 'CLAIM: LOST'; then
  ok "(a) unauthenticated push → CLAIM ERROR reason=auth, no transient/retry advice"
else
  bad "(a) expected reason=auth exit 1 with no transient/retry wording; got rc=$rcAuth
$outAuth"
fi
# The verdict must name the remediation, not just the fault.
if printf '%s\n' "$outAuth" | grep -q 'gh auth setup-git' \
   || printf '%s\n' "$outAuth" | grep -q 'bootstrap-agent-machine'; then
  ok "(a) auth verdict names the remediation"
else
  bad "(a) auth verdict named no remediation:
$outAuth"
fi
# It must NOT echo git's raw stderr (a remote URL can carry an embedded secret).
if ! printf '%s\n' "$outAuth" | grep -q 'terminal prompts disabled'; then
  ok "(a) auth verdict does not echo raw git stderr"
else
  bad "(a) auth verdict echoed raw git stderr (secret-leak surface):
$outAuth"
fi

# (b) genuine transient (unreachable host) -> unchanged retryable infra verdict.
NETSHIM="$T/shim-git-netfail"
mk_push_fail_shim "$NETSHIM" "fatal: unable to access 'https://github.com/x.git/': Could not resolve host: github.com"
rc=0; outNet=$( cd "$A" && PATH="$NETSHIM:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" claim 21 ) || rc=$?
rcNet=$rc
if [ "$rcNet" -eq 1 ] && printf '%s\n' "$outNet" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outNet" | grep -q 'reason=infra' \
   && printf '%s\n' "$outNet" | grep -q 'transient' \
   && ! printf '%s\n' "$outNet" | grep -q 'reason=auth'; then
  ok "(b) genuine transient still reports the retryable infra verdict (#2665 contract intact)"
else
  bad "(b) expected reason=infra transient exit 1; got rc=$rcNet
$outNet"
fi

# (c) `403 Forbidden` must NOT be treated as auth. A proxy or edge outage returns it,
# and turning a transient into a permanent stop is the one direction the #2665
# contract says never to move. (GitHub's rate-limit text is `HTTP 403`.)
F403="$T/shim-git-403"
mk_push_fail_shim "$F403" "fatal: unable to access 'https://github.com/x.git/': The requested URL returned error: 403 Forbidden"
rc=0; out403=$( cd "$A" && PATH="$F403:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" claim 24 ) || rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$out403" | grep -q 'reason=infra' \
   && ! printf '%s\n' "$out403" | grep -q 'reason=auth'; then
  ok "(c) a 403 stays a RETRYABLE transient (an edge/proxy outage is not a credential fault)"
else
  bad "(c) expected reason=infra for a 403; got rc=$rc
$out403"
fi

# (d) adopt: the CAS push is unauthenticated. The confirm read succeeds on a public
# repo, so without the auth check this reports ADOPT-LOST — blaming the lease for a
# broken machine.
runA claim 25 >/dev/null
oldsha25=$(ref_sha 25)
rc=0; outAdoptAuth=$( cd "$B" && PATH="$AUTHSHIM:$PATH" CLAIM_MACHINE=machineB bash "$CLAIM" adopt 25 --expect "$oldsha25" ) || rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$outAdoptAuth" | grep -q 'reason=auth' \
   && ! printf '%s\n' "$outAdoptAuth" | grep -q 'ADOPT-LOST'; then
  ok "(d) adopt under an auth failure → reason=auth, never ADOPT-LOST"
else
  bad "(d) expected adopt reason=auth exit 1 with no ADOPT-LOST; got rc=$rc
$outAdoptAuth"
fi

# (e) release --force (the reaper path): an unauthenticated delete is not transient.
runA claim 26 >/dev/null
rc=0; outRelAuth=$( cd "$A" && PATH="$AUTHSHIM:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" release 26 --force ) || rc=$?
ref26=$(ref_sha 26)
if [ "$rc" -eq 1 ] && printf '%s\n' "$outRelAuth" | grep -q 'reason=auth' \
   && ! printf '%s\n' "$outRelAuth" | grep -q 'transient' \
   && [ -n "$ref26" ]; then
  ok "(e) release --force under an auth failure → reason=auth (ref intact, no false RELEASED)"
else
  bad "(e) expected release reason=auth exit 1 with the ref intact; got rc=$rc ref=$ref26
$outRelAuth"
fi

# (f) smoke: the preflight's whole job is diagnosing the remote, so blaming the
# refs/claims/* namespace for a credential fault sends the operator hunting the
# wrong thing on a brand-new box.
rc=0; outSmokeAuth=$( cd "$A" && PATH="$AUTHSHIM:$PATH" CLAIM_MACHINE=machineA bash "$CLAIM" smoke 2>/dev/null ) || rc=$?
if [ "$rc" -eq 1 ] && printf '%s\n' "$outSmokeAuth" | grep -q 'SMOKE-FAIL' \
   && printf '%s\n' "$outSmokeAuth" | grep -q 'reason=auth' \
   && ! printf '%s\n' "$outSmokeAuth" | grep -q 'push-rejected'; then
  ok "(f) smoke under an auth failure → reason=auth, not 'does origin permit refs/claims/*?'"
else
  bad "(f) expected SMOKE-FAIL reason=auth; got rc=$rc
$outSmokeAuth"
fi

# (g) smoke on a remote that ACCEPTS the create and refuses the delete (#3369). It used
# to emit SMOKE-OK — whose own text says "delete verified" — after a stderr-only warning,
# so a caller could not tell a clean cycle from a stranded ref. Delete capability is
# REQUIRED: `release` deletes refs/claims/issue-<N>, so such a namespace is unusable.
DELORIGIN="$T/deleteproof.git"
gg init --bare -q "$DELORIGIN"
cat >"$DELORIGIN/hooks/pre-receive" <<'HOOK'
#!/usr/bin/env bash
zero=0000000000000000000000000000000000000000
while read -r old new ref; do
  if [ "$new" = "$zero" ]; then echo "deletion of $ref denied by policy" >&2; exit 1; fi
done
exit 0
HOOK
chmod +x "$DELORIGIN/hooks/pre-receive"
rc=0; outSmokeDel=$( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE="$DELORIGIN" bash "$CLAIM" smoke 2>/dev/null ) || rc=$?
strayDel=$(gg -C "$A" ls-remote "$DELORIGIN" 'refs/claims/smoke-*' | wc -l | tr -d ' ')
# The reason code states the OBSERVATION (a nonzero cleanup exit) and attributes NO
# cause: one exit status cannot distinguish this remote's deletion policy from a network
# drop or a post-readback auth failure, and naming one would be the affirmative-
# measurement violation this whole change exists to remove (#3369 review).
if [ "$rc" -ne 0 ] && printf '%s\n' "$outSmokeDel" | grep -q 'SMOKE-FAIL' \
   && printf '%s\n' "$outSmokeDel" | grep -q 'reason=cleanup-unverified' \
   && printf '%s\n' "$outSmokeDel" | grep -q 'no cause is attributed' \
   && printf '%s\n' "$outSmokeDel" | grep -q 'UNPROVEN' \
   && ! printf '%s\n' "$outSmokeDel" | grep -q 'SMOKE-OK'; then
  ok "(g) smoke whose cleanup delete fails → SMOKE-FAIL reason=cleanup-unverified, no cause attributed, never SMOKE-OK (refs left on that remote: $strayDel)"
else
  bad "(g) expected SMOKE-FAIL reason=cleanup-unverified attributing no cause; got rc=$rc
$outSmokeDel"
fi
# Positive control: the SAME probe against the ordinary origin still succeeds, so (g) is
# measuring the delete refusal and not a broken probe.
# Count NEW strays only: TEST 19 above deliberately leaves a `refs/claims/smoke-stray`
# on origin, so an absolute count of 0 is the wrong oracle — the property is that THIS
# probe adds none.
strayBefore=$(gg -C "$A" ls-remote origin 'refs/claims/smoke-*' | wc -l | tr -d ' ')
rc=0; outSmokeOk=$( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$CLAIM" smoke 2>/dev/null ) || rc=$?
strayAfter=$(gg -C "$A" ls-remote origin 'refs/claims/smoke-*' | wc -l | tr -d ' ')
if [ "$rc" -eq 0 ] && printf '%s\n' "$outSmokeOk" | grep -q 'SMOKE-OK' && [ "$strayAfter" = "$strayBefore" ]; then
  ok "(g) positive control: a normal remote still yields SMOKE-OK and leaves NO new stray ref"
else
  bad "(g) positive control failed: rc=$rc strays $strayBefore -> $strayAfter
$outSmokeOk"
fi

# (h) readback failure AND delete failure at once (#3369 review). The mismatch branch
# returned before reporting the delete result, so the ONE path that can leave a ref on the
# shared origin said nothing about it. Reproduced honestly: a post-receive hook removes
# whatever was just pushed, so the readback finds nothing AND the delete then fails
# ("remote ref does not exist"). The verdict must keep reason=ls-remote-mismatch (no new
# variant) and carry the cleanup-delete failure with the ls-remote check.
GHOSTORIGIN="$T/ghost.git"
gg init --bare -q "$GHOSTORIGIN"
# post-receive removes what the create just wrote -> the readback finds nothing;
# pre-receive refuses deletions (all-zeros new sha) -> the cleanup delete also fails.
# Both halves are needed: deleting an already-absent ref is a no-op SUCCESS for
# receive-pack, so the post-receive alone leaves delete_ok=1 (measured).
cat >"$GHOSTORIGIN/hooks/post-receive" <<'HOOK'
#!/usr/bin/env bash
while read -r old new ref; do git update-ref -d "$ref" 2>/dev/null || true; done
exit 0
HOOK
cat >"$GHOSTORIGIN/hooks/pre-receive" <<'HOOK'
#!/usr/bin/env bash
zero=0000000000000000000000000000000000000000
while read -r old new ref; do
  if [ "$new" = "$zero" ]; then echo "deletion of $ref denied by policy" >&2; exit 1; fi
done
exit 0
HOOK
chmod +x "$GHOSTORIGIN/hooks/post-receive" "$GHOSTORIGIN/hooks/pre-receive"
rc=0; outGhost=$( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE="$GHOSTORIGIN" bash "$CLAIM" smoke 2>/dev/null ) || rc=$?
if [ "$rc" -ne 0 ] && printf '%s\n' "$outGhost" | grep -q 'reason=ls-remote-mismatch' \
   && printf '%s\n' "$outGhost" | grep -q 'cleanup-delete=FAILED' \
   && printf '%s\n' "$outGhost" | grep -q 'git ls-remote' \
   && ! printf '%s\n' "$outGhost" | grep -q 'SMOKE-OK'; then
  ok "(h) readback mismatch + failed cleanup → one verdict naming BOTH, with the ls-remote check"
else
  bad "(h) expected reason=ls-remote-mismatch carrying cleanup-delete=FAILED; got rc=$rc
$outGhost"
fi
# Control: a readback mismatch whose cleanup SUCCEEDS must NOT claim a cleanup failure,
# or the field above would be noise rather than a signal. Same ghost remote, but the hook
# deletes only on a CREATE (new != zeros), so the probe's own delete still succeeds.
CLEANORIGIN="$T/ghost-clean.git"
gg init --bare -q "$CLEANORIGIN"
cat >"$CLEANORIGIN/hooks/post-receive" <<'HOOK'
#!/usr/bin/env bash
zero=0000000000000000000000000000000000000000
while read -r old new ref; do
  [ "$new" = "$zero" ] || git update-ref -d "$ref" 2>/dev/null || true
done
exit 0
HOOK
chmod +x "$CLEANORIGIN/hooks/post-receive"
rc=0; outGhost2=$( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE="$CLEANORIGIN" bash "$CLAIM" smoke 2>/dev/null ) || rc=$?
if [ "$rc" -ne 0 ] && printf '%s\n' "$outGhost2" | grep -q 'reason=ls-remote-mismatch' \
   && ! printf '%s\n' "$outGhost2" | grep -q 'cleanup-delete=FAILED'; then
  ok "(h) control: a mismatch whose cleanup SUCCEEDED reports no cleanup failure"
else
  bad "(h) control: cleanup-delete=FAILED reported on a successful cleanup; rc=$rc
$outGhost2"
fi

# (i) THE SMOKE REF NAME IS CONTENT-ADDRESSED, NOT AN AD-HOC NONCE (#3369 review).
# It used to be `$$-${RANDOM}-$(date -u +%s)`. Bash seeds $RANDOM from pid+time, so on
# identically provisioned machines booting simultaneously — a fleet launched from ONE AMI,
# this issue's own subject — pid, $RANDOM and a second-resolution timestamp are correlated
# rather than independent, and two boxes can pick the SAME name against the shared origin:
# a spurious push rejection, `git-push: FAILED`, and `--strict` refusing a healthy box.
# No race is provoked here (a passing race proves nothing); the PROPERTY is asserted —
# distinct names across runs, each one the sha of THIS run's claim commit, which is unique
# to its machine+pid+two-$RANDOMs+timestamp message.
strayI_before=$(gg -C "$A" ls-remote origin 'refs/claims/smoke-*' | wc -l | tr -d ' ')
rcI1=0; outI1=$( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$CLAIM" smoke 2>&1 ) || rcI1=$?
rcI2=0; outI2=$( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$CLAIM" smoke 2>&1 ) || rcI2=$?
strayI_after=$(gg -C "$A" ls-remote origin 'refs/claims/smoke-*' | wc -l | tr -d ' ')
# The name is announced in the probe's own note line, which is where an operator chasing a
# stranded ref reads it — so that is what is parsed, not an internal variable.
refI1=$(printf '%s\n' "$outI1" | grep -oE 'refs/claims/smoke-[0-9a-f]{40}' | head -1)
refI2=$(printf '%s\n' "$outI2" | grep -oE 'refs/claims/smoke-[0-9a-f]{40}' | head -1)
# Guard the guard: a 40-hex suffix could be hex-shaped by accident. It must be the OBJECT
# NAME of this run's smoke claim commit, which is the whole content-addressed claim.
objI1=$(gg -C "$A" cat-file commit "${refI1##*-}" 2>/dev/null | grep -c 'claim issue=smoke' || true)
if [ "$rcI1" -eq 0 ] && [ "$rcI2" -eq 0 ] \
   && printf '%s\n' "$outI1" | grep -q 'SMOKE-OK' && printf '%s\n' "$outI2" | grep -q 'SMOKE-OK' \
   && [ -n "$refI1" ] && [ -n "$refI2" ] && [ "$refI1" != "$refI2" ] \
   && [ "${objI1:-0}" -ge 1 ] && [ "$strayI_after" = "$strayI_before" ]; then
  ok "(i) two smoke runs on ONE origin use DISTINCT content-addressed refs (each = its own claim commit's sha), both SMOKE-OK, no strays left"
else
  bad "(i) smoke ref names are not distinct/content-addressed: rc=$rcI1/$rcI2 ref1=$refI1 ref2=$refI2 commit-match=${objI1:-0} strays $strayI_before -> $strayI_after
$outI1
$outI2"
fi

# ===========================================================================
echo "TEST 21: AC5 — every HELD line reports lane-lock=<state>; a WARNING that never changes the verdict"
# ===========================================================================
# The two locks arbitrate different things and used to know nothing about each
# other (#3436): `refs/claims/issue-<N>` is hard CROSS-machine and advisory
# LOCALLY, so a second session on one box walks into an occupied lane directory.
# `claim` now REPORTS the machine-local lock's state. The property under test is
# deliberately double-sided: the field must be PRESENT AND CORRECT, and the claim
# verdict + exit code must be BIT-FOR-BIT what they were before it existed — the
# whole risk of this feature is a warning that grows into a failure.
LANELOCK="$SCRIPT_DIR/../flow/lane-lock.sh"
# runAerr — like runA but with stderr captured to a file, since the occupant
# description (AC2) is a stderr note, not part of the verdict line.
runAerr() { local errf="$1"; shift; ( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$CLAIM" "$@" 2>"$errf" ); }

# (a) no lane directory at all — the ordinary case for a fresh claim. It must read
# as unremarkable (a distinct state, not a warning).
out21a=$(runA claim 30); rc21a=$?
if [ "$rc21a" -eq 0 ] && printf '%s\n' "$out21a" | grep -q 'CLAIM: HELD' \
   && printf '%s\n' "$out21a" | grep -q 'lane-lock=no-lane-dir'; then
  ok "(a) AC5: a claim with no lane directory reports lane-lock=no-lane-dir and still HELD (exit 0)"
else
  bad "(a) expected HELD + lane-lock=no-lane-dir exit 0; got rc=$rc21a
$out21a"
fi

# (b) the lane directory exists but nobody locked it -> free (also distinct).
mkdir -p "$LANE_ROOT/lane-31"
out21b=$(runA claim 31); rc21b=$?
if [ "$rc21b" -eq 0 ] && printf '%s\n' "$out21b" | grep -q 'CLAIM: HELD' \
   && printf '%s\n' "$out21b" | grep -q 'lane-lock=free'; then
  ok "(b) AC5: an existing but unlocked lane directory reports lane-lock=free, still HELD (exit 0)"
else
  bad "(b) expected HELD + lane-lock=free exit 0; got rc=$rc21b
$out21b"
fi

# (c) THE CASE THIS EXISTS FOR: the lane is held by a DIFFERENT LIVE PROCESS.
# A real `sleep` is the occupant, so liveness is measured against a real /proc
# entry rather than a fabricated record.
sleep 900 &
OCCUPANT_PID=$!
LANE_LOCK_PID=$OCCUPANT_PID bash "$LANELOCK" acquire 32 >/dev/null 2>&1
out21c=$(runAerr "$T/err21c" claim 32); rc21c=$?
err21c=$(cat "$T/err21c" 2>/dev/null)
ref32=$(ref_sha 32)
if [ "$rc21c" -eq 0 ] && printf '%s\n' "$out21c" | grep -q 'CLAIM: HELD' \
   && printf '%s\n' "$out21c" | grep -q 'lane-lock=occupied-alive' \
   && [ -n "$ref32" ]; then
  ok "(c) AC5: a lane held by a LIVE other pid reports lane-lock=occupied-alive and the claim is STILL GRANTED (exit 0, ref created)"
else
  bad "(c) expected HELD + lane-lock=occupied-alive exit 0 with the ref created; got rc=$rc21c ref=$ref32
$out21c"
fi
# AC2's principle: a collision diagnosed generically sends the reader to the wrong
# problem, so the note must NAME the occupant.
if printf '%s\n' "$err21c" | grep -q "holder-pid=$OCCUPANT_PID" \
   && printf '%s\n' "$err21c" | grep -q 'acquired-ts=' \
   && printf '%s\n' "$err21c" | grep -q 'age=' \
   && printf '%s\n' "$err21c" | grep -q 'OCCUPIED'; then
  ok "(c) AC5: the stderr note NAMES the occupant (pid=$OCCUPANT_PID, acquired-ts, age)"
else
  bad "(c) occupied note did not name the occupant (expected holder-pid=$OCCUPANT_PID + acquired-ts + age):
$err21c"
fi

# (d) NON-VACUITY: with lane-lock.sh unavailable the claim must still succeed and
# say so. The artifact is SUBSTITUTED in a scratch copy of the tree — there is no
# env override to point the report elsewhere, on #3312's ruling that the
# constrained party must not choose its own enforcer.
mkdir -p "$T/scratch-nolanelock/flow"
cp "$CLAIM" "$T/scratch-nolanelock/flow/claim.sh"
rc=0; out21d=$( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$T/scratch-nolanelock/flow/claim.sh" claim 34 2>/dev/null ) || rc=$?
if [ "$rc" -eq 0 ] && printf '%s\n' "$out21d" | grep -q 'CLAIM: HELD' \
   && printf '%s\n' "$out21d" | grep -q 'lane-lock=unmeasured('; then
  ok "(d) AC5 non-vacuity: with lane-lock.sh absent the claim still succeeds and reports lane-lock=unmeasured(...)"
else
  bad "(d) expected HELD + lane-lock=unmeasured(...) exit 0 with lane-lock.sh absent; got rc=$rc
$out21d"
fi

# (e) THE FIELD IS ON THE RE-ENTRANT PATH TOO. There are four HELD emit sites in
# cmd_claim; a fix applied to one of them must not pass. (a)-(c) cover the plain
# post-push win; this covers the pre-check re-entrant one.
runA claim 35 >/dev/null 2>&1
out21e=$(runA claim 35); rc21e=$?
if [ "$rc21e" -eq 0 ] && printf '%s\n' "$out21e" | grep -q 're-entrant' \
   && printf '%s\n' "$out21e" | grep -q 'lane-lock='; then
  ok "(e) AC5: the re-entrant HELD line carries lane-lock= too (not just the post-push win)"
else
  bad "(e) expected a re-entrant HELD carrying lane-lock=; got rc=$rc21e
$out21e"
fi

# (f) A SELF-HELD lane is its OWN state, not `occupied-alive` (#3436 review).
# Both mean "a live process holds this lane", but the urgency is opposite: our own
# lock is unremarkable, a peer's is the #3436 incident. Conflating them left anyone
# grepping `lane-lock=occupied-alive` in a log unable to triage, so the field --
# whose whole job IS triage -- must separate them. The lock here is taken for the
# claim process's OWN resolved identity, so `probe` reports liveness=SELF.
# `$$` (this suite's own shell) is a REAL live process, and passing the SAME pid to
# the acquire and to claim.sh's probe makes both compute the same five-component
# token -- which is what SELF means. No test-only seam: LANE_LOCK_PID is a shipped
# env var of lane-lock.sh, used here exactly as a supervisor would.
mkdir -p "$LANE_ROOT/lane-33"
LANE_LOCK_PID=$$ bash "$LANELOCK" acquire 33 >/dev/null 2>&1
out21f=$(LANE_LOCK_PID=$$ runAerr "$T/err21f" claim 33); rc21f=$?
err21f=$(cat "$T/err21f" 2>/dev/null)
if [ "$rc21f" -eq 0 ] && printf '%s\n' "$out21f" | grep -q 'CLAIM: HELD' \
   && printf '%s\n' "$out21f" | grep -q 'lane-lock=self'; then
  ok "(f) AC5: a lane held by THIS session reports lane-lock=self, still HELD (exit 0)"
else
  bad "(f) expected HELD + lane-lock=self exit 0; got rc=$rc21f
$out21f"
fi
# ...and it must NOT emit the alarming peer-occupancy note about our own lock: a
# warning fired on correct input is the warning readers learn to ignore.
if printf '%s\n' "$err21f" | grep -q 'held by THIS session' \
   && ! printf '%s\n' "$err21f" | grep -q 'ALREADY OCCUPIED'; then
  ok "(f) AC5: a self-held lane gets the THIS-session note and NOT the peer-occupancy warning"
else
  bad "(f) expected the THIS-session note without the ALREADY OCCUPIED warning; got:
$err21f"
fi

# (g) control: the field is not a constant. Four claims above measured four
# different lane states, so a hard-coded value cannot satisfy all of them, and
# `self` vs `occupied-alive` in particular cannot be one value.
s21a=$(printf '%s\n' "$out21a" | grep -o 'lane-lock=[^ ]*' | head -1)
s21b=$(printf '%s\n' "$out21b" | grep -o 'lane-lock=[^ ]*' | head -1)
s21c=$(printf '%s\n' "$out21c" | grep -o 'lane-lock=[^ ]*' | head -1)
s21f=$(printf '%s\n' "$out21f" | grep -o 'lane-lock=[^ ]*' | head -1)
uniq_states=$(printf '%s\n%s\n%s\n%s\n' "$s21a" "$s21b" "$s21c" "$s21f" | sort -u | grep -c .)
if [ -n "$s21a" ] && [ -n "$s21b" ] && [ -n "$s21c" ] && [ -n "$s21f" ] \
   && [ "$uniq_states" -eq 4 ]; then
  ok "(g) AC5 control: the four lane states are four DISTINCT values ($s21a / $s21b / $s21c / $s21f)"
else
  bad "(g) expected four distinct lane-lock states; got '$s21a' '$s21b' '$s21c' '$s21f' (uniq=$uniq_states)"
fi

# ===========================================================================
echo "TEST 22: AC6 — the legacy-branch refusal splits THREE ways by what the evidence PROVES, and none prints a runnable resume"
# ===========================================================================
# Measured on #3393: a slice shipped, the claim ref was released correctly and the
# board went back to Ready — proper finalize behaviour — then work resumed on the
# SAME branch for 20+ commits holding no claim. `claim` refused with
# reason=legacy-branch-lock and pointed at the abandoned-lane procedure, which is
# exactly the wrong advice when the lane is yours and live. The states have
# OPPOSITE remedies, so they must be textually distinct.
#
# ROUND-1 REVIEW FIX, and case (h) is the regression: three evidence rungs used to
# collapse onto reason=released-then-resumed, whose text says "the branch above is
# almost certainly YOUR OWN" and points at adoption. Only ONE of them proves that.
# A live LOCAL lane-lock holder proves a live process on THIS BOX owns the lane, NOT
# that it is THIS SESSION — and that is exactly #3436's scenario, so the refusal told
# a reader to adopt the claim for an actively-worked PEER lane (the inverse hazard).
# A lane DIRECTORY on the issue's branch proves less again. So: SELF ->
# released-then-resumed; live local peer -> lane-occupied-by-live-peer; everything
# else, worktree-only evidence INCLUDED -> legacy-branch-lock.
push_legacy_branch() {   # <issue> — an issue-<N>-* branch on origin, no claim ref
  (
    cd "$A" || exit 1
    gg checkout -q -b "issue-$1-slug" main
    gg commit -q --allow-empty -m "work on issue $1"
    gg push -q origin "issue-$1-slug"
    gg checkout -q main
    gg branch -q -D "issue-$1-slug"
  )
}

# (a) NO local evidence -> the unchanged generic verdict.
push_legacy_branch 40
out22a=$(runA claim 40 2>/dev/null); rc22a=$?
if [ "$rc22a" -eq 2 ] && printf '%s\n' "$out22a" | grep -q 'reason=legacy-branch-lock' \
   && printf '%s\n' "$out22a" | grep -q 'claim-ref=free' \
   && printf '%s\n' "$out22a" | grep -q 'lane-evidence=none' \
   && ! printf '%s\n' "$out22a" | grep -q 'released-then-resumed'; then
  ok "(a) AC6: an issue-40-* branch with NO local lane evidence keeps reason=legacy-branch-lock (exit 2, lane-evidence=none)"
else
  bad "(a) expected reason=legacy-branch-lock exit 2 with no released-then-resumed; got rc=$rc22a
$out22a"
fi

# (b) THIS SESSION holds the lane lock (SELF, the strongest evidence) -> the new
# verdict. LANE_LOCK_PID is lane-lock.sh's own documented env, used identically for
# the acquire and the claim, so the token really is this session's — no test-only
# seam is introduced in either script.
push_legacy_branch 41
LANE_LOCK_PID=$OCCUPANT_PID bash "$LANELOCK" acquire 41 >/dev/null 2>&1
rc=0; out22b=$( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin LANE_LOCK_PID=$OCCUPANT_PID bash "$CLAIM" claim 41 2>/dev/null ) || rc=$?
rc22b=$rc
if [ "$rc22b" -eq 2 ] && printf '%s\n' "$out22b" | grep -q 'reason=released-then-resumed' \
   && printf '%s\n' "$out22b" | grep -q 'claim-ref=free' \
   && printf '%s\n' "$out22b" | grep -q 'lane-evidence=lane-lock-self' \
   && ! printf '%s\n' "$out22b" | grep -q 'reason=legacy-branch-lock' \
   && ! printf '%s\n' "$out22b" | grep -q 'reason=lane-occupied-by-live-peer'; then
  ok "(b) AC6: the lane lock held by THIS session yields reason=released-then-resumed (exit 2, lane-evidence=lane-lock-self)"
else
  bad "(b) expected reason=released-then-resumed exit 2 with lane-evidence=lane-lock-self; got rc=$rc22b
$out22b"
fi

# (c) The prose must send the reader to the RIGHT procedure: it must say the
# abandoned-lane test does NOT apply, and must name verify as the first step.
if printf '%s\n' "$out22b" | grep -q 'should-reap' \
   && printf '%s\n' "$out22b" | grep -qi 'does not apply' \
   && printf '%s\n' "$out22b" | grep -q "'verify' subcommand"; then
  ok "(c) AC6: the released-then-resumed text names the abandoned-lane procedure as NOT applicable and points at verify"
else
  bad "(c) released-then-resumed text did not disclaim the abandoned-lane procedure / name verify:
$out22b"
fi

# (f) ROUND-1 REVIEW: WORKTREE-ONLY EVIDENCE IS REPORTED AND DECIDES NOTHING.
# A lane directory on this issue's branch used to be evidence enough for
# released-then-resumed — i.e. an unattributed directory got a reader told the branch
# was theirs, with an adoption pointer attached. A directory existing says nobody is
# necessarily in it, and the branch says nothing about WHICH session put it there. So
# the verdict falls back to the generic one AND the observation still shows up in
# `lane-evidence=`, which is what keeps the rung visible without it deciding.
push_legacy_branch 42
mkdir -p "$LANE_ROOT/lane-42"
(
  cd "$LANE_ROOT/lane-42" || exit 1
  gg init -q .
  echo x >x.txt; gg add x.txt; gg commit -qm x
  gg checkout -q -b issue-42-slug
)
out22f=$(runA claim 42 2>/dev/null); rc22f=$?
if [ "$rc22f" -eq 2 ] && printf '%s\n' "$out22f" | grep -q 'reason=legacy-branch-lock' \
   && printf '%s\n' "$out22f" | grep -q 'lane-evidence=lane-worktree-branch' \
   && ! printf '%s\n' "$out22f" | grep -q 'released-then-resumed' \
   && ! printf '%s\n' "$out22f" | grep -q 'lane-occupied-by-live-peer'; then
  ok "(f) AC6: a local lane checkout on issue-42-* (no lane lock) keeps reason=legacy-branch-lock, with the worktree still REPORTED in lane-evidence="
else
  bad "(f) expected reason=legacy-branch-lock with lane-evidence=lane-worktree-branch...; got rc=$rc22f
$out22f"
fi

# (g) NEGATIVE control for (f), so the branch match is proven to do work: the same
# lane directory shape on an UNRELATED branch must fall back to the generic verdict.
# Fail-closed direction: an unread/unmatched signal is NO evidence.
push_legacy_branch 43
mkdir -p "$LANE_ROOT/lane-43"
(
  cd "$LANE_ROOT/lane-43" || exit 1
  gg init -q .
  echo x >x.txt; gg add x.txt; gg commit -qm x
)
out22g=$(runA claim 43 2>/dev/null); rc22g=$?
if [ "$rc22g" -eq 2 ] && printf '%s\n' "$out22g" | grep -q 'reason=legacy-branch-lock' \
   && printf '%s\n' "$out22g" | grep -q 'lane-evidence=none' \
   && ! printf '%s\n' "$out22g" | grep -q 'released-then-resumed'; then
  ok "(g) AC6 control: a lane checkout NOT on issue-43-* is no evidence at all (lane-evidence=none) — so (f)'s branch match is proven to do work"
else
  bad "(g) expected the generic legacy-branch-lock verdict for a non-matching branch; got rc=$rc22g
$out22g"
fi

# (h) THE ROUND-1 REGRESSION CASE. Evidence (b): a LIVE LOCAL holder that is NOT this
# session. The lane lock is acquired for the sleeper's pid but the claim runs WITHOUT
# LANE_LOCK_PID, so lane-lock `verify` fails (the token is not ours) and rung (a)
# cannot fire; the probe still reports ALIVE with the holder machine equal to ours and
# a holder token DIFFERENT from ours, which is (b). The lane directory here is the one
# lane-lock.sh created — NOT a git checkout — so (c) cannot fire either, which is what
# makes this case attribute the verdict to (b) alone.
#
# THIS IS THE CASE THAT WAS PREVIOUSLY WRONG: it returned reason=released-then-resumed,
# telling a session that a lane an ACTIVELY-WORKED PEER holds was "almost certainly
# YOUR OWN" and pointing it at claim adoption — the inverse of the right advice, and
# the exact collision #3436 exists to prevent. It must now be its own verdict.
push_legacy_branch 44
LANE_LOCK_PID=$OCCUPANT_PID bash "$LANELOCK" acquire 44 >/dev/null 2>&1
out22h=$(runA claim 44 2>/dev/null); rc22h=$?
if [ "$rc22h" -eq 2 ] && printf '%s\n' "$out22h" | grep -q 'reason=lane-occupied-by-live-peer' \
   && printf '%s\n' "$out22h" | grep -q 'lane-evidence=lane-lock-alive-local-peer' \
   && printf '%s\n' "$out22h" | grep -q "lane-holder-pid=$OCCUPANT_PID" \
   && ! printf '%s\n' "$out22h" | grep -q 'reason=released-then-resumed' \
   && ! printf '%s\n' "$out22h" | grep -q 'reason=legacy-branch-lock'; then
  ok "(h) AC6 regression: a LIVE LOCAL lane-lock holder that is NOT this session is reason=lane-occupied-by-live-peer (exit 2), naming the holder pid — NOT released-then-resumed"
else
  bad "(h) expected reason=lane-occupied-by-live-peer exit 2 with lane-evidence=lane-lock-alive-local-peer and lane-holder-pid=$OCCUPANT_PID; got rc=$rc22h
$out22h"
fi

# (h2) ...and its PROSE must send the reader somewhere else again: not to adoption,
# and not to the abandonment tests, which describe a lane nobody is in.
if printf '%s\n' "$out22h" | grep -q 'DO NOT ADOPT THE CLAIM REF' \
   && printf '%s\n' "$out22h" | grep -q 'should-reap' \
   && printf '%s\n' "$out22h" | grep -qi 'does not apply' \
   && printf '%s\n' "$out22h" | grep -qi 'find that session'; then
  ok "(h) AC6: the live-peer text refuses BOTH other remedies by name (no adoption, no abandonment procedure) and says to find that session"
else
  bad "(h) live-peer text did not refuse adoption + the abandonment procedure:
$out22h"
fi

kill "$OCCUPANT_PID" 2>/dev/null || true
wait "$OCCUPANT_PID" 2>/dev/null || true

# (d) THE #2945 RULING, and a test is the only thing that keeps it true: NO refusal
# may print a runnable resume command. The readers are agents that execute printed
# remediations literally, and an older-fleet worker holds only the BRANCH, so a
# printed empty-lease adopt WOULD succeed against a live lane. All FOUR refusal
# outputs above are checked, the live-peer one included — it is the one where a
# printed adopt would do the most damage.
d22_bad=""
for v in a b f h; do
  eval "d22_out=\"\$out22$v\""
  if printf '%s\n' "$d22_out" | grep -q 'claim.sh adopt' \
     || printf '%s\n' "$d22_out" | grep -q -- '--expect none'; then
    d22_bad="$d22_bad ($v)"
  fi
done
if [ -z "$d22_bad" ]; then
  ok "(d) AC6: no refusal prints a runnable resume command (no 'claim.sh adopt', no '--expect none') — checked on all four"
else
  bad "(d) a refusal printed a runnable resume command (#2945 violation):$d22_bad
generic:   $out22a
resumed:   $out22b
worktree:  $out22f
live-peer: $out22h"
fi

# (e) DISTINCTNESS control over ALL THREE reason tokens: a two-verdict implementation
# fails here, which is precisely what the round-1 defect was. The tokens are compared
# as VALUES, never searched for as substrings (`legacy-branch-lock` is a substring of
# nothing else here, but `released-then-resumed` vs a hypothetical
# `released-then-resumed-peer` would defeat a substring test).
r22a=$(printf '%s\n' "$out22a" | grep -o 'reason=[^ ]*' | head -1)
r22b=$(printf '%s\n' "$out22b" | grep -o 'reason=[^ ]*' | head -1)
r22h=$(printf '%s\n' "$out22h" | grep -o 'reason=[^ ]*' | head -1)
uniq_reasons=$(printf '%s\n%s\n%s\n' "$r22a" "$r22b" "$r22h" | sort -u | grep -c .)
if [ -n "$r22a" ] && [ -n "$r22b" ] && [ -n "$r22h" ] && [ "$uniq_reasons" -eq 3 ]; then
  ok "(e) AC6 control: the three refusals carry THREE DISTINCT reason tokens ($r22a / $r22b / $r22h)"
else
  bad "(e) expected three distinct reason tokens; got '$r22a' '$r22b' '$r22h' (uniq=$uniq_reasons)"
fi

# ===========================================================================
echo
echo "==== CLAIM-LOCK TEST SUMMARY: PASS=$PASS FAIL=$FAIL ===="
if [ "$FAIL" -eq 0 ]; then echo "RESULT: PASS"; exit 0; else echo "RESULT: FAIL"; exit 1; fi
