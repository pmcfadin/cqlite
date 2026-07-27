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
# still refuses), its RE-ENTRANCY (a retry after a confirm blip must not abandon an
# issue we hold), a REAL two-machine race on that path, the guard's fail-CLOSED
# behaviour on an enumeration outage (#2677 item 2), and the IDENTITY-FORGERY
# hardening of the two user-controlled fields (--reason, --actor) that land in the
# very commit message the holder-identity parser reads.
#
# WRITES ARE WITNESSED, NOT INFERRED: a `post-receive` hook on the bare origin logs
# every ACCEPTED ref update ("<ref> <old> <new>"), so tests assert how many writes
# the SERVER applied and from what old value. Counting refs afterwards cannot do
# that — an exact ref is structurally <=1, and every claimant reads its verdict back
# AFTER all the pushes, so last-writer-wins satisfies a ref count.
#
# WHERE THE EMPTY LEASE IS ACTUALLY PROVEN (measured, mutation-verified):
# replacing `--force-with-lease=<ref>:` with a plain `git push --force` is caught
# DETERMINISTICALLY by TEST 2/TEST 10 (a second claimant whose ref advertisement is
# FRESH — it saw the holder's ref — sends that sha as the update's old value, so
# --force overwrites it: the hook log shows TWO applied updates and the holder loses
# its claim). It is NOT reliably caught by the concurrent race in TEST 3: when both
# claimants' advertisements say "absent", each sends an all-zero old value and git's
# own protocol-level stale-info check rejects the second push regardless of the
# lease. So TEST 3 pins the CONCURRENCY invariants (exactly one winner, exactly one
# accepted create, the loser fails loudly) and TEST 2/10 are the LEASE oracle — do
# not "simplify" either one into the other.
#
# Fast + hermetic: a mktemp BARE repo stands in for origin plus two clones playing
# machines A and B (each overriding CLAIM_MACHINE). No network, no GitHub; `gh` is
# stubbed only where `release` consults it. No wall-clock assertions — every verdict
# is a git-ref state, a receive-hook record, or an exit code.
#
# Run standalone:  bash scripts/flow/tests/claim-resume.test.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLAIM="$SCRIPT_DIR/../claim.sh"
REALGIT="$(command -v git)"   # absolute git, for the shims in TESTS 6/9/10

PASS=0
FAIL=0
SKIP=0
fail() { echo "  ✗ $*"; FAIL=$((FAIL+1)); }
ok()   { echo "  ✓ $*"; PASS=$((PASS+1)); }
# skip — a WITNESS whose host dependency is missing. Loud, counted, and reported in
# the footer, but NOT a failure: this suite runs in the gate of record, where a
# missing hi-res clock is an environment fact, not a defect (the repo's SKIP-aware
# convention). Never use it for an assertion the host CAN make.
skip() { echo "  ⊘ SKIP: $*"; SKIP=$((SKIP+1)); }
# diag — a measurement worth printing that is NOT a verdict.
diag() { echo "  · $*"; }

# git in a throwaway identity so commits/pushes work in any sandbox.
g() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/claim-resume-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

ORIGIN="$T/origin.git"
A="$T/A"
B="$T/B"
HOOKLOG="$T/accepted-updates.log"
ZEROS=0000000000000000000000000000000000000000
g init --bare -q "$ORIGIN"

# post-receive fires ONLY for ref updates the remote actually APPLIED, so this log
# is ground truth for "how many writes did the server accept, and from what old
# value" — the property a race test must assert.
cat >"$ORIGIN/hooks/post-receive" <<HOOK
#!/usr/bin/env bash
while read -r old new ref; do
  printf '%s %s %s\n' "\$ref" "\$old" "\$new" >>"$HOOKLOG"
done
HOOK
chmod +x "$ORIGIN/hooks/post-receive"
: >"$HOOKLOG"

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

# `gh` stub: `release` (without --force) refuses when it cannot check for an open
# PR, and the real gh cannot talk to a local bare repo. The stub answers "no open
# PRs" so the holder-release round trip is exercised, not the gh-missing branch
# (which test_claim_lock.sh already covers).
GHSHIM="$T/shim-gh"
mkdir -p "$GHSHIM"
cat >"$GHSHIM/gh" <<'GH'
#!/usr/bin/env bash
for a in "$@"; do [ "$a" = "list" ] && exit 0; done
exit 1
GH
chmod +x "$GHSHIM/gh"

# runA/runB — claim.sh from clone A/B as a distinct machine. The function EXIT CODE
# is claim.sh's, so callers use `out=$(runA ...); rc=$?`.
runA() { ( cd "$A" && CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$CLAIM" "$@" ); }
runB() { ( cd "$B" && CLAIM_MACHINE=machineB CLAIM_REMOTE=origin bash "$CLAIM" "$@" ); }
# …with the gh stub on PATH (release only).
runA_gh() { ( cd "$A" && PATH="$GHSHIM:$PATH" CLAIM_MACHINE=machineA CLAIM_REMOTE=origin bash "$CLAIM" "$@" ); }
runB_gh() { ( cd "$B" && PATH="$GHSHIM:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin bash "$CLAIM" "$@" ); }

ref_sha()      { g -C "$A" ls-remote origin "refs/claims/issue-$1" | awk '{print $1}' | head -1; }
ref_count()    { g -C "$A" ls-remote origin "refs/claims/issue-$1" | wc -l | tr -d ' '; }
branch_exists() { [ -n "$(g -C "$A" ls-remote --heads origin "$1" | awk '{print $1}')" ]; }

# accepted_updates <issue> — how many ref updates the SERVER applied to this claim
# ref (from the post-receive log).
accepted_updates() { grep -c "^refs/claims/issue-$1 " "$HOOKLOG" 2>/dev/null || true; }
# accepted_olds <issue> — the OLD value of each applied update (a create is all-zeros).
accepted_olds()    { awk -v r="refs/claims/issue-$1" '$1==r {print $2}' "$HOOKLOG" 2>/dev/null; }

# line_field <line> <key> — pull `<key>=<value>` out of a CLAIM: line using the same
# exact-key/first-match token semantics claim.sh's own msg_field uses, so assertions
# test the RECORD, not a verbatim rendering of the whole line.
line_field() { printf '%s' "$1" | tr ' ' '\n' | grep -m1 "^$2=" | sed "s/^$2=//"; }

# --- THE TWO WORK-BRANCH PREMISES ------------------------------------------
# The name of each fixture states which premise a test encodes, because the liveness
# gate treats them DIFFERENTLY and the difference is the whole point of TEST 17: a tip
# with own commits is AGE-JUDGEABLE, a tip without any is INDETERMINATE.
#
# push_branch_with_own_commit <branch> [msg] [tip-date] — an ORDINARY work branch on
# origin: cut from main, WITH an ordinary commit of its own on top. Its tip carries NO
# claim trailers, which is exactly why the old tip-based re-entrancy could never fire.
#
# <tip-date> is the tip's committer date and DEFAULTS TO A LONG-STALE FIXED DATE,
# because the refusal now only advertises the resume hatch for a branch whose tip is
# older than the reap threshold (#2945 review: a FRESH tip is an older-fleet worker
# mid-implementation, which has no open PR yet either). A fixed literal date keeps the
# fixture deterministic — no wall-clock arithmetic in an assertion.
STALE_TIP_DATE='2020-03-01T12:00:00Z'
push_branch_with_own_commit() {
  local branch="$1" msg="${2:-ordinary work commit}" when="${3:-$STALE_TIP_DATE}"
  (
    cd "$A" || exit 1
    g checkout -q -b "$branch" main
    GIT_AUTHOR_DATE="$when" GIT_COMMITTER_DATE="$when" g commit -q --allow-empty -m "$msg"
    g push -q origin "$branch"
    g checkout -q main
    g branch -q -D "$branch"
  )
}

# push_branch_without_own_commits <branch> — THE PRODUCTION SHAPE AT ACTIVATION TIME:
# `flow-activate` runs `git worktree add -b issue-<N>-<slug> origin/main` and pushes the
# branch immediately, so the tip IS origin/main's tip and the branch has no commits of
# its own until PR time. Its committer date is therefore main's, not the worker's — the
# premise under which the tip-age liveness signal is VACUOUS (#2945 round 5).
push_branch_without_own_commits() {
  local branch="$1"
  (
    cd "$A" || exit 1
    g fetch -q origin
    g push -q origin "refs/remotes/origin/main:refs/heads/$branch"
  )
}

# advance_main_with_stale_tip — move origin's DEFAULT branch to a long-stale-dated tip,
# i.e. "main has been quiet longer than the reap threshold" (overnight is routine). This
# is what made the vacuous tip-age signal dangerous: a commit-less branch inherits that
# date and reads as reapable. Called once, by TEST 17.
advance_main_with_stale_tip() {
  (
    cd "$A" || exit 1
    g fetch -q origin
    g checkout -q main
    g reset -q --hard origin/main
    GIT_AUTHOR_DATE="$STALE_TIP_DATE" GIT_COMMITTER_DATE="$STALE_TIP_DATE" \
      g commit -q --allow-empty -m "main has been quiet since $STALE_TIP_DATE"
    g push -q origin main
  )
}

# push_machine_claim <ref> <issue> <ts> — a supervisor-authored liveness ref;
# push_raw_liveness_ref <ref> <msg> — the same NAMESPACE with an arbitrary (legacy) message.
# (`refs/machine-claims/<machine>` / `refs/heartbeats/<machine>`, #2655) naming
# <issue>, with <ts> as its recorded timestamp. Same root-commit shape
# claim-heartbeat.sh writes, so claim.sh's liveness scan reads a REAL ref, not a mock.
push_raw_liveness_ref() {
  local ref="$1" msg="$2"
  (
    cd "$A" || exit 1
    local tree commit
    tree=$(g hash-object -t tree --stdin </dev/null)
    commit=$(g commit-tree "$tree" -m "$msg")
    g push -q --force origin "${commit}:${ref}"
  )
}
push_machine_claim() {
  local ref="$1" issue="$2" ts="$3"
  (
    cd "$A" || exit 1
    local tree commit
    tree=$(g hash-object -t tree --stdin </dev/null)
    commit=$(GIT_AUTHOR_DATE="$ts" GIT_COMMITTER_DATE="$ts" \
      g commit-tree "$tree" -m "claim issue=${issue} machine=machineC pid=1 ts=${ts}")
    g push -q --force origin "${commit}:${ref}"
  )
}
now_ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# --- ONE SHARED start signal for the concurrency rounds --------------------
# The racers must be released TOGETHER. The earlier shape used TWO independent
# FIFOs whose writers were launched sequentially, so each racer was released the
# moment ITS OWN writer connected: machine A could finish its whole adopt before
# B even started, and every assertion in the round still held under full
# serialization — a "race" test that could not detect that it never raced.
# Now both children announce readiness and then spin on ONE shared flag file the
# parent creates only after BOTH are ready.
race_reset() { rm -f "$T/go" "$T/ready-a" "$T/ready-b"; }
# race_wait_go <ready-file> — announce readiness, then poll the shared flag. The
# poll SLEEPS: a bare `while ...; do :; done` busy-spin burns a core per racer while
# competing with the very git work the round is timing, and this suite runs inside the
# gate (whose components share the box) — the racers must wait cheaply, not fight the
# thing they are measuring. 2ms is far below the git push it gates (tens of ms).
race_wait_go() { : >"$1"; while [ ! -e "$T/go" ]; do sleep 0.002; done; }
# race_release — bounded wait for BOTH readiness marks, then flip the one flag.
race_release() {
  local spins=0
  while [ ! -e "$T/ready-a" ] || [ ! -e "$T/ready-b" ]; do
    spins=$((spins + 1))
    if [ "$spins" -gt 1000 ]; then echo "  ! barrier: a racer never signalled ready" >&2; break; fi
    sleep 0.01
  done
  : >"$T/go"
}

# now_ns — a hi-res clock, so the race rounds carry POSITIVE EVIDENCE that they
# raced (overlapping windows) instead of only asserting invariants that also hold
# under serialization. GNU date has %N; BSD/macOS date does not (it emits a
# literal 'N'), hence the perl/python3 fallbacks. No hi-res clock at all is a loud
# SKIP of the witness below (a missing host dependency, not a defect) — the race
# INVARIANTS still run and still assert.
NS_IMPL=none
nsprobe=$(date -u +%s%N 2>/dev/null || true)
if [ -n "$nsprobe" ] && [ -z "${nsprobe//[0-9]/}" ] && [ "${#nsprobe}" -ge 16 ]; then
  NS_IMPL=date
elif command -v perl >/dev/null 2>&1 && perl -MTime::HiRes -e 'exit 0' 2>/dev/null; then
  NS_IMPL=perl
elif command -v python3 >/dev/null 2>&1; then
  NS_IMPL=python3
fi
now_ns() {
  case "$NS_IMPL" in
    date)    date -u +%s%N ;;
    perl)    perl -MTime::HiRes -e 'printf "%.0f\n", Time::HiRes::time() * 1000000000' ;;
    python3) python3 -c 'import time; print(time.time_ns())' ;;
    *)       printf '0\n' ;;
  esac
}
# read_ns <file> — the recorded timestamp, or 0 when unusable.
read_ns() {
  local v
  v=$(cat "$1" 2>/dev/null || true)
  if [ -n "$v" ] && [ -z "${v//[0-9]/}" ]; then printf '%s\n' "$v"; else printf '0\n'; fi
}

# ===========================================================================
echo "TEST 1: FREE claim ref + foreign-tip issue-<N>-* branch — claim refuses WITH the remediation, adopt --expect none SUCCEEDS"
# ===========================================================================
push_branch_with_own_commit "issue-2001-owner-approved-spec" "docs(#2001): OpenSpec change approved by owner"
( cd "$B" && g fetch -q origin )
[ -z "$(ref_sha 2001)" ] && ok "precondition: refs/claims/issue-2001 is FREE" \
  || fail "precondition broken: a claim ref already exists for 2001"

# runB_gh: the gh stub reports NO open PR, i.e. a demonstrably orphaned endgame —
# the only state in which the escape hatch may be advertised (see TEST 15).
outClaim=$(runB_gh claim 2001); rcClaim=$?
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
   && printf '%s\n' "$outClaim" | grep -q 'claim-ref=free' \
   && printf '%s\n' "$outClaim" | grep -q 'open-prs=0'; then
  ok "refusal names the exact remediation command, the blocking branch, claim-ref=free, and open-prs=0"
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
# The server applied exactly ONE update, and it was a CREATE (old = all-zeros).
if [ "$(accepted_updates 2001)" = "1" ] && [ "$(accepted_olds 2001)" = "$ZEROS" ]; then
  ok "the resume was a single server-accepted CREATE (old=all-zeros), not an overwrite"
else
  fail "expected 1 accepted create for issue-2001; got $(accepted_updates 2001) update(s), olds='$(accepted_olds 2001)'"
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

# Criterion 1: the command records WHO took it and WHY. Assert PROPERTIES of the
# record (it re-extracts as one whitespace-free token that still carries the
# operator's words), not a verbatim sanitized string — a format-coupled literal
# would break on any future sanitizer tweak without any behaviour regressing.
statusOut=$( cd "$B" && CLAIM_MACHINE=machineB bash "$CLAIM" status 2001 )
statusMachine=$(line_field "$statusOut" machine)
statusReason=$(line_field "$statusOut" reason)
if [ "$statusMachine" = "machineB" ] && [ -n "$statusReason" ] \
   && [ "$statusReason" = "${statusReason%%[[:space:]]*}" ] \
   && printf '%s' "$statusReason" | grep -q 'resume' \
   && printf '%s' "$statusReason" | grep -q '1883'; then
  ok "the record round-trips as who=machineB + a single whitespace-free reason token carrying the operator's words"
else
  fail "claim record missing holder and/or a well-formed reason (machine='$statusMachine' reason='$statusReason'):
$statusOut"
fi
if [ "$(printf '%s\n' "$statusOut" | grep -c 'CLAIM: STATUS issue=2001 ')" = "1" ] \
   && printf '%s\n' "$statusOut" | grep -q 'reason='; then
  ok "a multi-word reason stays ONE parseable status ROW that still reports reason="
else
  fail "reason sanitization leaked whitespace into the record (or the row lost reason=):
$statusOut"
fi

# Round trip: a resume-adopted claim must be RELEASABLE by its holder. `release`
# (no --force) is holder-gated AND CAS-deleted, so if the resume record ever broke
# identity parsing the issue would be permanently unreleasable.
outRelease=$(runB_gh release 2001); rcRelease=$?
if [ "$rcRelease" -eq 0 ] && printf '%s\n' "$outRelease" | grep -q 'CLAIM: RELEASED' \
   && [ -z "$(ref_sha 2001)" ]; then
  ok "the resume-adopted claim releases cleanly by its holder (exit 0, ref gone)"
else
  fail "expected RELEASED exit 0 with the ref gone; got rc=$rcRelease ref='$(ref_sha 2001)'
$outRelease"
fi
outAfter=$(runA_gh claim 2001); rcAfter=$?
if [ "$rcAfter" -eq 2 ] && printf '%s\n' "$outAfter" | grep -q 'reason=legacy-branch-lock'; then
  ok "after the release the surviving work branch still guards a plain claim (exit 2)"
else
  fail "expected the branch guard to still refuse a plain claim; got rc=$rcAfter
$outAfter"
fi

# ===========================================================================
echo "TEST 2: HELD claim ref + branch present — the resume path is still REFUSED (exit 2, holder keeps it)"
# ===========================================================================
push_branch_with_own_commit "issue-2002-active-effort"
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
# THE lease oracle (see the header): machineB's advertisement SAW the holder's ref,
# so its push carries that sha as the update's old value. `--force-with-lease=<ref>:`
# demands all-zeros → the server refuses and applies NOTHING. A plain `--force`
# would be ACCEPTED here, giving two applied updates and evicting the holder — which
# is exactly how this assertion kills that mutant, deterministically.
if [ "$(accepted_updates 2002)" = "1" ] && [ "$(accepted_olds 2002)" = "$ZEROS" ]; then
  ok "the refused resume produced NO server-accepted write (still exactly one applied update: the holder's create)"
else
  fail "expected exactly 1 applied create for issue-2002; got $(accepted_updates 2002) update(s), olds='$(accepted_olds 2002)'"
fi
runA verify 2002 >/dev/null 2>&1; rcHolderStill=$?
[ "$rcHolderStill" -eq 0 ] && ok "the original holder still verifies after the refused attempt" \
  || fail "holder lost its claim to a refused resume (verify rc=$rcHolderStill)"

# ===========================================================================
echo "TEST 3: two machines RACE the resume path — exactly one SERVER-ACCEPTED create (git arbitrates), 5 rounds"
# ===========================================================================
# Real competing pushes: both machines are released by ONE SHARED start signal and
# run the empty-lease adopt concurrently against the same origin. Nothing is mocked
# — the remote's ref update is the arbiter, and the post-receive log is the witness.
# Repeated over distinct issue ids because a single interleaving proves little.
#
# AND THE ROUNDS PROVE THEY RACED: each racer records a hi-res timestamp immediately
# before and after its adopt (the push is the bulk of that call), and a MAJORITY of the
# rounds must show the two windows OVERLAPPING. Every other assertion here also holds
# under full serialization, so without this witness a barrier regression is invisible —
# which is exactly how a non-racing barrier survived two reviews.
#
# WHY >=1 ROUND AND NOT ALL 5 (NOR A MAJORITY): overlap is derived from PROCESS
# SCHEDULING, and this suite is a gate-of-record component that runs alongside a parallel
# component pool (and on macOS CI). A racer can be descheduled long enough for its window
# to miss the other's, which is a scheduling fact, not a defect — a fatal all-5 or
# majority threshold makes the gate red for no bug (#2945 review). >=1 still kills the
# regression the witness exists for, because a serializing barrier fails STRUCTURALLY:
# it yields ZERO overlapping rounds, never one (measured — see the mutation note in this
# file's header). EVERY non-overlapping round is still printed as a diagnostic, so a
# drift from 5/5 toward 1/5 stays visible long before it becomes a failure.
raceRounds=0; raceOk=0; raceAtomic=0; raceLoser=0; raceWinnerVerified=0; raceDiag=""
raceOverlapped=0; ovMin=""; ovMax=""; ovDiag=""
for id in 2101 2102 2103 2104 2105; do
  raceRounds=$((raceRounds+1))
  push_branch_with_own_commit "issue-${id}-resumable"
  ( cd "$B" && g fetch -q origin )
  race_reset
  ( race_wait_go "$T/ready-a"
    now_ns >"$T/race-a.t0"
    runA adopt "$id" --expect none --reason "racing machineA" >"$T/race-a.out" 2>&1
    echo "$?" >"$T/race-a.rc"
    now_ns >"$T/race-a.t1" ) &
  pidA=$!
  ( race_wait_go "$T/ready-b"
    now_ns >"$T/race-b.t0"
    runB adopt "$id" --expect none --reason "racing machineB" >"$T/race-b.out" 2>&1
    echo "$?" >"$T/race-b.rc"
    now_ns >"$T/race-b.t1" ) &
  pidB=$!
  race_release
  wait "$pidA" "$pidB" 2>/dev/null
  # Concurrency witness: [t0,t1] per racer must intersect. Reported in the verdict
  # so the log carries the measured overlap, not just a boolean.
  t0a=$(read_ns "$T/race-a.t0"); t1a=$(read_ns "$T/race-a.t1")
  t0b=$(read_ns "$T/race-b.t0"); t1b=$(read_ns "$T/race-b.t1")
  if [ "$t0a" -gt 0 ] && [ "$t1a" -gt 0 ] && [ "$t0b" -gt 0 ] && [ "$t1b" -gt 0 ]; then
    ovStart="$t0a"; [ "$t0b" -gt "$ovStart" ] && ovStart="$t0b"
    ovEnd="$t1a";   [ "$t1b" -lt "$ovEnd" ]   && ovEnd="$t1b"
    ovNs=$((ovEnd - ovStart))
    if [ "$ovNs" -gt 0 ]; then
      raceOverlapped=$((raceOverlapped+1))
      [ -n "$ovMin" ] && [ "$ovMin" -le "$ovNs" ] || ovMin="$ovNs"
      [ -n "$ovMax" ] && [ "$ovMax" -ge "$ovNs" ] || ovMax="$ovNs"
    else
      ovDiag="$ovDiag
  round $id: the two adopt windows did NOT overlap (gap $((0 - ovNs))ns) — that round proves nothing about concurrency"
    fi
  else
    ovDiag="$ovDiag
  round $id: no usable hi-res timestamps (NS_IMPL=$NS_IMPL) — cannot witness concurrency"
  fi
  rcRaceA="$(cat "$T/race-a.rc" 2>/dev/null || echo missing)"
  rcRaceB="$(cat "$T/race-b.rc" 2>/dev/null || echo missing)"
  winners=0
  [ "$rcRaceA" = "0" ] && winners=$((winners+1))
  [ "$rcRaceB" = "0" ] && winners=$((winners+1))
  raceRef=$(ref_sha "$id")
  adoptedLines=$(cat "$T/race-a.out" "$T/race-b.out" 2>/dev/null | grep -c 'CLAIM: ADOPTED')
  if [ "$winners" -eq 1 ] && [ "$(ref_count "$id")" = "1" ] && [ "$adoptedLines" = "1" ] && [ -n "$raceRef" ]; then
    raceOk=$((raceOk+1))
  else
    raceDiag="$raceDiag
  round $id: winners=$winners refs=$(ref_count "$id") adopted-lines=$adoptedLines rcA=$rcRaceA rcB=$rcRaceB
  A: $(cat "$T/race-a.out" 2>/dev/null)
  B: $(cat "$T/race-b.out" 2>/dev/null)"
  fi
  # The concurrency invariant, from the SERVER's own record: exactly ONE update was
  # applied to the claim ref and it was a create (old=all-zeros) — so no claimant
  # ever overwrote another's create, and the surviving ref is not a torn/second write.
  # (The lease itself is proven in TEST 2/10 — see the header for why a both-see-
  # absent race cannot discriminate it.)
  updates=$(accepted_updates "$id")
  olds=$(accepted_olds "$id")
  if [ "$updates" = "1" ] && [ "$olds" = "$ZEROS" ]; then
    raceAtomic=$((raceAtomic+1))
  else
    raceDiag="$raceDiag
  round $id: server accepted $updates update(s) to refs/claims/issue-$id, olds='$olds' (want 1 create)"
  fi
  # The loser must fail LOUDLY: never ADOPTED, and never a vacuous silent exit —
  # exit 2 with ADOPT-LOST (it read the winner's ref) or exit 1 with ERROR infra
  # (its push was rejected before the winner's create was visible).
  if [ "$rcRaceA" = "0" ]; then loserOut="$T/race-b.out"; loserRc="$rcRaceB"; else loserOut="$T/race-a.out"; loserRc="$rcRaceA"; fi
  loserText="$(cat "$loserOut" 2>/dev/null)"
  loserVerdict=1
  printf '%s\n' "$loserText" | grep -q 'CLAIM: ADOPTED' && loserVerdict=0
  if printf '%s\n' "$loserText" | grep -q 'CLAIM: ADOPT-LOST'; then :
  elif printf '%s\n' "$loserText" | grep -q 'CLAIM: ERROR' && printf '%s\n' "$loserText" | grep -q 'infra'; then :
  else loserVerdict=0
  fi
  case "$loserRc" in 1 | 2) : ;; *) loserVerdict=0 ;; esac
  if [ "$loserVerdict" -eq 1 ]; then
    raceLoser=$((raceLoser+1))
  else
    raceDiag="$raceDiag
  round $id: loser rc=$loserRc verdict not in {ADOPT-LOST, ERROR infra}: $loserText"
  fi
  # And the surviving ref really belongs to the winner, not a torn state.
  winnerVerify=2
  if [ "$rcRaceA" = "0" ]; then runA verify "$id" >/dev/null 2>&1; winnerVerify=$?
  elif [ "$rcRaceB" = "0" ]; then runB verify "$id" >/dev/null 2>&1; winnerVerify=$?; fi
  [ "$winnerVerify" -eq 0 ] && raceWinnerVerified=$((raceWinnerVerified+1))
done
[ "$raceOk" -eq "$raceRounds" ] \
  && ok "concurrent empty-lease adopts: exactly one exit-0 ADOPTED winner, every round ($raceRounds/$raceRounds)" \
  || fail "expected one winner per round; $raceOk/$raceRounds rounds clean$raceDiag"
[ "$raceAtomic" -eq "$raceRounds" ] \
  && ok "the SERVER accepted exactly one create per raced ref ($raceAtomic/$raceRounds) — the update is atomic, not last-writer-wins" \
  || fail "atomicity witness failed: $raceAtomic/$raceRounds rounds had a single all-zeros create$raceDiag"
[ "$raceLoser" -eq "$raceRounds" ] \
  && ok "the loser fails loudly every round (exit 1|2 with ADOPT-LOST or ERROR infra, never ADOPTED)" \
  || fail "loser contract failed in $((raceRounds - raceLoser))/$raceRounds rounds$raceDiag"
[ "$raceWinnerVerified" -eq "$raceRounds" ] \
  && ok "the winner verifies as the holder of the single surviving ref, every round" \
  || fail "the race winner did not verify as holder in $((raceRounds - raceWinnerVerified))/$raceRounds rounds$raceDiag"
# The barrier itself, measured. Without a hi-res clock the witness is unmeasurable on
# this host — a loud SKIP of the WITNESS ONLY (the invariants above already asserted),
# never a failure and never a silent omission.
[ -n "$ovDiag" ] && diag "concurrency witness, non-overlapping rounds:$ovDiag"
ovNeeded=1
diag "concurrency witness: $raceOverlapped/$raceRounds rounds overlapped (fatal below >=$ovNeeded)"
if [ "$NS_IMPL" = none ]; then
  skip "no hi-res clock (date %N / perl Time::HiRes / python3) — the racers' overlap could not be MEASURED on this host; the race invariants above still ran"
else
  ok "hi-res clock available to witness concurrency (NS_IMPL=$NS_IMPL)"
  if [ "$raceOverlapped" -ge "$ovNeeded" ]; then
    ok "the two racers' adopt windows OVERLAP in at least one round ($raceOverlapped/$raceRounds, need >=$ovNeeded; overlap min=$((${ovMin:-0} / 1000000))ms max=$((${ovMax:-0} / 1000000))ms, min=${ovMin:-0}ns) — they really ran concurrently"
  else
    fail "NO round ($raceOverlapped/$raceRounds) had overlapping adopt windows (need >=$ovNeeded) — a serializing barrier is the only thing that produces zero$ovDiag"
  fi
fi

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
# A hex --expect that is not a FULL object name is a CALLER BUG, and both of its
# shapes used to misreport: a short all-zero value ('0', '00') slipped into the
# all-zero branch and became a silent CREATE instead of a compare-and-swap, and a
# TRUNCATED sha built a lease git cannot resolve, so the push failed and the confirm
# read said ADOPT-LOST — a race-loss verdict for a usage error.
lenRc=""
for badExpect in 0 00 abc123 0000000000000000000000000000000000000; do
  runB adopt 2004 --expect "$badExpect" --reason "truncated sha from a caller bug" >/dev/null 2>&1
  lenRc="$lenRc $?"
done
if [ "$lenRc" = " 64 64 64 64" ] && [ -z "$(ref_sha 2004)" ] && [ "$(accepted_updates 2004)" = "0" ]; then
  ok "a short/truncated hex --expect (0, 00, abc123, 39 zeros) is a usage error (exit 64) — never a create, never ADOPT-LOST"
else
  fail "expected 64 from every short --expect with nothing created; got rcs='$lenRc' ref='$(ref_sha 2004)' updates=$(accepted_updates 2004)"
fi
# A non-numeric issue is a usage error on every subcommand (no ref namespace games).
argRc=""
for sub in "claim 20x4" "verify 20x4" "release 20x4" "adopt 20x4 --expect none --reason why"; do
  # shellcheck disable=SC2086  # deliberate word-split of the fixed arg list
  runB $sub >/dev/null 2>&1
  argRc="$argRc $?"
done
[ "$argRc" = " 64 64 64 64" ] \
  && ok "a non-numeric issue number is a usage error (exit 64) on claim/verify/release/adopt" \
  || fail "expected 64 from every subcommand for a non-numeric issue; got rcs='$argRc'"
# A valid-hex but WRONG --expect against a FREE ref must NOT create the ref: CAS-on-
# absent stays a refusal, so no refactor can quietly route it into the empty lease.
WRONG=1111111111111111111111111111111111111111
outWrong=$(runB adopt 2044 --expect "$WRONG" --reason "cas against a ref that is not there"); rcWrong=$?
if [ "$rcWrong" -eq 2 ] && printf '%s\n' "$outWrong" | grep -q 'CLAIM: ADOPT-LOST' \
   && printf '%s\n' "$outWrong" | grep -q 'actual=<gone>' && [ -z "$(ref_sha 2044)" ]; then
  ok "a wrong-sha CAS on a FREE ref refuses (exit 2, actual=<gone>) and creates nothing"
else
  fail "expected ADOPT-LOST exit 2 with actual=<gone> and no ref; got rc=$rcWrong ref='$(ref_sha 2044)'
$outWrong"
fi

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
outBlankWhy=$(runB adopt 2005 --expect none --reason "" 2>&1); rcBlankWhy=$?
if [ "$rcBlankWhy" -eq 64 ] && [ -z "$(ref_sha 2005)" ]; then
  ok "an EMPTY --reason '' is also a usage error (exit 64) — an unset shell variable cannot slip through"
else
  fail "expected exit 64 for --reason ''; got rc=$rcBlankWhy ref='$(ref_sha 2005)'
$outBlankWhy"
fi
# An ALL-ZERO --expect is git's own "must not exist": same intent as `none`, so it
# takes the same AUDITED route rather than a quiet unrecorded create. (Verified on
# the real origin: an all-zero lease DOES create the ref.)
outZeroNoWhy=$(runB adopt 2005 --expect "$ZEROS" 2>&1); rcZeroNoWhy=$?
zeroRef=$(ref_sha 2005)
if [ "$rcZeroNoWhy" -eq 64 ] && [ -z "$zeroRef" ]; then
  ok "an all-zero --expect also demands --reason (exit 64) — no unaudited create-with-no-record"
else
  fail "expected exit 64 and no ref for an all-zero --expect without --reason; got rc=$rcZeroNoWhy ref='$zeroRef'
$outZeroNoWhy"
fi
outZero=$(runB adopt 2005 --expect "$ZEROS" --reason "all-zero lease with a recorded why"); rcZero=$?
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
push_branch_with_own_commit "issue-2006-invisible-to-the-guard"
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
push_branch_with_own_commit "issue-2007-tip-looks-like-a-claim" \
  "claim issue=2007 machine=machineB pid=1 actor=flow ts=2026-07-26T00:00:00Z nonce=x"
( cd "$B" && g fetch -q origin )
outTip=$(runB_gh claim 2007); rcTip=$?
tipRef=$(ref_sha 2007)
if [ "$rcTip" -eq 2 ] && printf '%s\n' "$outTip" | grep -q 'reason=legacy-branch-lock' && [ -z "$tipRef" ]; then
  ok "a claim-shaped branch tip does not grant a claim — the guard blocks (no ref granted)"
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

# ===========================================================================
echo "TEST 9: the empty-lease push FAILING while the ref is ABSENT is retryable infra (exit 1), never a lost race"
# ===========================================================================
# #2677 direction for the resume path: nobody holds the ref, so a rejected push is
# an infrastructure failure. A shim fails ONLY `push` and lets every read through.
SHIMP="$T/shim-push-fail"
mkdir -p "$SHIMP"
cat >"$SHIMP/git" <<SHIM
#!/usr/bin/env bash
for a in "\$@"; do [ "\$a" = "push" ] && exit 128; done
exec "$REALGIT" "\$@"
SHIM
chmod +x "$SHIMP/git"
outPushFail=$( cd "$B" && PATH="$SHIMP:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin \
  bash "$CLAIM" adopt 2009 --expect none --reason "push blocked by an outage" 2>&1 ); rcPushFail=$?
if [ "$rcPushFail" -eq 1 ] && printf '%s\n' "$outPushFail" | grep -q 'CLAIM: ERROR' \
   && printf '%s\n' "$outPushFail" | grep -q 'infra' \
   && ! printf '%s\n' "$outPushFail" | grep -q 'ADOPT-LOST' \
   && [ -z "$(ref_sha 2009)" ] && [ "$(accepted_updates 2009)" = "0" ]; then
  ok "empty-lease push failure over an ABSENT ref → ERROR infra exit 1, no ADOPT-LOST, nothing created"
else
  fail "expected ERROR infra exit 1 with no ref/no ADOPT-LOST; got rc=$rcPushFail ref='$(ref_sha 2009)' updates=$(accepted_updates 2009)
$outPushFail"
fi

# ===========================================================================
echo "TEST 10: adopt is RE-ENTRANT — a retry after a confirm-read blip must never abandon a claim we hold"
# ===========================================================================
# Reproduce the reported sequence exactly: a shim fails ONLY the post-push
# `ls-remote refs/claims/*` confirm, so the push LANDS and adopt still reports the
# retryable ERROR whose documented remedy is "retry".
SHIMC="$T/shim-claims-lookup-fail"
mkdir -p "$SHIMC"
cat >"$SHIMC/git" <<SHIM
#!/usr/bin/env bash
saw_ls=0; saw_claims=0
for a in "\$@"; do
  [ "\$a" = "ls-remote" ] && saw_ls=1
  case "\$a" in refs/claims/*) saw_claims=1 ;; esac
done
if [ "\$saw_ls" = 1 ] && [ "\$saw_claims" = 1 ]; then exit 128; fi
exec "$REALGIT" "\$@"
SHIM
chmod +x "$SHIMC/git"
outBlip=$( cd "$B" && PATH="$SHIMC:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin \
  bash "$CLAIM" adopt 2010 --expect none --reason "resume with a flaky confirm read" 2>&1 ); rcBlip=$?
blipSha=$(ref_sha 2010)
if [ "$rcBlip" -eq 1 ] && printf '%s\n' "$outBlip" | grep -q 'CLAIM: ERROR' && [ -n "$blipSha" ]; then
  ok "a confirm-read outage after a LANDED empty-lease push reports retryable ERROR (exit 1) with the ref created"
else
  fail "could not stage the confirm-blip case (rc=$rcBlip ref='$blipSha')
$outBlip"
fi
outRetry=$(runB adopt 2010 --expect none --reason "resume with a flaky confirm read"); rcRetry=$?
retrySha=$(ref_sha 2010)
if [ "$rcRetry" -eq 0 ] && printf '%s\n' "$outRetry" | grep -q 'CLAIM: ADOPTED' \
   && printf '%s\n' "$outRetry" | grep -q 're-entrant' \
   && [ "$retrySha" = "$blipSha" ] && [ "$(accepted_updates 2010)" = "1" ]; then
  ok "the RETRY by the same machine+actor is re-entrant: ADOPTED exit 0, ref unchanged, no second server write"
else
  fail "expected a re-entrant ADOPTED exit 0 leaving the ref at $blipSha; got rc=$rcRetry ref='$retrySha' updates=$(accepted_updates 2010)
$outRetry"
fi
outOtherActor=$(runB adopt 2010 --expect none --reason "a different actor on the same machine"  --actor closer); rcOtherActor=$?
if [ "$rcOtherActor" -eq 2 ] && printf '%s\n' "$outOtherActor" | grep -q 'CLAIM: ADOPT-LOST' \
   && [ "$(ref_sha 2010)" = "$blipSha" ]; then
  ok "re-entrancy is machine+ACTOR scoped: a different actor still gets ADOPT-LOST (exit 2)"
else
  fail "expected ADOPT-LOST exit 2 for a different actor; got rc=$rcOtherActor
$outOtherActor"
fi
outOtherMachine=$(runA adopt 2010 --expect none --reason "another machine must not inherit re-entrancy"); rcOtherMachine=$?
[ "$rcOtherMachine" -eq 2 ] && ok "another MACHINE still gets ADOPT-LOST (exit 2) — re-entrancy is not a bypass" \
  || fail "expected ADOPT-LOST exit 2 for another machine; got rc=$rcOtherMachine
$outOtherMachine"
# The SAME shortcut in CAS mode covers a VIOLATED compare-and-swap: the ref sits at
# Y != our --expect X, and Y happens to be ours. Exit 2 would abandon a claim we
# demonstrably hold, but a plain `ADOPTED … from=X` would print a value the ref never
# had and make a FAILED CAS indistinguishable from a satisfied one — so the CAS path
# has its own verdict naming BOTH shas.
STALE=2222222222222222222222222222222222222222
heldSha10=$(ref_sha 2010)
outCasReent=$(runB adopt 2010 --expect "$STALE" --reason "cas retry carrying a stale expected sha"); rcCasReent=$?
if [ "$rcCasReent" -eq 0 ] && printf '%s\n' "$outCasReent" | grep -q 'CLAIM: ADOPTED' \
   && printf '%s\n' "$outCasReent" | grep -q 'lease-mismatch' \
   && printf '%s\n' "$outCasReent" | grep -q "expected=$STALE" \
   && printf '%s\n' "$outCasReent" | grep -q "actual=$heldSha10" \
   && ! printf '%s\n' "$outCasReent" | grep -q "from=$STALE" \
   && [ "$(ref_sha 2010)" = "$heldSha10" ] && [ "$(accepted_updates 2010)" = "1" ]; then
  ok "a VIOLATED CAS on a ref we already hold reports ADOPTED (re-entrant, lease-mismatch) naming BOTH shas, never a bare from=<expected>"
else
  fail "expected a lease-mismatch re-entrant ADOPTED naming expected=$STALE and actual=$heldSha10; got rc=$rcCasReent ref='$(ref_sha 2010)' updates=$(accepted_updates 2010)
$outCasReent"
fi
# …but the mismatch wording is reserved for a GENUINE divergence (#2945 review). A CAS
# from the ref's CURRENT sha whose push does not land (the push-fail shim from TEST 9)
# leaves expected == actual: the precondition DID hold, so `lease-mismatch expected=X
# actual=X` named a divergence that never happened. It must be the plain re-entrant
# verdict instead.
outCasSame=$( cd "$B" && PATH="$SHIMP:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin \
  bash "$CLAIM" adopt 2010 --expect "$heldSha10" --reason "cas retry from the ref's current sha" 2>&1 ); rcCasSame=$?
if [ "$rcCasSame" -eq 0 ] && printf '%s\n' "$outCasSame" | grep -q 'CLAIM: ADOPTED' \
   && printf '%s\n' "$outCasSame" | grep -q 're-entrant' \
   && ! printf '%s\n' "$outCasSame" | grep -q 'lease-mismatch' \
   && printf '%s\n' "$outCasSame" | grep -q "from=$heldSha10" \
   && [ "$(ref_sha 2010)" = "$heldSha10" ] && [ "$(accepted_updates 2010)" = "1" ]; then
  ok "a CAS whose expected == actual reports the PLAIN re-entrant verdict — no 'lease-mismatch expected=X actual=X'"
else
  fail "expected a plain re-entrant ADOPTED (no lease-mismatch) when expected == actual; got rc=$rcCasSame ref='$(ref_sha 2010)' updates=$(accepted_updates 2010)
$outCasSame"
fi
if printf '%s\n' "$outRetry" | grep -q 'from=none' && ! printf '%s\n' "$outRetry" | grep -q 'lease-mismatch'; then
  ok "the EMPTY-lease re-entrant verdict stays distinct (from=none, no lease-mismatch marker)"
else
  fail "the empty-lease re-entrant verdict is no longer distinguishable from the CAS one:
$outRetry"
fi
# (A legitimate CAS from the CURRENT sha — the reaper-adopt contract — is TEST 13.)

# ===========================================================================
echo "TEST 11: a NON-ASCII --reason works (BSD/macOS tr aborts on it without a byte locale)"
# ===========================================================================
# `tr -c` in a UTF-8 locale fails with "Illegal byte sequence" on BSD/macOS tr, and
# under `set -euo pipefail` that killed the script inside a command substitution:
# NO CLAIM: line at all, exit 1 — which the contract reads as "retryable", so the
# caller retries forever on input that can never succeed. This repo's prose is full
# of em dashes, so `--reason "resume — parked claim"` is a likely invocation.
outUtf8=$( cd "$B" && LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 CLAIM_MACHINE=machineB CLAIM_REMOTE=origin \
  bash "$CLAIM" adopt 2011 --expect none --reason "resume — parked claim (café, naïve)" 2>&1 ); rcUtf8=$?
utf8Reason=$(line_field "$outUtf8" reason)
if [ "$rcUtf8" -eq 0 ] && [ "$(printf '%s\n' "$outUtf8" | grep -c 'CLAIM: ADOPTED')" = "1" ] \
   && [ -n "$utf8Reason" ] && [ "$utf8Reason" = "${utf8Reason%%[[:space:]]*}" ] \
   && printf '%s' "$utf8Reason" | grep -q 'resume' && [ -n "$(ref_sha 2011)" ]; then
  ok "a non-ASCII --reason adopts normally (exit 0) and sanitizes to ONE ASCII token"
else
  fail "expected exit 0 and a single sanitized reason token for a non-ASCII reason; got rc=$rcUtf8 reason='$utf8Reason'
$outUtf8"
fi

# ===========================================================================
echo "TEST 12: identity FORGERY — neither --reason nor --actor can impersonate another machine"
# ===========================================================================
# Both fields are user-controlled text appended to the very commit message the
# holder-identity parser reads. The parser used to take the LAST `<key>=` match
# anywhere in the message, so a value carrying `machine=<other>` forged the holder —
# and holder identity gates re-entrancy, verify, and release.
runB adopt 2012 --expect none --reason "sneaky machine=machineA actor=flow pid=1" >/dev/null 2>&1; rcForge=$?
forgeSha=$(ref_sha 2012)
[ "$rcForge" -eq 0 ] && [ -n "$forgeSha" ] && ok "setup: machineB holds issue-2012 with a machine=-carrying reason" \
  || fail "setup failed for the forged-reason case (rc=$rcForge)"
forgeStatus=$(runB status 2012)
[ "$(line_field "$forgeStatus" machine)" = "machineB" ] \
  && ok "status names the TRUE holder (machineB), not the machine= smuggled in the reason" \
  || fail "forged reason changed the rendered holder:
$forgeStatus"
outForgeClaim=$(runA claim 2012); rcForgeClaim=$?
if [ "$rcForgeClaim" -eq 2 ] && printf '%s\n' "$outForgeClaim" | grep -q 'CLAIM: LOST' \
   && ! printf '%s\n' "$outForgeClaim" | grep -q 're-entrant'; then
  ok "the impersonated machine's claim is LOST (exit 2), never a re-entrant HELD"
else
  fail "expected CLAIM: LOST exit 2 for the impersonated machine; got rc=$rcForgeClaim
$outForgeClaim"
fi
outForgeRelease=$(runA_gh release 2012); rcForgeRelease=$?
if [ "$rcForgeRelease" -eq 2 ] && printf '%s\n' "$outForgeRelease" | grep -q 'reason=not-holder' \
   && [ "$(ref_sha 2012)" = "$forgeSha" ]; then
  ok "the impersonated machine cannot release the forged-reason claim (RELEASE-REFUSED, ref intact)"
else
  fail "expected RELEASE-REFUSED not-holder with the ref intact; got rc=$rcForgeRelease ref='$(ref_sha 2012)'
$outForgeRelease"
fi
# Same attack through --actor, which is itself part of the holder identity.
runB claim 2022 --actor 'flow machine=machineA' >/dev/null 2>&1; rcActorSetup=$?
actorSha=$(ref_sha 2022)
[ "$rcActorSetup" -eq 0 ] && [ -n "$actorSha" ] && ok "setup: machineB claims issue-2022 with a machine=-carrying --actor" \
  || fail "setup failed for the forged-actor case (rc=$rcActorSetup)"
outActorForge=$(runA claim 2022); rcActorForge=$?
if [ "$rcActorForge" -eq 2 ] && printf '%s\n' "$outActorForge" | grep -q 'CLAIM: LOST' \
   && ! printf '%s\n' "$outActorForge" | grep -q 're-entrant'; then
  ok "a forged --actor does NOT hand machineA re-entrancy on machineB's ref (LOST exit 2)"
else
  fail "forged --actor granted a second writer on one issue; got rc=$rcActorForge
$outActorForge"
fi
runB verify 2022 --actor 'flow machine=machineA' >/dev/null 2>&1; rcActorVerify=$?
[ "$rcActorVerify" -eq 0 ] \
  && ok "the sanitized actor round-trips for its own holder (verify exit 0) — sanitizing is consistent, not lossy" \
  || fail "the holder can no longer verify its own sanitized actor (rc=$rcActorVerify)"

# ===========================================================================
echo "TEST 13: the reason RECORD — CAS mode, unrepresentable text, truncation, embedded newline"
# ===========================================================================
runB claim 2013 >/dev/null 2>&1
casFrom=$(ref_sha 2013)
outCas=$(runA adopt 2013 --expect "$casFrom" --reason "adopting a reaped claim after the 4h threshold"); rcCas=$?
casStatus=$(runA status 2013)
if [ "$rcCas" -eq 0 ] && printf '%s\n' "$outCas" | grep -q 'mode=cas' \
   && [ "$(line_field "$casStatus" machine)" = "machineA" ] \
   && printf '%s' "$(line_field "$casStatus" reason)" | grep -q 'reaped'; then
  ok "--reason is recorded on the CAS path too (mode=cas, new holder, reason readable)"
else
  fail "expected a recorded mode=cas adoption by machineA; got rc=$rcCas
$outCas
$casStatus"
fi
# A reason with NOTHING RECORDABLE in it is a usage error, not a recorded
# `reason=unspecified`. The gate used to validate the RAW argument and record the
# SANITIZED one, so '   ', '---', '…' or an expansion like "$UNSET_VAR " passed and
# landed as `reason=unspecified` — indistinguishable from supplying no reason, which
# defeats the "record WHY" requirement. Same fail-closed direction as --expect ''.
whyRc=""
for badWhy in '   ' '---' '…' 'x' '  ' '=='; do
  runB adopt 2023 --expect none --reason "$badWhy" >/dev/null 2>&1
  whyRc="$whyRc $?"
done
if [ "$whyRc" = " 64 64 64 64 64 64" ] && [ -z "$(ref_sha 2023)" ]; then
  ok "a --reason with nothing recordable ('   ', '---', '…', 'x', '==') is a usage error (exit 64), nothing acquired"
else
  fail "expected 64 from every unrecordable --reason with no ref created; got rcs='$whyRc' ref='$(ref_sha 2023)'"
fi
# A PLACEHOLDER reason is refused too (#2945 review). `--reason <why>` copy-pasted from
# a help line sanitizes to `why`: 3 recordable chars, so the length gate passes and the
# record reads `reason=why` — as uninformative as the no-reason case this gate exists to
# reject. The sentinel vocabulary is refused by name, case-insensitively.
placeholderRc=""
for ph in '<why>' 'why' 'TODO' 'tbd' 'xxx' 'placeholder' 'FIXME' 'n/a'; do
  runB adopt 2023 --expect none --reason "$ph" >/dev/null 2>&1
  placeholderRc="$placeholderRc $?"
done
if [ "$placeholderRc" = " 64 64 64 64 64 64 64 64" ] && [ -z "$(ref_sha 2023)" ]; then
  ok "a PLACEHOLDER reason ('<why>', 'why', 'TODO', 'tbd', 'xxx', 'placeholder', 'FIXME', 'n/a') is a usage error (exit 64), nothing acquired"
else
  fail "expected 64 from every placeholder --reason with no ref created; got rcs='$placeholderRc' ref='$(ref_sha 2023)'"
fi
runB adopt 2023 --expect none --reason 'wip' >/dev/null 2>&1; rcMinWhy=$?
if [ "$rcMinWhy" -eq 0 ] && [ "$(line_field "$(runB status 2023)" reason)" = "wip" ]; then
  ok "a short but RECORDABLE reason ('wip') is accepted and recorded verbatim"
else
  fail "expected a recordable 3-char reason to adopt and record; got rc=$rcMinWhy reason='$(line_field "$(runB status 2023)" reason)'"
fi
longReason=$(printf 'a%.0s' $(seq 1 200))
runB adopt 2033 --expect none --reason "$longReason" >/dev/null 2>&1; rcLong=$?
longToken=$(line_field "$(runB status 2033)" reason)
if [ "$rcLong" -eq 0 ] && [ "${#longToken}" -eq 120 ]; then
  ok "an over-long reason is truncated to the documented 120 chars"
else
  fail "expected a 120-char reason token; got rc=$rcLong len=${#longToken}"
fi
# TRUNCATION MUST NOT RE-INTRODUCE A TRAILING SEPARATOR (#2945 review). The single-word
# fixture above cannot see this: the sanitizer trimmed leading/trailing '-' BEFORE the
# 120-char cut, so a MULTI-WORD reason whose 120th byte is a word separator recorded as
# '…foo-' — contradicting the documented trim. 31 'abc' words sanitize to 123 chars whose
# 120th is exactly a '-', so the cut lands on the separator.
multiReason="$(printf 'abc %.0s' $(seq 1 31))"
runB adopt 2053 --expect none --reason "$multiReason" >/dev/null 2>&1; rcMulti=$?
multiToken=$(line_field "$(runB status 2053)" reason)
if [ "$rcMulti" -eq 0 ] && [ "${#multiToken}" -le 120 ] \
   && [ "${multiToken%-}" = "$multiToken" ] && [ "${multiToken#abc-abc}" != "$multiToken" ]; then
  ok "a truncated MULTI-WORD reason keeps the documented trim (len=${#multiToken}, no trailing separator)"
else
  fail "expected a <=120-char token with no trailing '-'; got rc=$rcMulti len=${#multiToken} token='$multiToken'"
fi
runB adopt 2043 --expect none --reason "$(printf 'first line\nsecond line')" >/dev/null 2>&1; rcNl=$?
nlStatus=$(runB status 2043)
nlToken=$(line_field "$nlStatus" reason)
if [ "$rcNl" -eq 0 ] && [ "$(printf '%s\n' "$nlStatus" | grep -c 'CLAIM: STATUS issue=2043 ')" = "1" ] \
   && [ "$nlToken" = "first-line-second-line" ]; then
  ok "an embedded newline collapses into the single-line record (no injected extra row)"
else
  fail "expected one STATUS row with a collapsed reason token; got rc=$rcNl token='$nlToken'
$nlStatus"
fi

# ===========================================================================
echo "TEST 14: reaper vs resumer — a forced release racing an empty-lease adopt never yields a bogus owner"
# ===========================================================================
# The realistic fleet collision: flow-board force-releases an abandoned claim at the
# same moment another machine resumes it. Legal end states are "no ref" (the reaper's
# delete landed last) or "the resumer's ref" — never machineA's stale sha, and never
# a resumer that reported ADOPTED while some other commit owns the ref.
reapRounds=0; reapOk=0; reapDiag=""
for id in 2201 2202 2203; do
  reapRounds=$((reapRounds+1))
  runA claim "$id" >/dev/null 2>&1
  staleSha=$(ref_sha "$id")
  # Same ONE-shared-signal barrier as TEST 3 (two independent FIFOs released each
  # side as its own writer connected, which permits full serialization).
  race_reset
  ( race_wait_go "$T/ready-a"
    runA_gh release "$id" --force >"$T/reap.out" 2>&1; echo "$?" >"$T/reap.rc" ) &
  pidR=$!
  ( race_wait_go "$T/ready-b"
    runB adopt "$id" --expect none --reason "resumer racing the reaper" >"$T/resume.out" 2>&1; echo "$?" >"$T/resume.rc" ) &
  pidS=$!
  race_release
  wait "$pidR" "$pidS" 2>/dev/null
  rcResume="$(cat "$T/resume.rc" 2>/dev/null || echo missing)"
  resumeOut="$(cat "$T/resume.out" 2>/dev/null)"
  resumeSha=$(line_field "$resumeOut" sha)
  finalSha=$(ref_sha "$id")
  verdict=1
  # 1. The stale holder's commit must never survive the round.
  [ -n "$finalSha" ] && [ "$finalSha" = "$staleSha" ] && verdict=0
  # 2. A reported ADOPTED must be truthful: the surviving ref (if any) is ours.
  if printf '%s\n' "$resumeOut" | grep -q 'CLAIM: ADOPTED'; then
    [ "$rcResume" = "0" ] || verdict=0
    [ -z "$finalSha" ] || [ "$finalSha" = "$resumeSha" ] || verdict=0
    if [ -n "$finalSha" ]; then runB verify "$id" >/dev/null 2>&1 || verdict=0; fi
  else
    # 3. Otherwise it must have failed loudly (exit 1|2), never a silent exit 0.
    case "$rcResume" in 1 | 2) : ;; *) verdict=0 ;; esac
  fi
  if [ "$verdict" -eq 1 ]; then
    reapOk=$((reapOk+1))
  else
    reapDiag="$reapDiag
  round $id: stale=$staleSha final='$finalSha' resumeRc=$rcResume resume='$resumeOut' reap='$(cat "$T/reap.out" 2>/dev/null)'"
  fi
done
[ "$reapOk" -eq "$reapRounds" ] \
  && ok "forced-release vs empty-lease-adopt: every round ends at no-ref or the resumer's own ref ($reapOk/$reapRounds)" \
  || fail "reaper/resumer barrier race produced a bogus owner in $((reapRounds - reapOk))/$reapRounds rounds$reapDiag"

# ===========================================================================
echo "TEST 15: the escape hatch is advertised ONLY when the endgame is demonstrably orphaned (open PR / unknown → withheld)"
# ===========================================================================
# The refusal's safety argument ("git rejects it if any machine holds the ref") does
# NOT cover the case the guard exists for: an OLDER-fleet worker locks with the
# BRANCH and holds no claim ref, so `claim-ref=free` is true for it and the advertised
# empty-lease adopt WOULD succeed — handing an actively-worked issue to a second
# machine. The readers are agents that run printed remediations literally, so the
# command is printed only at open-prs=0, and an unreadable PR list withholds too.
# The fixture tip is LONG STALE (the default), so the PR signal is the ONLY thing that
# can withhold here — the tip-age/liveness signals are pinned separately in TEST 16.
push_branch_with_own_commit "issue-2015-live-endgame"
( cd "$B" && g fetch -q origin )
PRSHIM="$T/shim-gh-open-pr"
mkdir -p "$PRSHIM"
cat >"$PRSHIM/gh" <<'GH'
#!/usr/bin/env bash
# Fake gh: one OPEN PR whose head branch is this issue's.
for a in "$@"; do [ "$a" = "list" ] && { echo "issue-2015-live-endgame"; exit 0; }; done
exit 1
GH
chmod +x "$PRSHIM/gh"
outLive=$( cd "$B" && PATH="$PRSHIM:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin bash "$CLAIM" claim 2015 ); rcLive=$?
if [ "$rcLive" -eq 2 ] && printf '%s\n' "$outLive" | grep -q 'reason=legacy-branch-lock' \
   && printf '%s\n' "$outLive" | grep -q 'remediation=withheld' \
   && printf '%s\n' "$outLive" | grep -q 'open-prs=1' \
   && ! printf '%s\n' "$outLive" | grep -q 'adopt 2015 --expect none' \
   && [ "$(printf '%s\n' "$outLive" | grep -c 'CLAIM:')" = "1" ]; then
  ok "with an OPEN PR the refusal withholds the hatch (remediation=withheld open-prs=1, no copy-pasteable adopt) on one line"
else
  fail "expected a withheld remediation with open-prs=1; got rc=$rcLive
$outLive"
fi
# gh missing/failing = UNKNOWN, and unknown must fail closed exactly like release's
# open-PR guard — never advertise a hand-away on an unread signal.
GHFAIL="$T/shim-gh-broken"
mkdir -p "$GHFAIL"
cat >"$GHFAIL/gh" <<'GH'
#!/usr/bin/env bash
echo "gh: simulated failure" >&2
exit 1
GH
chmod +x "$GHFAIL/gh"
outUnknown=$( cd "$B" && PATH="$GHFAIL:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin bash "$CLAIM" claim 2015 2>/dev/null ); rcUnknown=$?
if [ "$rcUnknown" -eq 2 ] && printf '%s\n' "$outUnknown" | grep -q 'remediation=withheld' \
   && printf '%s\n' "$outUnknown" | grep -q 'open-prs=-1' \
   && ! printf '%s\n' "$outUnknown" | grep -q 'adopt 2015 --expect none'; then
  ok "an UNREADABLE PR list also withholds the hatch (open-prs=-1) — fail closed on an unread signal"
else
  fail "expected a withheld remediation with open-prs=-1; got rc=$rcUnknown
$outUnknown"
fi
# Withholding is ADVICE, not a lock: an operator who has confirmed ownership can
# still resume, and git remains the sole arbiter (the claim ref really is free).
outStillWorks=$(runB adopt 2015 --expect none --reason "ownership confirmed with the PR author"); rcStillWorks=$?
if [ "$rcStillWorks" -eq 0 ] && printf '%s\n' "$outStillWorks" | grep -q 'CLAIM: ADOPTED' \
   && [ -n "$(ref_sha 2015)" ]; then
  ok "the hatch itself still works when invoked deliberately — the guard changes ADVICE, not the arbiter"
else
  fail "expected the deliberate resume to still succeed; got rc=$rcStillWorks
$outStillWorks"
fi

# ===========================================================================
echo "TEST 16: the PRE-PR window — a FRESH branch tip or a fresh liveness ref withholds the hatch; the advertised command is informative VERBATIM"
# ===========================================================================
# open-prs=0 alone does NOT prove the lane is orphaned (#2945 review): this pipeline
# opens the PR LATE, so an older-fleet worker that is actively implementing has zero
# open PRs for most of its life while holding only the BRANCH. The liveness evidence for
# that window is the branch TIP AGE plus the supervisor-authored machine-claim/heartbeat
# refs — and every unreadable signal withholds.

# (a) FRESH tip, no open PR: a worker mid-implementation. The hatch must be WITHHELD.
push_branch_with_own_commit "issue-2016-being-worked-right-now" "wip commit" "$(now_ts)"
( cd "$B" && g fetch -q origin )
outFresh=$(runB_gh claim 2016); rcFresh=$?
if [ "$rcFresh" -eq 2 ] && printf '%s\n' "$outFresh" | grep -q 'remediation=withheld' \
   && printf '%s\n' "$outFresh" | grep -q 'open-prs=0' \
   && printf '%s\n' "$outFresh" | grep -q 'newest-branch-tip=' \
   && ! printf '%s\n' "$outFresh" | grep -q 'adopt 2016 --expect none' \
   && [ "$(printf '%s\n' "$outFresh" | grep -c 'CLAIM:')" = "1" ]; then
  ok "a FRESH branch tip at open-prs=0 (legacy worker mid-implementation) WITHHOLDS the hatch"
else
  fail "expected the hatch withheld for a fresh tip at open-prs=0; got rc=$rcFresh
$outFresh"
fi

# (b) STALE tip, no open PR, no liveness ref: demonstrably orphaned → advertised. And
# the printed command, run VERBATIM (which is what the readers do), must record an
# INFORMATIVE reason — not the `why` a `--reason <why>` placeholder would have recorded.
push_branch_with_own_commit "issue-2017-abandoned-lane"
( cd "$B" && g fetch -q origin )
outAdv=$(runB_gh claim 2017); rcAdv=$?
if [ "$rcAdv" -eq 2 ] && printf '%s\n' "$outAdv" | grep -q "adopt 2017 --expect none --reason resume-legacy-branch-lock:issue-2017-abandoned-lane" \
   && printf '%s\n' "$outAdv" | grep -q 'liveness-refs=none-naming-2017' \
   && printf '%s\n' "$outAdv" | grep -q 'stale-after='; then
  ok "a STALE tip + open-prs=0 + no liveness ref advertises the hatch with a CONCRETE self-describing --reason"
else
  fail "expected an advertised hatch with a concrete reason; got rc=$rcAdv
$outAdv"
fi
verbatimArgs=$(printf '%s\n' "$outAdv" | sed -n "s/.*remediation='bash scripts\/flow\/claim.sh \([^']*\)'.*/\1/p")
# Deliberate word splitting: the point is to run the PRINTED argv, unedited.
# shellcheck disable=SC2086
outVerbatim=$(runB $verbatimArgs); rcVerbatim=$?
verbatimReason=$(line_field "$(runB status 2017)" reason)
if [ "$rcVerbatim" -eq 0 ] && printf '%s\n' "$outVerbatim" | grep -q 'CLAIM: ADOPTED' \
   && [ "$verbatimReason" != "why" ] && [ "$verbatimReason" != "unspecified" ] \
   && printf '%s' "$verbatimReason" | grep -q 'resume-legacy-branch-lock' \
   && printf '%s' "$verbatimReason" | grep -q 'issue-2017-abandoned-lane'; then
  ok "copy-pasted VERBATIM the printed command adopts and records an informative reason ('$verbatimReason')"
else
  fail "the printed command did not adopt with an informative reason; rc=$rcVerbatim reason='$verbatimReason'
$outVerbatim"
fi

# (c) STALE tip but a FRESH machine-claim ref naming the issue: a supervisor says a
# worker is on it (#2655), so the hatch is WITHHELD even though the tip aged out.
push_branch_with_own_commit "issue-2018-stale-branch-live-worker"
push_machine_claim "refs/machine-claims/machineFresh" 2018 "$(now_ts)"
( cd "$B" && g fetch -q origin )
outLiveRef=$(runB_gh claim 2018); rcLiveRef=$?
if [ "$rcLiveRef" -eq 2 ] && printf '%s\n' "$outLiveRef" | grep -q 'remediation=withheld' \
   && printf '%s\n' "$outLiveRef" | grep -q 'liveness-ref=refs/machine-claims/machineFresh' \
   && ! printf '%s\n' "$outLiveRef" | grep -q 'adopt 2018 --expect none'; then
  ok "a FRESH machine-claim ref naming the issue WITHHOLDS the hatch even with a stale branch tip"
else
  fail "expected the hatch withheld for a fresh liveness ref; got rc=$rcLiveRef
$outLiveRef"
fi

# (d) …and a liveness ref older than the threshold does NOT withhold forever: it is
# itself reapable, so an abandoned heartbeat must not lock the lane out permanently.
push_branch_with_own_commit "issue-2019-stale-branch-stale-heartbeat"
push_machine_claim "refs/heartbeats/machineStale" 2019 "$STALE_TIP_DATE"
( cd "$B" && g fetch -q origin )
outStaleRef=$(runB_gh claim 2019); rcStaleRef=$?
if [ "$rcStaleRef" -eq 2 ] && printf '%s\n' "$outStaleRef" | grep -q 'adopt 2019 --expect none --reason resume-legacy-branch-lock:' \
   && printf '%s\n' "$outStaleRef" | grep -q 'liveness-refs=none-naming-2019'; then
  ok "a liveness ref STALER than the reap threshold ages out and does not withhold the hatch"
else
  fail "expected the hatch advertised despite a stale liveness ref; got rc=$rcStaleRef
$outStaleRef"
fi

# (e) UNREADABLE threshold (claim.sh without its claim-heartbeat.sh sibling): the lane
# cannot be shown orphaned, so the hatch is withheld rather than defaulting to some
# second copy of "4h".
push_branch_with_own_commit "issue-2020-threshold-unreadable"
( cd "$B" && g fetch -q origin )
LONE="$T/lone-claim"
mkdir -p "$LONE"
cp "$CLAIM" "$LONE/claim.sh"
outNoThresh=$( cd "$B" && PATH="$GHSHIM:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin \
  bash "$LONE/claim.sh" claim 2020 2>/dev/null ); rcNoThresh=$?
if [ "$rcNoThresh" -eq 2 ] && printf '%s\n' "$outNoThresh" | grep -q 'remediation=withheld' \
   && printf '%s\n' "$outNoThresh" | grep -q 'liveness=threshold-unreadable' \
   && ! printf '%s\n' "$outNoThresh" | grep -q 'adopt 2020 --expect none'; then
  ok "an UNREADABLE staleness threshold withholds the hatch (no hardcoded fallback)"
else
  fail "expected the hatch withheld when the threshold cannot be read; got rc=$rcNoThresh
$outNoThresh"
fi

# (f) UNREADABLE liveness refs (a git shim fails ONLY the machine-claims enumeration,
# everything else passes through): "we could not prove nobody is on it" withholds.
push_branch_with_own_commit "issue-2021-liveness-enumeration-outage"
( cd "$B" && g fetch -q origin )
SHIML="$T/shim-liveness-fail"
mkdir -p "$SHIML"
cat >"$SHIML/git" <<SHIM
#!/usr/bin/env bash
saw_ls=0; saw_liveness=0
for a in "\$@"; do
  [ "\$a" = "ls-remote" ] && saw_ls=1
  case "\$a" in refs/machine-claims/* | refs/heartbeats/*) saw_liveness=1 ;; esac
done
if [ "\$saw_ls" = 1 ] && [ "\$saw_liveness" = 1 ]; then exit 128; fi
exec "$REALGIT" "\$@"
SHIM
chmod +x "$SHIML/git"
outNoLive=$( cd "$B" && PATH="$SHIML:$GHSHIM:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin \
  bash "$CLAIM" claim 2021 2>/dev/null ); rcNoLive=$?
if [ "$rcNoLive" -eq 2 ] && printf '%s\n' "$outNoLive" | grep -q 'remediation=withheld' \
   && printf '%s\n' "$outNoLive" | grep -q 'liveness-refs=unreadable' \
   && ! printf '%s\n' "$outNoLive" | grep -q 'adopt 2021 --expect none'; then
  ok "an UNREADABLE liveness-ref enumeration withholds the hatch (fail closed on an unread signal)"
else
  fail "expected the hatch withheld on a liveness enumeration outage; got rc=$rcNoLive
$outNoLive"
fi

# (g) …and withholding still changes only ADVICE: the deliberate resume of the
# actively-worked lane from (a) remains possible for an operator who confirmed ownership.
outDeliberate=$(runB adopt 2016 --expect none --reason "confirmed with the branch author that the lane is free"); rcDeliberate=$?
if [ "$rcDeliberate" -eq 0 ] && printf '%s\n' "$outDeliberate" | grep -q 'CLAIM: ADOPTED' \
   && [ -n "$(ref_sha 2016)" ]; then
  ok "the liveness gate changes the printed ADVICE only — a deliberate resume still works, git still arbitrates"
else
  fail "expected the deliberate resume of a withheld lane to still succeed; got rc=$rcDeliberate
$outDeliberate"
fi

# ===========================================================================
echo "TEST 17: a branch with NO COMMITS OF ITS OWN is INDETERMINATE, never 'stale' — the vacuous tip-age case"
# ===========================================================================
# THE PRODUCTION PREMISE THE OTHER FIXTURES MISS (#2945 round 5). Every fixture above
# uses push_branch_with_own_commit, so its tip has a date of its own. `flow-activate`
# does NOT: it creates the branch at origin/main and pushes it with no commits, and
# `flow-implement` pushes again only at PR time. So for a freshly-activated lane the
# tip's committer date IS main's — zero information about the worker. Whenever main had
# been quiet longer than the threshold (overnight is routine), that lane presented as
# open-prs=0 + a stale tip + no liveness refs, and the guard printed the copy-pasteable
# hand-away FOR AN ACTIVELY-WORKED ISSUE — the two-writer outcome it exists to prevent.
# So: no own commits => no age signal => remediation WITHHELD, with its own marker.
advance_main_with_stale_tip

# (a) The vacuous case: commit-less branch, main quiet since 2020, no liveness refs.
push_branch_without_own_commits "issue-2060-just-activated-no-commits"
( cd "$B" && g fetch -q origin )
outVacuous=$(runB_gh claim 2060); rcVacuous=$?
if [ "$rcVacuous" -eq 2 ] && printf '%s\n' "$outVacuous" | grep -q 'remediation=withheld' \
   && printf '%s\n' "$outVacuous" | grep -q 'newest-branch-tip=no-own-commits' \
   && ! printf '%s\n' "$outVacuous" | grep -q 'stale-after=' \
   && ! printf '%s\n' "$outVacuous" | grep -q 'adopt 2060 --expect none' \
   && [ "$(printf '%s\n' "$outVacuous" | grep -c 'CLAIM:')" = "1" ]; then
  ok "a commit-less branch on a LONG-QUIET main withholds the hatch (newest-branch-tip=no-own-commits, no age verdict)"
else
  fail "expected the hatch withheld as no-own-commits; got rc=$rcVacuous
$outVacuous"
fi

# (b) THE MOTIVATING CASES STILL WORK. #2043 and #1883 each carried their own
# OpenSpec spec commit, so their tips ARE informative and genuinely old — on the very
# same long-quiet main, such a branch must still be advertised.
push_branch_with_own_commit "issue-2061-abandoned-with-its-own-commit"
( cd "$B" && g fetch -q origin )
outOwn=$(runB_gh claim 2061); rcOwn=$?
if [ "$rcOwn" -eq 2 ] && printf '%s\n' "$outOwn" | grep -q 'adopt 2061 --expect none --reason resume-legacy-branch-lock:' \
   && printf '%s\n' "$outOwn" | grep -q 'newest-branch-tip=' \
   && printf '%s\n' "$outOwn" | grep -q 'stale-after=' \
   && ! printf '%s\n' "$outOwn" | grep -q 'no-own-commits'; then
  ok "a branch carrying its OWN stale commit is still age-judged and still advertises (the #2043/#1883 shape)"
else
  fail "expected the hatch advertised for a stale branch with its own commit; got rc=$rcOwn
$outOwn"
fi

# (c) …and the new signal opens NO unreadable-signal hole: when the base (origin's
# default branch) cannot be read, the ancestry test cannot be run, so the tip cannot be
# judged either way — withhold, never "it must carry own commits".
push_branch_with_own_commit "issue-2062-base-unreadable"
( cd "$B" && g fetch -q origin )
SHIMB="$T/shim-base-fetch-fail"
mkdir -p "$SHIMB"
cat >"$SHIMB/git" <<SHIM
#!/usr/bin/env bash
saw_fetch=0; saw_base=0
for a in "\$@"; do
  [ "\$a" = "fetch" ] && saw_fetch=1
  case "\$a" in +HEAD:*) saw_base=1 ;; esac
done
if [ "\$saw_fetch" = 1 ] && [ "\$saw_base" = 1 ]; then exit 128; fi
exec "$REALGIT" "\$@"
SHIM
chmod +x "$SHIMB/git"
outNoBase=$( cd "$B" && PATH="$SHIMB:$GHSHIM:$PATH" CLAIM_MACHINE=machineB CLAIM_REMOTE=origin \
  bash "$CLAIM" claim 2062 2>/dev/null ); rcNoBase=$?
if [ "$rcNoBase" -eq 2 ] && printf '%s\n' "$outNoBase" | grep -q 'remediation=withheld' \
   && printf '%s\n' "$outNoBase" | grep -q 'default-branch-tip=unreadable' \
   && ! printf '%s\n' "$outNoBase" | grep -q 'adopt 2062 --expect none'; then
  ok "an UNREADABLE default-branch tip withholds the hatch (the ancestry test is a signal, so it fails closed too)"
else
  fail "expected the hatch withheld on an unreadable base; got rc=$rcNoBase
$outNoBase"
fi

# (d) The liveness-ref scan is SCOPED TO THIS ISSUE: a readable LEGACY ref in the
# namespace that names NO issue used to withhold the hatch fleet-wide, for EVERY issue —
# the permanent dead end this change removes. It must be skipped, not withheld.
push_raw_liveness_ref "refs/heartbeats/machineLegacyFormat" "heartbeat machine=machineLegacy pid=7 at=whenever"
push_branch_with_own_commit "issue-2063-legacy-liveness-ref-present"
( cd "$B" && g fetch -q origin )
outLegacyRef=$(runB_gh claim 2063); rcLegacyRef=$?
if [ "$rcLegacyRef" -eq 2 ] && printf '%s\n' "$outLegacyRef" | grep -q 'adopt 2063 --expect none --reason resume-legacy-branch-lock:' \
   && printf '%s\n' "$outLegacyRef" | grep -q 'liveness-refs=none-naming-2063' \
   && ! printf '%s\n' "$outLegacyRef" | grep -q 'liveness-refs=unreadable'; then
  ok "a LEGACY liveness ref naming no issue does not withhold another issue's hatch (the scan is issue-scoped)"
else
  fail "expected an issue-scoped liveness scan to still advertise; got rc=$rcLegacyRef
$outLegacyRef"
fi

echo ""
echo "================  claim-resume (#2945): $PASS passed, $FAIL failed, $SKIP skipped  ================"
[ "$FAIL" -eq 0 ]
