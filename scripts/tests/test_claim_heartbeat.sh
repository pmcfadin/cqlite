#!/usr/bin/env bash
#
# Regression tests for scripts/flow/claim-heartbeat.sh (issue #2089).
#
# Fast + hermetic: a mktemp BARE repo stands in for origin, plus one clone that
# plays every "machine" via HEARTBEAT_MACHINE overrides. No network, no
# GitHub — heartbeats are a pure git-ref mechanism.
#
# Run standalone:   bash scripts/tests/test_claim_heartbeat.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HB="$SCRIPT_DIR/../flow/claim-heartbeat.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# git in a throwaway identity so commits/pushes work in any sandbox, matching
# scripts/flow/tests/finalize-cleanup.test.sh's convention.
g() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/claim-heartbeat-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

ORIGIN="$T/origin.git"
WORK="$T/work"
g init --bare -q "$ORIGIN"
g clone -q "$ORIGIN" "$WORK" 2>/dev/null
(
  cd "$WORK" || exit 1
  echo seed >seed.txt
  g add seed.txt
  g commit -qm seed
  g push -q -u origin main
)

# ts_to_epoch_test <ISO8601 UTC ts> — the same portable GNU/BSD parse the
# script uses, duplicated here only to build assertions (never imported).
ts_to_epoch_test() {
  local ts="$1" epoch
  if epoch=$(date -u -d "$ts" +%s 2>/dev/null); then
    printf '%s\n' "$epoch"
    return 0
  fi
  date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$ts" +%s
}

# craft_old_heartbeat <work> <machine> <issue> <age_seconds> — pushes a
# heartbeat ref directly (bypassing `beat`) with a ts crafted <age_seconds> in
# the past, so age assertions don't depend on real sleeps.
craft_old_heartbeat() {
  local work="$1" machine="$2" issue="$3" age="$4"
  (
    cd "$work" || exit 1
    local now_epoch old_epoch old_ts empty_tree csha
    now_epoch=$(date -u +%s)
    old_epoch=$((now_epoch - age))
    if ! old_ts=$(date -u -r "$old_epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null); then
      old_ts=$(date -u -d "@$old_epoch" +%Y-%m-%dT%H:%M:%SZ)
    fi
    empty_tree=$(git hash-object -t tree --stdin </dev/null)
    csha=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
      GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$empty_tree" -m "heartbeat issue=${issue} machine=${machine} ts=${old_ts}")
    g push -q origin "${csha}:refs/heartbeats/${machine}"
  )
}

# ===========================================================================
echo "TEST 1: beat creates the ref"
# ===========================================================================
out=$(cd "$WORK" && HEARTBEAT_MACHINE=machineA bash "$HB" beat 100 2>&1)
sha1=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineA" | awk '{print $1}')
if [ -n "$sha1" ]; then
  ok "beat pushed refs/heartbeats/machineA ($sha1)"
else
  bad "beat did not create refs/heartbeats/machineA — output: $out"
fi

# ===========================================================================
echo "TEST 2: second beat force-updates (same ref, new sha; one ref per machine)"
# ===========================================================================
sleep 1
(cd "$WORK" && HEARTBEAT_MACHINE=machineA bash "$HB" beat 101 >/dev/null 2>&1)
sha2=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineA" | awk '{print $1}')
count=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineA" | wc -l | tr -d ' ')
if [ "$count" = "1" ] && [ -n "$sha2" ] && [ "$sha2" != "$sha1" ]; then
  ok "second beat force-updated the SAME ref to a new sha ($sha1 -> $sha2), count=$count"
else
  bad "expected 1 ref with a changed sha; got count=$count sha1=$sha1 sha2=$sha2"
fi

# ===========================================================================
echo "TEST 3: list renders machine/issue/age for 2 machines"
# ===========================================================================
(cd "$WORK" && HEARTBEAT_MACHINE=machineB bash "$HB" beat 202 >/dev/null 2>&1)
list_out=$(cd "$WORK" && bash "$HB" list)
if printf '%s\n' "$list_out" | grep -qE '^machineA +101 ' \
  && printf '%s\n' "$list_out" | grep -qE '^machineB +202 '; then
  ok "list rendered both machineA (issue 101) and machineB (issue 202)"
else
  bad "list output missing expected machine/issue rows:
$list_out"
fi
if printf '%s\n' "$list_out" | grep -qE '^machineA +[0-9]+ +[0-9TZ:-]+ +[0-9]+[smhd]$'; then
  ok "list row shape is machine/issue/ts/age"
else
  bad "list row shape unexpected:
$list_out"
fi

# ===========================================================================
echo "TEST 4: age reflects an old ts (crafted 10h in the past)"
# ===========================================================================
craft_old_heartbeat "$WORK" "machineOld" 6000 36000
list_out=$(cd "$WORK" && bash "$HB" list)
old_line=$(printf '%s\n' "$list_out" | grep -E '^machineOld ' || true)
old_age=$(printf '%s\n' "$old_line" | awk '{print $NF}')
if [[ "$old_age" =~ ^([0-9]+)h$ ]] && [ "${BASH_REMATCH[1]}" -ge 9 ]; then
  ok "machineOld (crafted 10h old) reports age=$old_age (>= 9h)"
else
  bad "expected machineOld age >= 9h, got '$old_age' from line: $old_line"
fi

# ===========================================================================
echo "TEST 5: clear removes the ref"
# ===========================================================================
# clear now guards on open-PR state (issue #2655); pass the hermetic no-open-PR
# hook so the test never reaches gh/network. (The open-PR REFUSAL path is
# covered by TEST 20.)
(cd "$WORK" && CLAIM_OPEN_PR_CMD='exit 1' bash "$HB" clear machineOld >/dev/null 2>&1)
remaining=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineOld")
if [ -z "$remaining" ]; then
  ok "clear removed refs/heartbeats/machineOld"
else
  bad "refs/heartbeats/machineOld still present after clear: $remaining"
fi
# idempotent: clearing an already-absent ref must not error
if (cd "$WORK" && CLAIM_OPEN_PR_CMD='exit 1' bash "$HB" clear machineOld >/dev/null 2>&1); then
  ok "clear on an already-absent ref is a graceful no-op (exit 0)"
else
  bad "clear on an already-absent ref exited non-zero"
fi

# ===========================================================================
echo "TEST 6: beat does not modify the working tree or current branch"
# ===========================================================================
before_branch=$(g -C "$WORK" rev-parse --abbrev-ref HEAD)
before_head=$(g -C "$WORK" rev-parse HEAD)
before_status=$(g -C "$WORK" status --porcelain)
(cd "$WORK" && HEARTBEAT_MACHINE=machineA bash "$HB" beat 303 >/dev/null 2>&1)
after_branch=$(g -C "$WORK" rev-parse --abbrev-ref HEAD)
after_head=$(g -C "$WORK" rev-parse HEAD)
after_status=$(g -C "$WORK" status --porcelain)
if [ "$before_branch" = "$after_branch" ] && [ "$before_head" = "$after_head" ] && [ "$before_status" = "$after_status" ]; then
  ok "beat left branch ($after_branch), HEAD ($after_head), and working tree untouched"
else
  bad "beat mutated local state: branch $before_branch->$after_branch, HEAD $before_head->$after_head, status changed=$([ "$before_status" != "$after_status" ] && echo yes || echo no)"
fi

# ===========================================================================
echo "TEST 7: zero heartbeats renders gracefully"
# ===========================================================================
EMPTY_ORIGIN="$T/empty.git"
g init --bare -q "$EMPTY_ORIGIN"
(cd "$WORK" && g remote add empty-origin "$EMPTY_ORIGIN")
empty_out=$(cd "$WORK" && HEARTBEAT_REMOTE=empty-origin bash "$HB" list)
if printf '%s\n' "$empty_out" | grep -qi "no heartbeats found"; then
  ok "list against a heartbeat-free remote renders gracefully: '$empty_out'"
else
  bad "expected a graceful 'no heartbeats found' message, got: $empty_out"
fi

# ===========================================================================
echo "TEST 8: HEARTBEAT_REMOTE env var is honored (non-default remote name)"
# ===========================================================================
(cd "$WORK" && HEARTBEAT_REMOTE=empty-origin HEARTBEAT_MACHINE=machineC bash "$HB" beat 404 >/dev/null 2>&1)
on_alt=$(g -C "$WORK" ls-remote empty-origin "refs/heartbeats/machineC")
on_default=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineC")
if [ -n "$on_alt" ] && [ -z "$on_default" ]; then
  ok "HEARTBEAT_REMOTE=empty-origin routed the beat to the non-default remote only"
else
  bad "expected machineC only on empty-origin; on_alt='$on_alt' on_default='$on_default'"
fi

# craft_old_claim <work> <machine> <issue> <pid> <age_seconds> — like
# craft_old_heartbeat but writes a refs/machine-claims/<machine> ref carrying a pid, so
# should-reap / reap assertions don't depend on real sleeps.
craft_old_claim() {
  local work="$1" machine="$2" issue="$3" pid="$4" age="$5"
  (
    cd "$work" || exit 1
    local now_epoch old_epoch old_ts empty_tree csha
    now_epoch=$(date -u +%s)
    old_epoch=$((now_epoch - age))
    if ! old_ts=$(date -u -r "$old_epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null); then
      old_ts=$(date -u -d "@$old_epoch" +%Y-%m-%dT%H:%M:%SZ)
    fi
    empty_tree=$(git hash-object -t tree --stdin </dev/null)
    csha=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
      GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$empty_tree" -m "claim issue=${issue} machine=${machine} pid=${pid} ts=${old_ts}")
    g push -q origin "${csha}:refs/machine-claims/${machine}"
  )
}

# Hermetic open-PR hooks (never touch gh/network).
NO_OPEN_PR='exit 1'   # $1=issue -> always "no open PR"
HAS_OPEN_PR='exit 0'  # $1=issue -> always "has open PR"

# ===========================================================================
echo "TEST 9: stamp creates refs/machine-claims/<machine> with issue+pid"
# ===========================================================================
(cd "$WORK" && HEARTBEAT_MACHINE=claimA bash "$HB" stamp 900 4242 >/dev/null 2>&1)
claim_sha=$(g -C "$WORK" ls-remote origin "refs/machine-claims/claimA" | awk '{print $1}')
claim_msg=""
if [ -n "$claim_sha" ]; then
  g -C "$WORK" fetch -q origin "refs/machine-claims/claimA" 2>/dev/null
  claim_msg=$(g -C "$WORK" log -1 --format=%B FETCH_HEAD 2>/dev/null)
fi
if [ -n "$claim_sha" ] && printf '%s' "$claim_msg" | grep -q 'issue=900' \
  && printf '%s' "$claim_msg" | grep -q 'pid=4242'; then
  ok "stamp created refs/machine-claims/claimA carrying issue=900 pid=4242"
else
  bad "stamp did not create a well-formed claim ref (sha='$claim_sha' msg='$claim_msg')"
fi

# ===========================================================================
echo "TEST 10: list-claims renders machine/issue/pid/age"
# ===========================================================================
lc_out=$(cd "$WORK" && bash "$HB" list-claims)
if printf '%s\n' "$lc_out" | grep -qE '^claimA +900 +4242 '; then
  ok "list-claims rendered claimA issue=900 pid=4242"
else
  bad "list-claims output missing expected row:
$lc_out"
fi

# ===========================================================================
echo "TEST 11: should-reap KEEPS a fresh claim (age <= threshold)"
# ===========================================================================
if (cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" should-reap claimA 14400 >/dev/null 2>&1); then
  bad "should-reap returned reap(0) for a FRESH claim — must keep"
else
  ok "should-reap keeps a fresh claim (exit non-zero)"
fi

# ===========================================================================
echo "TEST 12: should-reap REAPS a stale claim with no open PR (foreign machine)"
# ===========================================================================
craft_old_claim "$WORK" "claimStale" 901 5555 36000  # 10h old
if (cd "$WORK" && HEARTBEAT_MACHINE=someOtherMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap claimStale 14400 >/dev/null 2>&1); then
  ok "should-reap reaps a stale, no-open-PR, foreign-machine claim (exit 0)"
else
  bad "should-reap did NOT reap a 10h-old no-open-PR foreign claim"
fi

# ===========================================================================
echo "TEST 13: should-reap KEEPS a stale claim that has an open PR"
# ===========================================================================
if (cd "$WORK" && HEARTBEAT_MACHINE=someOtherMachine CLAIM_OPEN_PR_CMD="$HAS_OPEN_PR" \
  bash "$HB" should-reap claimStale 14400 >/dev/null 2>&1); then
  bad "should-reap reaped a claim WITH an open PR — must keep (endgame in flight)"
else
  ok "should-reap keeps a stale claim that has an open PR"
fi

# ===========================================================================
echo "TEST 14: should-reap KEEPS a stale LOCAL claim whose pid is still alive"
# ===========================================================================
# A long-lived local pid: use this test process's own pid ($$), guaranteed alive.
craft_old_claim "$WORK" "localMachine" 902 "$$" 36000
if (cd "$WORK" && HEARTBEAT_MACHINE=localMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap localMachine 14400 >/dev/null 2>&1); then
  bad "should-reap reaped a LOCAL claim whose pid ($$) is still alive — must keep"
else
  ok "should-reap keeps a stale local claim whose pid is still alive"
fi

# ===========================================================================
echo "TEST 15: should-reap REAPS a stale LOCAL claim whose pid is dead"
# ===========================================================================
# A pid that is essentially never alive. 999999 is beyond default pid_max on
# Linux/macOS; kill -0 fails -> dead.
craft_old_claim "$WORK" "localMachine2" 903 999999 36000
if (cd "$WORK" && HEARTBEAT_MACHINE=localMachine2 CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap localMachine2 14400 >/dev/null 2>&1); then
  ok "should-reap reaps a stale local claim whose pid is dead"
else
  bad "should-reap did NOT reap a stale local claim with a dead pid"
fi

# ===========================================================================
echo "TEST 16: should-reap KEEPS on unparseable ts (never reap on unknown age)"
# ===========================================================================
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  cs=$(GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim issue=904 machine=badTs pid=1 ts=not-a-date")
  g push -q origin "${cs}:refs/machine-claims/badTs"
)
if (cd "$WORK" && HEARTBEAT_MACHINE=someOtherMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap badTs 14400 >/dev/null 2>&1); then
  bad "should-reap reaped a claim with an unparseable ts — must keep on unknown age"
else
  ok "should-reap keeps a claim with an unparseable ts"
fi

# ===========================================================================
echo "TEST 17: should-reap returns 2 (no ref) for an absent claim"
# ===========================================================================
rc=0
(cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" should-reap doesNotExist 14400 >/dev/null 2>&1) || rc=$?
if [ "$rc" -eq 2 ]; then
  ok "should-reap returns exit 2 for a nonexistent claim ref"
else
  bad "should-reap returned $rc for a nonexistent claim (expected 2)"
fi

# ===========================================================================
echo "TEST 18: reap DELETES a claim with no open PR"
# ===========================================================================
(cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" reap claimStale >/dev/null 2>&1)
if [ -z "$(g -C "$WORK" ls-remote origin "refs/machine-claims/claimStale")" ]; then
  ok "reap deleted refs/machine-claims/claimStale (no open PR)"
else
  bad "reap did NOT delete refs/machine-claims/claimStale"
fi

# ===========================================================================
echo "TEST 19: reap REFUSES to delete a claim whose issue has an open PR"
# ===========================================================================
craft_old_claim "$WORK" "claimHasPR" 905 6666 36000
rc=0
(cd "$WORK" && CLAIM_OPEN_PR_CMD="$HAS_OPEN_PR" bash "$HB" reap claimHasPR >/dev/null 2>&1) || rc=$?
still_there=$(g -C "$WORK" ls-remote origin "refs/machine-claims/claimHasPR")
if [ "$rc" -eq 3 ] && [ -n "$still_there" ]; then
  ok "reap refused (exit 3) to delete a claim with an open PR; ref preserved"
else
  bad "reap should have refused an open-PR claim: rc=$rc still_there='$still_there'"
fi

# ===========================================================================
echo "TEST 20: clear REFUSES to delete a heartbeat whose issue has an open PR"
# ===========================================================================
(cd "$WORK" && HEARTBEAT_MACHINE=hbHasPR bash "$HB" beat 906 >/dev/null 2>&1)
rc=0
(cd "$WORK" && CLAIM_OPEN_PR_CMD="$HAS_OPEN_PR" bash "$HB" clear hbHasPR >/dev/null 2>&1) || rc=$?
hb_still=$(g -C "$WORK" ls-remote origin "refs/heartbeats/hbHasPR")
if [ "$rc" -eq 3 ] && [ -n "$hb_still" ]; then
  ok "clear refused (exit 3) a heartbeat with an open PR; ref preserved"
else
  bad "clear should have refused an open-PR heartbeat: rc=$rc still='$hb_still'"
fi

# ===========================================================================
echo "TEST 21: clear DELETES a heartbeat with no open PR (default behavior intact)"
# ===========================================================================
(cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" clear hbHasPR >/dev/null 2>&1)
if [ -z "$(g -C "$WORK" ls-remote origin "refs/heartbeats/hbHasPR")" ]; then
  ok "clear deleted a heartbeat with no open PR"
else
  bad "clear did NOT delete a no-open-PR heartbeat"
fi

# ===========================================================================
echo "TEST 22: dead-lanes reports a LOCAL claim whose pid is DEAD, with no 4h wait (#3393 AC3)"
# ===========================================================================
# The silence this closes: `should-reap` only looks at the pid AFTER age > threshold
# (4h default), so a worker OOM-killed one minute ago is indistinguishable from a
# healthy one for four hours — and even then the answer is an exit code, not a report.
# A FRESH claim with a dead pid is the exact shape of an OOM kill, so that is the fixture.
craft_old_claim "$WORK" "deadFresh" 3393 999999 30   # 30s old: far INSIDE the reap threshold
dl_out=$(cd "$WORK" && HEARTBEAT_MACHINE=deadFresh CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
dl_rc=$?
if [ "$dl_rc" -eq 3 ] \
  && printf '%s\n' "$dl_out" | grep -q 'DEAD-NO-PROCESS' \
  && printf '%s\n' "$dl_out" | grep -q 'deadFresh' \
  && printf '%s\n' "$dl_out" | grep -q '3393'; then
  ok "dead-lanes reported DEAD-NO-PROCESS for a 30s-old claim with a dead pid (rc=3), naming machine and issue"
else
  bad "dead-lanes must report a fresh dead-pid claim with rc=3: rc=$dl_rc out:
$dl_out"
fi
# NON-VACUITY: should-reap KEEPS that very same claim, so the new report is not a
# restatement of an existing signal — it sees something should-reap cannot.
if (cd "$WORK" && HEARTBEAT_MACHINE=deadFresh CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap deadFresh 14400 >/dev/null 2>&1); then
  bad "NON-VACUITY broken: should-reap reaped the fresh claim, so dead-lanes adds nothing"
else
  ok "NON-VACUITY: should-reap KEEPS the same fresh dead-pid claim (age gate) — dead-lanes is the only signal that sees it"
fi

# ===========================================================================
echo "TEST 23: a LIVE local pid is reported ALIVE, and does not set the dead exit code"
# ===========================================================================
# age 0, not 30: a supervisor cannot stamp a claim BEFORE its own process started, so a
# "30s-old" claim naming a process that started 20s ago is a self-inconsistent fixture —
# and the identity check correctly reads it as UNKNOWN-IDENTITY. Realistic ordering is
# process first, stamp after.
craft_old_claim "$WORK" "aliveLocal" 3394 "$$" 0
al_out=$(cd "$WORK" && HEARTBEAT_MACHINE=aliveLocal CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
al_rc=$?
# machine=aliveLocal is the local identity here, so ITS row must read ALIVE — and the
# deadFresh claim, being FOREIGN from this vantage point, is UNKNOWN-FOREIGN rather than
# dead, so nothing sets the dead code and rc is 0. That asymmetry is the point of the
# three-valued verdict: the same ref reads DEAD on its own machine and UNKNOWN elsewhere.
if printf '%s\n' "$al_out" | grep -E '^aliveLocal ' | grep -q 'ALIVE' && [ "$al_rc" -eq 0 ]; then
  ok "dead-lanes reported ALIVE for a live local pid, rc=0 (the same deadFresh ref reads UNKNOWN-FOREIGN from here, never dead)"
else
  bad "dead-lanes must report ALIVE for a live local pid: out:
$al_out"
fi

# ===========================================================================
echo "TEST 24: a FOREIGN claim is UNKNOWN-FOREIGN — never guessed alive OR dead"
# ===========================================================================
# A foreign machine's pid is unknowable from here (the header says so for should-reap).
# Reporting it as dead would page an operator about a healthy box; reporting it as alive
# would be a permissive branch keyed on an unmeasured signal, which is the vacuous-pass
# shape doctrine forbids. So it gets its OWN token.
fo_out=$(cd "$WORK" && HEARTBEAT_MACHINE=someThirdMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$fo_out" | grep -E '^deadFresh ' | grep -q 'UNKNOWN-FOREIGN'; then
  ok "dead-lanes reported UNKNOWN-FOREIGN for a claim owned by another machine (neither alive nor dead)"
else
  bad "a foreign claim must be UNKNOWN-FOREIGN: out:
$fo_out"
fi

# ===========================================================================
echo "TEST 25: an OPEN PR does NOT suppress a dead-lane report (#2499 orphaned endgame)"
# ===========================================================================
# This is the sharpest difference from should-reap, which KEEPS (and stays silent about)
# a claim with an open PR. A dead process holding an unfinished endgame is the MOST
# important thing to say out loud — it must be reported AND must not be reaped.
pr_out=$(cd "$WORK" && HEARTBEAT_MACHINE=deadFresh CLAIM_OPEN_PR_CMD="$HAS_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
pr_rc=$?
if [ "$pr_rc" -eq 3 ] \
  && printf '%s\n' "$pr_out" | grep -E '^deadFresh ' | grep -q 'DEAD-NO-PROCESS' \
  && printf '%s\n' "$pr_out" | grep -E '^deadFresh ' | grep -q 'open-pr=yes'; then
  ok "dead-lanes still reports a dead lane that holds an OPEN PR, annotating open-pr=yes (reap would refuse; the report must not)"
else
  bad "an open PR must not suppress the dead-lane report: rc=$pr_rc out:
$pr_out"
fi
# ...and reap still REFUSES it, so reporting has not become reaping.
if (cd "$WORK" && HEARTBEAT_MACHINE=deadFresh CLAIM_OPEN_PR_CMD="$HAS_OPEN_PR" \
  bash "$HB" reap deadFresh >/dev/null 2>&1); then
  bad "reap deleted an open-PR claim — reporting must not have relaxed reaping"
else
  ok "reap still REFUSES the same open-PR claim: the report is loud, the reaper stays conservative"
fi

# ===========================================================================
echo "TEST 26: a claim ref with NO pid is UNKNOWN-NO-PID, not silently alive"
# ===========================================================================
# A pre-#2655 or hand-crafted ref carries no pid. Two-valued logic would fold that
# onto one answer; the permissive fold ("assume alive") is how a dead lane goes unseen.
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  cs=$(GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim issue=3395 machine=noPid ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)")
  g push -q origin "${cs}:refs/machine-claims/noPid"
)
np_out=$(cd "$WORK" && HEARTBEAT_MACHINE=noPid CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$np_out" | grep -E '^noPid ' | grep -q 'UNKNOWN-NO-PID'; then
  ok "dead-lanes reported UNKNOWN-NO-PID for a claim ref carrying no pid (never folded onto ALIVE)"
else
  bad "a pid-less claim must be UNKNOWN-NO-PID: out:
$np_out"
fi

# ===========================================================================
echo "TEST 27: zero claims is exit 0 and says so (an empty fleet is not a dead lane)"
# ===========================================================================
empty_origin="$T/origin-empty.git"
empty_work="$T/work-empty"
g init --bare -q "$empty_origin"
g clone -q "$empty_origin" "$empty_work" 2>/dev/null
(
  cd "$empty_work" || exit 1
  echo seed >seed.txt; g add seed.txt; g commit -qm seed; g push -q -u origin main
)
# INVERTED in round 5 (Medium). This case used to demand exit 0, on the reasoning that
# "an empty fleet is not a finding". But an empty NAMESPACE does not establish an idle
# fleet: a lane running with claim stamping disabled (`CLAIM_CMD=""`, a documented
# supervisor option) or one whose stamps have been failing looks identical from here. So
# exit 0 would have reported clean about lanes that were never measured — the same
# false-clean as the all-foreign case, and by the same rule (`local_seen == 0`).
ze_out=$(cd "$empty_work" && bash "$HB" dead-lanes 2>&1)
ze_rc=$?
if [ "$ze_rc" -eq 1 ] \
  && printf '%s\n' "$ze_out" | grep -qi 'not the same as an idle fleet' \
  && printf '%s\n' "$ze_out" | grep -q 'CLAIM_CMD'; then
  ok "zero claim refs exits 1 and explains that an empty namespace is not an idle fleet (naming the disabled-stamping case)"
else
  bad "zero claims must be exit 1 with the reason stated: rc=$ze_rc out:
$ze_out"
fi

# ===========================================================================
echo "TEST 28: dead-lanes is documented in --help, and rejects a stray argument"
# ===========================================================================
help_out=$(cd "$WORK" && bash "$HB" --help 2>&1 || true)
if printf '%s\n' "$help_out" | grep -q 'dead-lanes'; then
  ok "dead-lanes appears in --help (an undocumented subcommand is one nobody runs)"
else
  bad "dead-lanes must be documented in --help"
fi
# The message must come from dead-lanes ITSELF, not from the unknown-subcommand arm —
# both exit 64, so asserting only the code would have passed before the subcommand existed.
stray_out=$(cd "$WORK" && bash "$HB" dead-lanes extra-arg 2>&1)
stray_rc=$?
if [ "$stray_rc" -eq 64 ] \
  && printf '%s\n' "$stray_out" | grep -q 'dead-lanes takes no arguments' \
  && ! printf '%s\n' "$stray_out" | grep -q 'unknown subcommand'; then
  ok "dead-lanes rejects a stray argument with exit 64 and its OWN diagnostic (not the unknown-subcommand arm)"
else
  bad "dead-lanes must reject a stray argument with its own exit-64 diagnostic: rc=$stray_rc out: $stray_out"
fi

# ===========================================================================
echo "TEST 29: a git FAILURE is not an empty fleet — dead-lanes exits 1, never 0"
# ===========================================================================
# roborev round 1 (Medium): `git ls-remote ... || true` turned an OUTAGE into
# "no claims found" + exit 0. That is the shape doctrine forbids — a pass derived
# from the ABSENCE of a bad signal — and for a monitor it is the worst direction:
# during the very outage when lanes are dying, it reports all clear.
ls_out=$(cd "$WORK" && HEARTBEAT_REMOTE=no-such-remote bash "$HB" dead-lanes 2>&1)
ls_rc=$?
if [ "$ls_rc" -eq 1 ] && ! printf '%s\n' "$ls_out" | grep -qi 'no claims found'; then
  ok "dead-lanes on an unreachable remote exits 1 and does NOT claim an empty fleet"
else
  bad "a git failure must be exit 1, not an empty-fleet exit 0: rc=$ls_rc out:
$ls_out"
fi
if printf '%s\n' "$ls_out" | grep -qiE 'could not|failed|unreachable|cannot'; then
  ok "the git-failure diagnostic says the measurement failed (not that nothing was found)"
else
  bad "the git-failure path must name the failure: $ls_out"
fi

# ===========================================================================
echo "TEST 30: an UNREADABLE claim ref makes the measurement INCOMPLETE (exit 1)"
# ===========================================================================
# A ref that lists but will not fetch is "we cannot tell about this lane", which must
# not be reported as "this lane is fine". Crafted by writing a ref in the bare origin
# that points at an object the origin does not have.
printf '%s\n' "0000000000000000000000000000000000000000" >/dev/null
dangling="deadbeef00000000000000000000000000000000"
mkdir -p "$ORIGIN/refs/machine-claims"
printf '%s\n' "$dangling" >"$ORIGIN/refs/machine-claims/unreadableMachine"
ur_out=$(cd "$WORK" && HEARTBEAT_MACHINE=unreadableMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
ur_rc=$?
if printf '%s\n' "$ur_out" | grep -q 'UNKNOWN-UNREADABLE' && [ "$ur_rc" -eq 1 ]; then
  ok "an unfetchable claim ref is UNKNOWN-UNREADABLE and makes the run exit 1 (incomplete, not clean)"
else
  bad "an unreadable ref must yield UNKNOWN-UNREADABLE + exit 1: rc=$ur_rc out:
$ur_out"
fi
rm -f "$ORIGIN/refs/machine-claims/unreadableMachine"

# ===========================================================================
echo "TEST 31: a DEAD lane still wins the exit code over an incomplete measurement"
# ===========================================================================
# Precedence is stated in the header: a found dead lane is ACTIONABLE NOW, so exit 3
# outranks exit 1, and the incompleteness is reported in the text rather than lost.
# ORDER MATTERS: a dangling ref in the bare origin makes EVERY subsequent push to it
# fail ("missing necessary objects"), so the dead claim must be pushed BEFORE the
# unreadable ref is planted — otherwise the fixture silently has no dead lane in it.
craft_old_claim "$WORK" "deadPrecedence" 3396 999999 30
printf '%s\n' "$dangling" >"$ORIGIN/refs/machine-claims/unreadableMachine"
pr2_out=$(cd "$WORK" && HEARTBEAT_MACHINE=deadPrecedence CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
pr2_rc=$?
if [ "$pr2_rc" -eq 3 ] \
  && printf '%s\n' "$pr2_out" | grep -q 'DEAD-NO-PROCESS' \
  && printf '%s\n' "$pr2_out" | grep -qi 'incomplete'; then
  ok "a dead lane returns 3 even alongside an unreadable ref, and the incompleteness is still reported"
else
  bad "dead (3) must outrank incomplete (1), with the incompleteness still stated: rc=$pr2_rc out:
$pr2_out"
fi
# Remove the dangling ref BEFORE the cleanup push, for the same reason.
rm -f "$ORIGIN/refs/machine-claims/unreadableMachine"
(cd "$WORK" && g push -q origin ":refs/machine-claims/deadPrecedence" 2>/dev/null || true)

# ===========================================================================
echo "TEST 32: issue=0 (supervisor stamped before the issue was known) is not queried as a PR"
# ===========================================================================
# worker-supervisor.sh stamps issue "0" when the issue is still unknown for that
# iteration. Treating 0 as a real issue number would send the open-PR probe hunting
# for PR #0 and print a bogus issue in the report.
craft_old_claim "$WORK" "zeroIssue" 0 999999 30
# A MARKER FILE, not a grep of the hook's output (roborev round 3, Low): `open_pr_state`
# sends the hook's stdout AND stderr to /dev/null, so an output-based negative assertion
# passed whether or not the hook ran — it could not fail. A file survives the redirect.
probe_marker="$T/probe-ran-marker"
rm -f "$probe_marker"
zi_out=$(cd "$WORK" && HEARTBEAT_MACHINE=zeroIssue \
  CLAIM_OPEN_PR_CMD="touch '$probe_marker'; exit 0" bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$zi_out" | grep -E '^zeroIssue ' | grep -q 'DEAD-NO-PROCESS' \
  && [ ! -e "$probe_marker" ]; then
  ok "a claim stamped issue=0 is still reported DEAD, without probing for a PR on issue 0"
else
  bad "issue=0 must be reported dead but never PR-probed (marker present=$([ -e "$probe_marker" ] && echo yes || echo no)): out:
$zi_out"
fi
# NON-VACUITY of the marker itself: with a REAL issue number the same hook DOES run and
# DOES create the marker. Without this, an always-absent marker would prove nothing.
rm -f "$probe_marker"
(cd "$WORK" && HEARTBEAT_MACHINE=zeroIssue2 \
  CLAIM_OPEN_PR_CMD="touch '$probe_marker'; exit 0" bash "$HB" dead-lanes >/dev/null 2>&1) || true
craft_old_claim "$WORK" "zeroIssue2" 3401 999999 30
rm -f "$probe_marker"
(cd "$WORK" && HEARTBEAT_MACHINE=zeroIssue2 \
  CLAIM_OPEN_PR_CMD="touch '$probe_marker'; exit 0" bash "$HB" dead-lanes >/dev/null 2>&1) || true
if [ -e "$probe_marker" ]; then
  ok "NON-VACUITY: the same hook DOES fire (marker created) for a real issue number, so the absent-marker assertion above is meaningful"
else
  bad "NON-VACUITY broken: the PR-probe hook never fires at all, so TEST 32 proves nothing"
fi
rm -f "$probe_marker"
(cd "$WORK" && g push -q origin ":refs/machine-claims/zeroIssue2" 2>/dev/null || true)

# ===========================================================================
echo "TEST 33: the recorded pid is the SUPERVISOR's — the semantic dead-lanes actually tests"
# ===========================================================================
# roborev round 1 (High) was right about the mechanism: worker-supervisor.sh stamps
# its OWN pid ($$) — "the stable per-machine anchor, not a transient worker
# subprocess" (its own comment). So DEAD-NO-PROCESS means the LANE-OWNING process is
# gone (the whole tmux scope died, which is what #3393's three lane deaths did), NOT
# that some worker subprocess was killed under a live supervisor. Pinned here so the
# documentation cannot drift from the mechanism, and so nobody "fixes" dead-lanes to
# expect a worker pid without also changing what stamp records.
if grep -q 'PID stamped is the SUPERVISOR' "$SCRIPT_DIR/../local/worker-supervisor.sh" \
  && grep -qE '\$CLAIM_CMD stamp "\$issue" "\$\$"' "$SCRIPT_DIR/../local/worker-supervisor.sh"; then
  ok "the supervisor stamps its OWN pid, so DEAD-NO-PROCESS means the lane-owning process is gone (semantic pinned)"
else
  bad "worker-supervisor.sh no longer stamps \$\$ — dead-lanes' documented meaning must be revisited"
fi
if grep -q 'the SUPERVISOR' "$HB" && grep -qi 'worker-only kill' "$HB"; then
  ok "claim-heartbeat.sh documents WHOSE pid it checks and states the worker-only-kill non-coverage"
else
  bad "dead-lanes must document whose pid it checks and what it does NOT cover"
fi

# ===========================================================================
echo "TEST 34: a LOCAL pid-less claim makes the run INCOMPLETE; a FOREIGN one does not"
# ===========================================================================
# roborev round 2 (Medium): UNKNOWN-NO-PID was printed but not counted, so the run
# could exit 0 while unable to judge a lane on THIS machine. The foreign case is
# deliberately different: a foreign pid is unknowable BY DESIGN, so counting it would
# make every multi-machine fleet exit 1 forever and train everyone to ignore the code.
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  cs=$(GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim issue=3397 machine=noPidLocal ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)")
  g push -q origin "${cs}:refs/machine-claims/noPidLocal"
)
npl_out=$(cd "$WORK" && HEARTBEAT_MACHINE=noPidLocal CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
npl_rc=$?
if [ "$npl_rc" -eq 1 ] && printf '%s\n' "$npl_out" | grep -E '^noPidLocal ' | grep -q 'UNKNOWN-NO-PID'; then
  ok "a LOCAL pid-less claim is UNKNOWN-NO-PID and makes the run exit 1 (cannot judge != clean)"
else
  bad "a local pid-less claim must make the run incomplete (exit 1): rc=$npl_rc out:
$npl_out"
fi
# Same ref, viewed from ANOTHER machine: still UNKNOWN-FOREIGN per row, but the RUN as a
# whole measured no local process at all — so it must NOT report a clean fleet
# (roborev round 4, High). This assertion previously demanded exit 0 here, which let a
# run from an operator or CI box say "no dead lanes" about a fleet it never inspected.
fpl_out=$(cd "$WORK" && HEARTBEAT_MACHINE=someFarMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
fpl_rc=$?
if [ "$fpl_rc" -eq 1 ] \
  && printf '%s\n' "$fpl_out" | grep -E '^noPidLocal ' | grep -q 'UNKNOWN-FOREIGN' \
  && printf '%s\n' "$fpl_out" | grep -qi 'LOCAL-ONLY'; then
  ok "a run from a machine owning NO claim exits 1 and says it is LOCAL-ONLY — never 'no dead lanes' about a fleet it never inspected"
else
  bad "an all-foreign run measured nothing and must exit 1 saying so: rc=$fpl_rc out:
$fpl_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/noPidLocal" 2>/dev/null || true)

# ===========================================================================
echo "TEST 35: a RECYCLED pid is not reported ALIVE (roborev round 2, Medium)"
# ===========================================================================
# `kill -0` cannot tell the stamped supervisor from an unrelated process that later
# inherited its pid — and that error runs in the DANGEROUS direction: a dead lane
# reported alive is precisely the silence #3393 is about. The claim's own `ts` settles
# it without changing what `stamp` records: a process that STARTED AFTER the claim was
# stamped cannot be the process that stamped it.
tail -f /dev/null &
live_pid=$!
craft_old_claim "$WORK" "reusedPid" 3398 "$live_pid" 7200   # claim stamped 2h BEFORE this process started
ru_out=$(cd "$WORK" && HEARTBEAT_MACHINE=reusedPid CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
ru_rc=$?
if [ "$ru_rc" -eq 3 ] && printf '%s\n' "$ru_out" | grep -E '^reusedPid ' | grep -q 'DEAD-PID-REUSED'; then
  ok "a LIVE pid that started AFTER its claim was stamped is DEAD-PID-REUSED, not ALIVE (rc=3)"
else
  bad "a recycled pid must not read ALIVE: rc=$ru_rc out:
$ru_out"
fi
# NON-VACUITY: that pid really is alive, so a bare `kill -0` would have said ALIVE.
if kill -0 "$live_pid" 2>/dev/null; then
  ok "NON-VACUITY: the pid above IS live (kill -0 succeeds), so the pre-fix check would have reported ALIVE"
else
  bad "NON-VACUITY broken: the fixture pid is not alive, so TEST 35 proves nothing"
fi
kill "$live_pid" 2>/dev/null
wait "$live_pid" 2>/dev/null
(cd "$WORK" && g push -q origin ":refs/machine-claims/reusedPid" 2>/dev/null || true)

# ===========================================================================
echo "TEST 36: a live pid CONSISTENT with its claim ts is ALIVE, and says identity=verified"
# ===========================================================================
# The accept half of TEST 35 — without it, a check that called everything reused would
# pass TEST 35 while being useless.
# The TEST SHELL's own pid: alive, and started well before the claim is stamped, so
# start < ts holds by seconds rather than by luck. A freshly-spawned helper lands within
# the rounding window and would (correctly) read UNKNOWN-IDENTITY, making this case flaky.
craft_old_claim "$WORK" "goodPid" 3399 "$$" 0
gp_out=$(cd "$WORK" && HEARTBEAT_MACHINE=goodPid CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$gp_out" | grep -E '^goodPid ' | grep -q 'ALIVE' \
  && printf '%s\n' "$gp_out" | grep -E '^goodPid ' | grep -q 'identity=verified'; then
  ok "a live pid whose start precedes its claim ts is ALIVE with identity=verified"
else
  bad "a consistent live pid must read ALIVE/identity=verified: out:
$gp_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/goodPid" 2>/dev/null || true)

# ===========================================================================
echo "TEST 37: a FAILED PR probe renders open-pr=unknown, never a definite yes"
# ===========================================================================
# roborev round 2 (Low): `issue_has_open_pr` is fail-SAFE for the REAPER (a gh outage
# reads as "has open PR", so nothing is reaped on unproven information). Rendering that
# same guess as a definite `open-pr=yes` told the operator an orphaned endgame exists
# when the probe had simply failed. Reporting needs three states, not two.
craft_old_claim "$WORK" "probeFail" 3400 999999 30
pf_out=$(cd "$WORK" && HEARTBEAT_MACHINE=probeFail \
  CLAIM_OPEN_PR_CMD='exit 7' bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$pf_out" | grep -E '^probeFail ' | grep -q 'open-pr=unknown' \
  && ! printf '%s\n' "$pf_out" | grep -E '^probeFail ' | grep -q 'open-pr=yes'; then
  ok "a failed PR probe renders open-pr=unknown (never a definite yes on a guess)"
else
  bad "a failed PR probe must render unknown: out:
$pf_out"
fi
# ...and a CONFIRMED open PR still says yes, so unknown has not swallowed the real signal.
pf2_out=$(cd "$WORK" && HEARTBEAT_MACHINE=probeFail \
  CLAIM_OPEN_PR_CMD="$HAS_OPEN_PR" bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$pf2_out" | grep -E '^probeFail ' | grep -q 'open-pr=yes'; then
  ok "a CONFIRMED open PR still renders open-pr=yes (the orphaned-endgame signal survives)"
else
  bad "a confirmed open PR must still say yes: out:
$pf2_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/probeFail" 2>/dev/null || true)

# ===========================================================================
echo "TEST 38: the pid-identity check is TIMEZONE-FREE (roborev round 3, Medium)"
# ===========================================================================
# The first cut read `ps -o lstart=` (LOCAL wall time, no zone) and parsed it with
# `date -u`, so on a non-UTC host the start epoch was shifted by the whole offset —
# far past PID_IDENTITY_SLACK_SECS, which would declare a live supervisor
# DEAD-PID-REUSED. Driven by running the check under a deliberately skewed TZ: the
# verdict must not depend on it.
craft_old_claim "$WORK" "tzMachine" 3402 "$$" 0
tz_utc=$(cd "$WORK" && TZ=UTC HEARTBEAT_MACHINE=tzMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1 | grep -E '^tzMachine ' | awk '{print $4}')
tz_ist=$(cd "$WORK" && TZ=Asia/Kolkata HEARTBEAT_MACHINE=tzMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1 | grep -E '^tzMachine ' | awk '{print $4}')
tz_neg=$(cd "$WORK" && TZ=America/Los_Angeles HEARTBEAT_MACHINE=tzMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1 | grep -E '^tzMachine ' | awk '{print $4}')
# The property is AGREEMENT plus the absence of a false recycle — asserted rather than a
# specific verdict, so the case cannot go green merely because every zone is equally wrong.
if [ "$tz_utc" = "$tz_ist" ] && [ "$tz_ist" = "$tz_neg" ] && [ "$tz_utc" = "ALIVE" ]; then
  ok "the identity verdict is identical under UTC, +05:30 and -07:00 (ALIVE in all three) — no timezone dependence"
else
  bad "the identity check must not depend on TZ: UTC=$tz_utc IST=$tz_ist PST=$tz_neg"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/tzMachine" 2>/dev/null || true)

# ===========================================================================
echo "TEST 39: an UNVERIFIABLE identity is UNKNOWN-IDENTITY + incomplete, never ALIVE"
# ===========================================================================
# roborev round 3 (Medium): reporting ALIVE with an `identity=unverified` annotation
# still let the run exit 0, so a recycled pid reproduced the false-clean this monitor
# exists to prevent. An annotation is not a substitute for an exit code — nobody greps
# the annotation. Driven by shimming `ps` so existence succeeds but elapsed time is
# unavailable, which is exactly the state that used to be called ALIVE.
shimdir="$T/psshim"
mkdir -p "$shimdir"
cat >"$shimdir/ps" <<'PSEOF'
#!/usr/bin/env bash
# Existence probe (`ps -p <pid>`) succeeds; every elapsed-time query returns nothing.
for a in "$@"; do
  case "$a" in
    etimes=|etime=) exit 0 ;;
  esac
done
exit 0
PSEOF
chmod +x "$shimdir/ps"
tail -f /dev/null &
shim_pid=$!
craft_old_claim "$WORK" "shimMachine" 3403 "$shim_pid" 0
shim_out=$(cd "$WORK" && PATH="$shimdir:$PATH" HEARTBEAT_MACHINE=shimMachine \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
shim_rc=$?
if printf '%s\n' "$shim_out" | grep -E '^shimMachine ' | grep -q 'UNKNOWN-IDENTITY' \
  && [ "$shim_rc" -eq 1 ]; then
  ok "an unverifiable pid identity is UNKNOWN-IDENTITY and makes the run incomplete (exit 1), never a clean ALIVE"
else
  bad "an unverifiable identity must be UNKNOWN-IDENTITY + exit 1: rc=$shim_rc out:
$shim_out"
fi
kill "$shim_pid" 2>/dev/null
wait "$shim_pid" 2>/dev/null
(cd "$WORK" && g push -q origin ":refs/machine-claims/shimMachine" 2>/dev/null || true)

# ===========================================================================
echo "TEST 40: a git WARNING on stderr is not parsed as a claim ref (round 3, Low)"
# ===========================================================================
# `2>&1` merged git/SSH warnings into the ref listing, where every warning line became
# a bogus row printed as UNKNOWN-UNREADABLE — and flipped the exit code to 1 on a
# healthy fleet. Driven by shimming `git` to emit a warning on stderr while returning a
# perfectly good listing on stdout.
gitshim="$T/gitshim"
mkdir -p "$gitshim"
cat >"$gitshim/git" <<'GITEOF'
#!/usr/bin/env bash
for a in "$@"; do
  if [ "$a" = "ls-remote" ]; then
    echo "Warning: Permanently added a host key to the list of known hosts." >&2
    echo "warning: redirecting to https://example.invalid/repo.git" >&2
    exec /usr/bin/git "$@"
  fi
done
exec /usr/bin/git "$@"
GITEOF
chmod +x "$gitshim/git"
craft_old_claim "$WORK" "warnMachine" 3404 "$$" 0
warn_out=$(cd "$WORK" && PATH="$gitshim:$PATH" HEARTBEAT_MACHINE=warnMachine \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
if ! printf '%s\n' "$warn_out" | grep -q 'UNKNOWN-UNREADABLE' \
  && ! printf '%s\n' "$warn_out" | grep -qE '^Warning:|^warning:'; then
  ok "git warnings on stderr produce no bogus ref rows and no spurious UNKNOWN-UNREADABLE"
else
  bad "stderr must not be parsed as the ref listing: out:
$warn_out"
fi
# NON-VACUITY: the shim really did emit those warnings (otherwise this asserts nothing).
if PATH="$gitshim:$PATH" git ls-remote "$ORIGIN" 'refs/machine-claims/*' 2>&1 >/dev/null | grep -q 'Permanently added'; then
  ok "NON-VACUITY: the git shim does emit stderr warnings during ls-remote"
else
  bad "NON-VACUITY broken: the git shim emitted no warning, so TEST 40 proves nothing"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/warnMachine" 2>/dev/null || true)

# ===========================================================================
echo "TEST 41: one HEALTHY local lane + foreign lanes still exits 0 (round 4 balance)"
# ===========================================================================
# The other side of TEST 34: foreign rows must not EACH count as incomplete, or a
# healthy multi-machine fleet would sit at exit 1 forever and everyone would learn to
# ignore the code. The rule is "did I measure ANY local lane", not "is every lane local".
craft_old_claim "$WORK" "mixedLocal" 3405 "$$" 0
mixed_out=$(cd "$WORK" && HEARTBEAT_MACHINE=mixedLocal CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
mixed_rc=$?
foreign_rows=$(printf '%s\n' "$mixed_out" | grep -c 'UNKNOWN-FOREIGN' || true)
if [ "$mixed_rc" -eq 0 ] \
  && [ "$foreign_rows" -gt 0 ] \
  && printf '%s\n' "$mixed_out" | grep -E '^mixedLocal ' | grep -q 'ALIVE'; then
  ok "one measured-healthy local lane alongside $foreign_rows foreign rows exits 0 — foreign rows alone do not force incompleteness"
else
  bad "a healthy local lane must yield exit 0 despite foreign rows: rc=$mixed_rc foreign=$foreign_rows out:
$mixed_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/mixedLocal" 2>/dev/null || true)

# ===========================================================================
echo "TEST 42: a pid recycled INSIDE the rounding window is UNKNOWN, not ALIVE"
# ===========================================================================
# roborev round 4 (Medium): with a 60s slack, `ts < start <= ts+60` was reported ALIVE —
# so a pid recycled within a minute read clean. The window absorbs second-resolution
# rounding; it is not evidence of identity. Driven by crafting a claim stamped 1s BEFORE
# a live process started, i.e. inside the tolerance but still not verifiable.
tail -f /dev/null &
win_pid=$!
craft_old_claim "$WORK" "windowPid" 3406 "$win_pid" 1
win_out=$(cd "$WORK" && PID_IDENTITY_SLACK_SECS=30 HEARTBEAT_MACHINE=windowPid \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
win_rc=$?
if printf '%s\n' "$win_out" | grep -E '^windowPid ' | grep -q 'UNKNOWN-IDENTITY' \
  && [ "$win_rc" -eq 1 ]; then
  ok "a start time inside the rounding window is UNKNOWN-IDENTITY + exit 1, never a clean ALIVE"
else
  bad "the slack window must not produce ALIVE: rc=$win_rc out:
$win_out"
fi
kill "$win_pid" 2>/dev/null
wait "$win_pid" 2>/dev/null
(cd "$WORK" && g push -q origin ":refs/machine-claims/windowPid" 2>/dev/null || true)

# ===========================================================================
echo "TEST 43: the one-ref-per-machine LIMIT is documented (round 4, High — not silently shipped)"
# ===========================================================================
# refs/machine-claims/<machine> is ONE ref per machine (#2655, premised on one worker per
# machine, #1930), yet #3393's own evidence is FOUR lanes per box. So concurrent lanes on
# one box overwrite each other's claim and only the last supervisor pid stays observable —
# a real bound on what this command can report. Changing the ref layout is a design
# decision on a namespace shared with should-reap and the CI reaper, so it is escalated
# rather than made here; what must not happen is shipping the limit unstated.
if grep -q 'ONE CLAIM REF PER MACHINE' "$HB" && grep -qi 'four lanes\|4 lanes' "$HB"; then
  ok "the one-ref-per-machine limitation is stated in the script, with the multi-lane case named"
else
  bad "the one-ref-per-machine limit must be documented where a reader of dead-lanes will see it"
fi

# ===========================================================================
echo "TEST 44: a ZOMBIE supervisor is DEAD, not ALIVE (roborev round 5, Medium)"
# ===========================================================================
# `ps -p` succeeds for a process in state Z: it has exited and is only awaiting reap, so
# it cannot drive a lane — but the identity check would then confirm its start time and
# report ALIVE, i.e. a dead lane reported healthy. Driven against a REAL zombie: a forked
# child that exits while its parent never waits.
zparent_out="$T/zombie-pid"
python3 - "$zparent_out" <<'PYZ' &
import os, sys, time
pid = os.fork()
if pid == 0:
    os._exit(0)          # the child exits immediately and is never waited for -> Z
open(sys.argv[1], "w").write(str(pid))
time.sleep(25)           # keep the parent alive so the zombie is not reparented+reaped
PYZ
zshell=$!
for _ in 1 2 3 4 5 6 7 8 9 10; do
  [ -s "$zparent_out" ] && break
  sleep 0.3
done
zpid="$(cat "$zparent_out" 2>/dev/null || true)"
zstate="$(ps -o stat= -p "$zpid" 2>/dev/null | tr -d ' ')"
if [ -n "$zpid" ] && [ "${zstate#Z}" != "$zstate" ]; then
  ok "PREMISE ASSERTED: pid $zpid is a real zombie (ps state '$zstate'), and ps -p reports it as existing"
  craft_old_claim "$WORK" "zombieMachine" 3407 "$zpid" 0
  z_out=$(cd "$WORK" && HEARTBEAT_MACHINE=zombieMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
    bash "$HB" dead-lanes 2>&1)
  z_rc=$?
  if [ "$z_rc" -eq 3 ] \
    && printf '%s\n' "$z_out" | grep -E '^zombieMachine ' | grep -q 'DEAD-NO-PROCESS' \
    && printf '%s\n' "$z_out" | grep -E '^zombieMachine ' | grep -qi 'zombie'; then
    ok "a ZOMBIE supervisor is reported DEAD-NO-PROCESS (rc=3), with the zombie state named in the detail"
  else
    bad "a zombie supervisor must be reported dead, naming the state: rc=$z_rc out:
$z_out"
  fi
  # NON-VACUITY: ps -p DOES report it as existing, so the pre-fix existence test alone
  # would have called this lane ALIVE.
  if ps -p "$zpid" >/dev/null 2>&1; then
    ok "NON-VACUITY: ps -p still reports the zombie as existing, so an existence-only check would have said ALIVE"
  else
    bad "NON-VACUITY broken: ps -p does not see the zombie, so TEST 44 proves nothing"
  fi
  (cd "$WORK" && g push -q origin ":refs/machine-claims/zombieMachine" 2>/dev/null || true)
else
  bad "could not construct a zombie fixture (pid='$zpid' state='$zstate') — this case must not be skipped silently"
fi
kill "$zshell" 2>/dev/null
wait "$zshell" 2>/dev/null

# ===========================================================================
echo "TEST 45: an UNREADABLE process state is not treated as a zombie"
# ===========================================================================
# The zombie test must not have made "cannot read the state" mean "dead": a false DEAD on
# a healthy fleet is how a monitor gets ignored. Shimmed so `ps -p` succeeds while
# `ps -o stat=` returns nothing.
zshim="$T/zshim"
mkdir -p "$zshim"
cat >"$zshim/ps" <<'PSEOF'
#!/usr/bin/env bash
for a in "$@"; do
  case "$a" in
    stat=) exit 0 ;;                 # state unreadable
    etimes=) echo 999999; exit 0 ;;  # started long ago -> identity verifiable
  esac
done
exit 0                               # `ps -p <pid>` succeeds: the process exists
PSEOF
chmod +x "$zshim/ps"
craft_old_claim "$WORK" "unreadState" 3408 "$$" 0
us_out=$(cd "$WORK" && PATH="$zshim:$PATH" HEARTBEAT_MACHINE=unreadState \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
if ! printf '%s\n' "$us_out" | grep -E '^unreadState ' | grep -q 'DEAD'; then
  ok "an unreadable process state is NOT reported dead (cannot-tell must not become a false DEAD)"
else
  bad "an unreadable state must not be treated as a zombie: out:
$us_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/unreadState" 2>/dev/null || true)

echo
echo "=== claim-heartbeat.sh: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
