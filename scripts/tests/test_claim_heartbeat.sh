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

echo
echo "=== claim-heartbeat.sh: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
