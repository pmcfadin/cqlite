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
# skip: an ENVIRONMENTAL non-result — a premise this host will not stage. COUNTED and printed in the
# summary, because this suite previously had NO skip helper: a case that called `skip` got
# "command not found" on stderr, the status was discarded under `set -uo pipefail` with no errexit,
# and the run still reported "181 passed, 0 failed" having asserted NOTHING. That is the same
# harness-level vacuity the supervisor suite's `t` wrapper exists for (#3393 round 27), one file over.
SKIP=0
skip() { printf 'SKIP - %s\n' "$1"; SKIP=$((SKIP + 1)); }

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

# craft_lane_claim <work> <machine> <issue> <pid> <age_seconds> — the PER-LANE equivalent of
# craft_old_claim (#3393). `craft_old_claim` deliberately keeps writing the LEGACY path so the drain
# stays covered; cases about the new layout use this.
craft_lane_claim() {
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
    g push -q origin "${csha}:refs/lane-claims/${machine}/${issue}"
  )
}

# A pid that is DETERMINISTICALLY ABSENT, verified rather than assumed (roborev round 12,
# Low). Several cases used the literal 999999 as "a pid that cannot exist", but Linux
# `pid_max` is commonly far higher — MEASURED 4194304 on this host — so that pid can name a
# live process and the "dead pid" fixtures would fail nondeterministically. `pid_max + 1` can
# never be allocated, so it is the first candidate; each candidate is then CHECKED to be
# absent by every probe, and a suite that cannot find one FAILS rather than proceeding on a
# pid that might be live.
ABSENT_PID=""
_pid_max="$(cat /proc/sys/kernel/pid_max 2>/dev/null || true)"
case "$_pid_max" in
  '' | *[!0-9]*) _candidates="4194305 999999 999998" ;;
  *)             _candidates="$((_pid_max + 1)) 4194305 999999" ;;
esac
for _cand in $_candidates; do
  if ! ps -p "$_cand" >/dev/null 2>&1 \
    && ! kill -0 "$_cand" 2>/dev/null \
    && { [ ! -d /proc ] || [ ! -e "/proc/$_cand" ]; }; then
    ABSENT_PID="$_cand"
    break
  fi
done
if [ -n "$ABSENT_PID" ]; then
  ok "fixture: pid $ABSENT_PID is verified absent by every probe (pid_max=${_pid_max:-unknown}) — the 'dead pid' cases cannot name a live process"
else
  bad "could not find a verifiably-absent pid from '$_candidates'; the dead-pid fixtures would be unsound"
fi

# A `ps` reporting a LONG-RUNNING process, for the cases that need a deterministic ALIVE.
#
# Why a shim rather than a real pid: identity requires the start to predate the claim ts by
# more than the tolerance, and the only pid a test can be sure is alive is its own shell —
# whose age at this point in the suite is a few SECONDS, i.e. inside the tolerance band, so
# the verdict flips with machine speed. MEASURED: the test shell was 2s old here and read
# UNKNOWN-IDENTITY, correctly. Fixing the elapsed time fixes the verdict.
#
# It does NOT weaken the regression it guards: the shim implements only `etimes=`/`stat=`,
# so if the identity check ever went back to parsing `ps -o lstart=` (the timezone defect of
# round 3) the start time would come back EMPTY here and these cases would fail.
ALIVE_SHIM="$T/aliveshim"
mkdir -p "$ALIVE_SHIM"
cat >"$ALIVE_SHIM/ps" <<'PSEOF'
#!/usr/bin/env bash
for a in "$@"; do
  case "$a" in
    stat=)   echo "S";      exit 0 ;;   # a normal sleeping process, not a zombie
    etimes=) echo 999999;   exit 0 ;;   # started long ago => start predates any claim ts
  esac
done
exit 0                                   # `ps -p <pid>` succeeds: the process exists
PSEOF
chmod +x "$ALIVE_SHIM/ps"

# A `ps` that fakes only ELAPSED TIME and STATE, delegating EXISTENCE to the real ps.
#
# ALIVE_SHIM cannot be used where a case needs both a live and a dead pid: its unconditional
# `exit 0` makes every `ps -p` succeed, so an absent pid looks present to ps while /proc and the
# signal probe say absent — the probes disagree and the verdict is UNKNOWN-PROBE rather than DEAD.
# MEASURED while writing the two-lanes-one-machine case, which needs a real DEAD row.
#
# Faking state/elapsed is safe for the absent pid because presence is decided FIRST: a pid the real
# ps cannot see never reaches the state or identity checks.
# Resolved rather than hardcoded (roborev round 6, Low): macOS ships ps at /bin/ps, and this suite
# is registered in the canonical gate, which runs there too. A hardcoded /usr/bin/ps would be a
# permanent red on macOS rather than a finding about this code — the same shape as the /proc
# assumption caught earlier.
REAL_PS="$(command -v ps)"
export REAL_PS
MIXED_SHIM="$T/mixedshim"
mkdir -p "$MIXED_SHIM"
cat >"$MIXED_SHIM/ps" <<'PSEOF'
#!/usr/bin/env bash
for a in "$@"; do
  case "$a" in
    stat=)   echo "S";      exit 0 ;;   # a normal sleeping process, never a zombie
    etimes=) echo 999999;   exit 0 ;;   # started long ago => identity verifiable
  esac
done
exec "${REAL_PS:?REAL_PS not set}" "$@"  # EXISTENCE is answered by the real ps
PSEOF
chmod +x "$MIXED_SHIM/ps"

# Hermetic open-PR hooks (never touch gh/network).
NO_OPEN_PR='exit 1'   # $1=issue -> always "no open PR"
HAS_OPEN_PR='exit 0'  # $1=issue -> always "has open PR"

# ===========================================================================
echo "TEST 9: stamp creates refs/lane-claims/<machine>/<issue> with issue+pid (#3393 ruling A)"
# ===========================================================================
# PER-LANE since #3393. The ref path itself now carries the issue, which is what lets several lanes
# on ONE machine coexist instead of overwriting each other.
(cd "$WORK" && HEARTBEAT_MACHINE=claimA bash "$HB" stamp 900 4242 >/dev/null 2>&1)
claim_sha=$(g -C "$WORK" ls-remote origin "refs/lane-claims/claimA/900" | awk '{print $1}')
claim_msg=""
if [ -n "$claim_sha" ]; then
  # Private ref here too: the suite fetches concurrently in other cases, and FETCH_HEAD is shared.
  g -C "$WORK" fetch -q --no-write-fetch-head --no-tags origin "+refs/lane-claims/claimA/900:refs/tmp/t9" 2>/dev/null
  claim_msg=$(g -C "$WORK" log -1 --format=%B refs/tmp/t9 2>/dev/null)
  g -C "$WORK" update-ref -d refs/tmp/t9 2>/dev/null || true
fi
if [ -n "$claim_sha" ] && printf '%s' "$claim_msg" | grep -q 'issue=900' \
  && printf '%s' "$claim_msg" | grep -q 'pid=4242'; then
  ok "stamp created refs/lane-claims/claimA/900 carrying issue=900 pid=4242"
else
  bad "stamp did not create a well-formed per-lane claim ref (sha='$claim_sha' msg='$claim_msg')"
fi
# ...and it must NOT have written the legacy per-machine ref, or both layouts would drift in
# parallel and the drain would never finish.
if [ -z "$(g -C "$WORK" ls-remote origin 'refs/machine-claims/claimA' | awk '{print $1}')" ]; then
  ok "stamp wrote ONLY the per-lane ref — no legacy refs/machine-claims/claimA left behind"
else
  bad "stamp also wrote the legacy ref; the two layouts would drift and the drain could not finish"
fi
# The layout must survive a DASH-BEARING machine name, which is the constraint that forced a slash
# separator rather than <machine>-<issue>: 'ip-172-31-7-163-900' cannot be split back.
(cd "$WORK" && HEARTBEAT_MACHINE=ip-172-31-7-163 bash "$HB" stamp 901 4243 >/dev/null 2>&1)
if [ -n "$(g -C "$WORK" ls-remote origin 'refs/lane-claims/ip-172-31-7-163/901' | awk '{print $1}')" ]; then
  ok "a dash-bearing machine name round-trips: refs/lane-claims/ip-172-31-7-163/901"
else
  bad "a dash-bearing machine name did not produce the expected per-lane ref"
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
# A pid VERIFIED absent at suite start (see ABSENT_PID). The comment here used to claim
# 999999 is beyond default pid_max on Linux/macOS; that is not true — MEASURED pid_max
# 4194304 on this host — so the literal is replaced by a checked value.
craft_old_claim "$WORK" "localMachine2" 903 "$ABSENT_PID" 36000
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
craft_old_claim "$WORK" "deadFresh" 3393 "$ABSENT_PID" 30   # 30s old: far INSIDE the reap threshold
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
al_out=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" HEARTBEAT_MACHINE=aliveLocal CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
al_rc=$?
# machine=aliveLocal is the local identity here, so ITS row must read ALIVE — and the
# deadFresh claim, being FOREIGN from this vantage point, is UNKNOWN-FOREIGN rather than
# dead, so nothing raises a FINDING. That asymmetry is the point of the multi-valued
# verdict: the same ref reads DEAD on its own machine and UNKNOWN elsewhere.
# The ROW is the property. There is no longer any exit 0 to assert (round 13): the clean
# verdict was REMOVED because a per-machine claim cannot establish the absence of a dead lane.
if printf '%s\n' "$al_out" | grep -E '^aliveLocal ' | grep -q 'ALIVE' && [ "$al_rc" -ne 3 ]; then
  ok "dead-lanes reported ALIVE for a live local pid and did NOT raise a finding (rc=$al_rc; the deadFresh ref reads UNKNOWN-FOREIGN from here, never dead)"
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
echo "TEST 27: zero claim refs is INCOMPLETE (exit 1) — an empty namespace is not an idle fleet"
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
craft_old_claim "$WORK" "deadPrecedence" 3396 "$ABSENT_PID" 30
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
craft_old_claim "$WORK" "zeroIssue" 0 "$ABSENT_PID" 30
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
craft_old_claim "$WORK" "zeroIssue2" 3401 "$ABSENT_PID" 30
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
  && grep -qE '\$CLAIM_CMD stamp "\$lane_id" "\$\$"' "$SCRIPT_DIR/../local/worker-supervisor.sh"; then
  ok "the supervisor stamps its OWN pid, so DEAD-NO-PROCESS means the lane-owning process is gone (semantic pinned)"
else
  bad "worker-supervisor.sh no longer stamps the supervisor pid alongside a lane id — dead-lanes' documented meaning must be revisited"
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
gp_out=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" HEARTBEAT_MACHINE=goodPid CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
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
craft_old_claim "$WORK" "probeFail" 3400 "$ABSENT_PID" 30
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
tz_utc=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" TZ=UTC HEARTBEAT_MACHINE=tzMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1 | grep -E '^tzMachine ' | awk '{print $4}')
tz_ist=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" TZ=Asia/Kolkata HEARTBEAT_MACHINE=tzMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1 | grep -E '^tzMachine ' | awk '{print $4}')
tz_neg=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" TZ=America/Los_Angeles HEARTBEAT_MACHINE=tzMachine CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
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
# Existence succeeds and the STATE is readable (a normal sleeping process), so this case
# isolates the IDENTITY gap: only the elapsed-time queries come back empty. Leaving the
# state unreadable too would land on UNKNOWN-STATE and test a different branch.
for a in "$@"; do
  case "$a" in
    stat=) echo "S"; exit 0 ;;
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
echo "TEST 41: one HEALTHY local lane + foreign lanes raises NO finding (round 4 balance)"
# ===========================================================================
# The other side of TEST 34: foreign rows must not EACH count as incomplete, or a
# healthy multi-machine fleet would sit at exit 1 forever and everyone would learn to
# ignore the code. The rule is "did I measure ANY local lane", not "is every lane local".
craft_old_claim "$WORK" "mixedLocal" 3405 "$$" 0
mixed_out=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" HEARTBEAT_MACHINE=mixedLocal CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
mixed_rc=$?
foreign_rows=$(printf '%s\n' "$mixed_out" | grep -c 'UNKNOWN-FOREIGN' || true)
# REFRAMED in round 13. This case used to assert exit 0, which no longer exists — the clean
# verdict was removed because a per-machine claim cannot establish the ABSENCE of a dead lane.
# What still matters, and is what this case was really about, is that foreign rows must not
# manufacture a FINDING: a healthy multi-machine fleet must not report a dead lane (exit 3).
if [ "$mixed_rc" -ne 3 ] \
  && [ "$foreign_rows" -gt 0 ] \
  && printf '%s\n' "$mixed_out" | grep -E '^mixedLocal ' | grep -q 'ALIVE' \
  && ! printf '%s\n' "$mixed_out" | grep -q 'DEAD'; then
  ok "a measured-healthy local lane alongside $foreign_rows foreign rows raises NO finding (rc=$mixed_rc, no DEAD row) — foreign rows cannot manufacture one"
else
  bad "foreign rows must not produce a dead-lane finding: rc=$mixed_rc foreign=$foreign_rows out:
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
# DETERMINISTIC via a controlled elapsed time (roborev round 8, Low). The first cut started
# a real process and backdated the claim by 1s, so a scheduling delay could put the start
# BEFORE the claim ts and yield ALIVE — a flaky tooling gate. The second cut shimmed
# elapsed=0 with an age-0 claim, which was flaky the OTHER way: craft and read can land in
# the same second, giving start == ts and therefore ALIVE.
#
# Both sides are now pinned by construction: the claim is backdated 5s and the shim reports
# elapsed=0 ("started just now"), so the computed start is the READ time and the gap is
# 5s + however long the run takes — always strictly positive (time moves forward) and
# always well inside the 30s window this case sets. No sub-second race in either direction.
winshim="$T/winshim"
mkdir -p "$winshim"
cat >"$winshim/ps" <<'PSEOF'
#!/usr/bin/env bash
for a in "$@"; do
  case "$a" in
    stat=) echo "S"; exit 0 ;;
    etimes=) echo 0; exit 0 ;;
  esac
done
exit 0
PSEOF
chmod +x "$winshim/ps"
win_pid=$$
craft_old_claim "$WORK" "windowPid" 3406 "$win_pid" 5
win_out=$(cd "$WORK" && PATH="$winshim:$PATH" PID_IDENTITY_SLACK_SECS=30 HEARTBEAT_MACHINE=windowPid \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
win_rc=$?
if printf '%s\n' "$win_out" | grep -E '^windowPid ' | grep -q 'UNKNOWN-IDENTITY' \
  && [ "$win_rc" -eq 1 ]; then
  ok "a start time inside the rounding window is UNKNOWN-IDENTITY + exit 1, never a clean ALIVE"
else
  bad "the slack window must not produce ALIVE: rc=$win_rc out:
$win_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/windowPid" 2>/dev/null || true)

# ===========================================================================
echo "TEST 43: the retired per-machine bound is GONE from the help, and the per-lane layout is stated"
# ===========================================================================
# INVERTED (#3393 ruling A). This case used to assert that the one-ref-per-machine limitation was
# DOCUMENTED, because it could not be fixed without an owner decision. The decision landed and the
# layout changed, so continuing to assert the documentation would pin a bound that no longer exists —
# the same way TEST 46 was found still enforcing "NEVER exits 0" after exit 0 came back. A guard
# aimed at a retired contract defends the error.
help43=$(cd "$WORK" && bash "$HB" --help 2>&1 || true)
if ! printf '%s\n' "$help43" | grep -qi 'ONE CLAIM REF PER MACHINE'; then
  ok "the help no longer presents one-ref-per-machine as a current limitation"
else
  bad "the retired per-machine bound is still documented as live"
fi
if printf '%s\n' "$help43" | grep -q 'refs/lane-claims/<machine>/<issue>' \
  && printf '%s\n' "$help43" | grep -qi 'never stamped is invisible\|never stamped'; then
  ok "the help states the per-lane layout AND the bound that actually remains (a lane that never stamped is invisible)"
else
  bad "the help must state the per-lane layout and the remaining invisible-lane bound"
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
# WAIT FOR THE Z STATE, NOT FOR THE PID FILE (roborev round 16, Low). The parent writes the
# file after forking, which can happen BEFORE the child has exited — so keying on the file made
# the fixture nondeterministic, and a gate-registered flaky test is worse than no test.
zpid=""
zstate=""
for _ in $(seq 1 40); do
  if [ -s "$zparent_out" ]; then
    zpid="$(cat "$zparent_out" 2>/dev/null || true)"
    zstate="$(ps -o stat= -p "$zpid" 2>/dev/null | tr -d ' ')"
    [ "${zstate#Z}" != "$zstate" ] && break
  fi
  sleep 0.25
done
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
us_rc=$?
# BOTH directions, because "not DEAD" alone was too weak (roborev round 7, Medium): it
# accepted the ALIVE + exit 0 that an unreadable state used to produce, which is the
# false-clean. The correct answer is NEITHER — an UNKNOWN that counts as incomplete.
if printf '%s\n' "$us_out" | grep -E '^unreadState ' | grep -q 'UNKNOWN-STATE' \
  && ! printf '%s\n' "$us_out" | grep -E '^unreadState ' | grep -qE 'DEAD|ALIVE' \
  && [ "$us_rc" -eq 1 ]; then
  ok "an unreadable process state is UNKNOWN-STATE + exit 1 — neither a false DEAD nor a false-clean ALIVE"
else
  bad "an unreadable state must be UNKNOWN-STATE + exit 1: rc=$us_rc out:
$us_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/unreadState" 2>/dev/null || true)

# ===========================================================================
echo "TEST 46: every verdict the code can emit is documented in --help (round 6, Low)"
# ===========================================================================
# roborev round 6 found the help had drifted: it still said zero claims returns 0 and
# that only DEAD-NO-PROCESS sets the finding code, after both had changed. Prose drift is
# not catchable by any behavioural test, so this compares the two SIDES — the verdict
# tokens the implementation assigns, and the tokens the help lists. Derived from the
# source, never a hand-kept list, so a NEW verdict added later fails this until it is
# documented.
help_text=$(cd "$WORK" && bash "$HB" --help 2>&1 || true)
emitted=$(grep -oE 'verdict="[A-Z][A-Z-]*"' "$HB" | sed -e 's/verdict="//' -e 's/"//' | sort -u)
undocumented=""
for v in $emitted; do
  printf '%s\n' "$help_text" | grep -q "$v" || undocumented="$undocumented $v"
done
if [ -n "$emitted" ] && [ -z "$undocumented" ]; then
  ok "all $(printf '%s\n' "$emitted" | wc -l | tr -d ' ') verdict tokens the code emits are documented in --help"
else
  bad "verdicts missing from --help:${undocumented:-<none>} (emitted: $(printf '%s' "$emitted" | tr '\n' ' '))"
fi
# ...and the exit codes the help claims must match what the code actually returns, for the
# two cases round 6 caught as stale.
zc_out=$(cd "$empty_work" && bash "$HB" dead-lanes 2>&1); zc_rc=$?
# The phrase is matched on ONE line: the help is a comment block, so a longer phrase
# spans a line break and would never match however correct the text is.
if [ "$zc_rc" -eq 1 ] && printf '%s\n' "$help_text" | grep -qi 'zero'; then
  ok "the help's exit-code contract matches the zero-claims behaviour it documents (both say incomplete/1)"
else
  bad "help and behaviour disagree on zero claims: rc=$zc_rc"
fi

# ...and every REF NAMESPACE the code touches must appear in the help (round 18, Low). The layout
# section still described one `refs/machine-claims/<machine>` per machine after the per-lane ruling,
# which is a defect rather than a typo: doctrine treats this help as the authoritative contract
# BECAUSE it lives in the same file as the code and therefore cannot drift from it. Derived from the
# source like the verdict half above, so a namespace added later fails this until it is documented.
ns_emitted=$(grep -oE "refs/(lane-claims|machine-claims|heartbeats|tmp)/" "$HB" | sed 's|/$||' | sort -u)
ns_missing=""
for ns in $ns_emitted; do
  printf '%s\n' "$help_text" | grep -q "$ns" || ns_missing="$ns_missing $ns"
done
if [ -n "$ns_emitted" ] && [ -z "$ns_missing" ]; then
  ok "every ref namespace the code touches is documented in --help ($(printf '%s' "$ns_emitted" | tr '\n' ' '))"
else
  bad "ref namespaces missing from --help:${ns_missing:-<none>} (emitted: $(printf '%s' "$ns_emitted" | tr '\n' ' '))"
fi
# The per-lane shape specifically, since that is the one that drifted: the help must show the
# lane-id component and must mark the legacy namespace as legacy.
if printf '%s\n' "$help_text" | grep -q 'refs/lane-claims/<machine>/<lane-id>' \
  && printf '%s\n' "$help_text" | grep -qi 'LEGACY'; then
  ok "--help documents the per-lane ref shape and marks refs/machine-claims/* as legacy"
else
  bad "--help must document refs/lane-claims/<machine>/<lane-id> and mark the per-machine namespace legacy"
fi
# ...and the help must not describe the PER-LANE world in PER-MACHINE terms (round 30, Low). Three
# separate spots in this help block have drifted across the change now — the ref layout (round 18), the
# should-reap forms (round 26), and `stamp`/`list-claims` here — so the phrases are pinned rather than
# re-read by eye each round. Brittle by construction, and accepted: doctrine treats this help as the
# authoritative contract precisely because it cannot drift from the code, which is only true if
# something checks.
if printf '%s\n' "$help_text" | grep -q 'stamp <lane-id>' \
  && printf '%s\n' "$help_text" | grep -qi 'list-claims .*one line per LANE'; then
  ok "--help describes stamp as taking a <lane-id> and list-claims as one line per LANE"
else
  bad "--help still describes the per-lane world in per-machine terms: $(printf '%s\n' "$help_text" | grep -E 'stamp <|list-claims ' | head -3)"
fi
# ...and no line may cite #1930's retracted "one worker per machine" as a LIVE justification. The two
# surviving citations are both explicitly marked as retracted, so the guard requires the retraction
# marker on the same line rather than banning the phrase (which would forbid recording the history).
retracted_live=""
while IFS= read -r line; do
  case "$line" in
    *"one worker per machine"*)
      case "$line" in
        *RETRACT*|*retract*|*"used to give"*) : ;;
        *) retracted_live="${retracted_live}|${line}" ;;
      esac
      ;;
  esac
done < <(printf '%s\n' "$help_text")
if [ -z "$retracted_live" ]; then
  ok "no help line cites #1930's retracted one-worker-per-machine as a live justification"
else
  bad "the help still asserts the retracted invariant: ${retracted_live}"
fi
# ...and the ONE-LINE subcommand summary must name BOTH should-reap forms (round 21, Medium). It
# advertised `should-reap <machine> [issue] [secs]` while a two-argument call is ALWAYS the legacy
# threshold form, so an operator following it got a verdict about the legacy ref with the issue number
# read as a threshold. The --help block was right and this summary was not, which is why both are
# asserted rather than just the one that happened to be wrong.
usage_text=$(cd "$WORK" && bash "$HB" 2>&1 || true)
if printf '%s\n' "$usage_text" | grep -q 'should-reap <machine> \[threshold_secs\]' \
  && printf '%s\n' "$usage_text" | grep -q 'should-reap <machine> <issue> <threshold_secs>'; then
  ok "the no-subcommand usage line names BOTH should-reap forms, so the two-argument trap is not advertised as a lane call"
else
  bad "the usage line must spell out both should-reap forms: $(printf '%s\n' "$usage_text" | tr '\n' ' ' | head -c 400)"
fi
# ...and the help's exit-0 contract must match the CURRENT implementation, which restored the clean
# verdict once per-lane refs removed the masking (#3393 ruling A).
#
# THIS GUARD WAS ENFORCING THE OPPOSITE AN HOUR AGO, which is the sharpest lesson in it: it asserted
# the help says "NEVER exits 0", so after the layout change it actively held the documentation wrong
# and would have failed a correct help. A drift guard is only as good as the contract it is pointed
# at, and a stale contract makes it worse than nothing — it defends the error. Flagged by roborev
# round 1 (Low) on exactly that basis.
# The help must match THIS slice: positive detection only, no exit 0 (#3393 split ruling). This
# assertion has now been pointed at three different contracts in one day — never-0, then 0-restored,
# now never-0-for-a-different-reason — which is exactly why it exists: each time the contract moved,
# this is what caught the documentation lagging behind the code.
if printf '%s\n' "$help_text" | grep -qi 'never exits 0' \
  && ! printf '%s\n' "$help_text" | grep -qi 'exit 0 = at least one LOCAL'; then
  ok "the help documents this slice's contract: positive detection only, never exit 0"
else
  bad "the help must match the implementation: this slice never exits 0"
fi
# ...and the retired per-machine limitation must not still be presented as current.
if ! printf '%s\n' "$help_text" | grep -qi 'ONE CLAIM REF PER MACHINE'; then
  ok "the help no longer presents one-ref-per-machine as a current limitation"
else
  bad "the help still describes the retired per-machine layout as a live bound"
fi

# ===========================================================================
echo "TEST 47: a FAILING existence probe is UNKNOWN-PROBE, never a fleet-wide false DEAD"
# ===========================================================================
# roborev round 8 (Medium): `process_exists` read any nonzero `ps` status as proof the pid
# was GONE, so a missing or failing `ps` turned every claim into DEAD-NO-PROCESS + exit 3.
# That is not hypothetical on the hosts this command is for — under the memory exhaustion
# #3393 records, a box that cannot fork cannot run `ps`, so the probe is most likely to
# fail at exactly the moment the report matters most. Driven by a `ps` that always fails.
psfail="$T/psfail"
mkdir -p "$psfail"
printf '#!/usr/bin/env bash\nexit 1\n' >"$psfail/ps"
chmod +x "$psfail/ps"
craft_old_claim "$WORK" "probeDown" 3409 "$$" 0
pd_out=$(cd "$WORK" && PATH="$psfail:$PATH" HEARTBEAT_MACHINE=probeDown \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
pd_rc=$?
if printf '%s\n' "$pd_out" | grep -E '^probeDown ' | grep -q 'UNKNOWN-PROBE' \
  && ! printf '%s\n' "$pd_out" | grep -E '^probeDown ' | grep -q 'DEAD' \
  && [ "$pd_rc" -eq 1 ]; then
  ok "a failing ps yields UNKNOWN-PROBE + exit 1 for a live pid — not a false DEAD-NO-PROCESS + exit 3"
else
  bad "a failing existence probe must be UNKNOWN-PROBE + exit 1: rc=$pd_rc out:
$pd_out"
fi
# NON-VACUITY: the pid used above really is alive, so a probe trusted blindly would have
# called this lane dead.
if ps -p "$$" >/dev/null 2>&1; then
  ok "NON-VACUITY: the claimed pid IS live under the real ps, so the pre-fix path would have reported it DEAD"
else
  bad "NON-VACUITY broken: the real ps cannot see the test shell, so TEST 47 proves nothing"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/probeDown" 2>/dev/null || true)

# ===========================================================================
echo "TEST 48: the identity tolerance is SYMMETRIC — a start just BEFORE the ts is UNKNOWN"
# ===========================================================================
# roborev round 9 (Medium): the band was one-sided. `start <= ts` claimed ALIVE right up to
# equality, but both numbers are whole seconds sampled at different moments, so a pid
# recycled just after stamping reconstructs as equal to — or a hair before — the claim ts
# and read clean. ALIVE must require the start to predate the ts by MORE than the tolerance.
#
# Isolated by making the TOLERANCE larger than the gap rather than by racing the clock: the
# shim reports a start ~999999s before the claim ts, and the tolerance is set wider than
# that, so the start lands inside the band on the NEGATIVE side by construction. Under the
# one-sided rule this is ALIVE; under the symmetric rule it is UNKNOWN-IDENTITY. No timing
# dependence in either direction.
craft_old_claim "$WORK" "symBand" 3410 "$$" 0
sym_out=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" PID_IDENTITY_SLACK_SECS=2000000 \
  HEARTBEAT_MACHINE=symBand CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
sym_rc=$?
if printf '%s\n' "$sym_out" | grep -E '^symBand ' | grep -q 'UNKNOWN-IDENTITY' \
  && [ "$sym_rc" -eq 1 ]; then
  ok "a start inside the tolerance band on the NEGATIVE side is UNKNOWN-IDENTITY + exit 1, not a one-sided ALIVE"
else
  bad "the tolerance band must be symmetric: rc=$sym_rc out:
$sym_out"
fi
# NON-VACUITY: the SAME fixture with a tolerance narrower than the gap IS ALIVE, so the case
# above is about the band and not about the shim refusing to ever verify identity.
sym2_out=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" PID_IDENTITY_SLACK_SECS=2 \
  HEARTBEAT_MACHINE=symBand CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$sym2_out" | grep -E '^symBand ' | grep -q 'ALIVE'; then
  ok "NON-VACUITY: the same fixture with a narrow tolerance IS ALIVE — the band is what decides, not the shim"
else
  bad "NON-VACUITY broken: the fixture never verifies identity at any tolerance: out:
$sym2_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/symBand" 2>/dev/null || true)

# ===========================================================================
echo "TEST 49: ps saying ABSENT while /proc says PRESENT is UNKNOWN-PROBE, not DEAD"
# ===========================================================================
# roborev round 9 (Medium): validating `ps` against our OWN pid proves the tool runs, not
# that it can see THIS target. A target-specific visibility restriction (`/proc` hidepid, a
# foreign owner) or a transient failure still made a nonzero `ps -p <target>` mean "gone",
# reporting a LIVE supervisor as DEAD-NO-PROCESS with exit 3.
#
# Driven by a `ps` that answers normally EXCEPT for one pid, which it hides. That pid is the
# live test shell, so /proc still has it: the probes disagree, and disagreement must be
# UNKNOWN, never a verdict.
hideshim="$T/hideshim"
mkdir -p "$hideshim"
cat >"$hideshim/ps" <<'PSEOF'
#!/usr/bin/env bash
# `-p <pid>` for the hidden pid reports no match; everything else behaves normally.
prev=""
for a in "$@"; do
  case "$a" in
    stat=)   echo "S";    exit 0 ;;
    etimes=) echo 999999; exit 0 ;;
  esac
  if [ "$prev" = "-p" ] && [ "$a" = "${SHIM_HIDE_PID:-}" ]; then exit 1; fi
  prev="$a"
done
exit 0
PSEOF
chmod +x "$hideshim/ps"
craft_old_claim "$WORK" "hiddenPid" 3411 "$$" 0
hp_out=$(cd "$WORK" && PATH="$hideshim:$PATH" SHIM_HIDE_PID="$$" HEARTBEAT_MACHINE=hiddenPid \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
hp_rc=$?
if printf '%s\n' "$hp_out" | grep -E '^hiddenPid ' | grep -q 'UNKNOWN-PROBE' \
  && ! printf '%s\n' "$hp_out" | grep -E '^hiddenPid ' | grep -q 'DEAD' \
  && [ "$hp_rc" -eq 1 ]; then
  ok "disagreeing presence probes (ps absent, /proc present) yield UNKNOWN-PROBE + exit 1, never a false DEAD"
else
  bad "a target ps cannot see must not be declared dead: rc=$hp_rc out:
$hp_out"
fi
# NON-VACUITY, PLATFORM-AWARE (roborev round 10, Medium). The first cut demanded `/proc/$$`
# unconditionally, which fails on macOS — and this suite is now registered in the canonical
# gate, which runs on macOS too, so that would have been a permanent red there rather than a
# finding about this code. The independent evidence of presence differs by platform: /proc on
# Linux, the signal probe everywhere.
shim_hides_it=false
if ! PATH="$hideshim:$PATH" SHIM_HIDE_PID="$$" ps -p "$$" >/dev/null 2>&1; then
  shim_hides_it=true
fi
present_elsewhere=false
if [ -d /proc ]; then
  [ -e "/proc/$$" ] && present_elsewhere=true
  present_evidence="/proc/$$ exists"
else
  kill -0 "$$" 2>/dev/null && present_elsewhere=true
  present_evidence="kill -0 succeeds (no /proc on this platform)"
fi
if [ "$shim_hides_it" = true ] && [ "$present_elsewhere" = true ]; then
  ok "NON-VACUITY: $present_evidence while the shimmed ps reports it absent — the probes genuinely disagree"
else
  bad "NON-VACUITY broken: the disagreement fixture is not in the state TEST 49 assumes (shim_hides=$shim_hides_it present_elsewhere=$present_elsewhere)"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/hiddenPid" 2>/dev/null || true)

# ===========================================================================
echo "TEST 50: the signal probe decodes EPERM as PRESENT (round 10, Medium)"
# ===========================================================================
# The voting scheme had CORRELATED voters: `ps -p` and `/proc/<pid>` are both VISIBILITY
# probes, hidden together by `hidepid=2`, so a different user's live process could be
# unanimously "absent" and reported DEAD. `kill -0` is the one independent probe, and its
# EPERM failure is affirmative evidence the process EXISTS — abstaining on it left only the
# correlated pair.
#
# Tested at the FUNCTION level, deliberately. End-to-end on Linux the /proc vote breaks the
# tie on its own, so a black-box case passes with or without this decode and proves nothing
# about it (verified: the black-box form was green against the pre-fix script). The three
# states are therefore asserted directly. The function is EXTRACTED from the shipped file
# rather than reimplemented here — the script cannot be sourced, since sourcing runs its
# dispatch — so this exercises the real code, and it fails outright if the function is gone.
probe_fn="$T/signal-probe.sh"
sed -n '/^signal_probe_class()/,/^}/p' "$HB" >"$probe_fn"
if [ -s "$probe_fn" ]; then
  ok "extracted signal_probe_class from the shipped script ($(wc -l <"$probe_fn" | tr -d ' ') lines)"
else
  bad "signal_probe_class is missing from $HB — the EPERM decode does not exist"
fi
# shellcheck disable=SC1090
. "$probe_fn" 2>/dev/null || true
if [ "$(type -t signal_probe_class 2>/dev/null)" = "function" ]; then
  # PRESENT: our own pid, signallable.
  sp_self="$(signal_probe_class "$$")"
  # DENIED: pid 1 is root-owned, so an unprivileged user gets EPERM. Skipping is not an
  # option — if this user CAN signal pid 1 the premise is broken and that is a failure.
  sp_root="$(signal_probe_class 1)"
  # ABSENT: a pid that cannot plausibly exist.
  sp_gone="$(signal_probe_class "$ABSENT_PID")"
  # EPERM IS DRIVEN BY A SHIM, NOT BY PID 1's PERMISSIONS (roborev round 11, Low). The first
  # cut asserted that this user cannot signal pid 1, which is false as root or with CAP_KILL
  # — and since this suite is now registered in the canonical gate, a legitimately privileged
  # environment would have gone red on correct behaviour. `kill` is a bash BUILTIN, so a PATH
  # shim cannot reach it; but `signal_probe_class` has been sourced into THIS shell, so a
  # local `kill` function overrides the builtin for it. Deterministic and privilege-free.
  (
    kill() {
      echo "bash: kill: ($2) - Operation not permitted" >&2
      return 1
    }
    r="$(signal_probe_class 4242)"
    [ "$r" = "denied" ] && exit 0
    echo "  (shimmed EPERM returned '$r')" >&2
    exit 1
  )
  if [ "$?" -eq 0 ]; then
    ok "signal_probe_class decodes an EPERM message as 'denied' (it EXISTS), driven by a shim so this holds as root too"
  else
    bad "an EPERM message must decode to 'denied'"
  fi
  (
    kill() {
      echo "bash: kill: ($2) - No such process" >&2
      return 1
    }
    r="$(signal_probe_class 4242)"
    [ "$r" = "absent" ] && exit 0
    echo "  (shimmed ESRCH returned '$r')" >&2
    exit 1
  )
  if [ "$?" -eq 0 ]; then
    ok "signal_probe_class decodes an ESRCH message as 'absent' — so 'denied' and 'absent' are genuinely different answers"
  else
    bad "an ESRCH message must decode to 'absent'"
  fi
  (
    kill() {
      echo "bash: kill: ($2) - Some unexpected condition" >&2
      return 1
    }
    r="$(signal_probe_class 4242)"
    [ "$r" = "unknown" ] && exit 0
    echo "  (shimmed unrecognised message returned '$r')" >&2
    exit 1
  )
  if [ "$?" -eq 0 ]; then
    ok "an UNRECOGNISED kill message decodes to 'unknown' — never folded onto present or absent"
  else
    bad "an unrecognised kill message must decode to 'unknown'"
  fi
  # A live pid still reads present through the real builtin.
  if [ "$(signal_probe_class "$$")" = "present" ]; then
    ok "signal_probe_class reports our own live pid as present through the REAL kill builtin"
  else
    bad "the real kill path must report a live pid as present"
  fi
  # REAL-WORLD CORROBORATION of the message TEXT, which the shims cannot give: the shims
  # assert my ASSUMPTION about what the system says, so at least once the assumption is
  # checked against the system itself. Exercised only where a genuinely unsignalable process
  # exists — as root it cannot be, and that is reported rather than silently skipped, because
  # the decode logic above is already covered and only the text corroboration is lost.
  if kill -0 1 2>/dev/null; then
    ok "NOT EXERCISABLE HERE: this user can signal pid 1 (root/CAP_KILL), so the real EPERM message text was not corroborated — the decode itself is covered by the shims above"
  else
    real_msg="$(LC_ALL=C kill -0 1 2>&1 || true)"
    if [ "$(signal_probe_class 1)" = "denied" ]; then
      ok "REAL EPERM corroborated: the system's own message ('${real_msg##*- }') decodes to 'denied', so the shimmed text matches reality"
    else
      bad "the REAL EPERM message ('$real_msg') did not decode to 'denied' — the shims are testing an assumption the system does not hold"
    fi
  fi
else
  bad "signal_probe_class could not be loaded from the shipped script"
fi

# End-to-end with OUR OWN pid hidden from ps: privilege-neutral, and it exercises the same
# path (a target ps cannot see, which another probe reports as present).
craft_old_claim "$WORK" "epermMachine" 3412 "$$" 0
ep_out=$(cd "$WORK" && PATH="$hideshim:$PATH" SHIM_HIDE_PID="$$" HEARTBEAT_MACHINE=epermMachine \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
if ! printf '%s\n' "$ep_out" | grep -E '^epermMachine ' | grep -q 'DEAD'; then
  ok "end-to-end: a pid hidden from ps but EPERM-on-signal is never reported DEAD"
else
  bad "an EPERM pid must not be declared dead: out:
$ep_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/epermMachine" 2>/dev/null || true)

# ===========================================================================
echo "TEST 51: dead-lanes never reads shared FETCH_HEAD (round 11, Medium)"
# ===========================================================================
# `FETCH_HEAD` is shared per-worktree, so ANY concurrent fetch — and this repository is
# routinely worked by several sessions in one checkout — can overwrite it between the fetch
# and the read, making a row report ANOTHER ref's pid and ts: a false ALIVE or false DEAD
# attributed to the wrong machine. The race is not reproducible on demand, so the invariant
# is asserted STRUCTURALLY: behavioural cases only cover the interleavings someone thought
# of, and this one is a window of microseconds.
dl_body="$T/dead-lanes-body.sh"
sed -n '/^cmd_dead_lanes()/,/^}/p' "$HB" >"$dl_body"
if [ -s "$dl_body" ]; then
  ok "extracted cmd_dead_lanes for the structural assert ($(wc -l <"$dl_body" | tr -d ' ') lines)"
else
  bad "could not extract cmd_dead_lanes from $HB"
fi
# ASSERTED AS A POSITIVE PLUS A TARGETED NEGATIVE, rather than "the token appears nowhere".
# Three legitimate mentions defeated the token-absence form in successive rounds: comments,
# the `--no-write-fetch-head` flag whose own name contains it, and the refusal diagnostic that
# EXPLAINS the hazard. A guard that flags its own remedy and its own explanation gets deleted
# by the next person, so it now checks the two things that actually matter: that the message
# is read from the private ref, and that no revision-reading git command names FETCH_HEAD.
if grep -q 'git log -1 --format=%B "\$tmpref"' "$dl_body"; then
  ok "cmd_dead_lanes reads the claim message from its own private ref, not from FETCH_HEAD"
else
  bad "cmd_dead_lanes must read the claim message from \$tmpref: $(grep -n 'git log' "$dl_body")"
fi
if ! grep -vE '^[[:space:]]*#' "$dl_body" \
  | grep -qE 'git[[:space:]]+(log|show|rev-parse|cat-file|for-each-ref)[^|]*FETCH_HEAD'; then
  ok "no revision-reading git command in cmd_dead_lanes names FETCH_HEAD"
else
  bad "a git revision read still names FETCH_HEAD: $(grep -vE '^[[:space:]]*#' "$dl_body" | grep -E 'git[[:space:]]+(log|show|rev-parse|cat-file|for-each-ref)[^|]*FETCH_HEAD')"
fi
# ...and it must actively suppress the WRITE too, or a concurrent run clobbers FETCH_HEAD for
# list / list-claims / should-reap / reap, which still fetch-then-read it.
# COMMENTS STRIPPED FIRST (roborev round 14, Low). The bare token search matched the comment
# that EXPLAINS the flag, so the check stayed green with the actual flag deleted — vacuous in
# the one direction that matters. It now asserts the executable fetch invocation.
if grep -vE '^[[:space:]]*#' "$dl_body" | grep -qE 'git fetch[[:space:]]+--no-write-fetch-head'; then
  ok "the executable fetch in cmd_dead_lanes passes --no-write-fetch-head, so it cannot clobber FETCH_HEAD for its neighbours"
else
  bad "the fetch command itself must carry --no-write-fetch-head: $(grep -vE '^[[:space:]]*#' "$dl_body" | grep -n 'git fetch')"
fi
# ...and it must fetch into a ref made unique per PROCESS and per ROW, or two rows (or two
# concurrent runs) would collide on the same temp ref and reintroduce the same defect.
if grep -q 'refs/tmp/claim-heartbeat' "$dl_body" \
  && grep -qE 'tmpref="refs/tmp/claim-heartbeat/\$\$-\$\{row\}"' "$dl_body"; then
  ok "cmd_dead_lanes fetches into refs/tmp/claim-heartbeat/<pid>-<row> — unique per process AND per row"
else
  bad "the temp ref must be unique per process and per row: $(grep -n 'tmpref=' "$dl_body")"
fi
# ...and it must clean up after itself, or a long-lived checkout accumulates refs.
if grep -q 'update-ref -d "\$tmpref"' "$dl_body"; then
  ok "the temp ref is deleted after use (no ref accumulation in a long-lived checkout)"
else
  bad "cmd_dead_lanes must delete its temp ref"
fi
# NON-VACUITY of the whole block: the grep target really is present in the shipped file, so
# a silently-empty extraction cannot make these three checks pass.
if grep -q 'refs/machine-claims' "$dl_body"; then
  ok "NON-VACUITY: the extracted body is really cmd_dead_lanes (it references refs/machine-claims)"
else
  bad "NON-VACUITY broken: the extracted body does not look like cmd_dead_lanes"
fi
# A real run must leave NO temp refs behind.
(cd "$WORK" && bash "$HB" dead-lanes >/dev/null 2>&1) || true
if [ -z "$(cd "$WORK" && git for-each-ref 'refs/tmp/**' 2>/dev/null)" ]; then
  ok "after a real dead-lanes run, no refs/tmp/** remain in the checkout"
else
  bad "dead-lanes leaked temp refs: $(cd "$WORK" && git for-each-ref 'refs/tmp/**')"
fi

# ===========================================================================
echo "TEST 52: this slice is POSITIVE-DETECTION ONLY — dead-lanes never exits 0 (#3393 split ruling)"
# ===========================================================================
# The reason changed even though the assertion is back to where it started, and the distinction
# matters. Interim C withheld exit 0 because a per-MACHINE ref let a surviving sibling MASK a dead
# lane, so a clean verdict was a lie. Per-lane refs remove that mechanism, and the restoration was
# implemented and reviewed over four rounds — then split out, because the FAIL-OPEN family (five
# instances) clustered in this exit-0 path and it is the value a cron reads.
#
# So: not "exit 0 is unsound" any more, but "exit 0 is not in THIS slice". Restoring it is tracked
# separately with the family census carried forward.
craft_lane_claim "$WORK" "noClean" 3413 "$$" 0
nc_out=$(cd "$WORK" && PATH="$ALIVE_SHIM:$PATH" HEARTBEAT_MACHINE=noClean \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
nc_rc=$?
if [ "$nc_rc" -ne 0 ] \
  && printf '%s\n' "$nc_out" | grep -E '^noClean ' | grep -q 'ALIVE' \
  && printf '%s\n' "$nc_out" | grep -qi 'POSITIVE-DETECTION ONLY'; then
  ok "a measured-healthy local lane does NOT exit 0 (rc=$nc_rc) and the output says why — the clean verdict is out of this slice"
else
  bad "this slice must never exit 0: rc=$nc_rc out:
$nc_out"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/noClean/3413" 2>/dev/null || true)

# ===========================================================================
echo "TEST 52b: TWO lanes on ONE machine — a dead lane is seen beside a live sibling (#3393 AC3)"
# ===========================================================================
# THE CASE THAT WAS STRUCTURALLY IMPOSSIBLE BEFORE, and the whole point of ruling A. Under
# per-machine refs the live sibling's stamp overwrote the dead lane's ref, so a 4-lane box could
# report at most one lane and #3393's two same-host deaths were invisible. Both must now appear.
craft_lane_claim "$WORK" "multiLane" 4001 "$ABSENT_PID" 30   # dead
craft_lane_claim "$WORK" "multiLane" 4002 "$$" 0             # live sibling, same machine
ml_out=$(cd "$WORK" && PATH="$MIXED_SHIM:$PATH" HEARTBEAT_MACHINE=multiLane \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
ml_rc=$?
ml_rows=$(printf '%s\n' "$ml_out" | grep -cE '^multiLane ')
if [ "$ml_rc" -eq 3 ] \
  && [ "$ml_rows" -eq 2 ] \
  && printf '%s\n' "$ml_out" | grep -E '^multiLane +4001 ' | grep -q 'DEAD' \
  && printf '%s\n' "$ml_out" | grep -E '^multiLane +4002 ' | grep -q 'ALIVE'; then
  ok "both lanes on one machine are reported: 4001 DEAD beside a live 4002 (rc=3) — the blind spot ruling A closes"
else
  bad "a dead lane must be visible beside a live sibling on the same machine: rc=$ml_rc rows=$ml_rows out:
$ml_out"
fi
# NON-VACUITY: under the OLD layout these two lanes shared ONE ref, so only the last stamp survived
# and one of them could not have been reported at all. Demonstrated on the legacy namespace, which
# is still writable, rather than asserted in prose.
craft_old_claim "$WORK" "collapse" 4001 "$ABSENT_PID" 30
craft_old_claim "$WORK" "collapse" 4002 "$$" 0
legacy_refs=$(g -C "$WORK" ls-remote origin 'refs/machine-claims/collapse*' | wc -l | tr -d ' ')
if [ "$legacy_refs" -eq 1 ]; then
  ok "NON-VACUITY: two lanes written to the LEGACY per-machine layout collapse to $legacy_refs ref — the second overwrote the first, which is the masking ruling A removes"
else
  bad "NON-VACUITY broken: the legacy layout kept $legacy_refs refs for two lanes, so the collapse this test relies on did not happen"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/multiLane/4001" ":refs/lane-claims/multiLane/4002" 2>/dev/null || true)
(cd "$WORK" && g push -q origin ":refs/machine-claims/collapse" 2>/dev/null || true)

# ===========================================================================
echo "TEST 53: an unsafe-fetch git makes the run REFUSE, not clobber FETCH_HEAD"
# ===========================================================================
# roborev round 13 (Medium): omitting --no-write-fetch-head on git < 2.29 traded "I cannot
# measure safely" for "I may corrupt someone else's REAP decision" — the worse of the two.
# A monitor may decline to answer; it may not damage the thing it monitors. Driven by a git
# shim reporting an ancient version.
oldgit="$T/oldgit"
mkdir -p "$oldgit"
cat >"$oldgit/git" <<'GITEOF'
#!/usr/bin/env bash
if [ "$1" = "--version" ]; then echo "git version 2.20.1"; exit 0; fi
exec /usr/bin/git "$@"
GITEOF
chmod +x "$oldgit/git"
og_out=$(cd "$WORK" && PATH="$oldgit:$PATH" HEARTBEAT_MACHINE=anyMachine \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
og_rc=$?
if [ "$og_rc" -eq 1 ] \
  && printf '%s\n' "$og_out" | grep -qi 'without writing FETCH_HEAD' \
  && printf '%s\n' "$og_out" | grep -qi 'NOTHING was measured'; then
  ok "a git too old to fetch safely makes dead-lanes refuse (exit 1) and say nothing was measured"
else
  bad "an unsafe-fetch git must make the run refuse: rc=$og_rc out:
$og_out"
fi
# NON-VACUITY: the shim really does report an old version, and the REAL git is new enough —
# otherwise this case would pass on every host regardless of the guard.
# The real git must be >= 2.29, i.e. actually CAPABLE, or this case cannot show that the guard
# is what refused (roborev round 17, Low). The first cut checked only that the major version is
# at least 2, which is true of 2.0-2.28 as well — every version that LACKS the option.
real_ver="$(git --version | awk '{print $3}')"
real_major="${real_ver%%.*}"
real_rest="${real_ver#*.}"
real_minor="${real_rest%%.*}"
real_capable=false
case "$real_major$real_minor" in
  *[!0-9]* | '') : ;;
  *)
    if [ "$real_major" -gt 2 ] || { [ "$real_major" -eq 2 ] && [ "$real_minor" -ge 29 ]; }; then
      real_capable=true
    fi
    ;;
esac
if [ "$(PATH="$oldgit:$PATH" git --version)" = "git version 2.20.1" ] && [ "$real_capable" = true ]; then
  ok "NON-VACUITY: the shim reports 2.20.1 while the real git ($real_ver) DOES support --no-write-fetch-head — so the guard, not the host, is what refused"
else
  bad "NON-VACUITY broken: shim='$(PATH="$oldgit:$PATH" git --version)' real='$real_ver' capable=$real_capable — TEST 53 cannot attribute the refusal to the guard"
fi

# ===========================================================================
echo "TEST 54: absence needs the INDEPENDENT probe to affirm it (round 15, Medium)"
# ===========================================================================
# Round 10 stopped the correlated pair (ps + /proc, hidden together by hidepid=2) from
# declaring absence when the signal probe answered `denied`. But when the signal probe answers
# `unknown` — a transient failure, or a message in a language it does not recognise — the pair
# was alone again and could still establish absence: a false DEAD for a live supervisor.
# Driven by a ps that hides the pid AND a kill whose failure message is unrecognisable.
pp_body="$T/presence.sh"
sed -n '/^signal_probe_class()/,/^}/p' "$HB" >"$pp_body"
sed -n '/^process_presence()/,/^}/p' "$HB" >>"$pp_body"
# shellcheck disable=SC1090
. "$pp_body" 2>/dev/null || true
if [ "$(type -t process_presence 2>/dev/null)" = "function" ]; then
  ok "extracted process_presence + signal_probe_class from the shipped script"
  (
    # ps hides everything; kill fails with an unrecognisable message => signal probe unknown.
    ps() { return 1; }
    kill() {
      echo "bash: kill: ($2) - Some condition nobody parsed" >&2
      return 1
    }
    r="$(process_presence "$ABSENT_PID")"
    [ "$r" = "unknown" ] && exit 0
    echo "  (visibility-only absence returned '$r')" >&2
    exit 1
  )
  if [ "$?" -eq 0 ]; then
    ok "with the signal probe UNKNOWN, negative visibility probes alone yield 'unknown' — not 'absent'"
  else
    bad "visibility probes alone must not establish absence when the independent probe cannot"
  fi
  # NON-VACUITY: when the signal probe DOES affirm absence, the same inputs give 'absent', so
  # the fix has not simply made absence unreachable.
  (
    ps() { return 1; }
    kill() {
      echo "bash: kill: ($2) - No such process" >&2
      return 1
    }
    r="$(process_presence "$ABSENT_PID")"
    [ "$r" = "absent" ] && exit 0
    echo "  (affirmed absence returned '$r')" >&2
    exit 1
  )
  if [ "$?" -eq 0 ]; then
    ok "NON-VACUITY: with the signal probe affirming ESRCH, the same fixture IS 'absent' — absence is still reachable"
  else
    bad "NON-VACUITY broken: absence is unreachable even when the independent probe affirms it"
  fi
else
  bad "could not load process_presence from the shipped script"
fi

# ===========================================================================
echo "TEST 55: a SLOW ps cannot buy an ALIVE verdict (round 15, Medium)"
# ===========================================================================
# `start = now - elapsed` needs `now` and `elapsed` to name the same instant, and they cannot.
# The first cut sampled `now` BEFORE running ps, so a slow ps shifted the computed start
# BACKWARD — making a REUSED pid look like it predates the claim, a false ALIVE. The delay is
# likeliest on exactly the exhausted hosts this command is for. The query is now bracketed and
# the start is an INTERVAL; a verdict requires the whole interval to sit on one side.
#
# Driven by a ps that sleeps well past the tolerance before answering. The claim is stamped
# NOW and the process reports elapsed=0, so without bracketing the delay pushes the computed
# start before the claim ts and yields ALIVE.
slowshim="$T/slowshim"
mkdir -p "$slowshim"
# THE NUMBERS ARE CHOSEN TO DISCRIMINATE (roborev round 16, Low). My first fixture used
# elapsed=0 with a 5s sleep, which does NOT distinguish the two implementations: the point
# calculation used the PRE-sleep clock, so its start landed inside the tolerance band and read
# UNKNOWN either way. To separate them the point estimate must fall on the ALIVE side while the
# interval straddles the boundary. With cts = craft time, t0 = now before ps, t1 = t0 + 20:
#   point (old):    start = t0 - 10  =>  ALIVE requires t0 - 10 < cts - 2, true for t0 < cts+8
#   interval (new): [t0-10, t1-10]   =>  ALIVE requires t1 - 10 < cts - 2, i.e. t0 + 8 < cts,
#                                        false; DEAD requires t0 - 10 > cts + 2, also false
#                                        => UNKNOWN
# The run reaches ps a second or two after crafting, so t0-cts is ~0-8s and both hold with
# margin. Verified: this fixture reads ALIVE against the pre-fix script and UNKNOWN here.
cat >"$slowshim/ps" <<'PSEOF'
#!/usr/bin/env bash
for a in "$@"; do
  case "$a" in
    stat=)   echo "S"; exit 0 ;;
    etimes=) sleep 20; echo 10; exit 0 ;;
  esac
done
exit 0
PSEOF
chmod +x "$slowshim/ps"
craft_old_claim "$WORK" "slowPs" 3415 "$$" 0
sp_out=$(cd "$WORK" && PATH="$slowshim:$PATH" HEARTBEAT_MACHINE=slowPs \
  CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$sp_out" | grep -E '^slowPs ' | grep -q 'UNKNOWN-IDENTITY' \
  && ! printf '%s\n' "$sp_out" | grep -E '^slowPs ' | grep -q 'ALIVE'; then
  ok "a ps slow enough to matter yields UNKNOWN-IDENTITY, not an ALIVE bought by measurement delay"
else
  bad "a slow ps must not produce ALIVE: out:
$sp_out"
fi
# ...and the reported interval must actually be a RANGE, or the bracketing is cosmetic.
if printf '%s\n' "$sp_out" | grep -E '^slowPs ' | grep -qE 'started somewhere in \[-?[0-9]+, -?[0-9]+\]s'; then
  ok "the detail reports the start as an INTERVAL, so the measurement uncertainty is visible to the operator"
else
  bad "the identity detail must report the bracketed interval: $(printf '%s\n' "$sp_out" | grep -E '^slowPs ')"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/slowPs" 2>/dev/null || true)

# ===========================================================================
echo "TEST 56: an UNREADABLE LOCAL claim is not reported as 'no local claim' (round 16, Low)"
# ===========================================================================
# The closing diagnostic is what tells an operator where to look. A local ref whose fetch fails
# never incremented local_seen, so the run said 'none is owned by this machine' about a machine
# that owns one — sending the reader to the wrong box.
dangling2="deadbeef11111111111111111111111111111111"
printf '%s\n' "$dangling2" >"$ORIGIN/refs/machine-claims/unreadableLocal"
ul_out=$(cd "$WORK" && HEARTBEAT_MACHINE=unreadableLocal CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
ul_rc=$?
if [ "$ul_rc" -eq 1 ] \
  && printf '%s\n' "$ul_out" | grep -qi 'DO belong to this machine' \
  && ! printf '%s\n' "$ul_out" | grep -qi 'none is owned by this machine'; then
  ok "an unreadable LOCAL claim reports 'belongs to this machine but could not be read', not 'no local claim'"
else
  bad "the unreadable-local diagnostic must not claim there is no local claim: rc=$ul_rc out:
$ul_out"
fi
# NON-VACUITY: viewed from a machine that owns NOTHING, the same fixture still gives the
# all-foreign message — so the two diagnostics are genuinely distinct rather than one renamed.
uf_out=$(cd "$WORK" && HEARTBEAT_MACHINE=ownsNothingAtAll CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$uf_out" | grep -qi 'none is owned by this machine'; then
  ok "NON-VACUITY: a machine owning no claim still gets the all-foreign diagnostic — the two messages are distinct"
else
  bad "NON-VACUITY broken: the all-foreign diagnostic no longer appears: out:
$uf_out"
fi
rm -f "$ORIGIN/refs/machine-claims/unreadableLocal"

# ===========================================================================
echo "TEST 57: should-reap's two-argument form is ALWAYS the legacy threshold, never a lane"
# ===========================================================================
# roborev round 1 (Medium) rejected the earlier design, which probed the remote to decide whether
# `should-reap <machine> <N>` meant a threshold or a lane issue. The flaw is that the CI workflow's
# legacy threshold is literally `14400`, so the moment some lane legitimately carries issue 14400 the
# same call would silently change meaning — and could then delete an unrelated legacy ref on the
# lane's verdict. A grammar whose meaning depends on which refs happen to exist is not a grammar.
#
# So the ambiguity is REMOVED, and this pins it in the case that used to break: a lane ref named
# exactly like a plausible threshold EXISTS, and the two-argument call must still mean the threshold.
craft_lane_claim "$WORK" "grammarBox" 14400 "$ABSENT_PID" 20000
gram_out=$(cd "$WORK" && HEARTBEAT_MACHINE=grammarBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap grammarBox 14400 2>&1)
gram_rc=$?
# rc=2 is "no such claim ref": the LEGACY ref refs/machine-claims/grammarBox does not exist. If the
# argument had been read as a lane issue it would have judged the lane ref that DOES exist instead.
if [ "$gram_rc" -eq 2 ] && ! printf '%s\n' "$gram_out" | grep -q 'lane-claims/grammarBox/14400'; then
  ok "two arguments mean the THRESHOLD even though a lane ref named 14400 exists — the call cannot change meaning with the ref set"
else
  bad "the two-arg form must always be the legacy threshold: rc=$gram_rc out:
$gram_out"
fi
# NON-VACUITY: the lane ref really is there, and the THREE-argument form does judge it — otherwise
# the case above would pass for a command that simply cannot see lanes at all.
gram2_out=$(cd "$WORK" && HEARTBEAT_MACHINE=grammarBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap grammarBox 14400 14400 2>&1)
gram2_rc=$?
if [ "$gram2_rc" -eq 0 ] && printf '%s\n' "$gram2_out" | grep -q 'lane-claims/grammarBox/14400'; then
  ok "NON-VACUITY: the three-arg form DOES judge lane 14400 (reaped, ref named) — so the two-arg case above is about the grammar, not about blindness"
else
  bad "the three-arg form must judge the lane: rc=$gram2_rc out:
$gram2_out"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/grammarBox/14400" 2>/dev/null || true)

# ===========================================================================
echo "TEST 58: a p<pid> placeholder lane id round-trips through stamp/should-reap/reap"
# ===========================================================================
# A supervisor whose issue is not yet known stamps `p<pid>` rather than the old shared "0" (roborev
# round 1, High: the shared placeholder made every unknown-issue supervisor on a machine write the
# SAME ref, re-creating the masking per-lane refs exist to remove). Every consumer must accept it.
(cd "$WORK" && HEARTBEAT_MACHINE=phBox bash "$HB" stamp p4242 1 >/dev/null 2>&1)
ph_ref=$(g -C "$WORK" ls-remote origin 'refs/lane-claims/phBox/p4242' | awk '{print $1}')
if [ -n "$ph_ref" ]; then
  ok "stamp accepts a p<pid> lane id and writes refs/lane-claims/phBox/p4242"
else
  bad "stamp must accept a p<pid> lane id"
fi
# It must NOT be mistaken for an issue: the open-PR guard has no issue to consult, so reap proceeds.
ph_reap=$(cd "$WORK" && HEARTBEAT_MACHINE=phBox CLAIM_OPEN_PR_CMD="$HAS_OPEN_PR" \
  bash "$HB" reap phBox p4242 2>&1)
if [ -z "$(g -C "$WORK" ls-remote origin 'refs/lane-claims/phBox/p4242' | awk '{print $1}')" ]; then
  ok "reap accepts p<pid> and does not block on the open-PR guard — a placeholder names no issue to protect"
else
  bad "reap must delete a p<pid> lane ref: out: $ph_reap"
fi
# ...and a malformed lane id is refused rather than silently creating a junk ref.
(cd "$WORK" && HEARTBEAT_MACHINE=phBox bash "$HB" stamp pnotanumber 1 >/dev/null 2>&1); mal_rc=$?
if [ "$mal_rc" -eq 64 ] && [ -z "$(g -C "$WORK" ls-remote origin 'refs/lane-claims/phBox/pnotanumber' | awk '{print $1}')" ]; then
  ok "a malformed lane id is refused (rc=64) and creates no ref"
else
  bad "a malformed lane id must be refused: rc=$mal_rc"
fi

# ===========================================================================
echo "TEST 59: delete_ref_guarded confirms ABSENCE rather than assuming it (round 3, Medium)"
# ===========================================================================
# It treated every failed `ls-remote` as "already absent" and returned SUCCESS, so a transient remote
# or auth failure made the supervisor log a successful claim clear — and CI proceed as though a ref
# had been reaped — while the ref was still there. `--exit-code` gives 2 for a confirmed no-match and
# something else (measured: 128) for an operational failure; only the former is absence.
abs_out=$(cd "$WORK" && HEARTBEAT_REMOTE=no-such-remote bash "$HB" reap somebox 4242 2>&1)
abs_rc=$?
if [ "$abs_rc" -ne 0 ] \
  && printf '%s\n' "$abs_out" | grep -qi 'could not determine whether' \
  && ! printf '%s\n' "$abs_out" | grep -qi 'already absent'; then
  ok "an unreadable remote makes reap FAIL rather than report a successful clear (rc=$abs_rc)"
else
  bad "an unreadable remote must not be read as 'already absent': rc=$abs_rc out:
$abs_out"
fi
# NON-VACUITY: a genuinely absent ref on a REACHABLE remote is still the quiet success path.
gone_out=$(cd "$WORK" && bash "$HB" reap somebox 4242 2>&1); gone_rc=$?
if [ "$gone_rc" -eq 0 ] && printf '%s\n' "$gone_out" | grep -qi 'already absent'; then
  ok "NON-VACUITY: a confirmed-absent ref still returns 0 with 'already absent' — only the unreadable case changed"
else
  bad "a confirmed-absent ref must still succeed quietly: rc=$gone_rc out:
$gone_out"
fi

# ===========================================================================
echo "TEST 60: a reap DELETE takes a compare-and-swap lease (round 3, Medium)"
# ===========================================================================
# Reaping was described as atomic and was not: should-reap judged one value and the delete removed
# whatever was there NOW, so a supervisor refresh landing in between was destroyed and its board item
# flipped back to Ready under a live lane. Same CAS discipline as `claim.sh adopt --expect`.
craft_lane_claim "$WORK" "leaseBox" 5501 "$ABSENT_PID" 30
lease_sha=$(g -C "$WORK" ls-remote origin 'refs/lane-claims/leaseBox/5501' | awk '{print $1}')
stale_out=$(cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" reap leaseBox 5501 deadbeefdeadbeefdeadbeefdeadbeefdeadbeef 2>&1)
stale_rc=$?
still_there=$(g -C "$WORK" ls-remote origin 'refs/lane-claims/leaseBox/5501' | awk '{print $1}')
if [ "$stale_rc" -eq 4 ] && [ -n "$still_there" ] \
  && printf '%s\n' "$stale_out" | grep -qi 'lease.*was not held'; then
  ok "a STALE lease refuses the delete (rc=4) and the ref survives — a concurrent refresh cannot be destroyed"
else
  bad "a stale lease must refuse and preserve the ref: rc=$stale_rc still='$still_there' out:
$stale_out"
fi
# NON-VACUITY: the CORRECT lease deletes, so the refusal is about the lease and not a broken delete.
good_out=$(cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" reap leaseBox 5501 "$lease_sha" 2>&1)
good_rc=$?
if [ "$good_rc" -eq 0 ] && [ -z "$(g -C "$WORK" ls-remote origin 'refs/lane-claims/leaseBox/5501' | awk '{print $1}')" ]; then
  ok "NON-VACUITY: the correct lease DOES delete (rc=0) — the refusal above is the lease working, not a broken path"
else
  bad "the correct lease must delete: rc=$good_rc out:
$good_out"
fi

# ===========================================================================
echo "TEST 61: a PLACEHOLDER lane is never automatically reaped (round 3, Medium)"
# ===========================================================================
# A `p…` id names no issue, so the open-PR guard has nothing to consult — and a worker can have
# claimed an issue and opened a PR before its supervisor received the marker. Reaping it would delete
# the claim of a lane with an unfinished endgame (#2499). should-reap must decline it outright.
craft_lane_claim "$WORK" "phReap" "p777-abc12345" "$ABSENT_PID" 20000
ph_sr=$(cd "$WORK" && HEARTBEAT_MACHINE=phReap CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap phReap p777-abc12345 1 2>&1)
ph_sr_rc=$?
if [ "$ph_sr_rc" -eq 1 ] && printf '%s\n' "$ph_sr" | grep -qi 'names no issue'; then
  ok "should-reap KEEPS a placeholder lane even when stale and pid-dead (rc=1) — an open PR cannot be ruled out"
else
  bad "a placeholder must never be automatically reaped: rc=$ph_sr_rc out:
$ph_sr"
fi
# NON-VACUITY: an equally stale NUMERIC lane in the same state IS reapable, so the refusal is about
# the placeholder and not about should-reap having stopped working.
craft_lane_claim "$WORK" "phReap" 5502 "$ABSENT_PID" 20000
num_sr_rc=0
(cd "$WORK" && HEARTBEAT_MACHINE=phReap CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap phReap 5502 1 >/dev/null 2>&1) || num_sr_rc=$?
if [ "$num_sr_rc" -eq 0 ]; then
  ok "NON-VACUITY: an equally stale NUMERIC lane is reapable (rc=0) — only the placeholder is declined"
else
  bad "a stale numeric lane must still be reapable: rc=$num_sr_rc"
fi
# ...and the owning supervisor can still clear its OWN placeholder directly, which is how a clean
# exit works — it knows it is finished, whereas a reaper cannot.
(cd "$WORK" && HEARTBEAT_MACHINE=phReap CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" reap phReap p777-abc12345 >/dev/null 2>&1) || true
if [ -z "$(g -C "$WORK" ls-remote origin 'refs/lane-claims/phReap/p777-abc12345' | awk '{print $1}')" ]; then
  ok "a direct reap of a placeholder still works, so a supervisor's clean exit can clear its own lane"
else
  bad "a direct reap must still clear a placeholder"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/phReap/5502" 2>/dev/null || true)

# ===========================================================================
echo "TEST 62: the lane-id grammar is EXACT — 0, 00, pdead and p- are all refused (round 4, Medium)"
# ===========================================================================
# The first cut was a loose character class, which MEASURABLY accepted `0`, `00`, `pdead` and `p-`.
# `0` is the serious one: it recreates the single shared refs/lane-claims/<machine>/0 whose
# collisions are the dead-lane masking this entire change exists to remove. A guard that admits the
# original defect is not a guard. `pdead` passed because d/e/a are hex digits — a character class is
# not a grammar.
for bad_id in 0 00 pdead p- p; do
  (cd "$WORK" && HEARTBEAT_MACHINE=gramBox bash "$HB" stamp "$bad_id" 1 >/dev/null 2>&1); bad_rc=$?
  bad_ref=$(g -C "$WORK" ls-remote origin "refs/lane-claims/gramBox/${bad_id}" | awk '{print $1}')
  if [ "$bad_rc" -eq 64 ] && [ -z "$bad_ref" ]; then
    ok "lane id '$bad_id' is refused (rc=64) and creates no ref"
  else
    bad "lane id '$bad_id' must be refused: rc=$bad_rc ref='$bad_ref'"
  fi
done
# NON-VACUITY: the two VALID shapes are still accepted, or the grammar would just be broken.
for good_id in 3367 p123 p123-abc12345; do
  (cd "$WORK" && HEARTBEAT_MACHINE=gramBox bash "$HB" stamp "$good_id" 1 >/dev/null 2>&1)
  if [ -n "$(g -C "$WORK" ls-remote origin "refs/lane-claims/gramBox/${good_id}" | awk '{print $1}')" ]; then
    ok "NON-VACUITY: valid lane id '$good_id' is accepted"
  else
    bad "valid lane id '$good_id' must be accepted"
  fi
  (cd "$WORK" && g push -q origin ":refs/lane-claims/gramBox/${good_id}" 2>/dev/null || true)
done

# ===========================================================================
echo "TEST 63: the open-PR safeguard fails CLOSED when the claim message is unreadable (round 4)"
# ===========================================================================
# `ref_msg_field` returned EMPTY on a failed fetch, and delete_ref_guarded reads it to find the issue
# for the open-PR safeguard — so a transient failure silently SKIPPED the safeguard and deleted a
# claim whose endgame was unfinished. Absence of an answer was being used as an answer. Exercised on
# a LEGACY ref, which is the shape that still needs the message (a per-lane ref carries the issue in
# its path, so the guard no longer depends on parsing anything).
dangling3="deadbeef22222222222222222222222222222222"
printf '%s\n' "$dangling3" >"$ORIGIN/refs/machine-claims/unreadableMsg"
fc_out=$(cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" reap unreadableMsg 2>&1)
fc_rc=$?
still_msg=$(g -C "$WORK" ls-remote origin 'refs/machine-claims/unreadableMsg' | awk '{print $1}')
if [ "$fc_rc" -ne 0 ] && [ -n "$still_msg" ] \
  && printf '%s\n' "$fc_out" | grep -qi 'could not be read'; then
  ok "an unreadable claim message REFUSES the delete (rc=$fc_rc) and the ref survives — the open-PR safeguard is not skipped"
else
  bad "an unreadable claim message must fail closed: rc=$fc_rc still='$still_msg' out:
$fc_out"
fi
rm -f "$ORIGIN/refs/machine-claims/unreadableMsg"

# ===========================================================================
echo "TEST 64: list-claims reports an unreadable namespace instead of 'no claims found' (round 4)"
# ===========================================================================
lc_out=$(cd "$WORK" && HEARTBEAT_REMOTE=no-such-remote bash "$HB" list-claims 2>&1)
lc_rc=$?
if [ "$lc_rc" -ne 0 ] \
  && printf '%s\n' "$lc_out" | grep -qi 'could not list claim refs' \
  && ! printf '%s\n' "$lc_out" | grep -qi 'no claims found'; then
  ok "list-claims on an unreadable remote exits non-zero and says the listing is incomplete"
else
  bad "list-claims must not render an outage as 'no claims found': rc=$lc_rc out:
$lc_out"
fi
# ...and a PLACEHOLDER lane must be identifiable in the table, since placeholders are never
# auto-reaped and an operator needs to know WHICH ref to clean up by hand.
craft_lane_claim "$WORK" "dispBox" "p88-c0ffee" "$ABSENT_PID" 30
disp_out=$(cd "$WORK" && bash "$HB" list-claims 2>&1)
if printf '%s\n' "$disp_out" | grep -qE '^dispBox +p88-c0ffee '; then
  ok "list-claims shows the placeholder lane id from the ref PATH, not '?'"
else
  bad "a placeholder lane must be identifiable in list-claims: out:
$(printf '%s\n' "$disp_out" | head -5)"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/dispBox/p88-c0ffee" 2>/dev/null || true)

# ===========================================================================
echo "TEST 65: no function is defined TWICE in the flow scripts (round 4 near-miss)"
# ===========================================================================
# A structural guard earned the hard way. Rewriting `ref_msg_field` to fail closed left the ORIGINAL
# fail-open definition in place further down the file, and bash uses the LAST definition — so the
# hardened version was dead code and the open-PR safeguard was still being skipped. Every behavioural
# test passed; only a direct trace of the function showed rc=0 on an unreadable ref. CLAUDE.md already
# warns about exactly this for agent-gate's `_tree*` helpers, which is why it deserves a guard rather
# than a comment.
for _fnfile in "$HB" "$SCRIPT_DIR/../flow/claim.sh" "$SCRIPT_DIR/../local/worker-supervisor.sh"; do
  [ -r "$_fnfile" ] || continue
  _dupes=$(grep -oE '^[a-z_][a-z0-9_]*\(\) \{' "$_fnfile" | sort | uniq -d || true)
  if [ -z "$_dupes" ]; then
    ok "no duplicate function definition in $(basename "$_fnfile") — a later definition would silently shadow an earlier one"
  else
    bad "duplicate function definition(s) in $(basename "$_fnfile"): $(printf '%s' "$_dupes" | tr '\n' ' ') — bash uses the LAST one, so the earlier is dead code"
  fi
done

# ===========================================================================
echo "TEST 66: NO shipped flow script or workflow reads shared FETCH_HEAD (#3393 self-sweep)"
# ===========================================================================
# TEST 51 pinned this for `cmd_dead_lanes` alone, and the sweep that followed found three more
# readers the narrow guard could not see: `cmd_list`, `cmd_list_claims` and the CI reaper's legacy
# message parse. FETCH_HEAD is shared per-worktree, so any concurrent fetch can make a read describe
# ANOTHER ref — and in the reaper's case the misread value is the issue the board flip acts on.
# Widened from one function to every shipped file, because the class kept reappearing one caller over.
_fh_leaks=""
for _fh_file in "$SCRIPT_DIR/../flow/claim-heartbeat.sh" "$SCRIPT_DIR/../flow/claim.sh" \
  "$SCRIPT_DIR/../local/worker-supervisor.sh" "$SCRIPT_DIR/../../.github/workflows/project-board-sync.yml"; do
  [ -r "$_fh_file" ] || continue
  _hits=$(grep -nE 'git[[:space:]]+(log|show|rev-parse|cat-file|for-each-ref)[^|]*FETCH_HEAD' "$_fh_file" \
    | grep -vE '^[0-9]+:[[:space:]]*#' || true)
  [ -z "$_hits" ] || _fh_leaks="$_fh_leaks $(basename "$_fh_file"):$(printf '%s' "$_hits" | head -1 | cut -d: -f1)"
done
if [ -z "$_fh_leaks" ]; then
  ok "no shipped flow script or workflow reads FETCH_HEAD with a revision-reading git command"
else
  bad "FETCH_HEAD is still read in:${_fh_leaks} — a concurrent fetch can make that read describe another ref"
fi
# NON-VACUITY: the guard must actually be able to see a read. Planted in a scratch copy.
_fh_probe="$T/fh-probe.sh"
printf '%s\n' '#!/usr/bin/env bash' 'msg=$(git log -1 --format=%B FETCH_HEAD)' >"$_fh_probe"
if grep -qE 'git[[:space:]]+(log|show|rev-parse|cat-file|for-each-ref)[^|]*FETCH_HEAD' "$_fh_probe"; then
  ok "NON-VACUITY: the same pattern DOES match a planted FETCH_HEAD read, so the clean result above is a measurement"
else
  bad "NON-VACUITY broken: the guard pattern cannot even match a planted read"
fi

# ===========================================================================
echo "TEST 67: --help emits HELP, and the shebang is on line 1 (round 5 blind spot)"
# ===========================================================================
# THE SUITE MISSED A BROKEN SCRIPT FOR AN ENTIRE ROUND. An edit inserted a function ABOVE the
# shebang, so line 1 was no longer `#!/usr/bin/env bash` and `print_help` — which awks from line 2 to
# the `---END-HELP---` marker — emitted the FUNCTION'S comment block as help text. 129 tests passed
# through it, because every one of them invokes the script as `bash "$HB"`, which does not care about
# line 1, and none of them read `--help` for its content. A structural property no test asserted.
if [ "$(head -1 "$HB")" = '#!/usr/bin/env bash' ]; then
  ok "the shebang is on line 1 (an edit above it silently corrupts print_help, which awks from line 2)"
else
  bad "line 1 of $(basename "$HB") is not the shebang: '$(head -1 "$HB")'"
fi
_help67=$(cd "$WORK" && bash "$HB" --help 2>&1 || true)
# The help must open with the script's own banner and must NOT contain a function definition.
# PIPELINE-FREE (roborev job 15, finding 1 — the same SIGPIPE class, found here by this round's
# plant runs rather than predicted). `printf | head -5 | grep -q` under this suite's `set -o
# pipefail` reports a FALSE FAIL whenever `head`/`grep` exits before the writer finishes: measured
# 1 spurious failure in 60 runs of this exact condition, 0 in 60 of the form below. The help
# content was correct every time; only the pipeline status was not.
_help67_head="$(head -5 <<<"$_help67")"
if grep -q 'claim-heartbeat.sh' <<<"$_help67_head" \
  && ! grep -qE '^[a-z_][a-z0-9_]*\(\) \{' <<<"$_help67"; then
  ok "--help emits the help block and leaks no function body"
else
  bad "--help does not look like help: first lines:
$(printf '%s\n' "$_help67" | head -3)"
fi
# NON-VACUITY, and the probe has to MIRROR THE REAL DEFECT to be worth anything. My first attempt
# planted a bare function definition at line 1 and failed — because `print_help` awks from line 2,
# so line 1 is the one thing it can never emit. The actual defect leaked the function's COMMENT
# BLOCK, which sat above its definition and therefore inside the awk range. Planted the same way.
_bad_copy="$T/hdr-probe.sh"
# The marker goes on line TWO, not line one: `print_help` awks from NR>=2, so line 1 is the single
# line it can never emit. Getting that wrong is what made my first two probe attempts fail.
{ printf '%s\n' '# displaced header line one' '# leaked_marker_67 this line is above the shebang' \
    'leaked_fn() { :; }'; cat "$HB"; } >"$_bad_copy"
# CAPTURED, NOT PIPED. `bash … | grep -q` under this file's `pipefail` is the #3387 flake: grep -q
# exits at the first match, SIGPIPEs the upstream, and the PIPELINE status becomes 141 — so a
# SUCCESSFUL match reads as a failed condition, non-deterministically depending on whether the
# upstream had finished writing. Observed here as pass-then-fail with no change in between, which is
# exactly how #3387 presents. I diagnosed that class this morning and then wrote one.
_bad_help="$(bash "$_bad_copy" --help 2>&1 || true)"
if [ "$(head -1 "$_bad_copy")" != '#!/usr/bin/env bash' ] \
  && case "$_bad_help" in *leaked_marker_67*) true ;; *) false ;; esac; then
  ok "NON-VACUITY: content planted above the shebang IS leaked into --help and detected by both halves"
else
  bad "NON-VACUITY broken: the check cannot detect content planted above the shebang"
fi

# ===========================================================================
echo "TEST 68: an ABSENT field in a readable claim message fails closed (round 5, Medium)"
# ===========================================================================
# `ref_msg_field` ended in a pipeline, so a readable-but-MALFORMED message (no `issue=` token) gave
# an empty value and exit 0 — indistinguishable from "there is no issue". delete_ref_guarded reads it
# for the open-PR safeguard, so the safeguard was skipped. The round-4 fix closed the unreadable-FETCH
# path and left this one open, which is the same shape one branch over.
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  # A well-formed commit whose message carries NO issue= field.
  cs=$(GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim machine=malformedBox pid=4242 ts=2026-08-29T00:00:00Z")
  g push -q origin "${cs}:refs/machine-claims/malformedBox"
)
mf_out=$(cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" reap malformedBox 2>&1)
mf_rc=$?
mf_still=$(g -C "$WORK" ls-remote origin 'refs/machine-claims/malformedBox' | awk '{print $1}')
if [ "$mf_rc" -ne 0 ] && [ -n "$mf_still" ] \
  && printf '%s\n' "$mf_out" | grep -qi 'could not be read'; then
  ok "a claim message with no issue= field REFUSES the delete (rc=$mf_rc) and the ref survives — the open-PR safeguard is not skipped"
else
  bad "an absent issue field must fail closed: rc=$mf_rc still='$mf_still' out:
$mf_out"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/malformedBox" 2>/dev/null || true)

# ===========================================================================
echo "TEST 69: the old-git refusal covers EVERY fetching subcommand (round 17, Medium)"
# ===========================================================================
# `dead-lanes` refused on a git that cannot fetch privately; the five OTHER subcommands that
# fetch the same way did not. On git < 2.29 `list`/`list-claims` printed every row
# `fetch-failed` and still exited 0 — a listing that measured NOTHING while reporting success —
# and `clear`/`reap`/`should-reap` lost the claim metadata their open-PR safeguard reads.
# Driven by the TEST 53 shim, so one version fact is checked at every entry point.
craft_lane_claim "$WORK" "oldgitBox" "7001" "$ABSENT_PID" 30
for sub in "list" "list-claims" "clear oldgitBox" "reap oldgitBox 7001" "should-reap oldgitBox 7001" "dead-lanes"; do
  # shellcheck disable=SC2086
  og2_out=$(cd "$WORK" && PATH="$oldgit:$PATH" HEARTBEAT_MACHINE=oldgitBox \
    CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" $sub 2>&1)
  og2_rc=$?
  if [ "$og2_rc" -ne 0 ] \
    && printf '%s\n' "$og2_out" | grep -qi 'without writing FETCH_HEAD' \
    && printf '%s\n' "$og2_out" | grep -qi 'NOTHING was measured' \
    && ! printf '%s\n' "$og2_out" | grep -qi 'fetch-failed'; then
    ok "'$sub' refuses on a git too old to fetch privately (rc=$og2_rc), naming git as the cause and printing no per-row verdict"
  else
    bad "'$sub' must refuse rather than measure nothing and report success: rc=$og2_rc out:
$og2_out"
  fi
done
# THE REFUSAL MUST NOT SPREAD TO THE PUSH-ONLY SUBCOMMANDS (the FAIL-SHUT family, #3464).
# `beat` and `stamp` never fetch, so an old git is irrelevant to them; a guard that stopped
# them too would break a caller for which the missing capability is legitimately unneeded —
# which is exactly how the round-5 `ref_msg_field` tightening broke `should-reap`.
for pushsub in "beat 7001" "stamp 7001 $$"; do
  # shellcheck disable=SC2086
  ps_out=$(cd "$WORK" && PATH="$oldgit:$PATH" HEARTBEAT_MACHINE=oldgitBox bash "$HB" $pushsub 2>&1)
  ps_rc=$?
  if [ "$ps_rc" -eq 0 ] && ! printf '%s\n' "$ps_out" | grep -qi 'NOTHING was measured'; then
    ok "'$pushsub' still succeeds on an old git — it only pushes, so the fetch guard must not reach it"
  else
    bad "the fetch guard must NOT break a push-only subcommand: '$pushsub' rc=$ps_rc out:
$ps_out"
  fi
done
(cd "$WORK" && g push -q origin ":refs/lane-claims/oldgitBox/7001" ":refs/heartbeats/oldgitBox" 2>/dev/null || true)
(cd "$WORK" && g push -q origin ":refs/lane-claims/oldgitBox/p$$" 2>/dev/null || true)

# ===========================================================================
echo "TEST 70: empty per-lane + FAILED legacy listing reports INCOMPLETE (round 17, Low)"
# ===========================================================================
# With no per-lane refs AND the legacy listing failing, `raw` is empty for two different
# reasons and the message asserted the wrong one: "no claim refs exist" is a claim about a
# namespace nobody read. The exit code was already 1, so only the sentence was wrong — and a
# monitor's sentence is what an operator acts on. Driven by a git shim that fails ls-remote
# for the LEGACY refspec only, while reporting a modern version so the dispatch guard passes.
legacyfail="$T/legacyfail"
mkdir -p "$legacyfail"
cat >"$legacyfail/git" <<'LFEOF'
#!/usr/bin/env bash
# Scan ALL arguments, not $1: git takes global options before the subcommand
# (`git -C <dir> ls-remote ...`), so a $1-only shim silently passes through every
# invocation that carries one — which is how the first cut of this test's own
# NON-VACIUTY probe failed to prove anything.
_is_lsremote=false
_hits_legacy=false
for a in "$@"; do
  [ "$a" = "ls-remote" ] && _is_lsremote=true
  case "$a" in refs/machine-claims/*) _hits_legacy=true ;; esac
done
if [ "$_is_lsremote" = true ] && [ "$_hits_legacy" = true ]; then
  echo "fatal: injected legacy-listing failure" >&2
  exit 2
fi
exec /usr/bin/git "$@"
LFEOF
chmod +x "$legacyfail/git"
# A SEPARATE, GENUINELY EMPTY REMOTE. Against `origin` the earlier cases have left
# lane-claims refs behind, so `raw` is non-empty, the new branch is never reached, and the
# assertion passes on the legacy note's own "INCOMPLETE" instead — a vacuous pass of exactly
# the kind this PR exists to remove. Caught here by the two NON-VACUITY halves below.
EMPTYREMOTE="$T/empty-origin.git"
g init --bare -q "$EMPTYREMOTE"
lf_out=$(cd "$WORK" && PATH="$legacyfail:$PATH" HEARTBEAT_MACHINE=noSuchLaneBox \
  HEARTBEAT_REMOTE="$EMPTYREMOTE" CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
lf_rc=$?
# ASSERT ON A PHRASE THE DENIAL CANNOT CONTAIN. The first cut forbade the substring
# "no claim refs exist", which the new message itself quotes in order to deny it
# ("...it is NOT 'no claim refs exist'") — so a correct fix failed its own test. A negative
# text assertion is defeated by any message that names the thing it rules out; key it on the
# ASSERTING sentence's own tail instead, which only the empty-namespace claim carries.
if [ "$lf_rc" -ne 0 ] \
  && printf '%s\n' "$lf_out" | grep -qi 'never determined' \
  && ! printf '%s\n' "$lf_out" | grep -q 'That is NOT the same as an idle fleet'; then
  ok "an empty per-lane namespace with a FAILED legacy listing reports INCOMPLETE (rc=$lf_rc), never 'no claim refs exist'"
else
  bad "an unread legacy namespace must not be asserted empty: rc=$lf_rc out:
$lf_out"
fi
# NON-VACUITY, BOTH HALVES. (a) the shim really does break ONLY the legacy refspec, so the
# per-lane listing genuinely succeeded-empty; (b) with the shim absent the SAME state prints
# the empty-namespace sentence, so the branch above is reached by the injected failure and not
# by something incidental.
if (cd "$WORK" && PATH="$legacyfail:$PATH" git ls-remote "$EMPTYREMOTE" 'refs/machine-claims/*' >/dev/null 2>&1); then
  bad "NON-VACUITY broken: the shim did not fail the legacy refspec, so TEST 70 proved nothing"
else
  if (cd "$WORK" && PATH="$legacyfail:$PATH" git ls-remote "$EMPTYREMOTE" 'refs/lane-claims/*' >/dev/null 2>&1); then
    ok "NON-VACUITY: the shim fails the LEGACY refspec while the per-lane refspec still lists — so the per-lane half really did succeed-empty"
  else
    bad "NON-VACUITY broken: the shim broke the per-lane listing too, so the run failed earlier for another reason"
  fi
fi
ctl_out=$(cd "$WORK" && HEARTBEAT_MACHINE=noSuchLaneBox \
  HEARTBEAT_REMOTE="$EMPTYREMOTE" CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$ctl_out" | grep -q 'That is NOT the same as an idle fleet'; then
  ok "NON-VACUITY: without the shim the same empty state DOES print the empty-namespace sentence, so the INCOMPLETE branch was reached by the injected failure"
else
  bad "NON-VACUITY broken: the control run did not reach the empty-namespace sentence, so TEST 70's two branches are not distinguished: out:
$ctl_out"
fi

# ===========================================================================
echo "TEST 71: should-reap maps an OPERATIONAL ls-remote failure to KEEP, not 'no ref' (round 18)"
# ===========================================================================
# FAIL-OPEN instance 7. `! git ls-remote --exit-code` collapsed every failure onto "no claim ref …
# return 2", so an auth failure or a network blip reported CONFIRMED ABSENCE for a ref nobody could
# look at. Only git's status 2 is no-match; 128 is operational. `delete_ref_guarded` already cased
# this correctly, so this is the same guard-width shape as round 17.
sr_out=$(cd "$WORK" && HEARTBEAT_REMOTE=no-such-remote CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap someBox 4242 1 2>&1)
sr_rc=$?
if [ "$sr_rc" -eq 1 ] \
  && printf '%s\n' "$sr_out" | grep -qi 'operational failure' \
  && printf '%s\n' "$sr_out" | grep -qi 'never confirmed absent'; then
  ok "an unreachable remote makes should-reap KEEP (rc=1) and name the operational failure — never rc=2 'no ref'"
else
  bad "an unverified claim must not read as confirmed absent: rc=$sr_rc out:
$sr_out"
fi
# NON-VACUITY: on a GOOD remote a genuinely absent ref still returns 2, so the case above is about
# the failure status and not about the ref being missing.
(cd "$WORK" && CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" bash "$HB" should-reap someBox 4242 1 >/dev/null 2>&1)
if [ $? -eq 2 ]; then
  ok "NON-VACUITY: a genuinely absent ref on a reachable remote still returns 2 (confirmed absence)"
else
  bad "NON-VACUITY broken: absence on a good remote no longer returns 2, so TEST 71 cannot tell the two apart"
fi

# ===========================================================================
echo "TEST 72: should-reap trusts the ref PATH, and KEEPS an unreadable legacy issue (round 18)"
# ===========================================================================
# FAIL-OPEN instance 8, two defects in three lines: `local issue` reset the caller's lane id and the
# message parse overwrote the AUTHORITATIVE path value, while `|| issue=""` turned an unreadable
# message into "no issue" — and the open-PR check is `[ -n "$issue" ] && …`, so the #2499
# orphaned-endgame safeguard was SKIPPED and the verdict was REAP.
# (a) A per-lane claim whose MESSAGE names a different issue. The path issue has an open PR; the
#     message's does not. Whichever one the code consults decides the verdict, so this discriminates.
craft_claim_msg_issue() {  # <machine> <lane-id> <message-issue> <pid> <age>
  local machine="$1" lane="$2" msgissue="$3" pid="$4" age="$5"
  (
    cd "$WORK" || exit 1
    local now_epoch old_epoch old_ts empty_tree csha
    now_epoch=$(date -u +%s); old_epoch=$((now_epoch - age))
    old_ts=$(date -u -r "$old_epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
      || old_ts=$(date -u -d "@$old_epoch" +%Y-%m-%dT%H:%M:%SZ)
    empty_tree=$(git hash-object -t tree --stdin </dev/null)
    csha=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
      GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$empty_tree" -m "claim issue=${msgissue} machine=${machine} pid=${pid} ts=${old_ts}")
    g push -q origin "${csha}:refs/lane-claims/${machine}/${lane}"
  )
}
craft_claim_msg_issue pathBox 5555 9999 "$ABSENT_PID" 30
# A SNIPPET, NOT AN EXECUTABLE. `issue_has_open_pr` runs `bash -c "$CLAIM_OPEN_PR_CMD" _ "$issue"`,
# so the value is shell source in which $1 is the issue — a script PATH here would be invoked with
# NO arguments (the `_ <issue>` become the -c string's own $0/$1), see it as empty, and answer "no
# open PR" for every issue, which is precisely how the first cut of TEST 72a failed. The existing
# NO_OPEN_PR='exit 1' / HAS_OPEN_PR='exit 0' stubs are the shape to copy.
only5555='[ "$1" = 5555 ] && exit 0; exit 1'   # "has an open PR" for 5555 ONLY
pa_out=$(cd "$WORK" && HEARTBEAT_MACHINE=someOtherMachine CLAIM_OPEN_PR_CMD="$only5555" \
  bash "$HB" should-reap pathBox 5555 1 2>&1)
pa_rc=$?
if [ "$pa_rc" -eq 1 ] && printf '%s\n' "$pa_out" | grep -q 'issue #5555 has an open PR'; then
  ok "should-reap consults the issue from the REF PATH (5555), not the message's (9999) — so the open-PR safeguard runs"
else
  bad "the ref path must outrank the claim message: rc=$pa_rc out:
$pa_out"
fi
# NON-VACUITY: the stub really does answer differently for the two issues, so the assertion above
# distinguishes path from message rather than passing whichever was used.
if bash -c "$only5555" _ 5555 && ! bash -c "$only5555" _ 9999; then
  ok "NON-VACUITY: the open-PR stub answers YES for 5555 and NO for 9999, so TEST 72a discriminates"
else
  bad "NON-VACUITY broken: the stub does not distinguish 5555 from 9999"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/pathBox/5555" 2>/dev/null || true)

# (b) A LEGACY claim whose message carries NO issue= at all. The message is the only source there,
#     so failing to read it means the safeguard cannot run: KEEP, never reap. Fail-CLOSED without
#     being fail-SHUT (#3464 family 4) — the caller still reaches a documented decision.
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  old_ts=$(date -u -r $(( $(date -u +%s) - 40000 )) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
    || old_ts=$(date -u -d "@$(( $(date -u +%s) - 40000 ))" +%Y-%m-%dT%H:%M:%SZ)
  cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
    GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim machine=legacyNoIssue pid=$ABSENT_PID ts=${old_ts}")
  g push -q origin "${cs}:refs/machine-claims/legacyNoIssue"
)
ln_out=$(cd "$WORK" && HEARTBEAT_MACHINE=legacyNoIssue CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap legacyNoIssue 1 2>&1)
ln_rc=$?
if [ "$ln_rc" -eq 1 ] && printf '%s\n' "$ln_out" | grep -qi 'issue is unknown'; then
  ok "a legacy claim whose issue cannot be read is KEPT (rc=1), so the open-PR safeguard is never skipped"
else
  bad "an unreadable legacy issue must not reach a reap verdict: rc=$ln_rc out:
$ln_out"
fi
# NON-VACUITY: the SAME legacy shape WITH a readable issue and no open PR still reaches REAP, so the
# KEEP above is caused by the unreadable field and not by the age/threshold or the legacy path.
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  old_ts=$(date -u -r $(( $(date -u +%s) - 40000 )) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
    || old_ts=$(date -u -d "@$(( $(date -u +%s) - 40000 ))" +%Y-%m-%dT%H:%M:%SZ)
  cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
    GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim issue=6161 machine=legacyOk pid=$ABSENT_PID ts=${old_ts}")
  g push -q origin "${cs}:refs/machine-claims/legacyOk"
)
(cd "$WORK" && HEARTBEAT_MACHINE=legacyOk CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap legacyOk 1 >/dev/null 2>&1)
if [ $? -eq 0 ]; then
  ok "NON-VACUITY: the same legacy shape with a READABLE issue and no open PR still reaches REAP (rc=0)"
else
  bad "NON-VACUITY broken: the readable-issue control did not reach a reap verdict, so TEST 72b's KEEP is not attributable to the unreadable field"
fi
(cd "$WORK" && g push -q origin ":refs/machine-claims/legacyNoIssue" ":refs/machine-claims/legacyOk" 2>/dev/null || true)

# ===========================================================================
echo "TEST 73: a failed LEASED push is only a transfer when the ref actually MOVED (round 20)"
# ===========================================================================
# FAIL-OPEN in a new spelling. A failed `--force-with-lease` push returned 4 ("lease not held") for
# auth, network and server failures alike — and worker-supervisor reads 4 as ownership TRANSFERRED and
# permanently DROPS the cleanup entry. Automated reaping refuses a placeholder lane, so a transient
# failure leaked a stale ref forever and dead-lanes then reported a lane that does not exist. The
# previous round's lease fix created that leak. Driven by a git shim that fails the leased push while
# leaving ls-remote working, so the three outcomes can be told apart.
pushfail="$T/pushfail"
mkdir -p "$pushfail"
cat >"$pushfail/git" <<'PFEOF'
#!/usr/bin/env bash
# Scan ALL args (git takes global options before the subcommand), and fail ONLY a leased delete.
_is_push=false; _leased=false
for a in "$@"; do
  [ "$a" = push ] && _is_push=true
  case "$a" in --force-with-lease=*) _leased=true ;; esac
done
if [ "$_is_push" = true ] && [ "$_leased" = true ]; then
  echo "fatal: injected push failure (simulating auth/network)" >&2
  exit 1
fi
exec /usr/bin/git "$@"
PFEOF
chmod +x "$pushfail/git"

# (a) ref UNCHANGED at the lease => OPERATIONAL failure (1), never 4, so the caller keeps retrying.
craft_lane_claim "$WORK" "leaseBox" "6001" "$ABSENT_PID" 30
lease_sha=$(g -C "$WORK" ls-remote origin 'refs/lane-claims/leaseBox/6001' | awk '{print $1}')
lp_out=$(cd "$WORK" && PATH="$pushfail:$PATH" CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" reap leaseBox 6001 "$lease_sha" 2>&1)
lp_rc=$?
if [ "$lp_rc" -eq 1 ] && printf '%s\n' "$lp_out" | grep -qi 'operational failure'; then
  ok "a failed leased push with the ref STILL at the lease is an operational failure (rc=1), not a transfer"
else
  bad "an operational push failure must not report a transfer: rc=$lp_rc out:
$lp_out"
fi

# (b) ref MOVED away from the lease => genuine transfer (4).
mv_out=$(cd "$WORK" && PATH="$pushfail:$PATH" CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" reap leaseBox 6001 0000000000000000000000000000000000000001 2>&1)
mv_rc=$?
if [ "$mv_rc" -eq 4 ] && printf '%s\n' "$mv_out" | grep -qi 'was not held'; then
  ok "a failed leased push whose ref is at a DIFFERENT sha is a genuine transfer (rc=4)"
else
  bad "a real lease mismatch must still report 4: rc=$mv_rc out:
$mv_out"
fi
# NON-VACUITY: the shim really does fail the leased push, and the ref really does still exist — so (a)
# and (b) differ only in the lease value, which is the thing under test.
still=$(g -C "$WORK" ls-remote origin 'refs/lane-claims/leaseBox/6001' | awk '{print $1}')
if [ -n "$still" ] && [ "$still" = "$lease_sha" ]; then
  ok "NON-VACUITY: the ref survived both attempts at its original sha, so neither push succeeded and the two verdicts came from the lease comparison"
else
  bad "NON-VACUITY broken: ref is now '$still' (was '$lease_sha') — a push got through, so TEST 73 is not comparing leases"
fi

# (c) ref ABSENT => nothing left to delete, report success rather than inventing a transfer.
(cd "$WORK" && g push -q origin ":refs/lane-claims/leaseBox/6001" 2>/dev/null || true)
ab_out=$(cd "$WORK" && PATH="$pushfail:$PATH" CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" reap leaseBox 6001 "$lease_sha" 2>&1)
ab_rc=$?
if [ "$ab_rc" -eq 0 ] && printf '%s\n' "$ab_out" | grep -qi 'already absent'; then
  ok "a failed leased push against an ALREADY-ABSENT ref reports success — nothing remained to delete"
else
  bad "an absent ref after a failed push must not be an error or a transfer: rc=$ab_rc out:
$ab_out"
fi

# ===========================================================================
echo "TEST 74: list/list-claims REPORT an unreadable row in the exit status (round 20)"
# ===========================================================================
# The PROPERTY behind census instance 6. Guarding the git version at dispatch closed the CAUSE that
# instance was found through (git < 2.29) and left the property open: a row fetch failing for network,
# auth or a ref deleted mid-run still printed `fetch-failed` and the command still exited 0 — a listing
# that measured nothing, reporting success. Driven by a shim that fails only per-row FETCHES.
rowfail="$T/rowfail"
mkdir -p "$rowfail"
cat >"$rowfail/git" <<'RFEOF'
#!/usr/bin/env bash
_is_fetch=false
for a in "$@"; do [ "$a" = fetch ] && _is_fetch=true; done
if [ "$_is_fetch" = true ]; then
  echo "fatal: injected row-fetch failure" >&2
  exit 1
fi
exec /usr/bin/git "$@"
RFEOF
chmod +x "$rowfail/git"
craft_lane_claim "$WORK" "rowBox" "6002" "$ABSENT_PID" 30
(cd "$WORK" && bash "$HB" beat 6002 >/dev/null 2>&1) || true
for sub in list list-claims; do
  rf_out=$(cd "$WORK" && PATH="$rowfail:$PATH" bash "$HB" "$sub" 2>&1)
  rf_rc=$?
  if [ "$rf_rc" -ne 0 ] \
    && printf '%s\n' "$rf_out" | grep -q 'fetch-failed' \
    && printf '%s\n' "$rf_out" | grep -qi 'INCOMPLETE'; then
    ok "'$sub' exits non-zero (rc=$rf_rc) when a row could not be read, while still rendering the rows that did"
  else
    bad "'$sub' must not exit 0 having measured nothing: rc=$rf_rc out:
$rf_out"
  fi
done
# NON-VACUITY: WITHOUT the shim the same commands exit 0 on the same refs, so the non-zero above is
# caused by the injected row failure and not by the listing being empty or the refs being malformed.
for sub in list list-claims; do
  (cd "$WORK" && bash "$HB" "$sub" >/dev/null 2>&1)
  if [ $? -eq 0 ]; then
    ok "NON-VACUITY: '$sub' exits 0 on the same refs with a working git"
  else
    bad "NON-VACUITY broken: '$sub' already failed without the shim, so TEST 74 proves nothing"
  fi
done
(cd "$WORK" && g push -q origin ":refs/lane-claims/rowBox/6002" ":refs/heartbeats/rowBox" 2>/dev/null || true)

# ===========================================================================
echo "TEST 75: a legacy issue that is NOT a number is unusable, not a licence to reap (round 21)"
# ===========================================================================
# `issue_has_open_pr` answers "no open PR" for a non-numeric issue because it cannot query one — a
# correct answer to the WRONG question — so a legacy claim saying `issue=abc` passed the #2499 safeguard
# BY FAILING TO BE CHECKABLE, and both `should-reap` and `reap` acted on it. Asserted at BOTH sites: the
# review named only should-reap, and delete_ref_guarded reads a legacy issue the same way.
craft_legacy_issue() {  # <machine> <issue-literal> <age-secs>
  local machine="$1" lit="$2" age="$3"
  (
    cd "$WORK" || exit 1
    local now_epoch old_epoch old_ts et cs
    now_epoch=$(date -u +%s); old_epoch=$((now_epoch - age))
    old_ts=$(date -u -r "$old_epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
      || old_ts=$(date -u -d "@$old_epoch" +%Y-%m-%dT%H:%M:%SZ)
    et=$(git hash-object -t tree --stdin </dev/null)
    cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
      GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$et" -m "claim issue=${lit} machine=${machine} pid=${ABSENT_PID} ts=${old_ts}")
    g push -q origin "${cs}:refs/machine-claims/${machine}"
  )
}
for bad_issue in abc 0 007 12x; do
  craft_legacy_issue "badIssue" "$bad_issue" 40000
  sr_out=$(cd "$WORK" && HEARTBEAT_MACHINE=badIssue CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
    bash "$HB" should-reap badIssue 1 2>&1)
  sr_rc=$?
  rp_out=$(cd "$WORK" && HEARTBEAT_MACHINE=badIssue CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
    bash "$HB" reap badIssue 2>&1)
  rp_rc=$?
  survived=$(g -C "$WORK" ls-remote origin 'refs/machine-claims/badIssue' | awk '{print $1}')
  if [ "$sr_rc" -eq 1 ] && [ "$rp_rc" -eq 5 ] && [ -n "$survived" ] \
    && printf '%s\n' "$sr_out" | grep -qi 'not an issue number' \
    && printf '%s\n' "$rp_out" | grep -qi 'not an issue number'; then
    ok "issue='${bad_issue}' makes should-reap KEEP (rc=1) and reap REFUSE (rc=5), and the ref survives"
  else
    bad "issue='${bad_issue}' must not reach a reap: sr_rc=$sr_rc rp_rc=$rp_rc survived='${survived:-<gone>}' sr:
$sr_out
rp:
$rp_out"
  fi
  (cd "$WORK" && g push -q origin ":refs/machine-claims/badIssue" 2>/dev/null || true)
done
# NON-VACUITY: the SAME legacy shape with a VALID issue and no open PR still reaches REAP and is
# deleted, so the four refusals above are caused by the issue value and not by the age, the legacy path
# or the open-PR stub.
craft_legacy_issue "okIssue" "4242" 40000
ok_sr_rc=0
(cd "$WORK" && HEARTBEAT_MACHINE=okIssue CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap okIssue 1 >/dev/null 2>&1) || ok_sr_rc=$?
(cd "$WORK" && HEARTBEAT_MACHINE=okIssue CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" reap okIssue >/dev/null 2>&1) || true
ok_gone=$(g -C "$WORK" ls-remote origin 'refs/machine-claims/okIssue' | awk '{print $1}')
if [ "$ok_sr_rc" -eq 0 ] && [ -z "$ok_gone" ]; then
  ok "NON-VACUITY: the same legacy shape with issue=4242 DOES reach reap (rc=0) and the ref is deleted"
else
  bad "NON-VACUITY broken: valid-issue control gave should-reap rc=$ok_sr_rc, ref '${ok_gone:-<gone>}' — TEST 75's refusals are not attributable to the issue value"
fi

# ===========================================================================
echo "TEST 76: a numeric PREFIX is not a parse — a malformed pid is UNKNOWN, never probed (round 25)"
# ===========================================================================
# The row parsers extracted `\([0-9][0-9]*\)`, which matches the numeric PREFIX of a malformed token:
# `pid=123x` yielded `123`, and that pid was then PROBED — so dead-lanes reported ALIVE or DEAD about a
# DIFFERENT PROCESS, and a false ALIVE masks exactly the dead lane the command exists to find.
# Driven with a pid whose numeric prefix is THIS TEST'S OWN LIVE PID, so a prefix parse would answer
# ALIVE with certainty while the real value is malformed and unknowable.
live_prefix="$$"
craft_malformed_pid() {  # <machine> <issue> <pid-literal>
  local machine="$1" issue="$2" lit="$3"
  (
    cd "$WORK" || exit 1
    local now_epoch old_ts et cs
    now_epoch=$(date -u +%s)
    old_ts=$(date -u -r "$((now_epoch - 60))" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
      || old_ts=$(date -u -d "@$((now_epoch - 60))" +%Y-%m-%dT%H:%M:%SZ)
    et=$(git hash-object -t tree --stdin </dev/null)
    cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
      GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$et" -m "claim issue=${issue} machine=${machine} pid=${lit} ts=${old_ts}")
    g push -q origin "${cs}:refs/lane-claims/${machine}/${issue}"
  )
}
craft_malformed_pid "prefixBox" "7100" "${live_prefix}x"
mp_out=$(cd "$WORK" && HEARTBEAT_MACHINE=prefixBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
mp_rc=$?
# NOTE the two renderings differ by command and that is not a defect: `dead-lanes` prints `none` for an
# absent pid, `list-claims` prints `?`. The first cut of this case asserted `?` for both and failed
# against a CORRECT fix — the assertion was wrong about the output, not the code.
if printf '%s\n' "$mp_out" | grep -qE '^prefixBox +7100 +none +UNKNOWN-NO-PID' \
  && ! printf '%s\n' "$mp_out" | grep -qE "^prefixBox +7100 +${live_prefix}"; then
  ok "a malformed pid '${live_prefix}x' reads as UNKNOWN-NO-PID, never coerced to its live numeric prefix"
else
  bad "a numeric prefix must not be parsed as the pid: rc=$mp_rc out:
$(printf '%s\n' "$mp_out" | grep prefixBox)"
fi
# ...and the same token must not reach `list-claims` as a number either.
lc_mp=$(cd "$WORK" && bash "$HB" list-claims 2>&1)
if printf '%s\n' "$lc_mp" | grep -qE '^prefixBox +7100 +\? ' \
  && ! printf '%s\n' "$lc_mp" | grep -qE "^prefixBox +7100 +${live_prefix} "; then
  ok "list-claims shows '?' for the malformed pid rather than its numeric prefix"
else
  bad "list-claims coerced the malformed pid: $(printf '%s\n' "$lc_mp" | grep prefixBox)"
fi
# NON-VACUITY, TRUE OF THE BROKEN CODE TOO: a WELL-FORMED pid equal to that same live pid must be read
# and probed, producing a pid column with the number in it. If this failed, the case above could pass
# merely because nothing is ever parsed.
(cd "$WORK" && g push -q origin ":refs/lane-claims/prefixBox/7100" 2>/dev/null || true)
craft_malformed_pid "prefixBox" "7101" "${live_prefix}"
wf_out=$(cd "$WORK" && HEARTBEAT_MACHINE=prefixBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$wf_out" | grep -qE "^prefixBox +7101 +${live_prefix} "; then
  ok "NON-VACUITY: a WELL-FORMED pid equal to the same value IS read and reported, so the '?' above is a refusal and not a parser that never works"
else
  bad "NON-VACUITY broken: a well-formed pid was not parsed either: $(printf '%s\n' "$wf_out" | grep prefixBox)"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/prefixBox/7101" 2>/dev/null || true)

# ===========================================================================
echo "TEST 77: a SUBSTRING key is not a key — notissue/rapid/nots must not satisfy issue/pid/ts (round 26)"
# ===========================================================================
# Both readers matched `.*${field}=`, with no token boundary before the key, so `notissue=42` satisfied
# `issue`, `rapid=123` satisfied `pid` and `nots=…` satisfied `ts`. A malformed or hand-made claim message
# could therefore SUPPLY a value the fail-closed parsing was meant to refuse — and a wrong pid is PROBED,
# so dead-lanes answers about a different process. Round 25 required the whole VALUE to be well-formed and
# left the KEY a substring match: the same class, one field over.
#
# Exercised through the SHIPPED readers rather than a copy of them, by crafting refs whose messages carry
# only the decoy keys.
craft_decoy_msg() {  # <machine> <lane-id> <message>
  local machine="$1" lane="$2" msg="$3"
  (
    cd "$WORK" || exit 1
    local et cs
    et=$(git hash-object -t tree --stdin </dev/null)
    cs=$(GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$et" -m "$msg")
    g push -q origin "${cs}:refs/lane-claims/${machine}/${lane}"
  )
}
decoy_pid="$$"
# A message with NO real pid/ts keys, only decoys whose names CONTAIN them.
craft_decoy_msg "decoyBox" "7200" "claim notissue=99 machine=decoyBox rapid=${decoy_pid} nots=2026-08-29T00:00:00Z"
dc_out=$(cd "$WORK" && HEARTBEAT_MACHINE=decoyBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$dc_out" | grep -qE '^decoyBox +7200 +none ' \
  && ! printf '%s\n' "$dc_out" | grep -qE "^decoyBox +7200 +${decoy_pid}"; then
  ok "'rapid=${decoy_pid}' does NOT satisfy the pid key — the decoy value is never probed"
else
  bad "a substring key must not be accepted: $(printf '%s\n' "$dc_out" | grep decoyBox)"
fi
lc_dc=$(cd "$WORK" && bash "$HB" list-claims 2>&1)
if printf '%s\n' "$lc_dc" | grep -qE '^decoyBox +7200 +\? +\? ' ; then
  ok "list-claims shows '?' for both pid and ts when only decoy keys are present"
else
  bad "list-claims accepted a decoy key: $(printf '%s\n' "$lc_dc" | grep decoyBox)"
fi
# And the LEGACY reap path: `notissue=` must not supply the issue the open-PR safeguard needs.
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  old_ts=$(date -u -r $(( $(date -u +%s) - 40000 )) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
    || old_ts=$(date -u -d "@$(( $(date -u +%s) - 40000 ))" +%Y-%m-%dT%H:%M:%SZ)
  cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
    GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim notissue=4242 machine=decoyLegacy pid=$ABSENT_PID ts=${old_ts}")
  g push -q origin "${cs}:refs/machine-claims/decoyLegacy"
)
dl_out=$(cd "$WORK" && HEARTBEAT_MACHINE=decoyLegacy CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap decoyLegacy 1 2>&1)
dl_rc=$?
if [ "$dl_rc" -eq 1 ] && printf '%s\n' "$dl_out" | grep -qi 'could not be read\|issue is unknown'; then
  ok "'notissue=4242' does NOT supply the legacy issue — should-reap KEEPS (rc=1) rather than reaping"
else
  bad "a decoy issue key must not reach a reap verdict: rc=$dl_rc out:
$dl_out"
fi
# NON-VACUITY, true of the BROKEN code too: the SAME refs with REAL keys are read normally, so the
# refusals above are refusals and not a parser that never works.
(cd "$WORK" && g push -q origin ":refs/lane-claims/decoyBox/7200" ":refs/machine-claims/decoyLegacy" 2>/dev/null || true)
craft_decoy_msg "decoyBox" "7201" "claim issue=7201 machine=decoyBox pid=${decoy_pid} ts=2026-08-29T00:00:00Z"
rk_out=$(cd "$WORK" && HEARTBEAT_MACHINE=decoyBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
if printf '%s\n' "$rk_out" | grep -qE "^decoyBox +7201 +${decoy_pid}"; then
  ok "NON-VACUITY: the REAL pid key on the same shape IS read and reported, so the decoy refusals are decisions"
else
  bad "NON-VACUITY broken: a real pid key was not read either: $(printf '%s\n' "$rk_out" | grep decoyBox)"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/decoyBox/7201" 2>/dev/null || true)

# ===========================================================================
echo "TEST 78: should-reap must PROVE a local pid gone — EPERM and no-pid are KEEP (round 28)"
# ===========================================================================
# Two ways this reaped a LIVE lane. `kill -0` was used two-valued, so EPERM — which means the process
# EXISTS and is not ours — read as "dead"; and when `pid` was empty the `[ -n "$pid" ]` guard was false,
# so a LOCAL claim fell through to the FOREIGN branch and was reaped with no pid check at all. The
# predicate is age AND no-open-PR AND pid-dead-IF-LOCAL: an unsatisfiable clause must FAIL the
# conjunction, not vanish from it.
#
# (a) LOCAL claim with NO pid => KEEP. Driven by a real claim message that omits `pid=`.
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  old_ts=$(date -u -r $(( $(date -u +%s) - 40000 )) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
    || old_ts=$(date -u -d "@$(( $(date -u +%s) - 40000 ))" +%Y-%m-%dT%H:%M:%SZ)
  cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
    GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim issue=7300 machine=localNoPid ts=${old_ts}")
  g push -q origin "${cs}:refs/lane-claims/localNoPid/7300"
)
np_out=$(cd "$WORK" && HEARTBEAT_MACHINE=localNoPid CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap localNoPid 7300 1 2>&1)
np_rc=$?
if [ "$np_rc" -eq 1 ] && printf '%s\n' "$np_out" | grep -qi 'no usable pid'; then
  ok "a LOCAL claim with no usable pid is KEPT (rc=1) — the unsatisfiable clause fails the predicate"
else
  bad "a local claim with no pid must not be reaped: rc=$np_rc out:
$np_out"
fi
# NON-VACUITY: the SAME ref on a FOREIGN machine still reaches the documented foreign reap, so the KEEP
# above is about locality-plus-no-pid rather than about this ref being unreapable in general.
fr_rc=0
(cd "$WORK" && HEARTBEAT_MACHINE=someOtherBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap localNoPid 7300 1 >/dev/null 2>&1) || fr_rc=$?
if [ "$fr_rc" -eq 0 ]; then
  ok "NON-VACUITY: the same ref judged from a FOREIGN machine still reaps (rc=0), so the KEEP is the local no-pid rule"
else
  bad "NON-VACUITY broken: the foreign path did not reap (rc=$fr_rc), so TEST 78a proves nothing"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/localNoPid/7300" 2>/dev/null || true)
# (b) EPERM => NOT ABSENT. Staged with a pid we genuinely cannot signal rather than with a shim:
# `kill` is a bash BUILTIN, so a PATH shim named `kill` is never consulted — the first cut of this case
# tried exactly that and the probe sailed past it. pid 1 is owned by root, so as an unprivileged user
# `kill -0 1` really returns "Operation not permitted", which is the input under test.
#
# The property is that EPERM is NEVER read as absent. `signal_probe_class 1` = `denied` and
# `process_presence 1` = `present` here (the visibility voters can see init), and BOTH of those are KEEP.
# The old two-valued `kill -0` read the same EPERM as "dead" and REAPED. Skipped rather than faked if the
# host lets us signal pid 1 — a case that cannot stage its input must not pretend to have run.
if kill -0 1 2>/dev/null; then
  skip "EPERM sub-case: this host can signal pid 1, so 'Operation not permitted' cannot be staged (would be a fabricated input)"
else
  craft_lane_claim_pid1() {
    (
      cd "$WORK" || exit 1
      local now_epoch old_ts et cs
      now_epoch=$(date -u +%s)
      old_ts=$(date -u -r "$((now_epoch - 40000))" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
        || old_ts=$(date -u -d "@$((now_epoch - 40000))" +%Y-%m-%dT%H:%M:%SZ)
      et=$(git hash-object -t tree --stdin </dev/null)
      cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
        GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
        git commit-tree "$et" -m "claim issue=7301 machine=epermBox pid=1 ts=${old_ts}")
      g push -q origin "${cs}:refs/lane-claims/epermBox/7301"
    )
  }
  craft_lane_claim_pid1
  ep_out=$(cd "$WORK" && HEARTBEAT_MACHINE=epermBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
    bash "$HB" should-reap epermBox 7301 1 2>&1)
  ep_rc=$?
  if [ "$ep_rc" -eq 1 ] && ! printf '%s\n' "$ep_out" | grep -qi 'CONFIRMED absent'; then
    ok "a pid we cannot signal (EPERM) is KEPT (rc=1) — 'not permitted' means the process EXISTS, never that it is gone"
  else
    bad "EPERM must not be read as dead: rc=$ep_rc out:
$ep_out"
  fi
  # NON-VACUITY: the SAME shape with a deterministically ABSENT pid DOES reap, so the KEEP above is the
  # presence decode and not the age, the open-PR stub, or the ref being unreapable.
  (cd "$WORK" && g push -q origin ":refs/lane-claims/epermBox/7301" 2>/dev/null || true)
  craft_lane_claim "$WORK" "epermBox" "7302" "$ABSENT_PID" 40000
  ns_rc=0
  (cd "$WORK" && HEARTBEAT_MACHINE=epermBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
    bash "$HB" should-reap epermBox 7302 1 >/dev/null 2>&1) || ns_rc=$?
  if [ "$ns_rc" -eq 0 ]; then
    ok "NON-VACUITY: the same shape with a deterministically ABSENT pid reaps (rc=0), so the KEEP is the presence decode"
  else
    bad "NON-VACUITY broken: the absent-pid control did not reap (rc=$ns_rc)"
  fi
  (cd "$WORK" && g push -q origin ":refs/lane-claims/epermBox/7302" 2>/dev/null || true)
fi

echo "TEST 79: a NONEXISTENT placeholder must answer 2 (no ref), not 1 (keep) (round 33)"
# ===========================================================================
# The placeholder no-auto-reap rule ran BEFORE the `ls-remote --exit-code` existence check, so
# `should-reap <machine> pXXXX` returned 1 ("keep") for a lane that has no ref at all. The two answers
# mean different things to a caller — 1 says "a claim exists and is alive", 2 says "there is nothing
# here" — and a placeholder is the DEFAULT lane id, so this is the common path, not a corner. Ordering
# is the whole fix: the rule is correct, it was just consulted about a subject that did not exist.
missing_rc=0
missing_out=$(cd "$WORK" && HEARTBEAT_MACHINE=phBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap phBox p999999 1 2>&1) || missing_rc=$?
if [ "$missing_rc" -eq 2 ]; then
  ok "a nonexistent placeholder lane answers rc=2 (no ref) — the documented confirmed-absence status"
else
  bad "nonexistent placeholder gave rc=$missing_rc (expected 2) out:
$missing_out"
fi
# NON-VACUITY, and it must be true of the BROKEN code too: an EXISTING placeholder still KEEPS (rc=1)
# under the placeholder rule. If this said 2 as well, the case above would be passing because
# placeholders are unreachable rather than because the ordering was fixed.
(
  cd "$WORK" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  old_ts=$(date -u -r $(( $(date -u +%s) - 40000 )) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
    || old_ts=$(date -u -d "@$(( $(date -u +%s) - 40000 ))" +%Y-%m-%dT%H:%M:%SZ)
  cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
    GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim machine=phBox ts=${old_ts}")
  g push -q origin "${cs}:refs/lane-claims/phBox/p999998"
)
present_rc=0
present_out=$(cd "$WORK" && HEARTBEAT_MACHINE=phBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap phBox p999998 1 2>&1) || present_rc=$?
if [ "$present_rc" -eq 1 ]; then
  ok "NON-VACUITY: an EXISTING placeholder still KEEPS (rc=1), so the rc=2 above is the ordering fix"
else
  bad "NON-VACUITY broken: an existing placeholder gave rc=$present_rc (expected 1) out:
$present_out"
fi
# And a nonexistent NUMERIC lane must answer 2 as well — the two lane kinds agree about absence, which
# is what makes 2 mean "no ref" rather than "no ref, unless the id looks like a placeholder".
num_rc=0
(cd "$WORK" && HEARTBEAT_MACHINE=phBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" should-reap phBox 999997 1 >/dev/null 2>&1) || num_rc=$?
if [ "$num_rc" -eq 2 ]; then
  ok "a nonexistent NUMERIC lane also answers rc=2 — absence is reported the same way for both kinds"
else
  bad "nonexistent numeric lane gave rc=$num_rc (expected 2)"
fi
(cd "$WORK" && g push -q origin ":refs/lane-claims/phBox/p999998" 2>/dev/null || true)

echo "TEST 80: a ZOMBIE local pid is not alive — should-reap must reap it (round 36)"
# ===========================================================================
# `process_presence` answers a VISIBILITY question, and a zombie is visible: `ps -p` lists it,
# `/proc/<pid>` exists and `kill -0` succeeds. So a zombie supervisor read as `present` and its claim
# was KEPT INDEFINITELY — while `dead-lanes`, in this same file, has classified that case as
# DEAD-NO-PROCESS since round 7. Two predicates in one file disagreeing about one fact.
#
# Staged with a REAL zombie: a child that exits while its parent never reaps it. If the host will not
# produce one within the wait cap the case SKIPs rather than faking the premise.
# A GENUINE zombie needs a parent that outlives the child and NEVER reaps it. A shell cannot be
# relied on for that — bash reaps background children opportunistically, and `exec`ing the parent
# orphans the child to init, which reaps it immediately. So the parent is a python process that
# forks, lets the child _exit, and then sleeps WITHOUT waitpid(). The first cut used two competing
# shell tricks, produced no zombie, and the case skipped — which is how the missing `skip` helper
# was found.
zombie_pid=""
python3 -c '
import os, sys, time
pid = os.fork()
if pid == 0:
    os._exit(0)
sys.stdout.write(str(pid) + "\n")
sys.stdout.flush()
time.sleep(25)
' >"$WORK/zombie.pid" 2>/dev/null &
zparent=$!
sleep 1
zombie_pid="$(head -1 "$WORK/zombie.pid" 2>/dev/null | tr -d ' ')"
zstate="$(ps -o stat= -p "${zombie_pid:-0}" 2>/dev/null | tr -d ' ')"
if [ -z "$zombie_pid" ] || [ -z "$zstate" ] || [ "${zstate#Z}" = "$zstate" ]; then
  skip "TEST 80: the host did not yield an observable zombie (state='${zstate:-<none>}') — premise unstageable, not faked"
else
  (
    cd "$WORK" || exit 1
    et=$(git hash-object -t tree --stdin </dev/null)
    old_ts=$(date -u -r $(( $(date -u +%s) - 40000 )) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
      || old_ts=$(date -u -d "@$(( $(date -u +%s) - 40000 ))" +%Y-%m-%dT%H:%M:%SZ)
    cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
      GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$et" -m "claim issue=7401 machine=zBox pid=${zombie_pid} ts=${old_ts}")
    g push -q origin "${cs}:refs/lane-claims/zBox/7401"
  )
  z_rc=0
  z_out=$(cd "$WORK" && HEARTBEAT_MACHINE=zBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
    bash "$HB" should-reap zBox 7401 1 2>&1) || z_rc=$?
  if [ "$z_rc" -eq 0 ] && printf '%s\n' "$z_out" | grep -qi 'zombie'; then
    ok "a ZOMBIE local pid is REAPED (rc=0) and the verdict names the zombie state"
  else
    bad "a zombie local pid must be reaped: rc=$z_rc out:
$z_out"
  fi
  # NON-VACUITY, and it must be true of the BROKEN code too: a LIVE local pid in the same shape is
  # still KEPT. Without this, a should-reap that reaped every local claim would satisfy the case above.
  ( sleep 25 ) & live_pid=$!
  (
    cd "$WORK" || exit 1
    et=$(git hash-object -t tree --stdin </dev/null)
    old_ts=$(date -u -r $(( $(date -u +%s) - 40000 )) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null) \
      || old_ts=$(date -u -d "@$(( $(date -u +%s) - 40000 ))" +%Y-%m-%dT%H:%M:%SZ)
    cs=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
      GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$et" -m "claim issue=7402 machine=zBox pid=${live_pid} ts=${old_ts}")
    g push -q origin "${cs}:refs/lane-claims/zBox/7402"
  )
  l_rc=0
  l_out=$(cd "$WORK" && HEARTBEAT_MACHINE=zBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
    bash "$HB" should-reap zBox 7402 1 2>&1) || l_rc=$?
  if [ "$l_rc" -eq 1 ] && printf '%s\n' "$l_out" | grep -qi 'still alive'; then
    ok "NON-VACUITY: a LIVE local pid is still KEPT (rc=1) — so the zombie reap is about the STATE, not about reaping every local claim"
  else
    bad "NON-VACUITY broken: a live local pid gave rc=$l_rc out:
$l_out"
  fi
  kill "$live_pid" 2>/dev/null || true
  (cd "$WORK" && g push -q origin ":refs/lane-claims/zBox/7401" ":refs/lane-claims/zBox/7402" 2>/dev/null || true)
fi
kill "${zparent:-0}" 2>/dev/null || true


# ===========================================================================
echo "TEST 81: --help carries the COMPLETE #3548 scope statement, phrase by phrase"
# ===========================================================================
# The owner ruling of 2026-09-01 (option C) is a DOCUMENTATION deliverable, so it is only worth
# something if it is still there next month. A wording pass that deletes the scope statement leaves
# a command whose subject set is empty on this fleet and no text saying so — the exact "reads as
# working while answering about the empty set" failure #3548 is about.
#
# EACH PHRASE IS MATCHED WHOLE, AND THAT IS THE FIX FOR THE FIRST DRAFT (roborev job 15, finding 2).
# The first version grepped for tokens — `worker-supervisor.sh`, `SINGLE-SLOT PER MACHINE` — which
# this large help text can satisfy from an UNRELATED occurrence elsewhere: the only-writer
# RELATIONSHIP was not required (the supervisor pid section mentions the script), and either
# namespace-specific REASON could be deleted with the case still green. So the phrases below are
# complete, each BINDS its namespace to its reason, and the failure message NAMES the phrase that
# went missing — a bare red would not say which guarantee was lost.
#
# Matched against a WHITESPACE-FLATTENED copy of the help: it is a comment block, so every phrase
# longer than a line is wrapped, and a line-wise grep for one could never match however correct the
# text is (that is why the older cases in this file assert single-line fragments). Brittle by
# construction and accepted, on TEST 44's precedent: a reflow reds this case, which is a cheap
# correction, whereas a token match cannot tell a reflow from a deletion.
help81=$(cd "$WORK" && bash "$HB" --help 2>&1 || true)
# here-strings, not `printf | grep` (roborev job 15, finding 1): under this suite's `set -o
# pipefail` a `grep -q` that exits at the first match can leave the writer with SIGPIPE, so the
# PIPELINE status goes non-zero and the assertion reads FALSE on correct input — a false FAIL.
help81_flat=$(tr '\n' ' ' <<<"$help81" | tr -s ' ')
require_help_phrase() {  # <the guarantee this phrase carries> <the COMPLETE phrase, matched literally>
  if grep -Fqi -- "$2" <<<"$help81_flat"; then
    ok "--help carries the $1 statement"
  else
    bad "--help is MISSING the $1 statement (#3548) — this exact phrase is gone: \"$2\""
  fi
}
require_help_phrase "supervisor-fleets-only scope" \
  'lane-granular dead-lane detection APPLIES TO SUPERVISOR FLEETS ONLY'
require_help_phrase "descope attribution (owner ruling + issue numbers)" \
  'DESCOPED (owner ruling 2026-09-01 on #3548, option C; completes #3393)'
# The RELATIONSHIP, not the mention: this is WHY the subject set is empty here, and without it the
# scope reads as an arbitrary restriction someone may "relax".
require_help_phrase "only-writer relationship" \
  'the ONLY writer of either in this tree is `scripts/local/worker-supervisor.sh`'
require_help_phrase "empty-subject-set consequence" \
  'On a supervisor-less `/drive-issue` fleet the subject set is EMPTY'
# Not a duplicate of TEST 52's behavioural check: that pins that the CODE never exits 0, this pins
# that the CONTRACT still tells an operator not to read the 1 as clean — and the descope is what
# makes a 1 the NORMAL outcome here, so deleting the sentence is more dangerous than it was.
require_help_phrase "exit-1-is-not-a-clean-bill-of-health rule (#3467)" \
  'EXIT 1 MEANS "NOTHING WAS REPORTED", NEVER a clean bill of health'
# The two MEASURED refusals, each binding its namespace to its own reason. A later reader's first
# instinct is "just point it at the populated namespace", so the evidence has to survive here.
require_help_phrase "refs/claims/issue-<N> refusal (transient claiming-shell pid)" \
  '`refs/claims/issue-<N>` — the per-issue lock, populated on every box. It records the pid of the TRANSIENT CLAIMING SHELL and never refreshes it'
require_help_phrase "refs/heartbeats/<machine> refusal (single-slot per machine)" \
  '`refs/heartbeats/<machine>` — populated on every box, but SINGLE-SLOT PER MACHINE and force-updated by `beat`, so N lanes on one box overwrite each other'
require_help_phrase "AC4 abstain rule" \
  'a STALE PID MUST NEVER YIELD A `DEAD-*` VERDICT — abstain with an `UNKNOWN-*` verdict instead'

# ===========================================================================
echo "TEST 82: NAMESPACE CONTAINMENT — a dead pid in refs/claims/issue-<N> yields NO verdict (#3548 AC4)"
# ===========================================================================
# THE PROPERTY THAT STOPS THE OBVIOUS "FIX". `refs/claims/issue-<N>` is populated on every
# /drive-issue box and carries a pid — but it is the TRANSIENT CLAIMING SHELL's, never
# refreshed, and MEASURED DEAD while its lane was running (pid 3775744 on #3548). Reading it
# would make this command report DEAD-NO-PROCESS for HEALTHY lanes. `refs/heartbeats/<machine>`
# is single-slot per machine, so it cannot carry a per-lane verdict either. Both are therefore
# out of the subject set, and that containment is pinned BEHAVIOURALLY rather than in prose:
# staged exactly as the real fleet has them, a later read-side change that widened the listing
# would turn a measured false positive into a verdict and fail here. RED-VERIFIED rather than
# reasoned: with the listing widened to `refs/*claims/*` in a scratch copy, this fixture produces
# a row naming issue 8801, so the assertion below reds — it is not passing because the fixture is
# undetectable.
#
# Its own remote, so no lane-claims ref from an earlier case can supply a subject and make the
# assertion pass for the wrong reason (the TEST 27 fixture idiom).
ns_origin="$T/origin-ns.git"
ns_work="$T/work-ns"
g init --bare -q "$ns_origin"
g clone -q "$ns_origin" "$ns_work" 2>/dev/null
(
  cd "$ns_work" || exit 1
  echo seed >seed.txt; g add seed.txt; g commit -qm seed; g push -q -u origin main
)
# The per-issue LOCK, in claim.sh's own shape, carrying a verified-absent pid...
(
  cd "$ns_work" || exit 1
  et=$(git hash-object -t tree --stdin </dev/null)
  now_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  cs=$(GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
    git commit-tree "$et" -m "claim issue=8801 machine=nsBox pid=${ABSENT_PID} actor=flow ts=${now_ts}")
  g push -q origin "${cs}:refs/claims/issue-8801"
)
# ...plus a machine heartbeat, the other populated namespace.
(cd "$ns_work" && HEARTBEAT_MACHINE=nsBox bash "$HB" beat 8801 >/dev/null 2>&1)
ns_out=$(cd "$ns_work" && HEARTBEAT_MACHINE=nsBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
ns_rc=$?
# NON-VACUITY FIRST: the fixture really is present and really would be judged dead if it were
# in the subject set. Without this, an empty or unpushed fixture passes the containment check.
ns_lock=$(g -C "$ns_work" ls-remote origin 'refs/claims/issue-8801' | awk '{print $1}')
ns_beat=$(g -C "$ns_work" ls-remote origin 'refs/heartbeats/nsBox' | awk '{print $1}')
if [ -n "$ns_lock" ] && [ -n "$ns_beat" ]; then
  ok "NON-VACUITY: both populated namespaces are staged (refs/claims/issue-8801, refs/heartbeats/nsBox) with pid $ABSENT_PID"
else
  bad "NON-VACUITY broken: fixture refs missing (lock='$ns_lock' beat='$ns_beat')"
fi
# here-strings throughout (roborev job 15, finding 1 — see TEST 81).
if [ "$ns_rc" -eq 1 ] \
  && ! grep -q 'DEAD-' <<<"$ns_out" \
  && ! grep -q '8801' <<<"$ns_out"; then
  ok "a dead pid in refs/claims/issue-<N> (and a heartbeat beside it) produces NO DEAD-* verdict and no row at all (rc=$ns_rc)"
else
  bad "the populated namespaces must be OUT of the subject set — no DEAD-* and no row: rc=$ns_rc out:
$ns_out"
fi
# ...and the run must say it measured NOTHING, so an operator is not left reading silence as
# health. This is the descope's operator-facing half: on a supervisor-less fleet THIS is the
# normal output, and it has to be self-describing.
if grep -qi 'not the same as an idle fleet' <<<"$ns_out" \
  && grep -qi "NOT 'no dead lanes'" <<<"$ns_out"; then
  ok "the empty-subject-set run says nothing was measured and that this is NOT 'no dead lanes'"
else
  bad "an empty subject set must report that nothing was measured: out:
$ns_out"
fi
# NON-VACUITY, the other direction: the SAME pid in the SAME shape, in the namespace this
# command DOES read, is reported DEAD. So TEST 82 pins the NAMESPACE boundary, not a fixture
# that simply fails to be detectable.
craft_lane_claim "$ns_work" "nsBox" 8802 "$ABSENT_PID" 30
nsd_out=$(cd "$ns_work" && HEARTBEAT_MACHINE=nsBox CLAIM_OPEN_PR_CMD="$NO_OPEN_PR" \
  bash "$HB" dead-lanes 2>&1)
nsd_rc=$?
if [ "$nsd_rc" -eq 3 ] && grep -Eq '^nsBox +8802 .*DEAD-NO-PROCESS' <<<"$nsd_out"; then
  ok "NON-VACUITY: the same absent pid in refs/lane-claims IS reported DEAD-NO-PROCESS (rc=3) — the boundary is the namespace"
else
  bad "NON-VACUITY broken: the same pid in the subject namespace must be DEAD: rc=$nsd_rc out:
$nsd_out"
fi

echo
echo "=== claim-heartbeat.sh: $PASS passed, $FAIL failed, $SKIP skipped ==="
[ "$FAIL" -eq 0 ]
