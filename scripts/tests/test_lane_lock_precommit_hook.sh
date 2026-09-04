#!/usr/bin/env bash
# test_lane_lock_precommit_hook.sh — THE END-TO-END CONTROL for #3436's wiring.
#
# roborev found (twice, jobs 434 and 436, at two different heads) that the lane lock had no
# production acquire path. scripts/git-hooks/pre-commit is that path. This suite is the
# "one end-to-end control proving a second entrant is refused" the ruling asked for.
#
# It exercises the REAL hook against a REAL git repository laid out as a lane, with a REAL live
# process holding the lock — not a simulation of any of the three. The cases are the ones that
# decide whether the feature works at all:
#   1. a lane held by a LIVE PEER refuses the commit and names the occupant   <- the incident
#   2. CONTROL: a lane held by THIS session commits normally                  <- not a brick
#   3. CONTROL: a FREE lane is acquired by the hook and the commit proceeds   <- entry wiring
#   4. CONTROL: outside a lane the hook no-ops                                <- root checkout
# Case 1 alone would pass if the hook refused EVERYTHING, which would be worse than no hook, so
# 2-4 are what give it meaning.
set -uo pipefail
PASS=0; FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS+1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL+1)); }
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
HOOK="$REPO/scripts/git-hooks/pre-commit"
SLEEPERS=""
cleanup() { for p in $SLEEPERS; do kill "$p" 2>/dev/null || true; done; [ -n "${T:-}" ] && rm -rf "$T"; }
trap cleanup EXIT
sleeper() {
  local pidfile="$T/sleeper.pid"; rm -f "$pidfile"
  ( sleep 300 >/dev/null 2>&1 & printf '%s\n' "$!" >"$pidfile" ) >/dev/null 2>&1
  REPLY_SLEEPER="$(cat "$pidfile" 2>/dev/null)"; SLEEPERS="$SLEEPERS $REPLY_SLEEPER"
}

# THE LANE PATH IS PART OF THE CONTRACT. The hook keys on /data/lanes/lane-<N>, so a fixture in
# mktemp would silently take the no-op branch and every case would "pass" having tested nothing.
# Use a real lane-shaped path under /data/lanes with an issue number no lane uses.
LANEROOT=/data/lanes
T="$LANEROOT/.t-hook-$$"
mkdir -p "$T" || { echo "cannot create $T"; exit 1; }
LANE="$LANEROOT/lane-99$$"; LANE="${LANE:0:24}"
rm -rf "$LANE"; mkdir -p "$LANE"
ISSUE="${LANE##*/lane-}"
cleanup() { for p in $SLEEPERS; do kill "$p" 2>/dev/null || true; done; rm -rf "$T" "$LANE"; rm -f "$LANEROOT/.lane-locks/lane-$ISSUE".*; }
trap cleanup EXIT

# A git repo AT the lane path, carrying the scripts the hook needs.
( cd "$LANE" && git init -q . && git config user.email t@t && git config user.name t )
mkdir -p "$LANE/scripts/flow"
cp "$REPO/scripts/flow/lane-lock.sh" "$LANE/scripts/flow/"
[ -r "$REPO/scripts/flow/lib/liveness.sh" ] && { mkdir -p "$LANE/scripts/flow/lib"; cp "$REPO/scripts/flow/lib/"*.sh "$LANE/scripts/flow/lib/" 2>/dev/null; }
mkdir -p "$LANE/.git/hooks"; cp "$HOOK" "$LANE/.git/hooks/pre-commit"; chmod +x "$LANE/.git/hooks/pre-commit"

# THE TEST SHELL ITSELF ENTERS THE LANE, and this is not cosmetic. The hook resolves the
# OUTERMOST ANCESTOR whose cwd is inside the lane. With `( cd "$LANE" && git commit )` that
# ancestor is the SUBSHELL, which exits the moment the commit ends — so the record it acquires
# reads DEAD-NO-PROCESS on the very next case. That is FIX 14's transient-owner failure
# reproduced inside the fixture, and it made TEST 3 look like a hook bug when it was a fixture
# bug. A real lane always has a durable in-lane shell (the session); the fixture must too, so
# the test script cds ONCE and stays there, and $$ is that durable ancestor.
cd "$LANE" || { echo "cannot enter $LANE"; exit 1; }
COMMIT_N=0
commit_try() {
  COMMIT_N=$((COMMIT_N+1))
  # Unique content per call: `date` alone repeats within a second, and git then exits non-zero
  # with "nothing to commit", which is indistinguishable from a hook refusal by exit code.
  printf 'c%s-%s\n' "$COMMIT_N" "$RANDOM" > f.txt
  git add f.txt && git commit -q -m "$1" 2>&1
}

# ---------------------------------------------------------------------------
echo "TEST 1: a lane held by a LIVE PEER REFUSES the commit"
# ---------------------------------------------------------------------------
sleeper; PEER="$REPLY_SLEEPER"
LANE_LOCK_PID=$PEER bash "$REPO/scripts/flow/lane-lock.sh" acquire "$ISSUE" --lane-dir "$LANE" >/dev/null 2>&1
before="$( git rev-list --count HEAD 2>/dev/null || echo 0 )"
out1="$(commit_try peer)"; rc1=$?
after="$( git rev-list --count HEAD 2>/dev/null || echo 0 )"
if [ "$rc1" -ne 0 ] && printf '%s' "$out1" | grep -q 'REFUSED' && [ "$before" = "$after" ]; then
  ok "a second entrant is REFUSED and NO commit was created (the #3436 incident, prevented)"
else
  bad "the peer-held lane accepted a commit: rc=$rc1 before=$before after=$after
$out1"
fi
if printf '%s' "$out1" | grep -q "holder pid: $PEER"; then
  ok "the refusal NAMES the occupying pid, so the operator can find that session"
else
  bad "the refusal did not name the holder pid ($PEER):
$out1"
fi
bash "$REPO/scripts/flow/lane-lock.sh" release "$ISSUE" --force >/dev/null 2>&1

# ---------------------------------------------------------------------------
echo "TEST 2: CONTROL — a FREE lane is ACQUIRED by the hook and the commit proceeds"
# ---------------------------------------------------------------------------
out2="$(commit_try free)"; rc2=$?
held="$(bash "$REPO/scripts/flow/lane-lock.sh" probe "$ISSUE" 2>/dev/null | tr ' ' '\n' | sed -n 's/^liveness=//p' | head -1)"
if [ "$rc2" -eq 0 ] && [ "$held" = "SELF" ]; then
  ok "a FREE lane is acquired by the hook and the commit succeeds — entry wiring works"
else
  bad "free-lane commit failed or did not acquire: rc=$rc2 liveness=$held
$out2"
fi

# ---------------------------------------------------------------------------
echo "TEST 3: CONTROL — a lane already held by THIS session commits normally"
# ---------------------------------------------------------------------------
out3="$(commit_try self)"; rc3=$?
if [ "$rc3" -eq 0 ]; then
  ok "a lane held by THIS session is not blocked — the hook does not brick its own holder"
else
  bad "the hook refused its OWN holder, which would brick every lane: rc=$rc3
$out3"
fi
bash "$REPO/scripts/flow/lane-lock.sh" release "$ISSUE" --force >/dev/null 2>&1

# ---------------------------------------------------------------------------
echo "TEST 4: CONTROL — outside a lane the hook NO-OPS"
# ---------------------------------------------------------------------------
# core.hooksPath is repo-wide, so this same hook runs in the root checkout and in the
# telemetry-<N> worktrees finalize creates. If it did anything there it would break them.
OUT="$T/notalane"; mkdir -p "$OUT"
( cd "$OUT" && git init -q . && git config user.email t@t && git config user.name t )
mkdir -p "$OUT/.git/hooks"; cp "$HOOK" "$OUT/.git/hooks/pre-commit"; chmod +x "$OUT/.git/hooks/pre-commit"
out4="$( cd "$OUT" && echo x > a.txt && git add a.txt && git commit -q -m outside 2>&1 )"; rc4=$?
cd "$LANE" || true
if [ "$rc4" -eq 0 ] && [ -z "$(printf '%s' "$out4" | grep -c 'lane-lock' | grep -v '^0$')" ]; then
  ok "outside /data/lanes/lane-<N> the hook is silent and does not interfere"
else
  bad "the hook acted outside a lane (would break the root checkout / telemetry worktrees): rc=$rc4
$out4"
fi

echo "==== lane-lock pre-commit hook: passed=$PASS failed=$FAIL ===="
[ "$FAIL" -eq 0 ] || exit 1
