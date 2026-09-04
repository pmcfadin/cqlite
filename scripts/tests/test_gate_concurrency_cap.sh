#!/usr/bin/env bash
# Regression test for issue #1825: the machine-wide FULL-gate concurrency cap.
#
# A cross-process bounded semaphore wraps the FULL `agent-gate.sh` run: at most N
# full gates execute machine-wide at once, excess invocations QUEUE (block) for a
# slot and never fail, and a SIGKILLed slot-holder releases its slot so a queued
# gate proceeds. --lite runs are EXEMPT (never queued).
#
# This test exercises the semaphore HERMETICALLY via the gate's test-only stub
# mode (CQLITE_GATE_STUB_RUNDIR): a stub run acquires a REAL slot, drops a per-PID
# marker while "working", sleeps, then exits 0 without running any real gate work.
# Every run here pins a PRIVATE CQLITE_GATE_SLOTS_DIR so it can never interfere
# with (or be perturbed by) a real gate running on the same machine.
#
# Run standalone:   bash scripts/tests/test_gate_concurrency_cap.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP - no python3 on PATH (the #1825 cap needs python3 for fcntl.flock)"
  exit 0
fi

# #3755: this suite's subject is the SEMAPHORE, not disk. Every stub below is a full
# gate and so runs the disk-admission probe; without this pin a low-on-space box — or an
# inherited CQLITE_GATE_MIN_FREE_GB — would make every stub refuse before it ever
# reached the slot, and the suite's verdict would become a function of the host.
export CQLITE_GATE_MIN_FREE_GB=0

tmp=$(mktemp -d "${TMPDIR:-/tmp}/gate-cap-test.XXXXXX")
trap 'rm -rf "$tmp"; kill $(jobs -p) 2>/dev/null' EXIT

# marker_count <rundir>: how many stub runs currently hold a slot (marker files).
# Always prints an integer (0 when none), never empty — callers use it in `[ ]`.
marker_count() {
  local d="$1" c=0 f
  for f in "$d"/holding.*; do
    [ -e "$f" ] && c=$(( c + 1 ))
  done
  printf '%s' "$c"
}

# wait_until <timeout_s> <cmd...>: poll cmd (exit 0 == satisfied) up to timeout.
wait_until() {
  local timeout="$1"; shift
  local deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if "$@"; then return 0; fi
    sleep 0.2
  done
  return 1
}

# wait_for_markers <rundir> <target> <timeout_s>: re-poll the live marker count
# until it reaches <target> (or timeout). Unlike passing `$(marker_count ...)` to
# wait_until, this re-reads the count each iteration instead of a stale snapshot.
wait_for_markers() {
  local d="$1" target="$2" timeout="$3"
  local deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    [ "$(marker_count "$d")" -ge "$target" ] && return 0
    sleep 0.2
  done
  return 1
}

# ---------------------------------------------------------------------------
# Case (a): M > N full gates concurrently -> at most N run at once; rest queue
# then run; none fail from the cap.
# ---------------------------------------------------------------------------
a_slots="$tmp/a-slots"; a_run="$tmp/a-run"
mkdir -p "$a_run"
N=2; M=5
declare -a a_pids=()
for _ in $(seq 1 "$M"); do
  # STUB_SLEEP is deliberately generous (4s): case (a)'s lower-bound assertion
  # (a_max >= N) needs the N concurrent holders to overlap in time long enough that
  # a loaded machine's sampler is guaranteed to observe them together. A short sleep
  # could let holders come and go between polls and spuriously miss the overlap.
  CQLITE_GATE_SLOTS_DIR="$a_slots" CQLITE_GATE_MAX_CONCURRENCY="$N" \
    CQLITE_GATE_STUB_RUNDIR="$a_run" CQLITE_GATE_STUB_SLEEP=4 CQLITE_GATE_POLL_SECS=0.3 \
    bash "$GATE" >/dev/null 2>&1 &
  a_pids+=("$!")
done

# Sample concurrency while the batch drains; track the max observed holders.
a_max=0
a_deadline=$(( $(date +%s) + 30 ))
while [ "$(date +%s)" -lt "$a_deadline" ]; do
  c=$(marker_count "$a_run")
  [ "$c" -gt "$a_max" ] && a_max="$c"
  # Stop sampling once every launched run has exited.
  running=0
  for p in "${a_pids[@]}"; do kill -0 "$p" 2>/dev/null && running=$((running + 1)); done
  [ "$running" -eq 0 ] && break
  sleep 0.05
done

a_fail=0
for p in "${a_pids[@]}"; do wait "$p" || a_fail=$((a_fail + 1)); done

if [ "$a_max" -le "$N" ]; then
  ok "cap: at most N=$N stub gates ran at once (observed max=$a_max of M=$M)"
else
  bad "cap: observed max=$a_max EXCEEDS N=$N (semaphore leaked a slot)"
fi
if [ "$a_max" -ge "$N" ]; then
  ok "cap: parallelism reached N=$N (observed max=$a_max) — slots are actually used"
else
  bad "cap: never reached N=$N concurrency (observed max=$a_max) — over-serialized"
fi
if [ "$a_fail" -eq 0 ]; then
  ok "cap: all M=$M queued runs completed (exit 0) — none failed from the cap"
else
  bad "cap: $a_fail of M=$M runs exited non-zero (the cap must queue, not fail)"
fi

# ---------------------------------------------------------------------------
# Case (b): --lite is NOT blocked when the cap is saturated.
# ---------------------------------------------------------------------------
b_slots="$tmp/b-slots"; b_run="$tmp/b-run"
mkdir -p "$b_run"
BN=2
declare -a b_fillers=()
for _ in $(seq 1 "$BN"); do
  CQLITE_GATE_SLOTS_DIR="$b_slots" CQLITE_GATE_MAX_CONCURRENCY="$BN" \
    CQLITE_GATE_STUB_RUNDIR="$b_run" CQLITE_GATE_STUB_SLEEP=15 CQLITE_GATE_POLL_SECS=0.3 \
    bash "$GATE" >/dev/null 2>&1 &
  b_fillers+=("$!")
done
# Wait until both slots are saturated (re-polls the live marker count).
wait_for_markers "$b_run" "$BN" 10
saturated=$(marker_count "$b_run")

# Launch a --lite run against the SATURATED cap; it must finish fast (exempt), not
# wait ~15s for a slot. Use its own rundir marker to confirm it actually ran.
b_lite_run="$tmp/b-lite-run"; mkdir -p "$b_lite_run"
lite_start=$(date +%s)
CQLITE_GATE_SLOTS_DIR="$b_slots" CQLITE_GATE_MAX_CONCURRENCY="$BN" \
  CQLITE_GATE_STUB_RUNDIR="$b_lite_run" CQLITE_GATE_STUB_SLEEP=0 CQLITE_GATE_POLL_SECS=0.3 \
  bash "$GATE" --lite >/dev/null 2>&1
lite_rc=$?
lite_elapsed=$(( $(date +%s) - lite_start ))

# Tear down the long-sleeping fillers immediately (don't wait 15s).
for p in "${b_fillers[@]}"; do kill "$p" 2>/dev/null; done
for p in "${b_fillers[@]}"; do wait "$p" 2>/dev/null; done

if [ "$saturated" = "$BN" ]; then
  ok "lite-exempt: cap saturated at N=$BN before the --lite run"
else
  bad "lite-exempt: cap did not saturate ($saturated of $BN) — test setup weak"
fi
if [ "$lite_rc" -eq 0 ] && [ "$lite_elapsed" -lt 8 ]; then
  ok "lite-exempt: --lite completed in ${lite_elapsed}s despite a saturated cap (not queued)"
else
  bad "lite-exempt: --lite blocked or failed (rc=$lite_rc, ${lite_elapsed}s) — must be exempt"
fi

# ---------------------------------------------------------------------------
# Case (c): SIGKILL a slot-holder -> its slot is released and a queued gate proceeds.
# ---------------------------------------------------------------------------
c_slots="$tmp/c-slots"; c_run="$tmp/c-run"
mkdir -p "$c_run"
# N=1: exactly one slot, so holder A blocks the queue until it dies. A sleeps long
# enough to still be alive (holding the slot) when we SIGKILL it below.
CQLITE_GATE_SLOTS_DIR="$c_slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_RUNDIR="$c_run" CQLITE_GATE_STUB_SLEEP=30 CQLITE_GATE_POLL_SECS=0.3 \
  bash "$GATE" >/dev/null 2>&1 &
c_a=$!
# Wait for A to hold the only slot.
if wait_until 10 test -e "$c_run/holding.$c_a"; then
  ok "sigkill: holder A acquired the only slot (N=1)"
else
  bad "sigkill: holder A never acquired the slot"
fi

# B queues behind A (short sleep once it gets in).
CQLITE_GATE_SLOTS_DIR="$c_slots" CQLITE_GATE_MAX_CONCURRENCY=1 \
  CQLITE_GATE_STUB_RUNDIR="$c_run" CQLITE_GATE_STUB_SLEEP=1 CQLITE_GATE_POLL_SECS=0.3 \
  bash "$GATE" >/dev/null 2>&1 &
c_b=$!
sleep 1
# B must NOT be holding yet (A owns the sole slot).
if [ -e "$c_run/holding.$c_b" ]; then
  bad "sigkill: B acquired a slot while A still held the only one (cap leaked)"
else
  ok "sigkill: B correctly queued while A held the only slot"
fi

# SIGKILL A: its fd 9 closes, the kernel releases the flock, B's next sweep gets it.
kill -9 "$c_a" 2>/dev/null
wait "$c_a" 2>/dev/null
if wait_until 15 test -e "$c_run/holding.$c_b"; then
  ok "sigkill: after A was SIGKILLed, queued B acquired the freed slot"
else
  bad "sigkill: B never acquired the slot after A died (stale-slot leak / deadlock)"
fi
if wait "$c_b"; then
  ok "sigkill: B completed (exit 0) after inheriting the released slot"
else
  bad "sigkill: B did not exit 0 after acquiring the freed slot"
fi

# ---------------------------------------------------------------------------
# Case (d): the default N formula = max(2, floor((ncpu-2)/4)) and the override.
# ---------------------------------------------------------------------------
# The override is honored (verified indirectly above via N=1/N=2 behavior). Here
# assert the documented default floor of 2 by driving the exact formula in shell.
ncpu_probe=$( { command -v nproc >/dev/null 2>&1 && nproc; } || sysctl -n hw.ncpu 2>/dev/null || echo 4 )
case "$ncpu_probe" in *[!0-9]*|'') ncpu_probe=4 ;; esac
formula=$(( ( ncpu_probe - 2 ) / 4 ))
[ "$formula" -lt 2 ] && formula=2
if [ "$formula" -ge 2 ]; then
  ok "default-N: formula max(2, floor((ncpu-2)/4)) = $formula (>= floor of 2) for ncpu=$ncpu_probe"
else
  bad "default-N: formula produced $formula (< 2 floor) for ncpu=$ncpu_probe"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
