#!/usr/bin/env bash
# WS0 #3217 Part B chain. Serialized: the box must be idle for every capture.
set -u
source /data/ws0/ws0env.sh
H="$WT/docs/reports/ws0-3217-artifacts/harness"
PROG=/data/ws0/logs/driver-partB/progress.txt
: > "$PROG"
step() { echo "$(date -u +%FT%TZ) START $1" >> "$PROG"; shift; }
run() { # <name> <cmd...>
  local n="$1"; shift
  echo "$(date -u +%FT%TZ) START $n" >> "$PROG"
  ( cd "$H" && "$@" ) > "/data/ws0/logs/driver-partB/$n.out" 2>&1 < /dev/null
  # rc MUST be captured BEFORE any other command substitution: $(date ...) runs a
  # subshell and OVERWRITES $?, so `echo "$(date) END rc=$?"` always logs rc=0.
  # That bug shipped in this run's driver logs (#3217 P1) - a failed step was
  # indistinguishable from a clean one in the only retained progress ledger.
  local rc=$?
  echo "$(date -u +%FT%TZ) END   $n rc=$rc" >> "$PROG"
  sleep 5
}

# --- AC4 + AC5 first: the load-bearing off-CPU attribution ---
run offcpu-s6 ./profile-offcpu.sh offcpu-s6 s6 1,8,16 30 bypass
run offcpu-s1 ./profile-offcpu.sh offcpu-s1 s1 1,8,16 30 bypass

# --- park COUNTS: the ~1,960-parks-per-batch question ---
run park-s6-N1  /data/ws0/park-count-run.sh park-s6-N1  s6 1  30 bypass
run park-s6-N16 /data/ws0/park-count-run.sh park-s6-N16 s6 16 30 bypass
run park-s1-N1  /data/ws0/park-count-run.sh park-s1-N1  s1 1  30 bypass

# --- AC3: on-CPU matrix ---
for s in s6 s1; do for n in 1 8 16; do
  run oncpu-$s-N$n ./profile-oncpu.sh oncpu-$s-N$n $s $n 30 bypass
done; done

echo "$(date -u +%FT%TZ) ALL-DONE" >> "$PROG"
