#!/usr/bin/env bash
# Part B round 2: waits for round 1, then re-runs the defective off-CPU captures
# with the patched (big counts-map) offcputime, and adds the perf sched_switch
# park-count captures.
set -u
source /data/ws0/ws0env.sh
H="$WT/docs/reports/ws0-3217-artifacts/harness"
PROG=/data/ws0/logs/driver-partB/progress.txt
while ! grep -q ALL-DONE "$PROG" 2>/dev/null; do sleep 20; done
run() { local n="$1"; shift
  echo "$(date -u +%FT%TZ) START $n" >> "$PROG"
  ( cd "$H" && "$@" ) > "/data/ws0/logs/driver-partB/$n.out" 2>&1 < /dev/null
  echo "$(date -u +%FT%TZ) END   $n rc=$?" >> "$PROG"; sleep 5; }

# Off-CPU re-run with the patched collector (bcc default counts map = 10240 keys
# SATURATED at N=8/16, silently dropping stacks).
export WS0_OFFCPUTIME_BIN=/data/ws0/tools/offcputime-bigmap
export WS0_STACK_STORAGE_SIZE=65536
run offcpu2-s6 ./profile-offcpu.sh offcpu2-s6 s6 1,8,16 30 bypass
run offcpu2-s1 ./profile-offcpu.sh offcpu2-s1 s1 1,8,16 30 bypass

# Fully-symbolized park COUNTS.
run sched-s6-N1  /data/ws0/sched-switch-run.sh sched-s6-N1  s6 1  10 bypass
run sched-s6-N16 /data/ws0/sched-switch-run.sh sched-s6-N16 s6 16 10 bypass
run sched-s1-N1  /data/ws0/sched-switch-run.sh sched-s1-N1  s1 1  10 bypass
echo "$(date -u +%FT%TZ) ROUND2-DONE" >> "$PROG"
