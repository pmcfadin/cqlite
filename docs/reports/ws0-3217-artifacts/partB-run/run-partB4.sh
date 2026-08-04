#!/usr/bin/env bash
set -u
PROG=/data/ws0/logs/driver-partB/progress.txt
run() { local n="$1"; shift; echo "$(date -u +%FT%TZ) START $n" >> "$PROG"
  "$@" > "/data/ws0/logs/driver-partB/$n.out" 2>&1 < /dev/null
  # rc MUST be captured BEFORE any other command substitution: $(date ...) runs a
  # subshell and OVERWRITES $?, so `echo "$(date) END rc=$?"` always logs rc=0.
  # That bug shipped in this run's driver logs (#3217 P1) - a failed step was
  # indistinguishable from a clean one in the only retained progress ledger.
  local rc=$?
  echo "$(date -u +%FT%TZ) END   $n rc=$rc" >> "$PROG"; sleep 5; }
run llc-s1-N2  /data/ws0/llc-run.sh llc-s1-N2  s1 2  20
run llc-s6-N16 /data/ws0/llc-run.sh llc-s6-N16 s6 16 20
run llc-s6-N1  /data/ws0/llc-run.sh llc-s6-N1  s6 1  20
echo "$(date -u +%FT%TZ) ROUND4-DONE" >> "$PROG"
