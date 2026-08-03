#!/usr/bin/env bash
set -u
source /data/ws0/ws0env.sh
PROG=/data/ws0/logs/driver-partB/progress.txt
run() { local n="$1"; shift
  echo "$(date -u +%FT%TZ) START $n" >> "$PROG"
  "$@" > "/data/ws0/logs/driver-partB/$n.out" 2>&1 < /dev/null
  echo "$(date -u +%FT%TZ) END   $n rc=$?" >> "$PROG"; sleep 5; }
run sched2-s6-N1  /data/ws0/sched-switch-run.sh sched2-s6-N1  s6 1  10 bypass
run sched2-s6-N16 /data/ws0/sched-switch-run.sh sched2-s6-N16 s6 16 10 bypass
run sched2-s1-N1  /data/ws0/sched-switch-run.sh sched2-s1-N1  s1 1  10 bypass
echo "$(date -u +%FT%TZ) ROUND3-DONE" >> "$PROG"
