#!/usr/bin/env bash
set -u
PROG=/data/ws0/logs/driver-partB/progress.txt
run() { local n="$1"; shift; echo "$(date -u +%FT%TZ) START $n" >> "$PROG"
  "$@" > "/data/ws0/logs/driver-partB/$n.out" 2>&1 < /dev/null
  echo "$(date -u +%FT%TZ) END   $n rc=$?" >> "$PROG"; sleep 5; }
run llc-s1-N2  /data/ws0/llc-run.sh llc-s1-N2  s1 2  20
run llc-s6-N16 /data/ws0/llc-run.sh llc-s6-N16 s6 16 20
run llc-s6-N1  /data/ws0/llc-run.sh llc-s6-N1  s6 1  20
echo "$(date -u +%FT%TZ) ROUND4-DONE" >> "$PROG"
