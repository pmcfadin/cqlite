#!/bin/bash
# generic dev-box supervisor: keep `bash SCRIPT` running in the runner across OOM restarts until DONE_TOKEN appears in DONE_FILE.  usage: supervise2.sh "ENV=.. " SCRIPT DONE_FILE TOKEN
K=/tmp/claude-1001/-home-dev-projects-cqlite/52732231-2324-4ae1-9047-f3acb72a1af8/scratchpad/bin/k
while true; do
  if $K exec tools -c runner -- bash -c "grep -q '$4' $3 2>/dev/null" 2>/dev/null; then echo "$(date -u +%T) DONE"; exit 0; fi
  if ! $K exec tools -c runner -- bash -c "pgrep -f '[b]in/$(basename $2)' >/dev/null" 2>/dev/null; then
    echo "$(date -u +%T) (re)start"; $K exec tools -c runner -- bash -c "$1 nohup bash $2 > /work/$(basename $2).log 2>&1 &" 2>&1 | tail -1; fi
  sleep 30
done
