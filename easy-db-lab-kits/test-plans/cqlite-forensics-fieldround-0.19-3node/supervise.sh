#!/bin/bash
# dev-box supervisor: keep runall.sh going inside the runner container across OOM-kill restarts.  usage: supervise.sh OUT_ROOT
K=/tmp/claude-1001/-home-dev-projects-cqlite/52732231-2324-4ae1-9047-f3acb72a1af8/scratchpad/bin/k
O=$1
while true; do
  if $K exec tools -c runner -- bash -c "grep -q ALL-DONE $O/done.log 2>/dev/null" 2>/dev/null; then echo "$(date -u +%T) ALL-DONE"; exit 0; fi
  if ! $K exec tools -c runner -- bash -c "pgrep -f '[b]in/runall.sh' >/dev/null" 2>/dev/null; then
    echo "$(date -u +%T) (re)starting runall"; $K exec tools -c runner -- bash -c "OUT_ROOT=$O SKIPLIST=${SKIPLIST:-} nohup bash /work/bin/runall.sh > $O/runall.log 2>&1 &" 2>&1 | tail -1
  fi
  sleep 30
done
