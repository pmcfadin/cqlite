#!/bin/bash
# flush every 120 s for N minutes (plan 1.3)
end=$(( $(date +%s) + $1*60 ))
while [ $(date +%s) -lt $end ]; do
  sleep 120
  for p in 0 1 2; do /tmp/claude-1001/-home-dev-projects-cqlite/52732231-2324-4ae1-9047-f3acb72a1af8/scratchpad/bin/k exec cassandra-$p -c cassandra -- nodetool flush fr >/dev/null 2>&1; done
  echo "flushed $(date -u +%T)"
done
