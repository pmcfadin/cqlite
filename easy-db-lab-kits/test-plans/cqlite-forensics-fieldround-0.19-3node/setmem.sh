#!/bin/bash
# usage: setmem.sh 128Mi|1536Mi : in-place resize of the runner memory limit (K8s pod resize subresource works in this vcluster)
K=/tmp/claude-1001/-home-dev-projects-cqlite/52732231-2324-4ae1-9047-f3acb72a1af8/scratchpad/bin/k
$K patch pod tools --subresource resize --type strategic -p "{\"spec\":{\"containers\":[{\"name\":\"runner\",\"resources\":{\"limits\":{\"memory\":\"$1\"}}}]}}" >/dev/null
sleep 2; $K exec tools -c runner -- cat /sys/fs/cgroup/memory.max
