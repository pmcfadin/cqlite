#!/bin/bash
# usage: run-tool.sh NODE TABLE TOOL [ID] -- cmd args...   (runs in the 128 MiB runner container)
# Records stdout/stderr/rtime and the container's memory.events oom_kill before/after; appends a row to /work/out/results.tsv
node=$1; table=$2; tool=$3; shift 3; id=""; if [ "$1" != "--" ]; then id=$1; shift; fi; shift
out=/work/out/$node/$table; mkdir -p $out; base=$out/$tool${id:+-$id}
oom() { awk '/^oom_kill /{print $2}' /sys/fs/cgroup/memory.events; }
o0=$(oom); printf '%s\n' "$*" > $base.cmd
/work/bin/rtime $base.time "$@" > $base.out 2> $base.err
ec=$?; o1=$(oom)
. $base.time 2>/dev/null
peak=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null)
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$node" "$table" "$tool${id:+-$id}" "$exit" "$wall_s" "$maxrss_kib" "$((o1-o0))" "$signaled" "$(stat -c %s $base.out)" >> /work/out/results.tsv
exit 0
