#!/bin/bash
# usage: run-tool.sh NODE TABLE TOOL [ID] -- cmd args...   (runs in the runner container)
# OUT_ROOT (default /work/out) selects the result tree: /work/out = enforce run (128 MiB limit), /work/outM = measurement run (raised limit).
# A START row is written first so a container-wide OOM kill (which also kills this wrapper) is detectable.
OUT_ROOT=${OUT_ROOT:-/work/out}
node=$1; table=$2; tool=$3; shift 3; id=""; if [ "$1" != "--" ]; then id=$1; shift; fi; shift
out=$OUT_ROOT/$node/$table; mkdir -p $out; base=$out/$tool${id:+-$id}
printf '%s\t%s\t%s\t%s\n' "$(date -u +%T)" "$node" "$table" "$tool${id:+-$id}" >> $OUT_ROOT/started.tsv
printf '%s\n' "$*" > $base.cmd; rm -f $base.time
ulimit -f 400000   # cap any single output file at ~400 MB (explain of the 2.5M-row partition)
/work/bin/rtime $base.time "$@" > $base.out 2> $base.err
ec=$?
. $base.time 2>/dev/null
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$node" "$table" "$tool${id:+-$id}" "$exit" "$wall_s" "$maxrss_kib" "$signaled" "$(stat -c %s $base.out)" "$anon_kib" "$file_kib" "$capped" >> $OUT_ROOT/results.tsv
exit 0
