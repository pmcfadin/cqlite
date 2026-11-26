#!/bin/bash
# sstabledump -t -k <all sample keys> per generation (db0), one JVM per generation. Output /work/out/oracle/dump/<table>/<gen>.json
export MAX_HEAP_SIZE=384M HEAP_NEWSIZE=96M CASSANDRA_CONF=/work/oracle/db0/conf; cd /tmp
O=/work/out/oracle/dump; mkdir -p $O
for t in t_tomb t_static t_udt t_coll t_wide t_basic t_resurrect; do
  [ $t = t_resurrect ] && d=$(ls -d /snap/rsA0/fr/t_resurrect-*) || d=$(ls -d /snap/db0/fr/$t-*)
  mkdir -p $O/$t; args=""; while read k; do args="$args -k $k"; done < /work/dumpkeys/$t.keys
  for f in $d/*-Data.db; do g=$(basename $f -Data.db); s=$(date +%s)
    /opt/cassandra/tools/bin/sstabledump $f -t $args > $O/$t/$g.json 2> $O/$t/$g.err; echo "$t $g rc=$? $(( $(date +%s)-s ))s" >> $O/dump.log
  done
done
echo DUMP-DONE >> $O/dump.log
