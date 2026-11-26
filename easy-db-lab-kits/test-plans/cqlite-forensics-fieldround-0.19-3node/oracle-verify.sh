#!/bin/bash
# C2 oracle: sstableverify and sstableverify -e per (node,table); offline, via symlink farm (needs -f)
export MAX_HEAP_SIZE=512M HEAP_NEWSIZE=100M; cd /tmp; O=/work/out/oracle; mkdir -p $O
for n in 0 1 2; do export CASSANDRA_CONF=/work/oracle/db$n/conf
 for t in t_tomb t_static t_udt t_wide t_coll t_basic; do
  for mode in plain ext; do
    flag=""; [ $mode = ext ] && flag="-e"
    s=$(date +%s); sstableverify -f -v $flag fr $t > $O/db$n-$t-verify-$mode.log 2>&1; rc=$?
    ng=$(grep -c "^Verifying" $O/db$n-$t-verify-$mode.log); ok=$(grep -c "succeeded" $O/db$n-$t-verify-$mode.log)
    printf 'db%s\t%s\t%s\t%s\tgenerations=%s\tsucceeded=%s\t%ss\n' $n $t $mode $rc $ng $ok $(( $(date +%s)-s )) >> $O/verify-differential.tsv
  done
 done
done
echo DONE >> $O/verify-differential.tsv
