#!/bin/bash
# usage: runall.sh  (OUT_ROOT in env) -- every node x table, small tables first
OUT_ROOT=${OUT_ROOT:-/work/out}; export OUT_ROOT; [ "$OUT_ROOT" = /work/outM ] && export RTIME_CAP_KIB=1700000
for t in t_static t_tomb t_udt t_wide t_coll t_basic; do for n in db0 db1 db2; do
  grep -q "MATRIX-DONE $n $t" $OUT_ROOT/done.log 2>/dev/null || /work/bin/matrix.sh $n $t
done; done
for n in rsA0 rsA1 rsA2; do grep -q "MATRIX-DONE $n t_resurrect$" $OUT_ROOT/done.log 2>/dev/null || /work/bin/matrix.sh $n t_resurrect; done
for n in rsB0 rsB1 rsB2; do grep -q "MATRIX-DONE $n t_resurrect_purged" $OUT_ROOT/done.log 2>/dev/null || /work/bin/matrix.sh $n t_resurrect_purged; done
echo ALL-DONE >> $OUT_ROOT/done.log
