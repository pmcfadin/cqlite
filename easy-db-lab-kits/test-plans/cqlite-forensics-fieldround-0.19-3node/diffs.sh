#!/bin/bash
# Phase 6 (runner container): diff cells. OUT_ROOT (default /work/outF); RTIME_CAP_KIB set by the caller for measurement runs.
OUT_ROOT=${OUT_ROOT:-/work/outF}; export OUT_ROOT; C=/work/bin/cqlite; R=/work/bin/run-tool.sh
NOW=1795662946; NOWFAR=$((1795659346 + 30*86400)); SNAPA=1795660350; PAST=$((SNAPA + 11*86400))
mkdir -p $OUT_ROOT; touch $OUT_ROOT/results.tsv
td() { echo /snap/$1/fr/$2-*; }
cell() { local id=$1; shift
  if awk -F'\t' -v x="$id" '$3==x{f=1}END{exit !f}' $OUT_ROOT/results.tsv; then return; fi
  if awk -F'\t' -v x="$id" '$4==x{f=1}END{exit !f}' $OUT_ROOT/started.tsv 2>/dev/null; then
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' diff x "$id" OOMKILLED "" "" 137 0 "" "" "" >> $OUT_ROOT/results.tsv; return; fi
  $R diff x diff $id -- "$@"; }
# 6.1 healthy replicas
for t in t_static t_tomb t_udt t_wide t_coll t_basic; do S=/work/schema/fr.$t.cql
  for p in "db0 db1" "db0 db2" "db1 db2" "db1 db0"; do set -- $p; a=$1; b=$2
    cell healthy-$t-$a-$b-now  $C --schema $S diff $(td $a $t) $(td $b $t) --table fr.$t --now $NOW --out json
    cell healthy-$t-$a-$b-far  $C --schema $S diff $(td $a $t) $(td $b $t) --table fr.$t --now $NOWFAR --out json
  done
  cell self-$t-db0-db0 $C --schema $S diff $(td db0 $t) $(td db0 $t) --table fr.$t --now $NOW --out json
done
# 6.2 scenario A (tombstone present)
S=/work/schema/fr.t_resurrect.cql; A=$(td rsA0 t_resurrect); A1=$(td rsA1 t_resurrect); B=$(td rsA2 t_resurrect)
cell resA-db0-db2-in    $C --schema $S diff $A $B --table fr.t_resurrect --now $SNAPA --out json
cell resA-db0-db2-past  $C --schema $S diff $A $B --table fr.t_resurrect --now $PAST --out json
cell resA-db2-db0-in    $C --schema $S diff $B $A --table fr.t_resurrect --now $SNAPA --out json
cell resA-db2-db0-past  $C --schema $S diff $B $A --table fr.t_resurrect --now $PAST --out json
cell resA-db0-db1-in    $C --schema $S diff $A $A1 --table fr.t_resurrect --now $SNAPA --out json
cell resA-db0-db2-p5    $C --schema $S diff $A $B --table fr.t_resurrect --now $SNAPA --partition 5 --out json
cell resA-db0-db2-p150  $C --schema $S diff $A $B --table fr.t_resurrect --now $SNAPA --partition 150 --out json
cell resA-db0-db2-p350  $C --schema $S diff $A $B --table fr.t_resurrect --now $SNAPA --partition 350 --out json
cell resA-db0-db2-p700  $C --schema $S diff $A $B --table fr.t_resurrect --now $SNAPA --partition 700 --out json
cell resA-self-db2      $C --schema $S diff $B $B --table fr.t_resurrect --now $SNAPA --out json
# 6.3 scenario B (tombstone purged)
S=/work/schema/fr.t_resurrect_purged.cql; A=$(td rsB0 t_resurrect_purged); A1=$(td rsB1 t_resurrect_purged); B=$(td rsB2 t_resurrect_purged)
cell resB-db0-db2-in    $C --schema $S diff $A $B --table fr.t_resurrect_purged --now $SNAPA --out json
cell resB-db0-db2-past  $C --schema $S diff $A $B --table fr.t_resurrect_purged --now $PAST --out json
cell resB-db2-db0-in    $C --schema $S diff $B $A --table fr.t_resurrect_purged --now $SNAPA --out json
cell resB-db0-db1-in    $C --schema $S diff $A $A1 --table fr.t_resurrect_purged --now $SNAPA --out json
echo DIFFS-DONE >> $OUT_ROOT/done.log
