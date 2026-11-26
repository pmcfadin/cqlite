#!/bin/bash
export OUT_ROOT=/work/outRR/diff; mkdir -p $OUT_ROOT; touch $OUT_ROOT/results.tsv; export RTIME_CAP_KIB=1700000
C=/work/bin/cqlite-e573ff8; R=/work/bin/run-tool.sh; NOW=1795662946; NOWFAR=$((1795659346 + 30*86400)); S=/work/schema/fr.t_udt.cql
args=(); while IFS=$'\t' read -r cat key; do args+=(--partition "$key"); done < /work/samples/t_udt.keys
td() { echo /snap/$1/fr/t_udt-*; }
for p in "db0 db1" "db0 db2" "db1 db2" "db1 db0"; do set -- $p
  $R diff t_udt diff healthy-t_udt-$1-$2-now -- $C --schema $S diff $(td $1) $(td $2) --table fr.t_udt --now $NOW "${args[@]}" --out json
  $R diff t_udt diff healthy-t_udt-$1-$2-far -- $C --schema $S diff $(td $1) $(td $2) --table fr.t_udt --now $NOWFAR "${args[@]}" --out json
done
$R diff t_udt diff self-t_udt-db0 -- $C --schema $S diff $(td db0) $(td db0) --table fr.t_udt --now $NOW "${args[@]}" --out json
# full-table cell exactly as the original (F5 expected)
$R diff t_udt diff full-t_udt-db0-db1-now -- $C --schema $S diff $(td db0) $(td db1) --table fr.t_udt --now $NOW --out json
echo UDTDIFF-DONE >> $OUT_ROOT/done.log
