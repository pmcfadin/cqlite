#!/bin/bash
# usage: matrix.sh NODE TABLE   (in the runner container; OUT_ROOT selects enforce vs measurement run). Idempotent: skips cells already in results.tsv.
OUT_ROOT=${OUT_ROOT:-/work/out}; export OUT_ROOT; [ "$OUT_ROOT" = /work/outM ] && export RTIME_CAP_KIB=1700000
node=$1; table=$2; C=/work/bin/cqlite; R=/work/bin/run-tool.sh
NOW=1795662946; NOWFAR=$((1795659346 + 30*86400))
tdir=$(basename /snap/$node/fr/$table-*); TD=/snap/$node/fr/$tdir; S=/work/schema/fr.$table.cql
mkdir -p $OUT_ROOT; touch $OUT_ROOT/results.tsv
cell() { # id tool -- cmd
  local tool=$1 id=$2; shift 2
  if [ -n "$SKIPLIST" ] && awk -F'\t' -v n="$node" -v t="$table" -v x="$tool${id:+-$id}" '$1==n&&$2==t&&$3==x{f=1}END{exit !f}' $SKIPLIST; then return; fi
  if awk -F'\t' -v n="$node" -v t="$table" -v x="$tool${id:+-$id}" '$1==n&&$2==t&&$3==x{f=1}END{exit !f}' $OUT_ROOT/results.tsv; then return; fi
  if awk -F'\t' -v n="$node" -v t="$table" -v x="$tool${id:+-$id}" '$2==n&&$3==t&&$4==x{f=1}END{exit !f}' $OUT_ROOT/started.tsv; then
    # started before but never recorded: the container was OOM-killed (cgroup limit) mid-run; record that as the result
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$node" "$table" "$tool${id:+-$id}" "OOMKILLED" "" "" 137 0 "" "" "" >> $OUT_ROOT/results.tsv; return
  fi
  $R $node $table $tool $id -- "$@"
}
# per-generation symlink dirs so verify (one generation dir) can be pointed at every generation
mkdir -p /work/gens/$node/$table /work/sweep/$node/$table/fr
ln -sfn $TD /work/sweep/$node/$table/fr/$tdir
for dg in $(ls $TD | sed -n 's/^\([a-z][a-z]-[0-9]*-[a-z]*\)-Data.db$/\1/p'); do
  mkdir -p /work/gens/$node/$table/$dg; for f in $TD/$dg-*; do ln -sfn $f /work/gens/$node/$table/$dg/; done
done
cell sweep-audit "" $C sweep /work/sweep/$node/$table --mode audit --out json
for g in $(ls /work/gens/$node/$table); do
  G=/work/gens/$node/$table/$g
  cell audit $g $C verify $G --mode audit --out json
  cell audit-deep $g $C verify $G --mode audit --deep --out json
  cell verify-full $g $C verify $G --mode full --out json
done
cell diagnose "" $C diagnose $TD --now $NOW --out json
cell diagnose-deep "" $C diagnose $TD --now $NOW --deep --top 10 --out json
cell diagnose-far "" $C diagnose $TD --now $NOWFAR --out json
cell diagnose-meta "" $C diagnose $TD --now 1796529208 --out json   # gcBefore = oracle run time (sstablemetadata uses the wall clock with no gc_grace)
i=0
while IFS=$'\t' read -r cat key; do
  i=$((i+1)); id=$(printf '%03d' $i)
  cell find $id $C --schema $S find $TD "$key" --out json
  cell explain $id $C --schema $S --data-dir /snap/$node explain fr.$table "$key" --now $NOW --out json
  if [ $i -le 10 ]; then cell explain-far $id $C --schema $S --data-dir /snap/$node explain fr.$table "$key" --now $NOWFAR --out json; fi
done < /work/samples/$table.keys
echo "MATRIX-DONE $node $table" >> $OUT_ROOT/done.log
