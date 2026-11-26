#!/bin/bash
# Phase 5 (runner container, 128 MiB limit): run every tool on every damaged copy. usage: cases.sh   (OUT_ROOT=/work/outD)
OUT_ROOT=/work/outD; export OUT_ROOT; C=/work/bin/cqlite; R=/work/bin/run-tool.sh; NOW=1795662946
mkdir -p $OUT_ROOT; touch $OUT_ROOT/results.tsv
for cdir in /dmg/D*; do case=$(basename $cdir)
  td=$(ls -d $cdir/fr/*); tdir=$(basename $td); tbl=${tdir%-*}; S=/work/schema/fr.$tbl.cql
  if [[ $case == *-big-* ]]; then g=nb-6-big; key=8722; healthy=/snap/db1/fr/$tdir; else g=da-10-bti; key="'pk86127', 6"; healthy=/snap/db1/fr/$tdir; fi
  mkdir -p $cdir/gen; for f in $td/$g-*; do ln -sfn $f $cdir/gen/; done
  # note: gen dir holds symlinks (never copies); a removed component leaves a dangling link, which is a valid "missing file" for the tool
  $R $case $tbl audit -- $C verify $cdir/gen --mode audit --out json
  $R $case $tbl audit-deep -- $C verify $cdir/gen --mode audit --deep --out json
  $R $case $tbl verify-full -- $C verify $cdir/gen --mode full --out json
  $R $case $tbl diagnose -- $C diagnose $td --now $NOW --out json
  $R $case $tbl diagnose-deep -- $C diagnose $td --now $NOW --deep --top 5 --out json
  $R $case $tbl find -- $C --schema $S find $td "$key" --out json
  $R $case $tbl explain -- $C --schema $S --data-dir $cdir explain fr.$tbl "$key" --now $NOW --out json
  $R $case $tbl diff -- $C --schema $S diff $td $healthy --table fr.$tbl --now $NOW --out json
  $R $case $tbl query -- $C --schema $S --data-dir $cdir query "SELECT * FROM fr.$tbl LIMIT 3"
  (cd $td && sha256sum -c $cdir/before.sha256 > $cdir/after-check.txt 2>&1; echo "sha256-check-rc=$?" >> $cdir/after-check.txt)
done
echo CASES-DONE >> $OUT_ROOT/done.log
