#!/bin/bash
# raised-limit pass (runner at 1800 MiB): (1) Phase 5 audit --deep / verify --mode full on every damaged copy, (2) healthy diff of the two largest tables measured with a sampler cap, (3) find --commitlog leg, (4) sweep --mode full.
export RTIME_CAP_KIB=1700000
HEAVY=1 bash /work/bin/cases.sh
export OUT_ROOT=/work/outH; mkdir -p $OUT_ROOT; R=/work/bin/run-tool.sh; C=/work/bin/cqlite; NOW=1795662946
for t in t_coll t_basic; do d=$(ls -d /snap/db0/fr/$t-*); e=$(ls -d /snap/db1/fr/$t-*)
  timeout 900 $R diffH $t diff healthy-db0-db1 -- $C --schema /work/schema/fr.$t.cql diff $d $e --table fr.$t --now $NOW --out json; done
# 4.5 CommitLog leg: cassandra-1 snapshot (db1) + the copied commitlog segments (50 unflushed t_basic keys 20000000..20000049; phase-B key 1500000 was flushed)
t=t_basic; d=$(ls -d /snap/db1/fr/$t-*)
for k in 20000000 20000007 20000049 1500000 20000050; do
  $R db1 $t find-commitlog $k -- $C --schema /work/schema/fr.$t.cql find $d $k --commitlog /snap/commitlog1 --out json; done
for n in db0; do $R $n sweep full-all -- $C sweep /snap/$n --mode full --out json --jobs 1; done
echo HEAVY-DONE >> /work/outH/done.log
