#!/bin/bash
# NEW C-SELECT: cqlite query SELECT * per sample key on each node's snapshot; output per key
C=/work/bin/cqlite-e573ff8; O=/work/outRR/sel; mkdir -p $O
for t in t_coll t_udt; do for n in db0 db1 db2; do mkdir -p $O/$n/$t; i=0
  while IFS=$'\t' read -r cat key; do i=$((i+1)); id=$(printf '%03d' $i)
    pkc=id; s=$(date +%s%N)
    $C --schema /work/schema/fr.$t.cql --data-dir /snap/$n --out json query "SELECT * FROM fr.$t WHERE id = $key" > $O/$n/$t/$id.json 2> $O/$n/$t/$id.err; rc=$?
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' $n $t $id $rc "$(( ($(date +%s%N) - s)/1000000 ))ms" "$key" >> $O/results.tsv
  done < /work/samples/$t.keys
done; done
echo SEL-DONE >> $O/done.log
