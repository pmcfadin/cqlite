#!/bin/bash
C=/work/bin/cqlite; O=/work/outRR/selold; mkdir -p $O
for t in t_coll t_udt; do mkdir -p $O/db0/$t; i=0
  while IFS=$'\t' read -r cat key; do i=$((i+1)); id=$(printf '%03d' $i)
    $C --schema /work/schema/fr.$t.cql --data-dir /snap/db0 --out json query "SELECT * FROM fr.$t WHERE id = $key" > $O/db0/$t/$id.json 2>/dev/null
  done < /work/samples/$t.keys
done; echo OLD-DONE >> /work/outRR/done.log
