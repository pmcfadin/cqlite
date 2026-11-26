#!/bin/bash
# sstablemetadata (batched: one JVM per node/table) -> TSV (node table gen minTS maxTS minLDT maxLDT droppable repairedAt comprRatio partitions)
export MAX_HEAP_SIZE=256M HEAP_NEWSIZE=64M CASSANDRA_CONF=/work/oracle/db0/conf; cd /tmp; O=/work/out/oracle; mkdir -p $O; : > $O/meta.tsv
for n in db0 db1 db2 rsA0 rsA1 rsA2 rsB0 rsB1 rsB2; do
 for d in /snap/$n/fr/*; do t=$(basename $d | sed 's/-[0-9a-f]\{32\}$//')
   /opt/cassandra/tools/bin/sstablemetadata $d/*-Data.db 2>/dev/null | awk -v n=$n -v t=$t -F': ' '
    function flush(){ if (g!="") printf "%s\t%s\t%s\tmin=%s\tmax=%s\tminldt=%s\tmaxldt=%s\tdrop=%s\trep=%s\tcr=%s\tparts=%s\n",n,t,g,mt,xt,ml,xl,dr,ra,cr,p; g=""; mt=xt=ml=xl=dr=ra=cr=""; p=0 }
    /^SSTable: /{flush(); g=$2; sub(/.*\//,"",g)}
    /^Minimum timestamp/{split($2,a," \\("); mt=a[2]; sub(/\)/,"",mt)} /^Maximum timestamp/{split($2,a," \\("); xt=a[2]; sub(/\)/,"",xt)}
    /^SSTable min local deletion time/{split($2,a," \\("); ml=(a[2]==""?a[1]:a[2]); sub(/\)/,"",ml)} /^SSTable max local deletion time/{split($2,a," \\("); xl=(a[2]==""?a[1]:a[2]); sub(/\)/,"",xl)}
    /^Estimated droppable tombstones/{dr=$2} /^Repaired at/{ra=$2} /^Compression ratio/{cr=$2}
    /^Partition Size:/{ps=1} /^Column Count:/{ps=0} ps && /\|/ {split($0,b,"|"); split(b[2],c," "); if (c[1]+0>0) p+=c[1]}
    END{flush()}' >> $O/meta.tsv
 done
done
echo META-DONE >> $O/meta.tsv
