#!/bin/bash
# usage: transfer.sh TAG DESTPREFIX "tables" "nodes"   e.g. transfer.sh fr1 db "t_basic t_coll" "0 1 2"
# Streams a snapshot from each Cassandra pod into the tools pod (oracle container) over two kubectl exec calls, then checks every file by sha256.
TAG=$1; PFX=$2; TABLES=$3; NODES=${4:-"0 1 2"}
K=/tmp/claude-1001/-home-dev-projects-cqlite/52732231-2324-4ae1-9047-f3acb72a1af8/scratchpad/bin/k
for n in $NODES; do for t in $TABLES; do
  s=$(date +%s)
  tdir=$($K exec cassandra-$n -c cassandra -- bash -c "basename \$(ls -d /var/lib/cassandra/data/fr/$t-*)")
  snap=/var/lib/cassandra/data/fr/$tdir/snapshots/$TAG
  $K exec cassandra-$n -c cassandra -- bash -c "cd $snap && find . -type f ! -name manifest.json ! -name schema.cql -printf '%P\n' | sort | xargs sha256sum" > /tmp/claude-1001/-home-dev-projects-cqlite/52732231-2324-4ae1-9047-f3acb72a1af8/scratchpad/fr/manifests-sha/$PFX$n-$t-$TAG.sha256 || { echo "FAIL manifest $n $t"; continue; }
  $K exec cassandra-$n -c cassandra -- bash -c "cd $snap && tar -cf - --exclude=manifest.json --exclude=schema.cql ." \
    | $K exec -i tools -c oracle -- bash -c "mkdir -p /snap/$PFX$n/fr/$tdir && tar -C /snap/$PFX$n/fr/$tdir -xf -" || echo "FAIL stream $n $t"
  if cat /tmp/claude-1001/-home-dev-projects-cqlite/52732231-2324-4ae1-9047-f3acb72a1af8/scratchpad/fr/manifests-sha/$PFX$n-$t-$TAG.sha256 | $K exec -i tools -c oracle -- bash -c "cd /snap/$PFX$n/fr/$tdir && sha256sum -c --quiet"; then
    echo "OK $PFX$n $t $tdir $(( $(date +%s)-s ))s"; else echo "SHA-MISMATCH $PFX$n $t"; fi
done; done
