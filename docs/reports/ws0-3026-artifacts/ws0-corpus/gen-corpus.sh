#!/usr/bin/env bash
# WS0 corpus generator / calibrator  (CQLite issue #3026, WS0 of umbrella #3023)
#
#   gen-corpus.sh <partitions> <payload_B> <payload_pop> <bloba_B> <bloba_pop> \
#                 <blobb_B> <blobb_pop> <seq_cluster> <et_cluster> [threads] [batch_parts]
#
# Loads <partitions> partitions x (seq_cluster * et_cluster) rows into ws0.events,
# in batches of <batch_parts> partitions with a nodetool flush between each batch
# (caps memtable growth -- this box OOM-killed a 15.4 GiB-heap daemon once already),
# then `nodetool flush` + `nodetool compact` to ONE SSTable and prints measured geometry.
#
# MEMORY: daemon heap is bounded by MAX_HEAP_SIZE at daemon start (see rerun.sh);
#         the stress client JVM is bounded here via JVM_OPTS -Xmx.
set -euo pipefail

CAS=/home/ubuntu/ws0/apache-cassandra-5.0.8
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
HERE=/home/ubuntu/ws0/ws0-corpus

PARTS=${1:-20000}
PAYLOAD=${2:-375}
PAYLOAD_POP=${3:-6}
BLOBA=${4:-96}
BLOBA_POP=${5:-3}
BLOBB=${6:-96}
BLOBB_POP=${7:-1000000000}
SEQ_CL=${8:-2}
ET_CL=${9:-10}
THREADS=${10:-8}
BATCH=${11:-50000}

# Bound the cassandra-stress client JVM (it is a JVM too, and an OOM suspect).
# NOT exported: `nodetool` shares $JVM_OPTS and dies with
# "Initial heap size set to a larger value than the maximum heap size".
STRESS_JVM_OPTS="-Xms512M -Xmx4G"

TAG="p${PARTS}-pl${PAYLOAD}x${PAYLOAD_POP}-ba${BLOBA}x${BLOBA_POP}-bb${BLOBB}x${BLOBB_POP}-cl${SEQ_CL}x${ET_CL}"
PROF="$HERE/.gen-${TAG}.yaml"

python3 - "$HERE/ws0-profile.yaml" "$PROF" \
  "$PAYLOAD" "$PAYLOAD_POP" "$BLOBA" "$BLOBA_POP" "$BLOBB" "$BLOBB_POP" "$SEQ_CL" "$ET_CL" <<'PY'
import sys
src, dst, pl, plp, ba, bap, bb, bbp, seqcl, etcl = sys.argv[1:11]
s = open(src).read()
s = s.replace("CAL_PAYLOAD_POP", plp).replace("CAL_PAYLOAD", pl)
s = s.replace("CAL_BLOBA_POP", bap).replace("CAL_BLOBA", ba)
s = s.replace("CAL_BLOBB_POP", bbp).replace("CAL_BLOBB", bb)
s = s.replace("CAL_SEQ_CLUSTER", seqcl).replace("CAL_ET_CLUSTER", etcl)
assert "CAL_" not in s, [l for l in s.splitlines() if "CAL_" in l]
open(dst, "w").write(s)
PY

echo "### profile: $PROF"
grep -E "cluster:|size: fixed|population: uniform" "$PROF" | sed 's/^/    /'

# ---- clean slate -------------------------------------------------------------
python3 "$HERE/cql.py" "DROP KEYSPACE IF EXISTS ws0" >/dev/null 2>&1 || true
sleep 2
# auto_snapshot: true is STOCK, so DROP leaves a snapshot dir behind; clear it so
# exactly one events-* directory exists afterwards.
"$CAS/bin/nodetool" clearsnapshot --all >/dev/null 2>&1 || true
rm -rf /home/ubuntu/ws0/cassandra-data/data/ws0 2>/dev/null || true

ROWS_PER_PART=$((SEQ_CL * ET_CL))
echo "### loading ${PARTS} partitions x ${ROWS_PER_PART} rows/partition (~$((PARTS*ROWS_PER_PART)) rows)"
echo "### stress: ${THREADS} threads, client JVM_OPTS='${STRESS_JVM_OPTS}', batches of ${BATCH} partitions"

START=1
BATCHNO=0
while [ "$START" -le "$PARTS" ]; do
  END=$((START + BATCH - 1)); [ "$END" -gt "$PARTS" ] && END=$PARTS
  N=$((END - START + 1))
  BATCHNO=$((BATCHNO + 1))
  echo "--- batch ${BATCHNO}: partitions ${START}..${END} (n=${N})"
  env JVM_OPTS="$STRESS_JVM_OPTS" \
  "$CAS/tools/bin/cassandra-stress" user "profile=$PROF" ops\(insert=1\) \
    n="$N" no-warmup cl=ONE -pop seq="${START}..${END}" -rate threads="$THREADS" \
    -node 127.0.0.1 \
    > "$HERE/.stress-${TAG}-b${BATCHNO}.log" 2>&1 \
    || { echo "STRESS FAILED"; tail -40 "$HERE/.stress-${TAG}-b${BATCHNO}.log"; exit 1; }
  grep -E "^Total (partitions|errors)|^Op rate|^Row rate" "$HERE/.stress-${TAG}-b${BATCHNO}.log" | sed 's/^/      /'
  "$CAS/bin/nodetool" flush ws0 events
  free -h | sed -n 2p | sed 's/^/      mem: /'
  START=$((END + 1))
done

# ---- flush + compact to a single SSTable ------------------------------------
echo "### nodetool flush + nodetool compact (DISCLOSURE: yields ONE SSTable, so a scan"
echo "###   is a single sequential pass and k-way merge is removed from the picture)"
"$CAS/bin/nodetool" flush ws0 events
time "$CAS/bin/nodetool" compact ws0 events

DIR=$(dirname "$(ls -S /home/ubuntu/ws0/cassandra-data/data/ws0/events-*/*-Data.db | head -1)")
echo "### sstable dir: $DIR"
ls -l "$DIR"
NDATA=$(ls "$DIR"/*-Data.db | wc -l)
echo "### Data.db count in dir = $NDATA  (1 == single SSTable)"

ROWS=$(python3 "$HERE/cql.py" "SELECT count(*) FROM ws0.events" 2>/dev/null | tail -1)
echo "### exact row count (SELECT count(*)) = $ROWS"
python3 "$HERE/measure-sstable.py" "$DIR"/*-CompressionInfo.db "$ROWS" | tee "$HERE/.measure-${TAG}.txt"
