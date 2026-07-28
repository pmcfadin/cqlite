#!/usr/bin/env bash
# Run N sharded cas-scan.py clients in parallel, each on its own core.
# Prints one JSON: summed rows, max wall.
MODE=$1; N=$2; INFLIGHT=$3; RANGES=$4; TAG=$5
CORES=(4 5 6 7 12 13 14 15)
T=$(mktemp -d)
for i in $(seq 0 $((N-1))); do
  taskset -c ${CORES[$i]} python3 /home/ubuntu/ws0/ws0-h2h/cas-scan.py --mode "$MODE" \
    --inflight "$INFLIGHT" --ranges "$RANGES" --shards "$N" --shard-index "$i" \
    --label "$TAG-s$i" > "$T/$i.json" 2>/dev/null &
done
wait
python3 - "$T" "$TAG" <<'PY'
import json,sys,glob,os
T,tag=sys.argv[1:]
rs=[json.load(open(f)) for f in glob.glob(os.path.join(T,'*.json'))]
rows=sum(r['rows'] for r in rs); wall=max(r['wall_secs'] for r in rs)
print(json.dumps({"label":tag,"engine":"cassandra-5.0.8","surface":rs[0]['surface'],
 "ranges":rs[0]['ranges'],"inflight":rs[0]['inflight'],"client_shards":len(rs),
 "rows":rows,"wall_secs":wall,"rows_per_sec_wall":rows/wall,
 "uncompressed_MB_per_s":rows*692.70/wall/1e6,"compressed_MB_per_s":rows*195.96/wall/1e6}))
PY
rm -rf "$T"
