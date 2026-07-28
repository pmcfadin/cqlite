#!/usr/bin/env bash
# Arm 2b: CQLite Arrow Flight server-direct, over the SAME staged SSTable bytes.
# The honest counterpart to Cassandra's serializing native protocol: the server
# decodes SSTable -> Arrow -> gRPC/IPC frames and streams them to a real client.
#
# Server pinned to <cpus>; loadgen client on a DIFFERENT physical core set, so
# CPU-wide perf on <cpus> attributes only server work. Concurrency 1 = one
# full-ring scan at a time, matching the single-stream bare-scan measurement.
#
# Usage: arm2-flight.sh <label> <warm|cold> <cpus> <step-seconds>
set -euo pipefail
LABEL="${1:?}"; TEMP="${2:?}"; CPUS="${3:?}"; STEP="${4:-90}"
OUT=/home/ubuntu/ws0/ws0-results/h2h; mkdir -p "$OUT"
BIN=/home/ubuntu/workspace/wt-3026/target/release
STAGE=/home/ubuntu/ws0/ws0-h2h/datasets/sstables
TPL=/home/ubuntu/ws0/ws0-h2h/ws0-events-template.json
CLIENT_CPUS=4-7,12-15
[ "$(cat /proc/sys/kernel/perf_event_paranoid)" = "-1" ] || \
  sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null

pkill -x cqlite-flight 2>/dev/null || true; sleep 1
[ "$TEMP" = "cold" ] && { sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null; }

taskset -c "$CPUS" "$BIN/cqlite-flight" --data-dir "$STAGE" --listen 127.0.0.1:8815 \
  > "$OUT/flight-$LABEL.server.log" 2>&1 &
SRV=$!
for i in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8815) 2>/dev/null && break; sleep 1; done
echo "flight server pid=$SRV cpus=$CPUS"

if [ "$TEMP" = "warm" ]; then
  taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" --endpoint http://127.0.0.1:8815 \
    --ticket-template "$TPL" --shape full --ramp 1 --step-duration 45s \
    --round prewarm --out /dev/null > /dev/null 2>&1 || true
fi

C0=$(awk '{print $14+$15}' /proc/$SRV/stat); IO0=$(cat /proc/$SRV/io)
perf stat -x, -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent \
  -C "$CPUS" -o "$OUT/perf-$LABEL.txt" -- \
  taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" --endpoint http://127.0.0.1:8815 \
    --ticket-template "$TPL" --shape full --ramp 1 --step-duration "${STEP}s" \
    --round "$LABEL" --out "$OUT/flight-$LABEL.jsonl" > "$OUT/flight-$LABEL.log" 2>&1
C1=$(awk '{print $14+$15}' /proc/$SRV/stat); IO1=$(cat /proc/$SRV/io)
HZ=$(getconf CLK_TCK)
kill $SRV 2>/dev/null || true; sleep 1; kill -9 $SRV 2>/dev/null || true

python3 - "$OUT/perf-$LABEL.txt" "$OUT/flight-$LABEL.jsonl" "$LABEL" "$TEMP" "$CPUS" "$C0" "$C1" "$HZ" \
  <<PYEOF > "$OUT/summary-$LABEL.txt"
import json,sys
stat,jsonl,label,temp,cpus,c0,c1,hz=sys.argv[1:]
io0="""$IO0"""; io1="""$IO1"""
def pio(s):
    d={}
    for l in s.strip().splitlines():
        k,v=l.split(':'); d[k.strip()]=int(v)
    return d
i0,i1=pio(io0),pio(io1)
rec=[json.loads(l) for l in open(jsonl) if l.strip()][-1]
c={}
for line in open(stat):
    if line.startswith('#') or not line.strip(): continue
    f=line.split(',')
    if len(f)>2 and f[0].strip():
        try: c[f[2].strip()]=c.get(f[2].strip(),0)+int(f[0])
        except ValueError: pass
ROWS_PER_SCAN=3999890
rows=rec['rows_total']; nc=len(cpus.split(','))
cyc=c.get('cycles',0); ins=c.get('instructions',0)
lin=c.get('l2_lines_in.all',0); lout=c.get('l2_lines_out.non_silent',0)
srv_cpu=(int(c1)-int(c0))/float(hz)
p50=rec['latency_ms']['p50']/1000.0
out={"engine":"cqlite-flight","label":label,"temp":temp,"pinned_cpus":cpus,
 "hw_threads":nc,"requests_ok":rec['requests_ok'],"requests_error":rec['requests_error'],
 "rows_total":rows,"rows_per_scan_expected":ROWS_PER_SCAN,
 "rows_per_scan_observed":rows/rec['requests_ok'] if rec['requests_ok'] else None,
 "step_seconds":rec['duration_s'],
 "rows_per_sec_step":rec['rows_per_s'],
 "rows_per_sec_from_p50_latency":ROWS_PER_SCAN/p50 if p50 else None,
 "full_scan_p50_secs":p50,"full_scan_p99_secs":rec['latency_ms']['p99']/1000.0,
 "arrow_capacity_bytes_per_s":rec['bytes_per_s'],
 "uncompressed_MB_per_s":rec['rows_per_s']*692.70/1e6,
 "compressed_MB_per_s":rec['rows_per_s']*195.96/1e6,
 "server_cpu_secs":srv_cpu,
 "server_cpu_utilization_of_pinned_set":srv_cpu/rec['duration_s']/nc,
 "rows_per_server_cpu_sec":rows/srv_cpu if srv_cpu else None,
 "cycles":cyc,"instructions":ins,
 "cycles_per_row":cyc/rows if rows else None,
 "instructions_per_row":ins/rows if rows else None,
 "IPC":ins/cyc if cyc else None,
 "mem_traffic_in_bytes_per_row":lin*64/rows if rows else None,
 "l2_evict_bytes_per_row":lout*64/rows if rows else None,
 "server_io_delta":{k:i1[k]-i0.get(k,0) for k in ("rchar","read_bytes","syscr")}}
if nc>1: out["cycles_per_row_WARNING"]="counted across %d SMT siblings; wall cycles double-counted"%nc
print(json.dumps(out,indent=1))
PYEOF
cat "$OUT/summary-$LABEL.txt"
