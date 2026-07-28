#!/usr/bin/env bash
# Arm 1 (final form) of the WS0 head-to-head, CQLite #3026.
#
# Cassandra 5.0.8, daemon pinned to <cpus>, metered with CPU-WIDE perf, driven by
# a SHARDED client so the metered daemon core is the bottleneck rather than the
# Python driver. `daemon_core_utilization` is reported so a client-bound run is
# visible instead of silently understating Cassandra.
#
# Usage: arm1c-cassandra.sh <label> <warm|cold> <count|rows> <cpus> <shards> [inflight] [ranges]
set -euo pipefail
LABEL="${1:?}"; TEMP="${2:?}"; MODE="${3:?}"; CPUS="${4:?}"; SHARDS="${5:-6}"
INFLIGHT="${6:-8}"; RANGES="${7:-512}"
OUT=/home/ubuntu/ws0/ws0-results/h2h; mkdir -p "$OUT"

PID=""
for p in $(pgrep -x java); do
  tr '\0' ' ' < "/proc/$p/cmdline" | grep -q 'org.apache.cassandra.service.CassandraDaemon' && { PID=$p; break; }
done
[ -n "$PID" ] || { echo "no cassandra daemon JVM"; exit 1; }
NT=$(ls /proc/$PID/task | wc -l); [ "$NT" -gt 20 ] || { echo "pid $PID not the daemon"; exit 1; }

[ "$(cat /proc/sys/kernel/perf_event_paranoid)" = "-1" ] || \
  sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null
sudo -n taskset -acp "$CPUS" "$PID" >/dev/null 2>&1 || taskset -acp "$CPUS" "$PID" >/dev/null
echo "daemon pid=$PID cpus=$(taskset -cp "$PID" | sed 's/.*: //') threads=$NT"

if [ "$TEMP" = "cold" ]; then
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
else
  bash /home/ubuntu/ws0/ws0-h2h/shardrun.sh count "$SHARDS" "$INFLIGHT" "$RANGES" prewarm >/dev/null 2>&1
fi

C0=$(awk '{print $14+$15}' /proc/$PID/stat); IO0=$(cat /proc/$PID/io)
perf stat -x, -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent \
  -C "$CPUS" -o "$OUT/perf-$LABEL.txt" -- \
  bash /home/ubuntu/ws0/ws0-h2h/shardrun.sh "$MODE" "$SHARDS" "$INFLIGHT" "$RANGES" "$LABEL" \
  > "$OUT/scan-$LABEL.json" 2> "$OUT/scan-$LABEL.err"
C1=$(awk '{print $14+$15}' /proc/$PID/stat); IO1=$(cat /proc/$PID/io)
HZ=$(getconf CLK_TCK)

python3 - "$OUT/perf-$LABEL.txt" "$OUT/scan-$LABEL.json" "$C0" "$C1" "$HZ" "$TEMP" "$CPUS" \
  > "$OUT/summary-$LABEL.txt" <<PYEOF
import json,sys
stat,scan,c0,c1,hz,temp,cpus=sys.argv[1:]
io0="""$IO0"""; io1="""$IO1"""
def pio(s):
    d={}
    for l in s.strip().splitlines():
        k,v=l.split(':'); d[k.strip()]=int(v)
    return d
i0,i1=pio(io0),pio(io1)
nc=len(cpus.split(','))
d=json.loads([l for l in open(scan) if l.strip().startswith('{')][-1])
c={}
for line in open(stat):
    if line.startswith('#') or not line.strip(): continue
    f=line.split(',')
    if len(f)>2 and f[0].strip():
        try: c[f[2].strip()]=c.get(f[2].strip(),0)+int(f[0])
        except ValueError: pass
rows=d['rows']; cyc=c.get('cycles',0); ins=c.get('instructions',0)
lin=c.get('l2_lines_in.all',0); lout=c.get('l2_lines_out.non_silent',0)
cpu_s=(int(c1)-int(c0))/float(hz)
util=cpu_s/d['wall_secs']/nc
print(json.dumps({
 "engine":"cassandra-5.0.8","label":d['label'],"temp":temp,"surface":d['surface'],
 "pinned_cpus":cpus,"hw_threads":nc,"client_shards":d['client_shards'],
 "inflight":d['inflight'],"ranges":d['ranges'],
 "rows":rows,"wall_secs":d['wall_secs'],
 "rows_per_sec_wall":d['rows_per_sec_wall'],
 "daemon_cpu_secs":cpu_s,"daemon_core_utilization":util,
 "client_bound": util < 0.9,
 "rows_per_daemon_cpu_sec":rows/cpu_s if cpu_s else None,
 "uncompressed_MB_per_s":d['uncompressed_MB_per_s'],
 "compressed_MB_per_s":d['compressed_MB_per_s'],
 "cycles":cyc,"instructions":ins,
 "cycles_per_row":cyc/rows if rows else None,
 "instructions_per_row":ins/rows if rows else None,
 "IPC":ins/cyc if cyc else None,
 "mem_traffic_in_bytes_per_row":lin*64/rows if rows else None,
 "l2_evict_bytes_per_row":lout*64/rows if rows else None,
 "daemon_io_delta":{k:i1[k]-i0.get(k,0) for k in ("rchar","read_bytes","syscr")},
 "cycles_note": None if nc==1 else
   "counted across %d SMT siblings; wall cycles double-counted - take cycles/row and IPC from the -c 2 run"%nc,
}, indent=1))
PYEOF
cat "$OUT/summary-$LABEL.txt"
