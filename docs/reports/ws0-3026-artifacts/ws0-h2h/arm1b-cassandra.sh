#!/usr/bin/env bash
# Arm 1 of the WS0 head-to-head (CQLite #3026): stock Cassandra 5.0.8 scan.
#
# Meters the DAEMON, never the client:
#   * daemon pinned to ONE hardware thread (CPU 2). Its SMT sibling (CPU 10) is
#     left IDLE, so CPU 2 has the whole physical core's execution resources and
#     `cycles` is counted on exactly one thread -- summing two busy siblings
#     would double-count wall cycles.
#   * client pinned to CPUs 4-7 (a different physical core set), so client CPU
#     never steals from the metered core.
#   * counters come from CPU-wide `perf stat -C <cpus>`, so client wall-time and
#     Python driver cost cannot leak into cycles/row or IPC.
#
# Usage: arm1-cassandra.sh <label> <warm|cold> <count|rows> [inflight] [ranges]
set -euo pipefail

LABEL="${1:?label}"; TEMP="${2:?warm|cold}"; MODE="${3:?count|rows}"
CPUS="${4:?cpus}"; INFLIGHT="${5:-8}"; RANGES="${6:-512}"
OUT=/home/ubuntu/ws0/ws0-results/h2h
mkdir -p "$OUT"

# NOT `pgrep -f CassandraDaemon`: the shell that launched the node still has that
# string in its own cmdline, so -f matches the WRAPPER first and every counter
# then reads 0. Select the JVM by executable name and confirm the main class.
PID=""
for p in $(pgrep -x java); do
  if tr '\0' ' ' < "/proc/$p/cmdline" | grep -q 'org.apache.cassandra.service.CassandraDaemon'; then
    PID=$p; break
  fi
done
[ -n "$PID" ] || { echo "no cassandra daemon JVM found"; exit 1; }
NT=$(ls /proc/$PID/task | wc -l)
[ "$NT" -gt 20 ] || { echo "pid $PID has only $NT threads - not the daemon"; exit 1; }

# perf_event_paranoid silently reverts to 4 on this box. Re-assert before EVERY run.
[ "$(cat /proc/sys/kernel/perf_event_paranoid)" = "-1" ] || \
  sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null

# Pin every existing daemon thread to CPU 2. New threads inherit the creator's mask.
sudo -n taskset -acp "$CPUS" "$PID" >/dev/null 2>&1 || taskset -acp "$CPUS" "$PID" >/dev/null
echo "daemon pid=$PID affinity=$(taskset -cp "$PID" | sed 's/.*: //') threads=$(ls /proc/$PID/task | wc -l)"

if [ "$TEMP" = "cold" ]; then
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  echo "page cache dropped"
else
  # Warm: one full untimed pre-pass so every Data.db chunk is page-cache resident.
  taskset -c 4-7 python3 /home/ubuntu/ws0/ws0-h2h/cas-scan.py \
     --mode count --inflight "$INFLIGHT" --ranges "$RANGES" --label prewarm \
     >/dev/null 2>&1 || true
  echo "warm pre-pass done"
fi

snap() { # $1=tag ; emits "utime stime rchar read_bytes syscr"
  awk '{print $14, $15}' /proc/$PID/stat
  awk -F': ' '/^rchar|^read_bytes|^syscr/{print $1"="$2}' /proc/$PID/io | paste -sd' '
}
CPU0=$(awk '{print $14+$15}' /proc/$PID/stat)
IO0=$(cat /proc/$PID/io)

STAT="$OUT/perf-$LABEL.txt"
# CPU-WIDE counting (-C), NOT per-task (-p): per-task counters are saved/restored
# on every context switch, which on this box cost >2x wall time for a
# switch-heavy scan. Nothing but the pinned daemon runs on $CPUS, and the SMT
# sibling is idle when CPUS=2, so CPU-wide counts are the daemon's work.
perf stat -x, -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent \
   -C "$CPUS" -o "$STAT" -- \
   taskset -c 4-7 python3 /home/ubuntu/ws0/ws0-h2h/cas-scan.py \
   --mode "$MODE" --inflight "$INFLIGHT" --ranges "$RANGES" --label "$LABEL" \
   > "$OUT/scan-$LABEL.json" 2> "$OUT/scan-$LABEL.err"
RC=$?

CPU1=$(awk '{print $14+$15}' /proc/$PID/stat)
IO1=$(cat /proc/$PID/io)
HZ=$(getconf CLK_TCK)

python3 - "$STAT" "$OUT/scan-$LABEL.json" "$CPU0" "$CPU1" "$HZ" "$TEMP" "$CPUS" \
  <<PYEOF > "$OUT/summary-$LABEL.txt"
import json,sys
stat,scan,c0,c1,hz,temp,cpus=sys.argv[1:]
nc=len(cpus.split(','))
io0="""$IO0"""; io1="""$IO1"""
def parse_io(s):
    d={}
    for l in s.strip().splitlines():
        k,v=l.split(':'); d[k.strip()]=int(v)
    return d
i0,i1=parse_io(io0),parse_io(io1)
d=json.load(open(scan))
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
print(json.dumps({
 "engine":"cassandra-5.0.8","label":d['label'],"temp":temp,"pinned_cpus":cpus,"hw_threads":nc,
 "daemon_core_utilization":cpu_s/d['wall_secs']/nc,
 "surface":d['surface'],"inflight":d['inflight'],"ranges":d['ranges'],
 "rows":rows,"wall_secs":d['wall_secs'],
 "rows_per_sec_wall":d['rows_per_sec_wall'],
 "daemon_cpu_secs":cpu_s,
 "rows_per_daemon_cpu_sec":rows/cpu_s if cpu_s else None,
 "uncompressed_MB_per_s":d['uncompressed_MB_per_s'],
 "compressed_MB_per_s":d['compressed_MB_per_s'],
 "cycles":cyc,"instructions":ins,
 "cycles_per_row":cyc/rows if rows else None,
 "instructions_per_row":ins/rows if rows else None,
 "IPC":ins/cyc if cyc else None,
 "l2_lines_in_all":lin,
 "mem_traffic_in_bytes_per_row":lin*64/rows if rows else None,
 "l2_evict_bytes_per_row":lout*64/rows if rows else None,
 "daemon_io_delta":{k:i1[k]-i0.get(k,0) for k in ("rchar","read_bytes","syscr")},
 "cycles_note": None if nc==1 else
   "counted across %d SMT siblings; wall cycles are double-counted - take cycles/row and IPC from the 1-hw-thread (-c 2) run"%nc,
}, indent=1))
PYEOF
cat "$OUT/summary-$LABEL.txt"
exit $RC
