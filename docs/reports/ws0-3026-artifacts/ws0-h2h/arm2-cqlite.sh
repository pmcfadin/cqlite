#!/usr/bin/env bash
# Arm 2 of the WS0 head-to-head (CQLite #3026): CQLite over the IDENTICAL
# SSTable bytes Cassandra reads (sha256 of Data.db proven equal, both arms).
#
# CPU-WIDE perf counting (`perf stat -C <cpus>`), NOT per-task (`-p`/workload).
# Measured on this box: per-task counting of this workload costs >2x wall time
# (163k rows/s metered vs 360k unmetered) because the scan performs ~540k
# context switches and per-task counters are saved/restored on every one. CPU-wide
# counting has no per-switch cost. The SMT sibling of the metered CPU is left
# IDLE and nothing else runs, so CPU-wide counts are this workload's.
#
# Two pinnings, deliberately:
#   -c 2      ONE hardware thread, sibling 10 idle -> valid cycles/row and IPC
#             (summing two busy siblings double-counts wall cycles).
#   -c 2,10   ONE PHYSICAL core, both its hardware threads -> the headline
#             rows/s-per-physical-core. CQLite needs >=2 runnable threads (tokio
#             current_thread + a blocking-pool thread), so restricting it to one
#             hardware thread understates a physical core's throughput.
#
# Usage: arm2-cqlite.sh <label> <warm|cold> <scan|scan-arrow|scan-collect> <cpus> [extra...]
set -euo pipefail

LABEL="${1:?label}"; TEMP="${2:?warm|cold}"; MODE="${3:?mode}"; CPUS="${4:?cpus}"
shift 4
EXTRA=("$@")

OUT=/home/ubuntu/ws0/ws0-results/h2h; mkdir -p "$OUT"
DR=/home/ubuntu/ws0/ws0-h2h/datasets
STAGE=/home/ubuntu/ws0/ws0-h2h/datasets/sstables
SCHEMA=/home/ubuntu/ws0/ws0-h2h/schemas/ws0-events.cql
H=/home/ubuntu/ws0/ws0-cqlite/harness-target/release/ws0-scan-harness

[ "$(cat /proc/sys/kernel/perf_event_paranoid)" = "-1" ] || \
  sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null

ARGS=(--datasets-root "$DR" --stage-dir "$STAGE" --keyspace ws0 --table events
      --schema "$SCHEMA" --mode "$MODE" --passes 1 --no-fold "${EXTRA[@]}")

if [ "$TEMP" = "cold" ]; then
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
else
  taskset -c "$CPUS" "$H" "${ARGS[@]}" >/dev/null 2>&1   # untimed warm pre-pass
fi

STAT="$OUT/perf-$LABEL.txt"
perf stat -x, -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent \
  -C "$CPUS" -o "$STAT" -- taskset -c "$CPUS" "$H" "${ARGS[@]}" \
  > "$OUT/scan-$LABEL.json" 2> "$OUT/scan-$LABEL.err"

python3 - "$STAT" "$OUT/scan-$LABEL.json" "$LABEL" "$TEMP" "$MODE" "$CPUS" \
  > "$OUT/summary-$LABEL.txt" <<'PY'
import json,sys
stat,scan,label,temp,mode,cpus=sys.argv[1:]
d=json.load(open(scan)); nc=len(cpus.split(','))
c={}
for line in open(stat):
    if line.startswith('#') or not line.strip(): continue
    f=line.split(',')
    if len(f)>2 and f[0].strip():
        try: c[f[2].strip()]=c.get(f[2].strip(),0)+int(f[0])
        except ValueError: pass
rows=d['rows_per_pass']; secs=d['cold_secs']
cyc=c.get('cycles',0); ins=c.get('instructions',0)
lin=c.get('l2_lines_in.all',0); lout=c.get('l2_lines_out.non_silent',0)
i0,i1=d['proc_io_start'],d['proc_io_end']
out={
 "engine":"cqlite","label":label,"temp":temp,"surface":mode,
 "pinned_cpus":cpus,"hw_threads":nc,
 "rows":rows,"digest":d['digest'],
 "scan_secs":secs,"rows_per_sec":rows/secs,
 "uncompressed_MB_per_s":rows*692.70/secs/1e6,
 "compressed_MB_per_s":rows*195.96/secs/1e6,
 "arrow_payload_bytes":d['arrow_payload_bytes_per_pass'],
 "cycles":cyc,"instructions":ins,
 "cycles_per_row":cyc/rows if rows else None,
 "instructions_per_row":ins/rows if rows else None,
 "IPC":ins/cyc if cyc else None,
 "l2_lines_in_all":lin,
 "mem_traffic_in_bytes_per_row":lin*64/rows if rows else None,
 "l2_evict_bytes_per_row":lout*64/rows if rows else None,
 "peak_rss_kib":d['peak_rss_kib'],
 "proc_io_delta":{k:i1.get(k,0)-i0.get(k,0) for k in ("rchar","read_bytes","syscr")},
}
if nc>1:
    out["cycles_per_row_WARNING"]="counted across %d SMT siblings; wall cycles are double-counted. Use the -c 2 run for cycles/row and IPC."%nc
print(json.dumps(out,indent=1))
PY
cat "$OUT/summary-$LABEL.txt"
