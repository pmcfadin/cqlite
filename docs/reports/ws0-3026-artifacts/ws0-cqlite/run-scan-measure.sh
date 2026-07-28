#!/usr/bin/env bash
# WS0 (#3026) scan-throughput measurement runner — validated 2026-07-27 on c7i.4xlarge.
#
# Emits, for ONE (table, surface, pinning) point: rows/s, cycles/row, IPC, and
# bytes-of-memory-traffic/row. Read the header block in this file before
# believing any number it prints.
#
# Usage:
#   ./run-scan-measure.sh <keyspace> <table> <schema.cql> [mode] [passes] [cpus]
# e.g.
#   ./run-scan-measure.sh test_basic simple_table basic-types.cql scan 200 2
set -euo pipefail

KS="${1:?keyspace}"; TBL="${2:?table}"; SCHEMA="${3:?schema.cql}"
MODE="${4:-scan}"; PASSES="${5:-200}"; CPUS="${6:-2}"

ROOT="${CQLITE_DATASETS_ROOT:-/home/ubuntu/workspace/repo/test-data/datasets}"
HARNESS=/home/ubuntu/ws0/ws0-cqlite/harness-target/release/ws0-scan-harness

# perf_event_paranoid reverts to 4 on this box (observed twice). Re-assert it.
if [ "$(cat /proc/sys/kernel/perf_event_paranoid)" != "-1" ]; then
  sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null
fi

# --no-fold: the harness's anti-elision digest costs +28.6% cycles / +17.7%
# offcore traffic (measured). Report --no-fold; use the folded run only to prove
# the values were really materialized (identical digest across passes).
STAT=$(mktemp)
taskset -c "$CPUS" perf stat -x, \
  -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent \
  -o "$STAT" \
  -- "$HARNESS" --datasets-root "$ROOT" --keyspace "$KS" --table "$TBL" \
       --schema "$SCHEMA" --mode "$MODE" --passes "$PASSES" --no-fold \
  > /tmp/ws0-harness.json 2>/tmp/ws0-harness.err

ROWS=$(python3 -c "import json,sys;print(json.load(open('/tmp/ws0-harness.json'))['perf_denominator_rows'])")
RPS=$(python3 -c "import json;d=json.load(open('/tmp/ws0-harness.json'));print(round(d['warm_rows_per_sec'],1))")
DATA_BYTES=$(find "$ROOT/sstables/$KS/$TBL"-* -name '*-Data.db' -printf '%s\n' 2>/dev/null | paste -sd+ | bc)
ROWS_PER_PASS=$(python3 -c "import json;print(json.load(open('/tmp/ws0-harness.json'))['rows_per_pass'])")

python3 - "$STAT" "$ROWS" "$RPS" "$DATA_BYTES" "$ROWS_PER_PASS" "$KS.$TBL" "$MODE" "$CPUS" <<'PY'
import sys
stat, rows, rps, data_bytes, rpp, table, mode, cpus = sys.argv[1:]
rows=int(rows); rpp=int(rpp); data_bytes=int(data_bytes or 0)
c={}
for line in open(stat):
    if line.startswith('#') or not line.strip(): continue
    f=line.split(',')
    if len(f)>2 and f[0].strip():
        try: c[f[2].strip()]=int(f[0])
        except ValueError: pass
cyc=c.get('cycles',0); ins=c.get('instructions',0)
lin=c.get('l2_lines_in.all',0); lout=c.get('l2_lines_out.non_silent',0)
print(f"table={table} mode={mode} cpus={cpus} rows_measured={rows} rows_per_scan={rpp}")
print(f"  warm_rows_per_sec   = {rps}")
print(f"  cycles_per_row      = {cyc/rows:,.0f}")
print(f"  instructions_per_row= {ins/rows:,.0f}")
print(f"  IPC                 = {ins/cyc:.2f}" if cyc else "  IPC = n/a")
print(f"  mem_traffic_IN_per_row  = {lin*64/rows:,.0f} bytes   [PROXY: l2_lines_in.all*64]")
print(f"  L2_evictions_per_row    = {lout*64/rows:,.0f} bytes  [NOT dirty-writeback-separable]")
if data_bytes:
    print(f"  Data.db bytes/row       = {data_bytes/rpp:,.0f}")
    print(f"  traffic amplification   = {(lin*64/rows)/(data_bytes/rpp):.1f}x on-disk bytes")
gbs = lin*64/rows*float(rps)/1e9
print(f"  offcore read bandwidth  = {gbs:.2f} GB/s  (single-core DRAM ceiling measured 10.84 GB/s -> {gbs/10.84*100:.1f}%)")
print("  NOTE: l2_lines_in.all*64 is OFFCORE (L2-fill) traffic, an UPPER BOUND on")
print("        DRAM traffic. It equals DRAM traffic only when the working set")
print("        exceeds the 105 MiB L3. Uncore IMC counters do NOT exist in this VM.")
PY
rm -f "$STAT"
