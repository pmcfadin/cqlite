#!/usr/bin/env bash
#
# Issue #2600 — merge-egress channel-backpressure characterization sweep.
#
# Drives `tools/flight-loadgen` (issue #2418) against a LOCAL `cqlite-flight`
# server across a concurrency ramp {8,32,80} x {full,limit-k} x {1,2 tables},
# recreating the round-12 (#2367) egress-depth backpressure locally
# (peak `cqlite.merge.egress_channel_depth` >= 1000).
#
# THROUGHPUT / LATENCY (qps, p50, p99) are captured directly from the loadgen
# JSONL and need NO code change — this script is a fully reproducible harness
# against a stock release build.
#
# EGRESS DEPTH (`cqlite.merge.egress_channel_depth`) is a process-global gauge
# exported only via OTLP; there is no public in-process accessor. To capture a
# high-resolution depth timeseries WITHOUT standing up an OTLP collector, the
# characterization run used a small, NON-COMMITTED instrumentation patch (see
# docs/architecture/flight-egress-backpressure-2026-07-19.md §Methodology):
#   * cqlite-core .../merge/channel_depth.rs: `pub(super) fn depth_snapshot()`
#     returning `DEPTH.load(Relaxed)`;
#   * merge/mod.rs: a `pub fn egress_channel_depth_snapshot()` re-export;
#   * cqlite-flight/src/main.rs: a thread that, when CQLITE_EGRESS_SAMPLE_FILE is
#     set, appends `elapsed_ms,depth,peak` every 15ms.
# That patch was reverted before commit. Re-apply it locally to reproduce the
# depth column; without it the sweep still reproduces the flat-throughput /
# rising-latency saturation signature that IS the attribution evidence.
#
# Usage:
#   test-data/scripts/egress-backpressure-2600/run-sweep.sh
#
# Env:
#   DATASETS_ROOT  (default: $HOME/local_projects/cqlite/test-data/datasets)
#   KLIMIT         --max-concurrent-scans (default 128; high so admission is NOT
#                  the limiter, matching the field's admission=12/64 observation)
#   DUR            per-cell hold time (default 8s)
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
DATASETS_ROOT="${DATASETS_ROOT:-$HOME/local_projects/cqlite/test-data/datasets}"
SS="$DATASETS_ROOT/sstables"
FLIGHT="$REPO/target/release/cqlite-flight"
LG="$REPO/target/release/flight-loadgen"
PORT="${PORT:-8899}"
KLIMIT="${KLIMIT:-128}"
DUR="${DUR:-8s}"
OUT="${OUT:-/tmp/egress-sweep-2600}"
mkdir -p "$OUT"

[ -x "$FLIGHT" ] || { echo "build first: cargo build --release -p cqlite-flight -p flight-loadgen"; exit 1; }
[ -d "$SS" ]     || { echo "missing SSTables under $SS (fetch-datasets.sh)"; exit 1; }

field() { sed "s/.*\(\"$1\":[0-9.]*\).*/\1/;t;s/.*//" ; }

run_cell() {
  local name="$1" ticket="$2" threads="$3" shape="$4" extra="$5"
  pkill -f "release/cqlite-flight" 2>/dev/null; sleep 1
  local samp="$OUT/$name.depth.csv"
  CQLITE_EGRESS_SAMPLE_FILE="$samp" "$FLIGHT" --data-dir "$SS" \
     --listen "127.0.0.1:$PORT" --max-concurrent-scans "$KLIMIT" \
     > "$OUT/$name.server.log" 2>&1 &
  sleep 2
  "$LG" --endpoint "http://127.0.0.1:$PORT" --ticket-template "$ticket" \
     --ramp "$threads" --step-duration "$DUR" --shape "$shape" $extra \
     --round "$name" --out "$OUT/$name.loadgen.jsonl" > "$OUT/$name.lg.log" 2>&1
  pkill -f "release/cqlite-flight" 2>/dev/null; sleep 1
  local rec peak
  rec="$(cat "$OUT/$name.loadgen.jsonl" 2>/dev/null)"
  peak="$(cut -d, -f2 "$samp" 2>/dev/null | sort -n | tail -1)"
  printf '%-22s thr=%-3s shape=%-8s peak_depth=%-7s qps=%-9s p50=%-8s p99=%-8s\n' \
     "$name" "$threads" "$shape" "${peak:-NA(no-instr)}" \
     "$(echo "$rec" | field qps)" "$(echo "$rec" | field p50)" "$(echo "$rec" | field p99)"
}

T="$HERE"
echo "=== egress backpressure sweep  KLIMIT=$KLIMIT DUR=$DUR ==="
run_cell simple_full_8  "$T/simple_table.ticket.json" 8  full    ""
run_cell simple_full_32 "$T/simple_table.ticket.json" 32 full    ""
run_cell simple_full_80 "$T/simple_table.ticket.json" 80 full    ""
run_cell simple_lim_8   "$T/simple_table.ticket.json" 8  limit-k "--limit-k 100"
run_cell simple_lim_32  "$T/simple_table.ticket.json" 32 limit-k "--limit-k 100"
run_cell simple_lim_80  "$T/simple_table.ticket.json" 80 limit-k "--limit-k 100"
run_cell sensor_full_80 "$T/sensor_data.ticket.json"  80 full    ""
echo "raw per-cell data under $OUT"
