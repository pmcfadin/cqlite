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
#     set, appends `elapsed_ms,depth,peak` every 15ms (col2=instantaneous depth,
#     col3=running monotonic peak — this script reports col3, the published peak).
# That patch was reverted before commit. Re-apply it locally to reproduce the
# depth column; without it the sweep still reproduces the flat-throughput /
# rising-latency saturation signature that IS the attribution evidence (depth
# cells then print `NA(no-instr)`).
#
# Usage:
#   test-data/scripts/egress-backpressure-2600/run-sweep.sh
#
# Env:
#   CQLITE_DATASETS_ROOT  datasets root (default: $REPO/test-data/datasets)
#   KLIMIT   --max-concurrent-scans (default 128; high so admission is NOT the
#            limiter, matching the field's admission=12/64. Set KLIMIT=64 to
#            reproduce the `simple_full_80_K64` diagnostic row.)
#   DUR      per-cell hold time (default 8s)
#   TWO_TABLE=1  additionally run the 2-table concurrent cell (simple x sensor)
set -u
set -o pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$REPO/test-data/datasets}"
SS="$DATASETS_ROOT/sstables"
FLIGHT="$REPO/target/release/cqlite-flight"
LG="$REPO/target/release/flight-loadgen"
PORT="${PORT:-8899}"
KLIMIT="${KLIMIT:-128}"
DUR="${DUR:-8s}"
OUT="${OUT:-/tmp/egress-sweep-2600}"
mkdir -p "$OUT"

[ -x "$FLIGHT" ] || { echo "build first: cargo build --release -p cqlite-flight -p flight-loadgen" >&2; exit 1; }
[ -x "$LG" ]     || { echo "build first: cargo build --release -p cqlite-flight -p flight-loadgen" >&2; exit 1; }
[ -d "$SS" ]     || { echo "missing SSTables under $SS (fetch-datasets.sh)" >&2; exit 1; }

# Extract a single numeric JSON field value (no key, no quotes).
numfield() { sed -n "s/.*\"$1\":\([0-9][0-9.]*\).*/\1/p"; }

# Running peak of the depth CSV = max of col3 (monotonic → also its last value).
peak_of() { [ -f "$1" ] && cut -d, -f3 "$1" 2>/dev/null | sort -n | tail -1; }

# Block until the server accepts a TCP connection on $PORT, or fail loudly.
wait_for_port() {
  local tries=0
  while ! (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; do
    tries=$((tries + 1))
    if [ "$tries" -ge 100 ]; then
      echo "FATAL: server never bound 127.0.0.1:$PORT (see $1)" >&2
      return 1
    fi
    sleep 0.1
  done
  exec 3>&- 2>/dev/null || true
}

SRV_PID=""
start_server() {
  local sample="$1" klimit="$2" log="$3"
  CQLITE_EGRESS_SAMPLE_FILE="$sample" "$FLIGHT" --data-dir "$SS" \
     --listen "127.0.0.1:$PORT" --max-concurrent-scans "$klimit" > "$log" 2>&1 &
  SRV_PID=$!
  wait_for_port "$log" || { stop_server; return 1; }
}
stop_server() {
  [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null && wait "$SRV_PID" 2>/dev/null
  SRV_PID=""
}

run_cell() {
  local name="$1" ticket="$2" threads="$3" shape="$4" extra="$5"
  local samp="$OUT/$name.depth.csv"
  start_server "$samp" "$KLIMIT" "$OUT/$name.server.log" || { echo "$name: SERVER-FAIL" >&2; return 1; }
  if ! "$LG" --endpoint "http://127.0.0.1:$PORT" --ticket-template "$ticket" \
       --ramp "$threads" --step-duration "$DUR" --shape "$shape" $extra \
       --round "$name" --out "$OUT/$name.loadgen.jsonl" > "$OUT/$name.lg.log" 2>&1; then
    echo "$name: LOADGEN-FAIL (see $OUT/$name.lg.log)" >&2; stop_server; return 1
  fi
  stop_server
  local rec; rec="$(cat "$OUT/$name.loadgen.jsonl" 2>/dev/null)"
  printf '%-22s thr=%-3s shape=%-8s peak_depth=%-8s qps=%-9s p50=%-8s p99=%-8s\n' \
     "$name" "$threads" "$shape" "$(peak_of "$samp" || echo 'NA(no-instr)')" \
     "$(echo "$rec" | numfield qps)" "$(echo "$rec" | numfield p50)" "$(echo "$rec" | numfield p99)"
}

# 2-table concurrent cell: two loadgens (40 thr each) against different tables.
run_two_table() {
  local samp="$OUT/two_table_80.depth.csv"
  start_server "$samp" "$KLIMIT" "$OUT/two_table_80.server.log" || { echo "two_table_80: SERVER-FAIL" >&2; return 1; }
  "$LG" --endpoint "http://127.0.0.1:$PORT" --ticket-template "$HERE/simple_table.ticket.json" \
     --ramp 40 --step-duration "$DUR" --shape full --round t2a --out "$OUT/t2a.jsonl" > "$OUT/t2a.lg.log" 2>&1 &
  local a=$!
  "$LG" --endpoint "http://127.0.0.1:$PORT" --ticket-template "$HERE/sensor_data.ticket.json" \
     --ramp 40 --step-duration "$DUR" --shape full --round t2b --out "$OUT/t2b.jsonl" > "$OUT/t2b.lg.log" 2>&1 &
  local b=$!
  wait "$a"; wait "$b"
  stop_server
  printf '%-22s thr=%-3s shape=%-8s peak_depth=%-8s (simple40 + sensor40 concurrent)\n' \
     two_table_80 80 full "$(peak_of "$samp" || echo 'NA(no-instr)')"
}

trap 'stop_server' EXIT

echo "=== egress backpressure sweep  KLIMIT=$KLIMIT DUR=$DUR ==="
run_cell simple_full_8  "$HERE/simple_table.ticket.json" 8  full    ""
run_cell simple_full_32 "$HERE/simple_table.ticket.json" 32 full    ""
run_cell simple_full_80 "$HERE/simple_table.ticket.json" 80 full    ""
run_cell simple_lim_8   "$HERE/simple_table.ticket.json" 8  limit-k "--limit-k 100"
run_cell simple_lim_32  "$HERE/simple_table.ticket.json" 32 limit-k "--limit-k 100"
run_cell simple_lim_80  "$HERE/simple_table.ticket.json" 80 limit-k "--limit-k 100"
run_cell sensor_full_80 "$HERE/sensor_data.ticket.json"  80 full    ""
[ "${TWO_TABLE:-0}" = "1" ] && run_two_table
echo "raw per-cell data under $OUT"
echo "note: the cap32_* rows in data/sweep-results.csv require the reverted"
echo "      STREAMING_CHANNEL_CAPACITY=32 experiment build (see doc §Lever); not stock-runnable."
