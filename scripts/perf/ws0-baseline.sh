#!/usr/bin/env bash
# ws0-baseline.sh — the committed, clean-checkout-runnable Arrow-encode
# measurement rig for CQLite issue #3096 (spec R1/R2).
#
# Measures BOTH arms in ONE session, over the SAME bytes, on the SAME verified
# physical-core sibling pair:
#
#   arm A  bare scan      cqlite_core::Database::execute_streaming  (ws0-scan-bench)
#   arm B  Flight do_get  the real gRPC RPC over loopback           (cqlite-flight + flight-loadgen)
#
# and reports each as rows/s AND cycles/row, warm and cold as SEPARATE claims,
# median of N with the observed spread, setup subtracted from the cycles/row
# denominator, and the row denominator printed beside every figure.
#
# ---------------------------------------------------------------------------
# The traps this rig is built around (spec R2) — do not "simplify" these away
# ---------------------------------------------------------------------------
#
#  1. CPU-WIDE COUNTERS ONLY. Every measurement uses `perf stat -C <cpu-list>`.
#     `perf stat -p` (per-process) measured >2x observer cost on this workload
#     and appears NOWHERE in this rig. There is a self-check below that greps
#     this script for a `-p` form and refuses to run if one appears.
#  2. VERIFIED SIBLING PINNING. The pinned pair is read from
#     `thread_siblings_list` and the run FAILS CLOSED if it is not one physical
#     core's siblings (`lib-cpu.sh`). Never assumed from CPU numbers.
#  3. WARM AND COLD ARE SEPARATE CLAIMS. Never averaged together. Cold does
#     `sync; echo 3 > /proc/sys/vm/drop_caches` before EVERY rep.
#  4. SETUP IS SUBTRACTED, AND SAID SO. Arm A runs `--setup-only` under its own
#     `perf stat` and the driver reports `(cycles_total - cycles_setup) / rows`.
#     Arm B starts and prewarms the server BEFORE the perf window opens, so its
#     setup is outside the window by construction.
#  5. ZERO ROWS IS A FAILURE. Any rep that observes zero rows exits non-zero
#     rather than reporting a measurement.
#  6. NEVER A CPU-SHARE CLAIM. This rig emits rows/s and cycles/row only.
#
# Usage:
#   scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096 [options]
#
# Generate the corpus first:
#   cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out /data/ws0-3096
#
# Full method, caveats and the recorded pinning: docs/reports/ws0-3096-artifacts/measurement-method.md

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=scripts/perf/lib-cpu.sh
source "$HERE/lib-cpu.sh"

CORPUS=""
SERVER_CPUS="2,10"
CLIENT_CPUS="4,12,5,13,6,14,7,15"
REPS=3
TEMPS="warm cold"
ARMS="bypass"
STEP_DURATION="60s"
SCAN_PASSES=1
PORT=18815
OUT_DIR=""
DO_BUILD=1
EVENTS="cycles,instructions"

usage() {
  cat <<EOF
ws0-baseline.sh — issue #3096 same-session Arrow-encode baseline

  --corpus DIR         Corpus root from ws0-corpus-gen (holds ws0/events/). REQUIRED.
  --server-cpus LIST   Pinned physical-core sibling pair for BOTH arms (default $SERVER_CPUS).
  --client-cpus LIST   CPUs for the Flight load generator; must not overlap (default $CLIENT_CPUS).
  --reps N             Reps per (arm, temperature). Median reported, spread printed (default $REPS).
  --temp WHICH         warm | cold | both (default both).
  --arm WHICH          bypass | merge | both (default bypass).
  --step-duration D    Flight loadgen step hold, e.g. 60s (default $STEP_DURATION).
  --scan-passes N      Timed passes per bare-scan rep (default $SCAN_PASSES).
  --port N             Loopback port for the Flight server (default $PORT).
  --out DIR            Results dir (default \$REPO/target/perf-ws0-3096/<timestamp>).
  --no-build           Skip the release build; use the binaries already in target/release.
  -h, --help           This text.

Physical-core sibling pairs on this box:
$(list_sibling_pairs)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --corpus) CORPUS="$2"; shift 2 ;;
    --server-cpus) SERVER_CPUS="$2"; shift 2 ;;
    --client-cpus) CLIENT_CPUS="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --temp)
      case "$2" in
        warm) TEMPS="warm" ;;
        cold) TEMPS="cold" ;;
        both) TEMPS="warm cold" ;;
        *) echo "FATAL: --temp must be warm|cold|both" >&2; exit 2 ;;
      esac; shift 2 ;;
    --arm)
      case "$2" in
        bypass) ARMS="bypass" ;;
        merge) ARMS="merge" ;;
        both) ARMS="bypass merge" ;;
        *) echo "FATAL: --arm must be bypass|merge|both" >&2; exit 2 ;;
      esac; shift 2 ;;
    --step-duration) STEP_DURATION="$2"; shift 2 ;;
    --scan-passes) SCAN_PASSES="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --no-build) DO_BUILD=0; shift ;;
    -h|--help) usage; exit 0 ;;
    # Every unrecognized argument is an ERROR, never ignored: a typo'd flag that
    # is silently dropped produces a measurement of something other than what
    # was asked for, and nothing in the output would say so.
    *) echo "FATAL: unrecognized argument '$1'" >&2; usage >&2; exit 2 ;;
  esac
done

# --- trap 1 self-check: this rig contains no per-process perf invocation ------
# Greps THIS FILE (spec R2's "contains no `perf stat -p` invocation"), so a
# future edit that reaches for `-p` cannot run at all.
if grep -nE 'perf stat[^|]*(-p |--pid)' "${BASH_SOURCE[0]}" | grep -v 'self-check' >/dev/null 2>&1; then
  echo "FATAL: this script contains a per-process 'perf stat -p' invocation." >&2
  echo "       Per-process counting measured >2x observer cost on this workload;" >&2
  echo "       CPU-wide 'perf stat -C <cpu-list>' is mandatory (issue #3096 spec R2)." >&2
  exit 2
fi

[[ -n "$CORPUS" ]] || { echo "FATAL: --corpus is required" >&2; usage >&2; exit 2; }
CORPUS="$(cd "$CORPUS" && pwd)"
TABLE_DIR="$CORPUS/ws0/events"
if ! ls "$TABLE_DIR"/*-Data.db >/dev/null 2>&1; then
  echo "FATAL: $TABLE_DIR holds no *-Data.db." >&2
  echo "       Generate it: cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out $CORPUS" >&2
  exit 2
fi
TICKET_TEMPLATE="$CORPUS/ticket-template.json"

for tool in perf taskset python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool is not installed" >&2; exit 2; }
done

verify_sibling_pair "$SERVER_CPUS" "server"
verify_sibling_pair "$CLIENT_CPUS" "client" 2>/dev/null \
  || echo "client CPUs: $CLIENT_CPUS (a multi-core set — only the SERVER set must be one physical core)"
verify_disjoint "$SERVER_CPUS" "$CLIENT_CPUS"

PARANOID="$(cat /proc/sys/kernel/perf_event_paranoid)"
if [[ "$PARANOID" != "-1" ]]; then
  echo "perf_event_paranoid is $PARANOID; CPU-wide counting needs -1. Trying sudo -n…"
  sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null || {
    echo "FATAL: cannot set kernel.perf_event_paranoid=-1 (needed for perf stat -C)." >&2
    exit 2
  }
fi

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/target/perf-ws0-3096/$TS}"
mkdir -p "$OUT_DIR"
BIN="$REPO_ROOT/target/release"

if [[ "$DO_BUILD" == "1" ]]; then
  echo "building release binaries…"
  (cd "$REPO_ROOT" && cargo build --release -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen) \
    > "$OUT_DIR/build.log" 2>&1 \
    || { echo "FATAL: release build failed — see $OUT_DIR/build.log" >&2; exit 2; }
fi
for b in ws0-scan-bench cqlite-flight flight-loadgen; do
  [[ -x "$BIN/$b" ]] || { echo "FATAL: $BIN/$b missing (drop --no-build, or build it)" >&2; exit 2; }
done

# The Flight ticket is derived from the DDL the corpus was WRITTEN with (the
# generator emits it beside the data), so both arms provably read one schema.
DDL_FILE="$CORPUS/ws0-events.cql"
[[ -r "$DDL_FILE" ]] || { echo "FATAL: $DDL_FILE missing — regenerate the corpus" >&2; exit 2; }
python3 - "$DDL_FILE" "$TICKET_TEMPLATE" <<'PY'
import json, sys
ddl = open(sys.argv[1]).read().strip().rstrip(';')
json.dump({"version": 2, "keyspace": "ws0", "table": "events", "ddl": ddl,
           "snapshot": None, "token_start": None, "token_end": None,
           "wraparound": False, "columns": None, "predicates": [],
           "filter": None, "aggregation": None, "limit": None},
          open(sys.argv[2], "w"), indent=1)
PY

drop_caches_if_cold() {
  [[ "$1" == "cold" ]] || return 0
  sync
  echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null \
    || { echo "FATAL: cannot drop caches (sudo -n) — a 'cold' claim would be a lie" >&2; exit 2; }
}

# perf stat -C <cpu-list>: CPU-WIDE, never per-process (trap 1).
perf_stat_c() {
  local outfile="$1"; shift
  perf stat -x, -e "$EVENTS" -C "$SERVER_CPUS" -o "$outfile" -- "$@"
}

# ---------------------------------------------------------------------------
# Arm A — the bare scan
# ---------------------------------------------------------------------------
measure_scan() {
  local temp="$1" rep="$2" tag="scan-$temp-$rep"
  drop_caches_if_cold "$temp"
  # Setup-only leg: the corpus open + schema ingest, under its OWN perf window,
  # so its cycles can be SUBTRACTED from the full run (spec R2).
  perf_stat_c "$OUT_DIR/perf-$tag-setup.csv" \
    taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
      --corpus "$CORPUS" --setup-only \
    > "$OUT_DIR/$tag-setup.json" 2> "$OUT_DIR/$tag-setup.err"

  drop_caches_if_cold "$temp"
  perf_stat_c "$OUT_DIR/perf-$tag.csv" \
    taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
      --corpus "$CORPUS" --passes "$SCAN_PASSES" \
    > "$OUT_DIR/$tag.json" 2> "$OUT_DIR/$tag.err" \
    || { echo "FATAL: bare-scan rep $tag failed — see $OUT_DIR/$tag.err" >&2; exit 1; }
  echo "  $tag done"
}

# ---------------------------------------------------------------------------
# Arm B — Flight do_get over a real loopback transport
# ---------------------------------------------------------------------------
measure_flight() {
  local temp="$1" rep="$2" arm="$3" tag="flight-$arm-$temp-$rep"
  pkill -x cqlite-flight >/dev/null 2>&1 || true
  sleep 1
  drop_caches_if_cold "$temp"

  CQLITE_FLIGHT_MERGE_PATH="$arm" taskset -c "$SERVER_CPUS" "$BIN/cqlite-flight" \
    --data-dir "$CORPUS" --listen "127.0.0.1:$PORT" \
    > "$OUT_DIR/$tag.server.log" 2>&1 &
  local srv=$!
  local i
  for i in $(seq 1 120); do
    (echo >"/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1 && break
    sleep 1
  done

  # Prewarm OUTSIDE the perf window (warm arm only): opens the readers and fills
  # the warm-handle registry, so the measured window is steady-state scan work
  # and not one-off setup. On the COLD arm this is deliberately skipped — a
  # prewarm would make "cold" meaningless.
  if [[ "$temp" == "warm" ]]; then
    taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" \
      --endpoint "http://127.0.0.1:$PORT" --ticket-template "$TICKET_TEMPLATE" \
      --shape full --ramp 1 --step-duration 20s --round prewarm --out /dev/null \
      > "$OUT_DIR/$tag.prewarm.log" 2>&1 || true
  fi

  perf_stat_c "$OUT_DIR/perf-$tag.csv" \
    taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" \
      --endpoint "http://127.0.0.1:$PORT" --ticket-template "$TICKET_TEMPLATE" \
      --shape full --ramp 1 --step-duration "$STEP_DURATION" \
      --round "$tag" --out "$OUT_DIR/$tag.jsonl" \
    > "$OUT_DIR/$tag.log" 2>&1 \
    || { kill "$srv" 2>/dev/null || true; echo "FATAL: flight rep $tag failed — see $OUT_DIR/$tag.log" >&2; exit 1; }

  kill "$srv" 2>/dev/null || true
  sleep 1
  kill -9 "$srv" 2>/dev/null || true
  echo "  $tag done"
}

echo
echo "=== issue #3096 same-session baseline ==="
echo "corpus:      $CORPUS"
echo "server CPUs: $SERVER_CPUS (verified physical-core siblings)"
echo "client CPUs: $CLIENT_CPUS"
echo "reps:        $REPS   temps: $TEMPS   arms: $ARMS"
echo "out:         $OUT_DIR"
echo

for temp in $TEMPS; do
  echo "-- bare scan ($temp) --"
  for rep in $(seq 1 "$REPS"); do measure_scan "$temp" "$rep"; done
  for arm in $ARMS; do
    echo "-- flight do_get / $arm ($temp) --"
    for rep in $(seq 1 "$REPS"); do measure_flight "$temp" "$rep" "$arm"; done
  done
done

python3 "$HERE/ws0_report.py" \
  --dir "$OUT_DIR" --corpus "$CORPUS" --server-cpus "$SERVER_CPUS" \
  --client-cpus "$CLIENT_CPUS" --reps "$REPS" --temps "$TEMPS" --arms "$ARMS" \
  --step-duration "$STEP_DURATION" --scan-passes "$SCAN_PASSES" \
  | tee "$OUT_DIR/summary.txt"

echo
echo "machine-readable: $OUT_DIR/results.json"
echo "human summary:    $OUT_DIR/summary.txt"
