#!/usr/bin/env bash
# WS0 #3217 Part B: does the residual inefficiency come from MORE WORK or SLOWER
# WORK? Part A's counters already say instructions/row is flat (+0.7%) while
# cycles/row is +34% (IPC 1.52 -> 1.14), i.e. slower work. This run tests the
# memory-hierarchy hypothesis for that IPC decay directly instead of inferring it.
set -euo pipefail
source /data/ws0/ws0env.sh
source "$WT/docs/reports/ws0-3217-artifacts/harness/common.sh"
LABEL="$1"; SRV_SPEC="$2"; N="$3"; WINDOW="${4:-20}"
SERVER_CPUS="$(ws0_cpulist_expand "$(ws0_server_cpus_for_s "${SRV_SPEC#s}")")"
CLIENT_CPUS="$(ws0_cpulist_expand "$WS0_CLIENT_CPUS_DEFAULT")"
OUTDIR="$WS0_PROFILES/$LABEL"; LOGDIR="$WS0_LOGS/$LABEL"; mkdir -p "$OUTDIR" "$LOGDIR"
ws0_assert_sysctl; ws0_require_inputs
trap 'ws0_stop_server; [ -n "${LOADGEN_PID:-}" ] && kill -9 "$LOADGEN_PID" 2>/dev/null || true' EXIT INT TERM
ws0_start_server "$SERVER_CPUS" bypass "$LOGDIR/server.log"
ws0_warm_prepass "$CLIENT_CPUS" 45 "$LOGDIR/prewarm.log"
LOAD_SECS=$(( 20 + WINDOW + 10 ))
taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" --endpoint "$WS0_ENDPOINT" \
    --ticket-template "$WS0_TICKET_TPL" --shape full --ramp "$N" \
    --step-duration "${LOAD_SECS}s" --seed "$WS0_SEED" --round "$LABEL" \
    --out "$LOGDIR/step.jsonl" >"$LOGDIR/loadgen.log" 2>&1 &
LOADGEN_PID=$!; sleep 20
perf stat -x, -C "$SERVER_CPUS" \
  -e cycles,instructions,cache-references,cache-misses,LLC-loads,LLC-load-misses,L1-dcache-loads,L1-dcache-load-misses,dTLB-load-misses,branch-misses,task-clock \
  -o "$OUTDIR/llc.csv" -- sleep "$WINDOW" >"$LOGDIR/perf-stat.log" 2>&1 || \
  ws0_warn "perf stat non-zero; see $LOGDIR/perf-stat.log"
wait "$LOADGEN_PID" 2>/dev/null || true; unset LOADGEN_PID
ws0_stop_server; trap - EXIT INT TERM
ws0_log "llc artefacts: $OUTDIR/llc.csv"
