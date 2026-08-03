#!/usr/bin/env bash
# WS0 #3217 Part B: voluntary-park COUNTS by stack via perf sched:sched_switch.
# WHY perf and not bpftrace: bpftrace left ~50% of user frames as RAW ADDRESSES
# (measured on park-s6-N1), while perf script demangles and symbolizes the same
# stacks completely (AC3 measured 0.000% unsymbolized inside the server threads).
# sched_switch fires in the context of the OUTGOING task, so the recorded stack is
# the stack the thread parked on. prev_state==0 (TASK_RUNNING) == preempted;
# anything else == blocked, i.e. a VOLUNTARY park.
set -euo pipefail
source /data/ws0/ws0env.sh
source "$WT/docs/reports/ws0-3217-artifacts/harness/common.sh"

LABEL="$1"; SRV_SPEC="$2"; N="$3"; WINDOW="${4:-10}"; MERGE_PATH="${5:-bypass}"
STEADY_PRE="${WS0_STEADY_PRE_SECS:-20}"; TAIL="${WS0_TAIL_SECS:-10}"
SERVER_CPUS="$(ws0_cpulist_expand "$(ws0_server_cpus_for_s "${SRV_SPEC#s}")")"
CLIENT_CPUS="$(ws0_cpulist_expand "$WS0_CLIENT_CPUS_DEFAULT")"
OUTDIR="$WS0_PROFILES/$LABEL"; LOGDIR="$WS0_LOGS/$LABEL"; mkdir -p "$OUTDIR" "$LOGDIR"

ws0_assert_sysctl
ws0_require_inputs
trap 'ws0_stop_server; [ -n "${LOADGEN_PID:-}" ] && kill -9 "$LOADGEN_PID" 2>/dev/null || true' EXIT INT TERM
ws0_start_server "$SERVER_CPUS" "$MERGE_PATH" "$LOGDIR/server.log"
ws0_warm_prepass "$CLIENT_CPUS" "${WS0_WARM_SECS:-45}" "$LOGDIR/prewarm.log"

LOAD_SECS=$(( STEADY_PRE + WINDOW + TAIL ))
taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" \
    --endpoint "$WS0_ENDPOINT" --ticket-template "$WS0_TICKET_TPL" \
    --shape full --ramp "$N" --step-duration "${LOAD_SECS}s" --seed "$WS0_SEED" \
    --round "$LABEL" --out "$LOGDIR/step.jsonl" >"$LOGDIR/loadgen.log" 2>&1 &
LOADGEN_PID=$!
sleep "$STEADY_PRE"

perf record -e sched:sched_switch -g --call-graph=fp -p "$WS0_SERVER_PID" \
  -o "$OUTDIR/sched.data" -- sleep "$WINDOW" >"$LOGDIR/perf-record.log" 2>&1 \
  || ws0_warn "perf record non-zero; see $LOGDIR/perf-record.log"

wait "$LOADGEN_PID" 2>/dev/null || true; unset LOADGEN_PID
ws0_stop_server; trap - EXIT INT TERM

perf script -i "$OUTDIR/sched.data" -F comm,tid,event,trace,ip,sym,dso \
  >"$OUTDIR/sched.script" 2>"$LOGDIR/perf-script.log"
gzip -f "$OUTDIR/sched.script"
cat >"$OUTDIR/sched-config.json" <<EOJ
{"label":"$LABEL","kind":"sched-switch-count","server_cpus":"$SERVER_CPUS","N":$N,
 "merge_path":"$MERGE_PATH","window_secs":$WINDOW,
 "unit":"EVENTS (context switches), split voluntary/involuntary by tracepoint prev_state",
 "utc":"$(date -u +%FT%TZ)"}
EOJ
ws0_log "sched artefacts: $OUTDIR/{sched.data,sched.script.gz}"
