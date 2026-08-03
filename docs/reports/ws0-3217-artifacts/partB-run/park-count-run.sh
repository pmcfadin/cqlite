#!/usr/bin/env bash
# WS0 #3217 Part B supplement: park COUNTS per stack (not durations) for one
# (S, N) point. Answers "~1,960 voluntary parks per batch — what is parking?"
set -euo pipefail
source /data/ws0/ws0env.sh
source "$WT/docs/reports/ws0-3217-artifacts/harness/common.sh"

LABEL="$1"; SRV_SPEC="$2"; N="$3"; WINDOW="${4:-30}"; MERGE_PATH="${5:-bypass}"
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

CT0="$(ws0_proc_ctxt_json "$WS0_SERVER_PID")"
sudo -n bpftrace /data/ws0/park-count.bt "$WS0_SERVER_PID" "$WINDOW" \
  >"$OUTDIR/park-count.txt" 2>"$LOGDIR/park-count.log" || \
  ws0_warn "bpftrace non-zero; see $LOGDIR/park-count.log"
CT1="$(ws0_proc_ctxt_json "$WS0_SERVER_PID")"

wait "$LOADGEN_PID" 2>/dev/null || true; unset LOADGEN_PID
ws0_stop_server; trap - EXIT INT TERM

cat >"$OUTDIR/park-count-config.json" <<EOJ
{"label":"$LABEL","kind":"park-count","server_cpus":"$SERVER_CPUS","N":$N,
 "merge_path":"$MERGE_PATH","window_secs":$WINDOW,"unit":"EVENTS (switch count), not microseconds",
 "utc":"$(date -u +%FT%TZ)"}
EOJ
ws0_log "park-count artefacts: $OUTDIR/park-count.txt  (loadgen: $LOGDIR/step.jsonl)"
