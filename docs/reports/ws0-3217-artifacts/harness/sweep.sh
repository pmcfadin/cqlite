#!/usr/bin/env bash
# Part A of issue #3217: the full-box C(N) driver.
#
# Sweeps concurrency N at a fixed server core count S, with >=3 reps per point,
# and emits one JSONL record per (S, N, rep) plus a human-readable table.
#
# Usage:
#   sweep.sh <label> <server-cpu-spec> <client-cpu-list> <N-ramp> <step-seconds> <reps> [bypass|merge]
#
#   server-cpu-spec  s1 | s2 | s4 | s6   (table-driven, see common.sh)
#                    or a literal CPU list e.g. "0-5,8-13"
#   client-cpu-list  literal list; default set is 6,7,14,15 (2 physical cores)
#   N-ramp           comma list, e.g. 1,2,4,8,16
#   step-seconds     per-point hold, e.g. 120 (matches #3100)
#   reps             >= 3 (AC1 requires per-N min/median/max dispersion)
#
# Required env (never hardcoded - the corpus is produced by a peer agent and the
# binaries live in the issue worktree):
#   WS0_STAGE, WS0_FLIGHT_BIN, WS0_LOADGEN_BIN, WS0_TICKET_TPL
# Optional env:
#   WS0_LOGICAL_BYTES_PER_ROW  override for the logical/uncompressed basis
#   WS0_SETTLE_SECS            idle gap between points (default 5)
#   WS0_WARM_SECS              warm pre-pass length (default 45; 0 disables)
#   WS0_DRY_RUN=1              validate args/topology/basis and exit without
#                              launching a server (mechanics smoke test)
#
# Example (S=1 reproduction of the #3100 pinned control):
#   WS0_STAGE=/data/ws0/ws0-corpus/sstables \
#   WS0_FLIGHT_BIN=$WT/target/release/cqlite-flight \
#   WS0_LOADGEN_BIN=$WT/target/release/flight-loadgen \
#   WS0_TICKET_TPL=$WT/docs/reports/ws0-3100-artifacts/ws0-h2h/ws0-events-template.json \
#   ./sweep.sh s1-bypass s1 6,7,14,15 1,2,4,8,16 120 3 bypass

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

usage() { sed -n '2,32p' "${BASH_SOURCE[0]}" >&2; exit 2; }
[ $# -ge 6 ] || usage

LABEL="$1"
SRV_SPEC="$2"
CLIENT_CPUS="${3:-$WS0_CLIENT_CPUS_DEFAULT}"
RAMP="$4"
STEP_SECS="$5"
REPS="$6"
MERGE_PATH="${7:-bypass}"

case "$MERGE_PATH" in bypass|merge) ;; *) ws0_die "merge-path must be 'bypass' or 'merge', got '$MERGE_PATH'";; esac
[[ "$REPS" =~ ^[0-9]+$ ]] || ws0_die "reps must be an integer"
[ "$REPS" -ge 3 ] || ws0_warn "reps=$REPS < 3: AC1 asks for >=3 per N for min/median/max dispersion"
[[ "$STEP_SECS" =~ ^[0-9]+$ ]] || ws0_die "step-seconds must be an integer"
[[ "$RAMP" =~ ^[0-9]+(,[0-9]+)*$ ]] || ws0_die "N-ramp must be a comma list of integers"

# ---- resolve the server CPU set from the table -------------------------------
S_CORES=""
case "$SRV_SPEC" in
  s1|s2|s4|s6) S_CORES="${SRV_SPEC#s}"; SERVER_CPUS="$(ws0_server_cpus_for_s "$S_CORES")" ;;
  *)           SERVER_CPUS="$SRV_SPEC" ;;
esac
S_JSON="${S_CORES:-null}"
SERVER_CPUS="$(ws0_cpulist_expand "$SERVER_CPUS")"
CLIENT_CPUS="$(ws0_cpulist_expand "$CLIENT_CPUS")"
N_SRV="$(ws0_cpulist_count "$SERVER_CPUS")"
N_CLI="$(ws0_cpulist_count "$CLIENT_CPUS")"

# Overlap between the two pinned sets would make CPU-wide perf on the server set
# count client work as engine work. That is not a warning, it is a broken run.
OVERLAP="$(python3 -c '
import sys
def ex(s):
    o=set()
    for p in s.split(","):
        if "-" in p: a,b=p.split("-"); o|=set(range(int(a),int(b)+1))
        else: o.add(int(p))
    return o
print(",".join(str(x) for x in sorted(ex(sys.argv[1]) & ex(sys.argv[2]))))' "$SERVER_CPUS" "$CLIENT_CPUS")"
[ -z "$OVERLAP" ] || ws0_die "server and client CPU sets overlap on {$OVERLAP}; server-set perf would count client work"

OUTDIR="$WS0_RESULTS/$LABEL"
LOGDIR="$WS0_LOGS/$LABEL"
mkdir -p "$OUTDIR" "$LOGDIR"
POINTS="$OUTDIR/points.jsonl"

ws0_assert_sysctl
ws0_verify_topology "$OUTDIR/cpu-topology.json"
ws0_require_inputs

# ---- byte basis (AC6) --------------------------------------------------------
BASIS_JSON="$OUTDIR/corpus-basis.json"
python3 "$HARNESS_DIR/corpus-basis.py" "$WS0_STAGE" -o "$BASIS_JSON" >"$LOGDIR/corpus-basis.log" 2>&1 \
  || ws0_die "corpus-basis.py failed; see $LOGDIR/corpus-basis.log"
ws0_log "corpus basis -> $BASIS_JSON"

HARNESS_COMMIT="$(git -C "$HARNESS_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
SERVER_FLAGS="--batch-size $WS0_BATCH_SIZE --max-batch-bytes $WS0_MAX_BATCH_BYTES --max-inflight-egress-bytes $WS0_MAX_INFLIGHT_EGRESS_BYTES --max-concurrent-scans $WS0_MAX_CONCURRENT_SCANS --admission-wait-timeout-ms $WS0_ADMISSION_WAIT_TIMEOUT_MS"

cat >"$OUTDIR/run-config.json" <<EOF
{"label":"$LABEL","server_physical_cores_S":$S_JSON,"server_cpus":"$SERVER_CPUS",
 "client_cpus":"$CLIENT_CPUS","ramp":"$RAMP","step_seconds":$STEP_SECS,"reps":$REPS,
 "merge_path":"$MERGE_PATH","seed":$WS0_SEED,"stage":"$WS0_STAGE",
 "flight_bin":"$WS0_FLIGHT_BIN","loadgen_bin":"$WS0_LOADGEN_BIN","ticket_template":"$WS0_TICKET_TPL",
 "server_flags":"$SERVER_FLAGS","client_saturation_threshold":$WS0_CLIENT_SAT_THRESHOLD,
 "harness_commit":"$HARNESS_COMMIT","started_utc":"$(date -u +%FT%TZ)"}
EOF

ws0_log "label=$LABEL S=${S_CORES:-custom} server_cpus=$SERVER_CPUS ($N_SRV hw threads) client_cpus=$CLIENT_CPUS ($N_CLI hw threads)"
ws0_log "ramp=$RAMP step=${STEP_SECS}s reps=$REPS merge_path=$MERGE_PATH -> $OUTDIR"

if [ "${WS0_DRY_RUN:-0}" = "1" ]; then
  ws0_log "WS0_DRY_RUN=1: args, topology, overlap check and corpus basis validated; not launching a server."
  exit 0
fi

trap 'ws0_stop_server' EXIT INT TERM

SERVER_LOG="$LOGDIR/server.log"
ws0_start_server "$SERVER_CPUS" "$MERGE_PATH" "$SERVER_LOG"

if [ "${WS0_WARM_SECS:-45}" -gt 0 ]; then
  ws0_warm_prepass "$CLIENT_CPUS" "${WS0_WARM_SECS:-45}" "$LOGDIR/prewarm.log"
fi

SETTLE="${WS0_SETTLE_SECS:-5}"
for rep in $(seq 1 "$REPS"); do
  for N in ${RAMP//,/ }; do
    TAG="N${N}-r${rep}"
    PERF_CSV="$LOGDIR/perf-$TAG.csv"
    STEP_JSONL="$LOGDIR/step-$TAG.jsonl"
    CTX_JSON="$LOGDIR/ctx-$TAG.json"

    kill -0 "$WS0_SERVER_PID" 2>/dev/null || ws0_die "server died before $TAG; see $SERVER_LOG"

    C0="$(ws0_proc_cpu_secs "$WS0_SERVER_PID")"
    IO0="$(ws0_proc_io_json "$WS0_SERVER_PID")"
    CT0="$(ws0_proc_ctxt_json "$WS0_SERVER_PID")"
    CLI0="$(ws0_cpuset_busy_secs "$CLIENT_CPUS")"
    T0="$(date +%s.%N)"

    set +e
    perf stat -x, -e cycles,instructions,context-switches,cpu-migrations,task-clock \
      -C "$SERVER_CPUS" -o "$PERF_CSV" -- \
      taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" \
        --endpoint "$WS0_ENDPOINT" --ticket-template "$WS0_TICKET_TPL" \
        --shape full --ramp "$N" --step-duration "${STEP_SECS}s" --seed "$WS0_SEED" \
        --round "$LABEL-$TAG" --out "$STEP_JSONL" \
        >"$LOGDIR/loadgen-$TAG.log" 2>&1
    RC=$?
    set -e
    [ $RC -eq 0 ] || ws0_warn "loadgen exited $RC for $TAG (see $LOGDIR/loadgen-$TAG.log) — point still recorded"

    T1="$(date +%s.%N)"
    C1="$(ws0_proc_cpu_secs "$WS0_SERVER_PID")"
    IO1="$(ws0_proc_io_json "$WS0_SERVER_PID")"
    CT1="$(ws0_proc_ctxt_json "$WS0_SERVER_PID")"
    CLI1="$(ws0_cpuset_busy_secs "$CLIENT_CPUS")"

    python3 - "$CTX_JSON" <<PYCTX
import json, sys
io0, io1 = json.loads('''$IO0'''), json.loads('''$IO1''')
ct0, ct1 = json.loads('''$CT0'''), json.loads('''$CT1''')
basis = json.load(open("$BASIS_JSON"))
ovr = "${WS0_LOGICAL_BYTES_PER_ROW:-}"
json.dump({
 "label": "$LABEL", "ts_unix_ms": int(float("$T0") * 1000),
 "harness_commit": "$HARNESS_COMMIT",
 # S_JSON is a bare number for the s1|s2|s4|s6 shorthands and the literal token `null` for a
 # custom CPU list. `null` is valid JSON but is NOT a Python name, so interpolating it raw here
 # raised NameError and killed the arm at the warm pre-pass (#3225, the S=3 arm). Parse it as JSON
 # instead: json.loads("6") == 6 for every value #3217 ever ran, so this is behaviour-identical
 # for the shorthands and merely correct for a custom set.
 "server_physical_cores_S": json.loads('''$S_JSON'''),
 "server_cpus": "$SERVER_CPUS", "server_cpu_count": $N_SRV,
 "client_cpus": "$CLIENT_CPUS", "client_cpu_count": $N_CLI,
 "merge_path": "$MERGE_PATH", "N": $N, "rep": $rep, "reps_total": $REPS,
 "step_seconds": $STEP_SECS, "server_flags": "$SERVER_FLAGS",
 "wall_secs": float("$T1") - float("$T0"),
 "server_cpu_secs_delta": float("$C1") - float("$C0"),
 "client_cpuset_busy_secs_delta": float("$CLI1") - float("$CLI0"),
 "client_saturation_threshold": $WS0_CLIENT_SAT_THRESHOLD,
 "server_io_delta": {k: io1.get(k, 0) - io0.get(k, 0) for k in ("rchar", "read_bytes", "syscr")},
 "server_ctxt_delta": {k: ct1.get(k, 0) - ct0.get(k, 0) for k in ct1},
 "corpus_basis": basis,
 "logical_bytes_per_row_override": float(ovr) if ovr else None,
}, open(sys.argv[1], "w"), indent=1)
PYCTX

    python3 "$HARNESS_DIR/emit-point.py" --perf-csv "$PERF_CSV" --step-jsonl "$STEP_JSONL" \
      --context-json "$CTX_JSON" --out "$POINTS"

    sleep "$SETTLE"
  done
done

ws0_stop_server
trap - EXIT INT TERM

python3 "$HARNESS_DIR/summarize-sweep.py" "$POINTS" \
  --out-json "$OUTDIR/summary.json" --out-table "$OUTDIR/summary.txt"
cat "$OUTDIR/summary.txt"
ws0_log "done: $OUTDIR"
