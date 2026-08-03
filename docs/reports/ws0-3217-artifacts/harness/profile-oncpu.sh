#!/usr/bin/env bash
# Issue #3217 AC3: on-CPU flame graph of the cqlite-flight do_get read path,
# sampled CPU-wide over the server's pinned set during a steady-state window.
#
# Usage:
#   profile-oncpu.sh <label> <server-cpu-spec> <N> <window-secs> [bypass|merge]
#
# Matrix AC3 asks for: N in {1,8,16} x {S=1 pinned, S=6 full-box}.
#   for s in s1 s6; do for n in 1 8 16; do
#     ./profile-oncpu.sh oncpu-$s-N$n $s $n 30; done; done
#
# TRAP - DO NOT USE `--call-graph=dwarf`. Against this ~143 MB binary dwarf
# unwinding HANGS past 120s. The server is built with -C force-frame-pointers=yes
# (plus CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=true), so
# frame-pointer unwinding (`-g` == `--call-graph=fp`) is both correct and fast.
#
# Retains BOTH the SVG and the folded text: the folded text is what allows
# re-plotting / differencing later (AC8), the SVG alone is a dead end.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

usage() { sed -n '2,20p' "${BASH_SOURCE[0]}" >&2; exit 2; }
[ $# -ge 4 ] || usage

LABEL="$1"; SRV_SPEC="$2"; N="$3"; WINDOW="$4"; MERGE_PATH="${5:-bypass}"
STEADY_PRE="${WS0_STEADY_PRE_SECS:-20}"   # let the ramp reach steady state first
TAIL="${WS0_TAIL_SECS:-10}"               # load outlives the record window

case "$MERGE_PATH" in bypass|merge) ;; *) ws0_die "merge-path must be bypass|merge";; esac
case "$SRV_SPEC" in
  s1|s2|s4|s6) S_CORES="${SRV_SPEC#s}"; SERVER_CPUS="$(ws0_server_cpus_for_s "$S_CORES")" ;;
  *)           S_CORES=""; SERVER_CPUS="$SRV_SPEC" ;;
esac
SERVER_CPUS="$(ws0_cpulist_expand "$SERVER_CPUS")"
CLIENT_CPUS="$(ws0_cpulist_expand "${WS0_CLIENT_CPUS:-$WS0_CLIENT_CPUS_DEFAULT}")"

OUTDIR="$WS0_PROFILES/$LABEL"; LOGDIR="$WS0_LOGS/$LABEL"
mkdir -p "$OUTDIR" "$LOGDIR"

ws0_assert_sysctl
ws0_verify_topology "$OUTDIR/cpu-topology.json"

if [ "${WS0_DRY_RUN:-0}" = "1" ]; then
  ws0_log "WS0_DRY_RUN=1: exercising the perf-record -> collapse -> flamegraph -> AC3-gate pipeline against 'sleep', no server."
  perf record -F 999 -g --call-graph=fp -C "$SERVER_CPUS" -o "$OUTDIR/perf.data" \
    -- sleep 2 >"$LOGDIR/perf-record.log" 2>&1 || ws0_die "perf record failed; see $LOGDIR/perf-record.log"
else
  ws0_require_inputs
  trap 'ws0_stop_server; [ -n "${LOADGEN_PID:-}" ] && kill -9 "$LOADGEN_PID" 2>/dev/null || true' EXIT INT TERM
  ws0_start_server "$SERVER_CPUS" "$MERGE_PATH" "$LOGDIR/server.log"
  [ "${WS0_WARM_SECS:-45}" -gt 0 ] && ws0_warm_prepass "$CLIENT_CPUS" "${WS0_WARM_SECS:-45}" "$LOGDIR/prewarm.log"

  LOAD_SECS=$(( STEADY_PRE + WINDOW + TAIL ))
  ws0_log "driving load N=$N for ${LOAD_SECS}s; recording ${WINDOW}s after ${STEADY_PRE}s of steady state"
  taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" \
      --endpoint "$WS0_ENDPOINT" --ticket-template "$WS0_TICKET_TPL" \
      --shape full --ramp "$N" --step-duration "${LOAD_SECS}s" --seed "$WS0_SEED" \
      --round "$LABEL" --out "$LOGDIR/step.jsonl" >"$LOGDIR/loadgen.log" 2>&1 &
  LOADGEN_PID=$!
  sleep "$STEADY_PRE"

  # -F 999 (not 1000) avoids lock-stepping with any 1 kHz periodic activity.
  # -C <cpus> matches the `perf stat -C` convention used by the C(N) sweep, so
  # the flame graph and the counters describe the same CPU set.
  perf record -F 999 -g --call-graph=fp -C "$SERVER_CPUS" -o "$OUTDIR/perf.data" \
    -- sleep "$WINDOW" >"$LOGDIR/perf-record.log" 2>&1 \
    || ws0_die "perf record failed; see $LOGDIR/perf-record.log"

  wait "$LOADGEN_PID" 2>/dev/null || true
  unset LOADGEN_PID
  ws0_stop_server
  trap - EXIT INT TERM
fi

ws0_log "perf script -> stackcollapse -> flamegraph"
perf script -i "$OUTDIR/perf.data" >"$OUTDIR/perf.script" 2>"$LOGDIR/perf-script.log" \
  || ws0_die "perf script failed; see $LOGDIR/perf-script.log"
"$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$OUTDIR/perf.script" >"$OUTDIR/oncpu.folded" \
  2>"$LOGDIR/stackcollapse.log"
[ -s "$OUTDIR/oncpu.folded" ] || ws0_die "folded output is empty — no samples on cpus $SERVER_CPUS"

"$FLAMEGRAPH_DIR/flamegraph.pl" \
  --title "On-CPU $LABEL (N=$N, server cpus $SERVER_CPUS, merge_path=$MERGE_PATH)" \
  --subtitle "perf -F 999 --call-graph=fp -C $SERVER_CPUS  |  issue #3217" \
  --width 1600 "$OUTDIR/oncpu.folded" >"$OUTDIR/oncpu.svg" 2>"$LOGDIR/flamegraph.log"

# AC3 gate: unsymbolized frames must be < 10% of samples.
set +e
python3 "$HARNESS_DIR/unsym-check.py" "$OUTDIR/oncpu.folded" \
  --threshold "$WS0_UNSYM_THRESHOLD" --label "$LABEL" --out "$OUTDIR/unsym-check.json"
UNSYM_RC=$?
set -e

cat >"$OUTDIR/run-config.json" <<EOF
{"label":"$LABEL","kind":"oncpu","server_physical_cores_S":"${S_CORES:-custom}",
 "server_cpus":"$SERVER_CPUS","client_cpus":"$CLIENT_CPUS","N":$N,"merge_path":"$MERGE_PATH",
 "window_secs":$WINDOW,"steady_pre_secs":$STEADY_PRE,"perf_freq_hz":999,
 "call_graph":"fp (frame pointers; dwarf HANGS on this binary - never use it)",
 "unsym_gate_rc":$UNSYM_RC,"dry_run":"${WS0_DRY_RUN:-0}","utc":"$(date -u +%FT%TZ)"}
EOF

ws0_log "artefacts: $OUTDIR/{oncpu.svg,oncpu.folded,perf.data,perf.script,unsym-check.json}"
[ $UNSYM_RC -eq 0 ] || ws0_die "AC3 unsymbolized-frame gate FAILED — see $OUTDIR/unsym-check.json"
ws0_log "AC3 unsymbolized-frame gate PASSED"
