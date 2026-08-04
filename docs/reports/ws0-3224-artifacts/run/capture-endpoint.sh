#!/usr/bin/env bash
# =============================================================================
# #3224 endpoint capture — ONE (S,N) point, ONE rep.
#
#   capture-endpoint.sh <label> <S> <N> <step_secs> <window_secs> <rep> <outdir>
#
# Produces, per rep:
#   perf-core.csv        core counter set,  ONE perf stat -C over an INTERIOR window
#   perf-core-aligned.csv core counter set, window == the WHOLE loadgen step
#   perf-uncore.csv      uncore_imc cas_count_{read,write}, SEPARATE invocation,
#                        --per-socket (core PMU group cannot host uncore events)
#   step-*.jsonl         the loadgen step records for each of the three arms
#   meta.json            everything the derivation needs, incl. both denominators
#
# WHY TWO CORE ARMS — this is the #3224 method question, settled with data.
# #3217 computed cycles/row as counter / (rows_per_s * window_secs), where
# rows_per_s came from the WHOLE loadgen step but the counters came from a 20 s
# interior slice. At llc-s1-N2 that step held 4 completed requests over 63.99 s.
# The concern is real: rows are CREDITED in lumps when a request completes, while
# cycles accrue CONTINUOUSLY. Counting "rows completed inside the window" is
# therefore NOISIER, not cleaner — at S=1/N=2 a 20 s window can contain 0, 1 or 2
# completions and imply a rate off by 2x in either direction.
#
# So we measure BOTH conventions per rep and report whether they agree:
#   (a) INTERIOR  — #3217's convention: counters over an interior window, rate
#                   from the step. Correct IFF in-flight occupancy is constant.
#   (b) ALIGNED   — the RUNBOOK's alternative: the perf window IS the loadgen
#                   step, so numerator and denominator share ONE interval BY
#                   CONSTRUCTION and no rate assumption is needed at all.
# If (a) and (b) agree, #3217's baseline stands and the +8,593 target holds. If
# they diverge, the target MOVES and that is a finding. Either way it is DATA.
#
# OCCUPANCY CHECK (what makes (a) legitimate) is emitted into meta.json:
#   rows_total must be an exact multiple of the corpus row count (whole scans
#   only, no partial credit), and requests_ok * p50_latency_s / N must be close
#   to duration_s (workers busy the whole step, no idle ramp/drain).
#
# rc DISCIPLINE (#3217's fabricated-rc bug): every rc is captured into a variable
# IMMEDIATELY after the command, BEFORE any other command substitution runs.
# `echo "$(date) rc=$?"` reports the rc of `date`, not of the step. Never do that.
# =============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/../harness/common.sh"

LABEL="${1:?label}"; S="${2:?S}"; N="${3:?N}"
STEP_SECS="${4:?step_secs}"; WINDOW_SECS="${5:?window_secs}"
REP="${6:?rep}"; OUT="${7:?outdir}"

CORPUS_ROWS="${CORPUS_ROWS:-3999890}"
SETTLE_SECS="${SETTLE_SECS:-12}"   # skip the leading part of the step for arm (a)

mkdir -p "$OUT"
ws0_require_inputs
ws0_assert_sysctl                      # re-asserted per capture, never once per session

SERVER_CPUS="$(ws0_server_cpus_for_s "$S")"
CLIENT_CPUS="$WS0_CLIENT_CPUS_DEFAULT"
ws0_assert_cpuset_on_node      "server(S=$S)" "$SERVER_CPUS"
ws0_assert_cpuset_on_node      "client"       "$CLIENT_CPUS"
ws0_assert_full_physical_cores "server(S=$S)" "$SERVER_CPUS" "$S"
ws0_assert_full_physical_cores "client"       "$CLIENT_CPUS" 2
ws0_assert_sets_disjoint       "$SERVER_CPUS" "$CLIENT_CPUS"
ws0_verify_topology "$OUT/cpu-topology.json" >/dev/null

CORE_EVENTS="cycles,instructions,LLC-loads,LLC-load-misses,cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses,dTLB-load-misses,branch-misses,task-clock"
UNCORE_EVENTS="$(python3 - <<'PY'
print(",".join("uncore_imc_%d/cas_count_%s/" % (i, k)
               for i in range(12) for k in ("read", "write")))
PY
)"

# ---------------------------------------------------------------- loadgen arm
# Runs ONE ramp step at concurrency N for STEP_SECS. Writes the step record.
# Echoes nothing; the caller reads the jsonl.
loadgen() { # $1 round-label  $2 out-jsonl  $3 log
  taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" \
    --endpoint "$WS0_ENDPOINT" --ticket-template "$WS0_TICKET_TPL" \
    --shape full --ramp "$N" --step-duration "${STEP_SECS}s" \
    --seed "$WS0_SEED" --round "$1" --out "$2" >"$3" 2>&1
}

# ------------------------------------------------------------------ the server
SRV_LOG="$OUT/server.log"
ws0_start_server "$SERVER_CPUS" bypass "$SRV_LOG"
SRV_PID="$WS0_SERVER_PID"
cleanup() { ws0_stop_server "${SRV_PID:-}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# Warmth: page-cache resident before ANY counted arm. Verified, not assumed —
# io_before/io_after in meta.json must show read_bytes delta 0.
ws0_warm_prepass "$CLIENT_CPUS" "${WARM_SECS:-45}" "$OUT/warm.log"

IO_BEFORE="$(ws0_proc_io_json "$SRV_PID")"
CPU_BEFORE="$(ws0_proc_cpu_secs "$SRV_PID")"
CLIENT_BUSY_BEFORE="$(ws0_cpuset_busy_secs "$CLIENT_CPUS")"
WALL_BEFORE="$(date +%s.%N)"

# ============================ ARM (a): INTERIOR window ======================
# Loadgen runs in the background; perf counts a WINDOW_SECS slice starting
# SETTLE_SECS in, so the window sits strictly inside a step with constant
# occupancy. This reproduces #3217's convention exactly.
ws0_log "[$LABEL rep$REP] arm(a) INTERIOR: step=${STEP_SECS}s settle=${SETTLE_SECS}s window=${WINDOW_SECS}s"
loadgen "${LABEL}-interior" "$OUT/step-interior.jsonl" "$OUT/loadgen-interior.log" &
LG_PID=$!
sleep "$SETTLE_SECS"
perf stat -x, -C "$SERVER_CPUS" -e "$CORE_EVENTS" \
  -o "$OUT/perf-core.csv" -- sleep "$WINDOW_SECS" >/dev/null 2>&1
RC_CORE=$?                                    # captured IMMEDIATELY
wait "$LG_PID"; RC_LG_A=$?                    # captured IMMEDIATELY

# ============================ ARM (b): ALIGNED window ======================
# perf's window IS the loadgen step: perf runs the loadgen as its own child, so
# the counted interval and the row-producing interval are the same interval.
ws0_log "[$LABEL rep$REP] arm(b) ALIGNED: perf window == whole loadgen step"
perf stat -x, -C "$SERVER_CPUS" -e "$CORE_EVENTS" \
  -o "$OUT/perf-core-aligned.csv" -- \
  taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" \
    --endpoint "$WS0_ENDPOINT" --ticket-template "$WS0_TICKET_TPL" \
    --shape full --ramp "$N" --step-duration "${STEP_SECS}s" \
    --seed "$WS0_SEED" --round "${LABEL}-aligned" \
    --out "$OUT/step-aligned.jsonl" > "$OUT/loadgen-aligned.log" 2>&1
RC_ALIGNED=$?                                 # captured IMMEDIATELY

# ============================ ARM (c): UNCORE ==============================
# Uncore events CANNOT share the core PMU group (they would multiplex), so this
# is a SEPARATE invocation. --per-socket because all 12 IMCs carry cpumask=0,32:
# each device counts on BOTH sockets, so -a would sum them and hide the split.
ws0_log "[$LABEL rep$REP] arm(c) UNCORE: uncore_imc cas_count, --per-socket"
loadgen "${LABEL}-uncore" "$OUT/step-uncore.jsonl" "$OUT/loadgen-uncore.log" &
LG_PID=$!
sleep "$SETTLE_SECS"
perf stat -x, --per-socket -a -e "$UNCORE_EVENTS" \
  -o "$OUT/perf-uncore.csv" -- sleep "$WINDOW_SECS" >/dev/null 2>&1
RC_UNCORE=$?
wait "$LG_PID"; RC_LG_C=$?

WALL_AFTER="$(date +%s.%N)"
IO_AFTER="$(ws0_proc_io_json "$SRV_PID")"
CPU_AFTER="$(ws0_proc_cpu_secs "$SRV_PID")"
CLIENT_BUSY_AFTER="$(ws0_cpuset_busy_secs "$CLIENT_CPUS")"
CTXT="$(ws0_proc_ctxt_json "$SRV_PID")"

ws0_stop_server "$SRV_PID"; trap - EXIT INT TERM

# ------------------------------------------------------------------- meta.json
python3 - "$OUT/meta.json" <<PY
import json, os
def step(p):
    try:
        recs=[json.loads(l) for l in open(p) if l.strip()]
        return recs[-1] if recs else None
    except OSError:
        return None
si = step("$OUT/step-interior.jsonl")
sa = step("$OUT/step-aligned.jsonl")
su = step("$OUT/step-uncore.jsonl")
rows = int("$CORPUS_ROWS")

def occupancy(s):
    """What makes the INTERIOR convention legitimate: whole scans only, and
    workers busy for the whole step (no idle ramp/drain)."""
    if not s: return None
    d = s["duration_s"]; n = s["target_concurrency"]
    ok = s["requests_ok"]; rt = s["rows_total"]
    p50 = s["latency_ms"]["p50"]/1000.0
    return {
      "rows_total": rt,
      "rows_total_is_exact_multiple_of_corpus": (rt % rows == 0),
      "whole_scans": rt/rows,
      "requests_ok": ok,
      "duration_s": d,
      "p50_latency_s": p50,
      "busy_fraction_estimate": (ok*p50/n/d) if (n and d) else None,
      "rows_per_s_step": s["rows_per_s"],
    }

doc = {
 "schema": "ws0-3224.capture/v1",
 "label": "$LABEL", "rep": int("$REP"),
 "S_physical_cores": int("$S"), "N_streams": int("$N"),
 "step_secs": float("$STEP_SECS"),
 "window_secs": float("$WINDOW_SECS"),
 "settle_secs": float("$SETTLE_SECS"),
 "server_cpus": "$SERVER_CPUS",
 "client_cpus": "$CLIENT_CPUS",
 "numa_node": int("$WS0_NUMA_NODE"),
 "server_hw_threads": len("$SERVER_CPUS".replace("-", ",").split(",")) and None,
 "merge_path": "bypass",
 "corpus_rows": rows,
 "server_flags": {
   "batch_size": int("$WS0_BATCH_SIZE"),
   "max_batch_bytes": int("$WS0_MAX_BATCH_BYTES"),
   "max_inflight_egress_bytes": int("$WS0_MAX_INFLIGHT_EGRESS_BYTES"),
   "max_concurrent_scans": int("$WS0_MAX_CONCURRENT_SCANS"),
   "admission_wait_timeout_ms": int("$WS0_ADMISSION_WAIT_TIMEOUT_MS"),
 },
 "rc": {"core_interior": int("$RC_CORE"), "loadgen_interior": int("$RC_LG_A"),
        "aligned": int("$RC_ALIGNED"),
        "uncore": int("$RC_UNCORE"), "loadgen_uncore": int("$RC_LG_C")},
 "steps": {"interior": si, "aligned": sa, "uncore": su},
 "occupancy": {"interior": occupancy(si), "aligned": occupancy(sa),
               "uncore": occupancy(su)},
 "server_io_before": json.loads('''$IO_BEFORE'''),
 "server_io_after":  json.loads('''$IO_AFTER'''),
 "server_ctxt":      json.loads('''$CTXT'''),
 "server_cpu_secs_before": float("$CPU_BEFORE"),
 "server_cpu_secs_after":  float("$CPU_AFTER"),
 "client_busy_secs_before": float("$CLIENT_BUSY_BEFORE"),
 "client_busy_secs_after":  float("$CLIENT_BUSY_AFTER"),
 "wall_before": float("$WALL_BEFORE"), "wall_after": float("$WALL_AFTER"),
}
# Warmth verified, not assumed: read_bytes delta must be 0. rchar/read_bytes/
# syscr are three different layers and are NEVER divided by one another.
rb0 = doc["server_io_before"].get("read_bytes"); rb1 = doc["server_io_after"].get("read_bytes")
doc["warm_read_bytes_delta"] = (rb1 - rb0) if (rb0 is not None and rb1 is not None) else None
doc["warm_verified_zero_disk_reads"] = (doc["warm_read_bytes_delta"] == 0)
# Client-saturation validity gate: >70% busy means the point measured the client.
wall = doc["wall_after"] - doc["wall_before"]
ncli = len(set(sum(([int(x)] if "-" not in p else list(range(int(p.split("-")[0]), int(p.split("-")[1])+1))
                    for p in "$CLIENT_CPUS".split(",") for x in [p]), [])))
busy = doc["client_busy_secs_after"] - doc["client_busy_secs_before"]
doc["client_cpu_count"] = ncli
doc["client_utilisation"] = (busy/(wall*ncli)) if wall and ncli else None
doc["client_saturation_gate_pass"] = (doc["client_utilisation"] is not None
                                      and doc["client_utilisation"] <= float("$WS0_CLIENT_SAT_THRESHOLD"))
open("$OUT/meta.json","w").write(json.dumps(doc, indent=1)+"\n")
print("meta -> $OUT/meta.json")
print("  warm read_bytes delta:", doc["warm_read_bytes_delta"],
      "| client util: %.4f" % (doc["client_utilisation"] or -1),
      "| gate:", "PASS" if doc["client_saturation_gate_pass"] else "FAIL")
for k, v in doc["occupancy"].items():
    if v: print("  occupancy[%s]: whole_scans=%.3f exact=%s busy_frac=%.4f rows/s=%.0f"
                % (k, v["whole_scans"], v["rows_total_is_exact_multiple_of_corpus"],
                   v["busy_fraction_estimate"] or -1, v["rows_per_s_step"]))
PY
RC_META=$?
ws0_log "[$LABEL rep$REP] done rc(core=$RC_CORE aligned=$RC_ALIGNED uncore=$RC_UNCORE meta=$RC_META)"
[ "$RC_CORE" -eq 0 ] && [ "$RC_ALIGNED" -eq 0 ] && [ "$RC_UNCORE" -eq 0 ] && [ "$RC_META" -eq 0 ]
