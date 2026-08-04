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
# shellcheck source=../harness/guards.sh
source "$HERE/../harness/guards.sh"

LABEL="${1:?label}"; S="${2:?S}"; N="${3:?N}"
STEP_SECS="${4:?step_secs}"; WINDOW_SECS="${5:?window_secs}"
REP="${6:?rep}"; OUT="${7:?outdir}"

CORPUS_ROWS="${CORPUS_ROWS:-3999890}"
SETTLE_SECS="${SETTLE_SECS:-12}"   # skip the leading part of the step for arm (a)

# THE COUNTER WINDOW MUST FIT INSIDE THE LOAD-GENERATOR STEP (roborev round 7
# finding #7). Nothing required SETTLE_SECS + WINDOW_SECS <= STEP_SECS, and with a
# shortened step the load generator finishes while `perf stat -- sleep WINDOW_SECS`
# keeps counting IDLE server CPUs. Every gate still passes, because occupancy
# describes the COMPLETED STEP and not the perf window: rows are positive, scans are
# whole, errors are zero, the rc of a `sleep` that slept is 0. The result is a
# per-row figure whose counters cover a partly-idle interval and whose denominator
# covers a fully-busy one — the numerator/denominator-from-different-intervals error
# the ALIGNED convention exists to avoid, reachable purely by mis-sizing a flag.
#
# The margin is not cosmetic: the loadgen DRAINS in-flight requests, so the actual
# duration EXCEEDS the requested step (run-all.sh records 120s requested -> 144.2s
# actual). That drain is why a window ending exactly at the step boundary is normally
# fine, and it is exactly why relying on it silently would be relying on an
# undocumented cushion. Required explicitly instead.
if ! awk -v s="$SETTLE_SECS" -v w="$WINDOW_SECS" -v t="$STEP_SECS" \
        'BEGIN { exit !(s >= 0 && w > 0 && t > 0 && s + w <= t) }'; then
  echo "FATAL: settle(${SETTLE_SECS}s) + window(${WINDOW_SECS}s) must fit inside the requested step(${STEP_SECS}s), and all three must be positive." >&2
  echo "       Otherwise the load generator finishes while perf keeps counting IDLE" >&2
  echo "       server CPUs, and every validity gate still passes because occupancy" >&2
  echo "       describes the completed STEP, not the counter window." >&2
  exit 2
fi

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

# ---------------------------------------------------------------- event groups
# THE RUNBOOK'S 11-EVENT CORE SET MULTIPLEXES ON THIS BOX AND MUST BE SPLIT.
# Measured in the first smoke capture, `perf stat -x,` field 5 (enabled %):
#   cycles 79 | instructions 89 | LLC-loads 90 | LLC-load-misses 70
#   cache-references 80 | cache-misses 90 | L1-dcache-loads 90
#   L1-dcache-load-misses 90 | dTLB-load-misses 59 | branch-misses 69
#   task-clock 100
# Every count except task-clock was a SCALED ESTIMATE. RUNBOOK step 6: "If the
# core set multiplexes, split it into two invocations rather than publishing
# scaled values silently." So we split, and every group is verified at 100%
# enabled before its numbers are used (the derivation fails closed otherwise).
#
# Probed on this host: each 7-event group below reads 100.00 enabled.
# cycles/instructions use fixed-function counters and task-clock is software, so
# the binding constraint is the 4 GP counters/thread with SMT on — hence 4
# hardware events per group.
#
# cycles + instructions + task-clock are DELIBERATELY REPEATED in both groups.
# That is not redundancy: it lets the derivation cross-check that the two groups
# observed the same workload (IPC must agree between them), which is the same
# kind of symmetry control the positive control's P2 applies to its two arms.
CORE_EVENTS_A="cycles,instructions,task-clock,LLC-loads,LLC-load-misses,cache-references,cache-misses"
CORE_EVENTS_B="cycles,instructions,task-clock,L1-dcache-loads,L1-dcache-load-misses,dTLB-load-misses,branch-misses"
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

# ---------------------------------------------------------------- arm runners
# ALIGNED: perf runs the loadgen as its OWN CHILD, so the counted interval and
# the row-producing interval are the same interval — numerator and denominator
# share one window BY CONSTRUCTION, no rate assumption needed.
aligned_arm() { # $1 events  $2 perf-out  $3 round  $4 step-jsonl  $5 log
  perf stat -x, -C "$SERVER_CPUS" -e "$1" -o "$2" -- \
    taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" \
      --endpoint "$WS0_ENDPOINT" --ticket-template "$WS0_TICKET_TPL" \
      --shape full --ramp "$N" --step-duration "${STEP_SECS}s" \
      --seed "$WS0_SEED" --round "$3" --out "$4" > "$5" 2>&1
}

# ================= ARM (a1): ALIGNED, group A — THE PRIMARY NUMBERS =========
ws0_log "[$LABEL rep$REP] arm(a1) ALIGNED groupA (cycles/instr/LLC/cache)"
aligned_arm "$CORE_EVENTS_A" "$OUT/perf-coreA-aligned.csv" \
  "${LABEL}-alignedA" "$OUT/step-alignedA.jsonl" "$OUT/loadgen-alignedA.log"
RC_ALIGNED_A=$?                               # captured IMMEDIATELY

# ================= ARM (a2): ALIGNED, group B — attribution counters ========
ws0_log "[$LABEL rep$REP] arm(a2) ALIGNED groupB (L1d/dTLB/branch)"
aligned_arm "$CORE_EVENTS_B" "$OUT/perf-coreB-aligned.csv" \
  "${LABEL}-alignedB" "$OUT/step-alignedB.jsonl" "$OUT/loadgen-alignedB.log"
RC_ALIGNED_B=$?                               # captured IMMEDIATELY

# ================= ARM (b): INTERIOR window, group A =======================
# Reproduces #3217's convention exactly: counters over an interior WINDOW_SECS
# slice starting SETTLE_SECS into the step, rate taken from the whole step.
# Compared against arm (a1) to settle whether the two conventions agree.
ws0_log "[$LABEL rep$REP] arm(b) INTERIOR groupA: settle=${SETTLE_SECS}s window=${WINDOW_SECS}s"
loadgen "${LABEL}-interior" "$OUT/step-interior.jsonl" "$OUT/loadgen-interior.log" &
LG_PID=$!
sleep "$SETTLE_SECS"
perf stat -x, -C "$SERVER_CPUS" -e "$CORE_EVENTS_A" \
  -o "$OUT/perf-coreA-interior.csv" -- sleep "$WINDOW_SECS" >/dev/null 2>&1
RC_CORE=$?                                    # captured IMMEDIATELY
wait "$LG_PID"; RC_LG_A=$?                    # captured IMMEDIATELY

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
si  = step("$OUT/step-interior.jsonl")
saA = step("$OUT/step-alignedA.jsonl")
saB = step("$OUT/step-alignedB.jsonl")
su  = step("$OUT/step-uncore.jsonl")
rows = int("$CORPUS_ROWS")

def occupancy(s):
    """What makes the INTERIOR convention legitimate: whole scans only, and
    workers busy for the whole step (no idle ramp/drain).

    FAIL-CLOSED ON AN EMPTY RUN. The first smoke run of this script returned
    rc=0 with rows_total=0 and 2,258,606 NotFound errors, because the corpus was
    staged FLAT and cqlite-flight logged "discovered 0 tables across 0
    keyspaces" (it needs <keyspace>/<table>-<uuid>/). The old check said
    exact=True because 0 % rows == 0 — a vacuously passing empty measurement,
    exactly what CLAUDE.md forbids ("never let a dataset-dependent test pass on
    an empty dataset; 0-rows-when-present is a failure"). So require positive
    rows, whole scans, and zero errors, and surface an ok flag for the caller.

    NOTE: this heredoc is UNQUOTED (it interpolates $OUT, $CORPUS_ROWS, ...), so
    bash still does command substitution inside it. Never write backticks or
    $(...) in these docstrings — an earlier version said `ok` here and bash
    dutifully tried to run a command named ok."""
    if not s: return None
    d = s["duration_s"]; n = s["target_concurrency"]
    ok = s["requests_ok"]; rt = s["rows_total"]
    err = s.get("requests_error", 0); unav = s.get("requests_unavailable", 0)
    p50 = s["latency_ms"]["p50"]/1000.0
    return {
      "rows_total": rt,
      "rows_positive": rt > 0,
      "requests_error": err,
      "requests_unavailable": unav,
      "error_codes": s.get("error_codes", {}),
      "rows_total_is_exact_multiple_of_corpus": (rt > 0 and rt % rows == 0),
      "whole_scans": rt/rows,
      "requests_ok": ok,
      "duration_s": d,
      "p50_latency_s": p50,
      "busy_fraction_estimate": (ok*p50/n/d) if (n and d) else None,
      "rows_per_s_step": s["rows_per_s"],
      # BUSY FRACTION IS GATED, NOT MERELY RECORDED (roborev round 4 finding #1).
      #
      # This field existed, was printed, and was left out of "ok" — so an arm with
      # long idle stretches passed every validity gate. That matters specifically for
      # the INTERIOR convention, whose whole legitimacy is the assumption that the
      # whole-step throughput represents the interior perf window: if the workers were
      # idle for part of the step, the interior window and the step have different
      # rates and the per-row figures divide counters from one interval by rows from
      # another. That is the numerator/denominator-from-different-intervals error the
      # ALIGNED convention exists to avoid, arriving through the back door.
      #
      # The floor is deliberately LOW (0.90). It is a check against IDLE PERIODS, not
      # a tuning target, and the estimate is ok*p50/n/d — a product of three measured
      # quantities, so it carries their combined error. A tight floor here would be a
      # false-FAIL generator, which is finding #1 of round 1 all over again. On the
      # committed reps this lands at ~0.99, so 0.90 has real margin.
      #
      # None (not computable, when n or d is zero) is a FAILURE, not a pass: it means
      # the step recorded no concurrency or no duration, and an unverifiable
      # occupancy is not an established one.
      "busy_fraction_floor": float("$WS0_BUSY_FRACTION_FLOOR"),
      "busy_fraction_ok": bool(
          (ok*p50/n/d) >= float("$WS0_BUSY_FRACTION_FLOOR")) if (n and d) else False,
      "ok": bool(rt > 0 and rt % rows == 0 and err == 0 and unav == 0 and ok > 0
                 and (n and d)
                 and (ok*p50/n/d) >= float("$WS0_BUSY_FRACTION_FLOOR")),
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
        "alignedA": int("$RC_ALIGNED_A"), "alignedB": int("$RC_ALIGNED_B"),
        "uncore": int("$RC_UNCORE"), "loadgen_uncore": int("$RC_LG_C")},
 "event_groups": {"A": "$CORE_EVENTS_A", "B": "$CORE_EVENTS_B"},
 "perf_files": {
   "alignedA": "perf-coreA-aligned.csv", "alignedB": "perf-coreB-aligned.csv",
   "interiorA": "perf-coreA-interior.csv", "uncore": "perf-uncore.csv"},
 "steps": {"interior": si, "alignedA": saA, "alignedB": saB, "uncore": su},
 "occupancy": {"interior": occupancy(si), "alignedA": occupancy(saA),
               "alignedB": occupancy(saB), "uncore": occupancy(su)},
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
bad = []
for k, v in doc["occupancy"].items():
    if not v:
        bad.append("%s: NO STEP RECORD" % k); continue
    print("  occupancy[%s]: rows=%d whole_scans=%.3f exact=%s err=%d busy_frac=%.4f rows/s=%.0f ok=%s"
          % (k, v["rows_total"], v["whole_scans"],
             v["rows_total_is_exact_multiple_of_corpus"], v["requests_error"],
             v["busy_fraction_estimate"] or -1, v["rows_per_s_step"], v["ok"]))
    if not v["ok"]:
        bad.append("%s: rows=%d err=%d codes=%s"
                   % (k, v["rows_total"], v["requests_error"], v["error_codes"]))
# The recorded return codes are a VALIDITY GATE, not decoration (roborev finding
# #4, PR #3286). meta.json has always carried all six, including the two load
# generators, but nothing here read them — so a rep with loadgen_uncore=1 wrote a
# structurally perfect meta.json, passed every gate below, and was later SKIPPED
# ON RESUME as "already complete and valid". Checked here as well as in the
# caller's shell condition deliberately: this is the copy that travels with the
# artefact, so anything re-reading meta.json (run-all.sh's resume predicate,
# derive.py, a future operator) inherits the same refusal.
#
# Enumerated from the dict rather than by name: a hardcoded roster is how the
# shell condition came to omit two arms, and a new arm added to "rc" must not
# default to unchecked.
_rc = doc.get("rc")
if not isinstance(_rc, dict) or not _rc:
    bad.append("rc: block absent or empty — a capture with no recorded return "
               "codes cannot be certified")
else:
    _nz = {k: v for k, v in _rc.items() if v != 0}
    if _nz:
        bad.append("rc: nonzero arm(s) %s (all of %s must be 0)"
                   % (_nz, sorted(_rc)))
if not doc["warm_verified_zero_disk_reads"]:
    bad.append("warmth: read_bytes delta = %s (want 0)" % doc["warm_read_bytes_delta"])
if not doc["client_saturation_gate_pass"]:
    bad.append("client saturation: util=%s > %s — this point measured the CLIENT"
               % (doc["client_utilisation"], "$WS0_CLIENT_SAT_THRESHOLD"))
if bad:
    raise SystemExit("CAPTURE INVALID (fail-closed):\n  - " + "\n  - ".join(bad))
print("  ALL VALIDITY GATES PASS")
PY
RC_META=$?
ws0_log "[$LABEL rep$REP] done rc(alignedA=$RC_ALIGNED_A alignedB=$RC_ALIGNED_B interiorA=$RC_CORE loadgenInterior=$RC_LG_A uncore=$RC_UNCORE loadgenUncore=$RC_LG_C meta=$RC_META)"
# Fail closed on ANY non-zero arm, and let the guard NAME the arms it tested.
#
# THIS EXPRESSION USED TO OMIT RC_LG_A AND RC_LG_C (roborev finding #4, PR #3286)
# while the comment above it claimed to "fail closed on ANY non-zero arm". Those
# two are the load generators for the interior and uncore arms — i.e. the
# processes that produce the ROWS that are the denominator of every per-row
# figure. A load generator that died mid-step leaves perf's own rc at 0 (perf was
# wrapping `sleep`, which succeeded) and a structurally valid meta.json, so the
# capture returned SUCCESS. A validity expression that omits an arm it advertises
# is worse than one that never claimed to cover it, because the claim is what
# stops anyone from checking.
#
# The guard prints its roster, so the coverage is visible in the log rather than
# inferred from this source line. RC_META is non-zero when a meta.json validity
# gate (occupancy / warmth / client saturation / recorded rc) failed.
ws0_guard_all_rc_zero \
  "alignedA=$RC_ALIGNED_A" "alignedB=$RC_ALIGNED_B" \
  "interiorA=$RC_CORE" "loadgenInterior=$RC_LG_A" \
  "uncore=$RC_UNCORE" "loadgenUncore=$RC_LG_C" \
  "meta=$RC_META"
