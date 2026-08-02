#!/usr/bin/env bash
# Issue #3217 AC4 + AC5: off-CPU (blocked-time) flame graphs, the ranked
# blocked-stack attribution table, and the per-N scheduler-cost table.
#
# This is the instrument that can actually INDICT OR ACQUIT the do_get mpsc
# handoff. An on-CPU profile cannot: a thread parked on a full bounded channel
# burns no cycles and is invisible to `perf record`. Blocked time is where the
# handoff cost, if there is one, lives.
#
# Usage:
#   profile-offcpu.sh <label> <server-cpu-spec> <N-list> <duration-secs> [bypass|merge]
#
# e.g.  ./profile-offcpu.sh offcpu-s6 s6 1,8,16 30
#       ./profile-offcpu.sh offcpu-s1 s1 1,8,16 30
#
# Collector: offcputime-bpfcc -f -p <pid> <dur>  (folded, microseconds).
# Fallback:  offcpu-fallback.bt (sched-switch kprobe, kernel+user stacks, folded
#            to the SAME microsecond unit). Which one ran is recorded in
#            run-config.json and in the classified table's label - never guess.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

usage() { sed -n '2,22p' "${BASH_SOURCE[0]}" >&2; exit 2; }
[ $# -ge 4 ] || usage

LABEL="$1"; SRV_SPEC="$2"; NLIST="$3"; DURATION="$4"; MERGE_PATH="${5:-bypass}"
STEADY_PRE="${WS0_STEADY_PRE_SECS:-20}"
TAIL="${WS0_TAIL_SECS:-10}"
OFFCPU_TOOL="${WS0_OFFCPU_TOOL:-auto}"     # auto | bpfcc | bpftrace
STACK_STORAGE="${WS0_STACK_STORAGE_SIZE:-32768}"

case "$MERGE_PATH" in bypass|merge) ;; *) ws0_die "merge-path must be bypass|merge";; esac
[[ "$NLIST" =~ ^[0-9]+(,[0-9]+)*$ ]] || ws0_die "N-list must be a comma list of integers"
case "$SRV_SPEC" in
  s1|s2|s4|s6) S_CORES="${SRV_SPEC#s}"; SERVER_CPUS="$(ws0_server_cpus_for_s "$S_CORES")" ;;
  *)           S_CORES=""; SERVER_CPUS="$SRV_SPEC" ;;
esac
SERVER_CPUS="$(ws0_cpulist_expand "$SERVER_CPUS")"
CLIENT_CPUS="$(ws0_cpulist_expand "${WS0_CLIENT_CPUS:-$WS0_CLIENT_CPUS_DEFAULT}")"

OUTDIR="$WS0_PROFILES/$LABEL"; LOGDIR="$WS0_LOGS/$LABEL"
mkdir -p "$OUTDIR" "$LOGDIR"
SCHED_JSONL="$OUTDIR/scheduler-cost.jsonl"
: >"$SCHED_JSONL"

ws0_assert_sysctl
ws0_verify_topology "$OUTDIR/cpu-topology.json"

# ---- pick the collector ------------------------------------------------------
OFFCPU_BPFCC="${WS0_OFFCPUTIME_BIN:-/usr/sbin/offcputime-bpfcc}"
choose_tool() {
  if [ "$OFFCPU_TOOL" = "bpftrace" ]; then echo bpftrace; return; fi
  if [ -x "$OFFCPU_BPFCC" ]; then
    # Probe it against ourselves for 1s: a load failure surfaces here, not
    # mid-measurement with the load running.
    if "$OFFCPU_BPFCC" -f -p $$ 1 >"$LOGDIR/offcpu-probe.log" 2>&1; then echo bpfcc; return; fi
    ws0_warn "offcputime-bpfcc probe failed (see $LOGDIR/offcpu-probe.log) — falling back to bpftrace"
  else
    ws0_warn "offcputime-bpfcc not found at $OFFCPU_BPFCC — falling back to bpftrace"
  fi
  echo bpftrace
}
TOOL="$(choose_tool)"
ws0_log "off-CPU collector: $TOOL"

fold_offcpu() {  # fold_offcpu <pid> <secs> <out-folded> <raw-log>
  local pid="$1" secs="$2" out="$3" raw="$4"
  if [ "$TOOL" = "bpfcc" ]; then
    "$OFFCPU_BPFCC" -f -p "$pid" --stack-storage-size "$STACK_STORAGE" "$secs" \
      >"$out" 2>"$raw" || ws0_warn "offcputime-bpfcc returned non-zero; see $raw"
  else
    bpftrace "$HARNESS_DIR/offcpu-fallback.bt" "$pid" "$secs" >"$raw.bt" 2>"$raw" \
      || ws0_warn "bpftrace returned non-zero; see $raw"
    "$FLAMEGRAPH_DIR/stackcollapse-bpftrace.pl" "$raw.bt" >"$out" 2>>"$raw" || true
  fi
}

# ---- dry run: prove the fold -> flamegraph -> classify pipeline works ---------
if [ "${WS0_DRY_RUN:-0}" = "1" ]; then
  ws0_log "WS0_DRY_RUN=1: capturing off-CPU stacks of a real 'sleep' process to exercise the pipeline"
  sleep 6 & PROBE=$!
  fold_offcpu "$PROBE" 4 "$OUTDIR/offcpu-dryrun.folded" "$LOGDIR/offcpu-dryrun.log"
  kill "$PROBE" 2>/dev/null || true; wait "$PROBE" 2>/dev/null || true
  if [ -s "$OUTDIR/offcpu-dryrun.folded" ]; then
    "$FLAMEGRAPH_DIR/flamegraph.pl" --countname=us --title="Off-CPU dry run" \
      --colors=io --width 1600 "$OUTDIR/offcpu-dryrun.folded" >"$OUTDIR/offcpu-dryrun.svg" 2>/dev/null
    python3 "$HARNESS_DIR/classify-offcpu.py" "$OUTDIR/offcpu-dryrun.folded" \
      --label "$LABEL-dryrun" --out-json "$OUTDIR/offcpu-dryrun.attribution.json" \
      --out-table "$OUTDIR/offcpu-dryrun.attribution.txt"
    cat "$OUTDIR/offcpu-dryrun.attribution.txt"
  else
    ws0_warn "dry-run capture produced no stacks (a sleeping 'sleep' may block in a way the probe missed)"
  fi
  ws0_log "dry run complete: collector=$TOOL, fold/flamegraph/classify pipeline exercised"
  exit 0
fi

# ---- real run ----------------------------------------------------------------
ws0_require_inputs
trap 'ws0_stop_server; [ -n "${LOADGEN_PID:-}" ] && kill -9 "$LOADGEN_PID" 2>/dev/null || true' EXIT INT TERM
ws0_start_server "$SERVER_CPUS" "$MERGE_PATH" "$LOGDIR/server.log"
[ "${WS0_WARM_SECS:-45}" -gt 0 ] && ws0_warm_prepass "$CLIENT_CPUS" "${WS0_WARM_SECS:-45}" "$LOGDIR/prewarm.log"

RUNQLAT="${WS0_RUNQLAT_BIN:-/usr/sbin/runqlat-bpfcc}"
LOAD_SECS=$(( STEADY_PRE + DURATION + TAIL ))

for N in ${NLIST//,/ }; do
  TAG="N$N"
  ws0_log "=== off-CPU capture $TAG (${DURATION}s window after ${STEADY_PRE}s steady state) ==="

  taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" \
      --endpoint "$WS0_ENDPOINT" --ticket-template "$WS0_TICKET_TPL" \
      --shape full --ramp "$N" --step-duration "${LOAD_SECS}s" --seed "$WS0_SEED" \
      --round "$LABEL-$TAG" --out "$LOGDIR/step-$TAG.jsonl" >"$LOGDIR/loadgen-$TAG.log" 2>&1 &
  LOADGEN_PID=$!
  sleep "$STEADY_PRE"

  CT0="$(ws0_proc_ctxt_json "$WS0_SERVER_PID")"

  # AC5: run-queue latency alongside the off-CPU capture, same window.
  RUNQLAT_TXT="$OUTDIR/runqlat-$TAG.txt"
  if [ -x "$RUNQLAT" ]; then
    "$RUNQLAT" -p "$WS0_SERVER_PID" "$DURATION" 1 >"$RUNQLAT_TXT" 2>"$LOGDIR/runqlat-$TAG.log" &
    RQ_PID=$!
  else
    ws0_warn "runqlat-bpfcc not found at $RUNQLAT — run-queue latency will be reported as unavailable"
    RQ_PID=""
  fi

  # AC5: perf stat over the server CPU set, same window, for cpu-wide cs/s.
  perf stat -x, -e context-switches,cpu-migrations,task-clock -C "$SERVER_CPUS" \
    -o "$OUTDIR/perf-cs-$TAG.csv" -- sleep "$DURATION" >"$LOGDIR/perf-cs-$TAG.log" 2>&1 &
  PERF_PID=$!

  fold_offcpu "$WS0_SERVER_PID" "$DURATION" "$OUTDIR/offcpu-$TAG.folded" "$LOGDIR/offcpu-$TAG.log"

  wait "$PERF_PID" 2>/dev/null || true
  [ -n "$RQ_PID" ] && { wait "$RQ_PID" 2>/dev/null || true; }
  CT1="$(ws0_proc_ctxt_json "$WS0_SERVER_PID")"

  kill -TERM "$LOADGEN_PID" 2>/dev/null || true
  wait "$LOADGEN_PID" 2>/dev/null || true
  unset LOADGEN_PID

  # ---- AC4: flame graph + ranked classified attribution table ----
  if [ -s "$OUTDIR/offcpu-$TAG.folded" ]; then
    "$FLAMEGRAPH_DIR/flamegraph.pl" --countname=us --colors=io --width 1600 \
      --title "Off-CPU $LABEL $TAG (server cpus $SERVER_CPUS, merge_path=$MERGE_PATH, collector=$TOOL)" \
      --subtitle "blocked time in microseconds | issue #3217 AC4" \
      "$OUTDIR/offcpu-$TAG.folded" >"$OUTDIR/offcpu-$TAG.svg" 2>"$LOGDIR/flamegraph-$TAG.log"
    python3 "$HARNESS_DIR/classify-offcpu.py" "$OUTDIR/offcpu-$TAG.folded" \
      --label "$LABEL-$TAG (collector=$TOOL)" --top 25 \
      --out-json "$OUTDIR/offcpu-$TAG.attribution.json" \
      --out-table "$OUTDIR/offcpu-$TAG.attribution.txt"
    cat "$OUTDIR/offcpu-$TAG.attribution.txt"
  else
    ws0_warn "no off-CPU stacks captured for $TAG — see $LOGDIR/offcpu-$TAG.log"
  fi

  if [ -s "$RUNQLAT_TXT" ]; then
    python3 "$HARNESS_DIR/parse-runqlat.py" "$RUNQLAT_TXT" --label "$LABEL-$TAG" \
      --out "$OUTDIR/runqlat-$TAG.json" >/dev/null
  fi

  # ---- AC5 record ----
  python3 - "$SCHED_JSONL" <<PYSCHED
import json, os, sys
ct0, ct1 = json.loads('''$CT0'''), json.loads('''$CT1''')
perf = {}
p = "$OUTDIR/perf-cs-$TAG.csv"
if os.path.exists(p):
    for line in open(p):
        f = line.strip().split(",")
        if len(f) > 2 and f[2].strip() and f[0].strip() not in ("<not counted>", "<not supported>", ""):
            try: perf[f[2].strip()] = float(f[0])
            except ValueError: pass
rq = None
rqp = "$OUTDIR/runqlat-$TAG.json"
if os.path.exists(rqp):
    rq = json.load(open(rqp))
att = None
ap_ = "$OUTDIR/offcpu-$TAG.attribution.json"
if os.path.exists(ap_):
    a = json.load(open(ap_))
    att = {"total_blocked_time_us": a["total_blocked_time_us"],
           "buckets": {b["bucket"]: b["blocked_time_us"] for b in a["buckets"]}}
d = float($DURATION)
rec = {
 "schema": "ws0-3217.scheduler-cost/v1", "label": "$LABEL", "N": $N,
 "server_cpus": "$SERVER_CPUS", "merge_path": "$MERGE_PATH", "window_secs": d,
 "offcpu_collector": "$TOOL",
 "server_voluntary_ctxt_switches": ct1.get("voluntary_ctxt_switches", 0) - ct0.get("voluntary_ctxt_switches", 0),
 "server_nonvoluntary_ctxt_switches": ct1.get("nonvoluntary_ctxt_switches", 0) - ct0.get("nonvoluntary_ctxt_switches", 0),
 "server_voluntary_ctxt_switches_per_s": (ct1.get("voluntary_ctxt_switches", 0) - ct0.get("voluntary_ctxt_switches", 0)) / d,
 "server_nonvoluntary_ctxt_switches_per_s": (ct1.get("nonvoluntary_ctxt_switches", 0) - ct0.get("nonvoluntary_ctxt_switches", 0)) / d,
 "ctxt_scope_note": "/proc/<pid>/status counters are MAIN-THREAD only and under-count a multi-threaded server; the perf -C figure below is the whole pinned set",
 "context_switches_cpu_wide": perf.get("context-switches"),
 "context_switches_per_second_cpu_wide": (perf["context-switches"] / d) if perf.get("context-switches") else None,
 "cpu_migrations_cpu_wide": perf.get("cpu-migrations"),
 "runqueue_latency": rq if rq else {"available": False, "reason": "runqlat-bpfcc not present or produced no output"},
 "offcpu_attribution": att,
}
open(sys.argv[1], "a").write(json.dumps(rec) + "\n")
PYSCHED
done

ws0_stop_server
trap - EXIT INT TERM

# ---- AC5 per-N table ---------------------------------------------------------
python3 - "$SCHED_JSONL" "$OUTDIR/scheduler-cost.txt" <<'PYTBL'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
recs = [json.loads(l) for l in open(src) if l.strip()]
L = ["==== WS0 #3217 SCHEDULER COST PER N (AC5) ===="]
if recs:
    L.append("label: %s   server_cpus=%s   merge_path=%s   collector=%s   window=%.0fs" % (
        recs[0]["label"], recs[0]["server_cpus"], recs[0]["merge_path"],
        recs[0]["offcpu_collector"], recs[0]["window_secs"]))
L.append("")
L.append("%-5s %14s %16s %18s %14s %-22s" % (
    "N", "cs/s cpu-wide", "vol cs/s (proc)", "nonvol cs/s (proc)", "migrations", "runq p99 bucket"))
for r in recs:
    rq = r.get("runqueue_latency") or {}
    p99 = rq.get("p99_bucket_usecs") or rq.get("p99_bucket_msecs")
    unit = rq.get("unit", "")
    L.append("%-5s %14s %16.1f %18.1f %14s %-22s" % (
        r["N"],
        ("%.0f" % r["context_switches_per_second_cpu_wide"]) if r.get("context_switches_per_second_cpu_wide") else "n/a",
        r["server_voluntary_ctxt_switches_per_s"], r["server_nonvoluntary_ctxt_switches_per_s"],
        r.get("cpu_migrations_cpu_wide") if r.get("cpu_migrations_cpu_wide") is not None else "n/a",
        ("[%s,%s] %s" % (p99[0], p99[1], unit)) if p99 else "unavailable"))
L.append("")
L.append("blocked-time attribution per N (seconds, AC4 buckets; explicit 0 = measured absent):")
buckets = ["mpsc_send_park", "mpsc_recv_park", "egress_credit_acquire",
           "tonic_grpc_socket_write", "tokio_scheduler", "disk_io", "other"]
L.append("%-5s %12s " % ("N", "total") + " ".join("%-13s" % b[:13] for b in buckets))
for r in recs:
    a = r.get("offcpu_attribution")
    if not a:
        L.append("%-5s %12s  (no off-CPU capture)" % (r["N"], "n/a")); continue
    L.append("%-5s %12.4f " % (r["N"], a["total_blocked_time_us"] / 1e6)
             + " ".join("%-13.4f" % (a["buckets"].get(b, 0) / 1e6) for b in buckets))
open(dst, "w").write("\n".join(L) + "\n")
print("\n".join(L))
PYTBL

cat >"$OUTDIR/run-config.json" <<EOF
{"label":"$LABEL","kind":"offcpu","server_physical_cores_S":"${S_CORES:-custom}",
 "server_cpus":"$SERVER_CPUS","client_cpus":"$CLIENT_CPUS","n_list":"$NLIST",
 "merge_path":"$MERGE_PATH","window_secs":$DURATION,"steady_pre_secs":$STEADY_PRE,
 "offcpu_collector":"$TOOL","stack_storage_size":$STACK_STORAGE,"utc":"$(date -u +%FT%TZ)"}
EOF
ws0_log "artefacts: $OUTDIR/{offcpu-N*.svg,offcpu-N*.folded,offcpu-N*.attribution.{json,txt},runqlat-N*.{txt,json},scheduler-cost.{jsonl,txt}}"
