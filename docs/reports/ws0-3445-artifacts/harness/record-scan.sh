#!/usr/bin/env bash
# record-scan.sh — take ONE perf observation of a WARM, PINNED, steady-state bare scan
# (issue #3445).
#
# It does not measure anything itself. Its whole job is to place a `perf` window entirely
# inside the #3299 scan worker's post-barrier steady state, so that no page-cache
# first-touch, no ingestion/schema setup and no process teardown lands in the samples.
#
# The worker (`docs/reports/ws0-3299-artifacts/harness/scan-worker`) is REUSED rather than
# reimplemented: it already drives `Database::execute_streaming` over the pinned corpus with
# `ws0_corpus_gen::scan_scope::verify_exact_scope`, already treats a 0-row pass as a failure
# rather than a measurement, and already prewarms before signalling ready. This script adds
# only the perf window and the pinning.
#
# WARM is structural here, not a convention: the worker writes `ready-0` only after
# `--prewarm-passes` full untimed passes, and this script does not start `perf` until it has
# seen that file AND released the barrier AND waited out `--settle`.
#
# Usage:
#   record-scan.sh --out DIR --binary PATH [--event EV] [--period N] [--secs N]
#                  [--cpu N] [--settle N] [--stat-events LIST]
#
# Two modes, selected by --mode:
#   record  perf record (sampling) -> perf.data, for annotate/srcline attribution (AC1)
#   stat    perf stat (counting)   -> counters.csv with pct_running, for AC2
set -euo pipefail

CORPUS=/data/ws0-3096
OUT=; BINARY=; EVENT=cycles; PERIOD=500009; SECS=40; CPU=2; SETTLE=5; MODE=record
# Quiescence bound. A rep is publishable only if the box was quiet across the WHOLE rep,
# not merely at its start, so the load is SAMPLED THROUGHOUT and the maximum is checked.
# 0 disables the check (and says so in the verdict file) -- it never silently disables it.
MAX_LOAD=3.0; LOAD_SAMPLE_SECS=5
STAT_EVENTS='cycles,instructions,cycle_activity.stalls_total,cycle_activity.stalls_l1d_miss,idq_uops_not_delivered.core,int_misc.recovery_cycles'
while [ $# -gt 0 ]; do
  case "$1" in
    --out) OUT=$2; shift 2;;
    --binary) BINARY=$2; shift 2;;
    --event) EVENT=$2; shift 2;;
    --period) PERIOD=$2; shift 2;;
    --secs) SECS=$2; shift 2;;
    --cpu) CPU=$2; shift 2;;
    --settle) SETTLE=$2; shift 2;;
    --mode) MODE=$2; shift 2;;
    --stat-events) STAT_EVENTS=$2; shift 2;;
    --max-load) MAX_LOAD=$2; shift 2;;
    --load-sample-secs) LOAD_SAMPLE_SECS=$2; shift 2;;
    --corpus) CORPUS=$2; shift 2;;
    *) echo "record-scan.sh: unknown argument: $1" >&2; exit 2;;
  esac
done
[ -n "$OUT" ] && [ -n "$BINARY" ] || { echo "record-scan.sh: --out and --binary are required" >&2; exit 2; }
[ -x "$BINARY" ] || { echo "record-scan.sh: not executable: $BINARY" >&2; exit 2; }
[ -d "$CORPUS/ws0/events" ] || { echo "record-scan.sh: no corpus at $CORPUS/ws0/events" >&2; exit 2; }

mkdir -p "$OUT"
RUNDIR=$(mktemp -d /tmp/ws0-3445-rep.XXXXXX)
# The sampler is a background child, so it is reaped here too: an early exit must not
# leave a loadavg loop running on a metered box after the rep it belonged to is gone.
cleanup() {
  touch "$RUNDIR/stop" 2>/dev/null || true; sleep 0.5
  kill "${WPID:-}" 2>/dev/null || true
  kill "${SAMPLER:-}" 2>/dev/null || true
}
trap cleanup EXIT

# Co-tenancy is RECORDED, never assumed away: other lanes share this box, and a rep taken
# beside a peer's gate is a rep whose validity has to be judged, not hidden.
#
# WHY A BEFORE/AFTER PAIR IS NOT ENOUGH. The gate semaphore
# (CQLITE_GATE_MAX_CONCURRENCY=1) serialises GATE against GATE; a perf run holds no slot,
# so a peer's gate can start, run and finish entirely INSIDE this rep's window while both
# endpoint samples look quiet. loadavg is also a decaying average, so its value at t=0
# describes the minute BEFORE the rep. Hence a sampler across the whole window, and a
# verdict taken from the MAXIMUM rather than from either endpoint.
{ echo "loadavg_before=$(cut -d' ' -f1-3 /proc/loadavg)"
  echo "nproc=$(nproc)"
  echo "peer_cargo_or_gate_procs=$(pgrep -c -f 'cargo|agent-gate' || true)"
} > "$OUT/cotenancy-before.txt"

taskset -c "$CPU" "$BINARY" \
  --corpus "$CORPUS" --rundir "$RUNDIR" --worker-id 0 \
  --prewarm-passes 1 --max-secs 900 --progress-ms 250 \
  > "$OUT/worker.stdout" 2> "$OUT/worker.stderr" &
WPID=$!

# Wait for the worker's own ready signal. Its absence is a FAILURE, never a short window:
# starting perf without it would put ingestion + the cold first pass inside the samples.
for _ in $(seq 1 1800); do [ -f "$RUNDIR/ready-0" ] && break; sleep 1
  kill -0 "$WPID" 2>/dev/null || { echo "record-scan.sh: worker died before ready" >&2; cat "$OUT/worker.stderr" >&2; exit 1; }
done
[ -f "$RUNDIR/ready-0" ] || { echo "record-scan.sh: worker never signalled ready" >&2; exit 1; }

touch "$RUNDIR/go"
sleep "$SETTLE"          # steady state, after the barrier release transient

# Affinity is READ BACK from the kernel rather than trusted to taskset's argument.
tr -d '\0' < "/proc/$WPID/status" | grep -E 'Cpus_allowed_list' > "$OUT/affinity-observed.txt" || true

# --- load sampler across the measured window ------------------------------------
: > "$OUT/load-samples.txt"
( while :; do
    echo "$(date -u +%H:%M:%S) $(cut -d' ' -f1 /proc/loadavg) $(pgrep -c -f 'cargo|agent-gate|maturin|rustc' || echo 0)"
    sleep "$LOAD_SAMPLE_SECS"
  done ) >> "$OUT/load-samples.txt" 2>/dev/null &
SAMPLER=$!
stop_sampler() { kill "$SAMPLER" 2>/dev/null || true; }

if [ "$MODE" = record ]; then
  perf record -e "$EVENT" -c "$PERIOD" -p "$WPID" -o "$OUT/perf.data" \
    -- sleep "$SECS" > "$OUT/perf-record.log" 2>&1 || true
else
  # -x, gives the machine-readable form whose 6th field is pct_running: the validity rule
  # is checked from THAT field, not from the absence of a warning in the human-readable form.
  # `perf stat -x,` writes its CSV to STDERR, not stdout: sending stdout to counters.csv
  # yields an EMPTY counters file and a log that happens to hold the data, which is how a
  # validity check ends up reading nothing and reporting nothing wrong. Capture stderr.
  perf stat -x, -e "$STAT_EVENTS" -p "$WPID" \
    -- sleep "$SECS" 2> "$OUT/counters.csv" > "$OUT/perf-stat.stdout" || true
fi

stop_sampler
{ echo "loadavg_after=$(cut -d' ' -f1-3 /proc/loadavg)"
  echo "peer_cargo_or_gate_procs=$(pgrep -c -f 'cargo|agent-gate' || true)"
} > "$OUT/cotenancy-after.txt"

# --- quiescence verdict: REFUSE loudly, never silently re-roll -------------------
# The verdict is written to a file in the rep directory whatever it says, so a REFUSED
# rep leaves a durable record that can be reported as a refusal. A rep quietly re-rolled
# until it looked clean is the worse outcome (#3299 AC5), so this script does not retry.
PEAK=$(awk '{ if ($2+0 > m) m = $2+0 } END { printf "%.2f", m }' "$OUT/load-samples.txt")
NSAMP=$(wc -l < "$OUT/load-samples.txt")
if [ "$NSAMP" -lt 2 ]; then
  printf 'verdict=REFUSED\nreason=quiescence-unmeasured\nsamples=%s\npeak_load=%s\nmax_load=%s\n' \
    "$NSAMP" "$PEAK" "$MAX_LOAD" > "$OUT/quiescence-verdict.txt"
elif [ "$(awk -v p="$PEAK" -v m="$MAX_LOAD" 'BEGIN{print (m+0==0) ? "off" : ((p+0>m+0)?"bad":"ok")}')" = bad ]; then
  printf 'verdict=REFUSED\nreason=box-not-quiet-across-rep\nsamples=%s\npeak_load=%s\nmax_load=%s\n' \
    "$NSAMP" "$PEAK" "$MAX_LOAD" > "$OUT/quiescence-verdict.txt"
elif [ "$(awk -v m="$MAX_LOAD" 'BEGIN{print (m+0==0)?"off":"on"}')" = off ]; then
  printf 'verdict=UNCHECKED\nreason=max-load-check-disabled\nsamples=%s\npeak_load=%s\n' \
    "$NSAMP" "$PEAK" > "$OUT/quiescence-verdict.txt"
else
  printf 'verdict=OK\nsamples=%s\npeak_load=%s\nmax_load=%s\n' \
    "$NSAMP" "$PEAK" "$MAX_LOAD" > "$OUT/quiescence-verdict.txt"
fi

touch "$RUNDIR/stop"
wait "$WPID" || true
cp "$RUNDIR/worker-0.summary.json" "$OUT/worker-summary.json" 2>/dev/null || true
trap - EXIT; rm -rf "$RUNDIR"
echo "record-scan.sh: rep written to $OUT"
