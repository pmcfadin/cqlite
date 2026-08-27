#!/usr/bin/env bash
# #3299 frequency calibration — measure TRUE core frequency f(S), S=1..6.
#
# Purpose: split the S=1->S=6 marginal-efficiency loss into the part the package
# CLOCK accounts for and an unattributed residual. Without it the whole discount
# reads as contention, which would overstate what a footprint lever (#3288) can
# recover — and #3288's ceiling is what this issue exists to calibrate.
#
# INSTRUMENT: msr/aperf + msr/mperf (frequency = TSC_freq x aperf/mperf, the
# canonical method). Both are present in sysfs on this box. `ref-cycles` is
# captured alongside as CORROBORATION only.
#
# `cycles / task-clock` is NOT used and must not be: under CPU-wide `-C` counting
# task-clock accrues elapsed x nCPUs INCLUDING IDLE CPUs while cycles accrue only
# where a core is unhalted, so that quotient is occupancy x frequency. The grid's
# own data shows how badly: at N=1 it reads 3.268 / 2.486 / 1.673 / 1.271 for
# S=1..4, and "1.27 GHz" at S=4/N=1 is one busy core diluted across eight pinned
# logical CPUs, not a downclock.
#
# SYSFS PRESENCE IS NOT COUNTING — the Step 1 census exists because counters on
# this host program cleanly at 100.00% and return a hard zero. Hence step 0.
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
CONTAIN="$REPO/test-data/scripts/perf-run-contained.sh"
REP="$HERE/../harness/rep.py"
GUARDS="$HERE/../harness/guards.py"
WORKER="${WS0_3299_TARGET_DIR:-/data/ws0-3299/worker-target}/release/ws0-3299-scan-worker"
CORPUS=/data/ws0-3096
RESULTS="${1:-/data/ws0-3299/freq-$(date +%H%M%S)}"
DURATION=20            # a frequency, not a throughput: 20 s is ample
REPS=3
EVENTS="msr/aperf/,msr/mperf/,cycles,ref-cycles,task-clock"

mkdir -p "$RESULTS"; RESULTS="$(cd "$RESULTS" && pwd)"
echo "[freq] results -> $RESULTS"

mapfile -t GROUPS < <(for c in /sys/devices/system/cpu/cpu[0-9]*; do
  cat "$c/topology/thread_siblings_list"; done | sort -u -t, -k1,1n)

# --- step 0: POSITIVE CONTROL --------------------------------------------------
# One compute-bound thread on ONE core with the rest of the box idle: the package
# must sit near max turbo, so a plausible answer (~3.0-3.9 GHz on a Xeon Platinum
# 8488C) is the prediction. A degenerate answer means the instrument is
# UNAVAILABLE, and the decomposition is DROPPED rather than approximated.
CH=/data/ws0-3299/census/cache-hostile
[[ -x "$CH" ]] || cc -O2 -std=c99 -pthread -o "$CH" "$REPO/docs/reports/ws0-3224-artifacts/cache-hostile.c"
perf stat -x, -o "$RESULTS/positive-control.csv" -C "${GROUPS[0]}" -e "$EVENTS" -- \
  taskset -c "${GROUPS[0]}" "$CH" chase --buffer-mib 64 --working-kib 64 \
    --accesses 3000000000 --delay-ms 0 --arm freq-control > "$RESULTS/positive-control.log" 2>&1 || true
python3 - "$RESULTS/positive-control.csv" <<'PY'
import sys
vals = {}
for line in open(sys.argv[1]):
    f = line.split(",")
    if len(f) > 4 and not line.startswith("#"):
        vals[f[2].strip()] = (f[0].strip(), f[4].strip())
print("POSITIVE CONTROL:", {k: v for k, v in vals.items()})
a, m = vals.get("msr/aperf/", ("x", "0"))[0], vals.get("msr/mperf/", ("x", "0"))[0]
if not (a.isdigit() and m.isdigit()) or int(m) == 0 or int(a) == 0:
    sys.exit("FATAL: aperf/mperf did not COUNT on a single-core busy loop. Treat as an "
             "UNAVAILABLE INSTRUMENT exactly like the LLC counters: report it as such and "
             "DROP the turbo decomposition. Do NOT substitute cycles/task-clock.")
print(f"aperf/mperf ratio = {int(a)/int(m):.4f}  (x TSC base => GHz; ~1.0 means base clock)")
PY

# --- steps 1..6: f(S) under FULL occupancy of the pinned set -------------------
# N = 2S so the set is genuinely busy: a diluted set measures nothing useful,
# which is precisely the retracted quotient's failure mode.
: > "$RESULTS/manifest.jsonl"
for S in 1 2 3 4 5 6; do
  CPUS="$(printf '%s\n' "${GROUPS[@]}" | head -n "$S" | paste -sd,)"
  N=$(( S * 2 ))
  WORKER_CPUS="$(python3 -c 'import json,sys; c=[int(x) for x in sys.argv[1].split(",")]; print(json.dumps([c]*int(sys.argv[2])))' "$CPUS" "$N")"
  for (( rep=1; rep<=REPS; rep++ )); do
    RD="$RESULTS/s${S}-round${rep}"
    "$CONTAIN" --mem 24G --swap 0 -- \
      python3 "$REP" --s "$S" --n "$N" --rep "$rep" --round "$rep" --rundir "$RD" \
        --worker-bin "$WORKER" --corpus "$CORPUS" --worker-cpus "$WORKER_CPUS" \
        --perf-cpus "$CPUS" --events "$EVENTS" --duration-s "$DURATION" \
        --progress-ms 25 --prewarm-passes 1 >> "$RESULTS/driver.log" 2>&1
    python3 "$GUARDS" perf-csv --csv "$RD/perf.csv" --events "$EVENTS"
    echo "{\"s\":$S,\"n\":$N,\"rep\":$rep,\"rundir\":\"$RD\"}" >> "$RESULTS/manifest.jsonl"
    echo "[freq] S=$S rep=$rep OK"
  done
done
python3 "$HERE/derive-freq.py" --results "$RESULTS" | tee "$RESULTS/frequency-table.md"
