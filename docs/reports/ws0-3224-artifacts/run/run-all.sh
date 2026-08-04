#!/usr/bin/env bash
# #3224 — both endpoints, >=3 reps each, STRICTLY SEQUENTIALLY.
#
# Sequential is not a style choice: each capture needs exclusive use of the box.
# Two concurrent captures would contend for CPU and memory bandwidth and measure
# each other (#1930's one-worker-per-machine rule applied to measurement).
#
# STEP SIZING, from the calibration run committed in run/calibration.md:
#   S=1/N=2  full-scan latency ~35.9 s, 221,900 rows/s
#   The loadgen DRAINS in-flight requests, so duration_s exceeds the requested
#   step (120 s requested -> 144.2 s actual) and always contains WHOLE scans:
#   rows_total came back exactly 8 x 3,999,890. That is what makes both
#   denominator conventions well-defined here.
#
# reps=3 closes #3217's method gap 1 (its llc-* captures were reps=1, so its
# headline IPC figures carry no dispersion; a delta between two undispersed
# points cannot be defended).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../harness/ws0env.sh"

REPS="${REPS:-3}"
OUTROOT="${OUTROOT:-/data/ws0/results}"
STEP="${STEP:-120}"        # requested step seconds (actual is drain-extended)
WINDOW="${WINDOW:-60}"     # interior counter window
SETTLE="${SETTLE:-20}"     # skip this much of the step before counting
export SETTLE_SECS="$SETTLE"
export WARM_SECS="${WARM_SECS:-45}"

mkdir -p "$OUTROOT"
FAILED=0
for rep in $(seq 1 "$REPS"); do
  for spec in "llc-s1-N2 1 2" "llc-s6-N16 6 16"; do
    set -- $spec
    label="$1"; S="$2"; N="$3"
    out="$OUTROOT/$label/rep$rep"
    # Resume-safe: a rep may be SKIPPED only if run/rep-complete.py certifies it —
    # occupancy arms ok, EVERY recorded return code zero, EVERY counter file named
    # in its own meta.json present and carrying supported non-multiplexed rows,
    # warmth verified, client-saturation gate passed.
    #
    # This predicate used to be three inlined conditions that read neither the
    # return codes nor the counter files (roborev finding #5, PR #3286), so a rep
    # whose load generator had failed, or whose perf-uncore.csv was absent, was
    # reported "already complete and valid" and skipped — and a skipped rep is
    # never revisited, so the failure became permanent. The diagnosis is printed
    # rather than swallowed: `2>/dev/null` on the old form also hid why a rep was
    # being redone.
    if python3 "$HERE/rep-complete.py" "$out"; then
      echo "[run-all] SKIP $label rep$rep (certified complete and valid)"; continue
    fi
    echo "=============================================================="
    echo "[run-all] $(date -u +%H:%M:%S) $label rep$rep  S=$S N=$N step=${STEP}s window=${WINDOW}s"
    echo "=============================================================="
    bash "$HERE/capture-endpoint.sh" "$label" "$S" "$N" "$STEP" "$WINDOW" "$rep" "$out"
    rc=$?                       # captured IMMEDIATELY, before any substitution
    echo "[run-all] $label rep$rep rc=$rc"
    [ "$rc" -eq 0 ] || FAILED=$((FAILED+1))
  done
done
echo "[run-all] DONE failed_captures=$FAILED"
exit "$FAILED"
