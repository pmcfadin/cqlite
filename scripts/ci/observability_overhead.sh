#!/usr/bin/env bash
# Observability zero-overhead-when-disabled gate (epic #1031, issue #1043).
#
# Runs the SAME `observability_overhead` Criterion bench under two builds on the
# SAME machine and fails if the export-disabled `observability` build is more than
# OVERHEAD_THRESHOLD_PCT (default 2%) slower (median) than the default build:
#
#   1) DEFAULT build              — `observability` OFF; helpers are no-ops.
#   2) --features observability   — OTel linked, but EXPORT DISABLED (init never
#      called → no global provider/exporter). The helper bodies run but do no work.
#
# Because one `cargo bench` process compiles exactly one feature set, the two
# arms are two separate invocations; comparing them on the same runner makes the
# delta immune to cross-machine variance (same approach as the perf-regression
# gate). The bench source is identical in both builds — only the feature set
# differs — so any delta is pure instrumentation overhead.
#
# Variance reduction (issue #4456). Two separate processes on a SHARED CI runner
# each absorb whatever scheduler/cache/co-tenant contention happens during their
# own measurement window, so a single noisy "disabled" window could land above a
# quiet "off" window even when the instrumentation is genuinely near-free — a
# physically-incoherent inversion (disabled < off, yet the gate fails) that
# false-failed unrelated PRs. To kill that single-shot noise WITHOUT weakening
# real regression detection, each arm is measured OVERHEAD_BENCH_REPEATS times,
# the two arms are interleaved per rep, and the compare takes the best-of-N
# (minimum median) per arm. The statistical justification is in the Python
# compare below (see best_median_ns).
#
# Usage (from repo root):
#   scripts/ci/observability_overhead.sh
#
# Env:
#   OVERHEAD_THRESHOLD_PCT  max tolerated median overhead, percent (default 2.0)
#   OVERHEAD_BENCH_REPEATS  repeated measurements per arm; the compare takes the
#                           best-of-N minimum median per arm (default 3)
#   BENCH_ARGS              extra Criterion args (default keeps CI runs short)
#   CQLITE_DATASETS_ROOT    required for the read_scan arm (fixtures)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

THRESHOLD_PCT="${OVERHEAD_THRESHOLD_PCT:-2.0}"
# Best-of-N variance reduction (issue #4456): measure each arm this many times
# and let the compare take the minimum median per arm. 3 is enough to collapse a
# single contended window toward each arm's floor while keeping CI short.
REPEATS="${OVERHEAD_BENCH_REPEATS:-3}"
# Small but stable Criterion config for CI. Override via BENCH_ARGS locally.
# warm-up/measurement are long enough for the slow-warming write_merge arm to
# complete its full sample count (issue #4456).
BENCH_ARGS="${BENCH_ARGS:---sample-size 30 --warm-up-time 3 --measurement-time 10}"

# Read arm needs cli-helpers; write arm needs write-support (default-on). Both
# builds below carry cli-helpers so the read_scan arm is present in each.
DEFAULT_FEATURES="cli-helpers,write-support"
OBS_FEATURES="cli-helpers,write-support,observability"

CRIT_DIR="$ROOT/target/criterion"
BASE_OFF="obs_default_off"
BASE_DISABLED="obs_export_disabled"

run_arm() {
  # $1 = feature set, $2 = baseline prefix, $3 = rep index, $4 = human label.
  # Each rep is saved under a distinct baseline "<prefix>_r<rep>" so the compare
  # can take the best-of-N minimum median across reps.
  local features="$1" prefix="$2" rep="$3" label="$4"
  echo "== $label (rep ${rep}/${REPEATS}), baseline '${prefix}_r${rep}' =="
  cargo bench -p cqlite-core --features "$features" \
    --bench observability_overhead -- --save-baseline "${prefix}_r${rep}" $BENCH_ARGS
}

# Interleave the two arms per rep so a mid-run load change on the shared runner
# perturbs BOTH arms in the same window instead of biasing one. After the first
# rep both feature sets are already compiled, so later reps only re-measure.
for rep in $(seq 1 "$REPEATS"); do
  run_arm "$DEFAULT_FEATURES" "$BASE_OFF" "$rep" \
    "Arm 1/2: DEFAULT build (observability OFF)"
  echo
  run_arm "$OBS_FEATURES" "$BASE_DISABLED" "$rep" \
    "Arm 2/2: --features observability (EXPORT DISABLED)"
  echo
done

echo "== Comparing best-of-${REPEATS} medians (threshold ${THRESHOLD_PCT}%) =="
python3 - "$CRIT_DIR" "$BASE_OFF" "$BASE_DISABLED" "$THRESHOLD_PCT" "$REPEATS" <<'PY'
import json, os, sys

crit_dir, base_off, base_disabled, threshold, repeats = sys.argv[1:6]
threshold = float(threshold)
repeats = int(repeats)

# Bench IDs produced by benches/observability_overhead.rs. write_merge needs
# write-support and read_scan needs cli-helpers — BOTH are enabled in BOTH arms
# above, so BOTH baselines MUST exist. A missing baseline means the bench was
# renamed, gated out, or its ID drifted from this list, which would silently let
# the overhead gate pass without measuring anything — so we FAIL (not skip) on a
# missing expected baseline.
BENCH_IDS = ["observability_overhead/read_scan", "observability_overhead/write_merge"]

# Issue #1703: subscriber-ON variants. These run the SAME workload with a real
# fmt `tracing` subscriber installed at INFO — the CLI's default posture, which
# the subscriber-less arms above never measured. Recorded ADVISORY-FIRST below
# (printed + warned, never failing) so the previously-unmeasured default posture
# becomes visible; a later change may promote it to a failing threshold.
SUBSCRIBER_ON_IDS = {
    "observability_overhead/read_scan": "observability_overhead/read_scan_subscriber_on",
    "observability_overhead/write_merge": "observability_overhead/write_merge_subscriber_on",
}

def best_median_ns(bench_id, baseline_prefix):
    """Best-of-N: the MINIMUM criterion median across the `repeats` repeated
    measurements saved under "<baseline_prefix>_r1".."<prefix>_rN".

    NOISE MODEL (issue #4456 — why "disabled" can measure slower than "off").
    The two arms are two separate processes measured in two separate windows on a
    SHARED CI runner. A benchmark timing is a hard true-cost FLOOR plus strictly
    non-negative runner noise — scheduler preemption, co-tenant cache/memory-bus
    contention, CPU frequency scaling, page faults. Noise only ever ADDS time; it
    cannot make a run faster than its true cost. So when a single noisy "disabled"
    window happens to absorb more contention than a quiet "off" window, the raw
    medians invert (disabled > off) and the 2% gate trips even though the
    instrumentation is genuinely near-free. That is the physically-incoherent
    one-shot inversion that false-failed unrelated PRs.

    WHY MIN-OF-N IS SOUND. Because the noise is one-sided (floor + nonneg), the
    smallest median across N repeats is the least-contaminated estimate of the
    arm's true cost — the maximum-likelihood floor. This is the standard reason
    perf tools report the minimum. Interleaving the arms (see the bash loop) means
    both arms sample the same run of good/bad windows, so their minima are reached
    under comparable conditions.

    WHY IT DOES NOT HIDE A REAL REGRESSION. Genuine instrumentation cost shifts
    BOTH floors by the same amount, so min(disabled)/min(off) still reflects the
    true overhead. Best-of-N removes one-sided variance, not signal — the gate
    stays exactly as sensitive to a real slowdown while ignoring runner noise.
    """
    vals = []
    for rep in range(1, repeats + 1):
        path = os.path.join(
            crit_dir, bench_id, f"{baseline_prefix}_r{rep}", "estimates.json"
        )
        if os.path.isfile(path):
            with open(path) as fh:
                vals.append(json.load(fh)["median"]["point_estimate"])
    return min(vals) if vals else None

fail = 0
print(f"{'bench':40} {'off (ns)':>14} {'disabled (ns)':>14} {'overhead %':>12}  result")
print("-" * 90)
for bid in BENCH_IDS:
    off = best_median_ns(bid, base_off)
    dis = best_median_ns(bid, base_disabled)
    if off is None or dis is None:
        missing = []
        if off is None:
            missing.append(base_off)
        if dis is None:
            missing.append(base_disabled)
        fail = 1
        print(
            f"{bid:40} {'-':>14} {'-':>14} {'-':>12}  "
            f"FAIL (expected baseline missing: {', '.join(missing)})"
        )
        continue
    overhead_pct = (dis - off) / off * 100.0
    ok = overhead_pct <= threshold
    status = "OK" if ok else "FAIL"
    if not ok:
        fail = 1
    print(f"{bid:40} {off:14.1f} {dis:14.1f} {overhead_pct:12.2f}  {status}")

print("-" * 90)
if fail:
    print(
        f"FAIL: export-disabled observability overhead exceeded {threshold}% on at least "
        "one bench, or an expected bench baseline was missing (renamed/gated-out bench?)."
    )
else:
    print(f"OK: all benches within the {threshold}% overhead threshold.")

# ---------------------------------------------------------------------------
# Issue #1703: subscriber-ON advisory. NEVER touches `fail` / the exit code.
# Measured on the DEFAULT build (observability OFF) — the CLI's default posture.
# ---------------------------------------------------------------------------
print()
print("== Subscriber-on posture (issue #1703, ADVISORY — does not fail the gate) ==")
print(f"{'bench':44} {'no-sub (ns)':>14} {'sub-on (ns)':>14} {'sub cost %':>12}  note")
print("-" * 92)
for base_bid, sub_bid in SUBSCRIBER_ON_IDS.items():
    no_sub = best_median_ns(base_bid, base_off)
    sub_on = best_median_ns(sub_bid, base_off)
    if no_sub is None or sub_on is None:
        print(
            f"{sub_bid:44} {'-':>14} {'-':>14} {'-':>12}  "
            "WARN: subscriber-on baseline absent (advisory only)"
        )
        continue
    sub_cost_pct = (sub_on - no_sub) / no_sub * 100.0
    note = "advisory"
    if sub_cost_pct > threshold:
        note = f"WARN: subscriber-on posture > {threshold}% (advisory, not failing)"
    print(f"{sub_bid:44} {no_sub:14.1f} {sub_on:14.1f} {sub_cost_pct:12.2f}  {note}")
print("-" * 92)
print(
    "NOTE: the subscriber-on numbers are advisory-first (issue #1703). They record "
    "the CLI's real default posture; a later change may promote them to a failing gate."
)

sys.exit(fail)
PY
