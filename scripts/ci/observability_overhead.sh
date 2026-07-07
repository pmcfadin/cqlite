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
# Usage (from repo root):
#   scripts/ci/observability_overhead.sh
#
# Env:
#   OVERHEAD_THRESHOLD_PCT  max tolerated median overhead, percent (default 2.0)
#   BENCH_ARGS              extra Criterion args (default keeps CI runs short)
#   CQLITE_DATASETS_ROOT    required for the read_scan arm (fixtures)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

THRESHOLD_PCT="${OVERHEAD_THRESHOLD_PCT:-2.0}"
# Small but stable Criterion config for CI. Override via BENCH_ARGS locally.
BENCH_ARGS="${BENCH_ARGS:---sample-size 30 --warm-up-time 1 --measurement-time 5}"

# Read arm needs cli-helpers; write arm needs write-support (default-on). Both
# builds below carry cli-helpers so the read_scan arm is present in each.
DEFAULT_FEATURES="cli-helpers,write-support"
OBS_FEATURES="cli-helpers,write-support,observability"

CRIT_DIR="$ROOT/target/criterion"
BASE_OFF="obs_default_off"
BASE_DISABLED="obs_export_disabled"

echo "== Arm 1/2: DEFAULT build (observability OFF), baseline '$BASE_OFF' =="
cargo bench -p cqlite-core --features "$DEFAULT_FEATURES" \
  --bench observability_overhead -- --save-baseline "$BASE_OFF" $BENCH_ARGS

echo
echo "== Arm 2/2: --features observability (EXPORT DISABLED), baseline '$BASE_DISABLED' =="
cargo bench -p cqlite-core --features "$OBS_FEATURES" \
  --bench observability_overhead -- --save-baseline "$BASE_DISABLED" $BENCH_ARGS

echo
echo "== Comparing medians (threshold ${THRESHOLD_PCT}%) =="
python3 - "$CRIT_DIR" "$BASE_OFF" "$BASE_DISABLED" "$THRESHOLD_PCT" <<'PY'
import json, os, sys

crit_dir, base_off, base_disabled, threshold = sys.argv[1:5]
threshold = float(threshold)

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

def median_ns(bench_id, baseline):
    path = os.path.join(crit_dir, bench_id, baseline, "estimates.json")
    if not os.path.isfile(path):
        return None
    with open(path) as fh:
        return json.load(fh)["median"]["point_estimate"]

fail = 0
print(f"{'bench':40} {'off (ns)':>14} {'disabled (ns)':>14} {'overhead %':>12}  result")
print("-" * 90)
for bid in BENCH_IDS:
    off = median_ns(bid, base_off)
    dis = median_ns(bid, base_disabled)
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
    no_sub = median_ns(base_bid, base_off)
    sub_on = median_ns(sub_bid, base_off)
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
