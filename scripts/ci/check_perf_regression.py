#!/usr/bin/env python3
"""Fail CI when a tracked Criterion bench regresses past the configured threshold.

Compares the Criterion *median* point estimate of each tracked benchmark between
two saved baselines (typically the PR's `pr` baseline and `main`'s `base`
baseline, both measured on the same CI runner — see
`.github/workflows/perf-regression.yml`). Exits non-zero if any tracked bench is
more than `threshold_pct` slower in `new` than in `base`.

Usage:
    check_perf_regression.py <criterion_dir> <new_baseline> <base_baseline> <perf_gate_json>

Example:
    check_perf_regression.py target/criterion pr base cqlite-core/benches/perf-gate.json

Benches missing from either baseline are reported as SKIP (e.g. a bench added in
the PR that main does not have yet) and never fail the gate.
"""

import json
import os
import sys


def _median_ns(criterion_dir, bench_id, baseline):
    """Return the median point estimate (ns) for a bench baseline, or None."""
    path = os.path.join(criterion_dir, bench_id, baseline, "estimates.json")
    if not os.path.isfile(path):
        return None
    with open(path) as fh:
        data = json.load(fh)
    return data["median"]["point_estimate"]


def main(argv):
    if len(argv) != 5:
        print(__doc__)
        return 2

    criterion_dir, new_baseline, base_baseline, cfg_path = argv[1:5]

    with open(cfg_path) as fh:
        cfg = json.load(fh)
    threshold_pct = float(cfg["threshold_pct"])
    threshold = threshold_pct / 100.0
    benches = cfg["benches"]

    print(
        f"Performance regression gate: '{new_baseline}' vs '{base_baseline}' "
        f"(fail if any bench > {threshold_pct:g}% slower)\n"
    )
    header = f"{'bench':<32} {'base (ns)':>16} {'new (ns)':>16} {'delta':>9}  status"
    print(header)
    print("-" * len(header))

    failures = []
    compared = 0
    for bench in benches:
        base = _median_ns(criterion_dir, bench, base_baseline)
        new = _median_ns(criterion_dir, bench, new_baseline)
        if base is None or new is None:
            missing = []
            if base is None:
                missing.append(f"{base_baseline}")
            if new is None:
                missing.append(f"{new_baseline}")
            print(
                f"{bench:<32} {'-':>16} {'-':>16} {'-':>9}  "
                f"SKIP (no data in: {', '.join(missing)})"
            )
            continue

        compared += 1
        ratio = (new / base) - 1.0
        if ratio > threshold:
            status = f"REGRESSION (> {threshold_pct:g}%)"
            failures.append((bench, ratio))
        else:
            status = "ok"
        print(f"{bench:<32} {base:>16.0f} {new:>16.0f} {ratio * 100:>+8.1f}%  {status}")

    print()
    if compared == 0:
        # No bench could be compared (e.g. baselines never produced) — surface
        # loudly rather than passing silently.
        print("❌ No tracked benches could be compared (no baseline data found).")
        return 1
    if failures:
        print(f"❌ {len(failures)} bench(es) regressed past {threshold_pct:g}%:")
        for bench, ratio in failures:
            print(f"   - {bench}: {ratio * 100:+.1f}%")
        print(
            "\nIf this slowdown is expected/intended, justify it in the PR. To change "
            "the threshold or tracked set, edit cqlite-core/benches/perf-gate.json."
        )
        return 1

    print(f"✅ All {compared} tracked bench(es) within {threshold_pct:g}% of main.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
