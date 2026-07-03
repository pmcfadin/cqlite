#!/usr/bin/env python3
"""Fail CI when a tracked Criterion bench regresses past the configured threshold.

Compares the Criterion *median* point estimate of each tracked benchmark between
two saved baselines (typically the PR's `pr` baseline and `main`'s `base`
baseline, both measured on the same CI runner — see
`.github/workflows/perf-regression.yml`). Exits non-zero if any STRICT bench is
more than its configured threshold slower in `new` than in `base`.

ADVISORY benches (listed in `advisory_benches` in perf-gate.json) are always
reported with their measured delta, but NEVER cause a non-zero exit, regardless
of how large the swing. This is appropriate for I/O-dominated benchmarks such as
`write/ingest_wal_on`, whose wall-clock time is dominated by fsync latency on
shared CI runners and varies well beyond any useful threshold (Issue #572).

Policy model in perf-gate.json:
  - `benches`: list of objects with `id` and per-bench `threshold_pct`
  - `advisory_benches`: list of bench IDs that are reported but never fail
  - `default_threshold_pct`: fallback if a bench entry lacks `threshold_pct`

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
    """Return the median point estimate (ns) for a bench baseline, or None.

    Criterion stores estimates in:
      <criterion_dir>/<bench_id>/<baseline>/estimates.json
    with shape {"median": {"point_estimate": <float_ns>}, ...}.
    """
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

    default_threshold_pct = float(cfg.get("default_threshold_pct", cfg.get("threshold_pct", 10)))
    advisory_set = set(cfg.get("advisory_benches", []))

    # Support both legacy (list of strings) and new (list of objects) bench format.
    raw_benches = cfg["benches"]
    bench_configs = []
    for entry in raw_benches:
        if isinstance(entry, str):
            bench_configs.append({"id": entry, "threshold_pct": default_threshold_pct})
        else:
            bench_configs.append({
                "id": entry["id"],
                "threshold_pct": float(entry.get("threshold_pct", default_threshold_pct)),
            })

    print(
        f"Performance regression gate: '{new_baseline}' vs '{base_baseline}'\n"
        f"  STRICT benches fail CI on regression; ADVISORY benches are reported only.\n"
    )
    col_w = 36
    header = f"{'bench':<{col_w}} {'base (ns)':>16} {'new (ns)':>16} {'delta':>9}  {'threshold':>10}  status"
    print(header)
    print("-" * len(header))

    failures = []
    advisory_regressions = []
    compared = 0

    for bc in bench_configs:
        bench_id = bc["id"]
        threshold_pct = bc["threshold_pct"]
        threshold = threshold_pct / 100.0
        is_advisory = bench_id in advisory_set

        base = _median_ns(criterion_dir, bench_id, base_baseline)
        new = _median_ns(criterion_dir, bench_id, new_baseline)

        if base is None or new is None:
            missing = []
            if base is None:
                missing.append(base_baseline)
            if new is None:
                missing.append(new_baseline)
            advisory_tag = " [advisory]" if is_advisory else ""
            print(
                f"{bench_id:<{col_w}} {'-':>16} {'-':>16} {'-':>9}  {threshold_pct:>9g}%  "
                f"SKIP (no data in: {', '.join(missing)}){advisory_tag}"
            )
            continue

        compared += 1
        ratio = (new / base) - 1.0

        if is_advisory:
            if ratio > threshold:
                status = f"ADVISORY REGRESSION (> {threshold_pct:g}%) — reported only, not failing CI"
                advisory_regressions.append((bench_id, ratio))
            else:
                status = "ok [advisory]"
        else:
            if ratio > threshold:
                status = f"REGRESSION (> {threshold_pct:g}%)"
                failures.append((bench_id, ratio))
            else:
                status = "ok"

        print(
            f"{bench_id:<{col_w}} {base:>16.0f} {new:>16.0f} {ratio * 100:>+8.1f}%  "
            f"{threshold_pct:>9g}%  {status}"
        )

    print()

    # -----------------------------------------------------------------------
    # Concurrency scaling floors (Issue #1564)
    #
    # A machine-independent intra-run invariant, evaluated on the `new` (PR)
    # baseline alone (both medians come from the same run on the same runner, so
    # the machine's absolute speed cancels):
    #
    #     scaling = degree_ratio * median(baseline_id) / median(id)
    #
    # Healthy parallel scans measure ≈degree_ratio; a re-serialized read path
    # (e.g. a reintroduced shared Mutex) collapses median(id)→≈degree_ratio*
    # median(baseline_id), driving scaling→≈1.0. A `scaling` below `min_scaling`
    # fails the gate. Legacy configs without `scaling_floors` are unaffected.
    #
    # Missing data for a configured floor is a FAILURE, not a SKIP (issue #1564
    # roborev): the whole point of a scaling gate is to catch regressions, so a
    # typo'd id, an omitted `--bench`, or a bench that produced no data must NOT
    # silently disable the gate. Unlike the pr-vs-main median benches (which
    # legitimately SKIP a bench absent from `main`), a scaling floor is intra-run,
    # so its data is always present on any run that benches the target. An entry
    # may opt into skip-on-absent with `"optional": true` (for a genuinely
    # optional fixture, mirroring the read/get_partition_bti convention).
    # -----------------------------------------------------------------------
    scaling_floors = cfg.get("scaling_floors", [])
    scaling_evaluated = 0
    # Reason text for each scaling-floor failure, keyed by bench id (the failures
    # list carries only (id, None) for scaling entries).
    scaling_fail_reason = {}
    if scaling_floors:
        print(
            f"Concurrency scaling floors (evaluated on '{new_baseline}' baseline):\n"
            f"  scaling = degree_ratio * median(baseline) / median(scaled); "
            f"fails below floor.\n"
        )
        sh = (
            f"{'scaling entry':<{col_w}} {'baseline (ns)':>16} {'scaled (ns)':>16} "
            f"{'scaling':>9}  {'floor':>7}  status"
        )
        print(sh)
        print("-" * len(sh))

    for entry in scaling_floors:
        bench_id = entry.get("id", "")
        baseline_id = entry.get("baseline_id", "")
        m_base = _median_ns(criterion_dir, baseline_id, new_baseline)
        m_scaled = _median_ns(criterion_dir, bench_id, new_baseline)
        min_scaling = float(entry.get("min_scaling", 0.0))

        if m_base is None or m_scaled is None:
            missing = []
            if m_base is None:
                missing.append(baseline_id)
            if m_scaled is None:
                missing.append(bench_id)
            missing_txt = ", ".join(missing)
            if entry.get("optional", False):
                # Genuinely optional fixture — skip without failing.
                print(
                    f"{bench_id:<{col_w}} {'-':>16} {'-':>16} {'-':>9}  "
                    f"{min_scaling:>7g}  SKIP (optional, no data in '{new_baseline}': {missing_txt})"
                )
            else:
                # Required floor with no data — fail loudly rather than silently
                # disabling the gate.
                print(
                    f"{bench_id:<{col_w}} {'-':>16} {'-':>16} {'-':>9}  "
                    f"{min_scaling:>7g}  MISSING DATA (required; '{new_baseline}': {missing_txt})"
                )
                failures.append((bench_id, None))
                scaling_fail_reason[bench_id] = (
                    f"required scaling data missing in '{new_baseline}': {missing_txt}"
                )
            continue

        scaling_evaluated += 1
        scaling = float(entry.get("degree_ratio", 1)) * m_base / m_scaled
        if scaling < min_scaling:
            status = f"REGRESSION (scaling < {min_scaling:g})"
            failures.append((bench_id, None))
            scaling_fail_reason[bench_id] = f"below scaling floor: {min_scaling:g}"
        else:
            status = "ok"
        print(
            f"{bench_id:<{col_w}} {m_base:>16.0f} {m_scaled:>16.0f} "
            f"{scaling:>9.2f}  {min_scaling:>7g}  {status}"
        )

    if scaling_floors:
        print()

    if compared == 0 and scaling_evaluated == 0:
        # Nothing could be compared or evaluated (e.g. baselines never produced)
        # — surface loudly rather than passing silently.
        print("❌ No tracked benches could be compared (no baseline data found).")
        return 1

    if advisory_regressions:
        print(f"⚠️  {len(advisory_regressions)} advisory bench(es) show elevated delta (not failing CI):")
        for bench, ratio in advisory_regressions:
            print(f"   - {bench}: {ratio * 100:+.1f}%  (advisory — I/O noise, not a code regression)")
        print()

    if failures:
        print(f"❌ {len(failures)} bench(es) regressed past their threshold:")
        for bench, ratio in failures:
            if ratio is None:
                # Scaling-floor failure (ratio is not a pr-vs-main delta): either
                # below the floor or required data missing.
                reason = scaling_fail_reason.get(bench, "scaling floor")
                print(f"   - {bench}: {reason}")
            else:
                bc_entry = next((b for b in bench_configs if b["id"] == bench), {})
                tpct = bc_entry.get("threshold_pct", default_threshold_pct)
                print(f"   - {bench}: {ratio * 100:+.1f}%  (threshold: {tpct:g}%)")
        print(
            "\nIf this slowdown is expected/intended, justify it in the PR. To change "
            "the threshold or tracked set, edit cqlite-core/benches/perf-gate.json."
        )
        return 1

    strict_count = compared - len([bc for bc in bench_configs if bc["id"] in advisory_set and
                                    _median_ns(criterion_dir, bc["id"], base_baseline) is not None and
                                    _median_ns(criterion_dir, bc["id"], new_baseline) is not None])
    print(f"✅ All strict bench(es) within threshold of main.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
