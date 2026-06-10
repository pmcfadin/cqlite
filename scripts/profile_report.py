#!/usr/bin/env python3
"""Aggregate profiling output into a ranked bottleneck report (docs/profiling.md).

Collects everything the profiling tools left behind and turns it into the
artifacts that drive the recursive-improvement loop:

  inputs (all optional — the report uses whatever exists):
    target/criterion/<group>/<bench>/{new,base}/estimates.json   criterion timings
    target/criterion/<group>/<bench>/new/benchmark.json          throughput config
    target/criterion/<group>/<bench>/profile/flamegraph.svg      pprof CPU profiles
    target/profiling/heap-summary.json                           dhat heap summary

  outputs:
    target/profiling/report.json     machine-readable, for tooling/agents
    target/profiling/report.md       human-readable ranked bottleneck table
    target/profiling/history.jsonl   append-only ledger (one line per run, with
                                     git revision) so improvement across
                                     iterations is auditable

Usage:
    scripts/profile_report.py [--criterion-dir target/criterion]
                              [--out-dir target/profiling]
"""

import argparse
import datetime
import json
import os
import subprocess
import sys

BUDGET_NOTE = "<128 MiB peak heap (CLAUDE.md memory target)"


def _load_json(path):
    if not os.path.isfile(path):
        return None
    with open(path) as fh:
        return json.load(fh)


def _git_rev():
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        )
        return out.stdout.strip()
    except Exception:
        return "unknown"


def _throughput_elements(benchmark_json):
    """Criterion stores the group throughput in benchmark.json, e.g.
    {"throughput": {"Elements": 999}} or {"Bytes": N} or null."""
    if not benchmark_json:
        return None, None
    tp = benchmark_json.get("throughput")
    if not isinstance(tp, dict):
        return None, None
    if "Elements" in tp:
        return "element", tp["Elements"]
    if "Bytes" in tp:
        return "byte", tp["Bytes"]
    return None, None


def collect_benches(criterion_dir):
    """Walk target/criterion for dirs holding new/estimates.json.

    The bench id is the directory path relative to criterion_dir
    (e.g. 'read/full_scan'). Criterion's aggregate 'report' dirs are skipped.
    """
    benches = []
    for dirpath, dirnames, _ in os.walk(criterion_dir):
        dirnames[:] = [d for d in dirnames if d != "report"]
        new_est = _load_json(os.path.join(dirpath, "new", "estimates.json"))
        if new_est is None:
            continue
        bench_id = os.path.relpath(dirpath, criterion_dir)
        base_est = _load_json(os.path.join(dirpath, "base", "estimates.json"))
        bench_json = _load_json(os.path.join(dirpath, "new", "benchmark.json"))
        tp_unit, tp_count = _throughput_elements(bench_json)

        median_ns = new_est["median"]["point_estimate"]
        entry = {
            "id": bench_id.replace(os.sep, "/"),
            "median_ns": median_ns,
            "mean_ns": new_est["mean"]["point_estimate"],
            "std_dev_ns": new_est["std_dev"]["point_estimate"],
        }
        if tp_count:
            entry["throughput_unit"] = tp_unit
            entry["throughput_count"] = tp_count
            entry[f"ns_per_{tp_unit}"] = median_ns / tp_count
        if base_est is not None:
            base_median = base_est["median"]["point_estimate"]
            entry["base_median_ns"] = base_median
            entry["delta_pct"] = (median_ns / base_median - 1.0) * 100.0
        flame = os.path.join(dirpath, "profile", "flamegraph.svg")
        if os.path.isfile(flame):
            entry["flamegraph"] = flame
        benches.append(entry)
    # Regressions first (largest positive delta), then by absolute cost.
    benches.sort(
        key=lambda b: (-(b.get("delta_pct") or float("-inf")), -b["median_ns"])
    )
    return benches


def fmt_ns(ns):
    if ns >= 1e9:
        return f"{ns / 1e9:.2f} s"
    if ns >= 1e6:
        return f"{ns / 1e6:.2f} ms"
    if ns >= 1e3:
        return f"{ns / 1e3:.2f} µs"
    return f"{ns:.0f} ns"


def render_markdown(report):
    lines = [
        "# CQLite profiling report",
        "",
        f"- generated: {report['generated_at']}",
        f"- git revision: `{report['git_rev']}`",
        "",
        "## Benchmarks (regressions first, then by absolute cost)",
        "",
        "| bench | median | vs base | per-unit | flamegraph |",
        "|-------|--------|---------|----------|------------|",
    ]
    for b in report["benches"]:
        delta = (
            f"{b['delta_pct']:+.1f}%" if "delta_pct" in b else "no base"
        )
        unit = b.get("throughput_unit")
        per_unit = f"{fmt_ns(b['ns_per_' + unit])}/{unit}" if unit else "—"
        flame = f"`{b['flamegraph']}`" if "flamegraph" in b else "—"
        lines.append(
            f"| {b['id']} | {fmt_ns(b['median_ns'])} | {delta} | {per_unit} | {flame} |"
        )

    heap = report.get("heap")
    lines += ["", "## Heap (dhat)", ""]
    if heap:
        verdict = "PASS" if heap.get("within_budget") else "**FAIL**"
        lines += [
            f"- peak heap: {heap['peak_bytes'] / 2**20:.1f} MiB — {verdict} ({BUDGET_NOTE})",
            f"- total allocations: {heap['total_allocations']:,} "
            f"({heap['total_bytes_allocated'] / 2**20:.1f} MiB total churn)",
            "- full profile: `target/profiling/dhat-heap.json` "
            "(open in <https://nnethercote.github.io/dh_view/dh_view.html>)",
        ]
    else:
        lines.append("- no heap data — run `./scripts/profile.sh heap` first")

    lines += [
        "",
        "## Next iteration",
        "",
        "1. Open the flamegraph of the worst bench above; the widest frames are the bottleneck.",
        "2. Make one targeted change; avoid speculative micro-optimizations.",
        "3. Re-measure: `./scripts/profile.sh bench && ./scripts/profile.sh compare`.",
        "4. Regenerate this report: `./scripts/profile.sh report` "
        "(history accumulates in `target/profiling/history.jsonl`).",
        "5. When the win is confirmed, re-save the baseline: `./scripts/profile.sh baseline`.",
        "",
    ]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--criterion-dir", default="target/criterion")
    ap.add_argument("--out-dir", default="target/profiling")
    args = ap.parse_args()

    benches = collect_benches(args.criterion_dir)
    if not benches:
        print(
            f"no criterion data under {args.criterion_dir} — "
            "run ./scripts/profile.sh bench first",
            file=sys.stderr,
        )
        return 1

    heap = _load_json(os.path.join(args.out_dir, "heap-summary.json"))

    report = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(
            timespec="seconds"
        ),
        "git_rev": _git_rev(),
        "benches": benches,
        "heap": heap,
    }

    os.makedirs(args.out_dir, exist_ok=True)
    json_path = os.path.join(args.out_dir, "report.json")
    md_path = os.path.join(args.out_dir, "report.md")
    with open(json_path, "w") as fh:
        json.dump(report, fh, indent=2)
    with open(md_path, "w") as fh:
        fh.write(render_markdown(report))

    # Append-only ledger: one compact line per run so successive iterations of
    # the improvement loop stay comparable even after baselines are overwritten.
    history_line = {
        "ts": report["generated_at"],
        "rev": report["git_rev"],
        "benches": {
            b["id"]: round(b["median_ns"]) for b in benches
        },
        "peak_heap_bytes": heap.get("peak_bytes") if heap else None,
    }
    with open(os.path.join(args.out_dir, "history.jsonl"), "a") as fh:
        fh.write(json.dumps(history_line) + "\n")

    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    print(f"appended {os.path.join(args.out_dir, 'history.jsonl')}")
    print()
    print(render_markdown(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
