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
    target/profiling/report.md       human-readable ranked bottleneck table +
                                     a longitudinal per-metric history view
    target/profiling/history.jsonl   the unified append-only ledger (Issue #1566,
                                     Epic A / A5): one JSON object PER METRIC per
                                     run, schema {ts, commit, bench, metric, value,
                                     unit}. The A-series harness benches append to
                                     this same file/schema via benches/bench_ledger,
                                     so criterion medians, bench percentiles, and the
                                     cold-open/memory gauges all live in one ledger.

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


def _git_commit():
    """Full HEAD SHA for the ledger `commit` field, matching the Rust bench_ledger
    writer (which uses `git rev-parse HEAD`) so the two sources group together."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


# Unified ledger schema (Issue #1566): one JSON object per line, one record per
# metric. Shared with the Rust `benches/bench_ledger` module.
LEDGER_FIELDS = ("ts", "commit", "bench", "metric", "value", "unit")


def build_ledger_records(report):
    """Flatten a report into unified per-metric ledger records: one `median_ns`
    record per criterion bench and one `peak_heap_bytes` record when heap data
    exists. All records in a run share the run's `ts` + `commit`."""
    ts = int(
        datetime.datetime.now(datetime.timezone.utc).timestamp()
    )
    commit = _git_commit()
    records = []
    for b in report["benches"]:
        records.append(
            {
                "ts": ts,
                "commit": commit,
                "bench": b["id"],
                "metric": "median_ns",
                "value": round(b["median_ns"]),
                "unit": "ns",
            }
        )
    heap = report.get("heap")
    if heap and heap.get("peak_bytes") is not None:
        records.append(
            {
                "ts": ts,
                "commit": commit,
                "bench": "heap",
                "metric": "peak_heap_bytes",
                "value": heap["peak_bytes"],
                "unit": "bytes",
            }
        )
    return records


def append_ledger(path, records):
    """Append one JSON line per record to the unified ledger (best-effort: a write
    failure is logged and does not abort the report)."""
    try:
        with open(path, "a") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")
    except OSError as e:
        print(f"warning: could not append ledger {path}: {e}", file=sys.stderr)


def read_ledger(path):
    """Read the unified ledger back, returning the valid unified records (schema
    {ts,commit,bench,metric,value,unit}). Malformed and legacy-schema lines (the
    pre-A5 run-summary `{ts,rev,benches,...}` lines) are skipped, so a report over a
    ledger that predates the migration still renders."""
    records = []
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(obj, dict):
                    continue
                if all(k in obj for k in LEDGER_FIELDS):
                    records.append(obj)
    except OSError:
        return []
    return records


def _latest_and_prev(group):
    """Given a metric's records sorted ascending by ts, return (latest_record,
    prev_record) where prev is the most recent record from a DIFFERENT commit than
    the latest (or None). Grouping the delta by *distinct commit* (not by row) means
    re-running the same commit does not show a spurious 0% delta against itself."""
    if not group:
        return None, None
    latest = group[-1]
    latest_commit = latest.get("commit")
    for rec in reversed(group[:-1]):
        if rec.get("commit") != latest_commit:
            return latest, rec
    return latest, None


def summarize_history(records):
    """Group ledger records by (bench, metric); for each, the latest value and the
    delta vs the previous distinct commit. Returns a list of dicts sorted by id."""
    groups = {}
    for rec in records:
        key = (rec["bench"], rec["metric"])
        groups.setdefault(key, []).append(rec)
    rows = []
    for (bench, metric), recs in groups.items():
        recs.sort(key=lambda r: r.get("ts", 0))
        latest, prev = _latest_and_prev(recs)
        row = {
            "bench": bench,
            "metric": metric,
            "unit": latest.get("unit", ""),
            "value": latest.get("value"),
            "commit": latest.get("commit", "unknown"),
            "prev_value": prev.get("value") if prev else None,
        }
        pv = row["prev_value"]
        cv = row["value"]
        if pv not in (None, 0) and isinstance(cv, (int, float)):
            row["delta_pct"] = (cv / pv - 1.0) * 100.0
        rows.append(row)
    rows.sort(key=lambda r: (r["bench"], r["metric"]))
    return rows


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


def _fmt_history_value(value, unit):
    """Human-readable value for the history table, unit-aware."""
    if not isinstance(value, (int, float)):
        return "—"
    if unit == "ns":
        return fmt_ns(value)
    if unit == "bytes":
        return f"{value / 2**20:.1f} MiB"
    if unit == "ratio":
        return f"{value:.3f}"
    return f"{value:g}"


def render_history(history_rows):
    """Render the longitudinal per-metric view: latest value + delta vs the previous
    distinct commit (Issue #1566). Reads the whole unified ledger back."""
    lines = [
        "## History (latest value + delta vs previous commit)",
        "",
    ]
    if not history_rows:
        lines += [
            "- no history yet — the unified ledger "
            "(`target/profiling/history.jsonl`) is empty.",
            "",
        ]
        return lines
    lines += [
        "| bench | metric | latest | Δ vs prev commit | unit | commit |",
        "|-------|--------|--------|------------------|------|--------|",
    ]
    for r in history_rows:
        delta = f"{r['delta_pct']:+.1f}%" if "delta_pct" in r else "—"
        commit = (r.get("commit") or "unknown")[:12]
        lines.append(
            f"| {r['bench']} | {r['metric']} | "
            f"{_fmt_history_value(r['value'], r['unit'])} | {delta} | "
            f"{r['unit']} | `{commit}` |"
        )
    lines.append("")
    return lines


def render_markdown(report, history_rows=None):
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

    lines.append("")
    lines += render_history(history_rows or [])

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
    ledger_path = os.path.join(args.out_dir, "history.jsonl")

    with open(json_path, "w") as fh:
        json.dump(report, fh, indent=2)

    # Unified append-only ledger (Issue #1566): one JSON object PER METRIC in the
    # {ts, commit, bench, metric, value, unit} schema, the same file+schema the
    # A-series harness benches append to via benches/bench_ledger. Append first,
    # then read the whole ledger back so the report's longitudinal view includes
    # this run.
    append_ledger(ledger_path, build_ledger_records(report))
    history_rows = summarize_history(read_ledger(ledger_path))

    md = render_markdown(report, history_rows)
    with open(md_path, "w") as fh:
        fh.write(md)

    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    print(f"appended {ledger_path}")
    print()
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
