#!/usr/bin/env python3
"""Aggregate sweep points into the C(N) curve (issue #3217, AC1/AC6).

Per N across reps: min / median / max of aggregate rows/s (dispersion, AC1),
per-stream rows/s, speedup vs the N=1 median, and MARGINAL EFFICIENCY, defined
as `rows_per_s(N) / (N * rows_per_s(N=1))` -- 1.0 means perfect linear scaling,
0.5 means half the concurrency bought nothing.

Two things this refuses to hide:
  * a point stamped `client_saturated` is EXCLUDED from the curve statistics and
    listed separately. It measured the client, not the engine.
  * a point with `requests_unavailable != 0` is flagged; AC1 asserts zero.

Usage: summarize-sweep.py <points.jsonl> [--out-json f] [--out-table f]
                          [--include-saturated]
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict


def fmt(v, spec="%.1f"):
    return "n/a" if v is None else spec % v


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("points")
    ap.add_argument("--out-json")
    ap.add_argument("--out-table")
    ap.add_argument("--include-saturated", action="store_true",
                    help="fold client-saturated points into the curve (default: exclude and list)")
    a = ap.parse_args()

    recs = [json.loads(l) for l in open(a.points) if l.strip()]
    if not recs:
        print("ERROR: no records in %s" % a.points, file=sys.stderr)
        return 1

    saturated = [r for r in recs if r.get("client_saturated")]
    admission_dirty = [r for r in recs if not r.get("admission_clean", True)]
    usable = recs if a.include_saturated else [r for r in recs if not r.get("client_saturated")]

    by_n = defaultdict(list)
    for r in usable:
        by_n[r["target_concurrency_N"]].append(r)

    def med(rs, key):
        vals = [r[key] for r in rs if r.get(key) is not None]
        return statistics.median(vals) if vals else None

    base = med(by_n.get(1, []), "rows_per_s_aggregate") if 1 in by_n else None

    rows = []
    for n in sorted(by_n):
        rs = by_n[n]
        agg = [r["rows_per_s_aggregate"] for r in rs if r.get("rows_per_s_aggregate") is not None]
        m = statistics.median(agg) if agg else None
        rows.append({
            "N": n,
            "reps": len(rs),
            "rows_per_s_aggregate_min": min(agg) if agg else None,
            "rows_per_s_aggregate_median": m,
            "rows_per_s_aggregate_max": max(agg) if agg else None,
            "rows_per_s_aggregate_spread_pct": (
                (max(agg) - min(agg)) / m * 100.0) if agg and m else None,
            "rows_per_s_per_stream_median": (m / n) if m else None,
            "speedup_vs_N1": (m / base) if (m and base) else None,
            "marginal_efficiency_vs_linear": (m / (n * base)) if (m and base) else None,
            "marginal_efficiency_definition": "rows_per_s(N) / (N * rows_per_s(N=1)); 1.0 = linear",
            "bytes_per_s_logical_uncompressed_median": med(rs, "bytes_per_s_logical_uncompressed"),
            "bytes_per_s_ondisk_compressed_median": med(rs, "bytes_per_s_ondisk_compressed"),
            "bytes_per_s_arrow_wire_capacity_median": med(rs, "bytes_per_s_arrow_wire_capacity"),
            "server_cpu_utilization_of_pinned_set_median": med(rs, "server_cpu_utilization_of_pinned_set"),
            "client_cpu_utilization_of_pinned_set_median": med(rs, "client_cpu_utilization_of_pinned_set"),
            "rows_per_server_cpu_sec_median": med(rs, "rows_per_server_cpu_sec"),
            "IPC_median": med(rs, "IPC"),
            "cycles_per_row_median": med(rs, "cycles_per_row"),
            "instructions_per_row_median": med(rs, "instructions_per_row"),
            "context_switches_per_second_cpu_wide_median": med(rs, "context_switches_per_second_cpu_wide"),
            "server_voluntary_ctxt_switches_per_s_median": med(rs, "server_voluntary_ctxt_switches_per_s"),
            "server_nonvoluntary_ctxt_switches_per_s_median": med(rs, "server_nonvoluntary_ctxt_switches_per_s"),
            "latency_p50_ms_median": med(rs, "latency_p50_ms") or (
                statistics.median([r["latency_ms"]["p50"] for r in rs
                                   if r.get("latency_ms")]) if any(r.get("latency_ms") for r in rs) else None),
            "requests_unavailable_total": sum(r.get("requests_unavailable") or 0 for r in rs),
            "excluded_client_saturated_reps": sum(
                1 for r in recs if r["target_concurrency_N"] == n and r.get("client_saturated")),
        })

    head = recs[0]
    doc = {
        "schema": "ws0-3217.sweep-summary/v1",
        "label": head.get("label"),
        "server_physical_cores_S": head.get("server_physical_cores_S"),
        "server_cpus": head.get("server_cpus"),
        "client_cpus": head.get("client_cpus"),
        "merge_path": head.get("merge_path"),
        "points_total": len(recs),
        "points_excluded_client_saturated": 0 if a.include_saturated else len(saturated),
        "admission_clean_all_points": not admission_dirty,
        "byte_basis_note": (
            "three separately-labelled bases per AC6: logical/uncompressed, on-disk/compressed, and "
            "Arrow wire CAPACITY (client-side buffer capacity, not gRPC-on-the-wire bytes)"),
        "curve": rows,
    }
    if saturated and not a.include_saturated:
        doc["excluded_points"] = [
            {"N": r["target_concurrency_N"], "rep": r["rep"],
             "client_cpu_utilization_of_pinned_set": r["client_cpu_utilization_of_pinned_set"],
             "rows_per_s_aggregate": r["rows_per_s_aggregate"],
             "reason": "client_saturated: measured the loadgen, not the engine"}
            for r in saturated]
    if admission_dirty:
        doc["admission_violations"] = [
            {"N": r["target_concurrency_N"], "rep": r["rep"],
             "requests_unavailable": r["requests_unavailable"]} for r in admission_dirty]

    lines = []
    lines.append("==== WS0 #3217 C(N) SWEEP SUMMARY ====")
    lines.append("label: %s   S=%s   server_cpus=%s   client_cpus=%s   merge_path=%s" % (
        doc["label"], doc["server_physical_cores_S"], doc["server_cpus"],
        doc["client_cpus"], doc["merge_path"]))
    lines.append("points: %d total, %d excluded (client-saturated)   admission_clean: %s" % (
        doc["points_total"], doc["points_excluded_client_saturated"],
        "YES" if doc["admission_clean_all_points"] else "NO  <<< requests_unavailable != 0"))
    lines.append("")
    lines.append("%-4s %-4s %-13s %-13s %-13s %-7s %-13s %-8s %-8s %-7s %-7s %-9s" % (
        "N", "rep", "rows/s min", "rows/s med", "rows/s max", "spr%",
        "rows/s/strm", "speedup", "margeff", "srvUtl", "cliUtl", "cs/s"))
    for r in rows:
        lines.append("%-4s %-4s %-13s %-13s %-13s %-7s %-13s %-8s %-8s %-7s %-7s %-9s" % (
            r["N"], r["reps"],
            fmt(r["rows_per_s_aggregate_min"]), fmt(r["rows_per_s_aggregate_median"]),
            fmt(r["rows_per_s_aggregate_max"]), fmt(r["rows_per_s_aggregate_spread_pct"], "%.1f"),
            fmt(r["rows_per_s_per_stream_median"]),
            fmt(r["speedup_vs_N1"], "%.3f"), fmt(r["marginal_efficiency_vs_linear"], "%.3f"),
            fmt(r["server_cpu_utilization_of_pinned_set_median"], "%.3f"),
            fmt(r["client_cpu_utilization_of_pinned_set_median"], "%.3f"),
            fmt(r["context_switches_per_second_cpu_wide_median"], "%.0f")))
    lines.append("")
    lines.append("byte bases (median, AC6 - never a bare MB/s):")
    lines.append("%-4s %-22s %-22s %-22s" % (
        "N", "logical-uncomp B/s", "on-disk-comp B/s", "arrow-wire-cap B/s"))
    for r in rows:
        lines.append("%-4s %-22s %-22s %-22s" % (
            r["N"], fmt(r["bytes_per_s_logical_uncompressed_median"]),
            fmt(r["bytes_per_s_ondisk_compressed_median"]),
            fmt(r["bytes_per_s_arrow_wire_capacity_median"])))
    lines.append("")
    lines.append("scheduler cost per N (AC5):")
    lines.append("%-4s %-16s %-16s %-16s" % ("N", "cs/s (cpu-wide)", "vol cs/s (proc)", "nonvol cs/s (proc)"))
    for r in rows:
        lines.append("%-4s %-16s %-16s %-16s" % (
            r["N"], fmt(r["context_switches_per_second_cpu_wide_median"], "%.0f"),
            fmt(r["server_voluntary_ctxt_switches_per_s_median"], "%.1f"),
            fmt(r["server_nonvoluntary_ctxt_switches_per_s_median"], "%.1f")))
    if doc.get("excluded_points"):
        lines.append("")
        lines.append("!!! EXCLUDED - CLIENT SATURATED (measured the loadgen, not the engine) !!!")
        for e in doc["excluded_points"]:
            lines.append("    N=%s rep=%s client_util=%.3f rows/s=%.1f" % (
                e["N"], e["rep"], e["client_cpu_utilization_of_pinned_set"], e["rows_per_s_aggregate"]))
    if doc.get("admission_violations"):
        lines.append("")
        lines.append("!!! ADMISSION SHED OBSERVED - AC1 asserts requests_unavailable == 0 !!!")
        for e in doc["admission_violations"]:
            lines.append("    N=%s rep=%s requests_unavailable=%s" % (e["N"], e["rep"], e["requests_unavailable"]))
    table = "\n".join(lines) + "\n"

    if a.out_json:
        open(a.out_json, "w").write(json.dumps(doc, indent=1) + "\n")
    if a.out_table:
        open(a.out_table, "w").write(table)
    else:
        sys.stdout.write(table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
