#!/usr/bin/env python3
"""Aggregate the #2605 per-cell bench JSONs into the report's results tables.

Reports the MEDIAN across iterations plus the [min, max] range, because the
corpus (10.35 GB) does not fit in the box's free page cache alongside the 7 GB
Cassandra container that produced it, so single-iteration wall times drift by up
to ~2x. A mean would hide that; the median plus an explicit range does not.

Never fabricates a number: a cell with no successful iteration is printed as
MISSING, and an unmeasured peak RSS is printed as `unmeasured`, not 0.
"""
import json
import pathlib
import statistics
import sys

cells_dir = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "cells")
runs = []
for path in sorted(cells_dir.glob("*.json")):
    doc = json.loads(path.read_text())
    for run in doc["runs"]:
        run["_cell"] = path.stem
        runs.append(run)

if not runs:
    sys.exit("no cell results found under %s" % cells_dir)


# `ArmKind` serialises with serde's snake_case rename, so `DataFusion` is
# `data_fusion` on the wire. Named as a constant because getting this wrong
# silently MERGED the two DataFusion parallelism configurations into one cell in
# the first version of this script.
DF_ARM = "data_fusion"


def key(run):
    """Arm identity including the DataFusion parallelism it ran with."""
    arm = run["arm"]
    if arm == DF_ARM:
        return "datafusion@tp%s" % run.get("df_target_partitions")
    return arm


SCENARIOS = ["full_scan_count", "projected_scan", "filtered_scan"]
groups = {}
for run in runs:
    groups.setdefault((run["scenario"], key(run)), []).append(run)

def med(values):
    return statistics.median(values)

print("## Per-cell results (median of %d iterations; [min, max])\n" % max(
    len(v) for v in groups.values()))
# NOTE: no `grpc-write` column. That sub-phase counter is fed by PRODUCTION's
# `ChannelSink`, not by the spike's `SpikeSink`, so it reads 0 here because it was
# never instrumented — not because the channel send was free. Printing it would be
# a fabricated zero.
hdr = ("| scenario | arm | wall s (median) | [min, max] | rows emitted/s "
       "| batches | encode ms | merge ms | decompress ms | cold-fault ms (sum "
       "over 2 producer threads) | peak RSS MiB | rows result |")
print(hdr)
print("|" + "---|" * (hdr.count("|") - 1))
for scenario in SCENARIOS:
    arms = sorted({k[1] for k in groups if k[0] == scenario})
    for arm in ["floor", "row_engine", "datafusion@tp1"] + [
        a for a in arms if a not in ("floor", "row_engine", "datafusion@tp1")
    ]:
        rs = groups.get((scenario, arm))
        if not rs:
            continue
        secs = [r["elapsed_nanos"] / 1e9 for r in rs]
        rss = [r["peak_rss_bytes"] for r in rs if r["peak_rss_bytes"] is not None]
        print("| %s | %s | %.1f | [%.1f, %.1f] | %.0f | %d | %.0f | %.0f | %.0f | %.0f | %s | %d |" % (
            scenario, arm, med(secs), min(secs), max(secs),
            med([r["rows_scanned"] / (r["elapsed_nanos"] / 1e9) for r in rs]),
            med([r["batches"] for r in rs]),
            med([r["subphase_encode_nanos"] / 1e6 for r in rs]),
            med([r["subphase_merge_nanos"] / 1e6 for r in rs]),
            med([r["subphase_decompress_nanos"] / 1e6 for r in rs]),
            med([r["subphase_cold_fault_nanos"] / 1e6 for r in rs]),
            ("%.1f" % (med(rss) / 1048576) if rss else "unmeasured"),
            med([r["rows_result"] for r in rs]),
        ))

print("\n## Derived deltas\n")
print("| scenario | floor s | row s | DF@tp1 s | DF@default s | pushdown s "
      "| vectorized-exec (row/DF@tp1) | concurrency (DF@tp1/DF@default) "
      "| pushdown vs floor | decode-to-column share of floor wall |")
print("|" + "---|" * 9)
for scenario in SCENARIOS:
    def m(arm, field="elapsed_nanos", scale=1e9):
        rs = groups.get((scenario, arm))
        return med([r[field] / scale for r in rs]) if rs else None
    floor = m("floor")
    row = m("row_engine")
    df1 = m("datafusion@tp1")
    dfd = next((m("datafusion@tp%d" % n) for n in range(2, 1025)
                if groups.get((scenario, "datafusion@tp%d" % n))), None)
    push = m("row_pushdown")
    enc = m("floor", "subphase_encode_nanos", 1e9)
    def fmt(v, unit=""):
        return "n/a" if v is None else ("%.1f%s" % (v, unit))
    def ratio(a, b):
        return "n/a" if a is None or b is None or b == 0 else "%.2fx" % (a / b)
    print("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
        scenario, fmt(floor), fmt(row), fmt(df1), fmt(dfd), fmt(push),
        ratio(row, df1), ratio(df1, dfd), ratio(floor, push),
        "n/a" if enc is None or floor in (None, 0) else "%.1f%%" % (100 * enc / floor),
    ))

print("\n## Preconditions (every run)\n")
bad_sources = [r["_cell"] for r in runs if r["sources"] < 2]
bad_arm = [r["_cell"] for r in runs if not r["merge_arm_observed"]]
print("- runs: %d" % len(runs))
print("- post-prune sources < 2: %s" % (bad_sources or "NONE"))
print("- merge arm NOT observed: %s" % (bad_arm or "NONE"))

# THE COMPARABILITY ASSERT. The harness enforces it WITHIN one process; in
# per-cell mode each process sees one arm, so it must be enforced here instead —
# otherwise nothing would notice the comparable arms reading different row sets,
# which is the difference between an engine delta and a correctness delta.
# `row_pushdown` is excluded BY DESIGN: its scan is narrowed, so it emits fewer
# rows (that is what it is measuring).
comparable = sorted({r["rows_scanned"] for r in runs if r["arm"] != "row_pushdown"})
print("- rows_scanned across the COMPARABLE arms (floor/row_engine/datafusion): %s%s"
      % (comparable, "" if len(comparable) == 1
         else "  <-- FAIL: the comparable arms did not read the same rows"))
push = sorted({r["rows_scanned"] for r in runs if r["arm"] == "row_pushdown"})
print("- rows_scanned for row_pushdown (narrowed scan, expected to differ): %s" % push)
print("- reconcile_entries range: %d..%d" % (
    min(r["reconcile_entries"] for r in runs),
    max(r["reconcile_entries"] for r in runs)))
rss_all = [r["peak_rss_bytes"] for r in runs if r["peak_rss_bytes"] is not None]
if rss_all:
    print("- peak RSS across ALL runs: %.1f MiB max (B4 budget 512 MiB)"
          % (max(rss_all) / 1048576))
else:
    print("- peak RSS: unmeasured on this platform")
