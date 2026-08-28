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

# ---------------------------------------------------------------------------
# I/O-controlled engine comparison.
#
# WHY THIS SECTION EXISTS. Raw wall time on this box CANNOT resolve the
# engine delta: the 10.35 GB corpus does not fit in the free page cache
# (30 GB box, 7 GB of it the Cassandra container that wrote the corpus), so
# per-iteration wall time swings by up to ~2.4x and the `floor` arm — which by
# construction does STRICTLY LESS work than every other arm — sometimes measures
# SLOWER than them. That is a proof, not a suspicion, that raw wall time here is
# noise-dominated, and quoting a wall-time ratio as a "vectorized-exec delta"
# would be reporting cache luck as an engine property.
#
# `stream_cold_fault` measures exactly the confounder (synchronous body-chunk
# page-in on the producer threads), and it is recorded per run. So the honest
# move is a covariate adjustment: regress wall on cold-fault across all runs and
# compare the per-arm mean RESIDUAL. A positive residual means "slower than this
# run's I/O alone predicts", which is the engine signal with I/O controlled.
#
# It is stated with its limits: this is an observational adjustment over 45 runs
# on one box, not a controlled experiment, and the residual spread is reported so
# a reader can see whether a difference clears it.
print("\n## Engine comparison with I/O controlled\n")
walls = [r["elapsed_nanos"] / 1e9 for r in runs]
faults = [r["subphase_cold_fault_nanos"] / 1e9 for r in runs]
n = len(runs)
mw, mf = sum(walls) / n, sum(faults) / n
sxx = sum((f - mf) ** 2 for f in faults)
sxy = sum((f - mf) * (w - mw) for f, w in zip(faults, walls))
if sxx == 0:
    print("- cold-fault is constant across runs; no adjustment possible")
else:
    slope = sxy / sxx
    intercept = mw - slope * mf
    sw = sum((w - mw) ** 2 for w in walls)
    r2 = (sxy ** 2) / (sxx * sw) if sw else float("nan")
    print("- wall = %.2f s + %.3f x cold_fault_s  (R^2 = %.3f over %d runs)"
          % (intercept, slope, r2, n))
    print("- i.e. %.0f%% of the wall-time variance across every run in this matrix is"
          " explained by page-in time ALONE\n" % (100 * r2))
    print("| scenario | arm | mean residual s (+ = slower than I/O predicts) | residual [min, max] |")
    print("|---|---|---|---|")
    for scenario in SCENARIOS:
        arms = sorted({k[1] for k in groups if k[0] == scenario})
        for arm in ["floor", "row_engine", "datafusion@tp1"] + [
            a for a in arms if a not in ("floor", "row_engine", "datafusion@tp1")
        ]:
            rs = groups.get((scenario, arm))
            if not rs:
                continue
            res = [
                r["elapsed_nanos"] / 1e9
                - (intercept + slope * r["subphase_cold_fault_nanos"] / 1e9)
                for r in rs
            ]
            print("| %s | %s | %+.1f | [%+.1f, %+.1f] |"
                  % (scenario, arm, sum(res) / len(res), min(res), max(res)))

# ---------------------------------------------------------------------------
# The STABLE half of the measurement: producer CPU sub-phases.
print("\n## Producer CPU sub-phases (the stable signal)\n")
print("| bucket | median ms over all 45 runs | [min, max] | us/row at 1,899,750 rows |")
print("|---|---|---|---|")
for label, field in [
    ("stream_encode (row->column transpose)", "subphase_encode_nanos"),
    ("stream_merge (merge + reconcile + row materialize)", "subphase_merge_nanos"),
    ("stream_decompress (LZ4)", "subphase_decompress_nanos"),
    ("stream_cold_fault (page-in, 2 threads summed)", "subphase_cold_fault_nanos"),
]:
    vals = [r[field] / 1e6 for r in runs]
    print("| %s | %.0f | [%.0f, %.0f] | %.2f |"
          % (label, med(vals), min(vals), max(vals), med(vals) * 1000 / 1899750))

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
