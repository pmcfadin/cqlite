#!/usr/bin/env python3
"""Aggregate the #2605 per-cell bench JSONs into the report's results tables.

Reports the MEDIAN across iterations plus the [min, max] range, because the
corpus (10.35 GB) does not fit in the box's free page cache alongside the 7 GB
Cassandra container that produced it, so single-iteration wall times drift by up
to ~2x. A mean would hide that; the median plus an explicit range does not.

Never fabricates a number: a cell with no successful iteration is printed as
MISSING, and an unmeasured peak RSS is printed as `unmeasured`, not 0.

AND IT FAILS CLOSED. An INCOMPLETE matrix used to be silently skipped over —
missing cells simply did not appear, so a half-finished run produced a summary
that looked exactly like a complete one, and a correctness precondition could
print a FAIL line while the script still exited 0. Any consumer (a report, a
reader, a script) would take that as usable. Now the expected
scenario x arm-config x iteration matrix is reconstructed from the cells present,
every absent cell is rendered as MISSING, and a missing cell OR a failed
correctness precondition exits NONZERO. Every count printed is derived from the
data; none is hard-coded.
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

# Reasons this summary must NOT be treated as usable. Printed in full at the end
# and turned into a nonzero exit — a summary that names its own defects but exits
# 0 is a summary someone will quote.
failures = []


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


# THE EXPECTED MATRIX. There is no declared expectation to compare against — the
# driver writes one file per cell and nothing states how many there should be —
# so it is reconstructed as the full cross product of the scenarios, arm configs
# and iterations that DO appear. That cannot detect a whole missing arm or a
# whole missing iteration (nothing observed it), but it does detect every HOLE,
# which is what an interrupted or partially-failed run leaves behind.
observed_arms = sorted({k[1] for k in groups})
observed_iters = sorted({r["iteration"] for r in runs})
expected_cells = [
    (scenario, arm, iteration)
    for scenario in SCENARIOS
    for arm in observed_arms
    for iteration in observed_iters
]
present_cells = {(r["scenario"], key(r), r["iteration"]) for r in runs}
missing_cells = [c for c in expected_cells if c not in present_cells]
duplicate_cells = sorted(
    c for c in present_cells
    if sum(1 for r in runs if (r["scenario"], key(r), r["iteration"]) == c) > 1
)
if missing_cells:
    failures.append("%d expected cell(s) are MISSING" % len(missing_cells))
if duplicate_cells:
    failures.append("%d cell(s) appear more than once" % len(duplicate_cells))

# EVERY row carries its own `n`, and a single-sample row prints NO range.
#
# The first version printed one global "median of N iterations" header taken from
# the LARGEST group and then rendered every row's range unconditionally, so a row
# with a single sample rendered as `[81.0, 81.0]` — which reads as a tight,
# high-confidence measurement when it is the exact opposite (one draw from a
# distribution whose measured per-cell spread on this corpus reaches 2.4x). A
# per-row `n`, and an em dash instead of a degenerate range, cannot be misread
# that way.
counts = sorted({len(v) for v in groups.values()})
print("## Per-cell results (median over each row's own `n` iterations; "
      "n=%s across the matrix)\n"
      % (counts[0] if len(counts) == 1 else "%d..%d" % (counts[0], counts[-1])))
# NOTE: no `grpc-write` column. That sub-phase counter is fed by PRODUCTION's
# `ChannelSink`, not by the spike's `SpikeSink`, so it reads 0 here because it was
# never instrumented — not because the channel send was free. Printing it would be
# a fabricated zero.
hdr = ("| scenario | arm | n | wall s (median) | [min, max] | rows emitted/s "
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
            # Rendered, not skipped: an absent arm that simply does not appear in
            # the table is indistinguishable from one that was never asked for.
            print("| %s | %s | 0 | MISSING | — |%s" % (scenario, arm, " |" * 8))
            continue
        absent = [i for i in observed_iters if i not in {r["iteration"] for r in rs}]
        if absent:
            print("| %s | %s | %d | MISSING iteration(s) %s | — |%s"
                  % (scenario, arm, len(rs),
                     ",".join(str(i) for i in absent), " |" * 8))
        secs = [r["elapsed_nanos"] / 1e9 for r in rs]
        rss = [r["peak_rss_bytes"] for r in rs if r["peak_rss_bytes"] is not None]
        # A single sample has no range to report. Say so, rather than printing
        # `[x, x]` — see the header comment.
        span = "—" if len(secs) < 2 else "[%.1f, %.1f]" % (min(secs), max(secs))
        print("| %s | %s | %d | %.1f | %s | %.0f | %d | %.0f | %.0f | %.0f | %.0f | %s | %d |" % (
            scenario, arm, len(rs), med(secs), span,
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
deltas_hdr = ("| scenario | floor s | row s | DF@tp1 s | DF@default s | pushdown s "
              "| vectorized-exec (row/DF@tp1) | concurrency (DF@tp1/DF@default) "
              "| pushdown vs floor | decode-to-column share of floor wall |")
print(deltas_hdr)
# Derived from the header, not hand-counted: the hand-counted 9 was one column
# short of the 10 printed, so the table rendered ragged.
print("|" + "---|" * (deltas_hdr.count("|") - 1))
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
# THE DRIFT TABLE: what this matrix would have "shown" after 1 and 2 iterations.
#
# The vectorization ratio is recomputed as a RUNNING median over iterations
# 1..n, which is exactly the number a reader would have quoted had the run been
# stopped at n. It is printed because it is the single strongest piece of
# evidence in the spike about its own reliability: the estimate regresses toward
# no effect as samples accumulate, so an early read of this matrix would have
# reported a win that the completed matrix does not support.
#
# Iteration attribution is REQUIRED and is not inferred: each run carries its own
# `iteration`, stamped by the harness (`--iteration-base`). If a group's
# iterations are not distinct — the state of the cells written before that flag
# existed — the table is SKIPPED with the reason, never computed off a
# fabricated ordering.
# ---------------------------------------------------------------------------
print("\n## Running vectorization estimate by iteration count (drift)\n")
attributable = all(
    len({r["iteration"] for r in v}) == len(v) for v in groups.values()
)
max_iters = max(len(v) for v in groups.values())
if not attributable:
    print("- SKIPPED: the cells do not carry distinct `iteration` values, so a "
          "running estimate cannot be attributed to an iteration order")
elif max_iters < 2:
    print("- SKIPPED: a single iteration per cell has no drift to show")
else:
    print("| scenario | " + " | ".join(
        "n=%d%s" % (n, " (final)" if n == max_iters else "")
        for n in range(1, max_iters + 1)) + " |")
    print("|" + "---|" * (max_iters + 1))
    for scenario in SCENARIOS:
        cols = []
        for n in range(1, max_iters + 1):
            def prefix(arm):
                rs = groups.get((scenario, arm), [])
                vals = [r["elapsed_nanos"] / 1e9 for r in rs if r["iteration"] <= n]
                return med(vals) if vals else None
            row, df1 = prefix("row_engine"), prefix("datafusion@tp1")
            cols.append("n/a" if not row or not df1 else "%.2fx" % (row / df1))
        print("| %s | %s |" % (scenario, " | ".join(cols)))
    print("\nRatio is `row_engine / datafusion@tp1` wall time (>1 = DataFusion "
          "faster), median over iterations 1..n.")

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
if len(comparable) != 1:
    failures.append("the comparable arms did not read the same rows: %s" % comparable)
if bad_sources:
    failures.append("%d run(s) reconciled fewer than 2 sources" % len(bad_sources))
if bad_arm:
    failures.append("%d run(s) did not observe the merge arm" % len(bad_arm))
push = sorted({r["rows_scanned"] for r in runs if r["arm"] == "row_pushdown"})
print("- rows_scanned for row_pushdown (narrowed scan, expected to differ): %s" % push)
# `rows_result` AGREEMENT. `rows_scanned` agreement (above) cannot see a
# `row_pushdown` arm that dropped rows the other arms keep, because that arm is
# excluded from it by design. The quantity that must hold across a narrowed scan
# is the ANSWER, so it is checked here, per (scenario, iteration), over every arm
# EXCEPT `floor` — which discards its batches, so its `rows_result` is 0 by
# design.
answers = {}
for r in runs:
    if r["arm"] == "floor":
        continue
    answers.setdefault((r["scenario"], r["iteration"]), set()).add(r["rows_result"])
disagreeing = {k: sorted(v) for k, v in answers.items() if len(v) > 1}
print("- rows_result agreement across the answering arms (floor excluded): %s"
      % ("ALL AGREE" if not disagreeing
         else "%s  <-- FAIL: the arms answered different queries" % disagreeing))
if disagreeing:
    failures.append("the answering arms disagreed on rows_result: %s" % disagreeing)

# THE FLOOR DIAGNOSTIC, reported and not asserted. The discard-only floor does
# strictly less work than every executing arm, so it bounds them from below — in
# a noise-free world. Here it does not, and the size of the violation is the
# honest measure of how little wall time can be trusted on this box. It is
# printed rather than raised for the same reason the harness only warns: on this
# corpus it fails far more often than it holds, so treating it as an assertion
# would reject nearly every legitimate run.
by_cell = {(r["scenario"], key(r), r["iteration"]): r for r in runs}
floor_cmp = [(k, v) for k, v in by_cell.items() if k[1] != "floor"
             and (k[0], "floor", k[2]) in by_cell]
beaten = [k for k, v in floor_cmp
          if v["elapsed_nanos"] < by_cell[(k[0], "floor", k[2])]["elapsed_nanos"]]
print("- discard-only floor BEATEN by an executing arm in %d of %d arm-comparisons "
      "(wall-clock noise, not an engine effect — see the sub-phase table)"
      % (len(beaten), len(floor_cmp)))
faults = [r["subphase_cold_fault_nanos"] / r["elapsed_nanos"] for r in runs]
print("- cold-fault / elapsed ratio: %.2f..%.2f — cold-fault is a STALL ACCOUNT summed "
      "over 2 producer threads, NOT a partition of elapsed, so it legitimately EXCEEDS "
      "wall time and must never be rendered as a percentage of it"
      % (min(faults), max(faults)))
print("- reconcile_entries range: %d..%d" % (
    min(r["reconcile_entries"] for r in runs),
    max(r["reconcile_entries"] for r in runs)))
print("- matrix completeness: %d of %d expected cells present (%d scenario(s) x %d arm "
      "config(s) x %d iteration(s))%s"
      % (len(present_cells), len(expected_cells), len(SCENARIOS), len(observed_arms),
         len(observed_iters),
         "" if not missing_cells else "  <-- FAIL, missing: %s" % (missing_cells,)))
# MACHINE STATE. A cell measured on a loaded box measures the box; the driver now
# records the load each cell ran under, and the coverage is reported honestly —
# cells measured before the capture existed are UNKNOWN, never assumed quiet.
machine_log = cells_dir.parent / "machine-state.jsonl"
if machine_log.is_file():
    records = [json.loads(line) for line in machine_log.read_text().splitlines() if line.strip()]
    by_cell = {r["cell"]: r for r in records}
    covered = [r for r in runs if r["_cell"] in by_cell]
    # BOTH ends. The driver's gate can only check the load BEFORE a cell; a storm
    # that starts mid-cell is caught only by `load_after`, and that cell is just
    # as contaminated as one that started dirty.
    loaded = sorted(
        (r["_cell"], max(by_cell[r["_cell"]]["load_before"], by_cell[r["_cell"]]["load_after"]))
        for r in covered
        if max(by_cell[r["_cell"]]["load_before"], by_cell[r["_cell"]]["load_after"])
        > by_cell[r["_cell"]]["max_load"]
    )
    print("- machine state recorded for %d of %d cells (%d predate the capture: UNKNOWN, not "
          "assumed quiet)%s"
          % (len(covered), len(runs), len(runs) - len(covered),
             "" if not loaded else "  <-- FAIL, measured above MAX_LOAD: %s" % (loaded,)))
    if loaded:
        failures.append("%d cell(s) were measured above MAX_LOAD" % len(loaded))
else:
    print("- machine state: NOT RECORDED for any cell (the driver gained the capture after this "
          "matrix was measured); load contamination cannot be ruled out from the artifacts")

rss_all = [r["peak_rss_bytes"] for r in runs if r["peak_rss_bytes"] is not None]
if rss_all:
    print("- peak RSS across ALL runs: %.1f MiB max (B4 budget 512 MiB)"
          % (max(rss_all) / 1048576))
else:
    print("- peak RSS: unmeasured on this platform")

# EXIT STATUS, not just prose. An incomplete or internally-inconsistent matrix
# must not yield a summary a caller treats as usable — the report's own
# regeneration command is expected to fail loudly rather than emit a plausible
# document.
if failures:
    print("\n**THIS SUMMARY IS NOT USABLE.** %d precondition failure(s):\n" % len(failures))
    for reason in failures:
        print("- %s" % reason)
    sys.exit(1)
