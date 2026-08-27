#!/usr/bin/env python3
"""#3299 — aggregate the validated reps into the C(S) table.

Reads a results tree written by `sweep.sh` (each `s<S>-round<R>/` already having
PASSED `guards.py perf-csv` and `guards.py window` at the moment it was measured)
and publishes, per S:

  * aggregate rows/s over the ALIGNED window — median across reps, with the
    min/max spread printed, never a silent average;
  * per-scan p50 rows/s (the median SCAN within a rep, then the median across
    reps);
  * marginal efficiency vs S=1, in #3217's table shape;
  * cycles/row and instructions/row.

Three properties this deliberately keeps:

  * **Medians, with spread.** #3217 shipped reps=1 counter captures and could not
    say whether a point was stable; two of its points later showed >10% spread.
  * **Nothing is derived from a counter that did not read 100.00%.** The guards
    already refused such a rep; this step re-asserts it rather than trusting that
    it ran, because an aggregation that can be pointed at an unvalidated tree
    will eventually be pointed at one.
  * **No LLC column exists at all.** AC3 is deferred on this box (see
    ../host/README.md); an empty or zero-filled column would read as a
    measurement of zero misses.
"""

import argparse
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import guards  # noqa: E402  (same-directory single implementation of the checks)


def median(xs):
    return statistics.median(xs)


def spread_pct(xs):
    lo, hi = min(xs), max(xs)
    m = median(xs)
    return 0.0 if m == 0 else (hi - lo) / m * 100.0


def load_rep(repdir):
    with open(os.path.join(repdir, "window.json")) as fh:
        win = json.load(fh)
    t0, t1, s = int(win["t0_ns"]), int(win["t1_ns"]), int(win["s"])

    # RE-ASSERT the counter contract here (see module docstring): guards.fail()
    # exits non-zero, so an unvalidated tree cannot be aggregated into a table.
    events = [e for e in win["events"].split(",") if e]
    rows_csv = guards.parse_perf_csv(os.path.join(repdir, win["perf_csv"]))
    counters = {}
    for ev in events:
        if ev not in rows_csv:
            guards.fail("PERF_EVENT_ABSENT", f"{repdir}: event {ev!r} absent at aggregation time")
        val, pct = rows_csv[ev]
        if "<not" in val:
            guards.fail("PERF_EVENT_NOT_COUNTED", f"{repdir}: event {ev!r} reads {val!r}")
        if float(pct) < 100.0:
            guards.fail("PERF_MULTIPLEXED", f"{repdir}: event {ev!r} at {pct}% — a scaled estimate")
        counters[ev] = float(val)

    per = guards.attribute_window(repdir, t0, t1, s, guards.DEFAULT_SHORTFALL_BOUND)
    rows = sum(p["rows_in_window"] for p in per)
    window_s = (t1 - t0) / 1e9
    return {
        "s": s,
        "round": int(win["round"]),
        "window_s": window_s,
        "rows": rows,
        "aggregate_rows_per_s": rows / window_s,
        "per_scan_p50_rows_per_s": median([p["rows_per_s"] for p in per]),
        "cycles_per_row": counters["cycles"] / rows,
        "instructions_per_row": counters["instructions"] / rows,
        "ipc": counters["instructions"] / counters["cycles"],
        "l1d_loads_per_row": counters["L1-dcache-loads"] / rows,
        "l1d_load_misses_per_row": counters["L1-dcache-load-misses"] / rows,
        # NOT a utilisation figure. Under CPU-wide (`-C`) counting `task-clock`
        # is elapsed x ncpus BY CONSTRUCTION, so a "utilisation" computed from it
        # would read 1.000 at every S no matter what the machine did — a column
        # that cannot vary is not a measurement. What IS real is unhalted cycles
        # per pinned-CPU-second: at full occupancy it is the clock rate, and it
        # falls if the cores idle OR if the clock drops. It cannot separate those
        # two causes, and is labelled accordingly rather than called "GHz".
        "unhalted_cycles_per_cpu_s": counters["cycles"] / (window_s * 2 * s),
        "counter_window_drift_frac": guards.counter_window_drift(repdir, win, counters),
        "shortfall_max_frac": max(p["attribution_shortfall_frac"] for p in per),
    }


def collect(results):
    reps = []
    manifest = os.path.join(results, "manifest.jsonl")
    if not os.path.exists(manifest):
        print(f"FATAL: {manifest} absent — nothing measured", file=sys.stderr)
        sys.exit(2)
    with open(manifest) as fh:
        for line in fh:
            if line.strip():
                reps.append(load_rep(json.loads(line)["rundir"]))
    return reps


def emit_table(reps, min_reps):
    by_s = {}
    for r in reps:
        by_s.setdefault(r["s"], []).append(r)

    thin = {s: len(v) for s, v in by_s.items() if len(v) < min_reps}
    base = None
    if 1 in by_s:
        base = median([r["aggregate_rows_per_s"] for r in by_s[1]])

    print("## C(S) — bare-scan scaling curve, aligned window\n")
    print(f"Corpus: #3096 'Corpus B' (4,000,000 rows, 693.69 B/row, UNCOMPRESSED). "
          f"Reps per point: {sorted({s: len(v) for s, v in by_s.items()}.items())}. "
          f"Medians; spread = (max-min)/median.\n")
    print("| S | aggregate rows/s (median) | spread | per-scan p50 rows/s | "
          "marg. eff. vs S=1 | cycles/row | instr/row | IPC | L1d miss/row | "
          "unhalted Gcyc/CPU·s |")
    print("|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for s in sorted(by_s):
        v = by_s[s]
        agg = [r["aggregate_rows_per_s"] for r in v]
        m = median(agg)
        eff = "n/a" if base is None else f"{(m / s) / base:.3f}"
        print(
            f"| {s} | {m:,.0f} | {spread_pct(agg):.1f}% | "
            f"{median([r['per_scan_p50_rows_per_s'] for r in v]):,.0f} | "
            f"**{eff}** | "
            f"{median([r['cycles_per_row'] for r in v]):,.1f} | "
            f"{median([r['instructions_per_row'] for r in v]):,.1f} | "
            f"{median([r['ipc'] for r in v]):.3f} | "
            f"{median([r['l1d_load_misses_per_row'] for r in v]):,.2f} | "
            f"{median([r['unhalted_cycles_per_cpu_s'] for r in v]) / 1e9:.3f} |"
        )
    print()
    print("`marg. eff. vs S=1` = (aggregate rows/s at S ÷ S) ÷ (aggregate rows/s at S=1). "
          "1.000 would be perfect scaling.\n")
    print("**No LLC column exists.** Every LLC instrument on this host is unavailable "
          "(`../host/README.md`), so AC3 is DEFERRED, not approximated: a hard 0 from a "
          "dead counter would read as 'no misses'.\n")
    print("`unhalted Gcyc/CPU·s` is unhalted cycles per pinned logical-CPU-second. It is "
          "NOT a utilisation percentage and NOT a reported clock: it conflates occupancy "
          "with frequency, and is shown so a collapse in either is visible.\n")
    print(f"Counter-window agreement (max over reps): "
          f"{max(r['counter_window_drift_frac'] for r in reps):.2e} — perf's enabled "
          f"interval versus the driver's [T0, T1]. This is the measured proof that the "
          f"counters and the rows were taken over the SAME interval.\n")
    print(f"Max attribution shortfall over all reps: "
          f"{max(r['shortfall_max_frac'] for r in reps):.4%} of the window "
          f"(bound {guards.DEFAULT_SHORTFALL_BOUND:.2%}). Rows are only counted between "
          f"progress records the workers actually emitted, so this biases every rows/s "
          f"figure DOWNWARD by at most that fraction.\n")
    if thin:
        print(f"**WARNING — under-replicated points:** {thin} have fewer than {min_reps} reps. "
              f"Their medians carry no usable dispersion (this is #3217's gap 1).\n")


def emit_equivalence(results):
    """Worker vs `ws0-scan-bench` on the same core, same session, same bytes."""
    with open(os.path.join(results, "equiv-scan-bench.json")) as fh:
        bench = json.load(fh)
    with open(os.path.join(results, "equiv-worker-window.json")) as fh:
        worker = json.load(fh)
    bench_rps = median([p["rows_per_sec"] for p in bench["passes"]])
    worker_rps = worker["aggregate_rows_per_s"]
    delta = (worker_rps - bench_rps) / bench_rps
    print("## Equivalence control — #3299 worker vs the rig's `ws0-scan-bench`\n")
    print(f"| arm | rows/s | note |")
    print(f"|---|--:|---|")
    print(f"| `ws0-scan-bench --passes 3` (median pass) | {bench_rps:,.0f} | the #3096/#3272 rig's bare-scan arm |")
    print(f"| `ws0-3299-scan-worker` S=1, aligned window | {worker_rps:,.0f} | this harness |")
    print(f"\nDelta: **{delta:+.2%}**. The worker's figure is measured over an aligned "
          f"window whose row attribution is bounded-low, so a small negative delta is "
          f"expected; a large divergence in either direction would mean the two are NOT "
          f"the same code path and the S=1 point is not comparable to the existing rig's.\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", help="results tree written by sweep.sh")
    ap.add_argument("--equivalence", metavar="RESULTS", help="render the equivalence control instead")
    ap.add_argument("--min-reps", type=int, default=3)
    args = ap.parse_args()
    if args.equivalence:
        emit_equivalence(args.equivalence)
        return
    if not args.results:
        ap.error("--results is required")
    emit_table(collect(args.results), args.min_reps)


if __name__ == "__main__":
    main()
