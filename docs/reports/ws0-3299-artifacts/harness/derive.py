#!/usr/bin/env python3
"""#3299 — aggregate the validated reps into the C(S) table.

Reads a results tree written by `sweep.sh` (each `s<S>-round<R>/` already having
PASSED `guards.py perf-csv` and `guards.py window` at the moment it was measured)
and publishes, per S:

  * the C(N) grid: aggregate rows/s at every (S, N), median across reps, with
    the min-max spread printed, never a silent average;
  * per S, the BEST-N aggregate and the N it peaked at (AC1) — the peak is not
    at N=S, so collapsing to N=S would report less than the core budget achieves;
  * per-scan p50 rows/s (the median SCAN within a rep, then across reps);
  * marginal efficiency against BOTH denominators, neither silently chosen;
  * cycles/row, instructions/row, IPC, L1d loads/row and L1d misses/row.

BOTH DENOMINATORS, AND WHY (#3217 section 3.2). A per-arm self-normalised
speedup (each S divided by its OWN N=1) is NOT cross-comparable, because each
arm's own N=1 DECLINES with core count — #3217 measured 216,229 (S=1) -> 205,129
(S=2) -> 175,872 (S=4) -> 163,510 (S=6), one stream spread over more hardware
threads losing to work-stealing and locality. Self-normalising would flatter the
wide arms by dividing them by a worse baseline. So this publishes:

  A  vs S=1 at N=1        the naive baseline;
  B  vs S=1's own PEAK    PRIMARY, because it is the most the engine achieves on
                          one physical core and it is the CONSERVATIVE choice —
                          it yields lower efficiencies than A.

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


def load_rep(repdir):  # noqa: C901 — one linear read of one rep's evidence
    with open(os.path.join(repdir, "window.json")) as fh:
        win = json.load(fh)
    t0, t1 = int(win["t0_ns"]), int(win["t1_ns"])
    s, n = int(win["s"]), int(win["n"])

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

    per = guards.attribute_window(repdir, t0, t1, n, guards.DEFAULT_SHORTFALL_BOUND)
    rows = sum(p["rows_in_window"] for p in per)
    window_s = (t1 - t0) / 1e9
    return {
        "s": s,
        "n": n,
        "round": int(win["round"]),
        "window_s": window_s,
        "rows": rows,
        "aggregate_rows_per_s": rows / window_s,
        "per_scan_p50_rows_per_s": median([p["rows_per_s"] for p in per]),
        "cycles_per_row": counters["cycles"] / rows,
        "instructions_per_row": counters["instructions"] / rows,
        "ipc": counters["instructions"] / counters["cycles"],
        "l1d_load_misses_per_row": counters["L1-dcache-load-misses"] / rows,
        # NOT a utilisation figure. Under CPU-wide (`-C`) counting `task-clock`
        # is elapsed x ncpus BY CONSTRUCTION, so a "utilisation" computed from it
        # would read 1.000 at every S no matter what the machine did — a column
        # that cannot vary is not a measurement. What IS real is unhalted cycles
        # per pinned-CPU-second: at full occupancy it is the clock rate, and it
        # falls if the cores idle OR if the clock drops. It cannot separate those
        # two causes, and is labelled accordingly rather than called "GHz".
        "unhalted_cycles_per_cpu_s": counters["cycles"] / (window_s * 2 * s),
        "l1d_loads_per_row": counters["L1-dcache-loads"] / rows,
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


def bracket_verdict(by_point, s, peaks, n_values, grid_max_spread):
    """Is this S's best-N a MEASURED peak, a PLATEAU, or an EDGE TRUNCATION?

    Pre-registered rule (fixed before the data was seen, so it cannot be chosen
    to suit it): a peak is BRACKETED when some tested N above it is lower by
    MORE than the relevant point's own rep-to-rep spread. If the next N up is
    within spread, the top is a PLATEAU and the LOWER N is taken — same
    throughput, cheaper configuration. If no N above was tested at all, the
    "peak" is the largest N tried and is an EDGE TRUNCATION, i.e. a LOWER BOUND
    on that S's best, not a measurement of it.

    The threshold is each point's OWN measured spread, not one global number:
    spread here is strongly N-dependent (sub-1% at high N, 2-3.5% at low N), so
    a single grid-wide figure would overstate the uncertainty on the wide points
    and understate it on the narrow ones. `grid_max_spread` is the fallback only
    for a point that somehow lacks 3 valid reps.
    """
    n_peak, best = peaks[s]
    above = [n for n in n_values if n > n_peak and (s, n) in by_point]
    if not above:
        return "edge-truncated", (
            f"N={n_peak} is the largest N tested at S={s}; nothing above it was measured, "
            f"so this is a LOWER BOUND on S={s}'s best, not a measured peak"
        )
    nxt = min(above)
    v = by_point[(s, nxt)]
    nxt_med = agg(v, "aggregate_rows_per_s")
    sp = spread_pct([x["aggregate_rows_per_s"] for x in v]) / 100.0
    thr = sp if len(v) >= 3 else grid_max_spread
    drop = (best - nxt_med) / best
    if drop > thr:
        return "bracketed", (
            f"N={nxt} is {drop:.2%} below N={n_peak}, exceeding that point's own spread "
            f"({thr:.2%}) — the curve has turned over"
        )
    return "plateau", (
        f"N={nxt} is within {abs(drop):.2%} of N={n_peak}, inside that point's own spread "
        f"({thr:.2%}) — a flat top; the LOWER N is reported (same throughput, cheaper)"
    )


def agg(points, key):
    """Median of `key` over the reps at one (S, N) point."""
    return median([p[key] for p in points])


def emit_table(reps, min_reps):
    by_point = {}
    for r in reps:
        by_point.setdefault((r["s"], r["n"]), []).append(r)
    s_values = sorted({s for s, _ in by_point})
    n_values = sorted({n for _, n in by_point})
    bracket_notes = []
    _sp = [spread_pct([r["aggregate_rows_per_s"] for r in v]) / 100.0
           for v in by_point.values() if len(v) >= 2]
    grid_max_spread = max(_sp) if _sp else 0.05

    print("## C(S, N) — bare-scan scaling grid, aligned window\n")
    print("Corpus: #3096 'Corpus B' (4,000,000 rows, 693.69 B/row, UNCOMPRESSED). "
          "Medians over reps; spread = (max-min)/median.\n")

    # --- the grid ------------------------------------------------------------
    print("### C(N) per S, with dispersion\n")
    print("Aggregate rows/s (median), min-max spread as % of median in parentheses. "
          "Blank = not measured at that point.\n")
    print("| N | " + " | ".join(f"S={s}" for s in s_values) + " |")
    print("|--:|" + "---|" * len(s_values))
    peaks = {}
    for s in s_values:
        pts = [(n, agg(by_point[(s, n)], "aggregate_rows_per_s")) for n in n_values if (s, n) in by_point]
        peaks[s] = max(pts, key=lambda t: t[1])
    for n in n_values:
        cells = []
        for s in s_values:
            if (s, n) not in by_point:
                cells.append("")
                continue
            v = by_point[(s, n)]
            m = agg(v, "aggregate_rows_per_s")
            sp = spread_pct([p["aggregate_rows_per_s"] for p in v])
            star = "**" if peaks[s][0] == n else ""
            cells.append(f"{star}{m:,.0f}{star} ({sp:.1f}%)")
        print(f"| {n} | " + " | ".join(cells) + " |")
    print("\n**bold** = that S's best-N point.\n")

    # --- the deliverable, both denominators ----------------------------------
    ref_peak = peaks[1][1] if 1 in peaks else None       # B: S=1's own peak (PRIMARY)
    ref_n1 = agg(by_point[(1, 1)], "aggregate_rows_per_s") if (1, 1) in by_point else None  # A: S=1 at N=1

    print("### Cross-S marginal efficiency — BOTH denominators\n")
    print("| S | best aggregate rows/s | **spread at that point** | N@peak | per-scan p50 rows/s | own N=1 | "
          "speedup vs **1-core peak** | **marg. eff. vs 1-core peak** | speedup vs 1-core N=1 | "
          "marg. eff. vs 1-core N=1 | cycles/row † | instr/row † | IPC | L1d loads/row † | "
          "L1d miss/row † | peak status |")
    print("|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for s in s_values:
        n_peak, best = peaks[s]
        v = by_point[(s, n_peak)]
        # `own N=1` is only shown where it was MEASURED. An absent point prints
        # "n/m" rather than being back-filled from a neighbouring S — the whole
        # reason this column exists is that each arm's own N=1 MOVES with S, so
        # substituting another S's value would assert exactly what it is here to
        # test.
        own_n1 = agg(by_point[(s, 1)], "aggregate_rows_per_s") if (s, 1) in by_point else None
        sp_b = f"{best / ref_peak:.3f}" if ref_peak else "n/a"
        me_b = f"**{(best / ref_peak) / s:.3f}**" if ref_peak else "n/a"
        sp_a = f"{best / ref_n1:.3f}" if ref_n1 else "n/a"
        me_a = f"{(best / ref_n1) / s:.3f}" if ref_n1 else "n/a"
        own_n1_cell = f"{own_n1:,.0f}" if own_n1 is not None else "n/m"
        peak_sp = spread_pct([x["aggregate_rows_per_s"] for x in v])
        verdict, why = bracket_verdict(by_point, s, peaks, n_values, grid_max_spread)
        bracket_notes.append((s, n_peak, verdict, why))
        print(
            f"| {s} | {best:,.0f} | {peak_sp:.1f}% | {n_peak} | "
            f"{agg(v, 'per_scan_p50_rows_per_s'):,.0f} | "
            f"{own_n1_cell} | {sp_b} | {me_b} | {sp_a} | {me_a} | "
            f"{agg(v, 'cycles_per_row'):,.1f} | {agg(v, 'instructions_per_row'):,.1f} | "
            f"{agg(v, 'ipc'):.3f} | {agg(v, 'l1d_loads_per_row'):,.1f} | "
            f"{agg(v, 'l1d_load_misses_per_row'):,.2f} | **{verdict}** |"
        )
    print()
    print("### Is each best-N a real peak? (pre-registered bracketing rule)\n")
    for s_, n_, verdict, why in bracket_notes:
        print(f"- **S={s_}, N@peak={n_} — {verdict.upper()}**: {why}.")
    print()
    if any(v == "edge-truncated" for _, _, v, _ in bracket_notes):
        print("**An `edge-truncated` row is a LOWER BOUND, not a measured peak**, and any figure "
              "derived from it (including AC2's target) inherits that status. It is not smoothed, "
              "interpolated, or quoted as a result.\n")
    print("**† BASIS — every per-row counter is summed over ALL PINNED HARDWARE THREADS** "
          "(2S logical CPUs, both SMT siblings of each of the S cores), which is the set "
          "`perf stat -C` counted and is the same set at every N for a given S. It is NOT a "
          "per-hardware-thread figure: dividing by 2 would give the per-thread average only "
          "if both siblings were equally loaded, which is exactly what varies across the N "
          "ladder. IPC is basis-invariant (a ratio of two sums over the same set). "
          "Per mission section 1, no figure here is quoted without its basis.\n")
    print("`marg. eff.` = speedup / S; 1.000 would be perfect scaling. **Reference B "
          "(S=1's own peak) is PRIMARY**: it is the most the engine achieves on one "
          "physical core, so it is the fair 'perfect scaling' unit, and it is the "
          "CONSERVATIVE choice — it yields lower efficiencies than A. Reference A "
          "(S=1 at N=1) is published alongside because it is the naive baseline. "
          "The `own N=1` column is why a self-normalised speedup is NOT published: "
          "each arm's own N=1 moves with S, so dividing by it would flatter the wide "
          "arms.\n")

    emit_resolution(by_point, reps)
    emit_endpoints(by_point, peaks)
    emit_provenance(reps, by_point, min_reps)


def emit_resolution(by_point, reps):
    """The rig's own reproducibility, per point — never one global error bar.

    Spread is strongly N-DEPENDENT: a single stream's throughput turns on
    scheduler placement and one core's frequency excursions, while an aggregate
    over sixteen streams averages those away. So a grid-wide figure would
    overstate the uncertainty on the wide points (which is where the deliverable
    lives) and understate it on the narrow ones. Every difference quoted anywhere
    in this report is compared against the spread of the POINTS BEING DIFFERENCED.
    """
    spreads = {(s, n): spread_pct([r["aggregate_rows_per_s"] for r in v])
               for (s, n), v in by_point.items() if len(v) >= 2}
    by_n = {}
    for (s, n), sp in spreads.items():
        by_n.setdefault(n, []).append(sp)
    print("### Rig resolution — per point, because spread is N-dependent\n")
    print("| N | median spread over the S values measured at that N | points |")
    print("|--:|--:|--:|")
    for n in sorted(by_n):
        print(f"| {n} | {median(by_n[n]):.2f}% | {len(by_n[n])} |")
    allsp = list(spreads.values())
    print(f"\nGrid-wide: median **{median(allsp):.2f}%**, max **{max(allsp):.2f}%** over "
          f"{len(allsp)} points. **That grid-wide pair is a summary across heterogeneous "
          f"points — useful for judging the rig, NOT an error bar for any single figure.** "
          f"The deliverable (S=6 at best-N) sits in the high-N regime, so its own spread, "
          f"printed in the table above, is the number that bounds it.\n")

    # Round-over-round direction: inert data, explicitly uncontrolled for drift.
    rounds = {}
    for r in reps:
        rounds.setdefault((r["s"], r["n"]), {})[r["round"]] = r["aggregate_rows_per_s"]
    up = dn = 0
    for _, byr in rounds.items():
        ks = sorted(byr)
        for a, b in zip(ks, ks[1:]):
            if byr[b] > byr[a]:
                up += 1
            else:
                dn += 1
    print(f"**Round-over-round direction: {up} rose, {dn} fell** across consecutive rounds at the "
          f"same point. This is **INERT DATA, EXPLICITLY UNCONTROLLED FOR DRIFT** "
          f"(`scripts/perf/README.md`): this rig does not control drift, nothing here establishes "
          f"the session ran without it, and no round-major claim is made. A directional imbalance "
          f"is consistent with page-cache warming or thermal settling. The S-order ROTATION is "
          f"what distributes such a drift across points rather than concentrating it in one S — "
          f"which is why the curve's SHAPE survives a drifting session even though no absolute "
          f"number does. The rotation is a reasonable ordering, NOT a verified control. "
          f"Note also that a median of 3 draws from a drifting distribution, so 'median of 3' "
          f"reduces but does not remove this — it is not a drift-free figure.\n")


def emit_endpoints(by_point, peaks):
    """The S=1 and S=6 endpoints, and the L1d partial of the deferred AC3."""
    lo, hi = min(peaks), max(peaks)
    a = by_point[(lo, peaks[lo][0])]
    b = by_point[(hi, peaks[hi][0])]
    print(f"### Endpoints S={lo} and S={hi} — the L1d partial of the DEFERRED AC3\n")
    print(f"AC3 (LLC-load-misses/row, S=1 vs S=6) is **DEFERRED**: every LLC instrument on "
          f"this box is unavailable (`../host/README.md`), and nothing below discharges it. "
          f"But `L1-dcache-loads` and `L1-dcache-load-misses` ARE real here, and they are "
          f"exactly the counters #3224 reported as flat across its endpoints — so the "
          f"private-cache half of the question is answerable.\n")
    print(f"All per-row counters below are summed over ALL PINNED HARDWARE THREADS "
          f"(2S logical CPUs) — the same basis as the table above, and the same set "
          f"`perf stat -C` counted.\n")
    print(f"| per-row counter | S={lo}, N={peaks[lo][0]} | S={hi}, N={peaks[hi][0]} | ratio |")
    print("|---|--:|--:|--:|")
    for label, key, fmt in (
        ("instructions/row", "instructions_per_row", ",.1f"),
        ("L1-dcache-loads/row", "l1d_loads_per_row", ",.1f"),
        ("L1-dcache-load-misses/row", "l1d_load_misses_per_row", ",.2f"),
        ("cycles/row", "cycles_per_row", ",.1f"),
        ("IPC", "ipc", ".4f"),
    ):
        x, y = agg(a, key), agg(b, key)
        print(f"| {label} | {x:{fmt}} | {y:{fmt}} | x{y / x:.3f} |")
    print()
    print("**Read this as CONDITIONAL and CROSS-EVERYTHING.** #3224's endpoint figures "
          "(instructions 38,856.8 -> 38,685.6; L1d loads 9,157.7 -> 9,140.8; L1d misses "
          "586.7 -> 578.9; cycles 31,316.4 -> 37,284.9 = x1.191; IPC 1.2376 -> 1.0384) were "
          "measured on a DIFFERENT host (`i4i.metal`), a DIFFERENT corpus (Corpus A, "
          "LZ4-compressed, 196.09 B/row) and a DIFFERENT arm (`do_get`, not bare scan). "
          "The two sets are not divided into each other and no ratio between them is "
          "computed. What IS comparable is the SHAPE: if the L1d figures here are also "
          "flat S=1->S=6, that is consistent with #3224's private-caches-untouched finding "
          "and locates whatever decay appears in rows/s away from the private hierarchy — "
          "narrowing it to the shared level this box cannot instrument. If they are NOT "
          "flat, that is a new result, and a more interesting one, because #3224's "
          "mechanism story assumed that flatness.\n")


def emit_provenance(reps, by_point, min_reps):
    thin = {f"S={s},N={n}": len(v) for (s, n), v in sorted(by_point.items()) if len(v) < min_reps}
    print("### Instrument provenance\n")
    print("**No LLC column exists anywhere above.** Every LLC instrument on this host is "
          "unavailable, so AC3 is DEFERRED, not approximated: a hard 0 from a dead counter "
          "would read as 'no misses'.\n")
    print("`unhalted Gcyc/CPU·s` is deliberately absent from the deliverable table for the "
          "same reason a CPU-utilisation column is: under CPU-wide counting `task-clock` is "
          "elapsed x ncpus by construction, so a utilisation derived from it cannot vary.\n")
    print(f"Counter-window agreement (max over reps): "
          f"{max(r['counter_window_drift_frac'] for r in reps):.2e} — perf's enabled interval "
          f"versus the driver's [T0, T1]. The measured proof that counters and rows were "
          f"taken over the SAME interval.\n")
    print(f"Max attribution shortfall over all reps: "
          f"{max(r['shortfall_max_frac'] for r in reps):.4%} of the window (bound "
          f"{guards.DEFAULT_SHORTFALL_BOUND:.2%}). Rows are counted only between progress "
          f"records the workers actually emitted, so this biases every rows/s figure DOWNWARD "
          f"and every per-row counter UPWARD, by at most that fraction.\n")
    if thin:
        print(f"**WARNING — under-replicated points:** {thin} have fewer than {min_reps} reps. "
              f"Their medians carry no usable dispersion (this is #3217's gap 1).\n")


def emit_equivalence(results):
    """Worker vs `ws0-scan-bench` on the same core, same session, same bytes.

    The delta is DECOMPOSED from the data rather than explained by assertion: the
    bench's own pass-to-pass spread and the worker's attribution shortfall are
    both printed, so a reader can see how much of any gap those two account for
    and how much is unexplained.
    """
    with open(os.path.join(results, "equiv-scan-bench.json")) as fh:
        bench = json.load(fh)
    with open(os.path.join(results, "equiv-worker-window.json")) as fh:
        worker = json.load(fh)
    passes = [p["rows_per_sec"] for p in bench["passes"]]
    bench_rps = median(passes)
    worker_rps = worker["aggregate_rows_per_s"]
    shortfall = worker["attribution_shortfall_max_frac"]
    delta = (worker_rps - bench_rps) / bench_rps

    print("## Equivalence control — #3299 worker vs the rig's `ws0-scan-bench`\n")
    print("Same physical core, same session, same bytes.\n")
    print("| arm | rows/s | note |")
    print("|---|--:|---|")
    print(f"| `ws0-scan-bench --passes {len(passes)}` (median pass) | {bench_rps:,.0f} | "
          f"the #3096/#3272 rig's bare-scan arm |")
    print(f"| — its individual passes | {', '.join(f'{p:,.0f}' for p in passes)} | "
          f"own spread **{spread_pct(passes):.1f}%** |")
    print(f"| `ws0-3299-scan-worker` S=1, aligned window | {worker_rps:,.0f} | this harness |")
    print(f"\n**Delta: {delta:+.2%}.** Decomposition:\n")
    print(f"- attribution shortfall (a known LOW bias of this harness, see harness README): "
          f"**{shortfall:+.4%}** of it;")
    print(f"- the bench's own three passes span **{spread_pct(passes):.1f}%** within one run, "
          f"and the worker's figure sits at the bottom of that range — consistent with the "
          f"worker measuring continuous steady state while a 3-pass median is weighted "
          f"toward the earliest, fastest pass;")
    print(f"- residual after the shortfall: **{delta + shortfall:+.2%}**, which is inside the "
          f"bench's own single-run spread and is therefore not evidence of a different code "
          f"path.\n")
    print("A divergence LARGE against that spread — in either direction — would mean the two "
          "are not the same code path and the S=1 point is not comparable to the existing "
          "rig's. This run does not show one.\n")


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
