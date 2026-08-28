#!/usr/bin/env python3
"""f(S) from aperf/mperf, and the turbo-vs-residual split it licenses.

frequency(S) = TSC_base x aperf/mperf, measured under FULL occupancy of the
pinned set (N = 2S) so it is the clock the grid's points actually ran at.

**The residual stays UNATTRIBUTED.** There is no LLC counter on this box, so
nothing here can say the residual is cache contention — AC3's deferral binds this
section too. The output is a BOUND on what a footprint lever (#3288) could
recover, never an attribution.
"""

import argparse
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
import derive  # noqa: E402  — one rundir resolver, shared
import guards  # noqa: E402  — one guard implementation, shared

# `lscpu` reports BogoMIPS 4800.00 on this host => a 2.40 GHz TSC base, which is
# the Xeon Platinum 8488C's nominal. aperf/mperf is a RATIO to that base.
TSC_BASE_GHZ = 2.40


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True)
    ap.add_argument("--tsc-base-ghz", type=float, default=TSC_BASE_GHZ)
    a = ap.parse_args()

    by_s = {}
    manifest = os.path.join(a.results, "manifest.jsonl")
    if not os.path.exists(manifest):
        guards.fail(
            "FREQ_MANIFEST_MISSING",
            f"{manifest} is absent. This tool reads its rundirs from the manifest, exactly as "
            f"derive.py does; without one there is no recorded set of frequency reps to read, "
            f"and guessing at directory names would be a different tool measuring a different "
            f"thing.",
        )
    reps = 0
    for line in open(manifest):
        if not line.strip():
            continue
        rec = json.loads(line)
        # RESOLVED AGAINST THE MANIFEST'S OWN DIRECTORY, not the current one.
        # Manifests record rundirs RELATIVE to the results root (sweep.sh writes
        # `basename`), so resolving against `os.getcwd()` made this tool unable to
        # read its OWN committed evidence from anywhere but that one directory.
        # `derive.resolve_rundir` is the one implementation of this rule.
        rundir = derive.resolve_rundir(a.results, rec["rundir"])
        reps += 1
        c = guards.parse_perf_csv(os.path.join(rundir, "perf.csv"))
        for ev in ("msr/aperf/", "msr/mperf/"):
            if ev not in c:
                guards.fail("PERF_EVENT_ABSENT", f"{rundir}: {ev} absent")
            val, pct = c[ev]
            if "<not" in val:
                guards.fail("PERF_EVENT_NOT_COUNTED", f"{rundir}: {ev} reads {val!r}")
            if float(pct) < 100.0:
                guards.fail("PERF_MULTIPLEXED", f"{rundir}: {ev} at {pct}%")
        aperf, mperf = float(c["msr/aperf/"][0]), float(c["msr/mperf/"][0])
        if mperf <= 0 or aperf <= 0:
            guards.fail(
                "PERF_EVENT_ZERO",
                f"{rundir}: aperf={aperf} mperf={mperf}. A zero here is an UNAVAILABLE "
                f"INSTRUMENT, not a measured zero — drop the decomposition, do not approximate it.",
            )
        by_s.setdefault(rec["s"], []).append(aperf / mperf * a.tsc_base_ghz)

    if not reps:
        guards.fail(
            "FREQ_MANIFEST_EMPTY",
            f"{manifest} records no reps. A table computed from nothing prints headers and no "
            f"rows, which reads as a successful run that measured nothing.",
        )

    print("## Measured core frequency f(S), from `msr/aperf` ÷ `msr/mperf`\n")
    print(f"Under FULL occupancy of the pinned set (N = 2S). TSC base "
          f"{a.tsc_base_ghz:.2f} GHz.\n")
    print("| S | f(S) GHz (median) | spread | reps |")
    print("|--:|--:|--:|--:|")
    med = {}
    for s in sorted(by_s):
        v = by_s[s]
        m = statistics.median(v)
        med[s] = m
        sp = 0.0 if m == 0 else (max(v) - min(v)) / m * 100
        print(f"| {s} | {m:.3f} | {sp:.2f}% | {len(v)} |")

    if 1 in med and 6 in med:
        ratio = med[6] / med[1]
        # Marginal efficiency at S=6 vs S=1's peak, from the main grid.
        me = 0.935
        loss = 1.0 - me
        clock_part = 1.0 - ratio
        print(f"\n### Turbo vs residual at S=6\n")
        print(f"- clock ratio **f(S=6)/f(S=1) = {ratio:.4f}** ⇒ the package clock alone "
              f"accounts for **{clock_part*100:.1f} pp** of loss.")
        print(f"- measured marginal-efficiency loss at S=6: **{loss*100:.1f} pp** "
              f"(efficiency {me:.3f}).")
        if loss > 0:
            print(f"- so the clock explains **{min(clock_part/loss,1.0)*100:.0f}%** of it; the "
                  f"**residual is {max(loss-clock_part,0.0)*100:.1f} pp**.")
        print(f"\n**The residual is UNATTRIBUTED.** With no LLC counter on this box nothing "
              f"here identifies its cause; AC3's deferral binds this section too. "
              f"`instructions/row` measured FLAT (×0.984) already establishes the residual is "
              f"not extra work, and the clock ratio now says how much of the `cycles/row` rise "
              f"(×1.041) is frequency rather than stalling. What remains is a **BOUND on what "
              f"#3288 could recover**, not a claim about what is consuming it.\n")
        print(f"Note the scale: `cycles/row` rises only 4.1% across the entire S=1→S=6 range, "
              f"so whatever the split, **the total available here is small**.\n")


if __name__ == "__main__":
    main()
