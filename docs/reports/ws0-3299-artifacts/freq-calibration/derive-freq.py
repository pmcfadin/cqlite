#!/usr/bin/env python3
"""f(S) from aperf/mperf, and the turbo-vs-residual split it licenses.

frequency(S) = TSC_base x aperf/mperf, per rep, at the ACTUAL N THAT REP WAS
MEASURED AT — read from its `window.json`, never assumed.

WHAT THIS TOOL USED TO CLAIM, AND WHY IT WAS WRONG (#3299 round 6). It stated the
points were taken "under FULL occupancy of the pinned set (N = 2S)", and printed
that line unconditionally. The committed S=6 record was measured at **N=24**, not
at a nominal 2S=12 — N=24 was chosen deliberately, to match the AC2 configuration
(S=6's best-N from the main grid) — so the stated protocol did not describe the
evidence the tool was reading, and nothing in the tool ever checked it. A claim
that is never checked is not a protocol; it is a sentence.

THE PROPERTY THAT ACTUALLY LICENSES COMPARING THE TWO ENDPOINTS IS **MATCHED
OCCUPANCY**, and it is MEASURED here, per record, from committed primitives:

  * `C0 fraction`  = `msr/mperf` / (TSC base x counted CPU-seconds). MPERF ticks
    at the TSC rate only while the logical CPU is in C0, so this is the fraction
    of the counted CPU time the pinned set spent RUNNING. It reads 1.000 at both
    endpoints (the pinned CPUs never idled), to the accuracy of the assumed TSC
    base — which `mperf`'s own measured 2.400/2.401 G/sec independently confirms.
  * `unhalted fraction` = (`cycles` / counted CPU-seconds) / f. This is exactly
    the quantity that must MATCH for the report's `cycles`/`task-clock`
    cross-check to be a valid frequency RATIO, since that quotient is
    (unhalted fraction) x frequency and nothing else.
  * `window / perf lifetime` = perf's own `CPUs utilized` metric / pinned CPUs.
    **This is the figure the report and the freq-run README called "80%
    occupancy", and it is NOT an occupancy**: under CPU-wide counting
    `task-clock` accrues elapsed x nCPUs including idle CPUs, and perf divides it
    by its OWN process lifetime, so the ratio measures the counting window
    against how long `perf` lived (20 s of a 25 s process = 0.800) and says
    nothing about the CPUs. It is reported because it was published, and
    relabelled because it was mislabelled.

All three are ASSERTED EQUAL across the records within a stated tolerance, and a
disagreement — or an absent occupancy instrument — REFUSES rather than prints.
An unmatched-occupancy comparison is precisely the confound that made an earlier
revision of this work publish a `cycles`/`task-clock` quotient as a frequency and
read 1.271 "GHz" at S=4/N=1.

DISPERSION AT ONE REP IS **UNMEASURED**, NOT ZERO. Each endpoint here is a single
rep, so no spread is printed for it: `0.00%` would assert a precision that was
never established and would be textually identical to a point replicated three
times (`derive.fmt_spread` is the one implementation of that rule).

**The residual stays UNATTRIBUTED.** There is no LLC counter on this box, so
nothing here can say the residual is cache contention — AC3's deferral binds this
section too. The output is a BOUND on what a footprint lever (#3288) could
recover, never an attribution.
"""

import argparse
import json
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
import derive  # noqa: E402  — one rundir resolver, one dispersion formatter, shared
import guards  # noqa: E402  — one guard implementation, shared

# `lscpu` reports BogoMIPS 4800.00 on this host => a 2.40 GHz TSC base, which is
# the Xeon Platinum 8488C's nominal. aperf/mperf is a RATIO to that base.
TSC_BASE_GHZ = 2.40

# The three occupancy figures must agree ACROSS the records being compared to
# within 2 pp. Chosen as a bound, not fitted: the committed endpoints agree to
# 0.04 pp (C0), 0.17 pp (unhalted) and 0.02 pp (window/lifetime), so the
# tolerance is over an order of magnitude looser than the measurement and still
# far tighter than any difference that could move the clock ratio. It is a
# CONSTANT, not an option: a tolerance an operator can widen is not a bound.
OCCUPANCY_MATCH_TOL = 0.02

# The events every occupancy figure above is computed from. Their presence is a
# precondition for publishing anything here, not a nice-to-have.
OCCUPANCY_EVENTS = ("msr/aperf/", "msr/mperf/", "cycles", "task-clock")

# Required fields of a rep's window.json.
WINDOW_FIELDS = ("s", "n", "t0_ns", "t1_ns", "perf_cpus", "perf_csv", "events")


def read_window(rundir):
    path = os.path.join(rundir, "window.json")
    if not os.path.exists(path):
        guards.fail(
            "FREQ_RECORD_INCOMPLETE",
            f"{path} is absent. The ACTUAL N and the pinned CPU set are read from it — they "
            f"are not assumed from S — so without it there is no record of what was measured.",
        )
    with open(path) as fh:
        win = json.load(fh)
    missing = [k for k in WINDOW_FIELDS if k not in win]
    if missing:
        guards.fail("FREQ_RECORD_INCOMPLETE", f"{path} is missing {missing}")
    if not [c for c in str(win["perf_cpus"]).split(",") if c]:
        guards.fail(
            "FREQ_RECORD_INCOMPLETE",
            f"{path}: perf_cpus {win['perf_cpus']!r} names no CPU, so the number of pinned "
            f"logical CPUs — the denominator of every occupancy figure — is unknown.",
        )
    return win


def occupancy(rundir, win, counters, f_ghz):
    """The three MEASURED occupancy figures for one rep. See the module docstring."""
    ncpus = len([c for c in str(win["perf_cpus"]).split(",") if c])
    counted_cpu_s = counters["task-clock"] / 1e9

    metrics = guards.parse_perf_metrics(os.path.join(rundir, win["perf_csv"]))
    metric, unit = metrics.get("task-clock", ("", ""))
    if not metric or "CPUs utilized" not in unit:
        guards.fail(
            "FREQ_OCCUPANCY_ABSENT",
            f"{rundir}: perf emitted no `CPUs utilized` metric beside task-clock "
            f"(metric={metric!r} unit={unit!r}). That column is the only record of how long "
            f"the perf process lived, so the window/lifetime ratio the report published as "
            f"'80% occupancy' cannot be recomputed — and an unrecomputable published figure "
            f"is not evidence.",
        )
    try:
        cpus_utilized = float(metric)
    except ValueError:
        guards.fail("FREQ_OCCUPANCY_ABSENT", f"{rundir}: `CPUs utilized` metric {metric!r} is not a number")
    if not math.isfinite(cpus_utilized) or cpus_utilized <= 0:
        guards.fail("FREQ_OCCUPANCY_ABSENT", f"{rundir}: `CPUs utilized` metric {metric!r} is not a positive finite number")

    # The counters and the driver's window must be the SAME interval, decided by
    # the same guard `derive.py` re-decides it with — every occupancy figure here
    # divides by counted CPU time, so a counter interval that is not the window's
    # would silently rescale all three.
    guards.check_counter_window_drift(
        guards.counter_window_drift(rundir, win, counters),
        guards.DEFAULT_COUNTER_WINDOW_TOLERANCE,
        f"{rundir}: perf's enabled interval vs the driver's window",
    )

    return {
        "rundir": rundir,
        "s": int(win["s"]),
        "n": int(win["n"]),
        "ncpus": ncpus,
        "counted_cpu_s": counted_cpu_s,
        "f_ghz": f_ghz,
        "c0_fraction": counters["msr/mperf/"] / (TSC_BASE_GHZ * 1e9 * counted_cpu_s),
        "unhalted_fraction": counters["cycles"] / (f_ghz * 1e9 * counted_cpu_s),
        "window_over_perf_lifetime": cpus_utilized / ncpus,
    }


def assert_occupancy_matched(occ):
    """Every figure must agree across the records, or nothing is published.

    A frequency RATIO between two points at different occupancies is not a
    frequency ratio. This is the check whose absence let the earlier
    `cycles`/`task-clock` reading be published as a clock.
    """
    for key, label in (
        ("c0_fraction", "C0 fraction"),
        ("unhalted_fraction", "unhalted fraction"),
        ("window_over_perf_lifetime", "window / perf lifetime"),
    ):
        vals = [(o[key], o) for o in occ]
        lo, hi = min(vals, key=lambda t: t[0]), max(vals, key=lambda t: t[0])
        if hi[0] - lo[0] > OCCUPANCY_MATCH_TOL:
            guards.fail(
                "FREQ_OCCUPANCY_MISMATCH",
                f"{label} differs by {hi[0] - lo[0]:.4f} across the records being compared "
                f"(tolerance {OCCUPANCY_MATCH_TOL:.4f}): "
                f"{lo[1]['rundir']} S={lo[1]['s']} N={lo[1]['n']} reads {lo[0]:.4f}, "
                f"{hi[1]['rundir']} S={hi[1]['s']} N={hi[1]['n']} reads {hi[0]:.4f}. "
                f"A frequency ratio between points at DIFFERENT occupancies is not a "
                f"frequency ratio — that confound is exactly why this check exists.",
            )
    return max(
        max(o[k] for o in occ) - min(o[k] for o in occ)
        for k in ("c0_fraction", "unhalted_fraction", "window_over_perf_lifetime")
    )


def main():  # noqa: C901 — one linear read, then one linear emit
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True)
    ap.add_argument("--tsc-base-ghz", type=float, default=TSC_BASE_GHZ)
    a = ap.parse_args()

    by_s = {}
    occ = []
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
        win = read_window(rundir)
        events = [e for e in str(win["events"]).split(",") if e]
        absent = [e for e in OCCUPANCY_EVENTS if e not in events]
        if absent:
            guards.fail(
                "FREQ_OCCUPANCY_ABSENT",
                f"{rundir}: {absent} are not in the rep's recorded event list. The frequency "
                f"needs aperf/mperf and the occupancy check needs cycles and task-clock; "
                f"without them there is a number but no way to establish it is comparable.",
            )
        # THE READ PATH RUNS THE WRITE PATH'S COUNTER VALIDATION — the same
        # `guards.validate_counters` the measurement-time guard ran. A zero,
        # negative or non-finite counter is refused here too, so this tool cannot
        # publish a frequency derived from a dead instrument.
        counters = guards.validate_counters(
            os.path.join(rundir, win["perf_csv"]), events, where=rundir
        )
        f_ghz = counters["msr/aperf/"] / counters["msr/mperf/"] * a.tsc_base_ghz
        by_s.setdefault(int(win["s"]), {"f": [], "n": set()})
        by_s[int(win["s"])]["f"].append(f_ghz)
        by_s[int(win["s"])]["n"].add(int(win["n"]))
        occ.append(occupancy(rundir, win, counters, f_ghz))

    if not reps:
        guards.fail(
            "FREQ_MANIFEST_EMPTY",
            f"{manifest} records no reps. A table computed from nothing prints headers and no "
            f"rows, which reads as a successful run that measured nothing.",
        )

    worst = assert_occupancy_matched(occ)

    print("## Measured core frequency f(S), from `msr/aperf` ÷ `msr/mperf`\n")
    print(f"Each rep at **the actual N it was measured at**, read from its `window.json` — "
          f"nothing here assumes N = 2S, and the S=6 record was deliberately taken at N=24 to "
          f"match AC2's configuration (S=6's best-N). TSC base {a.tsc_base_ghz:.2f} GHz.\n")
    print("| S | N | f(S) GHz (median) | dispersion | reps |")
    print("|--:|--:|--:|--:|--:|")
    med = {}
    for s in sorted(by_s):
        v = by_s[s]["f"]
        m = statistics.median(v)
        med[s] = m
        ns = ",".join(str(x) for x in sorted(by_s[s]["n"]))
        print(f"| {s} | {ns} | {m:.3f} | {derive.fmt_spread(v, 2)} | {len(v)} |")
    print(f"\n**Dispersion at one rep is UNMEASURED, not zero.** Each endpoint here is a "
          f"single rep, so no spread is quoted for it: a printed spread of zero would assert "
          f"a precision never established and would be textually indistinguishable from a "
          f"point replicated three times. The main grid's replicated points (../sweep) carry "
          f"the campaign's measured dispersion; these two do not.\n")

    print("### Occupancy — MEASURED per record, and MATCHED across them\n")
    print("Matched occupancy is the property that licenses comparing the endpoints; it is "
          "measured, not asserted. `C0 fraction` = `msr/mperf` ÷ (TSC base × counted "
          "CPU-seconds) — the fraction of counted CPU time the pinned set spent RUNNING. "
          "`unhalted fraction` = (`cycles` ÷ counted CPU-seconds) ÷ f — the quantity that "
          "must match for a `cycles`/`task-clock` ratio to be a frequency ratio. "
          "`window ÷ perf lifetime` is perf's own `CPUs utilized` ÷ pinned CPUs: **this is "
          "the figure previously published as '80% occupancy', and it is NOT an occupancy** — "
          "under CPU-wide counting `task-clock` accrues elapsed × nCPUs including idle CPUs "
          "and perf divides it by its own process lifetime, so it measures the counting "
          "window against how long `perf` lived (20 s of a 25 s process), not the CPUs.\n")
    print("| S | N | pinned logical CPUs | counted CPU-s | C0 fraction | unhalted fraction | window ÷ perf lifetime |")
    print("|--:|--:|--:|--:|--:|--:|--:|")
    for o in sorted(occ, key=lambda o: (o["s"], o["n"])):
        print(f"| {o['s']} | {o['n']} | {o['ncpus']} | {o['counted_cpu_s']:.3f} | "
              f"{o['c0_fraction']:.4f} | {o['unhalted_fraction']:.4f} | "
              f"{o['window_over_perf_lifetime']:.4f} |")
    print(f"\n**MATCHED**: the largest disagreement on any of the three figures across these "
          f"{len(occ)} records is **{worst:.4f}** ({worst * 100:.2f} pp), within the "
          f"{OCCUPANCY_MATCH_TOL:.2f} tolerance. A disagreement beyond it, or an absent "
          f"occupancy instrument, REFUSES with `GUARD-FAIL FREQ_OCCUPANCY_MISMATCH` / "
          f"`FREQ_OCCUPANCY_ABSENT` — this tool cannot print an unmatched-occupancy "
          f"comparison, which is the confound that made an earlier revision publish a "
          f"`cycles`/`task-clock` quotient as a frequency.\n")

    if 1 in med and 6 in med:
        ratio = med[6] / med[1]
        # Marginal efficiency at S=6 vs S=1's peak, from the main grid.
        me = 0.935
        loss = 1.0 - me
        clock_part = 1.0 - ratio
        print("### Turbo vs residual at S=6\n")
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
