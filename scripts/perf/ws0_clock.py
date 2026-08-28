#!/usr/bin/env python3
"""Occupancy-enforced clock derivation for the WS0 measurement rig (issue #3248).

WHY THIS FILE EXISTS
--------------------
AC4 of issue #3248 asks for a reconciliation "stating the clock basis". Stating it is
NOT ENOUGH, and that is not a stylistic opinion — it is the measured history:

  * #3299 published `cycles / task-clock` as a frequency. Under CPU-wide `perf stat -C`
    that quotient is **occupancy x frequency**, because `task-clock` accrues
    elapsed x nCPUs INCLUDING IDLE CPUs. It read `1.271 "GHz"` at S=4/N=1 — one busy
    core diluted across eight pinned logical CPUs — and was retracted.
  * The retraction had OVERRIDDEN a caption written specifically to prevent it.
  * Hours later the SAME quantity was reached for again, licensed by "matched occupancy
    80%/80%, and that WAS measured". The 0.80 was the counting window over perf's own
    process lifetime (20s/25s) — matched by HARNESS PARAMETERS, not by anything about
    the hardware. The coincidence is what made it look plausible.

So a caption did not survive contact with someone reaching for the quantity again,
including its own author's lead. A CHECK is required, and it must REFUSE rather than warn.

WHAT THIS TOOL GUARANTEES
-------------------------
It prints a frequency ONLY when occupancy was independently MEASURED and the independent
measures AGREE. Otherwise it refuses with a named cause and a non-zero exit. There is
deliberately NO environment variable and NO flag that relaxes this: an escape hatch on a
measurement guard can only ever buy a confident wrong number.

The clock basis it uses is the only one that is occupancy-free by construction:

    frequency = (aperf / mperf) x TSC_frequency

`aperf`/`mperf` are ratio counters that both stop in idle, so their quotient is the
average frequency WHILE RUNNING, independent of how much of the window was idle. TSC is
measured from the run itself (`msr/tsc/` over its own enabled time), never assumed from a
model name or a config constant.

TWO SUBTLETIES THAT ARE EASY TO GET WRONG, AND BOTH BITE
--------------------------------------------------------
1. **Normalize every event by its OWN `enabled_ns`, not by the run's elapsed time.**
   perf's own derived "G/sec" column uses the run elapsed for every event. Measured on
   this box: doing it perf's way made `msr/tsc/` read 2.474 GHz; normalizing by the
   event's own enabled time recovers **2.4000 GHz exactly** — the known nominal. That
   agreement with an independently-known constant is why the per-event denominator is the
   right one, and a 3% error in TSC propagates into every derived quantity.
2. **Multiplexing is fatal, not a caveat.** If the PMU multiplexes, perf SCALES the counts
   and the result is an estimate. A scaled `cycles` silently corrupts any figure derived
   from it, so a below-threshold enabled-percentage is refused.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Dict, Optional

# perf's own markers for "this is not a value". Copied deliberately rather than imported:
# this tool must be readable and runnable standalone.
PERF_NOT_A_VALUE = ("<not counted>", "<not supported>", "<unsupported>", "")

REQUIRED_EVENTS = ("msr/aperf/", "msr/mperf/", "msr/tsc/")
# Corroborating occupancy signal. Optional to PARSE, but see `occupancy_sources`: at least
# TWO independent occupancy measures are required to emit a clock, so in practice a run
# without `ref-cycles` must supply the second source some other way or be refused.
CORROBORATING_EVENTS = ("ref-cycles", "cycles", "task-clock")

DEFAULT_OCCUPANCY_TOLERANCE = 0.02
DEFAULT_MIN_ENABLED_PCT = 99.9


class Refusal(Exception):
    """A named, fail-closed refusal. The `cause` is a stable token for tests to assert."""

    def __init__(self, cause: str, detail: str) -> None:
        super().__init__(f"{cause}: {detail}")
        self.cause = cause
        self.detail = detail


def _exact_int(label: str, raw: str) -> int:
    """Parse a perf count with no coercion and no fabricated default.

    A counter that was not observed is an ERROR, never a `0` — a fabricated zero is how a
    run gets reported "setup-subtracted" having subtracted nothing.
    """
    text = raw.strip()
    if text in PERF_NOT_A_VALUE:
        raise Refusal(
            "PERF_COUNTER_NOT_OBSERVED",
            f"event {label!r} carries perf's non-value marker {text!r} — "
            "the counter did not run, so there is no measurement to report",
        )
    try:
        # Reject floats outright: a fractional perf count means the value was SCALED by
        # multiplexing, which is exactly what this tool refuses.
        if "." in text or "e" in text.lower():
            raise ValueError(text)
        return int(text)
    except ValueError as exc:
        raise Refusal(
            "PERF_COUNTER_UNPARSEABLE",
            f"event {label!r} value {text!r} is not an exact integer",
        ) from exc


def parse_perf_csv(text: str, *, min_enabled_pct: float) -> Dict[str, Dict[str, float]]:
    """Parse `perf stat -x,` output into {event: {count, enabled_ns, enabled_pct, rate_hz}}.

    Field order for `-x,` is:
        count, unit, event, enabled_ns, enabled_pct, [derived, derived_unit]
    Lines that are not counter rows (comments, the leading program output) are skipped.
    """
    events: Dict[str, Dict[str, float]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(",")
        if len(parts) < 5:
            continue
        raw_count, _unit, name, raw_enabled, raw_pct = (p.strip() for p in parts[:5])
        if not name:
            continue

        count = _exact_int(name, raw_count)
        enabled_ns = _exact_int(f"{name} (enabled_ns)", raw_enabled)
        try:
            enabled_pct = float(raw_pct)
        except ValueError as exc:
            raise Refusal(
                "PERF_ENABLED_PCT_UNPARSEABLE",
                f"event {name!r} enabled-percentage {raw_pct!r} is not a number",
            ) from exc

        if enabled_ns <= 0:
            raise Refusal(
                "PERF_COUNTER_NEVER_ENABLED",
                f"event {name!r} was enabled for {enabled_ns} ns — it never ran",
            )
        if enabled_pct < min_enabled_pct:
            raise Refusal(
                "PERF_MULTIPLEXED",
                f"event {name!r} ran for only {enabled_pct}% of the window "
                f"(< {min_enabled_pct}%), so perf SCALED its count. A scaled count is an "
                "estimate, and every quantity derived from it inherits that error",
            )

        events[name] = {
            "count": float(count),
            "enabled_ns": float(enabled_ns),
            "enabled_pct": enabled_pct,
            # Each event normalized by ITS OWN enabled time — see module docstring (2).
            "rate_hz": count / (enabled_ns / 1e9),
        }
    if not events:
        raise Refusal("PERF_CSV_EMPTY", "no parseable counter rows found in the input")
    return events


def derive(
    events: Dict[str, Dict[str, float]],
    *,
    occupancy_tolerance: float,
    window_over_lifetime: Optional[float] = None,
) -> Dict[str, object]:
    """Derive an occupancy-verified frequency, or refuse with a named cause."""
    missing = [e for e in REQUIRED_EVENTS if e not in events]
    if missing:
        raise Refusal(
            "FREQ_EVENTS_ABSENT",
            "cannot derive an occupancy-free clock without "
            f"{', '.join(REQUIRED_EVENTS)}; missing: {', '.join(missing)}",
        )

    aperf = events["msr/aperf/"]["rate_hz"]
    mperf = events["msr/mperf/"]["rate_hz"]
    tsc = events["msr/tsc/"]["rate_hz"]
    if mperf <= 0 or tsc <= 0:
        raise Refusal(
            "FREQ_DIVISOR_ZERO",
            f"mperf rate {mperf} / tsc rate {tsc} — cannot form the ratio",
        )

    # --- occupancy, measured from at least two INDEPENDENT sources ------------------
    # Keyed on the AFFIRMATIVE presence of each source. Never inferred from the absence
    # of a bad signal.
    occupancy_sources: Dict[str, float] = {
        # mperf counts only while the CPU is in C0; TSC counts unconditionally.
        "c0_fraction_mperf_over_tsc": mperf / tsc,
    }
    if "ref-cycles" in events:
        # ref-cycles counts unhalted cycles at the invariant reference rate, so its
        # ratio to TSC is a second, mechanically independent occupancy.
        occupancy_sources["unhalted_fraction_refcycles_over_tsc"] = (
            events["ref-cycles"]["rate_hz"] / tsc
        )

    if len(occupancy_sources) < 2:
        raise Refusal(
            "FREQ_OCCUPANCY_ABSENT",
            "only one occupancy source is available "
            f"({', '.join(sorted(occupancy_sources))}); this tool requires TWO "
            "independent measures that agree, because a single source cannot detect its "
            "own failure. Add `ref-cycles` to the event set",
        )

    lo, hi = min(occupancy_sources.values()), max(occupancy_sources.values())
    spread = hi - lo
    if spread > occupancy_tolerance:
        raise Refusal(
            "FREQ_OCCUPANCY_MISMATCH",
            f"independent occupancy measures disagree by {spread:.4f} "
            f"(> {occupancy_tolerance}): "
            + ", ".join(f"{k}={v:.4f}" for k, v in sorted(occupancy_sources.items()))
            + ". They measure the same physical quantity, so a disagreement means at "
            "least one is wrong and there is no way to tell which",
        )

    tsc_ghz = tsc / 1e9
    frequency_ghz = (aperf / mperf) * tsc_ghz

    out: Dict[str, object] = {
        "verdict": "OK",
        "frequency_ghz": frequency_ghz,
        "frequency_basis": "(aperf/mperf) x measured TSC — occupancy-free by construction",
        "tsc_ghz_measured": tsc_ghz,
        "occupancy": {
            "sources": occupancy_sources,
            "agreed_within": occupancy_tolerance,
            "observed_spread": spread,
            "value": sum(occupancy_sources.values()) / len(occupancy_sources),
        },
        "events": {k: dict(v) for k, v in sorted(events.items())},
    }

    # --- the trap value, recorded so it can never be mistaken for a clock -----------
    if "cycles" in events and "task-clock" in events:
        # `task-clock`'s COUNT is accrued CPU-nanoseconds. The trap quantity is
        # cycles / task-clock-in-seconds, i.e. counts and not rates — computing it from
        # rates cancels the very dilution that makes it wrong, and would silently print
        # something close to the true clock. Verified against perf's own derived column:
        # 4,623,007,899 cycles / 1.404409318 s = 3.293 GHz, and perf printed 3.292 GHz.
        task_clock_seconds = events["task-clock"]["count"] / 1e9
        if task_clock_seconds > 0:
            out["occupancy_times_frequency_NOT_A_CLOCK"] = {
                "value_ghz_LOOKS_LIKE": (
                    events["cycles"]["count"] / task_clock_seconds / 1e9
                ),
                "WARNING": (
                    "cycles/task-clock is occupancy x frequency, NOT a frequency. Under "
                    "CPU-wide `perf stat -C` task-clock accrues elapsed x nCPUs including "
                    "IDLE CPUs. Recorded here only so a reader can see how far it is from "
                    "the real clock. #3299 published this as a clock, retracted it, then "
                    "made the same error again. Do not use it."
                ),
            }

    if window_over_lifetime is not None:
        out["window_over_lifetime_NOT_AN_OCCUPANCY"] = {
            "value": window_over_lifetime,
            "WARNING": (
                "This is the counting window divided by the profiler's process lifetime. "
                "It is a HARNESS PARAMETER and says nothing about hardware occupancy. It "
                "is recorded under this name because it was once mistaken for an "
                "occupancy and used to license a comparison (#3299). It is NOT one of the "
                "occupancy sources above and takes no part in the agreement check."
            ),
        }
    return out


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Derive an occupancy-verified clock from `perf stat -x,` output.",
        epilog="There is deliberately no flag that relaxes the occupancy check.",
    )
    ap.add_argument("csv", help="path to `perf stat -x,` output, or '-' for stdin")
    ap.add_argument("--label", default="", help="label recorded in the output")
    ap.add_argument(
        "--occupancy-tolerance",
        type=float,
        default=DEFAULT_OCCUPANCY_TOLERANCE,
        help=f"max spread between independent occupancy measures (default {DEFAULT_OCCUPANCY_TOLERANCE}). "
        "May be TIGHTENED; a value above the default is refused.",
    )
    ap.add_argument(
        "--window-over-lifetime",
        type=float,
        default=None,
        help="optional harness ratio, recorded as explicitly NOT an occupancy",
    )
    ap.add_argument("--json-out", default=None, help="write the JSON record here")
    args = ap.parse_args(argv)

    if args.occupancy_tolerance > DEFAULT_OCCUPANCY_TOLERANCE:
        # The knob exists to make the check STRICTER, never weaker. A looser tolerance is
        # the escape hatch this tool exists to not have.
        print(
            f"ws0_clock: REFUSED: OCCUPANCY_TOLERANCE_LOOSENED: {args.occupancy_tolerance} "
            f"exceeds the maximum {DEFAULT_OCCUPANCY_TOLERANCE}; this knob may only tighten",
            file=sys.stderr,
        )
        return 2

    text = sys.stdin.read() if args.csv == "-" else open(args.csv, encoding="utf-8").read()

    try:
        events = parse_perf_csv(text, min_enabled_pct=DEFAULT_MIN_ENABLED_PCT)
        record = derive(
            events,
            occupancy_tolerance=args.occupancy_tolerance,
            window_over_lifetime=args.window_over_lifetime,
        )
    except Refusal as r:
        print(f"ws0_clock: REFUSED: {r.cause}: {r.detail}", file=sys.stderr)
        return 1

    record["label"] = args.label
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2, sort_keys=True)
            fh.write("\n")

    occ = record["occupancy"]
    print(f"ws0_clock: verdict: {record['verdict']}")
    print(f"  frequency:      {record['frequency_ghz']:.4f} GHz")
    print(f"  basis:          {record['frequency_basis']}")
    print(f"  TSC (measured): {record['tsc_ghz_measured']:.4f} GHz")
    print(f"  occupancy:      {occ['value']:.4f}  (spread {occ['observed_spread']:.4f}"
          f" <= {occ['agreed_within']}, {len(occ['sources'])} independent sources)")
    for k, v in sorted(occ["sources"].items()):
        print(f"    {k} = {v:.4f}")
    if "occupancy_times_frequency_NOT_A_CLOCK" in record:
        trap = record["occupancy_times_frequency_NOT_A_CLOCK"]
        print(f"  cycles/task-clock would have read {trap['value_ghz_LOOKS_LIKE']:.4f} "
              "-- NOT A CLOCK, see the record")
    return 0


if __name__ == "__main__":
    sys.exit(main())
