#!/usr/bin/env python3
"""Aggregate an interleaved A/B/C(/C0) session set into the report tables #3551 needs.

WHAT THIS IS FOR. `ws0-baseline.sh` measures ONE configuration per invocation, so an A/B/C
comparison is a SET of its sessions. This reads those sessions and reports what method §3b
step 4 requires and no single session can produce: the PAIRED per-round deltas and the
within-round direction count, beside every median's own spread.

WHAT IT REFUSES TO DO. It never blends temperatures, never reports a CPU-share (#2877), never
compares across a round an arm is missing from, and never prints a delta without the control's
own movement beside it -- an arm delta is only readable against a control that did not move.

THE CONTROL. Every arm pins the BARE-SCAN leg to the same `--server-cpus` and varies only the
Flight leg, so the bare scan is code-identical AND pin-identical across arms. Its movement
across arms is therefore drift plus contamination and nothing else, which is exactly the
control §3b step 3 asks for and the committed rig does not provide. If the control moves as
much as the treatment, there is NO READABLE RESULT and this tool says so rather than
publishing the treatment's delta.

THE CONFIGURATION IS VALIDATED OVER EVERY (round, arm), NOT READ FROM ONE ROUND. Measurements
are aggregated from every pairable round, so reading the configuration from the FIRST round
only meant a later round could carry a different pin, allocator, arena cap, counter mode or
admission ceiling and produce a delta ACROSS TREATMENTS while the printed configuration table
described just one of them (roborev F2). Two DISTINCT requirements, kept distinct because they
fail for different reasons and the operator's next action differs:

  * PER-ARM TREATMENT STABILITY -- for one arm, the flight pin, pin mode, allocator, arena cap
    and counter mode must be identical in EVERY round. A treatment that changed mid-set is not
    one arm, and its per-round deltas are not one arm's deltas.
  * CROSS-ARM INVARIANTS -- the bare-scan pin, the client CPUs, the corpus identity, the
    measured binaries' digests and the admission triple must be identical across EVERY session
    of the set, all arms included. If the scan pin differs between arms the drift control is
    GONE; if the admission triple differs the arms differ in TWO properties, because
    `cqlite-flight`'s `resolve_max_concurrent_scans` (cqlite-flight/src/main.rs:53) derives the
    ceiling from `available_parallelism`, which respects the CPU affinity mask.

All of it is read back out of each session's OWN `results.json` -- never re-derived from the
driver's table. That posture is deliberate and is stated in both files: this tool's job is to
detect a divergence between what the driver INTENDED and what was MEASURED, so re-deriving from
the intention would defeat it. An ABSENT field is COULD-NOT-MEASURE and is REFUSED with the
field named, never skipped: a comparison that could not be made has not been made.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import statistics
import sys

SCAN_ARM = "bare_scan"

# THE PER-ARM TREATMENT. These may legitimately DIFFER BETWEEN ARMS -- that is what an arm is --
# and must be IDENTICAL IN EVERY ROUND of one arm. `counter_mode` is in here rather than in the
# invariants because it names both pins, so it moves with the flight pin by construction.
TREATMENT_FIELDS = (
    "flight_server_cpus",
    "flight_pin_mode",
    "flight_allocator",
    "flight_malloc_arena_max",
    "counter_mode",
)

# THE CROSS-ARM INVARIANTS. `server_cpus` is the drift control's own pin: if it differs between
# arms the bare scan is a second treatment and there is nothing left to read the first one
# against (this is the property `ws0-3551-abc.sh` is shaped around). `client_cpus` is the load
# generator's pin, which is measurement apparatus and not a treatment.
PIN_INVARIANT_FIELDS = ("server_cpus", "client_cpus")

# The admission triple, read from whatever `ws0_flight_admission.py` recorded. All three, not
# just the ceiling: `max_concurrent_scans` alone cannot distinguish "the derivation gave the
# same answer" from "someone pinned it", and `available_parallelism` is the INPUT that moves
# with the affinity mask.
ADMISSION_FIELDS = (
    "max_concurrent_scans",
    "max_concurrent_scans_source",
    "available_parallelism",
)

# The corpus's identity, as the session recorded it. The PATH is deliberately not compared here:
# the driver's run fingerprint pins the path, and this tool's subject is the bytes that were
# measured -- two sessions reading the same corpus through different mount paths are comparable,
# two sessions reading different bytes are not.
CORPUS_IDENTITY_FIELDS = ("data_db_sha256", "rows")


class Unreadable(Exception):
    """A session that cannot be aggregated. Never downgraded to a warning."""


def _num(value, what):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Unreadable(f"{what} is {value!r}, not a number")
    return float(value)


def _positive(value, what, why):
    """A DIVISOR, validated FINITE and POSITIVE at the point of use.

    The same rule `ws0_report.positive_derived` applies, restated here because this tool has its
    own divisors: a `nan` or `0` reaching a ratio prints `nan x` / `inf x`, which is a printable
    figure standing in for an absent one -- and in this table the absent one is the arm under
    study.
    """
    number = _num(value, what)
    if not math.isfinite(number) or number <= 0:
        raise Unreadable(f"{what} is {value!r}, which is not usable as a divisor -- {why}")
    return number


def _require(mapping, key, where, why):
    """A key that is ABSENT was NEVER MEASURED, and an unmeasured field cannot be compared.

    This is the three-valued read the configuration validation rests on. A key PRESENT with a
    `null` value is a RECORDED value (arms A and B genuinely have no arena cap) and compares
    like any other; a key that is MISSING -- a session recorded before the field existed, or by
    something other than this rig -- is COULD-NOT-MEASURE and is refused with the field named.
    Skipping the comparison instead would let exactly the divergence this validation exists to
    catch pass unexamined.
    """
    if not isinstance(mapping, dict):
        raise Unreadable(
            f"{where} is {type(mapping).__name__}, not an object, so {key!r} could not be read"
            f" -- {why}"
        )
    if key not in mapping:
        raise Unreadable(
            f"{where} carries no {key!r} -- that field was NOT RECORDED, so it cannot be"
            f" compared across the set. Refused rather than skipped: {why}"
        )
    return mapping[key]


def load_session(d: pathlib.Path) -> dict:
    """One `ws0-baseline.sh` results.json, reduced to the fields this report prints.

    Every field is REQUIRED. An absent one is an Unreadable, never a default: a missing
    counter reported as 0 is the fabrication the rig's own integrity contract forbids.
    """
    p = d / "results.json"
    try:
        raw = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Unreadable(f"{p}: {exc}") from None
    out = {"dir": str(d), "warm": {}}
    for m in raw.get("measurements", []):
        if m.get("temperature") != "warm":
            continue
        arm = m.get("arm")
        if not arm:
            raise Unreadable(f"{p}: a measurement carries no `arm`")
        reps = m.get("reps") or []
        if not reps:
            raise Unreadable(f"{p}: arm {arm} carries no reps")
        out["warm"][arm] = {
            "rows_per_sec": _num(m["rows_per_sec"]["median"], f"{arm} rows_per_sec"),
            "cycles_per_row": _num(m["cycles_per_row"]["median"], f"{arm} cycles_per_row"),
            "ipc": _num(m["ipc"]["median"], f"{arm} ipc"),
            "spread_pct": _num(m["rows_per_sec"]["spread_pct_of_median"], f"{arm} spread"),
            "rows": m.get("row_denominator_total"),
            "n": m["rows_per_sec"]["n"],
        }
    if SCAN_ARM not in out["warm"]:
        raise Unreadable(f"{p}: no warm {SCAN_ARM} leg — the drift control is ABSENT, so no"
                         " delta from this session is readable")
    flight = [a for a in out["warm"] if a != SCAN_ARM]
    if len(flight) != 1:
        raise Unreadable(f"{p}: expected exactly one warm flight arm, found {flight!r}")
    out["flight_arm"] = flight[0]
    # THE RECORDED CONFIGURATION, every field REQUIRED. Note what is deliberately gone: this
    # used to read `pin.get("flight_server_cpus", pin.get("server_cpus"))`, a FALLBACK that
    # silently substituted the bare-scan pin for a session that never recorded a flight pin --
    # so the one field whose whole purpose is to differ between arms could read as identical
    # across a set none of whose sessions recorded it.
    pin = raw.get("pinning")
    where = f"{p}: `pinning`"
    why = ("the arms differ in exactly these fields, so an unrecorded one cannot be shown to be"
           " the difference the label claims")
    out["treatment"] = {f: _require(pin, f, where, why) for f in TREATMENT_FIELDS}
    out["invariants"] = {
        f: _require(pin, f, where,
                    "it is the drift control's own apparatus, which must not move across the"
                    " set") for f in PIN_INVARIANT_FIELDS
    }
    identity = raw.get("corpus_identity")
    for field in CORPUS_IDENTITY_FIELDS:
        out["invariants"][f"corpus_identity.{field}"] = _require(
            identity, field, f"{p}: `corpus_identity`",
            "two sessions that read different corpus bytes are not comparable",
        )
    binaries = _require(raw, "binary_provenance", str(p),
                        "this rig's output is a ratio between two binaries, so which binaries"
                        " were measured is the comparison itself")
    binaries = _require(binaries, "binaries", f"{p}: `binary_provenance`",
                        "the arms must measure IDENTICAL BYTES -- the whole reason --bin-dir is"
                        " not per-arm (#3248 withdrew a machine-code claim for exactly that)")
    if not isinstance(binaries, dict) or not binaries:
        raise Unreadable(
            f"{p}: `binary_provenance.binaries` is {binaries!r} — no measured binary is"
            " identified, so this session's figures cannot be attributed to a program"
        )
    for name in sorted(binaries):
        out["invariants"][f"binary_sha256.{name}"] = _require(
            binaries[name], "sha256", f"{p}: `binary_provenance.binaries[{name!r}]`",
            "an undigested binary cannot be shown to be the same bytes the other arms ran",
        )
    admission = _require(raw, "flight_admission", str(p),
                         "the admission ceiling is DERIVED from available_parallelism and"
                         " therefore from the affinity mask, so it moves with the flight pin")
    out["admission"] = {
        f: _require(admission, f, f"{p}: `flight_admission`",
                    "all three are needed: the ceiling alone cannot distinguish a re-derivation"
                    " from a pin, and available_parallelism is the input that moves")
        for f in ADMISSION_FIELDS
    }
    out["quiescence"] = raw.get("quiescence_verdict") or raw.get("quiescence")
    return out


def spread_pct(values):
    """(max - min) / median, or None when there is NOTHING TO SPREAD.

    A single value's spread used to print `0.00%`, which reads as a MEASURED tightness rather
    than as one round. `None` is rendered `n/a (1 round)` by `fmt_spread`, because a figure
    nobody could measure must not be printed as a figure somebody did.
    """
    if len(values) < 2:
        return None
    med = statistics.median(values)
    return 100.0 * (max(values) - min(values)) / med if med else float("nan")


def fmt_spread(values):
    pct = spread_pct(values)
    if pct is None:
        return f"n/a ({len(values)} round)"
    return f"{pct:.2f}%"


def table(rows, headers):
    widths = [max(len(str(r[i])) for r in [headers] + rows) for i in range(len(headers))]
    sep = "|" + "|".join("-" * (w + 2) for w in widths) + "|"
    def line(r):
        return "| " + " | ".join(str(v).ljust(w) for v, w in zip(r, widths)) + " |"
    return "\n".join([line(headers), sep] + [line(r) for r in rows])


def _tag(rnd, arm, session):
    return f"r{rnd}-{arm} ({session['dir']})"


def _refuse_unless_identical(subject, records, consequence):
    """Every record in `records` must carry the SAME fields with the SAME values.

    `records` is a list of `(tag, mapping)`. The refusal names the FIELD and BOTH VALUES beside
    the two sessions that disagree, because the operator's next action is on one of those two
    directories and "the configuration differs" names neither.
    """
    if len(records) < 2:
        return
    ref_tag, ref = records[0]
    for tag, cur in records[1:]:
        for field in sorted(set(ref) | set(cur)):
            if field not in cur:
                raise Unreadable(
                    f"{subject}: {field!r} was recorded by {ref_tag} and is NOT RECORDED by"
                    f" {tag}, so the two cannot be compared on it. {consequence}"
                )
            if field not in ref:
                raise Unreadable(
                    f"{subject}: {field!r} was recorded by {tag} and is NOT RECORDED by"
                    f" {ref_tag}, so the two cannot be compared on it. {consequence}"
                )
            if ref[field] != cur[field]:
                raise Unreadable(
                    f"{subject}: {field!r} DIFFERS — {ref_tag} recorded {ref[field]!r},"
                    f" {tag} recorded {cur[field]!r}. {consequence}"
                )


def validate_configuration(sessions, complete, arms) -> list[str]:
    """The F2 check: validate the recorded configuration of EVERY (round, arm) before computing.

    SCOPE, stated because it is a decision and not an oversight: the subject is exactly the
    sessions that are AGGREGATED — the arms of the pairable rounds. A round dropped as
    incomplete contributes to no figure, and refusing on it would red an interrupted-but-correct
    resume, which is the guard an operator learns to work around.
    """
    for arm in arms:
        _refuse_unless_identical(
            f"arm {arm}'s TREATMENT changed within the set",
            [(_tag(r, arm, sessions[(r, arm)]), sessions[(r, arm)]["treatment"])
             for r in complete],
            "A treatment that changed mid-set is not one arm, so its per-round deltas are not"
            " one arm's deltas.",
        )
    every = [(_tag(r, a, sessions[(r, a)]), sessions[(r, a)]) for r in complete for a in arms]
    _refuse_unless_identical(
        "the set's CROSS-ARM INVARIANTS are not identical",
        [(tag, s["invariants"]) for tag, s in every],
        "If the bare-scan pin or the client pin differs between arms the DRIFT CONTROL IS GONE —"
        " the bare scan becomes a second treatment and there is nothing left to read the first"
        " one against; if the corpus identity or a binary digest differs, the arms did not"
        " measure the same thing at all.",
    )
    _refuse_unless_identical(
        "the set's ADMISSION TRIPLE is not identical across arms",
        [(tag, s["admission"]) for tag, s in every],
        "The arms then differ in TWO properties, not one: cqlite-flight's"
        " `resolve_max_concurrent_scans` (cqlite-flight/src/main.rs:53) derives the ceiling from"
        " `available_parallelism`, which respects the CPU AFFINITY MASK — so the ceiling moves"
        " with the flight pin and a moved ceiling is a second treatment.",
    )
    inv = sessions[(complete[0], arms[0])]
    return [
        "Configuration VALIDATED over every aggregated (round, arm):"
        f" {len(every)} session(s) = {len(complete)} pairable round(s) x {len(arms)} arm(s).",
        "",
        f"* per-arm TREATMENT stability, all of {', '.join(TREATMENT_FIELDS)}: identical in"
        " every round of each arm.",
        f"* CROSS-ARM invariants, all {len(inv['invariants'])} of them"
        f" ({', '.join(PIN_INVARIANT_FIELDS)}, the corpus identity and every measured binary's"
        " sha256): identical in every session.",
        f"* the ADMISSION TRIPLE ({', '.join(ADMISSION_FIELDS)}): identical in every session.",
        "* SCOPE: the aggregated sessions only. A round dropped as incomplete contributes to no"
        " figure below and is not examined.",
        "",
    ]


def control_table(sessions, complete, arms) -> list[str]:
    """The drift control, FIRST, because the treatment is unreadable without it.

    The ORDER is load-bearing and not a preference: the control is what makes a treatment delta
    readable at all, so it is printed before any treatment figure rather than as a footnote
    somebody reads after quoting the delta.
    """
    lines = [
        "## The drift control (bare scan — code-identical AND pin-identical in every arm)",
        "",
        "Every figure is the MEDIAN over the pairable rounds; each `spread` is (max-min)/median"
        " over those same rounds, so it is a BETWEEN-ROUND spread and not a within-session one.",
        "",
    ]
    rows = []
    for a in arms:
        cyc = [sessions[(r, a)]["warm"][SCAN_ARM]["cycles_per_row"] for r in complete]
        rps = [sessions[(r, a)]["warm"][SCAN_ARM]["rows_per_sec"] for r in complete]
        ipc = [sessions[(r, a)]["warm"][SCAN_ARM]["ipc"] for r in complete]
        rows.append([a, f"{statistics.median(rps):,.0f}", fmt_spread(rps),
                     f"{statistics.median(cyc):,.0f}", fmt_spread(cyc),
                     f"{statistics.median(ipc):.4f}"])
    lines.append(table(rows, ["arm", "rows/s (median)", "rows/s spread",
                              "cycles/row (median)", "cycles/row spread", "IPC (median)"]))
    lines.append("")
    med = [statistics.median([sessions[(r, a)]["warm"][SCAN_ARM]["cycles_per_row"]
                              for r in complete]) for a in arms]
    if len(med) < 2:
        # One arm is NOTHING TO COMPARE, and `(max-min)/median == 0` over one value would print
        # a MEASURED-looking `0.00%` for a movement nobody could measure.
        lines.append("**Control movement across arms: NOT MEASURABLE — this set has ONE arm, so"
                     " there is no across-arm movement to read.**")
    else:
        divisor = _positive(statistics.median(med), "the median control cycles/row across arms",
                            "it is the divisor of the control-movement percentage")
        lines.append(
            f"**Control movement across arms: {100.0 * (max(med) - min(med)) / divisor:.2f}% on"
            " cycles/row.** The control is identical code on identical CPUs in every arm, so"
            " this is drift plus contamination and nothing else. Any treatment delta smaller"
            " than it is NOT READABLE.")
    lines.append("")
    return lines


def layer1(sessions, complete, arms, baseline) -> list[str]:
    """The invariant layer: cycles/row, IPC, the bare/flight ratio and the cycles/row delta.

    `ratio bare/flight` IS AN ESTABLISHED QUANTITY IN THIS RIG AND IT IS A ROWS/S RATIO. It used
    to be computed here as `flight cycles/row / bare cycles/row`, which was wrong TWICE over
    (roborev F3): inverted with respect to its own label, and a different quantity from the one
    that name denotes everywhere else. `ws0_report.py` prints
    `ratio = scan_rps / fl_rps` and `ws0-baseline.sh`'s own output for a real session read
    `ratio bare/flight = 1.34x` for bare 338,090 rows/s and flight 252,789 rows/s (1.337) --
    while the cycles quotient for that same session is 23374/19485 = 1.20. Two different
    numbers under one name is how a table stops being comparable with the rig that produced it,
    so the definition here is the rig's: rows/s(bare) / rows/s(flight), per round, then the
    median. A ratio ABOVE 1 means the bare scan is faster.

    `cycles/row delta` is the other quantity `ws0-3248-artifacts/ac0/DELTA-TABLE.md` reports and
    Layer 1 lacked. Also the rig's own definition (`ws0_report.py`): `flight - bare`, absolute
    and percent, unconstrained in sign, because a Flight arm cheaper per row than the bare scan
    is a legitimate and desirable result.
    """
    lines = [
        "## Layer 1 — the INVARIANT layer (cycles/row, IPC, ratio, cycles/row delta)",
        "",
        "`ratio bare/flight` is rows/s(bare) / rows/s(flight) and `cycles/row delta` is"
        " flight - bare, both the rig's own definitions (`ws0_report.py`), both taken WITHIN a"
        " round and then medianed. A ratio above 1 means the bare scan is faster.",
        "",
    ]
    rows = []
    base_cyc = {}
    for r in complete:
        s = sessions[(r, baseline)]
        base_cyc[r] = _positive(
            s["warm"][s["flight_arm"]]["cycles_per_row"],
            f"the baseline arm {baseline}'s flight cycles/row in round {r}",
            "it is the divisor of every paired cycles/row delta in this table")
    for a in arms:
        cyc, ipc, ratio, delta, delta_pct, paired = [], [], [], [], [], []
        for r in complete:
            s = sessions[(r, a)]
            fl = s["warm"][s["flight_arm"]]
            scan = s["warm"][SCAN_ARM]
            cyc.append(fl["cycles_per_row"])
            ipc.append(fl["ipc"])
            flight_rps = _positive(
                fl["rows_per_sec"], f"the flight median rows/s for {a} (round {r})",
                "it is the DENOMINATOR of `ratio bare/flight`")
            scan_cpr = _positive(
                scan["cycles_per_row"], f"the bare-scan cycles/row for {a} (round {r})",
                "it is the DIVISOR of the cycles/row percentage delta")
            ratio.append(scan["rows_per_sec"] / flight_rps)
            delta.append(fl["cycles_per_row"] - scan_cpr)
            delta_pct.append(100.0 * (fl["cycles_per_row"] / scan_cpr - 1.0))
            # PAIRED per round, never median-of-medians: the pairing is the control for drift.
            paired.append(100.0 * (fl["cycles_per_row"] - base_cyc[r]) / base_cyc[r])
        up = sum(1 for d in paired if d > 0)
        rows.append([
            a,
            f"{statistics.median(cyc):,.0f}", fmt_spread(cyc),
            f"{statistics.median(ipc):.4f}",
            f"{statistics.median(ratio):.4f}x",
            f"{statistics.median(delta):+,.0f} ({statistics.median(delta_pct):+.1f}%)",
            "baseline" if a == baseline else f"{statistics.median(paired):+.2f}%",
            "—" if a == baseline else f"{up}/{len(paired)} up",
        ])
    lines.append(table(rows, ["arm", "cycles/row (median)", "cycles/row spread", "IPC (median)",
                              "ratio bare/flight (median)", "cycles/row delta (median)",
                              f"paired Δcycles/row vs {baseline}",
                              f"direction (cycles/row vs {baseline})"]))
    lines.append("")
    return lines


def layer2(sessions, complete, arms, baseline) -> list[str]:
    lines = [
        "## Layer 2 — the ABSOLUTE layer (rows/s; no cross-session absolute is reusable)",
        "",
    ]
    rows = []
    base_rps = {}
    for r in complete:
        s = sessions[(r, baseline)]
        base_rps[r] = _positive(
            s["warm"][s["flight_arm"]]["rows_per_sec"],
            f"the baseline arm {baseline}'s flight rows/s in round {r}",
            "it is the divisor of every paired rows/s delta in this table")
    for a in arms:
        rps, paired, denominators = [], [], []
        for r in complete:
            s = sessions[(r, a)]
            fl = s["warm"][s["flight_arm"]]
            rps.append(fl["rows_per_sec"])
            paired.append(100.0 * (fl["rows_per_sec"] - base_rps[r]) / base_rps[r])
            denominators.append(fl["rows"])
        up = sum(1 for d in paired if d > 0)
        # THE ROW DENOMINATOR OVER EVERY ROUND, not round one's. It used to be read from
        # `complete[0]` alone and printed in a column beside all-rounds medians, so one round's
        # denominator stood under a heading that named none. It may legitimately DIFFER between
        # rounds (it is requests_ok x corpus_rows and a 45s step does not admit the same number
        # of requests twice), so it is medianed and LABELLED as a median rather than required to
        # be identical -- requiring that would red correct input.
        if any(v is None for v in denominators):
            denominator = "NOT RECORDED"
        else:
            denominator = "{:,.0f}".format(statistics.median(
                [_num(v, f"the row denominator for {a}") for v in denominators]))
        rows.append([a, f"{statistics.median(rps):,.0f}", fmt_spread(rps),
                     "baseline" if a == baseline else f"{statistics.median(paired):+.2f}%",
                     "—" if a == baseline else f"{up}/{len(paired)} up",
                     denominator])
    lines.append(table(rows, ["arm", "rows/s (median)", "rows/s spread",
                              f"paired Δrows/s vs {baseline}",
                              f"direction (rows/s vs {baseline})",
                              "row denominator (median)"]))
    lines.append("")
    return lines


def config_table(sessions, complete, arms) -> list[str]:
    """What each arm actually WAS, read back from its own artifacts.

    Printed from one round's record and SAFE to do so only because `validate_configuration`
    has already established that every round of each arm recorded the same treatment. The
    caption says which claim is which: the values are read back, the identity is verified.
    """
    lines = [
        "## Configuration, read back from each session's own recorded pinning",
        "",
        f"Read from round {complete[0]} of each arm and VERIFIED IDENTICAL in every aggregated"
        " round of that arm (see the validation above) — so this table describes the whole set"
        " and not just one round of it.",
        "",
    ]
    rows = []
    for a in arms:
        s = sessions[(complete[0], a)]
        t = s["treatment"]
        rows.append([a, s["invariants"]["server_cpus"], t["flight_server_cpus"],
                     t["flight_pin_mode"], t["flight_allocator"],
                     "—" if t["flight_malloc_arena_max"] in (None, "") else
                     t["flight_malloc_arena_max"],
                     s["admission"]["max_concurrent_scans"],
                     t["counter_mode"]])
    lines.append(table(rows, ["arm", "scan pin", "flight pin", "pin mode", "allocator",
                              "arena max", "admission ceiling", "counter mode"]))
    lines.append("")
    lines.append("Every figure above is rows/s AND cycles/row; **no CPU-share is reported**"
                 " (#2877: a share shift with rows/s unmoved is a FAIL, not a win).")
    return lines


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", required=True,
                    help="directory holding r<N>-<arm>/ session dirs")
    ap.add_argument("--arms", required=True,
                    help="comma-separated arm labels in report order, e.g. A,B,C0,C")
    ap.add_argument("--baseline", required=True, help="the arm every delta is taken against")
    ap.add_argument("--out", default=None, help="write the markdown here as well as stdout")
    args = ap.parse_args(argv)

    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    if args.baseline not in arms:
        print(f"REFUSED: baseline {args.baseline!r} is not in --arms {arms!r}", file=sys.stderr)
        return 2
    root = pathlib.Path(args.root)

    # rounds x arms, loaded strictly. A round missing ANY arm is dropped WHOLE and named:
    # a paired delta over a round that lacks its baseline is not a paired delta.
    sessions, rounds = {}, []
    for d in sorted(root.glob("r*-*")):
        if not d.is_dir():
            continue
        stem = d.name
        rnd, _, arm = stem.partition("-")
        if arm not in arms:
            continue
        try:
            rn = int(rnd.lstrip("r"))
        except ValueError:
            continue
        sessions[(rn, arm)] = load_session(d)
        if rn not in rounds:
            rounds.append(rn)
    rounds.sort()
    complete = [r for r in rounds if all((r, a) in sessions for a in arms)]
    dropped = [r for r in rounds if r not in complete]
    if not complete:
        print("REFUSED: no round carries every arm; nothing is pairable", file=sys.stderr)
        return 2

    lines = []
    lines.append(f"Rounds pairable: {complete}"
                 + (f"  (DROPPED, incomplete: {dropped})" if dropped else ""))
    lines.append("")
    # BEFORE ANY FIGURE IS COMPUTED. A delta taken across two treatments is not a delta, so the
    # configuration is validated first and the whole run refuses rather than printing a table
    # with a caveat beside it.
    lines += validate_configuration(sessions, complete, arms)
    lines += control_table(sessions, complete, arms)
    lines += layer1(sessions, complete, arms, args.baseline)
    lines += layer2(sessions, complete, arms, args.baseline)
    lines += config_table(sessions, complete, arms)

    text = "\n".join(lines)
    print(text)
    if args.out:
        pathlib.Path(args.out).write_text(text + "\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Unreadable as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        sys.exit(2)
