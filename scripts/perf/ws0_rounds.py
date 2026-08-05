#!/usr/bin/env python3
"""The INTERLEAVING contract: recorded round/position metadata, verified (#3272 R3/R4a).

Split out of `ws0_report.py` under the campsite rule, along a real responsibility seam:
this module owns everything about *when* each rep ran and whether the arms were genuinely
interleaved, while `ws0_report.py` aggregates the measurements themselves and
`ws0_validate.py` decides what may be aggregated.

# What was wrong, and why it is a whole module rather than a line

The report's NOTES printed, UNCONDITIONALLY:

    "the reps were INTERLEAVED — one rep per arm per round, arm order rotated"

as a claim about the session — while `paired_rounds` paired by REP INDEX and never read
anything the driver recorded about the actual round. The driver DID write `<tag>.round`
files (and a comment claiming the reporter read them); NOTHING read them. So any session
dir not produced by that exact loop — an arm-major run, reps re-run individually into one
`--out`, a hand-assembled dir — yielded a report ASSERTING an interleaving that was never
observed. An unconditional claim is not a measurement, and this rig exists because
"reports success without having measured" is the failure mode that costs the most.

Two further things the same defect hid (#3272 R4a):

* the driver's "rotation" ran the BARE SCAN FIRST every round and rotated only the
  FLIGHT arms — so with the default single Flight arm (`--arm bypass`) NO ROTATION
  OCCURRED AT ALL. The fix for the drift hazard did not close it: the bare scan, which
  is the DENOMINATOR of the reported ratio, held position 1 in every round.
* a "rotation" that reduces to a fixed order for the 2-arm case is the same defect, so
  the property asserted here is POSITIONAL — no arm may hold one position across all
  rounds — rather than "the driver called a function named rotate".

# The recorded shape

The driver writes one `<tag>.round` per rep, `key=value` per line:

    round=<1-based round index>
    position=<1-based position of this arm WITHIN the round>
    arms_in_round=<how many arms the round measured>

`position` is what makes the rotation checkable at all: `round` alone cannot distinguish
an interleaved session from an arm-major one, because an arm-major run also has a rep
index. And `arms_in_round` is recorded rather than inferred so a round that measured
fewer arms than it should is visible instead of looking like a complete smaller round.
"""

from __future__ import annotations

import pathlib
import statistics

from ws0_validate import Invalid

# Reject a metadata file larger than this. It holds three short lines; anything bigger is
# not this artifact, and reading an arbitrary file into memory to parse it is how a
# reporting path acquires a resource bug.
_MAX_META_BYTES = 4096


def round_meta_path(d: pathlib.Path, tag: str) -> pathlib.Path:
    """Where the driver records this rep's round/position."""
    return d / f"{tag}.round"


def load_round_meta(d: pathlib.Path, tag: str) -> dict[str, int]:
    """This rep's recorded `round`/`position`/`arms_in_round`, or `Invalid`.

    REQUIRED, never defaulted. An absent file means the interleaving of this rep was not
    recorded, and the report may not then claim the session was interleaved — that claim
    is the whole subject of #3272 R3. Every field is parsed to an int and range-checked;
    an unparseable one is an error rather than a value the caller would treat as 0.
    """
    p = round_meta_path(d, tag)
    if not p.exists():
        raise Invalid(
            f"rep {tag} has no round metadata at {p.name} — the round and the arm's"
            " POSITION within it were not recorded, so this session's interleaving cannot"
            " be established. The report may not print the interleaving claim over"
            " artifacts that do not carry it (#3272 R3): a median-vs-median comparison"
            " across two different time windows is exactly what measurement-method.md"
            " §3b forbids, and it is indistinguishable from an interleaved one without"
            " this file. Re-run the session with scripts/perf/ws0-baseline.sh, which"
            " records it per rep."
        )
    if p.stat().st_size > _MAX_META_BYTES:
        raise Invalid(
            f"{p.name} is {p.stat().st_size:,} bytes; the round metadata is three short"
            " lines, so this is not that artifact"
        )
    fields: dict[str, int] = {}
    for lineno, line in enumerate(p.read_text().splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise Invalid(f"{p.name} line {lineno} is not `key=value`: {line!r}")
        key, _, raw = line.partition("=")
        key = key.strip()
        try:
            fields[key] = int(raw.strip())
        except ValueError:
            raise Invalid(
                f"{p.name} line {lineno} records {key!r} as {raw.strip()!r}, which is not"
                " an integer — a corrupt metadata field is not a zero"
            ) from None
    for key in ("round", "position", "arms_in_round"):
        if key not in fields:
            raise Invalid(
                f"{p.name} carries no {key!r}. All three of round/position/arms_in_round"
                " are required: `round` alone cannot distinguish an interleaved session"
                " from an arm-major one, since both have a rep index."
            )
        if fields[key] < 1:
            raise Invalid(f"{p.name}: {key!r} is {fields[key]}, which is not a 1-based index")
    if fields["position"] > fields["arms_in_round"]:
        raise Invalid(
            f"{p.name}: position {fields['position']} exceeds arms_in_round"
            f" {fields['arms_in_round']} — the recorded round is internally inconsistent"
        )
    return fields


def collect_round_meta(d: pathlib.Path, tag: str, rep: int) -> dict[str, int]:
    """`load_round_meta`, plus the cross-check against the rep index in the FILENAME.

    The driver names artifacts `…-<rep>` and records `round=<rep>`, so a disagreement
    means the artifact set was assembled from more than one session (or renamed), and the
    round a figure is attributed to would not be the round it was measured in.
    """
    meta = load_round_meta(d, tag)
    if meta["round"] != rep:
        raise Invalid(
            f"rep {tag} is named for rep {rep} but records round={meta['round']}. The"
            " artifact set does not describe one session, so no figure can be attributed"
            " to a round with confidence."
        )
    return meta


def verify_interleaving(temp: str, arms_meta: dict[str, dict[int, dict[str, int]]]) -> dict:
    """Establish — or REFUSE — that this temperature's reps were interleaved.

    `arms_meta` maps arm label -> {round -> metadata}. Returns the OBSERVATION record the
    report prints; raises `Invalid` when interleaving cannot be established, because a
    report that cannot establish it must not claim it.

    Four properties, each of which a real session shape violates:

    * every arm covers the SAME rounds — otherwise there is no round to difference within;
    * within each round the positions are exactly `1..n` with no duplicate — two arms at
      one position means the recorded round is not a round;
    * `arms_in_round` AGREES with the number of arms actually present in that round — a
      round that measured fewer arms than it recorded is a partial round, not a small one;
    * ROTATION: with >=2 arms and >=2 rounds, NO arm may hold the same position in every
      round. This is the property, stated positionally. `--arm bypass` (the default) plus
      the bare scan is the 2-arm case, and it is exactly the case the pre-fix loop got
      wrong: the bare scan led every round and only the Flight arms rotated, so with one
      Flight arm nothing rotated at all.
    """
    if not arms_meta:
        raise Invalid(f"no arms recorded for the {temp} temperature — nothing to interleave")
    labels = sorted(arms_meta)
    round_sets = {label: set(arms_meta[label]) for label in labels}
    rounds = round_sets[labels[0]]
    for label in labels[1:]:
        if round_sets[label] != rounds:
            raise Invalid(
                f"the {temp} arms do not cover the same rounds"
                f" ({ {k: sorted(v) for k, v in round_sets.items()} }), so there is no"
                " round to difference WITHIN. The driver measures one rep of every arm per"
                " round precisely so each round is a contemporaneous set."
            )
    n_arms = len(labels)
    for rnd in sorted(rounds):
        positions = {label: arms_meta[label][rnd]["position"] for label in labels}
        if sorted(positions.values()) != list(range(1, n_arms + 1)):
            raise Invalid(
                f"round {rnd} ({temp}) records positions {positions}, which is not"
                f" 1..{n_arms} exactly once. Two arms cannot share a position, and a gap"
                " means an arm of this round was not measured."
            )
        for label in labels:
            recorded = arms_meta[label][rnd]["arms_in_round"]
            if recorded != n_arms:
                raise Invalid(
                    f"round {rnd} ({temp}) has {n_arms} arms present but {label} records"
                    f" arms_in_round={recorded}. A round that measured fewer arms than it"
                    " recorded is a PARTIAL round, and differencing within it would"
                    " compare against an arm that was not there."
                )
    fixed = [
        label
        for label in labels
        if len({arms_meta[label][r]["position"] for r in rounds}) == 1
    ]
    if n_arms >= 2 and len(rounds) >= 2 and fixed:
        raise Invalid(
            f"the {temp} arm(s) {', '.join(fixed)} held ONE FIXED POSITION across all"
            f" {len(rounds)} rounds. measurement-method.md §3b step 2 requires the arm"
            " order to ROTATE every round so no arm holds a fixed position: a fixed"
            " position means any within-round systematic effect (a cache left by the"
            " previous arm, a thermal ramp inside the round) lands on the same arm every"
            " time, and that is a bias the per-round direction count cannot see."
            " MEASURED before this was checked: the bare scan led every round and only the"
            " FLIGHT arms rotated, so with the default single Flight arm NOTHING rotated"
            " (#3272 R4a)."
        )
    return {
        "verified": True,
        "source": "per-rep <tag>.round files written by ws0-baseline.sh",
        "rounds": sorted(rounds),
        "arms_per_round": n_arms,
        "positions_by_round": {
            str(r): {label: arms_meta[label][r]["position"] for label in labels}
            for r in sorted(rounds)
        },
        "rotation_checked": n_arms >= 2 and len(rounds) >= 2,
        "rotation_note": (
            "no arm held a fixed position across rounds"
            if n_arms >= 2 and len(rounds) >= 2
            else f"only {len(rounds)} round(s) x {n_arms} arm(s): rotation is not"
            " observable at this size, so it is NOT claimed"
        ),
    }


def interleaving_lines(observation: dict) -> list[str]:
    """The interleaving claim, DERIVED from the observation above — never unconditional.

    The rotation sentence is printed only when `rotation_checked` is true, because at one
    round (or one arm) there is nothing to rotate and saying "arm order rotated" would be
    a claim about something not measured — the exact shape of the unconditional NOTES text
    this replaces.
    """
    lines = [
        "  * the reps were INTERLEAVED — one rep of every arm per round — and this is"
        " OBSERVED, not asserted: every rep records its round and its POSITION within"
        f" that round ({observation['source']}), and the report REFUSES a session whose"
        " interleaving cannot be established from those artifacts. Rounds observed:"
        f" {observation['rounds']}, {observation['arms_per_round']} arm(s) per round."
    ]
    if observation["rotation_checked"]:
        lines.append(
            "    The arm ORDER ROTATED: no arm held a fixed position across rounds, so no"
            " arm carries a within-round systematic effect every time"
            " (measurement-method.md §3b step 2). Positions by round:"
            f" {observation['positions_by_round']}."
        )
    else:
        lines.append(f"    NOTE: {observation['rotation_note']}.")
    lines.append(
        "    This rig produces no cross-session absolute: the untouched bare scan drifted"
        " ~10% in one hour on the recorded box, so an arm-after-arm ordering would put"
        " that drift straight onto the bare/flight ratio. Read the per-round direction"
        " count, not the median difference alone (measurement-method.md §3b)."
    )
    return lines


def paired_rounds(scan: dict, fl: dict) -> tuple[list[dict], list[str]]:
    """The WITHIN-ROUND bare/flight comparison, paired by the OBSERVED round (#3272 R3).

    Each rep's `round` field is the one `collect_round_meta` read from that rep's own
    `<tag>.round` artifact and cross-checked against its filename — NOT the rep index the
    reporter happened to iterate. That distinction is the finding: pairing by index
    produces a per-round table for an arm-major session too, and prints it under a claim
    that the session was interleaved.

    Differencing within a round, rather than the two medians, is what
    `measurement-method.md` §3b step 4 requires:

        "**Difference within a round**, and report the per-round deltas and how many
        were positive — not the medians alone. At these spreads (5-10% per arm) a
        median-vs-median difference of a couple of percent is not readable."

    That is not a stylistic preference; it is the check that caught a real error. The
    #3096 session's `+4,817 rows/s / +2.3%` lever-4 result re-measured at ZERO on 8
    interleaved rounds — median -72 rows/s (-0.03%), 4 of 8 rounds positive. A
    median-vs-median reading would have published the 2.3%.
    """
    scan_by_round = {r["round"]: r for r in scan["reps"]}
    fl_by_round = {r["round"]: r for r in fl["reps"]}
    if set(scan_by_round) != set(fl_by_round):
        raise Invalid(
            f"the bare scan and {fl['arm']} do not cover the same OBSERVED rounds"
            f" (scan {sorted(scan_by_round)}, flight {sorted(fl_by_round)}), so no"
            " within-round comparison is possible. The driver interleaves one rep of every"
            " arm per round precisely so each round is contemporaneous; differencing"
            " medians alone is what measurement-method.md §3b forbids."
        )
    rounds = []
    for rnd in sorted(scan_by_round):
        s, f = scan_by_round[rnd], fl_by_round[rnd]
        if f["rows_per_sec"] <= 0 or s["rows_per_sec"] <= 0:
            raise Invalid(
                f"round {rnd} has a non-positive rows/s (bare {s['rows_per_sec']},"
                f" flight {f['rows_per_sec']}) — there is no ratio for that round"
            )
        rounds.append(
            {
                "round": rnd,
                "bare_position_in_round": s["position_in_round"],
                "flight_position_in_round": f["position_in_round"],
                "bare_rows_per_sec": s["rows_per_sec"],
                "flight_rows_per_sec": f["rows_per_sec"],
                "ratio_bare_over_flight": s["rows_per_sec"] / f["rows_per_sec"],
                "cycles_per_row_delta": f["cycles_per_row"] - s["cycles_per_row"],
                # The 1.3x verdict, decided WITHIN the round rather than across windows.
                "flight_meets_target": f["rows_per_sec"] >= s["rows_per_sec"] / 1.3,
            }
        )
    met = sum(1 for r in rounds if r["flight_meets_target"])
    ratios = [r["ratio_bare_over_flight"] for r in rounds]
    lines = [
        "      per-round (PAIRED by the OBSERVED round, as method §3b step 4 requires):",
        "        ratios "
        + ", ".join(f"r{r['round']}={r['ratio_bare_over_flight']:.2f}x" for r in rounds),
        f"        within-round 1.3x target met in {met}/{len(rounds)} round(s);"
        f" paired ratio median {statistics.median(ratios):.2f}x"
        f" [{min(ratios):.2f}..{max(ratios):.2f}]",
        "        arm positions "
        + ", ".join(
            f"r{r['round']}=bare@{r['bare_position_in_round']}"
            f"/flight@{r['flight_position_in_round']}"
            for r in rounds
        ),
    ]
    if len(rounds) < 3:
        lines.append(
            f"        !! only {len(rounds)} round(s): the per-round direction count is"
            " the readable signal at this rig's 5-10% per-arm spread, and it needs"
            " several rounds. Raise --reps."
        )
    return rounds, lines
