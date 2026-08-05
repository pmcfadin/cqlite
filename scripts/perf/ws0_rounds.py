#!/usr/bin/env python3
"""RECORDED per-rep round metadata: kept, consistency-checked, and CLAIMED NOTHING ABOUT.

Split out of `ws0_report.py` under the campsite rule, along a real responsibility seam:
this module owns *what each rep recorded about when and where it ran*, while
`ws0_report.py` aggregates the measurements themselves and `ws0_validate.py` decides what
may be aggregated.

# THE INTERLEAVING CLAIM WAS DELETED (#3272 review round 4, owner ruling)

Earlier rounds of #3272 had this module DERIVE and PRINT an interleaving claim: "the reps
were INTERLEAVED … OBSERVED FROM THE CLOCK … the report verifies that every rep of round r
finished before any rep of round r+1", plus `results.json` verdict fields
(`interleaving.verified`, `timing.round_major_verified`, `timing.established`).

Round 4 found that claim FALSE at the rig's own default. At `--reps 1` there is exactly ONE
round, `zip(ordered, ordered[1:])` is EMPTY, **zero orderings are compared** — and the code
still returned `round_major_verified: True` and printed the sentence verbatim. That is a
positive verdict derived from the ABSENCE of a bad signal, which is the precise defect the
whole issue exists to remove; the fix that introduced the stronger wording is what made the
claim false.

The governing ruling was: the claim may stay only if it becomes a genuine observation whose
wording is bounded by what the artifacts prove — otherwise DELETE it, and amend
`docs/reports/ws0-3096-artifacts/measurement-method.md` §3b in the same change, because a
method doc calling a nonexistent control "binding" is worse than no control. It was
deleted. Re-adding an observed drift control on real hardware is tracked by #3287/#3299.

# What is left, and the line between the two kinds of thing

KEPT — RECORDED DATA. The driver writes one `<tag>.round` per rep, `key=value` per line:

    round=<1-based round index>
    position=<1-based position of this arm WITHIN the round>
    arms_in_round=<how many arms the round measured>
    monotonic_ns=<time.monotonic_ns() at the rep's COMPLETION>

All four are REQUIRED and validated, and they are surfaced in `results.json` verbatim under
`recorded_round_metadata` — inert data an operator (or #3287/#3299) can analyse. Nothing
derives a property from them.

KEPT — FAIL-CLOSED REFUSALS over that data. These are INTEGRITY checks on the artifact set,
not evidence of anything about the measurement:

* every arm covers the same rounds (otherwise `paired_rounds` has nothing to pair);
* within a round the positions are exactly `1..n`, no duplicate;
* `arms_in_round` agrees with the number of arms actually present in that round;
* no two reps record the IDENTICAL instant (a sequential loop cannot; that is a copy);
* the recorded round LABELS do not contradict the recorded INSTANTS (see
  `refuse_label_instant_contradiction`).

DELETED — every CLAIM and every VERDICT FIELD. No `verified`, no `established`, no
`round_major_verified`, no printed interleaving/rotation sentence. The report says, once and
explicitly, that it makes no such claim.
"""

from __future__ import annotations

import pathlib
import statistics

from ws0_validate import Invalid, positive_derived, positive_int

# Reject a metadata file larger than this. It holds three short lines; anything bigger is
# not this artifact, and reading an arbitrary file into memory to parse it is how a
# reporting path acquires a resource bug.
_MAX_META_BYTES = 4096


def round_meta_path(d: pathlib.Path, tag: str) -> pathlib.Path:
    """Where the driver records this rep's round/position."""
    return d / f"{tag}.round"


def load_round_meta(d: pathlib.Path, tag: str) -> dict[str, int]:
    """This rep's recorded `round`/`position`/`arms_in_round`/`monotonic_ns`, or `Invalid`.

    REQUIRED, never defaulted, and RECORDED rather than interpreted: these four fields are
    what the driver wrote about where and when this rep ran. The report derives no property
    from them (#3272 round 4 — the interleaving claim was deleted); it pairs the per-round
    comparison by the recorded `round`, refuses an artifact set whose fields contradict each
    other, and passes the values through to `results.json` unchanged.

    Required rather than optional because an absent file makes the artifact set
    unpairable and unattributable — a figure could not be tied to the round it was
    recorded in at all. Every field is parsed to an int and range-checked; an unparseable
    one is an error rather than a value the caller would treat as 0.
    """
    p = round_meta_path(d, tag)
    if not p.exists():
        raise Invalid(
            f"rep {tag} has no round metadata at {p.name} — the round and the arm's"
            " POSITION within it were not recorded, so this rep's figures cannot be"
            " attributed to a round and the per-round pairing has nothing to pair."
            " Re-run the session with scripts/perf/ws0-baseline.sh, which records it per"
            " rep. (This file is RECORDED DATA: the report makes no interleaving or"
            " ordering claim from it — see #3287/#3299.)"
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
        # `positive_int`, the SHARED validator, not a bare `int()` (#3272 review round 3,
        # B2/B5). Applied here rather than only to the three required keys below, so an
        # unrecognized key carrying junk is refused where it is read: `int("3.7")` used to
        # raise a bare ValueError, `int(" 3 ")` silently accepted padding, and a `-1` reached
        # the range test only for the three keys the loop below enumerated.
        try:
            fields[key] = positive_int(
                f"{p.name} line {lineno} {key!r}",
                raw,
                f"{p.name} records {key!r} as {raw.strip()!r}, which is not an integer or"
                " not a 1-based index — a corrupt metadata field is not a zero.",
            )
        except Invalid as exc:
            raise Invalid(
                f"{exc} A corrupt round-metadata field is not a zero; these are 1-based"
                " indices written by ws0-baseline.sh's record_round."
            ) from None
    for key in ("round", "position", "arms_in_round", "monotonic_ns"):
        if key not in fields:
            raise Invalid(
                f"{p.name} carries no {key!r}. All four of"
                " round/position/arms_in_round/monotonic_ns are required as RECORDED DATA:"
                " `round` is what the per-round pairing pairs on, `position` and"
                " `arms_in_round` are LABELS the driver computes, and `monotonic_ns` is when"
                " the rep completed. A partial record is refused rather than defaulted"
                " because the missing field cannot be reconstructed. NOTE: no ORDERING"
                " property is derived from these — the interleaving claim was deleted"
                " (#3272 round 4; re-adding an observed control is #3287/#3299)."
                " Re-run the session with scripts/perf/ws0-baseline.sh."
            )
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


def collect_recorded_round_metadata(
    temp: str, arms_meta: dict[str, dict[int, dict[str, int]]]
) -> dict:
    """INTEGRITY-check this temperature's recorded metadata, and return it VERBATIM.

    `arms_meta` maps arm label -> {round -> metadata}. Returns a record of WHAT WAS
    RECORDED — no verdict field, no `verified`, no `established`. Raises `Invalid` when the
    artifact set CONTRADICTS ITSELF, which is a statement about the files, never about the
    measurement.

    # No ordering/interleaving property is derived here (#3272 round 4)

    The deleted version returned `verified: True` and a `timing.round_major_verified: True`
    that was reached WITHOUT COMPARING ANYTHING at one round (`zip(ordered, ordered[1:])` is
    empty), and `interleaving_lines` then printed "the reps were INTERLEAVED … OBSERVED FROM
    THE CLOCK". A verdict no measurement backed is the defect this whole issue is about, so
    the claim is gone rather than re-worded a third time. `#3287/#3299` own re-adding a real,
    observed drift control on hardware that can support one.

    # The four refusals that remain, and why each is an INTEGRITY check

    None of these establishes a property of the session; each says the FILES disagree with
    each other, which makes the figures unattributable:

    * arms covering DIFFERENT round sets — `paired_rounds` would have nothing to pair;
    * positions within a round that are not exactly `1..n` — two arms at one position, or a
      gap, means the recorded round does not describe the arms present;
    * `arms_in_round` disagreeing with the number of arms actually present;
    * two reps recording the IDENTICAL `monotonic_ns` — a strictly sequential loop cannot
      produce that, so the file was copied rather than written by a measurement.

    The LABEL/INSTANT contradiction check lives in `refuse_label_instant_contradiction`,
    called from here, and its SCOPE (how many comparisons it was able to make) is recorded as
    a plain count rather than a pass.
    """
    if not arms_meta:
        raise Invalid(
            f"no arms recorded for the {temp} temperature — there is no round metadata to"
            " check or to record"
        )
    labels = sorted(arms_meta)
    round_sets = {label: set(arms_meta[label]) for label in labels}
    rounds = round_sets[labels[0]]
    for label in labels[1:]:
        if round_sets[label] != rounds:
            raise Invalid(
                f"the {temp} arms do not cover the same rounds"
                f" ({ {k: sorted(v) for k, v in round_sets.items()} }), so there is no"
                " round to difference WITHIN. Every figure would be attributed to a round"
                " some other arm never recorded."
            )
    n_arms = len(labels)
    for rnd in sorted(rounds):
        positions = {label: arms_meta[label][rnd]["position"] for label in labels}
        if sorted(positions.values()) != list(range(1, n_arms + 1)):
            raise Invalid(
                f"round {rnd} ({temp}) records positions {positions}, which is not"
                f" 1..{n_arms} exactly once. Two arms cannot share a position, and a gap"
                " means an arm of this round was not recorded."
            )
        for label in labels:
            recorded = arms_meta[label][rnd]["arms_in_round"]
            if recorded != n_arms:
                raise Invalid(
                    f"round {rnd} ({temp}) has {n_arms} arms present but {label} records"
                    f" arms_in_round={recorded}. A round that recorded fewer arms than it"
                    " claims is a PARTIAL round, and differencing within it would compare"
                    " against an arm that was not there."
                )
    # AN ARM AT A FIXED POSITION IS REFUSED — as a PRODUCER-CONTRACT check, not as evidence
    # of anything (#3272 round 4). `ws0-baseline.sh` rotates its arm list by round index
    # (`rotate_arms`), and with >=2 arms over >=2 rounds that rotation necessarily moves
    # every arm. So a recorded set in which some arm holds ONE position throughout did not
    # come from this driver's loop — either the artifacts were produced by something else, or
    # the loop regressed to the pre-#3272 shape where the bare scan led every round. Both
    # make the artifact set unattributable, which is why it is refused.
    #
    # WHAT THIS IS NOT: it is not a drift control, and passing it establishes NOTHING about
    # the session. The rotation CLAIM this check used to license was deleted; only the
    # refusal remains, because dropping a fail-closed check would be a loss of coverage.
    fixed = [
        label
        for label in labels
        if len({arms_meta[label][r]["position"] for r in rounds}) == 1
    ]
    if n_arms >= 2 and len(rounds) >= 2 and fixed:
        raise Invalid(
            f"the {temp} arm(s) {', '.join(fixed)} held ONE FIXED POSITION across all"
            f" {len(rounds)} rounds, which ws0-baseline.sh's rotation cannot produce for"
            f" {n_arms} arms over {len(rounds)} rounds: it left-rotates the arm list by round"
            " index, so every arm moves. This artifact set therefore was not written by this"
            " driver's loop (or the loop regressed to leading every round with the bare scan,"
            " #3272 R4a), and its figures cannot be attributed. This is a PRODUCER-CONTRACT"
            " refusal, not a drift control: the rig makes no rotation or interleaving claim"
            " (#3272 round 4; #3287/#3299)."
        )
    integrity = refuse_label_instant_contradiction(temp, arms_meta, labels, rounds)
    return {
        # PROVENANCE, named precisely: the reporter reads a directory and cannot establish
        # which program wrote it. There is deliberately no field here a reader could mistake
        # for a verified property of the session.
        "source": (
            "per-rep <tag>.round artifacts in the session dir (provenance UNVERIFIED — the"
            " reporter reads a directory and cannot establish which program wrote it)"
        ),
        "claims_made": "NONE",
        "claim_note": (
            "this block is RECORDED DATA, not a verdict. The rig makes no interleaving,"
            " round-major-ordering or rotation claim: the earlier claim was DELETED because"
            " it reported a positive verdict at one round having compared nothing (#3272"
            " round 4). Re-adding an OBSERVED drift control is tracked by #3287/#3299."
        ),
        "rounds_recorded": sorted(rounds),
        "arms_per_round_recorded": n_arms,
        "positions_by_round_recorded": {
            str(r): {label: arms_meta[label][r]["position"] for label in labels}
            for r in sorted(rounds)
        },
        "instants_by_round_recorded": {
            str(r): {label: arms_meta[label][r]["monotonic_ns"] for label in labels}
            for r in sorted(rounds)
        },
        # What the integrity check was ABLE to compare, as a count. Not a pass.
        "integrity_checks": integrity,
    }


def refuse_label_instant_contradiction(
    temp: str,
    arms_meta: dict[str, dict[int, dict[str, int]]],
    labels: list[str],
    rounds: set[int],
) -> dict:
    """Refuse an artifact set whose round LABELS contradict its recorded INSTANTS.

    This is an INTEGRITY check over the files, deliberately NOT a claim about the session
    (#3272 round 4). It answers one question: do the recorded `round` labels and the recorded
    `monotonic_ns` instants tell the same story? Two ways they can fail to:

    * two reps sharing an instant — a strictly sequential loop cannot, so the file was
      copied;
    * a rep labelled round `r+1` completing BEFORE a rep labelled round `r` — the labels and
      the clock cannot both be right, so neither can be used to attribute a figure.

    THE SCOPE IS RETURNED AS A COUNT, never as a pass. At one recorded round there are ZERO
    consecutive pairs to compare, so `round_pairs_compared` is 0 and this function has
    established nothing — which is exactly why nothing here returns a `verified` flag and why
    the report prints no ordering claim. The deleted version returned
    `round_major_verified: True` in precisely that case.
    """
    completed = {
        label: {r: arms_meta[label][r]["monotonic_ns"] for r in rounds} for label in labels
    }
    flat = [(label, r, completed[label][r]) for label in labels for r in sorted(rounds)]
    seen: dict[int, tuple[str, int]] = {}
    for label, r, ns in flat:
        if ns in seen:
            other_label, other_r = seen[ns]
            raise Invalid(
                f"the {temp} reps {other_label}@round{other_r} and {label}@round{r} record"
                f" the IDENTICAL completion instant {ns}. A strictly sequential measurement"
                " loop cannot complete two reps at the same nanosecond, so this metadata was"
                " COPIED rather than written by a measurement, and nothing recorded in it can"
                " be attributed to a rep."
            )
        seen[ns] = (label, r)

    ordered = sorted(rounds)
    by_round = {r: [completed[label][r] for label in labels] for r in ordered}
    pairs = 0
    for earlier, later in zip(ordered, ordered[1:]):
        pairs += 1
        last_of_earlier = max(by_round[earlier])
        first_of_later = min(by_round[later])
        if last_of_earlier >= first_of_later:
            late_arm = next(
                label for label in labels if completed[label][earlier] == last_of_earlier
            )
            early_arm = next(
                label for label in labels if completed[label][later] == first_of_later
            )
            raise Invalid(
                f"the {temp} round LABELS CONTRADICT the recorded INSTANTS. {late_arm} is"
                f" labelled round {earlier} and completed at {last_of_earlier} ns, while"
                f" {early_arm} is labelled round {later} and completed EARLIER, at"
                f" {first_of_later} ns. The labels and the clock cannot both describe this"
                " session, so no figure can be attributed to a round: the per-round pairing"
                " would pair reps by a label the artifact's own timestamps refute. Re-run the"
                " session rather than reporting it. (This is an INTEGRITY refusal over the"
                " artifact set — the rig claims no ordering property; see #3287/#3299.)"
            )
    return {
        "duplicate_instant_check": "applied to every recorded rep",
        "reps_examined": len(flat),
        # THE HONEST SCOPE. Zero pairs means zero orderings were compared, which is what a
        # single-round session gives you — and it is recorded rather than converted into a
        # verdict, because a verdict from zero comparisons is the round-4 finding itself.
        "round_pairs_compared": pairs,
        "scope_note": (
            f"{pairs} consecutive round pair(s) were available to compare. This count is NOT"
            " a verdict: at fewer than two recorded rounds it is 0 and nothing about ordering"
            " was — or could be — established. No ordering claim is derived from it anywhere"
            " (#3272 round 4)."
        ),
    }


def recorded_round_metadata_lines(record: dict) -> list[str]:
    """State what the round metadata IS, and state that no claim is made from it.

    The DELETED predecessor (`interleaving_lines`) printed "the reps were INTERLEAVED …
    OBSERVED FROM THE CLOCK … the report verifies that every rep of round r finished before
    any rep of round r+1" — unconditionally, including at one round where nothing was
    compared. These lines make no claim at all: they say what was recorded, that it is inert,
    and where the real control is tracked.
    """
    integrity = record["integrity_checks"]
    return [
        "  * this rig makes NO INTERLEAVING CLAIM and NO ROUND-ORDERING CLAIM. Each rep's"
        " recorded round/position/arms_in_round/monotonic_ns are carried through to"
        " results.json (.recorded_round_metadata) as INERT RECORDED DATA for an operator to"
        " analyse; nothing here derives a property from them."
        f" Rounds recorded: {record['rounds_recorded']},"
        f" {record['arms_per_round_recorded']} arm(s) per round.",
        "    What IS enforced is artifact-set INTEGRITY, which is a statement about the"
        " files and not about the measurement: every arm must cover the same rounds, the"
        " positions within a round must be 1..n exactly once, arms_in_round must match the"
        " arms present, no two reps may share an instant, and the round LABELS must not"
        " contradict the recorded INSTANTS. A session violating any of these is REFUSED."
        f" Consecutive round pairs available to compare: {integrity['round_pairs_compared']}"
        f" (over {integrity['reps_examined']} rep(s)) — a COUNT, not a verdict: at one"
        " recorded round it is 0 and no ordering was compared.",
        "    A same-session interleaved drift control WAS specified for this rig and is NOT"
        " IMPLEMENTED OR ENFORCED here. The earlier claim was deleted because it reported a"
        " positive verdict having compared nothing at the default --reps 1 (#3272 round 4);"
        " re-adding an OBSERVED control on real hardware is tracked by #3287/#3299. Until"
        " then, read the per-round direction count below and treat any cross-arm difference"
        " as UNCONTROLLED for drift: the untouched bare scan moved ~10% in one hour on the"
        " recorded box.",
    ]


def paired_rounds(scan: dict, fl: dict) -> tuple[list[dict], list[str]]:
    """The WITHIN-ROUND bare/flight comparison, paired by the OBSERVED round (#3272 R3).

    Each rep's `round` field is the one `collect_round_meta` read from that rep's own
    `<tag>.round` artifact and cross-checked against its filename — NOT the rep index the
    reporter happened to iterate. That distinction is the finding: pairing by index produces
    a per-round table for a session whose artifacts say something else entirely.

    STATED PLAINLY (#3272 round 4): pairing by the recorded round is NOT evidence that the
    reps were interleaved, and this rig makes no such claim. The pairing is only as
    contemporaneous as the session that produced the artifacts actually was.

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
            f"the bare scan and {fl['arm']} do not cover the same RECORDED rounds"
            f" (scan {sorted(scan_by_round)}, flight {sorted(fl_by_round)}), so no"
            " within-round comparison is possible. The driver records one rep of every arm"
            " per round; differencing medians alone is what measurement-method.md §3b"
            " forbids."
        )
    rounds = []
    for rnd in sorted(scan_by_round):
        s, f = scan_by_round[rnd], fl_by_round[rnd]
        # `positive_derived` on BOTH operands, via the shared validator (#3272 round 3, B2).
        # The pre-fix test was `<= 0`, which admitted `inf`/`nan`: a non-finite rows/s would
        # produce a printable `nanx` ratio and a `flight_meets_target` verdict decided by
        # NaN comparison semantics (always False), i.e. a BELOW-TARGET verdict for a rep that
        # measured nothing. Each collector already refuses these; this is the local
        # statement of the same rule at the point of division.
        positive_derived(
            f"round {rnd} bare rows/s", s["rows_per_sec"], "there is no ratio for that round"
        )
        positive_derived(
            f"round {rnd} flight rows/s", f["rows_per_sec"], "there is no ratio for that round"
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
