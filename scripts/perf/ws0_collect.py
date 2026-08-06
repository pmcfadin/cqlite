#!/usr/bin/env python3
"""Per-arm COLLECTION: one (arm, temperature) block from a session's artifacts (#3272).

Split out of `ws0_report.py` under the campsite rule (source target ~800 lines), along the
same seam the other two modules follow — this file turns ARTIFACTS into a measurement
block, `ws0_validate.py` decides what may be turned into one, `ws0_rounds.py` owns when
each rep ran, and `ws0_report.py` composes the blocks into a report and prints it.

Everything here obeys the one rule the whole rig exists for:

    **A quantity that was not validly OBSERVED is an ERROR, never a fabricated value.**

Which, after review round 2, includes the shape arrived at from the other side: an
accept condition written as `!= <bad>` rather than `== <good>`. `if errors > 0` treated a
NEGATIVE `requests_error` as a clean zero-error measurement, because only the positive
half of "not zero" was tested — the same fabricated-zero defect as the defaulting
`.get("requests_error", 0)` it replaced. Every counter comparison in this file is now
stated as the AFFIRMATIVE value the quantity must have (#3272 R6).
"""

from __future__ import annotations

import json
import pathlib
import statistics

from ws0_rounds import collect_round_meta
from ws0_loadgen_record import check_record_surface  # noqa: F401  (re-exported)
from ws0_scan_record import SCAN_FIXED_INPUTS, check_scan_fixed_inputs
from ws0_validate import (
    Invalid,
    classify_prewarm,
    non_negative_int,
    positive_derived,
    positive_finite_float,
    positive_int,
    read_perf_counters,
    require_complete,
)

# The events every perf leg must carry. Named here so an absent one is reported by
# name rather than defaulting to zero.
REQUIRED_EVENTS = ("cycles", "instructions")

def spread(values: list[float]) -> dict[str, float]:
    """Median + observed spread of a rep series, or `Invalid`.

    The `spread_pct_of_median` divisor used to be written
    `(hi - lo) / med * 100.0 if med else 0.0` — a PERMISSIVE NUMERIC FALLBACK in the
    reporting path (#3272 review). A zero median means every rep of this series
    measured zero rows/s (or zero cycles/row), which is not a series with an
    undefined spread: it is not a measurement at all. Reporting `spread 0.0%` beside
    it would have described the degenerate case as the TIGHTEST possible one — the
    exact inversion of what the number means.
    """
    if not values:
        raise Invalid("a rep series with no values reached spread() — nothing was observed")
    lo, hi = min(values), max(values)
    med = statistics.median(values)
    if med <= 0:
        raise Invalid(
            f"a rep series has a non-positive median ({med}; observed {values}). A zero"
            " median is not a series whose spread is undefined — it is a series that"
            " measured nothing, and `spread 0.0%` would read as the tightest possible"
            " result rather than as the absent one."
        )
    # EVERY member, not only the median (#3272 review round 3, B2). Refusing a non-positive
    # MEDIAN is a different property from refusing a non-positive MEMBER: with three reps,
    # one corrupt value leaves the median positive and is published as `min` — and, if it is
    # the middle value, as the figure itself. This is the last line of defence; each caller
    # validates its own quantities before appending, so reaching here means one slipped.
    for i, v in enumerate(values, start=1):
        positive_derived(
            f"rep {i} of a series reaching spread()", v, f"whole series {values}"
        )
    return {
        "median": med,
        "min": lo,
        "max": hi,
        "spread_abs": hi - lo,
        "spread_pct_of_median": (hi - lo) / med * 100.0,
        "n": len(values),
    }


def read_prewarm(d: pathlib.Path, tag: str) -> str:
    """The prewarm outcome THIS rep recorded, or `unrecorded`.

    Absent file => the driver predates the recording, or the rep died before its
    prewarm. Either way the warm/cold separation is UNVERIFIED for that rep, which
    is reported rather than assumed healthy (issue #3096 review, finding 1).
    """
    p = d / f"{tag}.prewarm.status"
    return p.read_text().strip() if p.exists() else "unrecorded"


def prewarm_block(prewarm: list[dict], temp: str) -> dict:
    """The prewarm record + the single `prewarm_all_ok` field a reader can check.

    `classify_prewarm` is TEMPERATURE-SCOPED (#3272 finding 2): the cold arm's
    `skipped-cold-arm` sentinel can only satisfy a COLD rep, and raises on a warm
    one instead of quietly counting as success.
    """
    return {
        "prewarm": prewarm,
        "prewarm_all_ok": all(
            classify_prewarm(temp, p["status"]) == "ok" for p in prewarm
        ),
        "prewarm_required_status": temp,
    }


def prewarm_warning(block: dict, arm_label: str, temp: str) -> list[str]:
    """The loud summary line for a degraded prewarm. Never swallowed.

    Keyed on `is True`, never on a defaulting `.get(..., True)` (#3272 review): the
    old form defaulted a VERDICT-CARRYING key to the PERMISSIVE value, so a block
    that had lost `prewarm_all_ok` — a future refactor, a hand-edited artifact —
    would have suppressed the warning by ABSENCE. A verdict that was not computed is
    an error, never a pass.
    """
    verdict = block.get("prewarm_all_ok")
    if verdict is not True and verdict is not False:
        raise Invalid(
            f"the {arm_label} block carries no boolean `prewarm_all_ok` (got"
            f" {verdict!r}) — the prewarm verdict was never computed, and an absent"
            " verdict may not be read as a passing one"
        )
    if verdict is True:
        return []
    degraded = [
        p for p in block["prewarm"] if classify_prewarm(temp, p["status"]) != "ok"
    ]
    return [
        f"      !! PREWARM DEGRADED on {arm_label} rep(s) "
        + ", ".join(f"{p['rep']}={p['status']}" for p in degraded)
        + " — this 'warm' figure may be partly cold; the warm/cold separation"
        " (spec R2/AC5) is UNVERIFIED for those reps"
    ]


def check_scan_passes(
    tag: str, payload: dict, scan_passes: int, corpus_rows: int
) -> tuple[int, float, list[dict]]:
    """`(rows, secs, passes)` DERIVED from the per-pass records, or `Invalid` (#3272 F2).

    # The finding

    The bare-scan collector read the aggregate `rows_denominator` and `timed_scan_secs` and
    NEVER LOOKED AT the `passes` array beside them, although `ws0-scan-bench` writes one
    record per timed pass and computes both aggregates from exactly those records
    (`scan_bench.rs`: `rows_denominator` and `scan_secs` are `passes.iter()…sum()`). Three
    things were therefore invisible:

    * a TRUNCATED scan — a rep that ran fewer passes than `--scan-passes` asked for, e.g. a
      bench killed mid-run, reported the aggregate of the passes that DID happen as if it
      were the whole measurement;
    * a MISMATCHED `--scan-passes` between the driver and the reporter — the reporter's own
      `--scan-passes` was recorded in `results.json` and compared against NOTHING;
    * a PASS THAT DID NOT SCAN THE WHOLE CORPUS — each pass is a full-corpus scan by
      construction, so a pass whose rows are not the corpus row count is a partial scan,
      and summing it into the aggregate hid it. This is the bare-scan analogue of the check
      the Flight arm already had (`check_request_count`), which the bare-scan arm lacked
      entirely.

    # The fix: three requirements, and the aggregates are DERIVED

    Exactly `scan_passes` records; every one observing `corpus_rows`; and `rows`/`secs`
    RECOMPUTED from those records rather than read. A derived value cannot be forged — the
    same principle as round 4's derived Flight throughput. The recorded aggregates are still
    read and CROSS-CHECKED against the derived ones, so a payload whose own fields disagree
    is refused rather than silently overridden: which of them is wrong cannot be known, so
    neither is reported.
    """
    passes = payload.get("passes")
    if passes is None:
        raise Invalid(
            f"bare-scan rep {tag} payload carries no `passes` array — the per-pass records"
            " were NOT OBSERVED, so the reported aggregate cannot be checked against the"
            " passes it is a sum of. A truncated scan (fewer passes than --scan-passes) and"
            " a pass that did not scan the whole corpus are both invisible in the aggregate"
            " alone (#3272 F2). Re-run with a ws0-scan-bench that records its passes."
        )
    if not isinstance(passes, list):
        raise Invalid(
            f"bare-scan rep {tag} `passes` is a {type(passes).__name__}, not a list of"
            " per-pass records"
        )
    if len(passes) != scan_passes:
        raise Invalid(
            f"bare-scan rep {tag} recorded {len(passes)} timed pass(es) but --scan-passes"
            f" is {scan_passes}. A rep with fewer passes than requested is not a smaller"
            " measurement — it is a TRUNCATED one, and its aggregate rows/seconds would be"
            " reported as though the whole scan had run. More passes than requested means"
            " the artifact is not the one this reporter models (a stale file, or a driver"
            " and reporter given different --scan-passes). Re-run the rep, or report it"
            " with the --scan-passes it actually ran."
        )
    derived_rows = 0
    derived_secs = 0.0
    records: list[dict] = []
    for i, p in enumerate(passes):
        if not isinstance(p, dict):
            raise Invalid(
                f"bare-scan rep {tag} pass {i} is a {type(p).__name__}, not a record"
            )
        p_rows = positive_int(
            f"bare-scan rep {tag} pass {i} rows",
            p.get("rows"),
            "It is this pass's contribution to the rep's row denominator, so a"
            " non-positive or absent value is refused rather than summed.",
        )
        p_secs = positive_finite_float(
            f"bare-scan rep {tag} pass {i} secs",
            p.get("secs"),
            "It is this pass's contribution to the rep's measurement window, and the"
            " window is the divisor of the reported rows/s.",
        )
        # EVERY pass observed the WHOLE corpus, not just the total. Each pass is a full-corpus
        # scan by construction, so a pass short of the corpus row count is a PARTIAL SCAN —
        # and a partial pass plus a compensating one sums to a plausible aggregate, which is
        # exactly what the aggregate-only check could not see.
        if p_rows != corpus_rows:
            raise Invalid(
                f"bare-scan rep {tag} pass {i} observed {p_rows:,} rows, but the corpus has"
                f" {corpus_rows:,}. Every timed pass is a FULL-corpus scan, so this pass did"
                " not read the whole corpus and the rep's row denominator is not the number"
                " this report would print. Checking only the SUM cannot see this: a short"
                " pass beside a long one adds up to a plausible total (#3272 F2)."
            )
        derived_rows += p_rows
        derived_secs += p_secs
        records.append({"pass": p.get("pass", i), "rows": p_rows, "secs": p_secs})
    # The RECORDED aggregates are cross-checked against the derived ones — same rule
    # `load_corpus_identity` applies to `bytes_per_row`, and `collect_flight` to `rows_per_s`.
    recorded_rows = positive_int(
        f"bare-scan rep {tag} rows_denominator",
        payload.get("rows_denominator"),
        "That is not a measurement: it is the denominator of every figure for this rep, so a"
        " non-positive one is refused rather than divided by.",
    )
    recorded_secs = positive_finite_float(
        f"bare-scan rep {tag} timed_scan_secs",
        payload.get("timed_scan_secs"),
        "There is no rows/s for a measurement window that is zero, negative, or not finite:"
        " the scan either never ran or its timer was never read, and dividing by it would"
        " raise inside the reporting path instead of naming the artifact.",
    )
    if recorded_rows != derived_rows:
        raise Invalid(
            f"bare-scan rep {tag} records rows_denominator={recorded_rows:,} but its own"
            f" {len(records)} pass record(s) sum to {derived_rows:,}. ws0-scan-bench computes"
            " that field from exactly those records, so a payload whose aggregate disagrees"
            " with its passes is not one this reporter models — and which of the two is wrong"
            " cannot be determined, so neither is reported. The reported figure is the DERIVED"
            " one; this check exists because the aggregate used to be trusted with the passes"
            " never read (#3272 F2)."
        )
    if abs(recorded_secs - derived_secs) > max(1e-9, derived_secs * 1e-6):
        raise Invalid(
            f"bare-scan rep {tag} records timed_scan_secs={recorded_secs!r} but its own pass"
            f" record(s) sum to {derived_secs!r}. As with the row count, the bench derives the"
            " aggregate from the passes, so a disagreement means neither can be reported."
        )
    return derived_rows, derived_secs, records


def collect_scan(
    d: pathlib.Path, temp: str, reps: int, scan_passes: int, corpus_rows: int
) -> dict:
    """The bare-scan arm, WITH each rep's observed round/position (#3272 R3).

    The round is read from the rep's own `<tag>.round` artifact and cross-checked against
    the rep index in its filename, so every figure below is attributed to the round it
    was MEASURED in rather than to the index this loop happens to be on.

    `scan_passes` and `corpus_rows` are REQUIRED (#3272 F2): the per-pass records are
    validated against both, and the rep's rows/seconds are DERIVED from them rather than
    read from the aggregate the bench also wrote. See `check_scan_passes`.
    """
    rows_per_sec: list[float] = []
    cycles_per_row: list[float] = []
    ipc: list[float] = []
    rows_total = 0
    setup_cycles_total = 0
    per_rep = []
    missing: list[str] = []
    prewarm: list[dict] = []
    round_meta: dict[int, dict[str, int]] = {}
    for rep in range(1, reps + 1):
        tag = f"scan-{temp}-{rep}"
        payload_path = d / f"{tag}.json"
        if not payload_path.exists():
            # A rep whose artifacts are missing is NOT a smaller sample: it is an
            # incomplete run, and silently `continue`ing it published a median over
            # fewer reps than the caller asked for with only `n=` to say so (issue
            # #3096 review). Fail instead — see require_complete below.
            missing.append(payload_path.name)
            continue
        payload = json.loads(payload_path.read_text())
        # THE FIXED SCAN CONTRACT, BEFORE ANY COUNTER IS CONSUMED (#3272).
        #
        # `check_scan_passes` below hardened the COUNTERS thoroughly and validated NOTHING ABOUT
        # WHAT PRODUCED THEM: `arm`, `surface`, `query` and `fold` were recorded by the bench and
        # read by nobody. So a scan run under a different `--fold` or `--project` satisfied every
        # check below — right pass count, every pass observing exactly the pinned corpus row count,
        # aggregates equal to the derived sums — while measuring materially different work, and was
        # published as the bare-scan arm the whole ratio is DIVIDED BY. Checked FIRST, like
        # `collect_flight`'s equivalent: a record from a different workload is refused FOR THAT
        # rather than for a downstream consequence.
        scan_fixed = check_scan_fixed_inputs(tag, payload)
        # THE ROWS AND SECONDS ARE DERIVED FROM THE PER-PASS RECORDS (#3272 F2), not read
        # from the aggregate. `check_scan_passes` requires exactly `--scan-passes` records,
        # requires EVERY one to have observed the whole corpus, sums them, and cross-checks
        # the recorded aggregates against the sums. Both quantities go through the same shared
        # validators as before (`positive_int` / `positive_finite_float`, which refuse a bool,
        # a fractional value, a junk-bearing string and a non-positive or non-finite one) —
        # per pass now, as well as on the aggregate.
        rows, secs, pass_records = check_scan_passes(
            tag, payload, scan_passes, corpus_rows
        )
        # Both legs' counters must be OBSERVED. `.get("cycles", 0)` used to
        # fabricate a zero here, so a run with no setup artifact at all was
        # reported "SETUP-SUBTRACTED" having subtracted nothing (#3272 finding 4).
        total = read_perf_counters(d / f"perf-{tag}.csv", f"{tag} (full run)", REQUIRED_EVENTS)
        setup = read_perf_counters(
            d / f"perf-{tag}-setup.csv", f"{tag} (setup-only leg)", REQUIRED_EVENTS
        )
        # Setup SUBTRACTED (spec R2). A non-positive result would mean the setup
        # leg somehow cost more than the full run, which is a broken measurement,
        # not a small number — surfaced rather than hidden.
        #
        # BOTH SUBTRACTIONS, which is #3272 review round 3's B2. Round 2 checked `cyc` and
        # not `ins` — the SAME subtraction over the same two artifacts, feeding
        # `ipc.append(ins / cyc)`. A perf CSV recording `instructions,0` for the full run (or
        # a setup leg that counted more instructions than the run) produced a zero or
        # negative IPC that `spread()` published as `ipc.min`, and as the printed `IPC` if it
        # was the median. Checking one arm of a two-arm subtraction is the partial-fix shape
        # this issue keeps finding, so both go through the same validator.
        cyc = positive_derived(
            f"{tag} setup-subtracted cycles",
            total["cycles"] - setup["cycles"],
            f"total={total['cycles']}, setup={setup['cycles']}; re-run",
        )
        ins = positive_derived(
            f"{tag} setup-subtracted instructions",
            total["instructions"] - setup["instructions"],
            f"total={total['instructions']}, setup={setup['instructions']};"
            " IPC = instructions/cycles, so this would publish a non-positive IPC; re-run",
        )
        # This arm's prewarm outcome, recorded by ws0-baseline.sh exactly as the
        # Flight arm's is (issue #3096 review, finding 1): the bare scan is the
        # DENOMINATOR of the 1.3x ratio, so an unprewarmed "warm" rep biases the
        # ratio in the claim's favour and must be visible in the artifact.
        prewarm.append({"rep": rep, "status": read_prewarm(d, tag)})
        meta = collect_round_meta(d, tag, rep)
        round_meta[rep] = meta
        rows_per_sec.append(rows / secs)
        cycles_per_row.append(cyc / rows)
        ipc.append(ins / cyc)
        rows_total += rows
        setup_cycles_total += setup["cycles"]
        per_rep.append(
            {
                "rep": rep,
                "round": meta["round"],
                "position_in_round": meta["position"],
                "arms_in_round": meta["arms_in_round"],
                "rows": rows,
                "secs": secs,
                "rows_per_sec": rows / secs,
                "cycles_total": total["cycles"],
                "cycles_setup": setup["cycles"],
                "cycles_scan": cyc,
                "cycles_per_row": cyc / rows,
                "setup_secs": payload.get("setup_secs"),
                # The per-pass records the rows/seconds above were DERIVED from, carried so a
                # reader can see the aggregate was checked against its parts rather than
                # trusted (#3272 F2), plus the count that was REQUIRED.
                "passes": pass_records,
                "passes_observed": len(pass_records),
                "passes_expected": scan_passes,
                # WHAT THE FIXED SCAN CONTRACT WAS VERIFIED TO BE (#3272), recorded per rep so a
                # later reader can see the arm/surface/query/fold this figure is conditional on
                # rather than taking the block's word for it — the block's `surface` used to be an
                # unconditional literal printed about a field nobody compared.
                #
                # DELIBERATELY NOT named `verified_fixed_inputs`, which is the FLIGHT arm's key for
                # the loadgen's own table. `ws0_assert_fixed_inputs_recorded.py` walks EVERY arm's
                # reps for that key and checks each against `ws0_loadgen_record.FIXED_INPUTS`, so
                # reusing the name here made a scan rep answer for a loadgen contract it does not
                # have — measured: that assert failed on `schema` the instant this landed. Two
                # producers, two tables, two key names.
                "verified_scan_fixed_inputs": scan_fixed,
                "rows_source": (
                    "DERIVED as the sum of the per-pass rows, each of which was required to"
                    " equal the corpus row count; the payload's recorded rows_denominator"
                    " and timed_scan_secs were cross-checked against the derived sums"
                ),
                "prewarm": prewarm[-1]["status"],
            }
        )
    # This (arm, temperature) was SELECTED by the caller — an unselected one is
    # never iterated — so it must be complete (#3272 finding 6).
    require_complete(f"bare scan ({temp})", per_rep, reps, missing)
    return {
        # READ FROM THE VERIFIED CONTRACT, never restated (#3272). These two used to be
        # UNCONDITIONAL LITERALS — the block asserted which arm and which public surface produced
        # the figure while the artifact's own statement of both went unread beside it. They are now
        # the values `check_scan_fixed_inputs` REQUIRED of every rep, so the block cannot claim a
        # surface no rep recorded, and a third spelling of the constant cannot drift from the table.
        "arm": SCAN_FIXED_INPUTS["arm"][0],
        "surface": SCAN_FIXED_INPUTS["surface"][0],
        "temperature": temp,
        "rows_per_sec": spread(rows_per_sec),
        "cycles_per_row": spread(cycles_per_row),
        "ipc": spread(ipc),
        "row_denominator_total": rows_total,
        "round_metadata": round_meta,
        "setup_cycles_subtracted_total": setup_cycles_total,
        # Issue #3096 review, finding 1: the warm bare-scan arm had no untimed
        # prewarm at all, so `prewarm_all_ok` here is the single field a reader can
        # check that this arm's "warm" really was warm.
        **prewarm_block(prewarm, temp),
        "reps": per_rep,
    }
