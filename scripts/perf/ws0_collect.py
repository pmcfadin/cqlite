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

# ===========================================================================
# THE LOAD GENERATOR'S COMPLETE RECORD SURFACE (#3272 review round 5, F4)
# ===========================================================================
# `requests_unavailable` — the loadgen's ADMISSION-SHED counter — was COMPLETELY UNREAD.
# Not defaulted, not mis-validated: never mentioned in the reporting path at all, while its
# sibling `requests_error` had by then been through three rounds of hardening. So a rep in
# which the server shed requests under admission control (`--max-concurrent-scans`, #2420)
# was reported as a clean, failure-free measurement — a DEGRADED run reading as a healthy
# one, and the degradation is precisely the thing a throughput figure must not hide.
#
# That is the SECOND counter found simply unread on this issue (the first was
# `requests_error` itself, defaulted to 0). Fixing this one site would be the same partial
# fix the whole issue keeps finding, so what follows is a CENSUS of the loadgen's ENTIRE
# record surface — every field of `StepRecord` (tools/flight-loadgen/src/record.rs), each
# classified, with a REASON IN CODE at the branch for every field deliberately not consumed.
#
# `RECORD_FIELD_DISPOSITION` is not decorative: `check_record_surface` walks it against the
# record actually present, so a field the loadgen ADDS and this reporter has never
# considered is a REFUSAL rather than a silent omission. An unclassified field cannot become
# a second `requests_unavailable`.
#
# Verified against `StepRecord` at `tools/flight-loadgen/src/record.rs` (19 fields).
RECORD_FIELD_DISPOSITION: dict[str, tuple[str, str]] = {
    # ---- CONSUMED: validated and used to derive a reported figure ----------------
    "rows_total": ("consumed", "the row denominator of every figure for this rep"),
    "duration_s": ("consumed", "the DIVISOR of the derived throughput"),
    "rows_per_s": ("consumed", "cross-checked against the DERIVED rows_total/duration_s"),
    "requests_ok": ("consumed", "the per-temperature request contract (cold == exactly 1)"),
    "requests_error": ("consumed", "required, and the rep is refused unless it is zero"),
    "requests_unavailable": (
        "consumed",
        "required, and the rep is refused unless it is zero: a shed request means the"
        " server was over its admission limit, so the throughput measures a degraded"
        " server (#3272 F4)",
    ),
    # ---- IGNORED, each with the reason recorded HERE, at the branch ---------------
    # A counter is only ever ignored when it CANNOT change the validity of a figure this
    # reporter prints. Anything that could is above.
    "error_codes": (
        "ignored",
        "a BREAKDOWN of requests_error, which must already be ZERO for the rep to be"
        " reported — so this map is empty whenever the rep is accepted, and carries no"
        " information the accept condition has not already used",
    ),
    "qps": (
        "ignored",
        "requests_ok/duration_s — both operands are validated and the rig reports ROWS/s,"
        " never a request rate; a figure this reporter does not print needs no domain",
    ),
    "bytes_total": (
        "ignored",
        "the rig's claims are rows/s and cycles/row (spec R1); no byte-throughput figure is"
        " printed, so this is recorded by the loadgen and not read here",
    ),
    "bytes_per_s": ("ignored", "as bytes_total: no byte-rate figure is reported"),
    "latency_ms": (
        "ignored",
        "per-request percentiles over a single full-corpus scan per rep, which is a"
        " DURATION this rig already reads as duration_s; no latency claim is made",
    ),
    "schema": ("ignored", "the record's own version tag, not a measurement"),
    "round": ("ignored", "the driver's label for the rep; the ROUND METADATA the reporter"
                         " integrity-checks comes from <tag>.round, not from here"),
    "endpoint": ("ignored", "the loopback address; not a measurement"),
    "ts_unix_ms": ("ignored", "wall-clock stamp; the rig's ordering uses monotonic_ns from"
                              " <tag>.round, never a wall clock"),
    "seed": ("ignored", "the loadgen's RNG seed; an INPUT, not a measurement"),
    "step": ("ignored", "the ramp step index; this rig runs exactly ONE step per rep and"
                        " refuses a file carrying more, so the index adds nothing"),
    "target_concurrency": ("ignored", "the requested concurrency (--ramp 1 here); an INPUT"),
    "shape": ("ignored", "the request shape (`full`); an INPUT, fixed by the driver"),
}

# Every counter that must be present AND zero for a rep to be reported.
ZERO_REQUIRED_COUNTERS = ("requests_error", "requests_unavailable")

# WHAT A NON-ZERO VALUE MEANS, per counter — appended to the refusal so the diagnostic names
# the MEASUREMENT rather than only the domain. The two are different failures and an operator
# acts differently on each: an error is a broken request, a shed is a server over its
# admission limit.
_ZERO_COUNTER_MEANING = {
    "requests_error": (
        "A failed request means the rep did not complete the work its row count is divided"
        " by, so the figure is not a measurement of a successful full-corpus scan."
    ),
    "requests_unavailable": (
        "A SHED request means the server refused admission (cqlite-flight's"
        " --max-concurrent-scans, #2420), so this rep measured a server operating at its"
        " admission limit rather than the steady-state scan the report claims. That is a"
        " DEGRADED run, and it was previously INVISIBLE: this counter was not read anywhere"
        " in the reporting path, so a shed rep was reported as failure-free (#3272 F4)."
        " Lower the concurrency, raise the server's limit, or report this as a shed run."
    ),
}

# The census and the zero-required list are the same fact written twice, so they are checked
# against each other AT IMPORT rather than left to drift. A counter added to one and not the
# other would otherwise be exactly the kind of half-wired guard this issue keeps finding.
for _k in ZERO_REQUIRED_COUNTERS:
    if RECORD_FIELD_DISPOSITION.get(_k, ("", ""))[0] != "consumed":
        raise Invalid(
            f"{_k} is required to be zero but is not classified as CONSUMED in"
            " RECORD_FIELD_DISPOSITION — the census and the accept rule disagree"
        )
    if _k not in _ZERO_COUNTER_MEANING:
        raise Invalid(f"{_k} must carry a stated MEANING for its non-zero refusal")
del _k


def check_record_surface(tag: str, rec: dict) -> None:
    """Refuse a step record carrying a field this reporter has never CLASSIFIED (#3272 F4).

    The mechanism that keeps `requests_unavailable` from happening a third time. A new
    loadgen counter arrives as an unclassified key, and an unclassified key is refused —
    so the decision to consume or ignore it is FORCED, in `RECORD_FIELD_DISPOSITION`, where
    the reason is recorded beside the choice.

    Deliberately NOT a check that every classified field is PRESENT: this reporter models
    one schema version, and an OLDER record legitimately lacks a field added later. What it
    must never do is silently skip a field that EXISTS and nobody has considered. The fields
    it actually depends on are required INDIVIDUALLY, by name, at their point of use.
    """
    unknown = sorted(k for k in rec if k not in RECORD_FIELD_DISPOSITION)
    if unknown:
        raise Invalid(
            f"flight rep {tag} step record carries field(s) this reporter has never"
            f" classified: {', '.join(unknown)}."
            " Every field of the load generator's record must be recorded in"
            " RECORD_FIELD_DISPOSITION as either CONSUMED or IGNORED-with-a-reason, because"
            " a counter nobody classified is a counter nobody reads: `requests_unavailable`"
            " (admission shed) went COMPLETELY UNREAD, so a rep whose requests were shed"
            " was reported as failure-free (#3272 F4). Classify it — and if it is a counter"
            " that can invalidate a figure, VALIDATE it rather than ignoring it."
        )


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


def collect_scan(d: pathlib.Path, temp: str, reps: int) -> dict:
    """The bare-scan arm, WITH each rep's observed round/position (#3272 R3).

    The round is read from the rep's own `<tag>.round` artifact and cross-checked against
    the rep index in its filename, so every figure below is attributed to the round it
    was MEASURED in rather than to the index this loop happens to be on.
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
        # THE SHARED VALIDATOR, not a bare `int()`/`float()` plus a local range test
        # (#3272 review round 3, B2/B5). `positive_int` refuses a bool, a fractional value
        # (`int(0.9)` was 0 — a fabricated zero arrived at by truncation), a junk-bearing
        # string, and `<= 0` — the row count is the DENOMINATOR of every figure this rep
        # produces, and a negative one used to sail past an `== 0` test to become a negative
        # rows/s and a negative cycles/row, printed and plausible-looking.
        rows = positive_int(
            f"bare-scan rep {tag} rows_denominator",
            payload["rows_denominator"],
            "That is not a measurement: it is the denominator of every figure for this"
            " rep, so a non-positive one is refused rather than divided by.",
        )
        # A DEGENERATE window is a named refusal, not a traceback (#3272 review round 2
        # nit). `rows / secs` with `timed_scan_secs: 0.0` raised `ZeroDivisionError` and
        # exited 1 with a Python traceback — a traceback names the DIVISION rather than the
        # artifact. `inf`/`nan` are refused for the same reason: they would propagate into
        # rows/s and into `spread()` as printable numbers standing in for an absent one.
        secs = positive_finite_float(
            f"bare-scan rep {tag} timed_scan_secs",
            payload["timed_scan_secs"],
            "There is no rows/s for a measurement window that is zero, negative, or not"
            " finite: the scan either never ran or its timer was never read, and dividing"
            " by it would raise inside the reporting path instead of naming the artifact.",
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
                "prewarm": prewarm[-1]["status"],
            }
        )
    # This (arm, temperature) was SELECTED by the caller — an unselected one is
    # never iterated — so it must be complete (#3272 finding 6).
    require_complete(f"bare scan ({temp})", per_rep, reps, missing)
    return {
        "arm": "bare_scan",
        "surface": "cqlite_core::Database::execute_streaming",
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


def expected_requests(temp: str) -> str:
    """The request count a rep of this temperature MUST have, stated not implied.

    Issue #3096 review, finding 2: this used to be reconstructible only by dividing
    a rep's `rows` by the corpus row count in your head. Recorded explicitly here so
    a reader sees the contract, and asserted per rep by `check_request_count`.
    """
    if temp == "cold":
        return "exactly 1 (only the first request after the cache drop is cold)"
    return ">=1, each one a full corpus scan"


def check_request_count(
    tag: str, temp: str, requests_ok: object, rows: int, corpus_rows: int
) -> int:
    """Assert the per-temperature request contract, or raise.

    Two properties, both fail-closed (issue #3096 review, finding 2):

    * A **cold** rep must have completed **exactly one** successful request. The
      driver keeps `--cold-step-duration` short precisely so the loadgen issues one
      request, but that is a duration heuristic against an unknown scan time — if
      the corpus finishes inside the step, requests 2..N read the pages request 1
      faulted in and their WARM rows land in a figure labelled "cold". A caller can
      also trigger it directly by raising the option. So the OBSERVED count is
      checked, not the duration.
    * Every rep's rows must be an exact multiple of the corpus row count, i.e.
      `rows == requests_ok * corpus_rows`. A remainder means some request did not
      scan the whole corpus, so the per-request row denominator is not what the
      report says it is.

    `corpus_rows` is a REQUIRED int, never `None`: an absent corpus identity used to
    disable the second property silently while the NOTES claimed it ran (#3272
    finding 1), so the identity is now loaded fail-closed by
    `ws0_validate.load_corpus_identity` before any of this runs.
    """
    if requests_ok is None:
        raise Invalid(
            f"flight rep {tag} step record carries no `requests_ok` — the"
            " per-temperature request contract cannot be verified, and an unverified"
            " cold rep is exactly how warm requests get reported as cold"
        )
    # `positive_int`, never a bare `int()` (#3272 review round 3, B5). `int(1.9)` is 1, so
    # `requests_ok: 1.9` SATISFIED the exactly-one-cold-request guard below — the guard of
    # #3096 finding 2 defeated by a truncation — and `requests_ok: true` became 1 the same
    # way. Both are now named refusals.
    count = positive_int(
        f"flight rep {tag} requests_ok",
        requests_ok,
        f"flight rep {tag} completed no successful requests, so there is nothing to"
        " report for it.",
    )
    if temp == "cold" and count != 1:
        raise Invalid(
            f"flight COLD rep {tag} completed {count} successful requests;"
            f" expected {expected_requests('cold')}."
            " Only the FIRST request after the cache drop is cold: requests 2..N read the"
            " pages request 1 faulted in, so their WARM rows would be blended into a figure"
            " reported as 'cold', which spec R2/AC5 forbids."
            " Lower --cold-step-duration so the loadgen issues a single request, or measure"
            " this as a warm rep."
        )
    if rows % corpus_rows != 0 or rows // corpus_rows != count:
        raise Invalid(
            f"flight rep {tag} observed {rows:,} rows over {count} successful"
            f" request(s), which is not {count} x the corpus row count"
            f" ({corpus_rows:,}). At least one request did not scan the whole corpus,"
            " so the per-request row denominator is not the one this report would"
            " print. Re-run rather than reporting a partial scan."
        )
    return count


def collect_flight(
    d: pathlib.Path, temp: str, arm: str, reps: int, corpus_rows: int
) -> dict:
    rows_per_sec: list[float] = []
    cycles_per_row: list[float] = []
    ipc: list[float] = []
    rows_total = 0
    per_rep = []
    missing: list[str] = []
    prewarm: list[dict] = []
    round_meta: dict[int, dict[str, int]] = {}
    for rep in range(1, reps + 1):
        tag = f"flight-{arm}-{temp}-{rep}"
        jsonl = d / f"{tag}.jsonl"
        if not jsonl.exists():
            missing.append(jsonl.name)
            continue
        records = [json.loads(x) for x in jsonl.read_text().splitlines() if x.strip()]
        if not records:
            raise Invalid(f"flight rep {tag} produced no step record")
        # EXACTLY ONE step record per rep, or refuse (#3272 review). This used to be
        # `rec = records[-1]`, which SILENTLY DROPPED every earlier record: the driver
        # runs one `--ramp 1` step per rep, so a second line means the artifact is not
        # the one this reporter models (a loadgen that ramped, a rep whose file was
        # appended to by a prior run, two reps sharing an --out path). Reporting the
        # last line alone would publish ONE step's rows as the rep's whole measurement
        # while the others existed on disk, unread and unmentioned.
        if len(records) != 1:
            raise Invalid(
                f"flight rep {tag} carries {len(records)} step records; this rig runs"
                " exactly ONE step per rep (--ramp 1), and reporting only the last"
                " would silently drop the others. Rounds present:"
                f" {[r.get('round') for r in records]}."
                " Re-run the rep into a fresh --out directory rather than reporting a"
                " subset of what was measured."
            )
        rec = records[0]
        # `positive_int`, the shared validator (#3272 review round 3, B2/B5): this is the
        # denominator of this rep's cycles/row and the numerator of its full-corpus check, so
        # it must be a real positive integer — not a bool, not a fractional value a bare
        # `int()` would truncate, not a negative one an `== 0` test would miss.
        rows = positive_int(
            f"flight rep {tag} rows_total",
            rec["rows_total"],
            "That is not a measurement: it is the denominator of this rep's cycles/row and"
            " the numerator of its full-corpus check, so a non-positive one is refused,"
            " not divided by.",
        )
        # NO UNCLASSIFIED FIELD (#3272 F4): a counter nobody has considered is refused,
        # which is what stops a third `requests_unavailable`. See the census above.
        check_record_surface(tag, rec)
        # EVERY ZERO-REQUIRED COUNTER, in ONE loop over `ZERO_REQUIRED_COUNTERS` (#3272 F4).
        #
        # This was written for `requests_error` alone, and its admission-shed sibling
        # `requests_unavailable` was COMPLETELY UNREAD — so a rep whose requests the server
        # SHED under admission control (#2420) was reported as a clean, failure-free
        # measurement. Validating the two by a shared rule rather than at two sites is the
        # point: a counter added to `ZERO_REQUIRED_COUNTERS` is validated identically, and
        # cannot be the one somebody hardened three times while its sibling went unread.
        #
        # Each counter is:
        #   * REQUIRED — absent is an ERROR, never a fabricated 0. `int(rec.get(k, 0))` used
        #     to default `requests_error`, so a record with no such key was reported CLEAN
        #     with the failed-request count never measured (#3272 AC3);
        #   * `non_negative_int` — which closes three defects at once rather than one each: a
        #     NEGATIVE value (round 2's `if errors > 0` read -3 as "no failed requests"), a
        #     FRACTIONAL one (round 3's B5 — a bare `int()` read 0.9 as a clean 0), and a
        #     BOOLEAN (`int(True)` is 1);
        #   * required to be EXACTLY ZERO, stated as the AFFIRMATIVE value the counter must
        #     have rather than as `> 0`.
        #
        # The domain sentence is attached by RE-RAISING rather than passed as a `why=`
        # argument, and that is not stylistic: the banned-idiom scan in
        # `test_ws0_fabrication_guards.sh` blanks string constants reachable from a `raise`
        # (prose necessarily quotes what it refuses) and deliberately leaves ARGUMENT
        # literals alone — blanking those made the whole scan vacuous, which was a real
        # defect of an earlier round. Prose that quotes the idiom therefore belongs in a
        # `raise`, where the scan can see it is prose.
        counters_zero: dict[str, int] = {}
        for key in ZERO_REQUIRED_COUNTERS:
            observed = rec.get(key)
            if observed is None:
                raise Invalid(
                    f"flight rep {tag} step record carries no `{key}` — that count was NOT"
                    f" OBSERVED, so a report cannot assert it was zero. A counter that was"
                    " not observed is an error, never a fabricated 0 (#3272 AC3)."
                    f" {_ZERO_COUNTER_MEANING[key]}"
                )
            try:
                observed = non_negative_int(f"flight rep {tag} {key}", observed)
            except Invalid as exc:
                raise Invalid(
                    f"{exc} A negative counter is a CORRUPT artifact, not a clean zero: the"
                    " check used to be `if errors > 0`, so -3 passed as 'no failed requests'"
                    f" (#3272 R6). An unparseable `{key}` is refused for the same reason — a"
                    " counter that was not validly observed is an error, never a 0 (AC3)."
                ) from None
            if observed != 0:
                # `requests_error` keeps its ORIGINAL wording ("had N failed request(s)").
                # Not cosmetic: that phrasing is what `test_ws0_fabrication_guards.sh`
                # asserts on, and rewording it while generalising the loop would have
                # SILENTLY WEAKENED an existing observed guard into one whose test no longer
                # matched its diagnostic. A refactor may not quietly relabel a refusal a test
                # is pinned to.
                headline = (
                    f"flight rep {tag} had {observed} failed request(s)"
                    if key == "requests_error"
                    else f"flight rep {tag} recorded {key}={observed}, which must be 0."
                )
                raise Invalid(f"{headline} {_ZERO_COUNTER_MEANING[key]}")
            counters_zero[key] = observed
        errors = counters_zero["requests_error"]
        requests_ok = check_request_count(tag, temp, rec.get("requests_ok"), rows, corpus_rows)
        # The prewarm outcome for THIS rep, recorded by ws0-baseline.sh.
        prewarm.append({"rep": rep, "status": read_prewarm(d, tag)})
        # ...and its OBSERVED round + position within that round (#3272 R3).
        meta = collect_round_meta(d, tag, rep)
        round_meta[rep] = meta
        counters = read_perf_counters(d / f"perf-{tag}.csv", tag, REQUIRED_EVENTS)
        # BOTH counters, not only `cycles` (#3272 review round 3, B2). This arm has no setup
        # leg to subtract, so the perf values ARE the derived quantities — and round 2
        # checked `cyc <= 0` while `ins` went straight into `ipc.append(ins / cyc)`. A perf
        # CSV recording `instructions,0` therefore published a ZERO IPC: `spread()` refuses
        # only a non-positive MEDIAN, so one such rep among three survived as `ipc.min`, and
        # as the printed `IPC` if it was the median.
        cyc = positive_derived(
            f"flight rep {tag} cycles", counters["cycles"], "the perf -C window was empty"
        )
        ins = positive_derived(
            f"flight rep {tag} instructions",
            counters["instructions"],
            "IPC = instructions/cycles, so a zero here publishes a zero IPC",
        )
        # THE THROUGHPUT IS DERIVED, NOT TRUSTED (#3272 review round 4). It used to be read
        # straight from `rec["rows_per_s"]` while `duration_s` went completely unvalidated —
        # so a record with plausible rows/request counters and an ARBITRARY throughput
        # produced a successful report, and the reported figure was the one field nothing
        # cross-checked. The loadgen's own invariant is
        # `rows_per_s == rows_total / duration_s` (tools/flight-loadgen/src/record.rs:150,
        # `per_s(self.rows_total)`), so the rate is RECOMPUTED from the two counters that ARE
        # checked. A derived value cannot be forged.
        #
        # `duration_s` is therefore now a REQUIRED, positive, finite quantity in its own
        # right — it is the divisor of the reported figure. An absent one used to reach
        # `results.json` as a `None` via `rec.get("duration_s")`.
        secs = positive_finite_float(
            f"flight rep {tag} duration_s",
            rec.get("duration_s"),
            "It is the DIVISOR of this rep's throughput, which is now DERIVED rather than"
            " read from the artifact: an unvalidated duration beside a trusted rate meant"
            " an arbitrary rows_per_s produced a successful report (#3272 round 4).",
        )
        arm_rps = positive_derived(
            f"flight rep {tag} rows/s (DERIVED as rows_total/duration_s)",
            rows / secs,
            f"rows_total={rows}, duration_s={secs}",
        )
        # The RECORDED rate is still read — and CROSS-CHECKED against the derived one, so a
        # record whose own fields disagree is refused rather than silently overridden. Which
        # of the two is wrong cannot be known, so neither is reported: that is the same rule
        # `load_corpus_identity` applies to `bytes_per_row` vs `data_db_bytes/rows`.
        recorded_rps = positive_finite_float(
            f"flight rep {tag} rows_per_s (recorded)",
            rec.get("rows_per_s"),
            "That is not a positive finite rate. It is cross-checked against the DERIVED"
            " rows_total/duration_s; both must be present for the check to mean anything.",
        )
        # A relative tolerance, because both sides are floats the producer wrote and read
        # back through JSON. 1e-6 is far tighter than any real divergence and far looser than
        # float round-trip noise.
        if abs(recorded_rps - arm_rps) > max(1e-9, arm_rps * 1e-6):
            raise Invalid(
                f"flight rep {tag} records rows_per_s={recorded_rps!r} but its own counters"
                f" give rows_total/duration_s = {rows}/{secs} = {arm_rps!r}. The load"
                " generator computes the rate from exactly those two fields"
                " (flight-loadgen record.rs, `per_s(self.rows_total)`), so a record whose"
                " rate disagrees with its counters is not one this reporter models — and"
                " which of the three fields is wrong cannot be determined, so none of them"
                " is reported. The reported figure is the DERIVED one; this check exists"
                " because trusting the recorded rate while never validating duration_s let"
                " an arbitrary throughput produce a successful report (#3272 round 4)."
            )
        rows_per_sec.append(arm_rps)
        cycles_per_row.append(cyc / rows)
        ipc.append(ins / cyc)
        rows_total += rows
        per_rep.append(
            {
                "rep": rep,
                "round": meta["round"],
                "position_in_round": meta["position"],
                "arms_in_round": meta["arms_in_round"],
                "rows": rows,
                "requests_ok": requests_ok,
                "requests_expected": expected_requests(temp),
                "rows_per_scan_observed": rows / requests_ok,
                "rows_per_scan_expected": corpus_rows,
                # Validated, never `rec.get(...)` — it is the DIVISOR of the figure above.
                "duration_s": secs,
                # DERIVED from rows_total/duration_s (#3272 round 4), with the recorded value
                # kept beside it so a reader can see the two agreed.
                "rows_per_sec": arm_rps,
                "rows_per_sec_recorded": recorded_rps,
                "rows_per_sec_source": (
                    "DERIVED as rows_total/duration_s; the artifact's recorded rows_per_s was"
                    " cross-checked against it and agreed within 1e-6 relative"
                ),
                "cycles": cyc,
                "cycles_per_row": cyc / rows,
                "prewarm": prewarm[-1]["status"],
            }
        )
    require_complete(f"flight do_get {arm} ({temp})", per_rep, reps, missing)
    return {
        "arm": f"flight_do_get_{arm}",
        "surface": "arrow_flight FlightService::do_get (loopback gRPC)",
        "temperature": temp,
        "forced_merge_path": arm,
        "rows_per_sec": spread(rows_per_sec),
        "cycles_per_row": spread(cycles_per_row),
        "ipc": spread(ipc),
        "row_denominator_total": rows_total,
        # Issue #3096 review, finding 2: the request count each temperature MUST
        # have, asserted per rep by `check_request_count` and stated here so no
        # reader has to reconstruct it from row denominators.
        "requests_expected_per_rep": expected_requests(temp),
        "round_metadata": round_meta,
        # Unconditionally true now: the corpus identity is REQUIRED, so this can
        # never be a report that skipped the check while claiming it (#3272 f1).
        "full_corpus_per_request_verified": True,
        "corpus_rows_used_for_verification": corpus_rows,
        "setup_cycles_subtracted_total": 0,
        "setup_note": (
            "server start + (warm only) prewarm happen BEFORE the perf window opens, "
            "so setup is outside the window by construction rather than subtracted"
        ),
        # Issue #3096 review: a failed prewarm silently degraded a "warm" claim.
        # Every rep's outcome is recorded here, and `prewarm_all_ok` is the single
        # field a reader can check.
        **prewarm_block(prewarm, temp),
        "reps": per_rep,
    }
