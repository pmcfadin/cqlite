#!/usr/bin/env python3
"""The load generator's RECORD SURFACE: what the reporter consumes, ignores, and why.

Split out of `ws0_collect.py` under the campsite rule when #3272's F4 fix pushed that file
past the ~800-line source target. The seam is by RESPONSIBILITY, and this is a distinct one:
`ws0_collect.py` turns artifacts into a measurement block, while this module answers a prior
question — WHICH FIELDS OF THE PRODUCER'S RECORD does the reporting path account for at all?

That question needed its own home because getting it wrong is how #3272's F4 happened:
`requests_unavailable` (the admission-shed counter, #2420) was not defaulted and not
mis-validated but NEVER MENTIONED, so a rep measured against a server at its admission limit
was reported as a clean, failure-free scan. It was the SECOND counter found simply unread.

So the census below is a MECHANISM, not documentation: `check_record_surface` refuses a record
carrying a field nobody classified, which forces the consume-or-ignore decision to be made —
here, beside the reason. A new loadgen counter cannot become a third `requests_unavailable`.

The subject is `StepRecord` in `tools/flight-loadgen/src/record.rs`, and the census is checked
against that LIVE struct by `scripts/tests/test_ws0_fabrication_guards.sh`, so it asserts
against the real producer rather than agreeing with itself.
"""

from __future__ import annotations

from ws0_validate import Invalid

# ============================================================================
# THE FIXED INPUTS — VERIFIED, NOT IGNORED (#3272 review round 11, F3)
# ============================================================================
# The census below started with five counters CONSUMED and everything else IGNORED, and
# `target_concurrency` was in the ignored half with the reason "the requested concurrency
# (--ramp 1 here); an INPUT". That reason is true and the disposition it justified was wrong,
# because of what the census's own rule says: a field may be ignored only when it CANNOT change
# the validity of a figure this reporter prints.
#
# `target_concurrency` changes it completely. The driver passes `--ramp 1`, so this rig's entire
# claim is a CONCURRENCY-ONE baseline: one request in flight, one full-corpus scan per rep, the
# `requests_ok == 1` contract for a cold rep, and a `cycles/row` figure whose perf window contains
# one scan's work. A record produced at `--ramp 8` satisfies EVERY existing check — the row count
# is still `requests_ok x corpus_rows`, the errors and sheds are still zero, the derived throughput
# still equals `rows_total/duration_s` — and it measures a MATERIALLY DIFFERENT WORKLOAD (eight
# concurrent scans contending for the same pinned core) which the report then publishes as the
# intended baseline. Nothing in the output would say so.
#
# The same argument applies to the other three inputs the driver FIXES, so they are verified
# together rather than one at a time — the `ZERO_REQUIRED_COUNTERS` posture, one rule for a class:
#
#   * `shape` — the driver passes `--shape full` (a full-ring SELECT *). A record produced at a
#     different shape measured a different QUERY, which is the M1 substitution one layer in.
#   * `step` — the driver runs exactly ONE step per rep, so the only legal index is 0. A non-zero
#     index means the record is step N of a ramp: `ws0_flight_arm` already refuses a FILE carrying
#     more than one record, and this refuses a file carrying the WRONG ONE (a single record salvaged
#     from a multi-step ramp, which that count check cannot see).
#   * `schema` — the record's own version tag. Previously ignored as "not a measurement", which
#     mistakes what a version tag is FOR: it is the statement that the field names below mean what
#     this reporter thinks they mean. A future `step/v2` that redefined `rows_total` would be read
#     with v1 semantics and silently mis-reported.
#
# `round` stays REQUIRED-but-not-compared, and the reason is recorded at its branch below rather
# than here, because it is the one field of the four whose authority lives elsewhere.
#
# Values are stated ONCE, here, as data. `ws0_flight_arm` reads them; the driver's actual argv is
# asserted against them by `test_ws0_fabrication_guards.sh`, so a driver that changed `--ramp` and
# a reporter that still demanded 1 cannot pass each other by.
FIXED_INPUTS: dict[str, tuple[object, str]] = {
    "target_concurrency": (
        1,
        "the driver passes `--ramp 1`, so this rig's whole claim is a CONCURRENCY-ONE baseline."
        " A record produced at a higher concurrency satisfies every row, request, error and shed"
        " check while measuring a materially different workload — N concurrent scans contending"
        " for the same pinned physical core — and it would be reported as the intended baseline"
        " (#3272 F3)",
    ),
    "shape": (
        "full",
        "the driver passes `--shape full` (the full-ring SELECT *). A different shape measured a"
        " DIFFERENT QUERY, which is the request-substitution #3272 M1 pinned one layer out",
    ),
    "step": (
        0,
        "this rig runs exactly ONE step per rep, so the only legal step index is 0. A non-zero"
        " index means this record is step N of a RAMP: `ws0_flight_arm` refuses a FILE holding"
        " more than one record, and this refuses a file holding the WRONG ONE — a single record"
        " salvaged from a multi-step ramp, which the count check cannot see",
    ),
    "schema": (
        "flight-loadgen.step/v1",
        "the record's own SCHEMA VERSION: the statement that the field names this reporter reads"
        " mean what it thinks they mean. A future `step/v2` that redefined `rows_total` or"
        " `duration_s` would otherwise be read with v1 semantics and silently mis-reported. Kept"
        " in sync with `SCHEMA_TAG` in tools/flight-loadgen/src/record.rs, which"
        " test_ws0_fabrication_guards.sh asserts against the live constant",
    ),
}

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
    # ---- VERIFIED FIXED INPUTS (#3272 F3) — see FIXED_INPUTS above for each reason ----
    # Not "consumed" (they produce no figure) and no longer "ignored" (they decide whether the
    # figures MEAN what the report says). A third disposition, because collapsing them into
    # either of the other two would misstate what is checked.
    "schema": ("verified-fixed-input", FIXED_INPUTS["schema"][1]),
    "target_concurrency": ("verified-fixed-input", FIXED_INPUTS["target_concurrency"][1]),
    "shape": ("verified-fixed-input", FIXED_INPUTS["shape"][1]),
    "step": ("verified-fixed-input", FIXED_INPUTS["step"][1]),
    # `round` is REQUIRED PRESENT and deliberately NOT compared to a value, which is a real
    # distinction rather than a softer version of the four above: its authority lives elsewhere.
    # The reporter's round bookkeeping comes from the driver's `<tag>.round` artifact, which
    # `ws0_rounds.collect_round_meta` integrity-checks across arms; comparing this copy to a tag
    # spelling would be a SECOND source of truth for one fact, and the two could disagree. So it
    # is checked for PRESENCE (an absent label means the record cannot be attributed to a rep at
    # all) and then left to the artifact that owns it.
    "round": ("required-present", "the driver's label for the rep. NOT compared to a value: the"
                                  " ROUND METADATA the reporter integrity-checks comes from"
                                  " <tag>.round, and a second comparison here would be a second"
                                  " source of truth for one fact"),
    "endpoint": ("ignored", "the loopback address; not a measurement"),
    "ts_unix_ms": ("ignored", "wall-clock stamp; the rig's ordering uses monotonic_ns from"
                              " <tag>.round, never a wall clock"),
    "seed": ("ignored", "the loadgen's RNG seed; an INPUT, not a measurement"),
    # `step`, `target_concurrency` and `shape` WERE HERE, as `ignored`. #3272's F3 moved them up to
    # `verified-fixed-input`. They are not left behind as duplicate keys, and that is not tidiness:
    # a repeated key in a python dict literal SILENTLY WINS, so a stale `ignored` entry below the
    # new one would have reverted all three dispositions while the new lines sat above reading as
    # the fix. The import-time closure check below is what catches such a reversion generally.
}

# THE DISPOSITIONS, as a closed set. An entry classified with anything else is refused AT IMPORT
# rather than reaching `check_record_surface`, where an unrecognised disposition would fall through
# every branch and behave exactly like `ignored` — a field silently unread while the census claimed
# otherwise, which is the `requests_unavailable` defect wearing the census's own clothes.
DISPOSITIONS = ("consumed", "verified-fixed-input", "required-present", "ignored")
for _f, (_d, _why) in RECORD_FIELD_DISPOSITION.items():
    if _d not in DISPOSITIONS:
        raise Invalid(
            f"{_f} is classified {_d!r}, which is not one of {DISPOSITIONS}. An unrecognised"
            " disposition would fall through every branch of check_record_surface and behave"
            " exactly like `ignored` — a field nobody reads under a census that claims coverage."
        )
    if not _why.strip():
        raise Invalid(f"{_f} carries no REASON; the census's whole value is the reason at the branch")
del _f, _d, _why

# ...and `FIXED_INPUTS` and the census are the same fact written twice, so they are checked against
# each other at import — the `ZERO_REQUIRED_COUNTERS` pattern. A field given an expected value but
# left classified `ignored` (or vice versa) is exactly the half-wired guard this issue keeps finding.
for _k in FIXED_INPUTS:
    if RECORD_FIELD_DISPOSITION.get(_k, ("", ""))[0] != "verified-fixed-input":
        raise Invalid(
            f"{_k} has an expected FIXED value but is not classified `verified-fixed-input` in"
            " RECORD_FIELD_DISPOSITION — the census and the verification disagree"
        )
for _k, (_d, _) in RECORD_FIELD_DISPOSITION.items():
    if _d == "verified-fixed-input" and _k not in FIXED_INPUTS:
        raise Invalid(
            f"{_k} is classified `verified-fixed-input` but FIXED_INPUTS states no value for it, so"
            " nothing would be verified — the classification would be a claim about no check"
        )
del _k, _d

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
            " RECORD_FIELD_DISPOSITION as CONSUMED, VERIFIED-FIXED-INPUT, REQUIRED-PRESENT or"
            " IGNORED-with-a-reason, because"
            " a counter nobody classified is a counter nobody reads: `requests_unavailable`"
            " (admission shed) went COMPLETELY UNREAD, so a rep whose requests were shed"
            " was reported as failure-free (#3272 F4). Classify it — and if it is a counter"
            " that can invalidate a figure, VALIDATE it rather than ignoring it."
        )


def check_fixed_inputs(tag: str, rec: dict) -> dict:
    """REQUIRE the inputs the driver FIXES to hold the values it fixed them to (#3272 F3).

    `target_concurrency` was classified IGNORED, so a record produced at `--ramp 8` satisfied every
    row, request, error and shed check and was reported as this rig's concurrency-one baseline while
    measuring N concurrent scans contending for one pinned physical core. `shape`, `step` and the
    `schema` version tag were ignored for the same "it is only an INPUT" reason and carry the same
    exposure — a different query, a record salvaged from step N of a ramp, and v2 field semantics
    read as v1.

    Each is REQUIRED PRESENT and compared EXACTLY. Absent is an ERROR rather than an assumed
    default, for the reason every counter here is: a value that was not observed cannot be asserted,
    and `rec.get(k, <the value we want>)` would make the check pass precisely when the record is
    silent about it.

    Compared with `==` after a TYPE check, never with a coercion: `int(True)` is 1 and `int(1.9)` is
    1, so a bare `int(...) == 1` would accept `target_concurrency: true` and `1.9` — the truncation
    defect #3272 R6/B5 found twice elsewhere in this file's reading path.

    Returns the verified values, so a caller can record WHAT was checked rather than merely that
    something was.
    """
    verified: dict[str, object] = {}
    for key, (want, why) in FIXED_INPUTS.items():
        if key not in rec:
            raise Invalid(
                f"flight rep {tag} step record carries no `{key}`, so the value this rig FIXES it"
                f" to ({want!r}) was NOT OBSERVED and cannot be asserted. A missing input is an"
                " error, never an assumed default — defaulting it would make the check pass exactly"
                f" when the record is silent (#3272 F3). {why}"
            )
        got = rec[key]
        # A bool is not an int here: `True == 1` in python, so `target_concurrency: true` would
        # otherwise satisfy the concurrency-one contract.
        if isinstance(got, bool) or type(got) is not type(want) or got != want:
            raise Invalid(
                f"flight rep {tag} recorded `{key}` = {got!r}, but this rig requires {want!r}."
                f" {why}. Re-run the rep with the driver, which fixes this input; do not report a"
                " record produced under different conditions as a result for these."
            )
        verified[key] = got
    # `round` is REQUIRED PRESENT and not compared — see its census entry for why the comparison
    # belongs to the `<tag>.round` artifact rather than to a second copy here.
    for key, (disp, why) in RECORD_FIELD_DISPOSITION.items():
        if disp != "required-present":
            continue
        if key not in rec:
            raise Invalid(
                f"flight rep {tag} step record carries no `{key}`, so this record cannot be"
                f" attributed to a rep at all. {why}"
            )
    return verified


