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


