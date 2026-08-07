#!/usr/bin/env python3
"""The ERROR-CODE BREAKDOWN subject: does the record's own account of its failures agree?

Split out of `ws0_loadgen_record.py` under the campsite rule (source target ~800 lines) when
#3272's round-20 fix took that file to 861. The seam is by RESPONSIBILITY, and it is the seam
`ws0_content_volume.py` already established: `ws0_loadgen_record.py` is the CENSUS — a table of
every field of the producer's record, walked against the record actually present — while this
module answers a single question about ONE of those fields, `error_codes`: does the per-code
breakdown ACCOUNT FOR the failed-request count beside it?

That question earns its own home for the same reason the content-volume one did: it is the only
subject on this path whose expectation comes from ANOTHER FIELD OF THE SAME RECORD (rather than
from a constant, the session's configuration, or a separate measurement), and its checker carries
the domain rules for a MAP — a shape no other field on this path has.

Nothing about the split changes what is checked. The census still classifies `error_codes`
`consumed` with this module's stated reason, still registers `CROSS_CHECKED_COUNTERS` in
`_CROSS_CHECK_TABLES`, and still names `check_error_code_breakdown` in `_CROSS_CHECKERS` — all
of those closures are asserted AT IMPORT in `ws0_loadgen_record.py`, which imports from here, so
a table or a checker that went missing in the move is a REFUSAL rather than a field silently
uncompared. That is the property the move had to preserve, because a half-wired guard is the
exact defect this issue keeps finding.
"""

from __future__ import annotations

from ws0_validate import Invalid, non_negative_int

# ============================================================================
# THE CROSS-CHECKED COUNTERS — AN INVARIANT ASSUMED IS AN INVARIANT UNENFORCED (#3272 round 20)
# ============================================================================
# `error_codes` was classified `ignored`, and the reason it carried was:
#
#     "a BREAKDOWN of requests_error, which must already be ZERO for the rep to be reported —
#      so this map is empty whenever the rep is accepted"
#
# That sentence is TRUE OF A WELL-FORMED RECORD and SILENT ABOUT A MALFORMED ONE, which is the
# same shape as `target_concurrency` ("an INPUT", round 12 F3), `endpoint` ("not a measurement",
# round 14 F2) and `bytes_total` ("no byte-throughput figure is printed", round 17). The word
# "must" is doing the work, and NOTHING IN THIS REPORTER ENFORCED IT. So the reporter accepted
#
#     {"requests_error": 0, "error_codes": {"Internal": 1}, …}
#
# — MEASURED, pre-fix: exit 0, the full five-line report published, and the string `Internal`
# appearing NOWHERE in the output. A record that states in one field that a request failed with an
# internal error and in another that no request failed is not a record this reporter models, and it
# was reported as a clean, failure-free scan. Which of the two fields is wrong cannot be known from
# the artifact, so NEITHER is reported — the rule `load_corpus_identity` applies to `bytes_per_row`
# vs `data_db_bytes/rows` and the derived-vs-recorded `rows_per_s` cross-check applies to the rate.
#
# THE INVARIANT ENFORCED IS THE SUM, NOT THE EMPTINESS, and the difference is not pedantic. "empty
# whenever requests_error is 0" is the special case at zero; `sum(error_codes.values()) ==
# requests_error` is the producer's actual invariant (`record_outcome` increments `self.error` and
# `self.error_codes[code]` on the SAME line — tools/flight-loadgen/src/record.rs), and it also
# catches a record whose breakdown DISAGREES at a non-zero count: `requests_error: 3` with a single
# code counted once. That record is refused for its self-contradiction rather than only for the
# non-zero count it would otherwise be refused for — the diagnostic an operator reads is then about
# a corrupt artifact, not about a failing server. An emptiness check would say nothing about it.
#
# `error_codes` is therefore CONSUMED, not a sixth disposition, and the precedent is in the census
# already: `rows_per_s` is `consumed` with the reason "cross-checked against the DERIVED
# rows_total/duration_s" — a field that produces no figure of its own, is validated, and is compared
# against OTHER FIELDS OF THE SAME RECORD. That is exactly this. It is NOT `session-bound` or
# `content-volume`, because those two exist for expectations that come from OUTSIDE the record (the
# session's configuration; a separate measurement), and this one's comes from the record itself —
# collapsing them would put "compared to another field of this same object" under a word that means
# "compared to something we established elsewhere".
#
# So the wiring mirrors `ZERO_REQUIRED_COUNTERS` (the other `consumed`-side mechanism) rather than
# `_EXPECTATION_TABLES` (which is for the VERIFYING dispositions): a table declared as data, a
# checker declared beside it, and closures in BOTH DIRECTIONS at import — a member not classified
# `consumed` is refused, and a member no checker reads is refused. `_EXPECTATION_TABLES` is
# deliberately NOT extended to cover it: registering a table against `consumed` would demand an
# entry for EVERY consumed field (that closure is bidirectional), so `rows_total` and `duration_s`
# would need cross-check entries they do not have, and the honest way to say "this is a consumed
# field with a cross-check" is a table of the consumed fields that have one.
#
# Each entry is `(SOURCE, WHY, CONSEQUENCE)`, the shape `SESSION_BOUND_INPUTS` uses, for the same
# reason: the CONSEQUENCE is the sentence the refusal ends with, and a refusal that names two
# disagreeing numbers and nothing about what the disagreement costs is one an operator cannot act on.
CROSS_CHECKED_COUNTERS: dict[str, tuple[str, str, str]] = {
    "error_codes": (
        "`requests_error` in this same record — the loadgen increments the count and the"
        " per-code breakdown on the same line (StepAgg::record_outcome)",
        "the PER-CODE BREAKDOWN of requests_error. It was classified IGNORED because the map"
        " `must be empty whenever the rep is accepted` — an invariant this reporter ASSUMED"
        " and NEVER ENFORCED, so a record carrying `requests_error: 0` beside"
        " `error_codes: {\"Internal\": 1}` was accepted and reported as a clean, failure-free"
        " scan with the failing code appearing nowhere in the output. The invariant checked is"
        " the SUM rather than the emptiness, because the sum is the producer's actual invariant"
        " and it also catches a breakdown that disagrees at a NON-ZERO count (#3272 round 20)",
        "THE RECORD CONTRADICTS ITSELF about whether any request failed, so neither field can be"
        " reported: this reporter cannot know whether the rep suffered failures its error count"
        " omitted or carries a breakdown from elsewhere, and both readings make the rows a"
        " measurement of something other than a clean full-corpus scan. Re-run the rep rather"
        " than reporting a self-contradictory record.",
    ),
}


def check_error_code_breakdown(tag: str, rec: dict, requests_error: int) -> dict:
    """REQUIRE the per-code breakdown to ACCOUNT FOR every failed request (#3272 round 20).

    `error_codes` was classified `ignored` because it "must be empty whenever the rep is accepted".
    The reporter never enforced that, so `{"requests_error": 0, "error_codes": {"Internal": 1}}` was
    accepted and published as a clean, failure-free scan with `Internal` appearing nowhere in the
    output. A record that says in one field that a request failed and in another that none did is
    self-contradictory, and neither reading may be reported.

    The invariant is the SUM, not the emptiness: `sum(error_codes.values()) == requests_error` is the
    producer's own invariant (`StepAgg::record_outcome` increments `self.error` and
    `self.error_codes[code]` on the same line), and unlike an emptiness test it also refuses a
    breakdown that disagrees at a NON-ZERO count — `requests_error: 3` beside one code counted once.

    `requests_error` is passed in ALREADY VALIDATED (the caller's `non_negative_int` over
    `ZERO_REQUIRED_COUNTERS`) rather than re-read from `rec` here: two independent reads of one field
    is how the two sites drift, and the whole subject of this function is two fields that must agree.

    Every count goes through `non_negative_int`, never a bare `int()` or a bare `sum()`: a bool
    (`int(True)` is 1), a fractional count (`int(0.9)` is 0 — which would make a broken breakdown sum
    to the clean total), a string, `inf`/`nan` and a negative count are each refused by name. A
    negative count matters specifically here, because summing one CANCELS a positive sibling: a
    `{"A": 2, "B": -2}` breakdown sums to 0 and would satisfy a clean `requests_error: 0`.

    ABSENT is an ERROR, never an assumed empty map — the AC3 rule. `rec.get("error_codes", {})` would
    make the check pass precisely when the record is silent about the breakdown, which is the
    fabricated-default shape this module refuses everywhere else.

    Returns what was verified, so the rep's record can state the check ran.
    """
    source, why, consequence = CROSS_CHECKED_COUNTERS["error_codes"]
    if "error_codes" not in rec:
        raise Invalid(
            f"flight rep {tag} step record carries no `error_codes`, so the per-code breakdown of"
            f" its {requests_error} failed request(s) was NOT OBSERVED and cannot be compared"
            f" against {source}. A missing field is an error, never an assumed empty map —"
            " defaulting it would make this check pass precisely when the record is silent about"
            f" the breakdown (#3272 round 20). {why}"
        )
    got = rec["error_codes"]
    if not isinstance(got, dict):
        raise Invalid(
            f"flight rep {tag} recorded `error_codes` = {got!r} ({type(got).__name__}), but the"
            " load generator writes it as a MAP of status-code label -> count"
            " (StepRecord.error_codes is a BTreeMap<String, u64>). A value of another shape cannot"
            f" be summed and compared against {source}, so it is refused rather than skipped:"
            " a breakdown this reporter cannot read is not a breakdown it may ignore (#3272"
            f" round 20). {why}"
        )
    total = 0
    for code, count in sorted(got.items()):
        if not isinstance(code, str):
            raise Invalid(
                f"flight rep {tag} recorded an `error_codes` key {code!r}"
                f" ({type(code).__name__}), but the load generator writes each key as a STATUS-CODE"
                " LABEL string (classify.rs). A key of another type means this is not the map this"
                f" reporter models (#3272 round 20). {why}"
            )
        total += non_negative_int(
            f"flight rep {tag} error_codes[{code!r}]",
            count,
            "It is a per-code FAILED-REQUEST COUNT, summed and compared against this record's own"
            " requests_error. A fractional one would be TRUNCATED by a bare int() into agreement"
            " with a clean total, and a NEGATIVE one would CANCEL a positive sibling so that a"
            " breakdown naming real failures summed to zero (#3272 round 20).",
        )
    if total != requests_error:
        raise Invalid(
            f"flight rep {tag} recorded `requests_error` = {requests_error} but its"
            f" `error_codes` breakdown sums to {total} ({dict(sorted(got.items()))}). The load"
            " generator increments the count and the per-code entry on the SAME line"
            " (StepAgg::record_outcome, tools/flight-loadgen/src/record.rs), so the two are the"
            " same fact written twice and a record whose halves disagree is not one this reporter"
            f" models. {consequence}"
        )
    return {
        "error_codes": dict(sorted(got.items())),
        "error_codes_sum": total,
        "requests_error": requests_error,
        "error_codes_source": (
            "CROSS-CHECKED: sum(error_codes.values()) == requests_error, the invariant the load"
            " generator maintains at StepAgg::record_outcome. This field used to be classified"
            " IGNORED on the ASSUMED invariant that the map is empty whenever requests_error is 0,"
            " which nothing enforced — so a record carrying requests_error 0 beside"
            " error_codes {'Internal': 1} was reported as a clean, failure-free scan (#3272"
            " round 20)"
        ),
    }
