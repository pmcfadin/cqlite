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

import json
import pathlib

from ws0_validate import Invalid, positive_int

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
# `round` is NOT one of these four, because its correct value is not a CONSTANT — it is the rep's
# own tag, which differs per rep. It is verified all the same, by `SESSION_BOUND_INPUTS` below.
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

# ============================================================================
# THE SESSION-BOUND INPUTS — VERIFIED AGAINST THE REP'S OWN IDENTITY (#3272 round 14, F1)
# ============================================================================
# `FIXED_INPUTS` above can only verify a field whose correct value is a CONSTANT. Some fields of the
# record are just as measurement-determining and have no constant correct value: what they must
# equal depends on WHICH REP is being read and WHICH SESSION produced it. Such a field had no
# mechanism, so it was verified by the weakest thing available, and `round` was exploitable:
#
#   `round` was REQUIRED PRESENT and never compared to the rep's tag. So SWAPPING TWO REPS' JSONL
#   FILES passed every check: rep 1's rows and duration were read beside rep 2's PERF COUNTERS
#   (`perf-<tag>.csv` is located by TAG, from the filename) and rep 2's `<tag>.round` metadata.
#   That corrupts `cycles/row` directly — a cycles count from one rep divided by a row count from
#   another — and silently re-labels which round each figure belongs to, which is what the
#   per-round pairing pairs on. The old reasoning ("a second comparison would be a second source
#   of truth") had it backwards: the tag is not a second source, it is the rep's IDENTITY, and
#   comparing a record to the identity of the file it was found in is how you learn the record
#   belongs to that rep at all. `<tag>.round` remains the authority for the round's METADATA
#   (index, position, arms) — this asserts only that the LOADGEN RECORD is this rep's.
#
# The expected values come from the REP TAG and the SESSION MANIFEST, never from a caller's
# argument: `session_bound_expectations` builds them from the tag and from what was recorded before
# the first rep.
#
# `endpoint` (#3272 round 14, F2) is the second member, and it is the reason this table is a
# MECHANISM rather than a one-off: it was classified `ignored` with the reason "the loopback address;
# not a measurement", which is the same sentence shape that was wrong for `target_concurrency` (F3)
# and for `round` (F1) — a true statement about the FIELD standing in for a claim about the FIGURE.
# The endpoint decides WHICH SERVER PRODUCED THE MEASURED ROWS, and the rig's whole arrangement is
# that one pinned local `cqlite-flight` process, on known cores, with a known data dir and known
# binaries, served every request. A record produced against a DIFFERENT server — another local
# process on another port (a peer lane, a hand-run server, a stale instance) or a remote host — is
# combined with THIS session's `perf -C` counters, which measure the pinned local cores, and
# published as this rig's cycles/row. Nothing else in the rig can see it: the rows are a legitimate
# multiple of the corpus row count, the request/error/shed counters are clean, the derived rate
# matches the record's own, the tag matches the filename, and the whole artifact set is
# self-consistent on disk. It is the request-substitution class of round 10's M1 and the
# corpus-substitution class of round 13's F3, one layer further out: the same query over the same
# bytes on a DIFFERENT MACHINE.
# Each entry is `(SOURCE, WHY, CONSEQUENCE)`. The third element is the sentence the MISMATCH
# refusal ends with, and it is per-field rather than one sentence for the table because the two
# members lose DIFFERENT things: a wrong `round` means this record is another REP's (its rows meet
# another rep's cycles), while a wrong `endpoint` means it is another SERVER's (its rows meet cores
# that served nothing). A single shared sentence would have to be true of both, and the only sentence
# true of both is "two strings differ" — which is exactly the diagnostic round 14's F1 test refuses,
# because an operator reading it cannot tell whether it matters.
SESSION_BOUND_INPUTS: dict[str, tuple[str, str, str]] = {
    "round": (
        "the rep TAG the artifact was found under",
        "the driver passes `--round <tag>`, so this field is the record's own statement of WHICH"
        " REP it is. It was REQUIRED PRESENT and never compared, so swapping two reps' JSONL files"
        " passed validation — combining one rep's rows and duration with ANOTHER rep's perf"
        " counters (located by tag from the filename) and round metadata, which corrupts"
        " cycles/row and mis-attributes every paired comparison (#3272 round 14, F1)",
        "The record does not belong to the rep whose filename it was found under, so its rows and"
        " duration would be combined with ANOTHER rep's perf counters; re-run rather than"
        " reporting a record from elsewhere.",
    ),
    "endpoint": (
        "`config.flight_endpoint` in the session manifest, pinned before the first rep",
        "the driver passes `--endpoint http://127.0.0.1:$PORT`, so this field is the record's own"
        " statement of WHICH SERVER PRODUCED THE MEASURED ROWS. It was classified IGNORED as `the"
        " loopback address; not a measurement`, so a record produced against a DIFFERENT server —"
        " another local process on another port, or a remote host — satisfied every row, request,"
        " error, shed, rate and tag check and was reported as this rig's result: its rows and"
        " duration divided by THIS session's `perf -C` cycles, which measure the pinned local"
        " cores that served nothing. The measured server is what every other pinned identity"
        " (cores, corpus, ticket, binaries) is an identity OF, so this is the corpus substitution"
        " of round 13 F3 and the request substitution of round 10 M1 one layer out — the same"
        " query over the same bytes on a different machine (#3272 round 14, F2)",
        "THE ROWS WERE SERVED BY A DIFFERENT SERVER than the one this session pinned and measured,"
        " so they would be divided by perf counters collected on cores that served nothing —"
        " every other pinned identity in this session (CPUs, corpus, ticket, binaries) describes"
        " the pinned server, and none of them describes the one that answered. Point the driver at"
        " the pinned endpoint and re-run; do not report rows measured elsewhere.",
    ),
}

# ============================================================================
# THE CONTENT-VOLUME INPUT — ROWS WERE VERIFIED, THE PAYLOAD WAS NOT (#3272 round 17)
# ============================================================================
# Every check above and below counts REQUESTS and ROWS. Not one of them looks at how much
# ARROW there was. `bytes_total` was classified `ignored` with the reason "the rig's claims are
# rows/s and cycles/row (spec R1); no byte-throughput figure is printed" — true, and insufficient
# in precisely the way the last five wrong reasons on this file were ("an INPUT", "not a
# measurement", "the loopback address", "a second source of truth"): a true statement about the
# FIELD standing in for a claim about the FIGURE. Whether a figure is PRINTED is not the test.
# The test the census states for itself is whether the field can change the VALIDITY of a printed
# figure — and this one can, more directly than any field yet found here.
#
# THE EXPOSURE. A `do_get` response carrying the expected NUMBER OF ROWS but FEWER ARROW COLUMNS
# (or narrower buffers) satisfies every existing check: `rows_total == requests_ok * corpus_rows`
# holds exactly, the request/error/shed counters are clean, the derived rate matches the recorded
# one, the tag and endpoint match the session. And it makes ARROW ENCODING LOOK FASTER, because
# the server encoded less. #3096 exists to measure Arrow-encode cost. So this defect flatters
# EXACTLY the quantity the parent issue set out to measure — and the rig's headline is a
# bare-scan-vs-Flight RATIO, so an asymmetric shortfall in one arm moves the published number
# directly rather than merely mislabelling it.
#
# WHY IT IS DERIVABLE AND SO IS VERIFIED RATHER THAN CONFESSED. `bytes` is summed client-side by
# `client.rs::do_get_drain` as `batch.get_array_memory_size()` per decoded `RecordBatch` — a
# function of the SCHEMA and the row count, not of timing, machine or load. It is therefore
# CONSTANT ACROSS EVERY FULL-CORPUS SCAN of a given corpus, and per-request it must be identical.
# Measured across all 40 committed loadgen records that carry both fields (#3217 partB counters,
# #3224 llc-s1/s6 steps, spanning concurrency 1..16 and 3..48 requests): `bytes_total /
# requests_ok` is 48,764,091,712 in EVERY ONE, with `bytes_total % requests_ok == 0` in every one,
# over the same 3,999,890-row full scan. So the per-scan Arrow extent is an OBSERVED INVARIANT of
# the corpus, not a hoped-for one.
#
# WHAT IS VERIFIED, AND FROM WHAT. The expectation is the rep's OWN UNTIMED PREFLIGHT — the warm
# prewarm leg, which `lib-measure.sh` already runs OUTSIDE the perf window and whose JSONL
# `ws0_prewarm` already requires to be a COMPLETE full-corpus scan (round 12's F2). That artifact
# is therefore a verified-complete observation of this corpus's Arrow extent, taken through the
# same server, before the timed window opened and at no cost to it. The timed requests must match
# it PER SCAN and EXACTLY:
#
#     bytes_total == requests_ok * (preflight bytes_total / preflight requests_ok)
#
# DERIVE, DON'T TRUST (round 12's F2 principle): the expectation comes from a separately-validated
# observation, never from the timed record itself. A record cannot certify its own payload.
#
# NO TOLERANCE, and no threshold. `get_array_memory_size()` is integer arithmetic over buffer
# capacities, so a legitimate full scan reproduces it bit-for-bit; a percentage band would be a
# number somebody chose, and the whole exposure here is a SHORTFALL, which a band lets through in
# the flattering direction by construction.
#
# WHAT THIS DOES NOT VERIFY, stated rather than left to be assumed: it pins the Arrow buffer
# EXTENT, not the SCHEMA identity. Two different column sets whose buffers happened to sum to the
# same capacity would pass. The oracle for that is the pinned `ARROW_BUFFER_DIGEST`
# (tools/ws0-corpus-gen/src/measurement_corpus.rs), which is folded per-column over validity
# bitmaps and value buffers and cannot be fooled that way — but NOTHING ON THIS PATH CAN REACH IT:
# `bytes` is the only thing `do_get_drain` retains from a batch before dropping it, so the loadgen
# record carries no per-column information at all, and making it carry any means changing
# `flight-loadgen`. That is outside this issue's scope. Successor work: have the loadgen fold the
# #3096 Arrow digest over the batches it drains and record it, so the rig can compare a rep's
# response against `ARROW_BUFFER_DIGEST` itself instead of against its total extent.
CONTENT_VOLUME_INPUTS: dict[str, tuple[str, str, str]] = {
    "bytes_total": (
        "this rep's own UNTIMED PREFLIGHT (`<tag>.prewarm.jsonl`), whose full-corpus completeness"
        " ws0_prewarm already verified, scaled to this rep's requests_ok",
        "the ARROW PAYLOAD VOLUME of the measured response. Every other check on this record"
        " counts REQUESTS and ROWS, so a response carrying the expected number of rows and FEWER"
        " ARROW COLUMNS — or narrower buffers — satisfied all of them: the rows are an exact"
        " multiple of the corpus count, the counters are clean, the derived rate matches the"
        " recorded one. And it makes ARROW ENCODING LOOK FASTER, because the server encoded less."
        " #3096 exists to measure Arrow-encode cost and the rig's headline is a bare-scan-vs-"
        "Flight RATIO, so this defect flattered exactly the quantity the measurement was for."
        " It was classified IGNORED as `no byte-throughput figure is printed`, which is true and"
        " is not the test: the test is whether the field can invalidate a figure that IS printed"
        " (#3272 round 17)",
        "THE MEASURED RESPONSE CARRIED A DIFFERENT ARROW PAYLOAD than the verified-complete"
        " preflight scan of this same corpus through this same server. A SHORT payload means the"
        " server encoded less Arrow than the report's cycles/row is divided by, which makes"
        " Arrow encoding look CHEAPER — the one quantity #3096 exists to measure — and moves the"
        " published bare/Flight ratio in the flattering direction. Re-run the rep; do not report"
        " a response whose payload volume disagrees with a complete scan of the corpus.",
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
    "bytes_per_s": (
        "ignored",
        "DERIVABLE FROM TWO VERIFIED OPERANDS, which is the only reason it needs no domain of"
        " its own: the loadgen computes it as `per_s(self.bytes_total)` = bytes_total/duration_s"
        " (tools/flight-loadgen/src/record.rs), and BOTH operands are now checked — bytes_total"
        " by CONTENT_VOLUME_VERIFIED below, duration_s as the validated positive finite divisor"
        " of the reported throughput. So no value of this field can be wrong while both of those"
        " are right, and no figure this reporter prints reads it. The reason it USED to carry —"
        " `no byte-rate figure is reported` — was true and insufficient in exactly the way its"
        " sibling's was (#3272 round 17): whether a figure is PRINTED is not the test; whether"
        " the field can invalidate a printed figure is",
    ),
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
    # ---- SESSION-BOUND INPUTS (#3272 round 14, F1) — see SESSION_BOUND_INPUTS above ----
    # A fourth disposition, and the distinction from `verified-fixed-input` is the whole reason it
    # exists: these fields ALSO decide whether the figures mean what the report says, but their
    # correct value is NOT A CONSTANT — it is derived from the identity of the rep being read. So
    # they cannot be verified by the `FIXED_INPUTS` mechanism, and the disposition that used to
    # cover `round` (`required-present`, i.e. verified only to EXIST) is what F1 found.
    "round": ("session-bound", SESSION_BOUND_INPUTS["round"][1]),
    "endpoint": ("session-bound", SESSION_BOUND_INPUTS["endpoint"][1]),
    # ---- CONTENT-VOLUME INPUT (#3272 round 17) — see CONTENT_VOLUME_INPUTS above ----
    # A fifth disposition, and it is not `session-bound` for a reason worth stating: a session-bound
    # field's expectation comes from the session's CONFIGURATION (a tag, a pinned endpoint), known
    # before anything ran. This one's comes from a SEPARATE MEASUREMENT — the untimed preflight —
    # which had to be taken and validated first. Collapsing the two would put "compared to a string
    # we chose" and "compared to another observation we verified" under one word.
    "bytes_total": ("content-volume", CONTENT_VOLUME_INPUTS["bytes_total"][1]),
    "ts_unix_ms": ("ignored", "wall-clock stamp; the rig's ordering uses monotonic_ns from"
                              " <tag>.round, never a wall clock. It cannot affect what was"
                              " measured: no figure, no pairing and no refusal reads it — the"
                              " ATTRIBUTION of a record to a rep is `round` (session-bound,"
                              " above) and the artifact-set integrity checks are over the"
                              " monotonic instants in <tag>.round, which this rig deliberately"
                              " prefers precisely because a wall clock can step"),
    "seed": ("ignored", "the loadgen's RNG seed for TICKET SELECTION. It cannot affect what was"
                        " measured HERE, and the reason is specific to this rig's shape rather"
                        " than to the word INPUT — which is the reason that was wrong three times"
                        " (#3272 F3's target_concurrency, round 14 F1's round, round 14 F2's"
                        " endpoint). Every rep runs"
                        " `--shape full`, a VERIFIED FIXED INPUT above, and the `Full` transform"
                        " (tools/flight-loadgen/src/shape.rs) is `t.limit = None` on the base"
                        " template: it draws NOTHING from the RNG, so two seeds produce"
                        " byte-identical tickets and the same full-ring scan. Only the `point`"
                        " and `mixed` shapes consume the seed — and a record carrying either is"
                        " already refused by the `shape` check, so the seed becomes measurement-"
                        "determining only in a record this reporter cannot accept at all"),
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
#
# `required-present` IS GONE (#3272 round 14, F1). It had exactly one member, `round`, and what it
# meant in practice was "verified to EXIST and nothing more" — which is how a swapped JSONL file
# passed. A disposition whose whole content is a weaker check is an invitation to classify the next
# measurement-determining field into it, so it is removed rather than left empty: an empty
# disposition also makes the loop that reads it vacuous, and a check with no subject prints exactly
# like a passing one.
DISPOSITIONS = (
    "consumed",
    "verified-fixed-input",
    "session-bound",
    "content-volume",
    "ignored",
)

# WHICH TABLE STATES THE EXPECTATION FOR EACH VERIFYING DISPOSITION. Declared as DATA, so the
# both-directions closure below is ONE loop over every verifying disposition rather than a
# hand-written pair of checks per table (#3272 round 14). The pre-F1 version checked `FIXED_INPUTS`
# against the census by name and had no equivalent for anything else — so `SESSION_BOUND_INPUTS`
# would have shipped with NO closure check at all, which is the half-wired-guard shape this issue
# keeps finding. A new verifying disposition is registered HERE, in one place, or it is not one of
# `DISPOSITIONS` and is refused below.
#
# The tuple SHAPES differ between tables — `FIXED_INPUTS` is `(value, why)`, `SESSION_BOUND_INPUTS`
# is `(source, why, consequence)` — and the annotation says so rather than pretending they agree.
# The closure loops below read only the KEYS and the census's own disposition, deliberately: a
# closure check that also decoded each table's value shape would have to know both shapes, i.e. it
# would break the moment a third table arrived, which is the drift a registry exists to remove.
# The per-table value contract is asserted by its OWN loop, immediately after this one.
_EXPECTATION_TABLES: dict[str, tuple[str, dict[str, tuple[object, ...]]]] = {
    "verified-fixed-input": ("FIXED_INPUTS", FIXED_INPUTS),
    "session-bound": ("SESSION_BOUND_INPUTS", SESSION_BOUND_INPUTS),
    "content-volume": ("CONTENT_VOLUME_INPUTS", CONTENT_VOLUME_INPUTS),
}
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

# ...and each expectation table and the census are the same fact written twice, so they are checked
# against each other at import, IN BOTH DIRECTIONS, for EVERY verifying disposition — the
# `ZERO_REQUIRED_COUNTERS` pattern. A field given an expected value but left classified `ignored`
# (or classified as verified with no table entry, so nothing would be verified) is exactly the
# half-wired guard this issue keeps finding.
for _disp, (_table_name, _table) in _EXPECTATION_TABLES.items():
    if _disp not in DISPOSITIONS:
        raise Invalid(
            f"{_table_name} states expectations for the disposition {_disp!r}, which is not one of"
            f" {DISPOSITIONS} — so no field could ever carry it and the whole table is dead code"
        )
    if not _table:
        raise Invalid(
            f"{_table_name} is EMPTY, so the {_disp!r} disposition verifies nothing while the"
            " census claims it does — a check with no subject prints exactly like a passing one"
        )
    for _k in _table:
        if RECORD_FIELD_DISPOSITION.get(_k, ("", ""))[0] != _disp:
            raise Invalid(
                f"{_k} has an expected value in {_table_name} but is not classified {_disp!r} in"
                " RECORD_FIELD_DISPOSITION — the census and the verification disagree"
            )
for _k, (_d, _) in RECORD_FIELD_DISPOSITION.items():
    if _d in _EXPECTATION_TABLES:
        _table_name, _table = _EXPECTATION_TABLES[_d]
        if _k not in _table:
            raise Invalid(
                f"{_k} is classified {_d!r} but {_table_name} states no expectation for it, so"
                " nothing would be verified — the classification would be a claim about no check"
            )
# EVERY verifying disposition must be REACHED BY A CHECKER, or a field could be classified as
# verified while no code compares it. Asserted against the checker functions' own declared coverage
# (`_CHECKED_DISPOSITIONS`, defined beside them), which is the direction round 12's F2 missed one
# level out: the freeze was performed and the check on it was nominal.
del _k, _d, _disp, _table_name, _table

# ...and EVERY (SOURCE, WHY, CONSEQUENCE) ENTRY MUST CARRY ALL THREE ELEMENTS (#3272 round 14, F2).
# `SESSION_BOUND_INPUTS` grew a third element — the per-field CONSEQUENCE sentence the mismatch
# refusal ends with — and a two-element entry would raise an unpacking `ValueError` deep inside the
# checker at report time rather than being refused here. Worse, an entry with an EMPTY consequence
# would unpack cleanly and produce a refusal that names two differing strings and nothing about what
# is lost, which is the diagnostic that third element exists to prevent. So the shape is a
# REQUIREMENT stated at import, where the tables are: a field cannot carry one of these dispositions
# without a stated source, a stated reason and a stated consequence.
#
# Applied to EVERY table of this shape, from a list, rather than to `SESSION_BOUND_INPUTS` by name
# (#3272 round 17). `CONTENT_VOLUME_INPUTS` arrived with the identical shape, and a per-name loop
# would have left it with NO shape check at all while this one read as covering "the tables" — the
# half-wired shape the `_EXPECTATION_TABLES` registry exists to remove, one level down. A table of
# this shape is registered HERE or its entries are unchecked.
_TRIPLE_TABLES = (
    ("SESSION_BOUND_INPUTS", SESSION_BOUND_INPUTS),
    ("CONTENT_VOLUME_INPUTS", CONTENT_VOLUME_INPUTS),
)
for _tname, _tbl in _TRIPLE_TABLES:
    for _k, _spec in _tbl.items():
        if len(_spec) != 3:
            raise Invalid(
                f"{_tname}[{_k!r}] has {len(_spec)} element(s); every entry must be"
                " (SOURCE, WHY, CONSEQUENCE). The consequence is the sentence the MISMATCH refusal"
                " ends with, and it is per-field because the members lose different things — a wrong"
                " `round` means another REP's record, a wrong `endpoint` means another SERVER's, a"
                " wrong `bytes_total` means a SHORT ARROW PAYLOAD (#3272 round 14 F2 / round 17)."
            )
        for _pos, _label in enumerate(("SOURCE", "WHY", "CONSEQUENCE")):
            if not isinstance(_spec[_pos], str) or len(_spec[_pos].strip()) < 20:
                raise Invalid(
                    f"{_tname}[{_k!r}]'s {_label} is not a substantive sentence"
                    f" ({_spec[_pos]!r}). An empty one unpacks cleanly and produces a refusal that"
                    " names two differing values and nothing about what the mismatch costs, which"
                    " an operator cannot act on."
                )
# ...and every table of this shape must be one the registry knows about, or its entries would be
# shape-checked while nothing ever compared them.
for _tname, _tbl in _TRIPLE_TABLES:
    if not any(_n == _tname for _n, _ in _EXPECTATION_TABLES.values()):
        raise Invalid(
            f"{_tname} is shape-checked as an expectation table but is not registered in"
            " _EXPECTATION_TABLES, so no disposition reads it and no field could be verified"
            " against it (#3272 round 17)"
        )
del _k, _spec, _pos, _label, _tname, _tbl

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
            " RECORD_FIELD_DISPOSITION as CONSUMED, VERIFIED-FIXED-INPUT, SESSION-BOUND or"
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
    return verified


def check_session_bound_inputs(tag: str, rec: dict, expected: dict[str, str]) -> dict:
    """REQUIRE the SESSION-BOUND inputs to match this rep's own identity (#3272 round 14, F1/F2).

    The sibling of `check_fixed_inputs` for the fields whose correct value is not a constant:
    `round` must be the rep's TAG, and `endpoint` must be the server the SESSION MANIFEST pinned
    before the first rep.

    Before this, each was verified only to EXIST or not at all:

    * `round` (F1) — SWAPPING TWO REPS' JSONL files passed everything. `perf-<tag>.csv` and
      `<tag>.round` are located by TAG (from the filename), so rep 1's rows and duration were
      divided by rep 2's cycles and attributed to rep 2's round — a corrupted `cycles/row` and a
      mis-paired comparison, from an artifact set that is entirely self-consistent on disk.
    * `endpoint` (F2) — classified IGNORED, so a record produced against ANOTHER SERVER (another
      local process on another port, or a remote host) was reported as this rig's result. Its rows
      were divided by THIS session's `perf -C` cycles, collected on the pinned local cores that
      served nothing.

    `expected` maps field -> the value this rep must carry, built by the CALLER from the session
    manifest and the tag (`ws0_flight_arm.session_bound_expectations`) — never from a default here.
    Every session-bound field must appear in it: a field the caller forgot to supply an expectation
    for is an ERROR, not a field waved through, because that is precisely the silent-skip shape
    (`rec.get(k, <what we want>)`) this module refuses everywhere else.

    Compared as STRINGS with an explicit `str` type check, never `str(got) == want`: a coercion
    would make `round: 1` match the tag `"1"`, and the point is to compare what the loadgen actually
    wrote (`round: String`, `endpoint: String` in `StepRecord`).

    Compared EXACTLY, never by a substring/prefix/host-suffix test. That is the same rule the
    prewarm status and the roborev verdict scan follow, and it matters most here: an endpoint
    comparison that accepted a prefix would call `http://127.0.0.1:18815` and
    `http://127.0.0.1:188150` the same server, and one that compared only the HOST would accept
    every port on the box — which is precisely the peer-lane case this closes, since a second local
    server is on the same loopback host by construction.

    Returns what was verified, so the rep's record can state it.
    """
    verified: dict[str, str] = {}
    for key, (source, why, consequence) in SESSION_BOUND_INPUTS.items():
        if key not in expected:
            raise Invalid(
                f"internal: no expected value was supplied for the session-bound field `{key}`"
                f" (rep {tag}), whose correct value comes from {source}. A session-bound field"
                " with no expectation would be verified by nothing while the census says it is"
                " verified — the half-wired guard #3272 keeps finding. Supply it in"
                " ws0_flight_arm.session_bound_expectations."
            )
        want = expected[key]
        if key not in rec:
            raise Invalid(
                f"flight rep {tag} step record carries no `{key}`, so {source} could not be"
                f" compared against it and this record cannot be attributed to this rep or this"
                f" server at all. A missing field is an error, never an assumed default (#3272"
                f" round 14). {why}"
            )
        got = rec[key]
        if not isinstance(got, str):
            raise Invalid(
                f"flight rep {tag} recorded `{key}` = {got!r} ({type(got).__name__}), but the"
                f" load generator writes it as a STRING (StepRecord.{key}). Compared without a"
                " coercion deliberately: `str(got) == want` would let a numeric 1 satisfy the tag"
                f" spelling '1'. {why}"
            )
        if got != want:
            # BOTH VALUES ARE NAMED, and the refusal ends with THIS FIELD'S consequence — not a
            # shared sentence about two differing strings, which an operator cannot act on (round
            # 14's F1 asserts that property for `round`, and it is the reason the consequence is a
            # per-field element of the table rather than one sentence for the loop).
            raise Invalid(
                f"flight rep {tag} recorded `{key}` = {got!r}, but for this rep {source} is"
                f" {want!r}. {why}. {consequence}"
            )
        verified[key] = got
    return verified


# WHAT THE SESSION HAS NO ORACLE FOR, NAMED IN THE OUTPUT (#3272 round 17).
#
# The preflight the expectation comes from is the WARM prewarm leg, and `lib-measure.sh` skips the
# prewarm on the COLD arm BY DESIGN — a prewarm there would make "cold" meaningless. So a COLD-ONLY
# session legitimately contains no preflight at all, and there is no way to manufacture one without
# destroying the thing it would verify.
#
# That is a real gap and it is RECORDED rather than skipped. The branch is keyed on the AFFIRMATIVE
# presence of the oracle, never on the absence of a bad signal, and when it is absent the rep's
# record carries THIS STRING instead of a verified block — so a reader sees the check did not run
# rather than reading a silence as a pass. Round 16's F2 precedent: an honest partial with a named
# gap beats a check that cannot fire, and beats a claim the rig cannot support.
CONTENT_VOLUME_NO_ORACLE = "NOT VERIFIED — no untimed preflight in this session"

CONTENT_VOLUME_NO_ORACLE_NOTE = (
    "The measured response's ARROW PAYLOAD VOLUME (`bytes_total`) was NOT verified for this rep."
    " The expectation is one verified-complete full-corpus scan's payload, taken from the UNTIMED"
    " prewarm leg — and lib-measure.sh skips the prewarm on the COLD arm by design, because a"
    " prewarm would make `cold` meaningless. So a cold-only session has no oracle for this"
    " property and one cannot be synthesised without destroying what it would verify. What that"
    " leaves unverified, stated plainly: a response carrying the expected ROW COUNT with FEWER"
    " ARROW COLUMNS, or narrower buffers, would satisfy every other check on this rep and would"
    " make Arrow encoding look CHEAPER — the quantity #3096 exists to measure. A session that"
    " includes a WARM arm verifies it from that arm's preflight. Successor work: have"
    " flight-loadgen fold the #3096 Arrow-buffer digest over the batches it drains and record it"
    " per step, so a rep's response can be compared against the pinned ARROW_BUFFER_DIGEST"
    " directly — that also closes the SCHEMA-identity half this extent check cannot reach, and it"
    " needs no prewarm, so it would cover the cold arm too."
)


def preflight_arrow_bytes_per_scan(session_dir: pathlib.Path) -> float | None:
    """The Arrow payload volume of ONE VERIFIED-COMPLETE full-corpus scan (#3272 round 17).

    THE UNTIMED PREFLIGHT, and it already exists. `lib-measure.sh` runs a prewarm leg per WARM rep
    OUTSIDE the perf window and retains its JSONL at `<tag>.prewarm.jsonl`; `ws0_prewarm` already
    refuses to call that leg `ok` unless EVERY successful request streamed the PINNED corpus row
    count (round 12's F2). So the rig already possesses, at zero cost to the timed measurement and
    with no change to `cqlite-flight` or `flight-loadgen`, a validated-complete observation of this
    corpus's Arrow extent taken through the same server — exactly the expectation the timed
    requests must match.

    Resolved at SESSION level, over EVERY preflight present, not per rep. The Arrow extent of a full
    scan is a function of the SCHEMA and the ROW COUNT alone (`client.rs::do_get_drain` sums
    `batch.get_array_memory_size()`), so it is invariant across reps, arms and temperatures of one
    corpus — which is why a cold rep can be checked against a warm rep's preflight, and why every
    preflight in the session MUST AGREE. A disagreement is refused: two different payload volumes
    for the same corpus means at least one of them is not what this report thinks it measured.

    Returns bytes PER SCAN (`bytes_total / requests_ok`), or `None` when the session holds no
    preflight at all — a COLD-ONLY session, where the prewarm is skipped by design. `None` is a
    NAMED gap the caller records in the output (`CONTENT_VOLUME_NO_ORACLE`), never a silent skip.

    Operands go through `positive_int`, never a bare `int()`: this value MULTIPLIES the expectation
    for every timed rep, so a truncated or boolean operand would silently move the bar rather than
    being refused (the #3272 R6/B5 class).
    """
    per_scan: dict[float, str] = {}
    for path in sorted(session_dir.glob("*.prewarm.jsonl")):
        try:
            records = [
                json.loads(line) for line in path.read_text().splitlines() if line.strip()
            ]
        except (OSError, ValueError) as exc:
            raise Invalid(
                f"the untimed preflight {path.name} is not readable JSONL ({exc}), so the"
                " verified-complete Arrow payload volume it is the record of cannot be read. An"
                " unparseable oracle is a refusal, never a skipped comparison (#3272 round 17)."
            ) from None
        if not records:
            raise Invalid(
                f"the untimed preflight {path.name} holds no step record, so it observed nothing"
                " and cannot state this corpus's Arrow payload volume (#3272 round 17)"
            )
        ok = 0
        total = 0
        for idx, rec in enumerate(records):
            for key in ("requests_ok", "bytes_total"):
                if key not in rec:
                    raise Invalid(
                        f"the untimed preflight {path.name} record {idx} carries no `{key}`, so"
                        " the Arrow payload volume of a complete scan was NOT OBSERVED and cannot"
                        " be asserted. A missing operand is an error, never an assumed default —"
                        " defaulting it would make the comparison pass precisely when the"
                        " preflight is silent about it (#3272 round 17)."
                    )
            ok += positive_int(
                f"preflight {path.name} record {idx} requests_ok",
                rec["requests_ok"],
                "The preflight's Arrow volume is divided by it to get bytes PER SCAN, so a"
                " non-positive or fractional count would move the expectation every timed rep is"
                " measured against.",
            )
            total += positive_int(
                f"preflight {path.name} record {idx} bytes_total",
                rec["bytes_total"],
                "It IS the verified-complete Arrow payload volume every timed rep's response is"
                " compared against; a zero means the preflight streamed no Arrow at all.",
            )
        if total % ok != 0:
            raise Invalid(
                f"the untimed preflight {path.name} streamed {total:,} Arrow bytes over {ok}"
                " successful request(s), which is not a whole number per scan. Every request in a"
                " verified-complete preflight scanned the SAME corpus, and the client's per-batch"
                " `get_array_memory_size()` sum is a function of the schema and the row count"
                " alone — so a remainder means those requests did not all carry the same payload,"
                " and no single per-scan volume describes them. Re-run rather than reporting"
                " against an expectation that averages unequal responses (#3272 round 17)."
            )
        per_scan.setdefault(total / ok, path.name)
    if not per_scan:
        return None
    if len(per_scan) != 1:
        detail = ", ".join(
            f"{name} = {v:,.0f} B/scan" for v, name in sorted(per_scan.items())
        )
        raise Invalid(
            "this session's untimed preflights DISAGREE about the Arrow payload volume of a"
            f" full-corpus scan: {detail}. That volume is a function of the SCHEMA and the ROW"
            " COUNT alone (the client sums `get_array_memory_size()` per decoded batch), so one"
            " corpus has exactly one value and a disagreement means at least one preflight did not"
            " scan what this report thinks it measured. No single expectation can be derived from"
            " these, so none is invented — re-run the session (#3272 round 17)."
        )
    return next(iter(per_scan))


def check_content_volume(
    tag: str, rec: dict, requests_ok: int, expected_per_scan: float
) -> dict:
    """REQUIRE the measured response to carry the PAYLOAD a complete scan carries (#3272 round 17).

    `bytes_total` was classified IGNORED because "no byte-throughput figure is printed" — true, and
    not the test. Every other check on this record counts REQUESTS and ROWS, so a response with the
    expected number of rows and FEWER ARROW COLUMNS (or narrower buffers) passed all of them, and
    it makes ARROW ENCODING LOOK FASTER because the server encoded less. #3096 exists to measure
    Arrow-encode cost, and the rig's headline is a bare-scan-vs-Flight RATIO, so the defect
    flattered exactly the quantity being measured.

    `expected_per_scan` comes from `preflight_arrow_bytes_per_scan` — the rep's OWN UNTIMED
    PREFLIGHT, already verified to be a complete full-corpus scan. DERIVE, DON'T TRUST: the
    expectation is a separately-validated observation, never this record's own field. A record
    cannot certify its own payload.

    Compared EXACTLY, with no tolerance and no threshold. `get_array_memory_size()` is integer
    arithmetic over buffer capacities, so a legitimate full scan reproduces it bit-for-bit — and
    the exposure is a SHORTFALL, which a percentage band would admit in the flattering direction by
    construction. `expected_per_scan` is a float only because it is a quotient; the product is
    compared against the integer `bytes_total` after an exact-integer check on the expectation, so
    no float rounding decides a verdict.

    Returns what was verified, so the rep's record can state it.
    """
    source, why, consequence = CONTENT_VOLUME_INPUTS["bytes_total"]
    if "bytes_total" not in rec:
        raise Invalid(
            f"flight rep {tag} step record carries no `bytes_total`, so the ARROW PAYLOAD VOLUME"
            " of the measured response was NOT OBSERVED and cannot be asserted. A missing field is"
            f" an error, never an assumed default (#3272 round 17). {why}"
        )
    observed = positive_int(
        f"flight rep {tag} bytes_total",
        rec["bytes_total"],
        "It is the ARROW PAYLOAD VOLUME of the measured response — the evidence that the server"
        " encoded the Arrow this rep's cycles/row is divided by. A zero means it encoded none.",
    )
    want = expected_per_scan * requests_ok
    if want != int(want):
        raise Invalid(
            f"internal: rep {tag}'s expected Arrow volume ({expected_per_scan!r} per scan x"
            f" {requests_ok} request(s) = {want!r}) is not a whole number of bytes, so no exact"
            " comparison is possible. The per-scan figure is validated as an exact quotient by"
            " preflight_arrow_bytes_per_scan; reaching this means that guard was bypassed."
        )
    if observed != int(want):
        short = int(want) - observed
        direction = (
            f"{short:,} bytes SHORT ({100.0 * short / want:.4f}% less Arrow than a complete scan"
            " carries — the flattering direction)"
            if short > 0
            else f"{-short:,} bytes MORE than a complete scan carries"
        )
        raise Invalid(
            f"flight rep {tag} streamed {observed:,} Arrow bytes over {requests_ok}"
            f" successful request(s), but {source} is {expected_per_scan:,.0f} bytes per scan, so"
            f" this rep should have carried {int(want):,} — it is {direction}."
            f" {why}. {consequence}"
        )
    return {
        "bytes_total": observed,
        "bytes_per_scan_observed": observed / requests_ok,
        "bytes_per_scan_expected": expected_per_scan,
        "bytes_per_scan_expected_source": source,
    }


# WHICH DISPOSITIONS A CHECKER ACTUALLY COMPARES — asserted against `_EXPECTATION_TABLES` at import,
# so a field cannot be classified as verified by a table no function reads. That is round 12's F2
# one level out: the freeze happened and the CHECK ON IT was nominal, which is the same defect as a
# table that exists and is never consulted.
_CHECKED_DISPOSITIONS = {
    "verified-fixed-input": check_fixed_inputs,
    "session-bound": check_session_bound_inputs,
    "content-volume": check_content_volume,
}
for _disp in _EXPECTATION_TABLES:
    if _disp not in _CHECKED_DISPOSITIONS:
        raise Invalid(
            f"the disposition {_disp!r} has an expectation table but no checker function reads it,"
            " so a field classified with it would be verified by nothing while the census says it"
            " is verified (#3272 round 14)"
        )
for _disp in _CHECKED_DISPOSITIONS:
    if _disp not in _EXPECTATION_TABLES:
        raise Invalid(
            f"{_disp!r} has a checker but no expectation table, so the checker would compare"
            " against nothing"
        )
del _disp


