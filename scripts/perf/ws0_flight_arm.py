#!/usr/bin/env python3
"""ARM B — the Flight `do_get` measurement block, from one session's artifacts.

Split out of `ws0_collect.py` under the campsite rule (source target ~800 lines) when #3272's
F2 fix landed. The seam is the one the rig itself is built around: the two MEASUREMENT ARMS.

    ws0_collect.py     arm A, the bare scan, plus what both arms share
    ws0_flight_arm.py  arm B, Flight do_get over a real loopback transport (this file)

They are separate claims measured through different surfaces with different contracts — the
Flight arm has a per-temperature REQUEST contract and no setup leg to subtract; the bare scan
has a setup leg and a per-PASS contract — so the checks do not overlap, and the shared
quantity validators live in `ws0_validate.py` where both reach them.

Everything here obeys the rule the whole rig exists for:

    **A quantity that was not validly OBSERVED is an ERROR, never a fabricated value.**
"""

from __future__ import annotations

import json
import pathlib

from ws0_collect import prewarm_block, read_prewarm, spread, REQUIRED_EVENTS
from ws0_content_volume import (
    CONTENT_VOLUME_NO_ORACLE,
    CONTENT_VOLUME_NO_ORACLE_NOTE,
    check_content_volume,
    preflight_arrow_bytes_per_scan,
)
from ws0_loadgen_record import (
    SESSION_BOUND_INPUTS,
    ZERO_REQUIRED_COUNTERS,
    _ZERO_COUNTER_MEANING,
    check_error_code_breakdown,
    check_fixed_inputs,
    check_record_surface,
    check_session_bound_inputs,
)
from ws0_rounds import collect_round_meta
from ws0_validate import (
    Invalid,
    non_negative_int,
    positive_derived,
    positive_finite_float,
    positive_int,
    read_perf_counters,
    require_complete,
)


# WHY THIS ARM'S PATH IS A *REQUEST* AND NOT AN OBSERVATION (#3272 round 16).
#
# `CQLITE_FLIGHT_MERGE_PATH=bypass` only PREFERS the single-source fast path. Read
# `cqlite-flight/src/bypass.rs`'s own module docs: "`bypass` requests the fast path but NEVER
# overrides a correctness precondition — a 2-source table under `bypass` still merges". The
# executed arm can therefore be the MERGER while the driver requested `bypass`, by two distinct
# routes, neither of which the rig can see:
#
#   1. the conjunctive predicate declines (`bypass_reason` returns `MultipleSources`,
#      `DroppedColumns`, `StaticColumns`, `MulticellArmDivergence`, `ReaderUnsupported`, ...) —
#      pinned by `bypass_tests.rs::forced_bypass_never_overrides_a_correctness_precondition`,
#      which asserts `ForcedMergePath::Bypass` over two sources yields `MultipleSources`;
#   2. the predicate SELECTS the fast path and `ScanRowSource::open` then returns `Ok(None)`
#      (the walk cannot serve that reader), so `producer_warm.rs` falls through to
#      `KWayMerger::new_from_readers`.
#
# This field used to be named `forced_merge_path` and carried the requested value, which read as
# the path the server took. So a rep whose requested arm was `bypass` and whose EXECUTED arm was
# the merger was published as a `bypass` measurement — and the rig's headline output is a
# bare-scan-vs-Flight RATIO PER ARM, so the two arm rows could be two measurements of the same
# code with different labels. That is not a labelling nit; it is the comparison itself.
#
# THE SERVER DOES NOT REPORT THE ARM IT TOOK, and that is why this is a relabel rather than an
# observation (the #3272 round 13 route — an honest absence beats a false claim — rather than
# round 14's, which could make its claim real because sysfs answered). Verified in
# cqlite-flight 2026-08-05:
#
#   * `producer_warm.rs` computes `reason = bypass_reason(...)` and consumes it ONLY in
#     `if reason.is_selected()`. It is never logged, never a metric, never a span attribute and
#     never returned to the caller; route 2 above records nothing at all.
#   * `bypass.rs`'s docs point at `cqlite_core::storage::read_path_probe` for observability, but
#     that is three PROCESS-GLOBAL `AtomicU64`s read IN-PROCESS via `snapshot`/`delta_since`
#     (its own docs: "the consumers are integration tests in a DIFFERENT crate"). It has no RPC,
#     log or metric export. This rig measures a SEPARATE server process over loopback gRPC, so
#     an in-process atomic in that process is unreachable by construction.
#   * The only out-of-band server→client surface is `do_action`'s `TABLE_STATS_ACTION`, whose
#     `TableStatsResponse` carries row/partition/SSTable counts and nothing about arms.
#
# Emitting the selected arm would mean changing production `cqlite-flight`, which is outside this
# issue's scope. So the rig states what it KNOWS (the value it set in the server's environment)
# and states, in results.json AND in the printed summary, that the executed arm was NOT OBSERVED.
# Successor work: make the server emit the selected `BypassReason` per request and have the rig
# REJECT any rep whose executed arm differs from the requested one (#3287/#3299 track the rig's
# other unobserved-control gap; this one needs the server-side emission first).
MERGE_PATH_NOT_OBSERVED = "NOT OBSERVED"

MERGE_PATH_OBSERVABILITY_NOTE = (
    "`requested_merge_path` is the value this rig set for CQLITE_FLIGHT_MERGE_PATH in the"
    " measured server's environment — a REQUEST, not an observation. `bypass` only PREFERS the"
    " single-source fast path: cqlite-flight/src/bypass.rs never lets it override a correctness"
    " precondition, so a rep can execute the K-WAY MERGER under a requested `bypass` either"
    " because `bypass_reason` declined (MultipleSources/DroppedColumns/StaticColumns/"
    "MulticellArmDivergence/ReaderUnsupported) or because ScanRowSource::open returned None and"
    " producer_warm.rs fell through to KWayMerger::new_from_readers. THE SERVER DOES NOT REPORT"
    " THE ARM IT TOOK: the computed reason is consumed only by an `if` and is never logged,"
    " metered or returned, and read_path_probe is an IN-PROCESS atomic with no export, which this"
    " rig — measuring a separate process over loopback gRPC — cannot read. Emitting it would"
    " require changing production cqlite-flight, outside this issue's scope. So the executed arm"
    " is recorded as NOT OBSERVED rather than asserted, and every per-arm figure and ratio below"
    " is conditional on a request the server was free to decline. Successor work: emit the"
    " selected BypassReason per request and REFUSE any rep whose executed arm differs from the"
    " requested one."
)


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


def session_bound_expectations(tag: str, flight_endpoint: str) -> dict[str, str]:
    """What each SESSION-BOUND field of rep `tag`'s record MUST equal (#3272 round 14, F1/F2).

    Built here rather than defaulted inside the checker, and every field of
    `SESSION_BOUND_INPUTS` must get an entry — an absent one is an `Invalid` from the checker,
    not a field waved through. The completeness is ASSERTED against the shipped table rather
    than trusted to this function staying in step with it: a field added to
    `SESSION_BOUND_INPUTS` and forgotten here would otherwise be classified as verified while
    nothing supplied a value to verify it against, which is the half-wired shape #3272 keeps
    finding one layer in.

    `round` is the rep's TAG. It is the same string the driver passes as `--round "$tag"`
    (lib-measure.sh) and the same string the artifact is NAMED for, which is exactly why the
    comparison is worth making: `perf-<tag>.csv` and `<tag>.round` are found by that name, so a
    record whose own `round` disagrees is a record from a different rep sitting in this rep's
    filename — and its rows would be divided by this rep's cycles.

    `endpoint` is the SESSION MANIFEST's `config.flight_endpoint` (#3272 round 14, F2), which the
    driver stamps before the first rep and passes to every rep as
    `--endpoint http://127.0.0.1:$PORT`. It is a REQUIRED PARAMETER of this function rather than
    something read from disk here, for the reason the whole rig prefers a passed-in pin: the caller
    already read and validated the manifest (`ws0_session.session_manifest_config`), and a second
    read here would be a second source of truth for one fact — the shape this issue keeps finding.
    It is NOT defaulted and NOT optional: `flight_endpoint: str = ""` would make the comparison pass
    against an empty expectation the moment a caller forgot it, which is the `rec.get(k, <what we
    want>)` silent-skip shape at the parameter list.

    The endpoint is a PRE-MEASUREMENT PIN, not a report-time argument, and that is what makes it
    provenance: the reporter cannot be told which server to believe, so a record from a different
    server cannot be excused by re-reporting.
    """
    if not isinstance(flight_endpoint, str) or not flight_endpoint.strip():
        raise Invalid(
            f"internal: no pinned flight endpoint was supplied for rep {tag}"
            f" (got {flight_endpoint!r}). `endpoint` is verified against the SESSION MANIFEST's"
            " `config.flight_endpoint`, so an empty expectation would compare every record against"
            " nothing while the census says the field is verified — the half-wired guard #3272"
            " keeps finding (#3272 round 14, F2)."
        )
    expected = {"round": tag, "endpoint": flight_endpoint}
    absent = [k for k in SESSION_BOUND_INPUTS if k not in expected]
    if absent:
        raise Invalid(
            f"internal: no expectation is built for the session-bound field(s)"
            f" {', '.join(sorted(absent))} (rep {tag}). Each is classified as VERIFIED against the"
            " session in ws0_loadgen_record.SESSION_BOUND_INPUTS, so leaving it without an expected"
            " value would mean the census claims a check that nothing performs (#3272 round 14)."
        )
    # ...AND THE OTHER DIRECTION: an expectation built for a field the table does not classify as
    # session-bound would be silently DROPPED by the checker, which loops over the TABLE. That is
    # the same half-wired shape from the other end — a value supplied and never compared — and it is
    # how a future field could be "wired here" while nothing verified it.
    extra = [k for k in expected if k not in SESSION_BOUND_INPUTS]
    if extra:
        raise Invalid(
            f"internal: an expectation is built for {', '.join(sorted(extra))} (rep {tag}), which"
            " ws0_loadgen_record.SESSION_BOUND_INPUTS does not classify as session-bound. The"
            " checker loops over the TABLE, so this value would be silently dropped — a field wired"
            " here while nothing verifies it (#3272 round 14)."
        )
    return expected


def flight_rep_tag(arm: str, temp: str, rep: int) -> str:
    """The artifact tag of ONE flight rep — the ONLY spelling of this convention (#3551).

    Extracted so `ws0_flight_admission.verify_flight_admission` can derive the same tag set this
    collector reads: a second copy of the convention would drift, and its failure mode is an
    absent-artifact refusal for a rep whose files exist under the other module's name.
    """
    return f"flight-{arm}-{temp}-{rep}"


def collect_flight(
    d: pathlib.Path, temp: str, arm: str, reps: int, corpus_rows: int, flight_endpoint: str,
    counted_events: tuple = REQUIRED_EVENTS,
) -> dict:
    """Arm B's measurement block for `temp`/`arm`, from this session's artifacts.

    `flight_endpoint` is the session manifest's pinned `config.flight_endpoint` (#3272 round 14,
    F2), REQUIRED positionally rather than defaulted: every rep's record is compared against it, so
    a default would silently disable the check for a caller that forgot to pass it.
    """
    # THE UNTIMED PREFLIGHT, resolved ONCE for the session (#3272 round 17). It is the expectation
    # every timed rep's ARROW PAYLOAD VOLUME is compared against: `bytes_total` was classified
    # IGNORED, so a response carrying the expected ROW COUNT with FEWER ARROW COLUMNS satisfied
    # every check in this function — and made Arrow encoding look CHEAPER, which is the one quantity
    # #3096 exists to measure. The oracle is the warm prewarm leg's retained JSONL, which
    # `ws0_prewarm` has already verified to be a COMPLETE full-corpus scan and which ran OUTSIDE the
    # perf window, so nothing new is executed and the timed measurement is untouched.
    #
    # `None` means this session has NO preflight (a cold-only session, where the prewarm is skipped
    # by design so `cold` stays meaningful). That is a real gap, and it is NAMED in every rep's
    # record rather than passed over — see CONTENT_VOLUME_NO_ORACLE.
    arrow_bytes_per_scan = preflight_arrow_bytes_per_scan(d)
    rows_per_sec: list[float] = []
    cycles_per_row: list[float] = []
    ipc: list[float] = []
    rows_total = 0
    per_rep = []
    missing: list[str] = []
    prewarm: list[dict] = []
    round_meta: dict[int, dict[str, int]] = {}
    for rep in range(1, reps + 1):
        tag = flight_rep_tag(arm, temp, rep)
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
        # ...and THE INPUTS THE DRIVER FIXES must hold the values it fixed them to (#3272 F3).
        # `target_concurrency` was classified IGNORED, so a record produced at `--ramp 8` passed
        # every row/request/error/shed check and was reported as this rig's CONCURRENCY-ONE
        # baseline while measuring N concurrent scans contending for one pinned physical core.
        # `shape`, `step` and the `schema` version tag carried the same exposure for the same
        # "it is only an INPUT" reason. Checked BEFORE the counters below, because a record from a
        # different workload should be refused for THAT rather than for a downstream consequence.
        fixed_inputs = check_fixed_inputs(tag, rec)
        # ...and THE SESSION-BOUND INPUTS must match THIS REP'S IDENTITY (#3272 round 14, F1).
        # `round` was REQUIRED PRESENT and never compared to anything, so SWAPPING TWO REPS' JSONL
        # FILES passed every check above: `perf-<tag>.csv` and `<tag>.round` are located by TAG from
        # the FILENAME, so rep 1's rows and duration were divided by rep 2's cycles and attributed
        # to rep 2's round — a corrupted cycles/row and a mis-paired comparison out of an artifact
        # set that is entirely self-consistent on disk. Recorded in its OWN block below rather than
        # merged into `verified_fixed_inputs`: these were verified against the SESSION, not against
        # a constant, and one label for two different kinds of check is how "verified" stops meaning
        # anything specific.
        #
        # `endpoint` joined it in F2. It was classified IGNORED as "the loopback address; not a
        # measurement" — true of the FIELD, false of the FIGURE: it names WHICH SERVER produced the
        # measured rows, so a record from another local process on another port, or from a remote
        # host, satisfied every check above and had its rows divided by the cycles below, which
        # `perf -C` collected on THIS session's pinned cores. Those cores served nothing.
        session_bound = check_session_bound_inputs(
            tag, rec, session_bound_expectations(tag, flight_endpoint)
        )
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
        # ...and THE PER-CODE BREAKDOWN MUST ACCOUNT FOR THAT COUNT (#3272 round 20). `error_codes`
        # was classified IGNORED because it "must be empty whenever the rep is accepted" — an
        # invariant ASSUMED and never enforced, so a record carrying `requests_error: 0` beside
        # `error_codes: {"Internal": 1}` passed every check above and was published as a clean,
        # failure-free scan with the failing code named nowhere in the output. Checked as a SUM
        # rather than as an emptiness test, because the sum is the producer's own invariant
        # (`StepAgg::record_outcome` increments both on one line) and it also catches a breakdown
        # that disagrees at a non-zero count. Passed the ALREADY-VALIDATED count from the loop
        # above rather than re-reading `rec`: two reads of one field is how two sites drift, and
        # this check's whole subject is two fields agreeing.
        error_breakdown = check_error_code_breakdown(tag, rec, errors)
        requests_ok = check_request_count(tag, temp, rec.get("requests_ok"), rows, corpus_rows)
        # ...and THE RESPONSE MUST HAVE CARRIED THE ARROW A COMPLETE SCAN CARRIES (#3272 round 17).
        # Every check above counts REQUESTS and ROWS. `bytes_total` — the client-side sum of each
        # decoded batch's `get_array_memory_size()` — was classified IGNORED because "no
        # byte-throughput figure is printed", which is true and is not the test: a response with the
        # expected number of rows and FEWER ARROW COLUMNS passed everything above while the server
        # encoded LESS ARROW, making the encode look cheaper. This rig's headline is a
        # bare-scan-vs-Flight RATIO, so that shortfall moves the published number rather than merely
        # mislabelling it. Compared per SCAN against the untimed preflight resolved above, exactly.
        content_volume = (
            check_content_volume(tag, rec, requests_ok, arrow_bytes_per_scan)
            if arrow_bytes_per_scan is not None
            else None
        )
        # The prewarm outcome for THIS rep, recorded by ws0-baseline.sh.
        prewarm.append({"rep": rep, "status": read_prewarm(d, tag)})
        # ...and its OBSERVED round + position within that round (#3272 R3).
        meta = collect_round_meta(d, tag, rep)
        round_meta[rep] = meta
        # Every CONFIGURED event required present, not just the derived two (#3248 finding 3).
        counters = read_perf_counters(d / f"perf-{tag}.csv", tag, counted_events)
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
                # WHAT WAS VERIFIED, not merely that something was (#3272 F3): the concurrency,
                # shape, step index and schema version this rep's figures are conditional on. A
                # reader comparing two sessions can see the baseline was the same baseline.
                "verified_fixed_inputs": fixed_inputs,
                # ...and what was verified against THIS SESSION'S IDENTITY (#3272 round 14, F1/F2):
                # the `round` label the record carries, compared to the tag the artifact was found
                # under, and the `endpoint` it carries, compared to the SERVER the manifest pinned
                # before the first rep. Kept separate from the block above because it is a different
                # claim — one is "this rep ran the workload the rig fixes", the other is "this record
                # is this rep's, from this session's server" — and a swapped JSONL or a record from a
                # peer lane's server satisfies the first while failing the second.
                "verified_session_bound_inputs": session_bound,
                # ...and the ERROR-CODE CROSS-CHECK (#3272 round 20): the breakdown, its SUM, and
                # the count it was compared against. Spread into the rep rather than nested, so
                # `requests_error` keeps the top-level key an existing consumer reads while the
                # two new siblings record that the comparison RAN. Recorded at all for the reason
                # `verified_fixed_inputs` is: a reader must be able to see WHAT was checked, not
                # merely that the rep was accepted — `error_codes` was `ignored` for four rounds
                # and no output said so.
                **error_breakdown,
                # ...and the ARROW PAYLOAD VOLUME comparison, or the NAMED reason it could not run.
                # Never absent and never silently `null`: a cold-only session has no untimed
                # preflight to compare against (the prewarm is skipped so `cold` stays meaningful),
                # and a reader must be able to see that this rep's payload was UNCHECKED rather than
                # infer it from a missing key.
                #
                # KEY RENAMED from `verified_content_volume` (#3272 round 18). The old name asserted
                # a verification the comparison cannot deliver: its reference is the untimed
                # preflight, which traverses the SAME ticket, server and response path as the timed
                # requests, so a UNIFORM shortfall cancels and passes. The rename is deliberate
                # rather than an added sibling — a consumer reading `verified_content_volume` must
                # FAIL to find it and come read what replaced it, rather than keep reading a
                # weakened claim under an unchanged key (the `forced_merge_path` rule, round 16).
                "content_volume_self_consistency": (
                    content_volume
                    if content_volume is not None
                    else {
                        "bytes_total_verified_against_independent_oracle": False,
                        "bytes_total_checked": CONTENT_VOLUME_NO_ORACLE,
                        "why": CONTENT_VOLUME_NO_ORACLE_NOTE,
                    }
                ),
                "prewarm": prewarm[-1]["status"],
            }
        )
    require_complete(f"flight do_get {arm} ({temp})", per_rep, reps, missing)
    return {
        "arm": f"flight_do_get_{arm}",
        "surface": "arrow_flight FlightService::do_get (loopback gRPC)",
        "temperature": temp,
        # RENAMED from `forced_merge_path` (#3272 round 16) — see MERGE_PATH_OBSERVABILITY_NOTE.
        # The old name asserted an observation the rig cannot make: the server never reports which
        # arm it took, and a requested `bypass` can legitimately execute the merger. The rename is
        # deliberate rather than an added sibling field: a consumer reading `forced_merge_path`
        # must FAIL to find it and come here, rather than keep reading a value whose meaning
        # silently changed under an unchanged key.
        "requested_merge_path": arm,
        "executed_merge_path": MERGE_PATH_NOT_OBSERVED,
        "merge_path_observability": MERGE_PATH_OBSERVABILITY_NOTE,
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
