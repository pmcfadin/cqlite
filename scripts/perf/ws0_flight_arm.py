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
import statistics
import sys

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


# ===========================================================================
# THE SERVER PROCESS'S RSS — R6.1 of the flight-allocator spec (issue #3997)
# ===========================================================================
# Two fields of `/proc/<server-pid>/status`, sampled ONCE per rep at scan end. What each one is,
# and what the scan-end timing does and does not buy, is stated at the field definition below
# (`RSS_SAMPLE_TIMING_NOTE`) rather than here, so the caveat travels with the value into
# `results.json` instead of living only in this file.
#
# WHY R6.1 EXISTS AT ALL: jemalloc's dirty-page decay is time-based, so a linked jemalloc can
# win on throughput while holding more resident. #3997's pre-registered criterion is therefore
# a JOINT one, and a peak-RSS figure that was not observed cannot satisfy half of it.
#
# EVERY UNMEASURED STATE IS AN EXPLICIT MARKER NAMING ITS CAUSE, NEVER A ZERO AND NEVER AN
# ABSENT KEY. A 0 kB resident set and an unreadable `/proc/<pid>/status` must not read alike:
# the first would satisfy any ratio ceiling and the second is a comparison that was not made.
# The marker is a STRING, so a downstream reader that averages it raises rather than publishing
# a number — the same shape `CONTENT_VOLUME_NO_ORACLE` uses one module over.
RSS_UNMEASURED = "UNMEASURED"

# The AFFIRMATIVE status, spelled once. The sampler writes it and `read_server_rss` requires
# exactly it or a marker — a status it cannot classify is a refusal, because "the record says
# something else" and "the record says it could not measure" are different states.
RSS_MEASURED = "measured"

RSS_STATUS_FIELDS = ("VmHWM", "VmRSS")

RSS_SAMPLE_TIMING_NOTE = (
    "Sampled ONCE per rep at SCAN END — after the perf window closed, before the server was"
    " stopped — from /proc/<server-pid>/status. `VmHWM` is a kernel-maintained HIGH-WATER MARK,"
    " so a scan-end read yields the PEAK over the whole rep and needs no sampling loop (which"
    " would perturb a pinned measurement). `VmRSS` is NOT a high-water mark: it is ONE"
    " INSTANTANEOUS SAMPLE at that moment, NOT a mean, NOT a time-weighted average and NOT a"
    " steady-state estimate. Do not average it across reps or arms and call the result an"
    " average RSS; nothing here computes one. An unmeasured field carries an explicit"
    f" '{RSS_UNMEASURED} — <cause>' marker, never a 0 and never an absent key."
)


def rss_unmeasured(cause: str) -> str:
    """The one spelling of an unmeasured RSS field: the marker, then the CAUSE.

    A bare sentinel would tell a reader that the number is absent and nothing about why, and
    the operator's next action is entirely determined by the why — a vanished pid is a rep to
    re-run, a `/proc` that could not be read is a box to fix, an absent `VmHWM` is a kernel
    that does not export it. So the cause is part of the value.
    """
    return f"{RSS_UNMEASURED} — {cause}"


def rss_is_measured(value: object) -> bool:
    """True only for a value that is a REAL observation of a resident-set size in kB.

    Keyed on the value being a non-bool integer, never on "it is not the marker string": a
    future third state would otherwise inherit the measured branch, which is the permissive
    default this rig refuses everywhere else. `bool` is excluded explicitly because
    `isinstance(True, int)` is true in Python and `True` is not a kilobyte count.
    """
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _rss_marker(value: object) -> bool:
    """True for an explicit `rss_unmeasured(...)` marker, and for nothing else."""
    return isinstance(value, str) and value.startswith(RSS_UNMEASURED)


def parse_proc_status_rss(text: str, where: str) -> dict:
    """`VmHWM`/`VmRSS` in kB out of a `/proc/<pid>/status` body, each three-valued.

    Returns a dict keyed by the two field names, each value either a positive int (kB) or an
    `rss_unmeasured(...)` marker naming why that field could not be read. Never raises: the
    caller is the measurement driver at scan end, and aborting a rep whose throughput was
    already measured would discard a good observation to report a missing one.

    Refused rather than guessed, field by field:

    * a field ABSENT from the body — some kernels/processes export no `Vm*` lines at all;
    * a UNIT other than `kB`. `/proc/<pid>/status` has always used kB, so a different unit is a
      body this parser does not model, and silently treating it as kB would publish a figure
      1024x wrong against a 1.10x ceiling;
    * a value that is not a non-negative integer, or is zero — a running server with a zero
      resident set is not a measurement, and zero is precisely the value that would satisfy any
      ratio ceiling.
    """
    out: dict[str, object] = {}
    seen: dict[str, str] = {}
    for line in text.splitlines():
        name, sep, rest = line.partition(":")
        if sep and name in RSS_STATUS_FIELDS:
            seen[name] = rest.strip()
    for field in RSS_STATUS_FIELDS:
        if field not in seen:
            out[field] = rss_unmeasured(
                f"{where} carries no '{field}:' line, so that field was NOT OBSERVED"
            )
            continue
        parts = seen[field].split()
        if len(parts) != 2 or parts[1] != "kB":
            out[field] = rss_unmeasured(
                f"{where} records '{field}: {seen[field]}', which is not the"
                " '<integer> kB' shape this parser models — a unit it does not recognise is"
                " refused rather than read as kB"
            )
            continue
        try:
            value = int(parts[0])
        except ValueError:
            out[field] = rss_unmeasured(
                f"{where} records '{field}: {seen[field]}', whose value is not an integer"
            )
            continue
        if value <= 0:
            out[field] = rss_unmeasured(
                f"{where} records '{field}: {value} kB'; a live server process with a"
                " non-positive resident set is not an observation, and zero is the one value"
                " that satisfies every ratio ceiling"
            )
            continue
        out[field] = value
    return out


def sample_server_rss(pid: object, status_path: object = None) -> dict:
    """ONE rep's scan-end RSS record for the Flight server process.

    `status_path` exists for the tests and for a caller that has already resolved the path; it
    defaults to `/proc/<pid>/status`. The record ALWAYS carries both fields and a `status`, so a
    consumer never has to distinguish "absent key" from "unmeasured" — see RSS_UNMEASURED.
    """
    record: dict[str, object] = {
        "pid": pid,
        "source": None,
        "sampled_at": "scan-end (perf window closed, server not yet stopped)",
        "sample_timing": RSS_SAMPLE_TIMING_NOTE,
    }
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        cause = (
            f"the driver supplied {pid!r} as the server pid, which is not a positive integer,"
            " so no process could be read"
        )
        record["source"] = RSS_UNMEASURED
        record["vm_hwm_kb"] = rss_unmeasured(cause)
        record["vm_rss_kb"] = rss_unmeasured(cause)
        record["status"] = rss_unmeasured(cause)
        return record
    path = pathlib.Path(status_path) if status_path is not None else pathlib.Path(
        f"/proc/{pid}/status"
    )
    record["source"] = str(path)
    try:
        text = path.read_text()
    except OSError as exc:
        # A VANISHED PID AND AN UNREADABLE /proc ARE ONE STATE HERE, and the errno text names
        # which: `FileNotFoundError` is the server having already exited (the rep's peak went
        # with it), anything else is a box that could not be read. Either way the field was not
        # observed, and the cause is carried rather than collapsed to "unavailable".
        cause = f"{path} could not be READ ({exc}) — the field was NOT OBSERVED"
        record["vm_hwm_kb"] = rss_unmeasured(cause)
        record["vm_rss_kb"] = rss_unmeasured(cause)
        record["status"] = rss_unmeasured(cause)
        return record
    fields = parse_proc_status_rss(text, str(path))
    record["vm_hwm_kb"] = fields["VmHWM"]
    record["vm_rss_kb"] = fields["VmRSS"]
    if rss_is_measured(record["vm_hwm_kb"]) and rss_is_measured(record["vm_rss_kb"]):
        record["status"] = RSS_MEASURED
    else:
        unread = [
            name for name, key in (("VmHWM", "vm_hwm_kb"), ("VmRSS", "vm_rss_kb"))
            if not rss_is_measured(record[key])
        ]
        record["status"] = rss_unmeasured(
            f"{', '.join(unread)} not observed from {path} — see the field(s) for the cause"
        )
    return record


def read_server_rss(d: pathlib.Path, tag: str) -> dict:
    """Rep `tag`'s scan-end RSS record, or a marker record naming why there is none.

    THREE-VALUED, and the third value is a REFUSAL rather than a marker:

    * the artifact is present and well-formed -> its record, as written at scan end;
    * the artifact is ABSENT -> a marker record. That is the `read_prewarm` case: the driver
      predates the sampling, or the rep died before it. R6.1 is then UNMEASURED for that rep,
      which is reported rather than assumed satisfied;
    * the artifact is present and MALFORMED -> `Invalid`. An unreadable JSON body, or a field
      that is neither a positive integer nor an `UNMEASURED — ...` marker, is a CORRUPT
      artifact, and a corrupt artifact is not an honest absence: something wrote a value this
      reader cannot classify, and classifying it as absent would hide that.
    """
    p = d / f"{tag}.server-rss.json"
    if not p.exists():
        cause = (
            f"no {p.name} beside this rep's artifacts — the server's RSS was not sampled for"
            " it (a driver that predates R6.1, or a rep that died before scan end)"
        )
        return {
            "pid": RSS_UNMEASURED,
            "source": RSS_UNMEASURED,
            "sampled_at": RSS_UNMEASURED,
            "sample_timing": RSS_SAMPLE_TIMING_NOTE,
            "vm_hwm_kb": rss_unmeasured(cause),
            "vm_rss_kb": rss_unmeasured(cause),
            "status": rss_unmeasured(cause),
        }
    try:
        record = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(
            f"flight rep {tag}: {p} exists but could not be read as JSON ({exc}). Refused"
            " rather than treated as an absent sample: something wrote that file, and calling a"
            " corrupt record an honest absence would hide it."
        ) from None
    if not isinstance(record, dict):
        raise Invalid(
            f"flight rep {tag}: {p} must hold a JSON object, got {type(record).__name__}."
        )
    for key in ("vm_hwm_kb", "vm_rss_kb", "status"):
        if key not in record:
            raise Invalid(
                f"flight rep {tag}: {p} carries no {key!r}. The sampler writes all three"
                " unconditionally — an unmeasured field carries an"
                f" '{RSS_UNMEASURED} — <cause>' marker — so an absent key is a record this"
                " reader does not model, not an unmeasured sample."
            )
    marked = _rss_marker(record["status"])
    if record["status"] != RSS_MEASURED and not marked:
        raise Invalid(
            f"flight rep {tag}: {p} records status={record['status']!r}, which is neither"
            f" {RSS_MEASURED!r} nor an '{RSS_UNMEASURED} — <cause>' marker. A verdict this"
            " reader cannot classify is refused rather than assumed to be either one."
        )
    for key in ("vm_hwm_kb", "vm_rss_kb"):
        value = record[key]
        if rss_is_measured(value) or _rss_marker(value):
            continue
        raise Invalid(
            f"flight rep {tag}: {p} records {key}={value!r}, which is neither a positive"
            f" integer of kB nor an '{RSS_UNMEASURED} — <cause>' marker. A value this reader"
            " cannot classify is refused: the one thing it must never become is a number in"
            " R6.1's peak-RSS ratio."
        )
    # ...AND THE STATUS MUST AGREE WITH THE FIELDS IT SUMMARISES. A record claiming
    # `measured` beside a marker value is not a partially-measured sample: it is a record whose
    # own verdict contradicts its own data, and the verdict is the field a hurried reader
    # believes. Both directions, because either alone leaves the other reachable.
    both = rss_is_measured(record["vm_hwm_kb"]) and rss_is_measured(record["vm_rss_kb"])
    if record["status"] == RSS_MEASURED and not both:
        raise Invalid(
            f"flight rep {tag}: {p} records status={RSS_MEASURED!r} while at least one of"
            f" vm_hwm_kb/vm_rss_kb carries an {RSS_UNMEASURED} marker. The verdict contradicts"
            " the data it summarises, so neither is trusted."
        )
    if marked and both:
        raise Invalid(
            f"flight rep {tag}: {p} records an {RSS_UNMEASURED} status"
            f" ({record['status']!r}) while BOTH vm_hwm_kb and vm_rss_kb hold real"
            " observations. The verdict contradicts the data it summarises, so neither is"
            " trusted."
        )
    record.setdefault("sample_timing", RSS_SAMPLE_TIMING_NOTE)
    return record


def server_rss_block(samples: list[dict], temp: str, arm: str) -> dict:
    """The arm-level RSS the aggregate reads, plus the census that keeps it honest.

    The two published figures are MEDIANS OVER THE REPS THAT WERE MEASURED, and the number of
    reps behind each one is published beside it — a median over 1 of 3 reps and a median over
    3 of 3 are different claims, and the abc driver runs `--reps 1`, so this is routinely a
    single observation and must not read as more.

    If NO rep of this arm was measured, the figure is the MARKER, never a median of an empty
    list and never a 0. `statistics.median([])` raises, which would abort the whole report for
    a quantity that is merely absent; and a 0 would satisfy R6.1's ceiling outright.
    """
    hwm = [s["vm_hwm_kb"] for s in samples if rss_is_measured(s["vm_hwm_kb"])]
    rss = [s["vm_rss_kb"] for s in samples if rss_is_measured(s["vm_rss_kb"])]
    absent = rss_unmeasured(
        f"no rep of {arm} ({temp}) yielded a scan-end sample; see each rep's own"
        " `server_rss.status` for the cause"
    )
    return {
        "server_vm_hwm_kb": statistics.median(hwm) if hwm else absent,
        "server_vm_rss_kb": statistics.median(rss) if rss else absent,
        "server_rss_reps_measured": len(hwm),
        "server_rss_reps_total": len(samples),
        "server_rss_sample_timing": RSS_SAMPLE_TIMING_NOTE,
    }


def _cli_sample_server_rss(argv: list[str]) -> int:
    """`ws0_flight_arm.py sample-server-rss <pid> <out.json>` — the scan-end sampler.

    Called by the measurement driver at scan end, between the perf window closing and
    `stop_server`. It ALWAYS writes a record — a measured one or a marker one naming the cause —
    and exits 0 for both, deliberately: the throughput measurement for that rep is already
    complete and correct, and aborting it because a peak-RSS read failed would discard a good
    observation in order to report a missing one. The MISSING half is then reported honestly,
    by the marker, all the way into the aggregate's table.

    exit 2 is reserved for the one state that is not an unmeasured sample: the record could not
    be WRITTEN at all. Nothing downstream would then be able to tell this rep from one the
    driver never sampled, so it is a refusal the caller can see.
    """
    if len(argv) != 2:
        sys.stderr.write(
            "usage: ws0_flight_arm.py sample-server-rss <server-pid> <out.json>\n"
        )
        return 2
    raw_pid, out = argv
    try:
        pid: object = int(raw_pid)
    except ValueError:
        pid = raw_pid
    record = sample_server_rss(pid)
    try:
        pathlib.Path(out).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    except OSError as exc:
        sys.stderr.write(
            f"FATAL: the scan-end RSS record could not be WRITTEN to {out} ({exc}). This is the"
            " one RSS failure that is refused rather than marked: with no record at all, this"
            " rep is indistinguishable downstream from one that was never sampled.\n"
        )
        return 2
    print(record["status"])
    return 0


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
    rss_samples: list[dict] = []
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
        # ...and the SERVER PROCESS'S SCAN-END RSS for this rep (#3997, R6.1). Read the same
        # way the prewarm status is — an absent artifact is an UNMEASURED marker naming its
        # cause, never a 0 — and carried into the rep below rather than summarised away, so a
        # reader can see which reps the arm-level median rests on.
        rss_samples.append(read_server_rss(d, tag))
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
                # THE SERVER'S SCAN-END RSS, or the NAMED reason it was not observed (#3997,
                # R6.1). Never absent and never 0: R6.1's criterion is a RATIO against arm A's
                # peak, and a 0 would satisfy every ceiling it could be compared with.
                "server_rss": rss_samples[-1],
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
        # THE ARM-LEVEL RSS FIGURES R6.1 IS READ FROM, with the rep census beside them (#3997).
        # `ws0_abc_aggregate.py` reads `server_vm_hwm_kb`/`server_vm_rss_kb` per session and
        # medians them across rounds; either may be an UNMEASURED marker, which that tool
        # prints as such instead of computing a ratio from it.
        **server_rss_block(rss_samples, temp, f"flight_do_get_{arm}"),
        "reps": per_rep,
    }


if __name__ == "__main__":
    # ONE subcommand, and an unrecognised one is a usage refusal rather than a default: this
    # module is a COLLECTOR that `ws0_report.py` imports, and its only executable surface is the
    # scan-end sampler the driver calls per rep (#3997, R6.1).
    if len(sys.argv) >= 2 and sys.argv[1] == "sample-server-rss":
        sys.exit(_cli_sample_server_rss(sys.argv[2:]))
    sys.stderr.write(
        "ws0_flight_arm.py is the Flight arm COLLECTOR, imported by ws0_report.py. Its only"
        " subcommand is:\n    ws0_flight_arm.py sample-server-rss <server-pid> <out.json>\n"
    )
    sys.exit(2)
