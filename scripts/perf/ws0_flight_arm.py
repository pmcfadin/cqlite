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
from ws0_loadgen_record import (
    ZERO_REQUIRED_COUNTERS,
    _ZERO_COUNTER_MEANING,
    check_record_surface,
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
