#!/usr/bin/env python3
"""THE STANDING NOTES — what this rig verifies, and WHAT IT DOES NOT (issue #3272 round 22).

Split out of `ws0_report.py` under the campsite rule: adding round 22's boundary-record read took
that file past its ~800-line target, and the `file-size` ratchet is `.rs`-ONLY, so a python file
crosses its campsite-rule target SILENTLY (checked with `wc -l`, never left to the gate). This is a
split by RESPONSIBILITY, not a waiver: `ws0_report.py` AGGREGATES a session, and this file owns the
BLOCK OF STANDING CLAIMS AND NON-CLAIMS printed under every report.

The seam is real and the rig already follows it — `ws0_content_volume` owns the wording of the
Arrow-volume caveat, and `ws0_boundary_observations` owns its own note, each beside the module that
knows the fact. What was left in the reporter was the ~90 lines of prose that belong to no single
check: the honest absences (§3b.1's unimplemented drift control, the unobservable executed arm, the
unreachable Arrow digest oracle) and the standing statements about how every figure is counted.

# WHY THESE NOTES ARE CODE AND NOT A README

Every bullet is a claim (or an explicit NON-claim) about THIS session's numbers, printed beside them.
A caveat in a README is a caveat nobody reading the figure sees — the shape round 20 found, where the
Arrow-volume withdrawal sat eleven bullets below the number it qualified. Two consequences kept here
deliberately:

* the per-session bullets (`recorded_round_metadata_lines`, the content-volume caveats, the boundary
  record) stay with their data in `ws0_report.py`; only the SESSION-INVARIANT text lives here;
* `MERGE_PATH_NOT_OBSERVED` is INTERPOLATED from `ws0_flight_arm` rather than spelled again, so the
  summary and `results.json` cannot describe the same non-observation differently.
"""

from __future__ import annotations

from ws0_flight_arm import MERGE_PATH_NOT_OBSERVED


def selection_and_request_note_lines() -> list[str]:
    """The SELECTION and the per-temperature request contract."""
    return [
        "  * only the SELECTION printed above was measured; an absent temperature or "
        "arm was NOT run and nothing here speaks to it (results.json .selection).",
        "  * every COLD flight rep is verified to be EXACTLY ONE successful request "
        "(requests_ok == 1) and every rep's rows an exact multiple of the corpus row "
        "count, so no warm request can be reported inside a cold figure; a rep that "
        "violates either is REFUSED, not blended. The corpus row count is REQUIRED "
        "(an absent corpus-identity.json is fatal), so this check can never be "
        "skipped while these notes claim it ran (#3272).",
    ]


def executed_arm_note_lines() -> list[str]:
    """THE ARM IS A REQUEST, NOT AN OBSERVATION (#3272 round 16).

    Stated the way §3b.1 states the interleaving control is not implemented: the honest absence, not
    a claim the rig cannot support.
    """
    return [
        "  * the ARM of each flight row above is the value this rig REQUESTED via "
        "CQLITE_FLIGHT_MERGE_PATH, and the arm actually EXECUTED is "
        f"{MERGE_PATH_NOT_OBSERVED} (results.json .executed_merge_path). `bypass` only "
        "PREFERS the single-source fast path: cqlite-flight never lets it override a "
        "correctness precondition, so a rep can execute the K-WAY MERGER under a requested "
        "`bypass` — and the server does not report the arm it took (the computed reason is "
        "consumed by an `if` and never logged, metered or returned; read_path_probe is an "
        "IN-PROCESS atomic this rig, measuring a separate process over gRPC, cannot read).",
        "    So read every per-arm figure and the per-arm bare/flight RATIO as conditional on "
        "a request the server was free to decline — in the limit the two arm rows could be the "
        "same code measured twice. Emitting the selected arm needs a change to production "
        "cqlite-flight; until then this is NOT verified, exactly as §3b.1's drift control is not.",
    ]


def content_volume_note_lines() -> list[str]:
    """THE ARROW PAYLOAD VOLUME IS NOT VERIFIED EITHER (#3272 round 18).

    The same posture as the arm above and as §3b.1's drift control. Round 17 added the check and
    named its output `verified_content_volume`; the reference it compares against is the untimed
    preflight, which goes through the SAME ticket, server process and response path as the timed
    requests — so a uniform omission cancels and the comparison passes on a short payload. The check
    is RETAINED (it still refuses a one-sided shortfall) and the CLAIM is withdrawn.

    ...and the bullet says WHICH OF ITS TWO CASES this session is in (#3272 round 20). It used to
    open "the ARROW PAYLOAD VOLUME of each flight rep IS COMPARED against this session's UNTIMED
    PREFLIGHT" unconditionally — FALSE on a session with no preflight (a cold-only run:
    `lib-measure.sh` skips the prewarm on the cold arm by design). So on exactly the session where
    NOTHING was compared, the only human-readable text about the payload asserted that a comparison
    had happened. The per-arm caveat lines beside each figure now name each rep's real state.
    """
    return [
        "  * the ARROW PAYLOAD VOLUME of each flight rep is compared against this session's "
        "UNTIMED PREFLIGHT WHERE ONE EXISTS — per scan and exactly — and that comparison is a "
        "SELF-CONSISTENCY check, NOT a verification "
        "(results.json .content_volume_self_consistency).",
        "    A session with NO PREFLIGHT (a cold-only run: the prewarm is skipped on the cold arm "
        "by design, because prewarming it would make `cold` meaningless) has NO COMPARISON AT ALL "
        "for this property — not a weak one, none. Which reps are in which state is stated beside "
        "each arm's figure above, never left to this bullet.",
        "    The preflight traverses the SAME ticket, the SAME server process and the SAME "
        "response path as the timed requests, so an omission that is a property of that path "
        "(a dropped Arrow column, a narrowed buffer) is present in BOTH in equal measure, their "
        "byte counts AGREE, and the check passes on a payload that is short — which would make "
        "Arrow encoding look CHEAPER, the one quantity #3096 exists to measure. What it does "
        "still refuse is a ONE-SIDED shortfall.",
        # WORDED WITHOUT THE TOKEN `cells` DELIBERATELY. `test_ws0_report_guards.sh`'s round-17
        # non-vacuity probe asserts that a PRE-FIX report naming the bare scan's cell shortfall does
        # not exist, by grepping the whole summary case-insensitively for `cells` — so a standing
        # caveat here that used the word would red that probe for a reason unrelated to its subject,
        # and the probe is right to be that blunt. The mechanism is stated as the ABSENT NULL
        # VALIDITY BITMAPS, which is the same fact in the vocabulary of the thing being folded.
        "    The independent oracle would be the pinned ARROW_BUFFER_DIGEST "
        "(tools/ws0-corpus-gen/src/measurement_corpus.rs); it is UNREACHABLE for this corpus, "
        "because the #3096 digest oracle refuses a corpus in which no Arrow validity bitmap ever "
        "carries an absent value, and ws0-corpus-gen writes every non-key column on every row. "
        "Closing this needs changes to production flight-loadgen (a per-step digest) AND a null "
        "plan in the corpus generator; until then this is NOT verified, exactly as the arm above "
        "is not.",
    ]


def counting_note_lines() -> list[str]:
    """HOW every printed figure was counted — the standing statements about the instrument."""
    return [
        "  * every figure is rows/s AND cycles/row; no CPU-share is reported "
        "(a share shift with unmoved rows/s is a FAIL, spec R1).",
        "  * the bare scan's cycles are SETUP-SUBTRACTED (a separately measured "
        "--setup-only perf window); the Flight arm's setup is outside its window. "
        "BOTH counters were observed — an absent or uncounted perf event is fatal, "
        "never a 0 (#3272).",
        # REWRITTEN FOR #3551, because the old wording became FALSE the moment the two arms
        # could be pinned differently. It read: "`cycles` is summed over BOTH SMT siblings of the
        # pinned physical core, so cycles/row is a per-physical-core figure ... Both arms are
        # counted identically, so the ratio and the arm-to-arm delta are unaffected." Under
        # `--flight-pin-mode distinct-cores` the counted pair is NOT one core's siblings and the
        # two arms are NOT counted identically — so the sentence had to become true in BOTH
        # configurations rather than be deleted. The quantity is stated in the terms that hold
        # either way (hardware-thread cycles over the counted list, printed beside every figure),
        # and the distinct-core case is named as the PROPERTY UNDER TEST rather than as a caveat.
        "  * `cycles` is summed over EVERY hardware thread in the counted list, which is "
        "printed beside each arm's figures ('counted on cpus ...'): cycles/row is "
        "hardware-thread cycles over that list per row.",
        "    Both arms count TWO hardware threads. When both lists are one physical core's "
        "siblings (the default) each arm's figure is a per-physical-core one and the two are "
        "counted identically, so the ratio and the arm-to-arm delta are directly comparable.",
        "    Under --flight-pin-mode distinct-cores the Flight arm's two threads sit on "
        "DIFFERENT physical cores and therefore do NOT share a core's execution resources. "
        "That is the PROPERTY UNDER TEST (#3551), not a nuisance: read the arm-to-arm delta as "
        "being about it, and note the two arms are then no longer counted over equivalent "
        "hardware — the bare scan remains the pin-identical drift control.",
        "  * every rep of BOTH arms records its PREWARM outcome in results.json "
        "(prewarm/prewarm_all_ok); a degraded prewarm is flagged above, never swallowed.",
        "    A warm rep is prewarmed by an UNTIMED full pass outside its perf window; "
        "the cold arm is deliberately never prewarmed, and its `skipped-cold-arm` "
        "sentinel satisfies the requirement for a COLD rep ONLY (#3272).",
        "  * the corpus identity is verified against the BYTES MEASURED, not trusted "
        "from corpus-identity.json: the recorded size is always re-stat'ed and the "
        "recorded sha256 re-derived from the Data.db unless --skip-corpus-digest was "
        "passed, in which case the line above says CORPUS DIGEST UNVERIFIED (#3272).",
    ]


def fixture_scope_note_lines() -> list[str]:
    """WHAT THIS CORPUS IS, and which absolutes are NOT reproduced here."""
    return [
        "  * the corpus is CQLite-written + CQLite-read: a PERFORMANCE FIXTURE ONLY "
        "(#3042), never a correctness oracle.",
        "  * the #3058/#3100 absolutes (240,100 / 312,155 rows/s) were corpus- and "
        "machine-bound and are NOT reproduced here.",
        "",
    ]
