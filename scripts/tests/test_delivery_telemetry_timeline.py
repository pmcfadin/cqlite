#!/usr/bin/env python3
"""Unit tests for delivery-telemetry.py's ISSUE-TIMELINE replay (issue #3559).

`--slice` asserts a fact about DELIVERY time — "this issue was deliberately OPEN when that
PR merged". Issue #3550 decided that from CURRENT state, which cannot answer it, so a genuine
slice became unstampable the moment its issue was ever closed or reopened. This module covers
the replacement: an authoritative replay of the issue's own timeline to the PR's `mergedAt`.

Stdlib-only and NETWORK-FREE, like scripts/tests/test_delivery_telemetry.py — every `gh`
call is monkeypatched, and `record` is driven through the `--from-json` seam.

Run standalone:   python3 scripts/tests/test_delivery_telemetry_timeline.py
Or via the gate:  scripts/agent-gate.sh --only delivery-telemetry
"""

import importlib.util
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]

# The module file name has a hyphen, so load it by path.
_spec = importlib.util.spec_from_file_location("delivery_telemetry",
                                               SCRIPTS / "delivery-telemetry.py")
dt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dt)

# Sentinel for "omit this key entirely" — distinct from None, which is a MEASURED value.
_OMIT = object()

ISSUE_URL = "https://github.com/pmcfadin/cqlite/issues/3393"


class _FakeProc:
    def __init__(self, stdout):
        self.stdout = stdout


def _ev(kind, at):
    """A minimal timeline entry of the shape the REST timeline endpoint returns."""
    return {"event": kind, "created_at": at, "id": 1, "actor": {"login": "pmcfadin"}}


def _fake_gh(timeline_pages, issue_json=None, pr_json=None):
    """Route `gh issue view` / `gh pr view` / `gh api …/timeline` to canned replies.

    `timeline_pages` is either a list of PAGES (each a list of entries), a pre-serialised
    string (to plant a malformed/truncated reply), or an Exception to raise.
    """
    def fake_run(argv, **kw):
        if argv[:3] == ["gh", "issue", "view"]:
            return _FakeProc(json.dumps(issue_json if issue_json is not None else {
                "createdAt": "2026-06-10T00:00:00Z", "closedAt": None,
                "labels": [{"name": "P1"}, {"name": "oracle"}], "url": ISSUE_URL}))
        if argv[:3] == ["gh", "pr", "view"]:
            return _FakeProc(json.dumps(pr_json if pr_json is not None else {
                "createdAt": "2026-06-10T01:00:00Z", "mergedAt": "2026-06-10T03:00:00Z",
                "closingIssuesReferences": []}))
        if argv[:2] == ["gh", "api"]:
            if isinstance(timeline_pages, Exception):
                raise timeline_pages
            if isinstance(timeline_pages, str):
                return _FakeProc(timeline_pages)
            return _FakeProc("".join(json.dumps(p) for p in timeline_pages))
        raise AssertionError(f"unexpected argv: {argv}")
    return fake_run


class TimelineReplayTests(unittest.TestCase):
    """`_issue_state_at` — the replay itself, at unit level."""

    MERGED = "2026-06-10T03:00:00Z"

    def _state(self, pages, merged=_OMIT, url=ISSUE_URL):
        with mock.patch.object(dt.subprocess, "run", _fake_gh(pages)):
            return dt._issue_state_at(
                url, 3393, self.MERGED if merged is _OMIT else merged)

    # ---- what it asks GitHub -------------------------------------------------------
    def test_it_queries_the_timeline_of_the_issues_own_repository(self):
        """owner/repo come from the issue's own canonical URL (via _issue_identity), never
        from a hardcoded repository: a record for another repo's issue must not be decided
        from pmcfadin/cqlite's timeline."""
        seen = []

        def fake_run(argv, **kw):
            seen.append(argv)
            return _FakeProc("[]")

        with mock.patch.object(dt.subprocess, "run", fake_run):
            dt._issue_state_at("https://github.com/other/repo/issues/77", 77, self.MERGED)
        self.assertEqual(len(seen), 1)
        argv = seen[0]
        self.assertEqual(argv[:2], ["gh", "api"])
        self.assertIn("--paginate", argv)
        path = next(a for a in argv if "timeline" in a)
        self.assertTrue(path.startswith("repos/other/repo/issues/77/timeline"), path)

    # ---- the decision rule --------------------------------------------------------
    def test_the_last_deciding_event_at_or_before_merged_at_decides(self):
        before, after = "2026-06-10T02:00:00Z", "2026-06-10T04:00:00Z"
        cases = (
            ("no deciding event at all", [], True, None),
            ("only non-deciding events",
             [_ev("labeled", before), _ev("commented", before)], True, None),
            ("closed before the merge", [_ev("closed", before)], False, before),
            ("closed then reopened before the merge",
             [_ev("closed", "2026-06-10T01:00:00Z"), _ev("reopened", before)], True, None),
            ("reopened then closed before the merge",
             [_ev("reopened", "2026-06-10T01:00:00Z"), _ev("closed", before)], False, before),
            ("closed only AFTER the merge (the #3393 shape)",
             [_ev("closed", after), _ev("reopened", "2026-06-10T05:00:00Z")], True, None),
            # cycles SPANNING mergedAt — the shapes a timestamp comparison could not see
            ("closed before the merge, reopened after it",
             [_ev("closed", "2026-06-10T01:00:00Z"), _ev("reopened", after)],
             False, "2026-06-10T01:00:00Z"),
            ("reopened before the merge, reclosed after it",
             [_ev("closed", "2026-06-10T00:30:00Z"), _ev("reopened", before),
              _ev("closed", after)], True, None),
            ("two full cycles, the last one before the merge",
             [_ev("closed", "2026-06-10T00:30:00Z"), _ev("reopened", "2026-06-10T01:00:00Z"),
              _ev("closed", before), _ev("reopened", after)], False, before),
        )
        for label, pages, open_expected, close_at in cases:
            with self.subTest(label):
                self.assertEqual(self._state([pages]), (open_expected, close_at))

    def test_an_event_in_the_same_second_as_merged_at_is_refused_not_ordered(self):
        """A second-precision tie with mergedAt is UNMEASURABLE, never a permissive 'before'.

        Both GitHub timestamps are one-second resolution, so an event stamped in the same
        second as the merge could have landed either side of it. These two cases previously
        asserted the `<=` behaviour, which resolved the tie permissively and could reject a
        genuine slice or accept one that was still closed when its PR merged (roborev
        finding, issue #3559). An event at exactly mergedAt is always the deciding one, so
        there is no case where the tie is harmless.
        """
        before = "2026-06-10T02:00:00Z"
        for label, pages in (
            ("closed at exactly mergedAt", [_ev("closed", self.MERGED)]),
            ("reopened at exactly mergedAt",
             [_ev("closed", before), _ev("reopened", self.MERGED)]),
            ("a tie that is NOT the newest event still decides",
             [_ev("closed", self.MERGED), _ev("reopened", "2026-06-10T05:00:00Z")]),
        ):
            with self.subTest(label):
                with self.assertRaises(SystemExit) as caught:
                    self._state([pages])
                msg = str(caught.exception)
                self.assertIn("SAME SECOND", msg)
                self.assertIn("UNMEASURABLE", msg)
                self.assertIn("#3559", msg)

    def test_a_tie_strictly_before_the_merge_is_still_ordered_by_the_api_order(self):
        """The refusal is scoped to a tie WITH mergedAt, not to ties in general.

        Two events sharing a second well before the merge fall unambiguously on the same
        side of it, so the classification is decidable and must not be refused — otherwise
        the fix for the mergedAt tie would red an ordinary correct invocation.
        """
        tie = "2026-06-10T01:00:00Z"
        self.assertEqual(self._state([[_ev("closed", tie), _ev("reopened", tie)]]),
                         (True, None))
        self.assertEqual(self._state([[_ev("reopened", tie), _ev("closed", tie)]]),
                         (False, tie))

    def test_the_decision_does_not_depend_on_the_apis_delivery_order(self):
        """Ordering is established by the parsed timestamps here, not assumed of the API."""
        shuffled = [_ev("reopened", "2026-06-10T02:00:00Z"),
                    _ev("closed", "2026-06-10T01:00:00Z")]
        self.assertEqual(self._state([shuffled]), (True, None))
        self.assertEqual(self._state([list(reversed(shuffled))]), (True, None))

    def test_a_non_deciding_event_needs_no_created_at(self):
        """Scope statement, not leniency: a `labeled`/`committed` entry's timestamp is never
        READ, so there is nothing there to be permissive about. Measured on the real #3393
        timeline (261 events, all carrying created_at) — the tolerance exists for event
        types this repo has not seen, not for the events that decide."""
        pages = [[{"event": "labeled"}, _ev("closed", "2026-06-10T02:00:00Z")]]
        self.assertEqual(self._state(pages), (False, "2026-06-10T02:00:00Z"))

    # ---- pagination ---------------------------------------------------------------
    def test_it_reads_every_page_of_a_concatenated_paginated_reply(self):
        """gh 2.98 merges array pages into ONE array; older gh concatenates one array per
        page. Both are read — a replay that saw only the first page could miss the deciding
        event and answer 'open' from an incomplete timeline."""
        pages = [[_ev("labeled", "2026-06-10T00:10:00Z")],
                 [_ev("closed", "2026-06-10T02:00:00Z")]]
        self.assertEqual(self._state(pages), (False, "2026-06-10T02:00:00Z"))

    def test_it_refuses_a_truncated_or_unparseable_timeline(self):
        for raw in ('[{"event": "closed", "created_at": "2026-06-10T02:00:00Z"',
                    '[] {"not": "an array"', "not json at all", '[] trailing-garbage'):
            with self.subTest(raw=raw[:24]):
                with self.assertRaises(SystemExit) as cm:
                    self._state(raw)
                msg = str(cm.exception)
                self.assertIn("timeline", msg)
                self.assertIn("3559", msg)

    def test_it_refuses_an_empty_reply(self):
        """An empty stdout is not an empty timeline: every issue has at least its own
        events, and a zero-byte reply is an unmeasured read."""
        with self.assertRaises(SystemExit) as cm:
            self._state("")
        self.assertIn("timeline", str(cm.exception))

    def test_it_refuses_a_page_that_is_not_an_array(self):
        for page in ({"message": "Not Found"}, 17, "x", None):
            with self.subTest(page=page):
                with self.assertRaises(SystemExit) as cm:
                    self._state(json.dumps(page))
                self.assertIn("timeline", str(cm.exception))

    def test_it_refuses_a_failed_gh_call(self):
        with self.assertRaises(SystemExit) as cm:
            self._state(dt.subprocess.CalledProcessError(
                1, ["gh", "api"], stderr="HTTP 403: rate limited"))
        self.assertIn("failed", str(cm.exception))

    # ---- refusals over the entries -----------------------------------------------
    def test_it_refuses_an_entry_whose_kind_cannot_be_read(self):
        """An entry whose `event` is absent or non-string could BE the `closed` event that
        decides this, so it is refused rather than skipped."""
        for bad in ({"created_at": "2026-06-10T02:00:00Z"}, {"event": None},
                    {"event": 7}, {"event": ["closed"]}):
            with self.subTest(entry=bad):
                with self.assertRaises(SystemExit) as cm:
                    self._state([[bad]])
                self.assertIn("'event'", str(cm.exception))

    def test_it_refuses_a_padded_or_recased_state_event_rather_than_skipping_it(self):
        """`closed ` / `CLOSED` matches a deciding event only after normalising. Skipping it
        is the PERMISSIVE direction (the replay would answer "open"), and normalising it is
        the lenient-reader shape this file refuses everywhere else — so it is a refusal."""
        for bad in ("closed ", " closed", "CLOSED", "Reopened", "reopened\t"):
            with self.subTest(event=bad):
                with self.assertRaises(SystemExit) as cm:
                    self._state([[{"event": bad, "created_at": "2026-06-10T02:00:00Z"}]])
                msg = str(cm.exception)
                self.assertIn("normalising", msg)
                self.assertIn("3559", msg)

    def test_it_refuses_a_timeline_entry_that_is_not_an_object(self):
        for bad in (None, 3, "closed", []):
            with self.subTest(entry=bad):
                with self.assertRaises(SystemExit) as cm:
                    self._state([[bad]])
                self.assertIn("entry", str(cm.exception))

    def test_it_refuses_a_deciding_event_without_a_usable_created_at(self):
        for bad in (_OMIT, None, "", "   ", 17, "2026-06-10", "2026-06-10T02:00:00",
                    "20260610T020000Z"):
            with self.subTest(created_at=bad):
                entry = {"event": "closed"}
                if bad is not _OMIT:
                    entry["created_at"] = bad
                with self.assertRaises(SystemExit) as cm:
                    self._state([[entry]])
                self.assertIn("created_at", str(cm.exception))

    def test_it_refuses_a_non_canonical_issue_url(self):
        for bad in ("https://github.com/pmcfadin/cqlite/pull/3393",
                    "https://example.com/pmcfadin/cqlite/issues/3393",
                    ISSUE_URL + "\n", "", "3393", None):
            with self.subTest(url=bad):
                with self.assertRaises(SystemExit) as cm:
                    self._state([[]], url=bad)
                self.assertIn("canonical", str(cm.exception))

    def test_it_refuses_an_unusable_merged_at(self):
        for bad in (None, "", "   ", "2026-06-10", 1780000000):
            with self.subTest(merged_at=bad):
                with self.assertRaises(SystemExit) as cm:
                    self._state([[]], merged=bad)
                self.assertIn("merged_at", str(cm.exception))


class GithubFieldsTimelineTests(unittest.TestCase):
    """The live seam: `_github_fields` derives `issue_open_at_merge` from the replay."""

    def test_it_supplies_issue_open_at_merge_as_a_bool(self):
        with mock.patch.object(dt.subprocess, "run",
                               _fake_gh([[_ev("closed", "2026-06-10T04:00:00Z")]])):
            fields = dt._github_fields(3393, 3467, slice_requested=True)
        # closed only AFTER the merge -> open at mergedAt
        self.assertIs(fields["issue_open_at_merge"], True)
        self.assertIsNone(fields["issue_close_event_at"])
        self.assertEqual((fields["issue"], fields["pr"]), (3393, 3467))

    def test_the_replay_is_SKIPPED_when_no_branch_will_read_it(self):
        """An ordinary completed delivery must not be blocked by a signal nobody reads.

        `build_record` consults the timeline only on the classification paths (`--slice`, or a
        null `closed_at`), so replaying it unconditionally let an unreachable endpoint, a
        malformed event, or a same-second-as-mergedAt tie refuse a routine stamp (roborev
        round 2, issue #3559). The tie case is the sharp one: this change INTRODUCED that
        refusal, and an auto-close can land inside the merge's second.

        Planted so the test fails if the replay happens at all: the timeline call raises, and
        the same-second event that would refuse is present. Reaching the assert proves neither
        was consulted.
        """
        exploding_timeline = AssertionError("timeline must not be consulted on this path")
        closed_issue = {"createdAt": "2026-06-10T00:00:00Z",
                        "closedAt": "2026-07-01T00:00:00Z",
                        "labels": [{"name": "P1"}, {"name": "oracle"}], "url": ISSUE_URL}
        with mock.patch.object(dt.subprocess, "run",
                               _fake_gh(exploding_timeline, issue_json=closed_issue)):
            fields = dt._github_fields(3393, 3467, slice_requested=False)
        # ABSENT, never defaulted — a fabricated value would be read as a measurement.
        self.assertNotIn("issue_open_at_merge", fields)
        self.assertNotIn("issue_close_event_at", fields)
        self.assertEqual(fields["closed_at"], "2026-07-01T00:00:00Z")

    def test_the_replay_still_runs_when_a_branch_WILL_read_it(self):
        """The skip must be scoped to the paths that read nothing, or it removes the fix.

        Both readers are covered: `--slice` on a closed-now issue, and a null `closed_at`
        without `--slice` (the propagation-window / reopened shapes).
        """
        closed_issue = {"createdAt": "2026-06-10T00:00:00Z",
                        "closedAt": "2026-07-01T00:00:00Z",
                        "labels": [{"name": "P1"}, {"name": "oracle"}], "url": ISSUE_URL}
        pages = [[_ev("closed", "2026-06-10T04:00:00Z")]]
        for label, issue_json, slice_requested in (
            ("--slice on a closed-now issue", closed_issue, True),
            ("null closed_at without --slice", None, False),
        ):
            with self.subTest(label):
                with mock.patch.object(dt.subprocess, "run",
                                       _fake_gh(pages, issue_json=issue_json)):
                    fields = dt._github_fields(3393, 3467,
                                               slice_requested=slice_requested)
                self.assertIs(fields["issue_open_at_merge"], True)

    def test_it_carries_the_closing_events_timestamp_for_the_refusal_message(self):
        with mock.patch.object(dt.subprocess, "run",
                               _fake_gh([[_ev("closed", "2026-06-10T02:00:00Z")]])):
            fields = dt._github_fields(3393, 3467, slice_requested=True)
        self.assertIs(fields["issue_open_at_merge"], False)
        self.assertEqual(fields["issue_close_event_at"], "2026-06-10T02:00:00Z")

    def test_it_refuses_a_pr_with_no_usable_merged_at(self):
        """The replay needs an authoritative merge instant; an unmerged PR has none."""
        for bad in (None, "", "not-a-time"):
            with self.subTest(mergedAt=bad):
                fake = _fake_gh([[]], pr_json={
                    "createdAt": "2026-06-10T01:00:00Z", "mergedAt": bad,
                    "closingIssuesReferences": []})
                with mock.patch.object(dt.subprocess, "run", fake):
                    with self.assertRaises(SystemExit) as cm:
                        dt._github_fields(3393, 3467, slice_requested=True)
                # absent/blank is named as gh's `mergedAt`; a malformed instant is named by
                # the shared strict-RFC-3339 check as `merged_at` — both are named refusals
                self.assertRegex(str(cm.exception), r"merged.?[aA]t")

    def test_the_state_reason_mechanism_is_gone_not_layered(self):
        """#3550's `stateReason` proxy answered 'has this issue EVER been closed'; the replay
        answers the real question. Two mechanisms approximating one fact is the defect, not
        the fix (issue #3559's own design note), so the proxy is REMOVED — asserted here so
        it cannot quietly return alongside the replay."""
        self.assertFalse(hasattr(dt, "_assert_never_closed"))
        seen = []

        def fake_run(argv, **kw):
            seen.append(argv)
            return _fake_gh([[]])(argv, **kw)

        with mock.patch.object(dt.subprocess, "run", fake_run):
            fields = dt._github_fields(3393, 3467, slice_requested=True)
        self.assertNotIn("state_reason", fields)
        joined = " ".join(a for argv in seen for a in argv)
        self.assertNotIn("stateReason", joined)


class SliceFromTimelineTests(unittest.TestCase):
    """End-to-end through `record --from-json`: the classification the ACs name.

        slice  <=>  (the issue was OPEN at mergedAt)  AND  (this PR closes nothing)

    The conjunction is load-bearing FOREVER, not a propagation-window stopgap: GitHub records
    an auto-close AFTER the merge, so an ordinary completed delivery whose PR declares
    `Closes #N` was ALSO literally open at `mergedAt`. Dropping the second operand would file
    a false slice record for essentially every ordinary delivery.
    """

    def _ghfields(self, tmp, *, closed_at, open_at_merge=True, close_event_at=_OMIT,
                  closes=False, merged_at="2026-06-10T03:00:00Z", issue=3393, pr=3467):
        fields = {
            "issue": issue, "pr": pr,
            "created_at": "2026-06-10T00:00:00Z",
            "pr_opened_at": "2026-06-10T01:00:00Z",
            "merged_at": merged_at,
            "closed_at": closed_at,
            "priority": "P1", "routing": "oracle",
        }
        if open_at_merge is not _OMIT:
            fields["issue_open_at_merge"] = open_at_merge
        if closes is not _OMIT:
            fields["pr_closes_this_issue"] = closes
        fields["issue_close_event_at"] = (None if close_event_at is _OMIT else close_event_at)
        p = tmp / "ghfields.json"
        p.write_text(json.dumps(fields))
        return str(p)

    def _argv(self, ledger, ghfields, *extra, issue=3393, pr=3467):
        return ["record", "--ledger", str(ledger),
                "--issue", str(issue), "--pr", str(pr), "--slug", "slice-from-timeline",
                "--gate", "pass", "--gate-runs", "1",
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "0", "--rework", "0",
                "--from-json", ghfields, *extra]

    # ---- ACCEPT ------------------------------------------------------------------
    def test_slice_accepted_for_an_issue_closed_NOW_but_open_at_merged_at(self):
        """The headline unblock: a genuine slice stays stampable after its issue closes. The
        record still carries closed_at null and bounds cycle_time_s on mergedAt, because it
        states what was true at DELIVERY time."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at="2026-07-01T00:00:00Z", open_at_merge=True)
            self.assertEqual(0, dt.main(self._argv(ledger, gh, "--slice")))
            rec = json.loads(ledger.read_text().strip())
            self.assertIsNone(rec["closed_at"])
            self.assertEqual(rec["cycle_time_s"], 3 * 3600)   # created -> mergedAt

    def test_an_asserted_classification_says_so_on_stderr(self):
        """A record whose kind rests on the OPERATOR must not look like one the tool proved.

        Both surviving classifications can rest partly on the operator (issue #3559,
        REQ-3559-02 option C), because `open_at_merge=True` + `closes=False` + a non-null
        `closed_at` is observationally identical for a genuine late slice and a completed
        delivery whose PR omitted `Closes #N`. The tool refuses only what it can DISPROVE, so
        it must SAY when the affirmation was a human's. Asserted here because an unasserted
        note is one nobody notices going missing.
        """
        for label, extra, expect_kind in (
            ("late-stamped slice", ("--slice",), "SLICE delivery"),
            ("completed on the omission of --slice", (), "COMPLETED delivery"),
        ):
            with self.subTest(label):
                with tempfile.TemporaryDirectory() as d:
                    tmp = Path(d)
                    ledger = tmp / "ledger.jsonl"
                    gh = self._ghfields(tmp, closed_at="2026-07-01T00:00:00Z",
                                        open_at_merge=True, closes=False)
                    err = io.StringIO()
                    with contextlib.redirect_stderr(err):
                        self.assertEqual(0, dt.main(self._argv(ledger, gh, *extra)))
                    msg = err.getvalue()
                    self.assertIn("YOUR ASSERTION", msg)
                    self.assertIn(expect_kind, msg)
                    self.assertIn("#3559", msg)

    def test_a_completed_delivery_with_a_closing_reference_emits_no_note(self):
        """The normal `Closes #N` path is PROVEN, so claiming an assertion there is false.

        Every non-slice completed record used to be labelled an operator assertion, including
        the ordinary path where the PR's own closing declaration proves the classification
        (roborev round 2, issue #3559). That made the note inaccurate exactly where deliveries
        actually flow — and a note that cries wolf on the common path is one readers learn to
        skip, costing the signal it exists to carry.
        """
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at="2026-07-01T00:00:00Z",
                                open_at_merge=True, closes=True)
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                self.assertEqual(0, dt.main(self._argv(ledger, gh)))
            self.assertNotIn("YOUR ASSERTION", err.getvalue())
            self.assertEqual(json.loads(ledger.read_text().strip())["closed_at"],
                             "2026-07-01T00:00:00Z")

    def test_a_refused_invocation_never_claims_a_record_was_written(self):
        """The note says "recorded as", so it must not outlive a failed write.

        `build_record` runs before schema validation, duplicate detection and the append, so
        building the note there and printing it immediately let a REFUSED invocation announce
        a classification that was never recorded (roborev round 2, issue #3559). Driven here
        through the duplicate-cycle refusal, which fails after the record is built.
        """
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at="2026-07-01T00:00:00Z",
                                open_at_merge=True, closes=False)
            # first stamp lands and legitimately notes its asserted basis
            first = io.StringIO()
            with contextlib.redirect_stderr(first):
                self.assertEqual(0, dt.main(self._argv(ledger, gh, "--slice")))
            self.assertIn("YOUR ASSERTION", first.getvalue())
            before = ledger.read_text()
            # same (issue, pr) again -> refused as a duplicate cycle, so NOTHING is recorded
            second = io.StringIO()
            with contextlib.redirect_stderr(second):
                self.assertEqual(1, dt.main(self._argv(ledger, gh, "--slice")))
            self.assertNotIn("YOUR ASSERTION", second.getvalue())
            self.assertNotIn("recorded as", second.getvalue())
            self.assertEqual(ledger.read_text(), before)   # ledger untouched

    def test_a_proven_classification_emits_no_assertion_note(self):
        """The note must be SCOPED to the undecidable case, or it means nothing.

        A slice of an issue that is still open, and a completed delivery inside the
        propagation window, are decided by evidence — claiming those rest on an assertion
        would make the note noise that readers learn to skip.
        """
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at=None, open_at_merge=True, closes=False)
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                self.assertEqual(0, dt.main(self._argv(ledger, gh, "--slice")))
            self.assertNotIn("YOUR ASSERTION", err.getvalue())

    def test_slice_accepted_for_an_issue_reopened_BEFORE_the_merge(self):
        """The false refusal #3550 knowingly accepted: a real slice of a genuinely-open
        issue whose reopen preceded this PR's merge."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at=None, open_at_merge=True)
            self.assertEqual(0, dt.main(self._argv(ledger, gh, "--slice")))
            self.assertIsNone(json.loads(ledger.read_text().strip())["closed_at"])

    def test_the_three_owed_3393_records_are_stampable(self):
        """Real data, pinned: #3393's timeline is `closed 2026-08-30T16:53:55Z` then
        `reopened 2026-08-30T16:58:51Z`, and PRs #3407/#3429/#3467 all merged BEFORE that
        close with closingIssuesReferences []. All three must ACCEPT --slice; a merge AFTER
        the close must not."""
        merges = {3407: "2026-08-28T07:21:36Z", 3429: "2026-08-28T11:49:27Z",
                  3467: "2026-08-30T05:49:25Z"}
        timeline = [[_ev("closed", "2026-08-30T16:53:55Z"),
                     _ev("reopened", "2026-08-30T16:58:51Z")]]
        for pr, merged in merges.items():
            with self.subTest(pr=pr):
                with mock.patch.object(dt.subprocess, "run", _fake_gh(timeline)):
                    open_at, close_at = dt._issue_state_at(ISSUE_URL, 3393, merged)
                self.assertIs(open_at, True)
                self.assertIsNone(close_at)
                with tempfile.TemporaryDirectory() as d:
                    tmp = Path(d)
                    ledger = tmp / "ledger.jsonl"
                    gh = self._ghfields(tmp, closed_at=None, open_at_merge=open_at,
                                        merged_at=merged, pr=pr)
                    self.assertEqual(0, dt.main(self._argv(ledger, gh, "--slice", pr=pr)))
                    self.assertIsNone(json.loads(ledger.read_text().strip())["closed_at"])
        # control: a PR merged AFTER the close was not shipping an open issue
        with mock.patch.object(dt.subprocess, "run", _fake_gh(timeline)):
            self.assertEqual(
                dt._issue_state_at(ISSUE_URL, 3393, "2026-08-30T16:55:00Z"),
                (False, "2026-08-30T16:53:55Z"))

    # ---- REFUSE ------------------------------------------------------------------
    def test_slice_refused_when_the_issue_was_provably_CLOSED_at_merged_at(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at="2026-06-10T02:00:00Z", open_at_merge=False,
                                close_event_at="2026-06-10T02:00:00Z")
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._argv(ledger, gh, "--slice"))
            msg = str(cm.exception)
            self.assertIn("--slice", msg)
            self.assertIn("CLOSED", msg)
            self.assertIn("2026-06-10T02:00:00Z", msg)   # names the closing event
            self.assertIn("hand-append", msg.lower())
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_slice_refused_when_the_issue_was_reopened_AFTER_this_pr_merged(self):
        """Open NOW (closed_at null) but CLOSED at mergedAt: the delivery completed the
        issue and must be recorded as completed, not as a slice."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at=None, open_at_merge=False,
                                close_event_at="2026-06-10T02:30:00Z")
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._argv(ledger, gh, "--slice"))
            msg = str(cm.exception)
            self.assertIn("CLOSED", msg)
            self.assertIn("2026-06-10T02:30:00Z", msg)
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_slice_refused_when_this_pr_declares_it_closes_the_issue_closed_now(self):
        """THE conjunction's second operand, on the path #3550 could not reach: with the
        timeline accepting a closed-NOW issue, an ordinary completed delivery (PR closes #N,
        issue closed now, open at mergedAt because the auto-close lands after the merge)
        would otherwise be accepted as a slice."""
        for closed_at in (None, "2026-06-10T03:00:05Z", "2026-07-01T00:00:00Z"):
            with self.subTest(closed_at=closed_at), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                gh = self._ghfields(tmp, closed_at=closed_at, open_at_merge=True, closes=True)
                with self.assertRaises(SystemExit) as cm:
                    dt.main(self._argv(ledger, gh, "--slice"))
                msg = str(cm.exception)
                self.assertIn("CLOSES issue #3393", msg)
                self.assertIn("WITHOUT --slice", msg)
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_a_completed_delivery_with_no_closing_reference_is_still_refused(self):
        """The AC for the no-`Closes #N` shape. It is decidable in exactly one direction: a
        `closed` event at or before mergedAt identifies the completed delivery whether or not
        the PR declared a closing reference.

        The OTHER direction is UNDECIDABLE and is deliberately not claimed: a completed
        delivery whose PR omits `Closes #N` and whose issue is closed BY HAND days later is
        observationally IDENTICAL to a genuine slice whose issue is later completed by another
        PR — both are open-at-mergedAt, close-nothing, closed-later. No GitHub signal
        separates them; the difference is intent. `--slice` is an operator ASSERTION, and this
        tool's job is to REFUSE it wherever it can be DISPROVED."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at="2026-06-10T02:00:00Z", open_at_merge=False,
                                close_event_at="2026-06-10T02:00:00Z", closes=False)
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._argv(ledger, gh, "--slice"))
            self.assertIn("CLOSED", str(cm.exception))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_no_slice_flag_on_an_open_at_merge_issue_still_routes_to_the_flag(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at=None, open_at_merge=True)
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._argv(ledger, gh))
            msg = str(cm.exception)
            self.assertIn("--slice", msg)
            self.assertIn("FORBIDDEN", msg)
            self.assertIn("hand-append", msg.lower())

    def test_no_slice_flag_on_a_reopened_after_merge_issue_names_the_completion(self):
        """closed_at is null (reopened since), so the completed path has no closure timestamp
        and the slice path is disproved. Both are refusals — the operator must not be told to
        pass --slice, which the next invocation would refuse."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at=None, open_at_merge=False,
                                close_event_at="2026-06-10T02:30:00Z")
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._argv(ledger, gh))
            msg = str(cm.exception)
            self.assertIn("2026-06-10T02:30:00Z", msg)
            # It must NOT advise waiting for a reclose (roborev round 4): the completed path
            # reads the issue's CURRENT closedAt, so a later close -- which belongs to a
            # LATER delivery cycle -- would be attributed to THIS pr. An earlier version of
            # this refusal told the operator to do exactly that.
            self.assertNotIn("again reflects that completion", msg)
            self.assertIn("DOES NOT FIX IT", msg)
            self.assertIn("later delivery cycle", msg)
            self.assertNotIn("Pass --slice", msg)
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    # ---- the seam field is a MEASUREMENT ----------------------------------------
    def test_an_unmeasured_issue_open_at_merge_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at=None, open_at_merge=_OMIT)
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._argv(ledger, gh, "--slice"))
            self.assertIn("issue_open_at_merge", str(cm.exception))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_a_non_boolean_issue_open_at_merge_is_refused(self):
        for bogus in (None, 0, 1, "", "true", "false", [], {}, "2026-06-10T00:00:00Z"):
            with self.subTest(value=bogus), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                gh = self._ghfields(tmp, closed_at=None, open_at_merge=bogus)
                with self.assertRaises(SystemExit) as cm:
                    dt.main(self._argv(ledger, gh, "--slice"))
                self.assertIn("must be a boolean", str(cm.exception))
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_an_ordinary_completed_delivery_needs_no_timeline_field(self):
        """The replay decides SLICE classification only. A completed delivery (closed_at
        present, no --slice) reads its terminal timestamp from closed_at, so requiring the
        seam field there would refuse a correct invocation for a field no branch reads."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = self._ghfields(tmp, closed_at="2026-06-10T04:00:00Z",
                                open_at_merge=_OMIT, closes=_OMIT)
            self.assertEqual(0, dt.main(self._argv(ledger, gh)))
            rec = json.loads(ledger.read_text().strip())
            self.assertEqual(rec["closed_at"], "2026-06-10T04:00:00Z")
            self.assertEqual(rec["cycle_time_s"], 4 * 3600)


if __name__ == "__main__":
    unittest.main(verbosity=2)
