#!/usr/bin/env python3
"""Unit tests for scripts/parity-failure-issue.py (stdlib unittest, no network).

Covers the public surface promised by the spec (issue #1028):
  - fingerprint stability across run-noise; distinctness across differing fields
  - dedup: marker match -> UPDATE (no new issue); no match -> CREATE
  - degraded fallback is surfaced (notice), not silent
  - zero failures parsed from a `failure`-concluded run is surfaced as an anomaly
  - token-free / dry-run logic runs without touching GitHub

Run standalone:   python3 scripts/tests/test_parity_failure_issue.py
"""

import argparse
import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]

# The module file name has a hyphen, so load it by path.
_spec = importlib.util.spec_from_file_location(
    "parity_failure_issue", SCRIPTS / "parity-failure-issue.py")
pfi = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pfi)


def _failure(**overrides) -> dict:
    base = {
        "scenario_id": "cass.compression.lz4_roundtrip",
        "workflow": "compression-corruption-parity.yml",
        "test_target": "sstable_parity_corruption_verify",
        "component_path": "test-data/datasets/corruption/test_comp_corrupt",
        "failure_class": "DigestMismatch",
    }
    base.update(overrides)
    return base


def _args(**overrides):
    ns = argparse.Namespace(
        failures_json=None, summary_file=None, open_issues_json=None,
        run_url="https://gh/run/1", dry_run=True, conclusion="failure",
        tier="nightly_docker", repro="cargo test -p x", workflow="",
        workflow_file="tombstone-ttl-parity.yml",
    )
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


def _write(tmp: Path, name: str, obj) -> str:
    p = tmp / name
    p.write_text(json.dumps(obj) if not isinstance(obj, str) else obj)
    return str(p)


class FingerprintTests(unittest.TestCase):
    def test_stable_across_run_noise(self):
        # Same logical failure — the five fields are identical. Any timestamps / run-ids
        # / counts live outside the fingerprint input, so they cannot change it.
        fp1 = pfi.compute_fingerprint(_failure())
        fp2 = pfi.compute_fingerprint(_failure())
        self.assertEqual(fp1, fp2)

    def test_case_and_whitespace_insensitive(self):
        fp1 = pfi.compute_fingerprint(_failure())
        fp2 = pfi.compute_fingerprint(_failure(
            scenario_id="  CASS.Compression.LZ4_Roundtrip ",
            failure_class="digestmismatch"))
        self.assertEqual(fp1, fp2)

    def test_differing_fields_differ(self):
        base = pfi.compute_fingerprint(_failure())
        for field, other in [
            ("scenario_id", "cass.other.scenario"),
            ("workflow", "cql-type-parity.yml"),
            ("test_target", "other_target"),
            ("component_path", "other/path"),
            ("failure_class", "ChunkDecompressionError"),
        ]:
            self.assertNotEqual(
                base, pfi.compute_fingerprint(_failure(**{field: other})),
                f"changing {field} must change the fingerprint")

    def test_versioned_and_short_form(self):
        fp = pfi.compute_fingerprint(_failure())
        self.assertEqual(len(pfi.short_fingerprint(fp)), 12)
        self.assertIn(pfi.short_fingerprint(fp), pfi.marker(fp))


class DedupTests(unittest.TestCase):
    def test_marker_match_updates_no_new_issue(self):
        failure = _failure()
        fp = pfi.compute_fingerprint(failure)
        open_issues = [{"number": 42, "body": f"stuff\n{pfi.marker(fp)}\nmore"}]
        plan = pfi._plan_failure(failure, open_issues, _args())
        self.assertEqual(plan["action"], "update")
        self.assertEqual(plan["issue_number"], 42)

    def test_no_match_creates(self):
        failure = _failure()
        open_issues = [{"number": 7, "body": "<!-- PARITY-FAIL:deadbeef0000 -->"}]
        plan = pfi._plan_failure(failure, open_issues, _args())
        self.assertEqual(plan["action"], "create")
        self.assertIn(pfi.PARENT_EPIC, plan["body"])
        self.assertIn(pfi.marker(plan["fingerprint"]), plan["body"])
        self.assertIn("nightly_docker", plan["body"])
        self.assertIn("cargo test -p x", plan["body"])

    def test_file_end_to_end_update_via_seams(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            failure = _failure()
            fp = pfi.compute_fingerprint(failure)
            fj = _write(tmp, "failures.json", [failure])
            oj = _write(tmp, "open.json", [{"number": 99, "body": pfi.marker(fp)}])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_file(_args(failures_json=fj, open_issues_json=oj))
            self.assertEqual(rc, 0)
            self.assertIn("update #99", out.getvalue())

    def test_within_run_duplicate_fingerprints_create_once(self):
        # R1: two input entries sharing ONE fingerprint (and no pre-existing open issue)
        # must yield exactly ONE create — never two duplicate parity-failure issues.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            failure = _failure()
            fj = _write(tmp, "failures.json", [failure, failure])
            oj = _write(tmp, "open.json", [])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_file(_args(failures_json=fj, open_issues_json=oj))
            self.assertEqual(rc, 0)
            rendered = out.getvalue()
            # Exactly one create plan (and thus one issue), not two.
            self.assertEqual(rendered.count("--- DRY RUN (create) ---"), 1)
            self.assertEqual(rendered.count("--- DRY RUN (update) ---"), 0)

    def test_within_run_duplicate_still_updates_preexisting_open_issue(self):
        # R1 cross-run invariant preserved: even with duplicate input entries, a
        # pre-EXISTING open issue for that fingerprint yields an UPDATE — never a create.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            failure = _failure()
            fp = pfi.compute_fingerprint(failure)
            fj = _write(tmp, "failures.json", [failure, failure])
            oj = _write(tmp, "open.json", [{"number": 55, "body": pfi.marker(fp)}])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_file(_args(failures_json=fj, open_issues_json=oj))
            self.assertEqual(rc, 0)
            rendered = out.getvalue()
            self.assertEqual(rendered.count("--- DRY RUN (create) ---"), 0)
            self.assertEqual(rendered.count("--- DRY RUN (update) ---"), 1)
            self.assertIn("update #55", rendered)

    def test_update_refreshes_latest_failure_link_in_body(self):
        # D4 / R1 scenario 2: a repeat failure must refresh the existing issue body's
        # "Latest failure" run link, then post a recurrence comment.
        failure = _failure()
        fp = pfi.compute_fingerprint(failure)
        stale_body = pfi.issue_body(
            failure, fp, "https://gh/run/OLD", "nightly_docker", "cargo test -p x")
        self.assertIn("https://gh/run/OLD", stale_body)
        open_issues = [{"number": 42, "body": stale_body}]
        args = _args(run_url="https://gh/run/NEW")
        plan = pfi._plan_failure(failure, open_issues, args)
        self.assertEqual(plan["action"], "update")
        # Body is refreshed to the new run link; stale link is gone.
        self.assertIn("https://gh/run/NEW", plan["new_body"])
        self.assertNotIn("https://gh/run/OLD", plan["new_body"])
        # Marker + repro survive the refresh.
        self.assertIn(pfi.marker(fp), plan["new_body"])
        self.assertIn("cargo test -p x", plan["new_body"])
        # A dated recurrence comment is still posted.
        self.assertIn("https://gh/run/NEW", plan["comment"])
        # End-to-end via seams: apply emits both the body refresh and the comment.
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            pfi._apply_plan(plan, dry_run=True)
        rendered = out.getvalue()
        self.assertIn("refreshed latest-failure link", rendered)
        self.assertIn("https://gh/run/NEW", rendered)

    def test_refresh_latest_failure_appends_when_section_absent(self):
        # Hand-edited / legacy body without a "Latest failure" section: the fresh
        # section is appended so the newest run link is never lost.
        body = "<!-- PARITY-FAIL:deadbeef0000 -->\nsome notes without the section"
        refreshed = pfi.refresh_latest_failure(body, "https://gh/run/NEW")
        self.assertIn("## Latest failure", refreshed)
        self.assertIn("https://gh/run/NEW", refreshed)
        self.assertIn("some notes without the section", refreshed)


class TierTests(unittest.TestCase):
    def _created_body(self, tier: str, workflow: str) -> str:
        # File one failure (no pre-existing issue) at the given tier; return the create body.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            failure = _failure(workflow=workflow)
            fj = _write(tmp, "failures.json", [failure])
            oj = _write(tmp, "open.json", [])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_file(_args(
                    failures_json=fj, open_issues_json=oj, tier=tier))
            self.assertEqual(rc, 0)
            return out.getvalue()

    def test_exhaustive_lane_records_exhaustive_tier(self):
        # Fix A (round 7): the exhaustive-regeneration lane must record tier
        # `exhaustive_regeneration` in the filed issue body (spec R1).
        body = self._created_body("exhaustive_regeneration", "exhaustive-regeneration.yml")
        self.assertIn("**CI tier:** `exhaustive_regeneration`", body)
        self.assertNotIn("**CI tier:** `nightly_docker`", body)

    def test_nightly_lane_records_nightly_tier(self):
        # A nightly parity lane records the default `nightly_docker` tier.
        body = self._created_body("nightly_docker", "tombstone-ttl-parity.yml")
        self.assertIn("**CI tier:** `nightly_docker`", body)
        self.assertNotIn("exhaustive_regeneration", body)


class DegradedFallbackTests(unittest.TestCase):
    def test_missing_artifact_surfaces_notice_and_uses_summary(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            summary = tmp / "parity_summary.md"
            summary.write_text("PASS cass.a\nFAIL cass.compression.lz4_roundtrip extra text\n")
            oj = _write(tmp, "open.json", [])
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = pfi.cmd_file(_args(
                    failures_json=None, summary_file=str(summary), open_issues_json=oj))
            self.assertEqual(rc, 0)
            self.assertIn("::notice::", err.getvalue())
            self.assertIn("degraded", err.getvalue().lower())
            self.assertIn("degraded=true", out.getvalue())

    def test_zero_parsed_on_failure_is_anomaly(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            empty_summary = tmp / "parity_summary.md"
            empty_summary.write_text("PASS everything\n")
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = pfi.cmd_file(_args(
                    failures_json=None, summary_file=str(empty_summary),
                    conclusion="failure"))
            self.assertEqual(rc, 2)
            self.assertIn("::error::", err.getvalue())
            self.assertIn("anomaly", err.getvalue().lower())

    def test_degraded_no_summary_on_failure_is_anomaly_not_silent(self):
        # R3: the workflow's genuine no-artifact / no-downloadable-summary case invokes
        # `file` with NEITHER --failures-json NOR --summary-file. On a failure-concluded
        # run that must surface a loud anomaly, never a silent zero no-op.
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = pfi.cmd_file(_args(
                failures_json=None, summary_file=None, conclusion="failure"))
        self.assertEqual(rc, 2)
        self.assertIn("::notice::", err.getvalue())  # degraded path surfaced
        self.assertIn("degraded", err.getvalue().lower())
        self.assertIn("::error::", err.getvalue())  # anomaly surfaced
        self.assertIn("anomaly", err.getvalue().lower())

    def test_degraded_summary_file_parses_failures_and_files(self):
        # R3: when a summary artifact IS located, load_degraded parses it and the run
        # files an issue (dry-run) rather than doing nothing.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            summary = tmp / "parity_summary.md"
            summary.write_text("PASS cass.ok\nFAIL cass.compaction.stcs_merge details\n")
            oj = _write(tmp, "open.json", [])
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = pfi.cmd_file(_args(
                    failures_json=None, summary_file=str(summary),
                    open_issues_json=oj, conclusion="failure"))
            self.assertEqual(rc, 0)
            self.assertIn("degraded=true", out.getvalue())
            # A create plan was emitted for the parsed scenario (dry-run output).
            self.assertIn("cass.compaction.stcs_merge", out.getvalue())

    def test_degraded_record_carries_known_lane_identity(self):
        # The degraded record must record the REAL lane workflow filename (matchable by the
        # green-run `resolve --workflow` filter) and a STABLE lane-level sentinel for
        # test_target/component_path — never the free-floating "unknown".
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            summary = tmp / "parity_summary.md"
            summary.write_text("FAIL cass.ttl.expiry details\n")
            recs = pfi.load_degraded(str(summary), "tombstone-ttl-parity.yml")
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["workflow"], "tombstone-ttl-parity.yml")
        self.assertEqual(recs[0]["test_target"], "tombstone-ttl-parity.yml:degraded")
        self.assertEqual(recs[0]["component_path"], "tombstone-ttl-parity.yml:degraded")

    def test_degraded_fingerprint_stable_across_runs_same_lane(self):
        # Two independent degraded parses of the same lane + scenario must fingerprint
        # identically (stable cross-run dedup of degraded failures).
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            s1 = tmp / "run1.md"
            s2 = tmp / "run2.md"
            s1.write_text("FAIL cass.ttl.expiry run1-extra\n")
            s2.write_text("FAIL cass.ttl.expiry run2-different-noise\n")
            r1 = pfi.load_degraded(str(s1), "tombstone-ttl-parity.yml")
            r2 = pfi.load_degraded(str(s2), "tombstone-ttl-parity.yml")
        self.assertEqual(
            pfi.compute_fingerprint(r1[0]), pfi.compute_fingerprint(r2[0]))

    def test_degraded_without_lane_fails_closed_with_warning(self):
        # If the lane filename is somehow unavailable, load_degraded fails closed (no
        # records) with a surfaced ::warning:: rather than writing an unresolvable identity.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            summary = tmp / "parity_summary.md"
            summary.write_text("FAIL cass.ttl.expiry details\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                recs = pfi.load_degraded(str(summary), None)
        self.assertEqual(recs, [])
        self.assertIn("::warning::", err.getvalue())

    def test_zero_parsed_on_success_is_noop(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = pfi.cmd_file(_args(failures_json=None, summary_file=None,
                                    conclusion="success"))
        self.assertEqual(rc, 0)


class StructuredArtifactTests(unittest.TestCase):
    def test_structured_artifact_drives_fingerprinting(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            fj = _write(tmp, "failures.json", [_failure()])
            failures = pfi.load_failures(fj)
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]["failure_class"], "DigestMismatch")

    def test_malformed_artifact_is_hard_error_not_empty(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            bad = _write(tmp, "bad.json", [{"scenario_id": "x"}])  # missing fields
            with self.assertRaises(SystemExit):
                pfi.load_failures(bad)

    def test_absent_artifact_raises_degraded(self):
        with self.assertRaises(pfi.DegradedParse):
            pfi.load_failures(None)
        with self.assertRaises(pfi.DegradedParse):
            pfi.load_failures("/nonexistent/parity-failures.json")


class ResolveTests(unittest.TestCase):
    def test_resolve_comments_but_does_not_close(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            failure = _failure()
            fp = pfi.compute_fingerprint(failure)
            fj = _write(tmp, "failures.json", [failure])
            oj = _write(tmp, "open.json", [{"number": 55, "body": pfi.marker(fp)}])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_resolve(_args(failures_json=fj, open_issues_json=oj))
            self.assertEqual(rc, 0)
            body = out.getvalue()
            self.assertIn("#55", body)
            self.assertIn("Not auto-closed", pfi.resolution_comment("u"))

    def test_fingerprint_resolve_dedups_duplicate_inputs_within_run(self):
        # LOW fix (round 10): the resolve --failures-json path must dedup its inputs by
        # fingerprint WITHIN one invocation. Two entries sharing a fingerprint that map to
        # the SAME open issue must post EXACTLY ONE resolution comment (not two), matching
        # the within-run dedup already applied to `cmd_file`.
        calls = self._capture_gh()
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            failure = _failure()
            fp = pfi.compute_fingerprint(failure)
            # Two duplicate entries (case/whitespace-normalized to the same fingerprint).
            dup = _failure(scenario_id="  CASS.Compression.LZ4_Roundtrip ",
                           failure_class="digestmismatch")
            self.assertEqual(fp, pfi.compute_fingerprint(dup))
            fj = _write(tmp, "failures.json", [failure, dup])
            oj = _write(tmp, "open.json", [{"number": 55, "body": pfi.marker(fp)}])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_resolve(_args(
                    failures_json=fj, open_issues_json=oj,
                    run_url="https://gh/run/DUP", dry_run=False))
            self.assertEqual(rc, 0)
        comment_calls = [c for c in calls
                         if len(c) > 2 and c[1] == "issue" and c[2] == "comment"]
        self.assertEqual(len(comment_calls), 1,
                         "duplicate fingerprints must yield exactly one resolution comment")
        self.assertEqual(comment_calls[0][3], "55")
        self.assertFalse(any("close" in c for c in calls),
                         "resolve must never close an issue")

    def _capture_gh(self):
        """Patch pfi._gh to record argv and never touch GitHub; returns the call log."""
        calls = []

        def fake_gh(argv):
            calls.append(argv)
            return ""  # gh create would return a URL; comment returns nothing meaningful

        self._orig_gh = pfi._gh
        pfi._gh = fake_gh
        self.addCleanup(lambda: setattr(pfi, "_gh", self._orig_gh))
        return calls

    def test_green_lane_resolve_comments_all_open_and_never_closes(self):
        # R4 green-run wiring: no --failures-json → lane-scoped resolve posts a resolution
        # comment on EVERY open parity-failure issue and NEVER closes any.
        calls = self._capture_gh()
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            oj = _write(tmp, "open.json", [
                {"number": 55, "body": "<!-- PARITY-FAIL:aaaaaaaaaaaa -->"},
                {"number": 56, "body": "<!-- PARITY-FAIL:bbbbbbbbbbbb -->"},
            ])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_resolve(_args(
                    failures_json=None, open_issues_json=oj,
                    run_url="https://gh/run/GREEN", dry_run=False))
            self.assertEqual(rc, 0)
        # One resolution comment per open issue.
        comment_calls = [c for c in calls if len(c) > 2 and c[1] == "issue" and c[2] == "comment"]
        self.assertEqual(len(comment_calls), 2)
        commented = {c[3] for c in comment_calls}
        self.assertEqual(commented, {"55", "56"})
        for c in comment_calls:
            self.assertIn("https://gh/run/GREEN", " ".join(c))
        # NEVER closes: no `gh issue close` call is ever emitted.
        self.assertFalse(any("close" in c for c in calls),
                         "resolve must never close an issue")

    def test_green_lane_resolve_workflow_filter_scopes_issues(self):
        # Fix A: a green lane's resolve is scoped by --workflow <filename> so it comments
        # ONLY on that lane's issues and leaves every other lane's issue untouched.
        calls = self._capture_gh()
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            oj = _write(tmp, "open.json", [
                {"number": 55, "body": "**Workflow:** `compression-corruption-parity.yml`"},
                {"number": 56, "body": "**Workflow:** `tombstone-ttl-parity.yml`"},
                {"number": 57, "body": "**Workflow:** `cql-type-parity.yml`"},
            ])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_resolve(_args(
                    failures_json=None, open_issues_json=oj,
                    workflow="compression-corruption-parity.yml", dry_run=False))
            self.assertEqual(rc, 0)
        comment_calls = [c for c in calls if len(c) > 2 and c[2] == "comment"]
        # Only lane X's issue (#55) is commented on.
        self.assertEqual([c[3] for c in comment_calls], ["55"])
        commented = {c[3] for c in comment_calls}
        # Unrelated lanes' issues (#56 tombstone, #57 cql-type) are left untouched.
        self.assertNotIn("56", commented)
        self.assertNotIn("57", commented)
        self.assertFalse(any("close" in c for c in calls))

    def _run_resolve(self, calls_sink, open_issues, workflow, run_url):
        """Run a lane-scoped green resolve against `open_issues`; return the call log slice."""
        with tempfile.TemporaryDirectory() as d:
            oj = _write(Path(d), "open.json", open_issues)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_resolve(_args(
                    failures_json=None, open_issues_json=oj,
                    workflow=workflow, run_url=run_url, dry_run=False))
            self.assertEqual(rc, 0)
        return out.getvalue()

    @staticmethod
    def _body_after_resolve(calls, number: str):
        """Extract the body written by the last `gh issue edit #<number> --body ...`."""
        for c in reversed(calls):
            if len(c) >= 6 and c[1] == "issue" and c[2] == "edit" and c[3] == number:
                return c[5]
        return None

    def test_green_resolve_is_idempotent_one_comment_per_transition(self):
        # Fix B (round 7): two consecutive green resolves on the SAME still-open issue
        # (never auto-closed) must post EXACTLY ONE resolution comment. The first resolve
        # stamps a resolved marker; the second sees it and skips (no duplicate comment).
        wf = "compression-corruption-parity.yml"
        issue_body = f"<!-- PARITY-FAIL:aaaaaaaaaaaa -->\n**Workflow:** `{wf}`\n"
        # Green run #1.
        calls = self._capture_gh()
        self._run_resolve(calls, [{"number": 55, "body": issue_body}], wf, "https://gh/run/G1")
        comment1 = [c for c in calls if len(c) > 2 and c[2] == "comment"]
        self.assertEqual(len(comment1), 1, "first green run comments exactly once")
        stamped = self._body_after_resolve(calls, "55")
        self.assertIsNotNone(stamped)
        self.assertTrue(pfi.is_resolved(stamped), "first resolve stamps the resolved marker")
        # Green run #2 sees the stamped body (issue never closed) → no duplicate comment.
        calls2 = self._capture_gh()
        out = self._run_resolve(calls2, [{"number": 55, "body": stamped}], wf, "https://gh/run/G2")
        comment2 = [c for c in calls2 if len(c) > 2 and c[2] == "comment"]
        self.assertEqual(len(comment2), 0, "second green run must NOT re-comment (idempotent)")
        self.assertIn("already marked resolved", out)
        self.assertFalse(any("close" in c for c in calls + calls2))

    def test_failure_recurrence_re_arms_resolution_comment(self):
        # Fix B: a failure recurrence BETWEEN two green runs clears the resolved marker, so
        # the second green run posts a resolution comment AGAIN (transition re-armed).
        wf = "compression-corruption-parity.yml"
        failure = _failure(workflow=wf)
        fp = pfi.compute_fingerprint(failure)
        base_body = f"{pfi.marker(fp)}\n**Workflow:** `{wf}`\n{pfi.latest_failure_section('https://gh/run/OLD')}"
        # Green run #1 stamps the resolved marker.
        calls = self._capture_gh()
        self._run_resolve(calls, [{"number": 60, "body": base_body}], wf, "https://gh/run/G1")
        resolved_body = self._body_after_resolve(calls, "60")
        self.assertTrue(pfi.is_resolved(resolved_body))
        # A NEW failure recurs (the `file` path) → the update plan clears the resolved marker.
        plan = pfi._plan_failure(failure, [{"number": 60, "body": resolved_body}],
                                 _args(run_url="https://gh/run/NEWFAIL"))
        self.assertEqual(plan["action"], "update")
        self.assertFalse(pfi.is_resolved(plan["new_body"]),
                         "recurrence must clear the resolved marker (re-arm)")
        # Green run #2 sees the re-armed (unresolved) body → comments once again.
        calls2 = self._capture_gh()
        self._run_resolve(calls2, [{"number": 60, "body": plan["new_body"]}], wf, "https://gh/run/G2")
        comment2 = [c for c in calls2 if len(c) > 2 and c[2] == "comment"]
        self.assertEqual(len(comment2), 1, "re-armed green run comments exactly once")
        self.assertFalse(any("close" in c for c in calls + calls2))

    def test_green_lane_resolve_unmatched_workflow_is_noop(self):
        # A green lane whose filename matches no open issue resolves nothing (and never
        # falls back to an all-lane sweep). Mirrors the workflow passing a lane filename.
        calls = self._capture_gh()
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            oj = _write(tmp, "open.json", [
                {"number": 55, "body": "**Workflow:** `compression-corruption-parity.yml`"},
            ])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_resolve(_args(
                    failures_json=None, open_issues_json=oj,
                    workflow="exhaustive-regeneration.yml", dry_run=False))
            self.assertEqual(rc, 0)
        self.assertEqual([c for c in calls if len(c) > 2 and c[2] == "comment"], [])
        self.assertIn("nothing to do", out.getvalue())

    def test_resolve_comments_before_stamping_resolved_marker(self):
        # Fix A (round 8): the resolution COMMENT must be posted BEFORE the body is edited
        # to carry the resolved marker, so a comment failure never leaves the marker stamped
        # (which would make a later green run skip the issue and drop the comment forever).
        wf = "compression-corruption-parity.yml"
        issue_body = f"<!-- PARITY-FAIL:aaaaaaaaaaaa -->\n**Workflow:** `{wf}`\n"
        calls = self._capture_gh()
        self._run_resolve(calls, [{"number": 55, "body": issue_body}], wf, "https://gh/run/G1")
        gh_calls = [c for c in calls if len(c) > 2 and c[1] == "issue"]
        ops = [c[2] for c in gh_calls]
        # Success path: comment first, then the body edit that stamps the resolved marker.
        self.assertEqual(ops, ["comment", "edit"])
        stamped = self._body_after_resolve(calls, "55")
        self.assertTrue(pfi.is_resolved(stamped), "success path stamps marker AFTER commenting")

    def test_resolve_comment_failure_leaves_marker_unstamped_so_retry_re_posts(self):
        # Fix A: if the resolution comment FAILS, the resolved marker must NOT be stamped, so
        # a later green run retries and still posts exactly one comment on eventual success.
        wf = "compression-corruption-parity.yml"
        issue_body = f"<!-- PARITY-FAIL:aaaaaaaaaaaa -->\n**Workflow:** `{wf}`\n"

        # First run: `gh issue comment` fails (transient); the failure propagates out of
        # _resolve_one (cmd_resolve stays non-gating at the workflow level). Assert NO body
        # edit was attempted → the marker is never stamped.
        calls = []

        def failing_gh(argv):
            calls.append(argv)
            if len(argv) > 2 and argv[2] == "comment":
                raise SystemExit("error: `gh issue comment` failed: boom")
            return ""

        orig_gh = pfi._gh
        pfi._gh = failing_gh
        self.addCleanup(lambda: setattr(pfi, "_gh", orig_gh))
        with tempfile.TemporaryDirectory() as d:
            oj = _write(Path(d), "open.json", [{"number": 55, "body": issue_body}])
            out = io.StringIO()
            with contextlib.redirect_stdout(out), self.assertRaises(SystemExit):
                pfi.cmd_resolve(_args(
                    failures_json=None, open_issues_json=oj,
                    workflow=wf, run_url="https://gh/run/FAIL", dry_run=False))
        self.assertTrue(any(c[2] == "comment" for c in calls if len(c) > 2),
                        "comment is attempted first")
        self.assertFalse(any(c[2] == "edit" for c in calls if len(c) > 2),
                         "body edit (marker stamp) must NOT run after a failed comment")
        self.assertFalse(pfi.is_resolved(issue_body),
                         "original body carries no resolved marker → next green run retries")

        # Retry: the SAME still-unresolved issue on a later green run → comment succeeds
        # once and the marker is stamped (so subsequent green runs are idempotent).
        calls2 = self._capture_gh()
        self._run_resolve(calls2, [{"number": 55, "body": issue_body}], wf, "https://gh/run/G2")
        comment2 = [c for c in calls2 if len(c) > 2 and c[2] == "comment"]
        self.assertEqual(len(comment2), 1, "retry posts exactly one comment on success")
        self.assertTrue(pfi.is_resolved(self._body_after_resolve(calls2, "55")),
                        "successful retry stamps the resolved marker")


if __name__ == "__main__":
    unittest.main()
