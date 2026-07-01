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
        # A --workflow filter resolves only issues whose body records that lane.
        calls = self._capture_gh()
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            oj = _write(tmp, "open.json", [
                {"number": 55, "body": "**Workflow:** `compression-corruption-parity.yml`"},
                {"number": 56, "body": "**Workflow:** `tombstone-ttl-parity.yml`"},
            ])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = pfi.cmd_resolve(_args(
                    failures_json=None, open_issues_json=oj,
                    workflow="compression-corruption-parity.yml", dry_run=False))
            self.assertEqual(rc, 0)
        comment_calls = [c for c in calls if len(c) > 2 and c[2] == "comment"]
        self.assertEqual([c[3] for c in comment_calls], ["55"])
        self.assertFalse(any("close" in c for c in calls))


if __name__ == "__main__":
    unittest.main()
