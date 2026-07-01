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
        tier="nightly_docker", repro="cargo test -p x",
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


if __name__ == "__main__":
    unittest.main()
