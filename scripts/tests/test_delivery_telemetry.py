#!/usr/bin/env python3
"""Unit tests for scripts/delivery-telemetry.py (stdlib unittest, no network/datasets).

Run standalone:   python3 scripts/tests/test_delivery_telemetry.py
Or via the gate:  scripts/agent-gate.sh --only delivery-telemetry
"""

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"

# The module file name has a hyphen, so load it by path.
_spec = importlib.util.spec_from_file_location("delivery_telemetry", SCRIPTS / "delivery-telemetry.py")
dt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dt)

SCHEMA = dt.load_schema(dt.DEFAULT_SCHEMA)


def _from_json_file(tmp: Path) -> str:
    """Write a GitHub-derived fields file (the offline seam for `record`)."""
    p = tmp / "ghfields.json"
    p.write_text(json.dumps({
        "created_at": "2026-06-10T00:00:00Z",
        "pr_opened_at": "2026-06-10T01:00:00Z",
        "merged_at": "2026-06-10T03:00:00Z",
        "closed_at": "2026-06-10T03:05:00Z",
        "priority": "P2",
        "routing": "design",
    }))
    return str(p)


class RecordTests(unittest.TestCase):
    def test_record_from_json_appends_schema_valid_line(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main([
                "record", "--ledger", str(ledger),
                "--issue", "1161", "--pr", "1170", "--slug", "delivery-telemetry-ledger",
                "--gate", "pass", "--gate-runs", "2",
                "--claim-collisions", "0", "--rebase-events", "1",
                "--roborev-findings", "0", "--rework", "0",
                "--from-json", _from_json_file(tmp),
            ])
            self.assertEqual(rc, 0)
            lines = [l for l in ledger.read_text().splitlines() if l.strip()]
            self.assertEqual(len(lines), 1)
            rec = json.loads(lines[0])
            self.assertEqual(dt.validate_record(rec, SCHEMA), [])
            # durations are arithmetic over the authoritative timestamps
            self.assertEqual(rec["cycle_time_s"], 11100)        # 3h05m
            self.assertEqual(rec["phase_s"]["to_pr_s"], 3600)   # 1h
            self.assertEqual(rec["phase_s"]["review_s"], 7200)  # 2h
            self.assertEqual(rec["issue"], 1161)
            self.assertEqual(rec["routing"], "design")

    def test_record_missing_required_counter_is_error_not_silent_zero(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit):
                dt.main([
                    "record", "--ledger", str(ledger),
                    "--issue", "1161", "--pr", "1170", "--slug", "x",
                    "--gate", "pass", "--gate-runs", "1",
                    "--claim-collisions", "0", "--rebase-events", "0",
                    "--roborev-findings", "0",   # --rework deliberately omitted
                    "--from-json", _from_json_file(tmp),
                ])
            self.assertFalse(ledger.exists() and ledger.read_text().strip(),
                             "no record should be written when a counter is missing")

    def test_record_null_timestamp_is_error_not_attribute_crash(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            # an unmerged PR -> merged_at is null; must be a clean SystemExit, not a crash
            ghfields = tmp / "ghfields.json"
            ghfields.write_text(json.dumps({
                "created_at": "2026-06-10T00:00:00Z",
                "pr_opened_at": "2026-06-10T01:00:00Z",
                "merged_at": None,
                "closed_at": "2026-06-10T03:05:00Z",
                "priority": "P2", "routing": "design",
            }))
            with self.assertRaises(SystemExit):
                dt.main([
                    "record", "--ledger", str(ledger),
                    "--issue", "1", "--pr", "2", "--slug", "x",
                    "--gate", "pass", "--gate-runs", "1",
                    "--claim-collisions", "0", "--rebase-events", "0",
                    "--roborev-findings", "0", "--rework", "0",
                    "--from-json", str(ghfields),
                ])
            self.assertFalse(ledger.exists() and ledger.read_text().strip())


class LintTests(unittest.TestCase):
    def test_clean_ledger_passes(self):
        rc = dt.main(["lint", "--ledger", str(FIXTURES / "sample-ledger.jsonl")])
        self.assertEqual(rc, 0)

    def test_malformed_timestamp_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
            rec["created_at"] = "not-a-timestamp"
            ledger.write_text(json.dumps(rec) + "\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 1)
            self.assertIn("date-time", err.getvalue())

    def test_malformed_line_fails_with_line_number(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            good = (FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0]
            # second line drops the required 'gate' field and uses a negative counter
            bad = json.loads(good)
            del bad["gate"]
            bad["rework"] = -1
            ledger.write_text(good + "\n" + json.dumps(bad) + "\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 1)
            self.assertIn("line 2", err.getvalue())


class RetroTests(unittest.TestCase):
    def test_retro_ranks_fixture_to_expected_top_and_files_nothing(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = dt.main([
                "retro",
                "--ledger", str(FIXTURES / "sample-ledger.jsonl"),
                "--open-issues-json", str(FIXTURES / "open-issues-empty.json"),
            ])
        self.assertEqual(rc, 0)
        text = out.getvalue()
        self.assertIn("top recurring failure: rework", text)
        self.assertIn("DRY RUN", text)              # default mode files nothing

    def test_retro_dedupes_against_existing_flow_meta_issue(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = dt.main([
                "retro",
                "--ledger", str(FIXTURES / "sample-ledger.jsonl"),
                "--open-issues-json", str(FIXTURES / "open-issues-rework.json"),
            ])
        self.assertEqual(rc, 0)
        text = out.getvalue()
        self.assertIn("already tracked", text)
        self.assertIn("#999", text)
        self.assertNotIn("DRY RUN", text)


class AggregateTests(unittest.TestCase):
    def test_aggregate_and_rank_are_deterministic_tallies(self):
        records = [json.loads(l) for l in
                   (FIXTURES / "sample-ledger.jsonl").read_text().splitlines() if l.strip()]
        tally = dt.aggregate(records)
        self.assertEqual(tally["rework"], 7)
        # failed gate ROUNDS from authoritative gate_runs: rec2 (fail, runs=2) -> 2;
        # rec1/rec3 (pass, runs=1) -> 0 each.
        self.assertEqual(tally["gate_failures"], 2)
        self.assertEqual(tally["claim_collisions"], 1)
        ranked = dt.rank(tally)
        self.assertEqual(ranked[0][0], "rework")
        self.assertEqual(ranked[0][3], 7 * dt.RETRO_WEIGHTS["rework"])  # 28


if __name__ == "__main__":
    unittest.main(verbosity=2)
