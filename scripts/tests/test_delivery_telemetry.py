#!/usr/bin/env python3
"""Unit tests for scripts/delivery-telemetry.py (stdlib unittest, no network/datasets).

Stdlib-only by design — the `delivery-telemetry` gate component installs nothing.
StandardValidatorCouplingTests is the ONE optional block: it needs `jsonschema` and
SKIPS without it. Nothing depends on that skip for coverage — the schema-side coupling
is also asserted structurally, with the stdlib, by SchemaCouplingDeclarationTests.

Run standalone:   python3 scripts/tests/test_delivery_telemetry.py
Or via the gate:  scripts/agent-gate.sh --only delivery-telemetry
"""

import contextlib
import datetime as _datetime
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"

# The module file name has a hyphen, so load it by path.
_spec = importlib.util.spec_from_file_location("delivery_telemetry", SCRIPTS / "delivery-telemetry.py")
dt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dt)

SCHEMA = dt.load_schema(dt.DEFAULT_SCHEMA)


def _from_json_file(tmp: Path, issue: int = 42, pr: int = 7,
                    name: str = "ghfields.json") -> str:
    """Write a GitHub-derived fields file (the offline seam for `record`).

    Carries `issue` because build_record binds the payload to --issue (issue #3550): a stale
    or copied file built for another delivery must not be applied wholesale.
    """
    p = tmp / name
    p.write_text(json.dumps({
        "issue": issue,
        "pr": pr,
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
                "--from-json", _from_json_file(tmp, 1161, 1170),
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
                    "--from-json", _from_json_file(tmp, 1161, 1170),
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
                "issue": 3393,
                "pr": 3467,
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


    def test_record_refuses_duplicate_cycle(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            base = ["record", "--ledger", str(ledger),
                    "--issue", "42", "--pr", "7", "--slug", "x",
                    "--gate", "pass", "--gate-runs", "1",
                    "--claim-collisions", "0", "--rebase-events", "0",
                    "--roborev-findings", "0", "--rework", "0",
                    "--from-json", _from_json_file(tmp, 42, 7)]
            self.assertEqual(dt.main(base), 0)
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                self.assertEqual(dt.main(base), 1)         # same (issue, pr) re-stamp refused
            self.assertIn("issue #42 / pr #7 already has a ledger record", err.getvalue())
            self.assertEqual(dt.main(base + ["--allow-duplicate"]), 0)  # explicit override
            lines = [l for l in ledger.read_text().splitlines() if l.strip()]
            self.assertEqual(len(lines), 2)

    def test_record_accepts_reopen_cycle_without_allow_duplicate(self):
        # issue #2314: same issue, a NEW pr (a reopened issue shipping again) must append
        # WITHOUT --allow-duplicate — it is a distinct delivery cycle, not a re-stamp.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            first = ["record", "--ledger", str(ledger),
                     "--issue", "2264", "--pr", "2282", "--slug", "x",
                     "--gate", "pass", "--gate-runs", "1",
                     "--claim-collisions", "0", "--rebase-events", "0",
                     "--roborev-findings", "0", "--rework", "0",
                     "--from-json", _from_json_file(tmp, 2264, 2282)]
            self.assertEqual(dt.main(first), 0)
            second = list(first)
            second[second.index("2282")] = "2301"     # same issue, second shipped PR
            # its own payload: the (issue, pr) binding (#3550) refuses a payload reused
            # across PRs, which is the point — each cycle carries its own PR timestamps
            second[second.index(_from_json_file(tmp, 2264, 2282))] = \
                _from_json_file(tmp, 2264, 2301, name="ghfields-2301.json")
            self.assertEqual(dt.main(second), 0)       # no --allow-duplicate needed
            lines = [l for l in ledger.read_text().splitlines() if l.strip()]
            self.assertEqual(len(lines), 2)
            # and the resulting two-cycle ledger lints clean
            self.assertEqual(dt.main(["lint", "--ledger", str(ledger)]), 0)

    def test_routing_required_when_not_determinable(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ghfields = tmp / "ghfields.json"
            ghfields.write_text(json.dumps({
                "issue": 3393,
                "pr": 3467,
                "created_at": "2026-06-10T00:00:00Z",
                "pr_opened_at": "2026-06-10T01:00:00Z",
                "merged_at": "2026-06-10T03:00:00Z",
                "closed_at": "2026-06-10T03:05:00Z",
                "priority": "P2", "routing": None,    # neither label present
            }))
            with self.assertRaises(SystemExit):
                dt.main([
                    "record", "--ledger", str(tmp / "l.jsonl"),
                    "--issue", "1", "--pr", "2", "--slug", "x",
                    "--gate", "pass", "--gate-runs", "1",
                    "--claim-collisions", "0", "--rebase-events", "0",
                    "--roborev-findings", "0", "--rework", "0",
                    "--from-json", str(ghfields),
                ])


    def test_record_rejects_negative_duration(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            ghfields = tmp / "ghfields.json"
            ghfields.write_text(json.dumps({       # merged BEFORE pr opened -> review_s < 0
                "issue": 1,
                "pr": 2,
                "created_at": "2026-06-10T00:00:00Z",
                "pr_opened_at": "2026-06-10T02:00:00Z",
                "merged_at": "2026-06-10T01:00:00Z",
                "closed_at": "2026-06-10T03:00:00Z",
                "priority": "P2", "routing": "design",
            }))
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main([
                    "record", "--ledger", str(ledger),
                    "--issue", "1", "--pr", "2", "--slug", "x",
                    "--gate", "pass", "--gate-runs", "1",
                    "--claim-collisions", "0", "--rebase-events", "0",
                    "--roborev-findings", "0", "--rework", "0",
                    "--from-json", str(ghfields),
                ])
            self.assertEqual(rc, 1)
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_rejects_bad_priority(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main([
                "record", "--ledger", str(ledger),
                "--issue", "1", "--pr", "2", "--slug", "x", "--priority", "high",
                "--gate", "pass", "--gate-runs", "1",
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "0", "--rework", "0",
                "--from-json", _from_json_file(tmp, 1, 2),
            ])
            self.assertEqual(rc, 1)
            self.assertFalse(ledger.exists() and ledger.read_text().strip())


class RoborevSeverityTests(unittest.TestCase):
    """issue #2088: optional roborev_blockers/roborev_nits severity split."""

    def test_record_with_valid_severity_pair_is_schema_valid(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main([
                "record", "--ledger", str(ledger),
                "--issue", "2088", "--pr", "2200", "--slug", "roborev-severity",
                "--gate", "pass", "--gate-runs", "1",
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "5", "--roborev-blockers", "2", "--roborev-nits", "3",
                "--rework", "0",
                "--from-json", _from_json_file(tmp, 2088, 2200),
            ])
            self.assertEqual(rc, 0)
            rec = json.loads(ledger.read_text().splitlines()[0])
            self.assertEqual(dt.validate_record(rec, SCHEMA), [])
            self.assertEqual(rec["roborev_blockers"], 2)
            self.assertEqual(rec["roborev_nits"], 3)

    def test_record_rejects_severity_sum_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit):
                dt.main([
                    "record", "--ledger", str(ledger),
                    "--issue", "1", "--pr", "2", "--slug", "x",
                    "--gate", "pass", "--gate-runs", "1",
                    "--claim-collisions", "0", "--rebase-events", "0",
                    "--roborev-findings", "5", "--roborev-blockers", "2", "--roborev-nits", "2",
                    "--rework", "0",
                    "--from-json", _from_json_file(tmp, 1, 2),
                ])
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_rejects_one_of_severity_pair(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit):
                dt.main([
                    "record", "--ledger", str(ledger),
                    "--issue", "1", "--pr", "2", "--slug", "x",
                    "--gate", "pass", "--gate-runs", "1",
                    "--claim-collisions", "0", "--rebase-events", "0",
                    "--roborev-findings", "5", "--roborev-blockers", "2",  # nits omitted
                    "--rework", "0",
                    "--from-json", _from_json_file(tmp, 1, 2),
                ])
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_legacy_record_without_severity_still_valid(self):
        # a record built with no --roborev-blockers/--roborev-nits (the existing 174-record
        # ledger's shape) must remain schema-valid — severity is additive, never required.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main([
                "record", "--ledger", str(ledger),
                "--issue", "1", "--pr", "2", "--slug", "x",
                "--gate", "pass", "--gate-runs", "1",
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "0", "--rework", "0",
                "--from-json", _from_json_file(tmp, 1, 2),
            ])
            self.assertEqual(rc, 0)
            rec = json.loads(ledger.read_text().splitlines()[0])
            self.assertNotIn("roborev_blockers", rec)
            self.assertNotIn("roborev_nits", rec)
            self.assertEqual(dt.validate_record(rec, SCHEMA), [])

    def test_validate_record_rejects_lone_blocker_field_directly(self):
        # exercise the schema-level cross-field check independent of the CLI (e.g. a
        # hand-edited or externally-produced ledger line).
        rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
        rec["roborev_blockers"] = 1
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("roborev_nits" in e for e in errors))

    def test_validate_record_rejects_sum_mismatch_directly(self):
        rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
        rec["roborev_findings"] = 5
        rec["roborev_blockers"] = 1
        rec["roborev_nits"] = 1   # 1+1 != 5
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("must equal roborev_findings" in e for e in errors))

    def test_real_ledger_174_records_valid_under_updated_schema(self):
        _, errors = dt.load_ledger(dt.DEFAULT_LEDGER, SCHEMA)
        self.assertEqual(errors, [])

    def test_retro_on_mixed_severity_ledger_prefers_blockers_and_reports_nits(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            lines = (FIXTURES / "sample-ledger.jsonl").read_text().splitlines()
            legacy = json.loads(lines[0])          # roborev_findings=0, no severity
            severe = json.loads(lines[1])          # roborev_findings=1, fail/gate_runs=2
            severe["roborev_blockers"] = 4
            severe["roborev_nits"] = 6
            severe["roborev_findings"] = 10
            ledger.write_text(json.dumps(legacy) + "\n" + json.dumps(severe) + "\n")

            records, errors = dt.load_ledger(ledger, SCHEMA)
            self.assertEqual(errors, [])
            tally = dt.aggregate(records)
            # legacy contributes its raw roborev_findings (0); severe contributes blockers
            # (4), NOT the raw findings count (10) — blockers-preferred weighting.
            self.assertEqual(tally["roborev_findings"], 0 + 4)
            self.assertEqual(tally["roborev_nits_total"], 6)
            self.assertEqual(tally["roborev_severity_records"], 1)

            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = dt.main(["retro", "--ledger", str(ledger),
                              "--open-issues-json", str(FIXTURES / "open-issues-empty.json")])
            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("roborev severity: 1 record(s) classified", text)
            self.assertIn("6 nit(s) excluded", text)


class StallObservabilityTests(unittest.TestCase):
    """issue #2667: optional nudges / orphan_minutes stall-observability counters."""

    def test_record_with_stall_counters_is_schema_valid(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main([
                "record", "--ledger", str(ledger),
                "--issue", "2667", "--pr", "2680", "--slug", "poll-to-push",
                "--gate", "pass", "--gate-runs", "1",
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "0", "--rework", "0",
                "--nudges", "2", "--orphan-minutes", "45",
                "--from-json", _from_json_file(tmp, 2667, 2680),
            ])
            self.assertEqual(rc, 0)
            rec = json.loads(ledger.read_text().splitlines()[0])
            self.assertEqual(dt.validate_record(rec, SCHEMA), [])
            self.assertEqual(rec["nudges"], 2)
            self.assertEqual(rec["orphan_minutes"], 45)

    def test_record_omits_stall_counters_when_unobserved(self):
        # Authoritative-only: an unsupplied counter must be ABSENT, never zero-filled.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main([
                "record", "--ledger", str(ledger),
                "--issue", "1", "--pr", "2", "--slug", "x",
                "--gate", "pass", "--gate-runs", "1",
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "0", "--rework", "0",
                "--from-json", _from_json_file(tmp, 1, 2),
            ])
            self.assertEqual(rc, 0)
            rec = json.loads(ledger.read_text().splitlines()[0])
            self.assertNotIn("nudges", rec)
            self.assertNotIn("orphan_minutes", rec)
            self.assertEqual(dt.validate_record(rec, SCHEMA), [])

    def test_stall_counters_are_independently_optional(self):
        # Either counter may appear without the other — they are not a both-or-neither pair.
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main([
                "record", "--ledger", str(ledger),
                "--issue", "3", "--pr", "4", "--slug", "y",
                "--gate", "pass", "--gate-runs", "1",
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "0", "--rework", "0",
                "--nudges", "1",
                "--from-json", _from_json_file(tmp, 3, 4),
            ])
            self.assertEqual(rc, 0)
            rec = json.loads(ledger.read_text().splitlines()[0])
            self.assertEqual(rec["nudges"], 1)
            self.assertNotIn("orphan_minutes", rec)
            self.assertEqual(dt.validate_record(rec, SCHEMA), [])

    def test_schema_rejects_negative_stall_counter(self):
        rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
        rec["orphan_minutes"] = -5
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(errors)


class GateNotRunTests(unittest.TestCase):
    """issue #3448: `gate: not-run` / `gate_runs: 0` — "no full gate of record ran".

    Adds a legal VALUE, never a default: --gate and --gate-runs stay REQUIRED arguments,
    and the two fields are coupled (not-run <=> 0) so the record cannot tell two stories.
    """

    def _rec_argv(self, ledger, tmp, gate, gate_runs, issue=3299, pr=3408):
        return ["record", "--ledger", str(ledger),
                "--issue", str(issue), "--pr", str(pr), "--slug", "x",
                "--gate", gate, "--gate-runs", str(gate_runs),
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "0", "--rework", "0",
                "--from-json", _from_json_file(tmp, 3299, 3408)]

    # ---- AC1/AC2: the new value is accepted end-to-end -----------------------------
    def test_record_accepts_not_run_with_zero_runs(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main(self._rec_argv(ledger, tmp, "not-run", 0))
            self.assertEqual(rc, 0)
            rec = json.loads(ledger.read_text().splitlines()[0])
            self.assertEqual(dt.validate_record(rec, SCHEMA), [])
            self.assertEqual(rec["gate"], "not-run")
            self.assertEqual(rec["gate_runs"], 0)

    # ---- AC1: BOTH rejection directions of the coupling -----------------------------
    def test_record_rejects_not_run_with_nonzero_runs(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit):
                dt.main(self._rec_argv(ledger, tmp, "not-run", 2))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_rejects_pass_with_zero_runs(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit):
                dt.main(self._rec_argv(ledger, tmp, "pass", 0))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_rejects_fail_with_zero_runs(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit):
                dt.main(self._rec_argv(ledger, tmp, "fail", 0))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_validate_record_rejects_not_run_with_nonzero_runs_directly(self):
        # the cross-field check runs inside validate_record, so `lint`/`retro` enforce the
        # coupling over the WHOLE ledger, not just freshly-recorded lines.
        rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
        rec["gate"] = "not-run"     # gate_runs stays >= 1
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("not-run" in e and "gate_runs" in e for e in errors), errors)

    def test_validate_record_rejects_pass_with_zero_runs_directly(self):
        rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
        rec["gate_runs"] = 0        # gate stays "pass"
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("gate_runs" in e and "not-run" in e for e in errors), errors)

    def test_lint_enforces_the_coupling_over_the_ledger(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
            rec["gate_runs"] = 0    # gate "pass" with 0 runs -> incoherent
            ledger.write_text(json.dumps(rec) + "\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 1)
            self.assertIn("not-run", err.getvalue())

    # ---- AC2: NO new defaulting path — omission is still an error --------------------
    def test_record_still_refuses_omitted_gate(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit):
                dt.main(["record", "--ledger", str(ledger),
                         "--issue", "1", "--pr", "2", "--slug", "x",
                         "--gate-runs", "1",              # --gate deliberately omitted
                         "--claim-collisions", "0", "--rebase-events", "0",
                         "--roborev-findings", "0", "--rework", "0",
                         "--from-json", _from_json_file(tmp, 1, 2)])
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_still_refuses_each_omitted_required_counter(self):
        # Pinning test (issue #3448): the required-counter guard is IDENTITY-based
        # (`is None`). A refactor to a falsy check (`if not value`) would silently accept an
        # omitted counter as 0 for every one of these — exactly the defaulting path this
        # change must not open. Assert omission per counter, not just for one of them.
        for omit in dt.REQUIRED_COUNTERS:
            with self.subTest(omitted=omit):
                with tempfile.TemporaryDirectory() as d:
                    tmp = Path(d)
                    ledger = tmp / "ledger.jsonl"
                    argv = ["record", "--ledger", str(ledger),
                            "--issue", "1", "--pr", "2", "--slug", "x", "--gate", "pass",
                            "--from-json", _from_json_file(tmp, 1, 2)]
                    for counter in dt.REQUIRED_COUNTERS:
                        if counter != omit:
                            argv += [f"--{counter.replace('_', '-')}", "1"]
                    with self.assertRaises(SystemExit):
                        dt.main(argv)
                    self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_zero_is_an_observed_value_not_a_missing_one(self):
        # The counterpart pin: an explicitly supplied 0 must SURVIVE the required-counter
        # guard (a falsy check would reject `--gate-runs 0` / `--rework 0` as "missing").
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main(["record", "--ledger", str(ledger),
                          "--issue", "1", "--pr", "2", "--slug", "x",
                          "--gate", "not-run", "--gate-runs", "0",
                          "--claim-collisions", "0", "--rebase-events", "0",
                          "--roborev-findings", "0", "--rework", "0",
                          "--from-json", _from_json_file(tmp, 1, 2)])
            self.assertEqual(rc, 0)
            rec = json.loads(ledger.read_text().splitlines()[0])
            for counter in dt.REQUIRED_COUNTERS:
                self.assertEqual(rec[counter], 0)

    # ---- AC4: retro must not fold a not-run into the gated-pass story ---------------
    def test_aggregate_counts_no_gate_failures_for_not_run(self):
        not_run = {"gate": "not-run", "gate_runs": 0, "claim_collisions": 0,
                   "rebase_events": 0, "roborev_findings": 0, "rework": 0}
        tally = dt.aggregate([not_run])
        # zero rounds were observed, so zero rounds failed — NOT a fabricated failure...
        self.assertEqual(tally["gate_failures"], 0)
        # ...and NOT silently indistinguishable from a clean one-run pass either.
        self.assertEqual(tally["gate_not_run_records"], 1)
        self.assertEqual(dt.aggregate([{**not_run, "gate": "pass", "gate_runs": 1}])
                         ["gate_not_run_records"], 0)

    def test_gate_not_run_records_carries_no_retro_weight(self):
        # informational extra, like roborev_nits_total: rank() iterates RETRO_WEIGHTS.
        self.assertNotIn("gate_not_run_records", dt.RETRO_WEIGHTS)
        tally = dt.aggregate([{"gate": "not-run", "gate_runs": 0, "claim_collisions": 0,
                               "rebase_events": 0, "roborev_findings": 0, "rework": 0}])
        self.assertNotIn("gate_not_run_records", [row[0] for row in dt.rank(tally)])

    def test_retro_reports_the_not_run_class_distinctly(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            lines = (FIXTURES / "sample-ledger.jsonl").read_text().splitlines()
            gated = json.loads(lines[0])
            ungated = json.loads(lines[2])
            ungated["gate"], ungated["gate_runs"] = "not-run", 0
            ledger.write_text(json.dumps(gated) + "\n" + json.dumps(ungated) + "\n")
            records, errors = dt.load_ledger(ledger, SCHEMA)
            self.assertEqual(errors, [])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = dt.main(["retro", "--ledger", str(ledger),
                              "--open-issues-json", str(FIXTURES / "open-issues-empty.json")])
            self.assertEqual(rc, 0)
            self.assertIn("no full gate of record ran", out.getvalue())

    def test_retro_says_nothing_about_not_run_on_an_all_gated_ledger(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = dt.main(["retro", "--ledger", str(FIXTURES / "sample-ledger.jsonl"),
                          "--open-issues-json", str(FIXTURES / "open-issues-empty.json")])
        self.assertEqual(rc, 0)
        self.assertNotIn("no full gate of record ran", out.getvalue())

    # ---- AC5: backward compatibility -------------------------------------------------
    def test_existing_ledger_validates_unchanged_under_the_new_schema(self):
        records, errors = dt.load_ledger(dt.DEFAULT_LEDGER, SCHEMA)
        self.assertEqual(errors, [])
        self.assertTrue(records)
        # every pre-#3448 record keeps its pass/fail + >=1 runs shape (no migration).
        legacy = [r for r in records if r["gate"] != "not-run"]
        self.assertTrue(all(r["gate_runs"] >= 1 for r in legacy))


# Sentinel for "omit this key entirely" — distinct from None, which is a MEASURED value.
# issues #3550/#3559.
_OMIT = object()

_U = "https://github.com/pmcfadin/cqlite/issues/3393"


class SliceDeliveryTests(unittest.TestCase):
    """issue #3550: a SLICE delivery — a merged PR shipping part of an issue that
    DELIBERATELY stays OPEN (lead ruling on #3393).

    `closed_at: null` IS the slice marker (total + derivable, so it is a definition, not an
    inference), `--slice` is the explicit opt-in coupled both ways, and `cycle_time_s` is
    then bounded by the PR's mergedAt — the authoritative terminal timestamp of a slice.
    The refusal for the non-opted-in path is load-bearing: the previous message read as an
    instruction to CLOSE the issue, which is exactly the forbidden workaround.
    """

    def setUp(self):
        self.schema = json.loads(dt.DEFAULT_SCHEMA.read_text())

    def _ghfields(self, tmp, closed_at, merged_at="2026-06-10T03:00:00Z", name="ghfields.json",
                  issue_open_at_merge=True, pr_closes_this_issue=False, issue=3393, pr=3467):
        p = tmp / name
        fields = {
            "created_at": "2026-06-10T00:00:00Z",
            "pr_opened_at": "2026-06-10T01:00:00Z",
            "merged_at": merged_at,
            "closed_at": closed_at,
            "priority": "P1",
            "routing": "oracle",
        }
        # `issue_open_at_merge` is the timeline-replayed answer to the question --slice
        # asserts (issue #3559); it REPLACED #3550's `state_reason` proxy. The replay itself
        # and its refusals live in scripts/tests/test_delivery_telemetry_timeline.py.
        if issue_open_at_merge is not _OMIT:
            fields["issue_open_at_merge"] = issue_open_at_merge
        if pr_closes_this_issue is not _OMIT:
            fields["pr_closes_this_issue"] = pr_closes_this_issue
        if issue is not _OMIT:
            fields["issue"] = issue
        if pr is not _OMIT:
            fields["pr"] = pr
        p.write_text(json.dumps(fields))
        return str(p)

    def _rec_argv(self, ledger, ghfields, *extra, issue=3393, pr=3467):
        return ["record", "--ledger", str(ledger),
                "--issue", str(issue), "--pr", str(pr), "--slug", "oom-loud-dead-lanes",
                "--gate", "pass", "--gate-runs", "1",
                "--claim-collisions", "0", "--rebase-events", "0",
                "--roborev-findings", "0", "--rework", "0",
                "--from-json", ghfields, *extra]

    def _committed(self):
        return json.loads(dt.DEFAULT_LEDGER.read_text().splitlines()[0])

    def _distinct_basis_record(self):
        """A schema-valid COMPLETED record whose two candidate bases DIFFER.

        Non-degeneracy matters: a PR merge usually closes its issue, so most committed
        records have closed_at == merged_at and both bases agree — a basis test built on one
        of those would pass under either rule (it would assert nothing).
        """
        rec = self._committed()
        rec.update({
            "issue": 3393,
            "pr": 3467,
            "created_at": "2026-06-10T00:00:00Z",
            "pr_opened_at": "2026-06-10T01:00:00Z",
            "merged_at": "2026-06-10T03:00:00Z",
            "closed_at": "2026-06-10T03:05:00Z",
            "cycle_time_s": 11100,                              # created -> closed
            "phase_s": {"to_pr_s": 3600, "review_s": 7200},
        })
        self.assertNotEqual(rec["closed_at"], rec["merged_at"], "degenerate basis fixture")
        self.assertEqual(dt.validate_record(rec, SCHEMA), [])
        return rec

    # ---- the minimal validator must understand a UNION type ------------------------
    def test_type_ok_accepts_a_union_of_type_names(self):
        self.assertTrue(dt._type_ok(None, ["string", "null"]))
        self.assertTrue(dt._type_ok("2026-06-10T00:00:00Z", ["string", "null"]))
        self.assertFalse(dt._type_ok(17, ["string", "null"]))
        self.assertFalse(dt._type_ok(True, ["string", "null"]))
        # a single type name keeps working exactly as before
        self.assertTrue(dt._type_ok("x", "string"))
        self.assertFalse(dt._type_ok("x", "integer"))

    # ---- schema declares the nullable marker + its semantics ------------------------
    def test_schema_declares_closed_at_nullable_and_still_required(self):
        schema = json.loads(dt.DEFAULT_SCHEMA.read_text())
        closed = schema["properties"]["closed_at"]
        self.assertIsInstance(closed["type"], list, "closed_at is no longer a nullable union")
        self.assertEqual(set(closed["type"]), {"string", "null"})
        # the KEY stays required: null is the marker, absence is a malformed record
        self.assertIn("closed_at", schema["required"])
        self.assertIn("3550", closed["description"])
        self.assertIn("slice", closed["description"].lower())

    def test_validator_accepts_a_null_closed_at_slice_record(self):
        rec = self._distinct_basis_record()
        rec["closed_at"] = None
        rec["cycle_time_s"] = 10800                             # created -> merged
        self.assertEqual(dt.validate_record(rec, SCHEMA), [])

    def test_validator_still_rejects_an_absent_closed_at_key(self):
        rec = self._committed()
        del rec["closed_at"]
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("closed_at" in e for e in errors), errors)

    def test_validator_still_rejects_a_wrong_typed_closed_at(self):
        rec = self._committed()
        rec["closed_at"] = 1749513600
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("closed_at" in e for e in errors), errors)

    def test_validator_still_rejects_a_malformed_closed_at_string(self):
        rec = self._committed()
        rec["closed_at"] = "2026-06-10"      # date-only: no 'T', no offset
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("closed_at" in e for e in errors), errors)

    # ---- cycle_time_s basis, cross-checked in BOTH directions -----------------------
    def test_slice_cycle_time_must_use_the_merged_at_basis(self):
        rec = self._distinct_basis_record()
        rec["closed_at"] = None              # cycle_time_s still on the closed_at basis
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("cycle_time_s" in e and "merged_at" in e for e in errors), errors)

    def test_completed_cycle_time_must_use_the_closed_at_basis(self):
        rec = self._distinct_basis_record()
        rec["cycle_time_s"] = 10800          # the SLICE basis on a closed issue
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(any("cycle_time_s" in e and "closed_at" in e for e in errors), errors)

    def test_cycle_time_basis_check_tolerates_a_malformed_timestamp(self):
        # a bad timestamp is already flagged by the generic walk; this check must not
        # ALSO raise a confusing traceback out of validate_record.
        rec = self._committed()
        rec["created_at"] = "not-a-timestamp"
        errors = dt.validate_record(rec, SCHEMA)
        self.assertTrue(errors)

    def test_every_committed_record_satisfies_the_cycle_time_basis(self):
        checked = 0
        for line in dt.DEFAULT_LEDGER.read_text().splitlines():
            if not line.strip():
                continue
            checked += 1
            errors: list = []
            dt._validate_cycle_time_basis(json.loads(line), errors)
            self.assertEqual(errors, [], f"committed record {checked}: {errors}")
        self.assertGreater(checked, 0, "committed ledger is empty - this test would be vacuous")

    # ---- CLI: --slice, coupled BOTH directions --------------------------------------
    def test_record_slice_writes_null_closed_at_on_the_merged_at_basis(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            rc = dt.main(self._rec_argv(ledger, self._ghfields(tmp, None), "--slice"))
            self.assertEqual(rc, 0)
            rec = json.loads(ledger.read_text().splitlines()[0])
            self.assertEqual(dt.validate_record(rec, SCHEMA), [])
            self.assertIsNone(rec["closed_at"])
            self.assertEqual(rec["cycle_time_s"], 10800)          # created -> merged (3h)
            self.assertEqual(rec["phase_s"]["to_pr_s"], 3600)     # unchanged basis
            self.assertEqual(rec["phase_s"]["review_s"], 7200)    # unchanged basis

    def test_record_slice_decided_by_the_timeline_not_by_the_current_closed_at(self):
        """#3550 refused --slice for a closed-NOW issue in every timestamp ordering, because
        CURRENT state cannot decide a question about DELIVERY time. #3559 replays the issue
        TIMELINE instead, so the SAME closed_at values now decide differently depending on
        where the closing EVENT sits relative to mergedAt — which is the point.

        A `closed_at <= merged_at` guard was tried and is wrong: an auto-closing PR merges
        BEFORE GitHub records the closure, so closed_at slightly AFTER merged_at is the
        NORMAL ordering of an ordinary COMPLETED delivery, and such a guard would have
        permitted --slice on essentially every ordinary delivery while looking like a check.
        Hence the three closed_at values below, each paired with BOTH timeline answers: the
        record's kind must track the timeline and be INDEPENDENT of that ordering.
        Message-level coverage of each refusal is in
        scripts/tests/test_delivery_telemetry_timeline.py.
        """
        for label, closed in (("closed before the merge", "2026-06-10T02:00:00Z"),
                              ("auto-closed just after the merge", "2026-06-10T03:00:05Z"),
                              ("closed long after the merge", "2026-07-01T00:00:00Z")):
            for open_at_merge in (True, False):
                with self.subTest(label, open_at_merge=open_at_merge), \
                        tempfile.TemporaryDirectory() as d:
                    tmp = Path(d)
                    ledger = tmp / "ledger.jsonl"
                    gh = self._ghfields(tmp, closed, issue_open_at_merge=open_at_merge)
                    argv = self._rec_argv(ledger, gh, "--slice")
                    if open_at_merge:
                        self.assertEqual(0, dt.main(argv))
                        rec = json.loads(ledger.read_text().strip())
                        self.assertIsNone(rec["closed_at"])
                        self.assertEqual(rec["cycle_time_s"], 10800)   # created -> merged
                    else:
                        with self.assertRaises(SystemExit) as cm:
                            dt.main(argv)
                        msg = str(cm.exception)
                        self.assertIn("CLOSED", msg)
                        self.assertIn("hand-append", msg.lower())
                        self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_a_slice_pr_closes_nothing_so_the_window_guard_does_not_fire(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            self.assertEqual(0, dt.main(self._rec_argv(
                ledger, self._ghfields(tmp, None, pr_closes_this_issue=False), "--slice")))
            self.assertIsNone(json.loads(ledger.read_text().strip())["closed_at"])

    def test_record_refuses_a_payload_built_for_another_issue(self):
        """A stale or copied --from-json file is the realistic seam error, and it corrupts
        MORE than one field: a wrong closed_at/created_at misstates the kind and the cycle
        time, a wrong pr_closes_this_issue disables the window guard. One scalar bound to one
        flag protects the whole payload — and does not reintroduce two injected operands that
        must agree with each other."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._rec_argv(
                    ledger, self._ghfields(tmp, None, issue=9999), "--slice"))
            msg = str(cm.exception)
            self.assertIn("built for issue #9999", msg)
            self.assertIn("--issue is 3393", msg)
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_refuses_a_payload_built_for_another_pr(self):
        """The payload carries PR-specific fields (pr_opened_at, merged_at,
        pr_closes_this_issue), so binding the ISSUE alone is insufficient: reusing one
        payload across two PRs of the SAME issue — the #3393 shape exactly, three PRs on one
        deliberately-open issue — would record the new pr with the old pr's timestamps and
        classification."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._rec_argv(
                    ledger, self._ghfields(tmp, None, pr=3407), "--slice", pr=3467))
            msg = str(cm.exception)
            self.assertIn("built for pr #3407", msg)
            self.assertIn("--pr is 3467", msg)
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_refuses_an_unbound_or_mistyped_payload_pr(self):
        for bogus in (_OMIT, None, "3467", True, 3467.0, [], {}):
            with self.subTest(pr=bogus), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                with self.assertRaises(SystemExit) as cm:
                    dt.main(self._rec_argv(
                        ledger, self._ghfields(tmp, None, pr=bogus), "--slice"))
                self.assertIn("'pr'", str(cm.exception))
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_refuses_an_unbound_or_mistyped_payload_issue(self):
        for bogus in (_OMIT, None, "3393", True, 3393.0, [], {}):
            with self.subTest(issue=bogus), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                with self.assertRaises(SystemExit) as cm:
                    dt.main(self._rec_argv(
                        ledger, self._ghfields(tmp, None, issue=bogus), "--slice"))
                # tight: bare "issue" also matches the "(issue #3550)" citation in nearly
                # every message, so it could not catch a WRONG message
                self.assertIn("'issue'", str(cm.exception))
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_refuses_an_unmeasured_pr_closes_this_issue(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._rec_argv(
                    ledger, self._ghfields(tmp, None, pr_closes_this_issue=_OMIT),
                    "--slice"))
            self.assertIn("pr_closes_this_issue", str(cm.exception))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_refuses_a_non_boolean_pr_closes_this_issue(self):
        """A bool cannot half-agree with itself, but a truthy/falsy STAND-IN can still be
        unmeasured input taking the affirmative branch — the shape this whole seam kept
        re-finding. Only a real bool is an answer."""
        for bogus in (None, 0, 1, "", "false", "true", [], {}):
            with self.subTest(value=bogus), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                with self.assertRaises(SystemExit) as cm:
                    dt.main(self._rec_argv(
                        ledger, self._ghfields(tmp, None, pr_closes_this_issue=bogus),
                        "--slice"))
                self.assertIn("must be a boolean", str(cm.exception))
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_issue_identity_is_the_full_triple_and_rejects_whitespace(self):
        """Unit-level, because this is where the recurring defect lived. Identity is
        (owner, repo, number) — a number alone collides across repositories — and a value
        with surrounding whitespace is UNRECOGNISED, never normalised: a lenient reader plus
        a strict consumer is how a non-match becomes indistinguishable from a correct
        non-match, which fails OPEN."""
        self.assertEqual(dt._issue_identity("https://github.com/pmcfadin/cqlite/issues/3393"),
                         ("pmcfadin", "cqlite", 3393))
        # same number, different repository -> different identity
        self.assertNotEqual(
            dt._issue_identity("https://github.com/other/repo/issues/3393"),
            dt._issue_identity("https://github.com/pmcfadin/cqlite/issues/3393"))
        for bad in (" https://github.com/pmcfadin/cqlite/issues/3393",
                    "https://github.com/pmcfadin/cqlite/issues/3393 ",
                    "https://github.com/pmcfadin/cqlite/issues/3393\n",
                    "https://github.com/pmcfadin/cqlite/pull/3393",
                    "http://github.com/pmcfadin/cqlite/issues/3393",
                    "https://example.com/pmcfadin/cqlite/issues/3393",
                    "https://github.com/cqlite/issues/3393",
                    "https://github.com/pmcfadin/cqlite/issues/",
                    "https://github.com/pmcfadin/cqlite/issues/3393#c1",
                    "3393", "", None, 3393, [], {}):
            with self.subTest(value=bad):
                self.assertIsNone(dt._issue_identity(bad))

    def test_record_refused_in_the_merge_to_autoclose_window(self):
        """The propagation window: GitHub records an auto-close AFTER the merge, so for a few
        seconds an ordinary COMPLETED delivery presents EXACTLY as a never-closed issue:
        closed_at is null AND the timeline replay truthfully answers "open at mergedAt",
        because the close is recorded AFTER the merge. So the timeline cannot decide this one
        either — the PR's own closing declaration is the only signal that can, since a slice
        pr closes NOTHING. That is why the #3550 guard is load-bearing FOREVER and was not
        removed with the state_reason proxy (issue #3559). Refused with or without --slice:
        recording either way in that window files a wrong record."""
        for extra in ((), ("--slice",)):
            with self.subTest(slice_flag=bool(extra)), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                gh = self._ghfields(tmp, None, pr_closes_this_issue=True)
                with self.assertRaises(SystemExit) as cm:
                    dt.main(self._rec_argv(ledger, gh, *extra))
                msg = str(cm.exception)
                self.assertIn("CLOSES issue #3393", msg)
                self.assertIn("auto-close", msg)
                self.assertIn("WITHOUT --slice", msg)
                self.assertIn("3550", msg)
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_github_fields_refuses_a_malformed_closing_issues_reference(self):
        """The live path must REFUSE a malformed reply, not silently filter it: discarding an
        unreadable element shrinks the list toward "closes nothing", the permissive answer."""
        for refs in (None, {}, "x", [{"title": "no url"}], [None], [{"url": 3550}],
                     [{"url": True}], [{"url": ""}], [3550], [{"url": _U}, "junk"]):
            with self.subTest(refs=refs):
                def fake_run(argv, **kw):
                    if argv[:2] == ["gh", "api"]:
                        return _FakeProc("[]")   # empty timeline (#3559)
                    if argv[:3] == ["gh", "issue", "view"]:
                        return _FakeProc(json.dumps({
                            "createdAt": "2026-06-01T00:00:00Z", "closedAt": None,
                            "labels": [{"name": "P1"}, {"name": "oracle"}],
                            "url": "https://github.com/pmcfadin/cqlite/issues/1",
                        }))
                    return _FakeProc(json.dumps({
                        "createdAt": "2026-06-01T00:30:00Z",
                        "mergedAt": "2026-06-01T01:30:00Z",
                        "closingIssuesReferences": refs}))

                with mock.patch.object(dt.subprocess, "run", fake_run):
                    with self.assertRaises(SystemExit) as cm:
                        dt._github_fields(1, 2)
                self.assertIn("closingIssuesReferences", str(cm.exception))

    def test_github_fields_refuses_an_absent_closing_issues_references(self):
        """`.get` would map an absent field to None -> "closes nothing" -> the window guard
        silently disabled. Absence must be a named refusal, like the url check's."""
        def fake_run(argv, **kw):
            if argv[:2] == ["gh", "api"]:
                return _FakeProc("[]")   # empty timeline: never closed (#3559)
            if argv[:3] == ["gh", "issue", "view"]:
                return _FakeProc(json.dumps({
                    "createdAt": "2026-06-01T00:00:00Z", "closedAt": None,
                    "labels": [{"name": "P1"}, {"name": "oracle"}],
                }))
            return _FakeProc(json.dumps({
                "createdAt": "2026-06-01T00:30:00Z", "mergedAt": "2026-06-01T01:30:00Z"}))

        with mock.patch.object(dt.subprocess, "run", fake_run):
            with self.assertRaises(SystemExit) as cm:
                dt._github_fields(1, 2)
        self.assertIn("closingIssuesReferences", str(cm.exception))

    def test_github_fields_refuses_when_the_issue_reply_omits_url(self):
        """url joined the required set with the URL-identity migration; if it can go missing
        the comparison's left operand is absent and the guard is off."""
        def fake_run(argv, **kw):
            if argv[:2] == ["gh", "api"]:
                return _FakeProc("[]")   # empty timeline: never closed (#3559)
            if argv[:3] == ["gh", "issue", "view"]:
                return _FakeProc(json.dumps({
                    "createdAt": "2026-06-01T00:00:00Z", "closedAt": None,
                    "labels": [{"name": "P1"}, {"name": "oracle"}],
                }))
            return _FakeProc(json.dumps({
                "createdAt": "2026-06-01T00:30:00Z", "mergedAt": "2026-06-01T01:30:00Z",
                "closingIssuesReferences": []}))

        with mock.patch.object(dt.subprocess, "run", fake_run):
            with self.assertRaises(SystemExit) as cm:
                dt._github_fields(1, 2)
        # tight discriminator: bare "url" also matches the issue_url refusal and the
        # closingIssuesReferences entry-without-url refusal
        self.assertIn("no url field", str(cm.exception))

    def test_strict_rfc3339_is_shared_by_the_cli_check_and_the_ledger_validator(self):
        """`fromisoformat` is far more permissive than RFC-3339: it accepts basic format,
        week dates and sub-minute offsets, all of which this ledger's published schema
        forbids. That leniency was PRE-EXISTING (identical on origin/main's _validate), but a
        lenient writer plus a strict published contract is the same fail-open shape as the
        rest of this issue, so ONE recogniser now serves both — and they must agree."""
        good = ("2026-06-10T00:00:00Z", "2026-06-10T00:00:00+00:00",
                "2026-06-10T00:00:00.123456Z", "2026-06-10T00:00:00-07:00")
        bad = ("2026-06-10T00:00:00+00:00:30",   # sub-minute offset
               "20260610T000000Z",               # basic format
               "2026-W24-1T00:00:00Z",           # week date
               "2026-06-10T00:00:00",            # tz-naive
               "2026-06-10",                     # date only
               "2026-13-10T00:00:00Z",           # impossible month (regex alone misses this)
               "2026-02-31T00:00:00Z",           # impossible day
               "2026-06-10t00:00:00z",           # lowercase; deliberately not accepted
               "2026-06-10T00:00:00Z\n",         # trailing newline (\Z, not $)
               "", "not-a-timestamp")
        for v in good:
            with self.subTest(good=v):
                dt._require_full_timestamp(v, "x")      # must not raise
                errs = []
                dt._validate(v, {"type": "string", "format": "date-time"}, "x", errs)
                self.assertEqual(errs, [], f"{v!r} rejected by the ledger validator")
        for v in bad:
            with self.subTest(bad=v):
                with self.assertRaises(SystemExit):
                    dt._require_full_timestamp(v, "x")
                errs = []
                dt._validate(v, {"type": "string", "format": "date-time"}, "x", errs)
                self.assertNotEqual(errs, [], f"{v!r} accepted by the ledger validator")

    def test_schema_pattern_and_is_rfc3339_agree_exactly(self):
        """The schema now STATES the canonical subset as a `pattern`, so the tool cannot be
        looser than the published contract (round 10) OR tighter than it (round 11). Both
        complaints are unexpressible once the two are the same fact — so assert they ARE.

        This is the schema's own two-audience rule: `format` is annotation-only for a
        standard Draft 2020-12 validator, so `pattern` is what actually enforces the subset
        for a third-party reader.
        """
        import re as _re
        schema = json.loads(dt.DEFAULT_SCHEMA.read_text())
        pats = {f: schema["properties"][f]["pattern"]
                for f in ("created_at", "pr_opened_at", "merged_at", "closed_at",
                          "stamped_at")}
        self.assertEqual(len(set(pats.values())), 1, f"timestamp patterns diverged: {pats}")
        # ONE FACT, not two approximations: the schema's pattern must be the SAME STRING the
        # tool compiles. Rounds 10-12 of this issue were three successive complaints about
        # these two disagreeing (tool looser / tool tighter / schema looser), so identity is
        # asserted mechanically rather than re-argued.
        self.assertEqual(next(iter(pats.values())), dt._TIMESTAMP_PATTERN)
        pattern = _re.compile(next(iter(pats.values())))
        for v in ("2026-06-10T00:00:00Z", "2026-06-10T00:00:00+00:00",
                  "2026-06-10T00:00:00.123456Z", "2026-06-10T00:00:00-07:00",
                  "2026-06-10T00:00:00+00:00:30", "20260610T000000Z",
                  "2026-W24-1T00:00:00Z", "2026-06-10T00:00:00", "2026-06-10",
                  "2026-06-10t00:00:00z", "2026-06-10T23:59:60Z", "", "x",
                  "2026-02-31T00:00:00Z", "2026-13-01T00:00:00Z", "2025-02-29T00:00:00Z",
                  "2026-06-10T99:99:00Z", "2026-06-10T24:00:00Z",
                  "2026-06-10T00:00:00+99:99", "2026-00-10T00:00:00Z",
                  "2026-06-32T00:00:00Z", "2026-06-10T00:60:00Z", "0000-06-10T00:00:00Z",
                  # round 14: Python \\d matches Unicode digits; [0-9] does not
                  "\u0662\u0660\u0662\u0666-06-10T00:00:00Z",
                  "2026-\u0660\u0666-10T00:00:00Z"):
            with self.subTest(value=v):
                # the pattern is a SYNTAX rule; _is_rfc3339 is syntax AND calendar. They must
                # agree on every value whose calendar is valid, and the tool may only be
                # stricter where the calendar is impossible.
                # THE RULE, not an exception list. An earlier revision enumerated the
                # permitted divergences, then a later one deleted the list and claimed full
                # equivalence — which was false, because a regex cannot judge CALENDAR
                # validity. So assert the invariant instead: the pattern may be more
                # permissive than the tool ONLY on a value that is syntactically well-formed
                # but calendrically impossible, and never the other way round. That is
                # checkable without naming any value, so it cannot be overclaimed again.
                by_pattern = bool(pattern.search(v))
                by_tool = dt._is_rfc3339(v)
                if by_tool:
                    self.assertTrue(by_pattern,
                                    f"{v!r}: tool accepts but the PUBLISHED pattern rejects "
                                    f"— lint would accept a record the schema forbids")
                elif by_pattern:
                    # Divergence permitted ONLY for a calendrically impossible DATE — the one
                    # thing a regex provably cannot express. Asserted SPECIFICALLY, not as
                    # "any parse failure": the looser form licensed a Unicode-digit value
                    # (which also fails to parse) and so hid a SYNTAX divergence that WAS
                    # expressible. Proven by constructing the date directly.
                    y, mo, dy = int(v[0:4]), int(v[5:7]), int(v[8:10])
                    with self.assertRaises(ValueError,
                                           msg=f"{v!r}: the published pattern accepts it and "
                                               f"the tool refuses it, but {y:04d}-{mo:02d}-"
                                               f"{dy:02d} is a REAL date — so the divergence "
                                               f"is not the documented calendar one and the "
                                               f"pattern is wrong"):
                        _datetime.date(y, mo, dy)

    def test_every_committed_timestamp_is_strict_rfc3339(self):
        """The tightening must not red the committed ledger — asserted, not assumed."""
        checked = 0
        for line in dt.DEFAULT_LEDGER.read_text().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            for key in ("created_at", "pr_opened_at", "merged_at", "closed_at", "stamped_at"):
                v = rec.get(key)
                if isinstance(v, str):
                    checked += 1
                    self.assertTrue(dt._is_rfc3339(v), f"{key}={v!r} in {rec.get('issue')}")
        self.assertGreater(checked, 0, "committed ledger is empty - this would be vacuous")

    def test_record_refuses_a_date_only_or_tz_naive_timestamp(self):
        """_parse_ts accepts both, and both then raise TypeError out of the cycle-time
        subtraction — so parseability is not the property worth asserting; being a full
        instant is. Checked for EVERY authoritative timestamp, not just closed_at."""
        for field, bad in (("closed_at", "2026-06-10"),
                           ("closed_at", "2026-06-10T04:00:00"),
                           ("merged_at", "2026-06-10"),
                           ("merged_at", "2026-06-10T03:00:00")):
            with self.subTest(field=field, value=bad), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                kwargs = {"closed_at": "2026-06-10T04:00:00Z"}
                kwargs[field] = bad
                argv = self._rec_argv(ledger, self._ghfields(
                    tmp, kwargs["closed_at"], merged_at=kwargs.get("merged_at",
                                                                   "2026-06-10T03:00:00Z")))
                with self.assertRaises(SystemExit) as cm:
                    dt.main(argv)
                self.assertIn(field, str(cm.exception))
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_refuses_a_truthy_non_string_timestamp(self):
        """_parse_ts calls .endswith() on its argument, so a truthy non-string raises
        AttributeError — neither of the exceptions _require_full_timestamp catches, so it
        would escape as the very traceback that helper exists to prevent. The falsy half is
        already covered by the presence checks; this is the truthy half."""
        for field, bogus in (("merged_at", 1780000000), ("merged_at", ["2026-06-10"]),
                             ("closed_at", {"at": "2026-06-10"}), ("closed_at", 17)):
            with self.subTest(field=field, value=bogus), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                gh = tmp / "gh.json"
                fields = {
                    "issue": 3393,
                    "pr": 3467,
                    "created_at": "2026-06-10T00:00:00Z",
                    "pr_opened_at": "2026-06-10T01:00:00Z",
                    "merged_at": "2026-06-10T03:00:00Z",
                    "closed_at": "2026-06-10T04:00:00Z",
                    "priority": "P1", "routing": "oracle",
                    "pr_closes_this_issue": False,
                }
                fields[field] = bogus
                gh.write_text(json.dumps(fields))
                with self.assertRaises(SystemExit) as cm:
                    dt.main(self._rec_argv(ledger, str(gh)))
                self.assertIn(field, str(cm.exception))
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_refuses_an_absent_closed_at_key(self):
        """The KEY carries the measurement; its absence is unmeasured, never a slice."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            gh = tmp / "gh.json"
            gh.write_text(json.dumps({
                "issue": 3393,
                "pr": 3467,
                "created_at": "2026-06-10T00:00:00Z",
                "pr_opened_at": "2026-06-10T01:00:00Z",
                "merged_at": "2026-06-10T03:00:00Z",
                "priority": "P1", "routing": "oracle",
            }))
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._rec_argv(ledger, str(gh), "--slice"))
            self.assertIn("absent", str(cm.exception))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_refuses_a_falsy_non_null_closed_at(self):
        """A truthiness read would accept "" / 0 / False as authoritative null and write a
        valid-looking slice record. Only None or a timestamp string is a measurement."""
        for bogus in ("", "   ", 0, False, []):
            with self.subTest(closed_at=bogus), tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                ledger = tmp / "ledger.jsonl"
                with self.assertRaises(SystemExit) as cm:
                    dt.main(self._rec_argv(ledger, self._ghfields(tmp, bogus), "--slice"))
                self.assertIn("closed_at", str(cm.exception))
                self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_rejects_an_unparseable_closed_at_rather_than_tracebacking(self):
        """An unparseable closed_at is refused BEFORE any slice/completed classification —
        with or without --slice — so it can never reach cycle_time_s's arithmetic and surface
        as a bare ValueError traceback instead of a named bad invocation."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._rec_argv(ledger, self._ghfields(tmp, "not-a-timestamp"),
                                       "--slice"))
            self.assertIn("not a strict RFC-3339", str(cm.exception))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._rec_argv(ledger, self._ghfields(tmp, "not-a-timestamp")))
            self.assertIn("not a strict RFC-3339", str(cm.exception))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_record_without_slice_refused_when_the_issue_is_open(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._rec_argv(ledger, self._ghfields(tmp, None)))
            msg = str(cm.exception)
            self.assertFalse(ledger.exists() and ledger.read_text().strip())
            # the refusal must be ACTIONABLE: it names the sanctioned path...
            self.assertIn("--slice", msg)
            self.assertIn("open", msg.lower())
            # ...names BOTH forbidden workarounds...
            self.assertIn("3550", msg)
            self.assertIn("forbidden", msg.lower())
            self.assertRegex(msg.lower(), r"clos\w+ the issue")
            self.assertIn("hand-append", msg.lower())
            self.assertIn("jsonl", msg.lower())
            # ...and must NOT read as an instruction to close the issue.
            self.assertNotIn("records only a merged, closed issue", msg)

    def test_slice_does_not_relax_the_other_authoritative_timestamps(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            with self.assertRaises(SystemExit) as cm:
                dt.main(self._rec_argv(
                    ledger, self._ghfields(tmp, None, merged_at=None), "--slice"))
            self.assertIn("merged_at", str(cm.exception))
            self.assertFalse(ledger.exists() and ledger.read_text().strip())

    def test_slice_record_round_trips_through_record_then_lint(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            ledger = tmp / "ledger.jsonl"
            self.assertEqual(
                dt.main(self._rec_argv(ledger, self._ghfields(tmp, None), "--slice")), 0)
            # a SECOND slice of the SAME open issue under a NEW pr is a legitimate cycle
            self.assertEqual(
                dt.main(self._rec_argv(
                    ledger,
                    self._ghfields(tmp, None, name="gh-3429.json", pr=3429),
                    "--slice", pr=3429)), 0)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 0, out.getvalue())
            self.assertIn("OK", out.getvalue())

    # ---- retro must not read slices as completed issues -----------------------------
    def test_aggregate_counts_slice_records_informationally(self):
        completed = self._committed()
        sliced = dict(completed)
        sliced["closed_at"] = None
        self.assertEqual(dt.aggregate([completed])["slice_records"], 0)
        self.assertEqual(dt.aggregate([sliced])["slice_records"], 1)
        self.assertEqual(dt.aggregate([completed, sliced])["slice_records"], 1)

    def test_slice_records_carries_no_retro_weight(self):
        self.assertNotIn("slice_records", dt.RETRO_WEIGHTS)
        tally = dt.aggregate([{**self._committed(), "closed_at": None}])
        self.assertNotIn("slice_records", [row[0] for row in dt.rank(tally)])

    def test_retro_reports_the_slice_class_distinctly(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            lines = (FIXTURES / "sample-ledger.jsonl").read_text().splitlines()
            completed = json.loads(lines[0])
            sliced = json.loads(lines[1])
            sliced["closed_at"] = None
            sliced["cycle_time_s"] = dt._seconds_between(sliced["created_at"], sliced["merged_at"])
            ledger.write_text(json.dumps(completed) + "\n" + json.dumps(sliced) + "\n")
            _, errors = dt.load_ledger(ledger, SCHEMA)
            self.assertEqual(errors, [])
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = dt.main(["retro", "--ledger", str(ledger),
                              "--open-issues-json", str(FIXTURES / "open-issues-empty.json")])
            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("SLICE", text)
            self.assertIn("3550", text)
            self.assertIn("separate deliveries", text)

    def test_retro_says_nothing_about_slices_on_an_all_completed_ledger(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = dt.main(["retro", "--ledger", str(FIXTURES / "sample-ledger.jsonl"),
                          "--open-issues-json", str(FIXTURES / "open-issues-empty.json")])
        self.assertEqual(rc, 0)
        self.assertNotIn("SLICE", out.getvalue())


class LintTests(unittest.TestCase):
    def test_clean_ledger_passes(self):
        rc = dt.main(["lint", "--ledger", str(FIXTURES / "sample-ledger.jsonl")])
        self.assertEqual(rc, 0)

    def test_validate_alias_works(self):
        rc = dt.main(["validate", "--ledger", str(FIXTURES / "sample-ledger.jsonl")])
        self.assertEqual(rc, 0)

    def test_const_bool_coercion_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
            rec["schema"] = True   # True == 1 but must NOT satisfy const: 1
            ledger.write_text(json.dumps(rec) + "\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 1)

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

    def test_unknown_field_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
            rec["reworkk"] = 9    # typo'd field; intended 'rework' silently absent otherwise
            ledger.write_text(json.dumps(rec) + "\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 1)
            self.assertIn("unknown field 'reworkk'", err.getvalue())

    def test_duplicate_cycle_is_flagged(self):
        # same (issue, pr) twice is a genuine duplicate cycle -> error naming both.
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            line = (FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0]
            ledger.write_text(line + "\n" + line + "\n")   # same (issue, pr) twice
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 1)
            msg = err.getvalue()
            self.assertIn("duplicate record for issue #1001 / pr #2001", msg)
            self.assertIn("first seen line 1", msg)

    def test_reopen_cycle_same_issue_different_pr_is_valid(self):
        # issue #2314: a reopened issue that ships twice (same issue, DIFFERENT pr) is a
        # legitimate per-cycle record, NOT a duplicate — lint must PASS.
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            first = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
            second = dict(first)
            second["pr"] = first["pr"] + 1     # same issue, a new shipped PR
            ledger.write_text(json.dumps(first) + "\n" + json.dumps(second) + "\n")
            rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 0)

    def test_non_object_line_is_clean_error_not_crash(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            ledger.write_text("[1, 2, 3]\n42\nnull\n")  # valid JSON, not objects
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main(["lint", "--ledger", str(ledger)])
            self.assertEqual(rc, 1)                 # clean failure, no traceback

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

    def test_retro_dryrun_degrades_when_gh_unavailable(self):
        # No --open-issues-json -> dedup would hit live gh; a gh failure in dry-run must
        # warn and still print the preview, not abort.
        def boom(argv, **kw):
            raise FileNotFoundError("gh")

        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(dt.subprocess, "run", boom):
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = dt.main(["retro", "--ledger", str(FIXTURES / "sample-ledger.jsonl")])
        self.assertEqual(rc, 0)
        self.assertIn("DRY RUN", out.getvalue())
        self.assertIn("dedup check skipped", err.getvalue())

    def test_retro_refuses_duplicate_issue_ledger(self):
        # a duplicate issue would double-count in the tally — retro must refuse (run lint),
        # mirroring lint's invariant via the shared loader.
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            line = (FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0]
            ledger.write_text(line + "\n" + line + "\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = dt.main(["retro", "--ledger", str(ledger),
                              "--open-issues-json", str(FIXTURES / "open-issues-empty.json")])
            self.assertEqual(rc, 1)
            self.assertIn("run `lint`", err.getvalue())

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


class _FakeProc:
    def __init__(self, stdout):
        self.stdout = stdout


class GhPathTests(unittest.TestCase):
    """Cover the live `gh`-touching paths offline by monkeypatching subprocess.run."""

    def test_github_fields_builds_argv_and_maps_json(self):
        def fake_run(argv, **kw):
            if argv[:2] == ["gh", "api"]:
                return _FakeProc("[]")   # empty timeline: never closed (#3559)
            if argv[:3] == ["gh", "issue", "view"]:
                self.assertIn("createdAt,closedAt,labels,url", argv)
                return _FakeProc(json.dumps({
                    "createdAt": "2026-06-01T00:00:00Z",
                    "closedAt": "2026-06-01T02:00:00Z",
                    "labels": [{"name": "P1"}, {"name": "oracle"}],
                    "url": "https://github.com/pmcfadin/cqlite/issues/1234",
                }))
            if argv[:3] == ["gh", "pr", "view"]:
                self.assertIn("createdAt,mergedAt,closingIssuesReferences", argv)
                return _FakeProc(json.dumps({
                    "createdAt": "2026-06-01T00:30:00Z",
                    "mergedAt": "2026-06-01T01:30:00Z",
                    "closingIssuesReferences": [
                        {"number": 1234,
                         "url": "https://github.com/pmcfadin/cqlite/issues/1234"}],
                }))
            raise AssertionError(f"unexpected argv: {argv}")

        with mock.patch.object(dt.subprocess, "run", fake_run):
            fields = dt._github_fields(1234, 5678)
        self.assertEqual(fields["created_at"], "2026-06-01T00:00:00Z")
        self.assertEqual(fields["closed_at"], "2026-06-01T02:00:00Z")
        self.assertEqual(fields["pr_opened_at"], "2026-06-01T00:30:00Z")
        self.assertEqual(fields["merged_at"], "2026-06-01T01:30:00Z")
        self.assertEqual(fields["priority"], "P1")
        self.assertEqual(fields["routing"], "oracle")  # explicit label, not inferred
        # the live path must ALWAYS supply the timeline-replayed answer: --slice refuses
        # when the key is absent, so omitting it here would make every real slice stamp fail
        # (issue #3559). The empty timeline above means "never closed" -> open at mergedAt.
        self.assertIs(fields["issue_open_at_merge"], True)
        self.assertIsNone(fields["issue_close_event_at"])
        self.assertNotIn("state_reason", fields)
        # the live path DERIVES the boolean from both authoritative queries; the operands
        # never leave this function, so they cannot be made to disagree
        self.assertIs(fields["pr_closes_this_issue"], True)

    def _issue_pr_fakes(self, issue_url, closing_url):
        def fake_run(argv, **kw):
            if argv[:2] == ["gh", "api"]:
                return _FakeProc("[]")   # empty timeline: never closed (#3559)
            if argv[:3] == ["gh", "issue", "view"]:
                return _FakeProc(json.dumps({
                    "createdAt": "2026-06-01T00:00:00Z", "closedAt": None,
                    "labels": [{"name": "P1"}, {"name": "oracle"}],
                    "url": issue_url}))
            return _FakeProc(json.dumps({
                "createdAt": "2026-06-01T00:30:00Z", "mergedAt": "2026-06-01T01:30:00Z",
                "closingIssuesReferences": ([] if closing_url is None
                                            else [{"number": 3393, "url": closing_url}])}))
        return fake_run

    def test_github_fields_derives_identity_by_repository_not_by_number(self):
        """`other/repo#3393` shares a NUMBER with this repo's #3393 and is a DIFFERENT issue,
        so the boolean must be False and such a slice must stay recordable.

        Pinned HERE because collapsing the window guard (issue #3550) moved the operands OUT
        of the --from-json seam — where they had an end-to-end test — and INTO this function,
        and the seam-level test was deleted without a replacement. Three mutants survived a
        green 120-test suite as a result: a number-scoped comparison, dropping the
        (issue, pr) binding from the returned dict, and deleting the canonical-issue-URL
        refusal. Each assertion below kills one.
        """
        with mock.patch.object(dt.subprocess, "run", self._issue_pr_fakes(
                "https://github.com/pmcfadin/cqlite/issues/3393",
                "https://github.com/other/repo/issues/3393")):
            fields = dt._github_fields(3393, 3467)
        # a same-NUMBER, different-REPOSITORY closing ref is not this issue
        self.assertIs(fields["pr_closes_this_issue"], False)
        # the live path must supply the (issue, pr) binding build_record requires
        self.assertEqual((fields["issue"], fields["pr"]), (3393, 3467))

        # and the positive control: the SAME repository's issue does close it
        with mock.patch.object(dt.subprocess, "run", self._issue_pr_fakes(
                "https://github.com/pmcfadin/cqlite/issues/3393",
                "https://github.com/pmcfadin/cqlite/issues/3393")):
            self.assertIs(dt._github_fields(3393, 3467)["pr_closes_this_issue"], True)

    def test_github_fields_refuses_a_non_canonical_issue_url(self):
        """The refusal added with the collapse had no test. Its failure mode is fail-OPEN:
        an unrecognised issue url yields an identity of None, which matches nothing, so the
        window guard silently reports 'this PR does not close this issue' and a completed
        delivery is recorded as a slice."""
        for bad in ("https://github.com/pmcfadin/cqlite/pull/3393",
                    "https://example.com/pmcfadin/cqlite/issues/3393",
                    "https://github.com/pmcfadin/cqlite/issues/3393\n", "", "3393"):
            with self.subTest(url=bad):
                with mock.patch.object(dt.subprocess, "run", self._issue_pr_fakes(bad, None)):
                    with self.assertRaises(SystemExit) as cm:
                        dt._github_fields(3393, 3467)
                self.assertIn("canonical", str(cm.exception))

    def test_github_fields_raises_on_multiple_priority_labels(self):
        def fake_run(argv, **kw):
            if argv[:2] == ["gh", "api"]:
                return _FakeProc("[]")   # empty timeline: never closed (#3559)
            if argv[:3] == ["gh", "issue", "view"]:
                return _FakeProc(json.dumps({
                    "createdAt": "2026-06-01T00:00:00Z",
                    "closedAt": "2026-06-01T02:00:00Z",
                    "labels": [{"name": "P1"}, {"name": "P2"}],  # invariant violation
                    # complete fixture: without these the missing-field check (#3550) short-
                    # circuits and this test never reaches the label logic it exists for
                    "url": "https://github.com/pmcfadin/cqlite/issues/1",
                }))
            return _FakeProc(json.dumps({
                "createdAt": "2026-06-01T00:30:00Z", "mergedAt": "2026-06-01T01:30:00Z",
                "closingIssuesReferences": []}))

        with mock.patch.object(dt.subprocess, "run", fake_run):
            with self.assertRaises(SystemExit) as cm:
                dt._github_fields(1, 2)
        # assert the LABEL error, not merely that something exited
        self.assertIn("multiple priority labels", str(cm.exception))

    def test_github_fields_design_label_maps(self):
        def fake_run(argv, **kw):
            if argv[:2] == ["gh", "api"]:
                return _FakeProc("[]")   # empty timeline: never closed (#3559)
            if argv[:3] == ["gh", "issue", "view"]:
                return _FakeProc(json.dumps({
                    "createdAt": "2026-06-01T00:00:00Z",
                    "closedAt": "2026-06-01T02:00:00Z",
                    "labels": [{"name": "P2"}, {"name": "design"}],
                    "url": "https://github.com/pmcfadin/cqlite/issues/1234",
                }))
            return _FakeProc(json.dumps({
                "createdAt": "2026-06-01T00:30:00Z", "mergedAt": "2026-06-01T01:30:00Z",
                "closingIssuesReferences": []}))

        with mock.patch.object(dt.subprocess, "run", fake_run):
            fields = dt._github_fields(1, 2)
        self.assertEqual(fields["routing"], "design")
        self.assertEqual(fields["priority"], "P2")

    def test_github_fields_raises_on_conflicting_routing_labels(self):
        def fake_run(argv, **kw):
            if argv[:2] == ["gh", "api"]:
                return _FakeProc("[]")   # empty timeline: never closed (#3559)
            if argv[:3] == ["gh", "issue", "view"]:
                return _FakeProc(json.dumps({
                    "createdAt": "2026-06-01T00:00:00Z",
                    "closedAt": "2026-06-01T02:00:00Z",
                    "labels": [{"name": "P2"}, {"name": "oracle"}, {"name": "design"}],
                    "url": "https://github.com/pmcfadin/cqlite/issues/1",
                }))
            return _FakeProc(json.dumps({
                "createdAt": "2026-06-01T00:30:00Z", "mergedAt": "2026-06-01T01:30:00Z",
                "closingIssuesReferences": []}))

        with mock.patch.object(dt.subprocess, "run", fake_run):
            with self.assertRaises(SystemExit) as cm:
                dt._github_fields(1, 2)
        self.assertIn("routing", str(cm.exception))

    def test_gh_failure_becomes_clean_systemexit(self):
        import subprocess as _sp

        def boom(argv, **kw):
            raise _sp.CalledProcessError(1, argv, stderr="gh: not authenticated")

        with mock.patch.object(dt.subprocess, "run", boom):
            with self.assertRaises(SystemExit):
                dt._gh(["gh", "issue", "view", "1"])

    def test_retro_file_invokes_gh_issue_create(self):
        calls = []

        def fake_run(argv, **kw):
            if argv[:2] == ["gh", "api"]:
                return _FakeProc("[]")   # empty timeline: never closed (#3559)
            calls.append(argv)
            return _FakeProc("https://github.com/pmcfadin/cqlite/issues/123")

        with mock.patch.object(dt.subprocess, "run", fake_run):
            rc = dt.main([
                "retro",
                "--ledger", str(FIXTURES / "sample-ledger.jsonl"),
                "--open-issues-json", str(FIXTURES / "open-issues-empty.json"),
                "--file",
            ])
        self.assertEqual(rc, 0)
        self.assertTrue(calls, "expected a gh subprocess call")
        argv = calls[-1]
        self.assertEqual(argv[:3], ["gh", "issue", "create"])
        self.assertIn("flow-meta", argv)
        # the marker for the top category (rework) must be in the body
        body = argv[argv.index("--body") + 1]
        self.assertIn("<!-- RETRO:rework -->", body)


    def test_retro_empty_ledger(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            ledger.write_text("")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = dt.main(["retro", "--ledger", str(ledger)])
            self.assertEqual(rc, 0)
            self.assertIn("ledger is empty", out.getvalue())

    def test_retro_all_zero_files_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Path(d) / "ledger.jsonl"
            rec = json.loads((FIXTURES / "sample-ledger.jsonl").read_text().splitlines()[0])
            rec.update({"claim_collisions": 0, "rebase_events": 0, "roborev_findings": 0,
                        "rework": 0, "gate": "pass", "gate_runs": 1})
            ledger.write_text(json.dumps(rec) + "\n")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = dt.main(["retro", "--ledger", str(ledger),
                              "--open-issues-json", str(FIXTURES / "open-issues-empty.json")])
            self.assertEqual(rc, 0)
            self.assertIn("no recurring failures", out.getvalue())


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

    def test_terminal_fail_counts_all_rounds(self):
        # gate == "fail" counts EVERY run as a failed round (no -1 for a final pass):
        # gate_runs=3, fail -> 3; the same runs with a terminal pass would be 2.
        fail_rec = {"gate": "fail", "gate_runs": 3, "claim_collisions": 0,
                    "rebase_events": 0, "roborev_findings": 0, "rework": 0}
        pass_rec = {**fail_rec, "gate": "pass"}
        self.assertEqual(dt.aggregate([fail_rec])["gate_failures"], 3)
        self.assertEqual(dt.aggregate([pass_rec])["gate_failures"], 2)


class SchemaCouplingDeclarationTests(unittest.TestCase):
    """issue #3448 / roborev job 67: UNCONDITIONAL coverage of the schema-side coupling.

    StandardValidatorCouplingTests below proves a real Draft 2020-12 validator ENFORCES the
    coupling, but it needs `jsonschema`, and this gate component's contract is explicitly
    "No third-party deps, no datasets, no network" — so on a host without the lib those
    tests SKIP, and a skip is not coverage ("a SKIP means the check never ran, which is the
    vacuous pass itself"). These tests therefore assert, with the stdlib alone and on every
    host, that the published schema DECLARES the coupling — so deleting or weakening the
    allOf can never pass CI silently.

    Deliberately structural, NOT a hand-rolled if/then evaluator: re-implementing JSON
    Schema semantics here would be a second implementation whose correctness is only
    knowable by differential testing against a real validator, which is precisely the trap
    CLAUDE.md records. Structure is checked here; SEMANTICS are checked against the real
    validator when it is installed.
    """

    def setUp(self):
        self.schema = json.loads(dt.DEFAULT_SCHEMA.read_text())

    def test_schema_declares_both_coupling_directions(self):
        clauses = self.schema.get("allOf")
        self.assertIsInstance(clauses, list, "schema lost its top-level allOf coupling block")
        self.assertEqual(len(clauses), 2, f"expected exactly 2 coupling clauses, got {len(clauses)}")

        def shape(c):
            """(condition field, condition const, consequent field, consequent const)."""
            if_props = c.get("if", {}).get("properties", {})
            then_props = c.get("then", {}).get("properties", {})
            self.assertEqual(len(if_props), 1, f"clause 'if' must constrain exactly one field: {c}")
            self.assertEqual(len(then_props), 1, f"clause 'then' must constrain exactly one field: {c}")
            (cf, cs), = if_props.items()
            (tf, ts), = then_props.items()
            # the condition must also REQUIRE its field, else it holds vacuously when absent
            self.assertIn(cf, c.get("if", {}).get("required", []),
                          f"clause condition on {cf!r} must list it in 'required', or it is vacuous: {c}")
            return (cf, cs.get("const"), tf, ts.get("const"))

        shapes = {shape(c) for c in clauses}
        self.assertEqual(
            shapes,
            {("gate", "not-run", "gate_runs", 0), ("gate_runs", 0, "gate", "not-run")},
            "the schema no longer declares gate 'not-run' <=> gate_runs 0 in BOTH directions")

    def test_gate_enum_and_minimum_still_admit_the_state(self):
        props = self.schema["properties"]
        self.assertIn("not-run", props["gate"]["enum"])
        self.assertEqual(props["gate_runs"]["minimum"], 0)

    def test_coupling_is_documented_as_enforced_in_both_places(self):
        """A reader of the published schema must not conclude it relies on the tool alone."""
        for field in ("gate", "gate_runs"):
            desc = self.schema["properties"][field]["description"]
            self.assertIn("allOf", desc, f"{field} description does not mention the schema-side allOf")
            self.assertIn("_validate_gate_coupling", desc,
                          f"{field} description does not mention the tool-side check")


try:  # optional: only needed by StandardValidatorCouplingTests below
    import jsonschema as _jsonschema
    from jsonschema import Draft202012Validator as _Draft202012Validator
except ImportError:  # pragma: no cover - exercised on hosts without the lib
    _jsonschema = None
    _Draft202012Validator = None


@unittest.skipUnless(_jsonschema is not None,
                     "jsonschema not installed; the schema-side coupling is unverifiable here")
class StandardValidatorCouplingTests(unittest.TestCase):
    """issue #3448 / roborev job 66: the coupling must hold under a STANDARD validator.

    The repo's own `_validate` is a minimal JSON-Schema subset (no if/then), so encoding the
    coupling ONLY in delivery-telemetry.py left the published schema independently accepting
    incoherent records like {"gate": "pass", "gate_runs": 0} — i.e. any third-party Draft
    2020-12 consumer of docs/reports/delivery-telemetry.schema.json would not enforce the
    documented contract. The schema now carries an `allOf` of two if/then clauses; these
    tests assert against the REAL validator, not our own, because our own is exactly the one
    that cannot see the keywords under test.
    """

    def setUp(self):
        self.schema = json.loads(dt.DEFAULT_SCHEMA.read_text())
        self.validator = _Draft202012Validator(self.schema)
        self.base = json.loads(dt.DEFAULT_LEDGER.read_text().splitlines()[0])

    def _valid(self, gate, gate_runs):
        rec = dict(self.base)
        rec["gate"] = gate
        rec["gate_runs"] = gate_runs
        return not list(self.validator.iter_errors(rec))

    def test_schema_is_itself_a_valid_draft_2020_12_schema(self):
        _Draft202012Validator.check_schema(self.schema)

    def test_standard_validator_accepts_not_run_with_zero(self):
        self.assertTrue(self._valid("not-run", 0))

    def test_standard_validator_rejects_not_run_with_nonzero(self):
        # the finding's second case
        self.assertFalse(self._valid("not-run", 2))
        self.assertFalse(self._valid("not-run", 1))

    def test_standard_validator_rejects_outcome_with_zero_runs(self):
        # the finding's first case
        self.assertFalse(self._valid("pass", 0))
        self.assertFalse(self._valid("fail", 0))

    def test_standard_validator_still_accepts_ordinary_gated_records(self):
        self.assertTrue(self._valid("pass", 1))
        self.assertTrue(self._valid("fail", 3))

    def test_every_committed_ledger_record_validates_under_the_standard_validator(self):
        """AC5 backward-compatibility, checked against the real validator too.

        Fail-closed on an empty corpus: a committed ledger that read as 0 records would
        otherwise make this pass vacuously.
        """
        checked = 0
        for line in dt.DEFAULT_LEDGER.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            checked += 1
            errors = list(self.validator.iter_errors(json.loads(line)))
            self.assertEqual(errors, [], f"committed record {checked} no longer validates: {errors}")
        self.assertGreater(checked, 0, "committed ledger is empty - this test would be vacuous")


@unittest.skipUnless(_jsonschema is not None,
                     "jsonschema not installed; the nullable closed_at is unverifiable here")
class StandardValidatorSliceTests(unittest.TestCase):
    """issue #3550: the nullable `closed_at` must hold under a STANDARD Draft 2020-12
    validator too — the published schema is consumed by third parties, not only by this
    repo's minimal subset validator (which is exactly the one that cannot see a union type
    it does not implement).
    """

    def setUp(self):
        self.schema = json.loads(dt.DEFAULT_SCHEMA.read_text())
        self.validator = _Draft202012Validator(self.schema)
        self.base = json.loads(dt.DEFAULT_LEDGER.read_text().splitlines()[0])

    def _valid(self, closed_at):
        rec = dict(self.base)
        rec["closed_at"] = closed_at
        return not list(self.validator.iter_errors(rec))

    def test_schema_is_still_a_valid_draft_2020_12_schema(self):
        _Draft202012Validator.check_schema(self.schema)

    def test_standard_validator_accepts_a_null_closed_at(self):
        self.assertTrue(self._valid(None))

    def test_standard_validator_still_accepts_a_timestamp_closed_at(self):
        self.assertTrue(self._valid("2026-08-29T12:00:00Z"))

    def test_standard_validator_still_rejects_a_wrong_typed_closed_at(self):
        self.assertFalse(self._valid(1749513600))
        self.assertFalse(self._valid(True))

    def test_standard_validator_still_requires_the_closed_at_key(self):
        rec = dict(self.base)
        del rec["closed_at"]
        self.assertTrue(list(self.validator.iter_errors(rec)))

    @unittest.skipUnless(_jsonschema is not None, "jsonschema not installed")
    def test_standard_validator_and_lint_agree_on_timestamps(self):
        """The gap that mattered was between a THIRD-PARTY reader and `lint`: Draft 2020-12
        treats `format` as annotation-only, so before the pattern existed a standard
        validator accepted records lint rejects. Measured through the real validator, not
        inferred from the pattern string."""
        schema = json.loads(dt.DEFAULT_SCHEMA.read_text())
        base = json.loads(dt.DEFAULT_LEDGER.read_text().splitlines()[0])
        for v in ("2026-06-10T00:00:00Z", "2026-06-10T00:00:00+00:00",
                  "2026-06-10T00:00:00.123456Z",
                  "2026-06-10T23:59:60Z",        # leap second: RFC-3339 legal, unparseable
                  "2026-06-10T00:00:00Z\n",     # trailing newline: `$` would have matched
                  "20260610T000000Z", "2026-06-10T00:00:00", "2026-06-10",
                  "2026-06-10t00:00:00z", "2026-06-10T00:00:00+00:00:30",
                  # the impossible-calendar / out-of-range cases round 13 found omitted
                  "2026-02-31T00:00:00Z", "2026-13-01T00:00:00Z", "2025-02-29T00:00:00Z",
                  "2026-06-10T99:99:00Z", "2026-06-10T24:00:00Z",
                  "2026-06-10T00:00:00+99:99", "2026-00-10T00:00:00Z",
                  "2026-06-32T00:00:00Z", "0000-06-10T00:00:00Z",
                  "\u0662\u0660\u0662\u0666-06-10T00:00:00Z",
                  "2026-\u0660\u0666-10T00:00:00Z"):
            with self.subTest(value=v):
                rec = dict(base)
                rec["created_at"] = v
                try:
                    _Draft202012Validator(schema).validate(rec)
                    std_ok = True
                except _jsonschema.ValidationError:
                    std_ok = False
                tool_ok = dt._is_rfc3339(v)
                if tool_ok:
                    self.assertTrue(std_ok, f"{v!r}: lint accepts, standard validator "
                                            f"REJECTS — the schema forbids what lint writes")
                elif std_ok:
                    # permitted ONLY for a calendrically impossible DATE (see the pattern
                    # parity test): a JSON Schema pattern cannot express leap-year rules
                    y, mo, dy = int(v[0:4]), int(v[5:7]), int(v[8:10])
                    with self.assertRaises(ValueError,
                                           msg=f"{v!r}: the standard validator accepts it "
                                               f"and lint refuses it, but {y:04d}-{mo:02d}-"
                                               f"{dy:02d} is a REAL date — undocumented "
                                               f"divergence"):
                        _datetime.date(y, mo, dy)

if __name__ == "__main__":
    unittest.main(verbosity=2)
