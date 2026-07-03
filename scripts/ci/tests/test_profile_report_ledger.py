"""Tests for the unified history ledger in scripts/profile_report.py (Issue #1566,
Epic A / A5).

Validates that criterion medians + peak heap are written to
`target/profiling/history.jsonl` in the unified `{ts, commit, bench, metric, value,
unit}` schema (one record per metric), that the ledger reads back, and that the
longitudinal per-metric view (latest value + delta vs the previous distinct commit)
round-trips into report.md. Mirrors test_check_perf_regression.py: import the script
as a module and drive its functions with temp fixtures.
"""

import importlib.util
import json
import os
import sys

# ---------------------------------------------------------------------------
# Import scripts/profile_report.py as a module (not a package, no __init__)
# ---------------------------------------------------------------------------
_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "..", "profile_report.py"
)
_spec = importlib.util.spec_from_file_location("profile_report", _SCRIPT)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _write_estimate(criterion_dir, bench_id, median_ns):
    """Write a criterion `new/estimates.json` under <criterion_dir>/<bench_id>/."""
    d = os.path.join(criterion_dir, *bench_id.split("/"), "new")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "estimates.json"), "w") as fh:
        json.dump(
            {
                "median": {"point_estimate": float(median_ns)},
                "mean": {"point_estimate": float(median_ns) * 1.1},
                "std_dev": {"point_estimate": float(median_ns) * 0.05},
            },
            fh,
        )


def _read_ledger_lines(path):
    with open(path) as fh:
        return [json.loads(l) for l in fh if l.strip()]


# ---------------------------------------------------------------------------
# Unit: ledger record build / append / read round-trip
# ---------------------------------------------------------------------------
class TestLedgerRoundTrip:
    def test_build_records_emit_unified_per_metric_schema(self):
        report = {
            "benches": [
                {"id": "read/get_partition_big", "median_ns": 1234.5},
                {"id": "read/full_scan", "median_ns": 9999.0},
            ],
            "heap": {"peak_bytes": 1048576},
        }
        recs = _mod.build_ledger_records(report)
        # 2 bench median_ns records + 1 peak_heap_bytes record.
        assert len(recs) == 3
        for rec in recs:
            assert set(rec.keys()) == set(_mod.LEDGER_FIELDS)
        medians = {r["bench"]: r for r in recs if r["metric"] == "median_ns"}
        assert medians["read/get_partition_big"]["value"] == 1234  # rounded
        assert medians["read/get_partition_big"]["unit"] == "ns"
        heap = [r for r in recs if r["metric"] == "peak_heap_bytes"]
        assert len(heap) == 1
        assert heap[0]["bench"] == "heap"
        assert heap[0]["value"] == 1048576
        assert heap[0]["unit"] == "bytes"
        # A run shares one ts + commit across all its records.
        assert len({r["ts"] for r in recs}) == 1
        assert len({r["commit"] for r in recs}) == 1

    def test_append_then_read_round_trips(self, tmp_path):
        path = str(tmp_path / "history.jsonl")
        records = [
            {"ts": 10, "commit": "aaa", "bench": "read/x", "metric": "median_ns",
             "value": 100, "unit": "ns"},
            {"ts": 10, "commit": "aaa", "bench": "heap", "metric": "peak_heap_bytes",
             "value": 2048, "unit": "bytes"},
        ]
        _mod.append_ledger(path, records)
        # A second append never truncates.
        _mod.append_ledger(path, [
            {"ts": 20, "commit": "bbb", "bench": "read/x", "metric": "median_ns",
             "value": 90, "unit": "ns"},
        ])
        back = _mod.read_ledger(path)
        assert len(back) == 3
        assert back[0]["bench"] == "read/x" and back[0]["value"] == 100

    def test_read_skips_legacy_and_malformed_lines(self, tmp_path):
        path = str(tmp_path / "history.jsonl")
        with open(path, "w") as fh:
            # legacy pre-A5 run-summary line (no `metric` field) — must be skipped
            fh.write(json.dumps({"ts": "iso", "rev": "abc", "benches": {"a": 1}}) + "\n")
            fh.write("not json at all\n")
            fh.write(json.dumps(
                {"ts": 1, "commit": "c", "bench": "b", "metric": "median_ns",
                 "value": 5, "unit": "ns"}) + "\n")
        back = _mod.read_ledger(path)
        assert len(back) == 1
        assert back[0]["metric"] == "median_ns"


# ---------------------------------------------------------------------------
# Unit: longitudinal summary (latest + delta vs previous distinct commit)
# ---------------------------------------------------------------------------
class TestSummarizeHistory:
    def test_delta_is_vs_previous_distinct_commit(self):
        records = [
            {"ts": 1, "commit": "old", "bench": "read/x", "metric": "median_ns",
             "value": 100, "unit": "ns"},
            {"ts": 2, "commit": "new", "bench": "read/x", "metric": "median_ns",
             "value": 150, "unit": "ns"},
            # Re-run of the SAME (latest) commit must not become the delta baseline.
            {"ts": 3, "commit": "new", "bench": "read/x", "metric": "median_ns",
             "value": 150, "unit": "ns"},
        ]
        rows = _mod.summarize_history(records)
        assert len(rows) == 1
        row = rows[0]
        assert row["value"] == 150
        assert row["prev_value"] == 100
        # +50% vs the previous DISTINCT commit ("old"), not 0% vs the re-run.
        assert abs(row["delta_pct"] - 50.0) < 1e-9

    def test_single_commit_has_no_delta(self):
        records = [
            {"ts": 1, "commit": "only", "bench": "b", "metric": "median_ns",
             "value": 42, "unit": "ns"},
        ]
        rows = _mod.summarize_history(records)
        assert rows[0]["value"] == 42
        assert "delta_pct" not in rows[0]


# ---------------------------------------------------------------------------
# Unit: commit resolver honors GIT_COMMIT (mirrors the Rust bench_ledger writer)
# ---------------------------------------------------------------------------
class TestCommitResolver:
    def test_git_commit_env_override_wins(self, monkeypatch):
        # A non-empty GIT_COMMIT wins over `git rev-parse HEAD` so a CI run that
        # sets it does not split its records across two `commit` values.
        monkeypatch.setenv("GIT_COMMIT", "envsha1234")
        assert _mod._git_commit() == "envsha1234"

    def test_git_commit_blank_env_falls_back_to_git(self, monkeypatch):
        # A blank/whitespace override is ignored (matches the Rust `.trim()` guard);
        # falls back to `git rev-parse HEAD` (stubbed) or "unknown".
        monkeypatch.setenv("GIT_COMMIT", "   ")
        monkeypatch.setattr(
            _mod.subprocess, "run",
            lambda *a, **k: type("R", (), {"stdout": "fallbackhead\n"})(),
        )
        assert _mod._git_commit() == "fallbackhead"

    def test_ledger_commit_field_uses_git_commit_env(self, tmp_path, monkeypatch):
        # End-to-end: the ledger `commit` field reflects GIT_COMMIT when set.
        monkeypatch.setenv("GIT_COMMIT", "ledgerenvsha")
        criterion_dir = str(tmp_path / "criterion")
        out_dir = str(tmp_path / "profiling")
        _write_estimate(criterion_dir, "read/x", 1000)
        monkeypatch.setattr(
            sys, "argv",
            ["profile_report.py", "--criterion-dir", criterion_dir, "--out-dir", out_dir],
        )
        assert _mod.main() == 0
        lines = _read_ledger_lines(os.path.join(out_dir, "history.jsonl"))
        assert lines, "ledger must have records"
        assert all(r["commit"] == "ledgerenvsha" for r in lines)


# ---------------------------------------------------------------------------
# End-to-end: main() writes the unified ledger and renders the history table
# ---------------------------------------------------------------------------
class TestMainEndToEnd:
    def test_main_writes_unified_ledger_and_history_table(self, tmp_path, monkeypatch):
        criterion_dir = str(tmp_path / "criterion")
        out_dir = str(tmp_path / "profiling")
        _write_estimate(criterion_dir, "read/get_partition_big", 1000)
        _write_estimate(criterion_dir, "read/full_scan", 5000)

        monkeypatch.setattr(
            sys, "argv",
            ["profile_report.py", "--criterion-dir", criterion_dir, "--out-dir", out_dir],
        )
        rc = _mod.main()
        assert rc == 0

        # The ledger holds unified per-metric records for both benches.
        ledger = os.path.join(out_dir, "history.jsonl")
        lines = _read_ledger_lines(ledger)
        by_bench = {r["bench"]: r for r in lines if r["metric"] == "median_ns"}
        assert by_bench["read/get_partition_big"]["value"] == 1000
        assert by_bench["read/full_scan"]["value"] == 5000
        for r in lines:
            assert set(r.keys()) == set(_mod.LEDGER_FIELDS)

        # report.md contains the History section and the metrics round-trip into it.
        with open(os.path.join(out_dir, "report.md")) as fh:
            md = fh.read()
        assert "## History (latest value + delta vs previous commit)" in md
        assert "read/get_partition_big" in md
        assert "median_ns" in md

    def test_second_run_records_delta_vs_prior_commit(self, tmp_path, monkeypatch):
        criterion_dir = str(tmp_path / "criterion")
        out_dir = str(tmp_path / "profiling")
        _write_estimate(criterion_dir, "read/x", 1000)

        # Run 1 at commit "c1".
        monkeypatch.setattr(_mod, "_git_commit", lambda: "c1")
        monkeypatch.setattr(
            sys, "argv",
            ["profile_report.py", "--criterion-dir", criterion_dir, "--out-dir", out_dir],
        )
        assert _mod.main() == 0

        # Run 2 at commit "c2" with a slower median.
        _write_estimate(criterion_dir, "read/x", 1200)
        monkeypatch.setattr(_mod, "_git_commit", lambda: "c2")
        assert _mod.main() == 0

        rows = _mod.summarize_history(_mod.read_ledger(os.path.join(out_dir, "history.jsonl")))
        row = next(r for r in rows if r["bench"] == "read/x")
        assert row["value"] == 1200
        assert row["prev_value"] == 1000
        assert abs(row["delta_pct"] - 20.0) < 1e-9
