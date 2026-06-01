"""Tests for scripts/ci/check_perf_regression.py (Issue #572).

Validates the strict-vs-advisory gate model using fixture Criterion estimate
directories. Each fixture tree mirrors the layout Criterion produces:

    <criterion_dir>/<bench_group>/<bench_name>/<baseline>/estimates.json

with the shape:
    {"median": {"point_estimate": <float_ns>}, ...}

Test matrix:
  A) CPU bench (read/point_lookup) regresses 20% → non-zero exit (FAIL gate)
  B) Massive write/ingest_wal_on swing (50%) with all strict benches OK → zero
     exit (advisory reported, gate passes)
  C) write/ingest_wal_off regresses 20% (strict bench) → non-zero exit
  D) All benches within threshold → zero exit
"""

import importlib.util
import os
import sys
import types

import pytest

# ---------------------------------------------------------------------------
# Import the script as a module (it is not a package, has no __init__)
# ---------------------------------------------------------------------------
_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "check_perf_regression.py"
)
_spec = importlib.util.spec_from_file_location("check_perf_regression", _SCRIPT)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
main = _mod.main

# ---------------------------------------------------------------------------
# Fixture paths
# ---------------------------------------------------------------------------
_FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
_GATE_JSON = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..", "cqlite-core", "benches", "perf-gate.json"
)

# Scenario directories (named to avoid .gitignore's `criterion/` pattern)
_CRIT_CPU_REGRESSION = os.path.join(_FIXTURES, "cpu_regression")
_CRIT_ADVISORY_ONLY = os.path.join(_FIXTURES, "criterion_advisory_only")
_CRIT_WAL_OFF_REGRESSION = os.path.join(_FIXTURES, "criterion_wal_off_regression")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _run(criterion_dir, new_baseline="pr", base_baseline="base"):
    """Invoke main() with the real perf-gate.json and return the exit code."""
    argv = [
        "check_perf_regression.py",
        criterion_dir,
        new_baseline,
        base_baseline,
        _GATE_JSON,
    ]
    return main(argv)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCpuRegressionFails:
    """A CPU-bound bench (read/point_lookup) regressing > threshold → exit 1."""

    def test_read_point_lookup_20pct_regression_fails(self, capsys):
        """20% regression on read/point_lookup must fail the gate (non-zero exit)."""
        exit_code = _run(_CRIT_CPU_REGRESSION)
        assert exit_code != 0, (
            "Expected non-zero exit for a 20% regression in read/point_lookup "
            "(a strictly gated bench), but got exit code 0."
        )

    def test_regression_output_mentions_bench(self, capsys):
        """The failure output must name the regressing bench."""
        _run(_CRIT_CPU_REGRESSION)
        captured = capsys.readouterr()
        assert "read/point_lookup" in captured.out


class TestAdvisoryWalOnNeverFails:
    """write/ingest_wal_on swings (50%) must NOT fail the gate."""

    def test_wal_on_50pct_swing_passes(self, capsys):
        """50% swing on write/ingest_wal_on (advisory bench) must exit 0."""
        exit_code = _run(_CRIT_ADVISORY_ONLY)
        assert exit_code == 0, (
            "Expected zero exit when only write/ingest_wal_on swings (advisory bench), "
            f"but got exit code {exit_code}."
        )

    def test_wal_on_swing_is_reported(self, capsys):
        """The advisory swing must still appear in the output."""
        _run(_CRIT_ADVISORY_ONLY)
        captured = capsys.readouterr()
        assert "write/ingest_wal_on" in captured.out

    def test_wal_on_output_labels_advisory(self, capsys):
        """Output must indicate the bench is advisory (not a hard failure)."""
        _run(_CRIT_ADVISORY_ONLY)
        captured = capsys.readouterr()
        # The script should print something distinguishing advisory status
        assert "advisory" in captured.out.lower() or "ADVISORY" in captured.out


class TestWalOffRegressionFails:
    """write/ingest_wal_off is a strict CPU bench — regressions must fail."""

    def test_wal_off_20pct_regression_fails(self, capsys):
        """20% regression on write/ingest_wal_off (strict bench) must exit non-zero."""
        exit_code = _run(_CRIT_WAL_OFF_REGRESSION)
        assert exit_code != 0, (
            "Expected non-zero exit for a 20% regression in write/ingest_wal_off "
            "(a strictly gated bench), but got exit code 0."
        )

    def test_wal_off_regression_output_mentions_bench(self, capsys):
        """The failure output must name the regressing bench."""
        _run(_CRIT_WAL_OFF_REGRESSION)
        captured = capsys.readouterr()
        assert "write/ingest_wal_off" in captured.out


class TestAllWithinThresholdPasses:
    """When all strict benches are within threshold, exit 0 even with advisory swing."""

    def test_zero_exit_when_no_strict_regressions(self, capsys):
        """Advisory-only scenario (no strict regressions) → exit 0."""
        exit_code = _run(_CRIT_ADVISORY_ONLY)
        assert exit_code == 0

    def test_success_output_present(self, capsys):
        """Success message should appear when no strict regressions."""
        _run(_CRIT_ADVISORY_ONLY)
        captured = capsys.readouterr()
        assert "✅" in captured.out or "All" in captured.out


class TestMissingDataSkipped:
    """Benches missing from either baseline are SKIP, never failures."""

    def test_empty_criterion_dir_returns_nonzero(self, tmp_path, capsys):
        """An empty criterion dir (no data at all) must surface an error, not silent pass."""
        exit_code = _run(str(tmp_path))
        assert exit_code != 0, (
            "Expected non-zero exit when no baseline data exists (should surface loudly)."
        )
