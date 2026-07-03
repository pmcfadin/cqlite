"""Tests for scripts/ci/check_perf_regression.py (Issue #572).

Validates the strict-vs-advisory gate model using fixture Criterion estimate
directories. Each fixture tree mirrors the layout Criterion produces:

    <criterion_dir>/<bench_group>/<bench_name>/<baseline>/estimates.json

with the shape:
    {"median": {"point_estimate": <float_ns>}, ...}

Test matrix:
  A) CPU bench (read/get_partition_big) regresses 20% → non-zero exit (FAIL gate)
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

# Concurrency scaling-floor scenarios (Issue #1564). These trees carry only the
# `pr` baseline because the scaling floor is an *intra-run* ratio
# (degree_ratio · median(n1)/median(n4)) evaluated on the PR baseline alone.
_CRIT_SCALING_PASS = os.path.join(_FIXTURES, "scaling_floor_pass")
_CRIT_SCALING_FAIL = os.path.join(_FIXTURES, "scaling_floor_fail")
# Missing-required-data tree: buffered n4 absent (required floor → MISSING DATA →
# fail), mmap present + healthy. Required-floor data is never legitimately absent in
# CI, so a missing median is a gate failure, not a silent skip (issue #1564).
_CRIT_SCALING_SKIP = os.path.join(_FIXTURES, "scaling_floor_skip")


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
    """A CPU-bound bench (read/get_partition_big) regressing > threshold → exit 1."""

    def test_read_get_partition_big_20pct_regression_fails(self, capsys):
        """20% regression on read/get_partition_big must fail the gate (non-zero exit)."""
        exit_code = _run(_CRIT_CPU_REGRESSION)
        assert exit_code != 0, (
            "Expected non-zero exit for a 20% regression in read/get_partition_big "
            "(a strictly gated bench), but got exit code 0."
        )

    def test_regression_output_mentions_bench(self, capsys):
        """The failure output must name the regressing bench."""
        _run(_CRIT_CPU_REGRESSION)
        captured = capsys.readouterr()
        assert "read/get_partition_big" in captured.out


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


class TestScalingFloor:
    """Concurrency scaling floor (Issue #1564).

    scaling = degree_ratio · median(n1) / median(n4), evaluated on the `pr`
    baseline. Healthy parallel scans measure ≈3.0; a re-serialized read path
    (shared Mutex) collapses median(n4)→≈4·median(n1) so scaling→≈1.0, below the
    1.8 floor in perf-gate.json.
    """

    def test_healthy_scaling_passes(self, capsys):
        """scaling ≈3.0 (buffered) / ≈3.2 (mmap) is above the floor → exit 0."""
        exit_code = _run(_CRIT_SCALING_PASS)
        assert exit_code == 0, (
            "Expected zero exit when concurrent_scan scaling is healthy (≈3.0), "
            f"but got exit code {exit_code}."
        )

    def test_serialized_scaling_fails(self, capsys):
        """median(n4) ≈ 4·median(n1) → scaling ≈1.0 < 1.8 → non-zero exit."""
        exit_code = _run(_CRIT_SCALING_FAIL)
        assert exit_code != 0, (
            "Expected non-zero exit when concurrent_scan n4 median is ~4x the n1 "
            "median (re-serialized scan path, scaling ≈1.0), but got exit code 0."
        )

    def test_serialized_scaling_output_names_bench(self, capsys):
        """The failure output must name the floored scaling entry."""
        _run(_CRIT_SCALING_FAIL)
        captured = capsys.readouterr()
        assert "concurrent_scan/buffered/n4" in captured.out

    def test_missing_required_data_fails(self, capsys):
        """Missing data for a required scaling floor FAILS the gate (issue #1564).

        A scaling floor is intra-run: its data is always present on any run that
        benches the target, so a missing median means a typo'd id / omitted
        `--bench` / no-data bench — which must red, not silently disable the gate.
        Here the buffered n4 median is absent (required buffered floor → MISSING
        DATA → fail) while the mmap floor is present and healthy.
        """
        exit_code = _run(_CRIT_SCALING_SKIP)
        assert exit_code != 0, (
            "Expected non-zero exit when a required scaling floor's data is "
            f"missing (must not silently skip), but got exit code {exit_code}."
        )
        captured = capsys.readouterr()
        assert "concurrent_scan/buffered/n4" in captured.out
        assert "MISSING DATA" in captured.out

    def test_optional_floor_skips_when_absent(self, tmp_path, capsys):
        """A scaling floor marked ``optional: true`` SKIPs (exit 0) when absent.

        Uses a custom gate config: one present required floor (evaluated ok, so
        the "nothing evaluated" guard does not fire) plus one optional floor whose
        data is missing (→ SKIP, not a failure).
        """
        import json

        # Present, healthy required floor: scaling = 4 * 2.4M / 3.2M = 3.0.
        crit = tmp_path / "criterion"
        for name, ns in (("n1", 2_400_000.0), ("n4", 3_200_000.0)):
            d = crit / "concurrent_scan" / "buffered" / name / "pr"
            d.mkdir(parents=True)
            (d / "estimates.json").write_text(
                json.dumps({"median": {"point_estimate": ns}})
            )

        cfg = {
            "default_threshold_pct": 10,
            "advisory_benches": [],
            "benches": [],
            "scaling_floors": [
                {
                    "id": "concurrent_scan/buffered/n4",
                    "baseline_id": "concurrent_scan/buffered/n1",
                    "degree_ratio": 4,
                    "min_scaling": 1.8,
                },
                {
                    "id": "concurrent_scan/absent/n4",
                    "baseline_id": "concurrent_scan/absent/n1",
                    "degree_ratio": 4,
                    "min_scaling": 1.8,
                    "optional": True,
                },
            ],
        }
        cfg_path = tmp_path / "gate.json"
        cfg_path.write_text(json.dumps(cfg))

        exit_code = main(
            ["check_perf_regression.py", str(crit), "pr", "base", str(cfg_path)]
        )
        captured = capsys.readouterr()
        assert exit_code == 0, (
            "Expected zero exit: the required floor passes and the optional floor "
            f"skips, but got exit code {exit_code}.\n{captured.out}"
        )
        assert "SKIP (optional" in captured.out
