"""Tests for scripts/ci/check_tail_latency.py (Issue #1563).

Validates the advisory-vs-enforcing exit-code model of the tail-latency ratio
gate. Mirrors test_check_perf_regression.py: import the script as a module and
drive main() with temp harness/gate JSON fixtures.

Matrix:
  A) Advisory breach          → exit 0 (breach reported, gate does not fail)
  B) Enforce flag + breach     → exit 1
  C) advisory:false + breach   → exit 1
  D) Within threshold (either mode) → exit 0
"""

import importlib.util
import json
import os

# ---------------------------------------------------------------------------
# Import the script as a module (it is not a package, has no __init__)
# ---------------------------------------------------------------------------
_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "check_tail_latency.py")
_spec = importlib.util.spec_from_file_location("check_tail_latency", _SCRIPT)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
main = _mod.main


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _write(tmp_path, name, obj):
    path = tmp_path / name
    path.write_text(json.dumps(obj))
    return str(path)


def _harness(p99_over_p50=2.0, p99_mixed_over_scan_free=3.0):
    return {
        "mixed": {"p50": 100, "p99": 300, "p999": 500},
        "scan_free": {"p50": 90, "p99": 100, "p999": 150},
        "p99_over_p50": p99_over_p50,
        "p99_mixed_over_scan_free": p99_mixed_over_scan_free,
    }


def _gate(advisory=True, p50_max=40.0, cross_max=12.0):
    return {
        "advisory": advisory,
        "ratios": {
            "p99_over_p50": {"max": p50_max},
            "p99_mixed_over_scan_free": {"max": cross_max},
        },
    }


def _run(tmp_path, harness, gate, *flags):
    h = _write(tmp_path, "harness.json", harness)
    g = _write(tmp_path, "gate.json", gate)
    return main(["check_tail_latency.py", h, g, *flags])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestAdvisoryBreachReportsButPasses:
    def test_advisory_breach_exits_zero(self, tmp_path):
        rc = _run(
            tmp_path,
            _harness(p99_mixed_over_scan_free=99.0),
            _gate(advisory=True, cross_max=12.0),
        )
        assert rc == 0, "advisory breach must exit 0 (reported, not failing)"

    def test_advisory_breach_is_reported(self, tmp_path, capsys):
        _run(
            tmp_path,
            _harness(p99_mixed_over_scan_free=99.0),
            _gate(advisory=True, cross_max=12.0),
        )
        out = capsys.readouterr().out
        assert "p99_mixed_over_scan_free" in out
        assert "advisory" in out.lower()


class TestEnforcingBreachFails:
    def test_enforce_flag_breach_exits_nonzero(self, tmp_path):
        rc = _run(
            tmp_path,
            _harness(p99_mixed_over_scan_free=99.0),
            _gate(advisory=True, cross_max=12.0),
            "--enforce",
        )
        assert rc != 0, "enforce flag on a breach must exit non-zero"

    def test_advisory_false_breach_exits_nonzero(self, tmp_path):
        rc = _run(
            tmp_path,
            _harness(p99_mixed_over_scan_free=99.0),
            _gate(advisory=False, cross_max=12.0),
        )
        assert rc != 0, "advisory:false on a breach must exit non-zero"


class TestMissingRatioFailsClosedWhenEnforcing:
    def test_enforcing_missing_required_ratio_exits_nonzero(self, tmp_path):
        # A thresholded ratio absent from the harness JSON (stale/malformed output)
        # must NOT silently pass the enforcing gate.
        harness = _harness()
        del harness["p99_mixed_over_scan_free"]
        rc = _run(tmp_path, harness, _gate(advisory=False))
        assert rc != 0, "enforcing gate must fail when a required ratio is missing"

    def test_advisory_missing_ratio_still_exits_zero(self, tmp_path):
        # Advisory mode reports a missing ratio as SKIP and still passes.
        harness = _harness()
        del harness["p99_mixed_over_scan_free"]
        rc = _run(tmp_path, harness, _gate(advisory=True))
        assert rc == 0, "advisory gate skips a missing ratio (reported, not failing)"


class TestWithinThresholdPasses:
    def test_within_threshold_advisory_exits_zero(self, tmp_path):
        rc = _run(tmp_path, _harness(), _gate(advisory=True))
        assert rc == 0

    def test_within_threshold_enforcing_exits_zero(self, tmp_path):
        rc = _run(tmp_path, _harness(), _gate(advisory=False))
        assert rc == 0

    def test_within_threshold_success_message(self, tmp_path, capsys):
        _run(tmp_path, _harness(), _gate(advisory=False))
        out = capsys.readouterr().out
        assert "✅" in out or "within threshold" in out.lower()
