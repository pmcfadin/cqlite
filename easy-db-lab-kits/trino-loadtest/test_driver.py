#!/usr/bin/env python3
"""Smoke tests for driver.py — plain python3, no pytest/trino dependency.

Run with: python3 test_driver.py

Exercises the percentile math, stats aggregation, interval-line formatting,
traceparent generation, query-file parsing, and the worker loop itself using a
fake connect/exec function — no real Trino connection, no real threading
timing (the fake exec_fn stops the run deterministically after N calls, so
there is nothing wall-clock-based to flake). driver.py's `trino` import is
lazy (see its module docstring), so importing it here requires nothing beyond
the standard library.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
import threading
import traceback
from typing import Callable, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import driver  # noqa: E402


_FAILURES: List[str] = []


def check(name: str, fn: Callable[[], None]) -> None:
    try:
        fn()
        print(f"ok   - {name}")
    except AssertionError as e:
        _FAILURES.append(name)
        print(f"FAIL - {name}: {e}")
    except Exception:  # noqa: BLE001 - report and keep going through the rest of the suite
        _FAILURES.append(name)
        print(f"FAIL - {name}: unexpected exception\n{traceback.format_exc()}")


def approx(a: float, b: float, tol: float = 1e-9) -> bool:
    return abs(a - b) <= tol


# --------------------------------------------------------------------------
# percentile()
# --------------------------------------------------------------------------


def test_percentile_empty() -> None:
    assert driver.percentile([], 50) == 0.0
    assert driver.percentile([], 99) == 0.0


def test_percentile_single_value() -> None:
    assert driver.percentile([42.0], 50) == 42.0
    assert driver.percentile([42.0], 99) == 42.0


def test_percentile_known_values() -> None:
    # 4-element sorted list; linear-interpolation percentile (numpy default).
    values = [10.0, 20.0, 30.0, 40.0]
    # p50: rank = 3*0.5 = 1.5 -> interpolate between index 1 (20) and 2 (30)
    assert approx(driver.percentile(values, 50), 25.0), driver.percentile(values, 50)
    # p99: rank = 3*0.99 = 2.97 -> interpolate between index 2 (30) and 3 (40)
    assert approx(driver.percentile(values, 99), 39.7, tol=1e-6), driver.percentile(values, 99)


def test_percentile_monotonic() -> None:
    values = sorted([float(i) for i in range(1, 101)])
    p50 = driver.percentile(values, 50)
    p99 = driver.percentile(values, 99)
    assert p50 <= p99
    assert 49.0 <= p50 <= 51.0
    assert 98.0 <= p99 <= 100.0


# --------------------------------------------------------------------------
# StatsCollector
# --------------------------------------------------------------------------


def test_stats_collector_accumulates_and_resets() -> None:
    stats = driver.StatsCollector()
    stats.record_success(10.0, 5)
    stats.record_success(20.0, 7)
    stats.record_error()

    snap = stats.snapshot_and_reset()
    assert snap.queries == 3
    assert snap.rows == 12
    assert snap.errors == 1
    assert sorted(snap.latencies_ms) == [10.0, 20.0]

    # snapshot_and_reset must clear internal state
    empty_snap = stats.snapshot_and_reset()
    assert empty_snap.queries == 0
    assert empty_snap.rows == 0
    assert empty_snap.errors == 0
    assert empty_snap.latencies_ms == []


def test_stats_collector_cumulative_snapshot_survives_interval_drains() -> None:
    """Pins issue N2: interval drains (``snapshot_and_reset``, what
    ``reporter_loop`` calls every ``--interval`` seconds) must not erase the
    run's cumulative totals — ``cumulative_snapshot`` keeps accumulating
    across them, including a still-open partial interval that never gets
    drained before the run ends.
    """
    stats = driver.StatsCollector()

    # Interval 1 (later drained by the reporter).
    stats.record_success(10.0, 5)
    stats.record_success(20.0, 7)
    stats.record_error()
    interval1 = stats.snapshot_and_reset()
    assert interval1.queries == 3
    assert interval1.rows == 12
    assert interval1.errors == 1

    # Interval 2 (also drained) — draining interval 1 must not have reset the
    # cumulative counters, and draining interval 2 must not reset them either.
    stats.record_success(5.0, 1)
    stats.record_error()
    interval2 = stats.snapshot_and_reset()
    assert interval2.queries == 2
    assert interval2.rows == 1
    assert interval2.errors == 1

    # Interval 3: a partial interval still open when the run "ends" — never drained.
    stats.record_success(30.0, 3)

    cumulative = stats.cumulative_snapshot()
    assert cumulative.queries == interval1.queries + interval2.queries + 1
    assert cumulative.rows == interval1.rows + interval2.rows + 3
    assert cumulative.errors == interval1.errors + interval2.errors
    assert sorted(cumulative.latencies_ms) == sorted(
        interval1.latencies_ms + interval2.latencies_ms + [30.0]
    )

    # cumulative_snapshot is read-only: calling it again must not lose totals.
    cumulative_again = stats.cumulative_snapshot()
    assert cumulative_again.queries == cumulative.queries
    assert cumulative_again.rows == cumulative.rows
    assert cumulative_again.errors == cumulative.errors


def test_format_final_line_reports_true_run_totals_not_last_interval() -> None:
    """Pins issue N2 end-to-end: the ``[ final ]`` line must sum every
    reporter interval for the whole run, not just whatever partial interval
    was still sitting in the collector when the run ended.
    """
    stats = driver.StatsCollector()

    # Two full reporter drains, as `reporter_loop` would perform periodically...
    stats.record_success(10.0, 100)
    stats.snapshot_and_reset()
    stats.record_success(20.0, 200)
    stats.record_error()
    stats.snapshot_and_reset()

    # ...then a third, partial interval the run ends inside of (never drained).
    stats.record_success(30.0, 50)

    final = stats.cumulative_snapshot()
    line = driver.format_final_line(4, final)

    # True run totals: 3 successes + 1 error = 4 queries; 100+200+50 = 350 rows; 1 error.
    # (The pre-fix bug would report only the undrained partial interval: 1 query, 50 rows, 0 errors.)
    assert "queries: 4" in line, line
    assert "rows: 350" in line, line
    assert "errors: 1" in line, line


def test_stats_collector_thread_safety() -> None:
    stats = driver.StatsCollector()

    def hammer() -> None:
        for _ in range(200):
            stats.record_success(1.0, 1)

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = stats.snapshot_and_reset()
    assert snap.queries == 1600, snap.queries
    assert snap.rows == 1600, snap.rows


# --------------------------------------------------------------------------
# format_interval_line / format_final_line
# --------------------------------------------------------------------------


def test_format_interval_line() -> None:
    snap = driver.IntervalStats(queries=100, rows=5000, errors=2, latencies_ms=[10.0, 20.0, 30.0, 40.0])
    line = driver.format_interval_line(30, 8, 10.0, snap)
    assert line.startswith("[ 30s ] threads: 8 ")
    assert "qps: 10.00" in line
    assert "rows_s: 500.00" in line
    assert "err_s: 0.20" in line
    assert re.match(r"^\[ [0-9]+s \]", line)


def test_format_final_line_does_not_match_interval_regex() -> None:
    snap = driver.IntervalStats(queries=10, rows=100, errors=0, latencies_ms=[1.0, 2.0])
    line = driver.format_final_line(4, snap)
    assert line.startswith("[ final ] threads: 4 queries: 10 rows: 100 ")
    assert not re.match(r"^\[ [0-9]+s \]", line)


# --------------------------------------------------------------------------
# random_traceparent()
# --------------------------------------------------------------------------


def test_random_traceparent_format() -> None:
    pattern = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-01$")
    seen = set()
    for _ in range(20):
        tp = driver.random_traceparent()
        assert pattern.match(tp), tp
        trace_id, span_id = tp.split("-")[1], tp.split("-")[2]
        assert trace_id != "0" * 32
        assert span_id != "0" * 16
        seen.add(tp)
    # 20 random 16-byte trace-ids colliding would be effectively impossible
    assert len(seen) == 20


# --------------------------------------------------------------------------
# query loading
# --------------------------------------------------------------------------


def test_default_queries_are_schema_agnostic() -> None:
    queries = driver.default_queries("test_basic", "simple_table")
    assert len(queries) == 3
    assert all("cqlite.test_basic.simple_table" in q for q in queries)
    assert any("count(*)" in q.lower() for q in queries)


def test_load_queries_from_file_skips_blanks_and_comments() -> None:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as fh:
        fh.write("# a comment\n")
        fh.write("\n")
        fh.write("SELECT * FROM cqlite.ks.tbl LIMIT 10\n")
        fh.write("   \n")
        fh.write("SELECT count(*) FROM cqlite.ks.tbl\n")
        path = fh.name
    try:
        queries = driver.load_queries(path, "ks", "tbl")
        assert queries == [
            "SELECT * FROM cqlite.ks.tbl LIMIT 10",
            "SELECT count(*) FROM cqlite.ks.tbl",
        ]
    finally:
        os.unlink(path)


def test_load_queries_rejects_empty_file() -> None:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as fh:
        fh.write("# only comments\n\n")
        path = fh.name
    try:
        try:
            driver.load_queries(path, "ks", "tbl")
            raise AssertionError("expected ValueError for an all-comment queries file")
        except ValueError:
            pass
    finally:
        os.unlink(path)


def test_load_queries_falls_back_to_default_when_no_path() -> None:
    assert driver.load_queries(None, "ks", "tbl") == driver.default_queries("ks", "tbl")
    assert driver.load_queries("", "ks", "tbl") == driver.default_queries("ks", "tbl")


# --------------------------------------------------------------------------
# argument parsing / validation
# --------------------------------------------------------------------------


def test_validate_args_requires_ks_tbl_without_queries_file() -> None:
    args = driver.parse_args(["--threads", "2"])
    assert driver.validate_args(args) is not None


def test_validate_args_passes_with_queries_file() -> None:
    args = driver.parse_args(["--queries-file", "/tmp/does-not-need-to-exist-yet.txt"])
    assert driver.validate_args(args) is None


def test_validate_args_passes_with_ks_and_tbl() -> None:
    args = driver.parse_args(["--ks", "test_basic", "--tbl", "simple_table"])
    assert driver.validate_args(args) is None


def test_validate_args_rejects_bad_numbers() -> None:
    args = driver.parse_args(["--ks", "a", "--tbl", "b", "--threads", "0"])
    assert driver.validate_args(args) is not None

    args = driver.parse_args(["--ks", "a", "--tbl", "b", "--interval", "0"])
    assert driver.validate_args(args) is not None


def test_parse_args_defaults() -> None:
    args = driver.parse_args(["--ks", "a", "--tbl", "b"])
    assert args.host == "localhost"
    assert args.port == 8080
    assert args.catalog == "cqlite"
    assert args.threads == 4
    assert args.duration == 60
    assert args.traceparent is False


class _env_var:  # noqa: N801 - small helper, lowercase-by-convention context manager
    """Set ``name=value`` in os.environ for the duration of a ``with`` block,
    restoring (or removing) whatever was there before on exit.

    ``value=None`` means "ensure unset" for the duration of the block.
    """

    def __init__(self, name: str, value: Optional[str]) -> None:
        self._name = name
        self._value = value
        self._old: Optional[str] = None

    def __enter__(self) -> "_env_var":
        self._old = os.environ.get(self._name)
        if self._value is None:
            os.environ.pop(self._name, None)
        else:
            os.environ[self._name] = self._value
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._old is None:
            os.environ.pop(self._name, None)
        else:
            os.environ[self._name] = self._old


def test_env_default_port_falls_back_on_k8s_service_injection() -> None:
    """Issue #2130: a K8s Service named "trino" injects
    ``TRINO_PORT=tcp://<ip>:8080`` into every pod's environment. Parser
    construction must never crash on that, and the driver must not read the
    bare (unnamespaced) ``TRINO_PORT`` at all — only the renamed
    ``TRINO_LOADTEST_PORT``.
    """
    with _env_var("TRINO_PORT", "tcp://10.43.73.27:8080"), _env_var("TRINO_LOADTEST_PORT", None):
        args = driver.parse_args(["--ks", "a", "--tbl", "b"])
        assert args.port == driver.DEFAULT_PORT


def test_env_default_port_falls_back_on_malformed_namespaced_var() -> None:
    """Simulates the easy-db-lab kit runner injecting a malformed value
    directly into the namespaced var — the defensive int parse must fall
    back to the default rather than raising at parser-construction time.
    """
    with _env_var("TRINO_LOADTEST_PORT", "tcp://10.0.0.1:8080"):
        args = driver.parse_args(["--ks", "a", "--tbl", "b"])
        assert args.port == driver.DEFAULT_PORT


def test_env_default_port_uses_namespaced_var_when_valid() -> None:
    with _env_var("TRINO_LOADTEST_PORT", "9999"):
        args = driver.parse_args(["--ks", "a", "--tbl", "b"])
        assert args.port == 9999


def test_explicit_port_flag_wins_over_malformed_env() -> None:
    """Even with a malformed TRINO_LOADTEST_PORT sitting in the env, an
    explicit --port flag (as start.sh always passes) must win.
    """
    with _env_var("TRINO_LOADTEST_PORT", "tcp://10.0.0.1:8080"):
        args = driver.parse_args(["--ks", "a", "--tbl", "b", "--port", "8080"])
        assert args.port == 8080


def test_int_env_default_helper_falls_back_on_bad_value() -> None:
    with _env_var("CQLITE_TEST_INT_VAR", "not-an-int"):
        assert driver._int_env_default("CQLITE_TEST_INT_VAR", 42) == 42


def test_int_env_default_helper_parses_good_value() -> None:
    with _env_var("CQLITE_TEST_INT_VAR", "7"):
        assert driver._int_env_default("CQLITE_TEST_INT_VAR", 42) == 7


def test_float_env_default_helper_falls_back_on_bad_value() -> None:
    with _env_var("CQLITE_TEST_FLOAT_VAR", "not-a-float"):
        assert driver._float_env_default("CQLITE_TEST_FLOAT_VAR", 5.0) == 5.0


# --------------------------------------------------------------------------
# run_worker() — the real concurrency logic, driven with a fake connection/exec
# --------------------------------------------------------------------------


def test_run_worker_records_success_and_error_deterministically() -> None:
    stop_event = threading.Event()
    stats = driver.StatsCollector()
    calls = {"n": 0}

    def fake_connect() -> object:
        return object()

    def fake_exec(_conn: object, sql: str, headers) -> int:  # noqa: ANN001 - test double
        calls["n"] += 1
        # Stop the loop deterministically after exactly 5 calls — no sleeps,
        # no wall-clock races (see project rule against timing-based test flakes).
        if calls["n"] >= 5:
            stop_event.set()
        if calls["n"] == 3:
            raise RuntimeError("simulated query failure")
        return 7

    driver.run_worker(
        connect_fn=fake_connect,
        exec_fn=fake_exec,
        queries=["SELECT 1"],
        stop_event=stop_event,
        stats=stats,
        traceparent_enabled=False,
    )

    snap = stats.snapshot_and_reset()
    assert calls["n"] == 5
    assert snap.queries == 5
    assert snap.errors == 1
    assert snap.rows == 7 * 4  # 4 successes * 7 rows each


def test_run_worker_closes_connection() -> None:
    stop_event = threading.Event()
    stats = driver.StatsCollector()
    closed = {"flag": False}

    class FakeConn:
        def close(self) -> None:
            closed["flag"] = True

    def fake_connect() -> object:
        return FakeConn()

    def fake_exec(_conn: object, sql: str, headers) -> int:  # noqa: ANN001 - test double
        stop_event.set()
        return 1

    driver.run_worker(
        connect_fn=fake_connect,
        exec_fn=fake_exec,
        queries=["SELECT 1"],
        stop_event=stop_event,
        stats=stats,
        traceparent_enabled=False,
    )
    assert closed["flag"] is True


def test_run_worker_generates_traceparent_header_when_enabled() -> None:
    stop_event = threading.Event()
    stats = driver.StatsCollector()
    seen_headers = []

    def fake_connect() -> object:
        return object()

    def fake_exec(_conn: object, sql: str, headers) -> int:  # noqa: ANN001 - test double
        seen_headers.append(headers)
        if len(seen_headers) >= 3:
            stop_event.set()
        return 0

    driver.run_worker(
        connect_fn=fake_connect,
        exec_fn=fake_exec,
        queries=["SELECT 1"],
        stop_event=stop_event,
        stats=stats,
        traceparent_enabled=True,
    )

    assert len(seen_headers) == 3
    for h in seen_headers:
        assert h is not None and "traceparent" in h
        assert re.match(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-01$", h["traceparent"])
    # per-query headers must vary (fresh trace-id each call), not be reused
    assert len({h["traceparent"] for h in seen_headers}) == 3


def test_run_worker_traceparent_disabled_passes_no_headers() -> None:
    stop_event = threading.Event()
    stats = driver.StatsCollector()
    seen_headers = []

    def fake_connect() -> object:
        return object()

    def fake_exec(_conn: object, sql: str, headers) -> int:  # noqa: ANN001 - test double
        seen_headers.append(headers)
        stop_event.set()
        return 0

    driver.run_worker(
        connect_fn=fake_connect,
        exec_fn=fake_exec,
        queries=["SELECT 1"],
        stop_event=stop_event,
        stats=stats,
        traceparent_enabled=False,
    )
    assert seen_headers == [None]


# --------------------------------------------------------------------------
# find_leaked_snapshots() / run_snapshot_leak_check() (D12 hygiene, #2399)
# --------------------------------------------------------------------------


def test_find_leaked_snapshots_extracts_matching_prefix() -> None:
    output = (
        "Snapshot Details: \n"
        "Snapshot name Keyspace name Column family name True size Size on disk\n"
        "cqlite-abc123 test_basic keyvalue 1.2 MiB 1.2 MiB\n"
        "cqlite-def456 test_basic keyvalue 800 KiB 800 KiB\n"
        "\n"
        "Total TrueDiskSpaceUsed: 2.0 MiB\n"
    )
    leaked = driver.find_leaked_snapshots(output)
    assert leaked == ["cqlite-abc123", "cqlite-def456"]


def test_find_leaked_snapshots_empty_when_none_match() -> None:
    output = (
        "Snapshot Details: \n"
        "Snapshot name Keyspace name Column family name True size Size on disk\n"
        "\n"
        "Total TrueDiskSpaceUsed: 0 bytes\n"
    )
    assert driver.find_leaked_snapshots(output) == []


def test_find_leaked_snapshots_ignores_non_matching_prefixes() -> None:
    output = "manual-backup-20260101 test_basic keyvalue 1.0 MiB 1.0 MiB\n"
    assert driver.find_leaked_snapshots(output) == []


def test_find_leaked_snapshots_honours_custom_prefix() -> None:
    output = "other-prefix-xyz ks tbl 1.0 MiB 1.0 MiB\ncqlite-abc ks tbl 1.0 MiB 1.0 MiB\n"
    assert driver.find_leaked_snapshots(output, prefix="other-prefix-") == ["other-prefix-xyz"]


def test_run_snapshot_leak_check_unconfigured_reports_not_ran() -> None:
    result = driver.run_snapshot_leak_check(None)
    assert result.ran is False
    assert result.leaked == []
    # An unrun check is NOT a pass — callers must never treat it as green (#2399).
    assert result.passed is False


def test_run_snapshot_leak_check_passes_when_clean() -> None:
    result = driver.run_snapshot_leak_check(lambda: "Snapshot Details: \nno rows\n")
    assert result.ran is True
    assert result.leaked == []
    assert result.passed is True


def test_run_snapshot_leak_check_fails_when_leaked() -> None:
    result = driver.run_snapshot_leak_check(lambda: "cqlite-abc123 ks tbl 1.0 MiB 1.0 MiB\n")
    assert result.ran is True
    assert result.leaked == ["cqlite-abc123"]
    assert result.passed is False


def test_snapshot_leak_check_wired_into_cli_flag() -> None:
    args = driver.parse_args(["--ks", "a", "--tbl", "b", "--snapshot-check-cmd", "echo hi"])
    assert args.snapshot_check_cmd == "echo hi"


def test_snapshot_leak_check_cli_flag_defaults_to_none() -> None:
    args = driver.parse_args(["--ks", "a", "--tbl", "b"])
    assert args.snapshot_check_cmd is None


def test_make_default_list_snapshots_fn_runs_command_and_captures_stdout() -> None:
    fn = driver.make_default_list_snapshots_fn("echo cqlite-leaked-one ks tbl 1.0MiB 1.0MiB")
    output = fn()
    assert "cqlite-leaked-one" in output
    leaked = driver.find_leaked_snapshots(output)
    assert leaked == ["cqlite-leaked-one"]


def test_make_default_list_snapshots_fn_raises_on_nonzero_exit() -> None:
    # A probe command that FAILED to run (auth failure, unreachable pod, typo'd
    # one-liner) must never be treated as "ran cleanly, found 0 snapshots" — that
    # empty-stdout-on-failure gap was a roborev finding on this change's first
    # review round (#2399). It must raise, not return "".
    fn = driver.make_default_list_snapshots_fn("echo 'kubectl: connection refused' >&2; exit 7")
    try:
        fn()
        raise AssertionError("expected RuntimeError on nonzero exit")
    except RuntimeError as e:
        assert "7" in str(e)
        assert "connection refused" in str(e)


def test_make_default_list_snapshots_fn_raises_with_placeholder_when_no_stderr() -> None:
    fn = driver.make_default_list_snapshots_fn("exit 1")
    try:
        fn()
        raise AssertionError("expected RuntimeError on nonzero exit")
    except RuntimeError as e:
        assert "(no stderr)" in str(e)


def test_run_snapshot_leak_check_propagates_probe_command_failure() -> None:
    # run_snapshot_leak_check itself must not swallow a failing list_snapshots_fn
    # into a false "ran cleanly, 0 leaked" result.
    def _failing() -> str:
        raise RuntimeError("snapshot-check-cmd exited 7: connection refused")

    try:
        driver.run_snapshot_leak_check(_failing)
        raise AssertionError("expected the probe failure to propagate")
    except RuntimeError as e:
        assert "connection refused" in str(e)


def main() -> int:
    check("percentile: empty input", test_percentile_empty)
    check("percentile: single value", test_percentile_single_value)
    check("percentile: known interpolated values", test_percentile_known_values)
    check("percentile: monotonic p50 <= p99", test_percentile_monotonic)
    check("StatsCollector: accumulates and resets", test_stats_collector_accumulates_and_resets)
    check(
        "StatsCollector: cumulative snapshot survives interval drains",
        test_stats_collector_cumulative_snapshot_survives_interval_drains,
    )
    check(
        "format_final_line: reports true run totals, not the last interval",
        test_format_final_line_reports_true_run_totals_not_last_interval,
    )
    check("StatsCollector: thread safety under concurrent writers", test_stats_collector_thread_safety)
    check("format_interval_line: fields and regex shape", test_format_interval_line)
    check("format_final_line: does not match interval regex", test_format_final_line_does_not_match_interval_regex)
    check("random_traceparent: W3C format + uniqueness", test_random_traceparent_format)
    check("default_queries: schema-agnostic scan+aggregate set", test_default_queries_are_schema_agnostic)
    check("load_queries: skips blanks/comments", test_load_queries_from_file_skips_blanks_and_comments)
    check("load_queries: rejects all-comment file", test_load_queries_rejects_empty_file)
    check("load_queries: falls back to default set", test_load_queries_falls_back_to_default_when_no_path)
    check("validate_args: requires ks/tbl without queries-file", test_validate_args_requires_ks_tbl_without_queries_file)
    check("validate_args: passes with queries-file", test_validate_args_passes_with_queries_file)
    check("validate_args: passes with ks and tbl", test_validate_args_passes_with_ks_and_tbl)
    check("validate_args: rejects bad numeric args", test_validate_args_rejects_bad_numbers)
    check("parse_args: defaults", test_parse_args_defaults)
    check(
        "parse_args: --port default ignores K8s-injected bare TRINO_PORT",
        test_env_default_port_falls_back_on_k8s_service_injection,
    )
    check(
        "parse_args: --port default falls back on malformed TRINO_LOADTEST_PORT",
        test_env_default_port_falls_back_on_malformed_namespaced_var,
    )
    check(
        "parse_args: --port default honours a valid TRINO_LOADTEST_PORT",
        test_env_default_port_uses_namespaced_var_when_valid,
    )
    check(
        "parse_args: explicit --port flag wins over malformed env",
        test_explicit_port_flag_wins_over_malformed_env,
    )
    check("_int_env_default: falls back on unparseable value", test_int_env_default_helper_falls_back_on_bad_value)
    check("_int_env_default: parses a valid value", test_int_env_default_helper_parses_good_value)
    check("_float_env_default: falls back on unparseable value", test_float_env_default_helper_falls_back_on_bad_value)
    check("run_worker: deterministic success/error accounting", test_run_worker_records_success_and_error_deterministically)
    check("run_worker: closes the connection on exit", test_run_worker_closes_connection)
    check("run_worker: traceparent header set and varies per query", test_run_worker_generates_traceparent_header_when_enabled)
    check("run_worker: no headers when traceparent disabled", test_run_worker_traceparent_disabled_passes_no_headers)
    check("find_leaked_snapshots: extracts matching prefix", test_find_leaked_snapshots_extracts_matching_prefix)
    check("find_leaked_snapshots: empty when none match", test_find_leaked_snapshots_empty_when_none_match)
    check(
        "find_leaked_snapshots: ignores non-matching prefixes",
        test_find_leaked_snapshots_ignores_non_matching_prefixes,
    )
    check("find_leaked_snapshots: honours custom prefix", test_find_leaked_snapshots_honours_custom_prefix)
    check(
        "run_snapshot_leak_check: unconfigured reports not-ran (never a silent pass)",
        test_run_snapshot_leak_check_unconfigured_reports_not_ran,
    )
    check("run_snapshot_leak_check: passes when clean", test_run_snapshot_leak_check_passes_when_clean)
    check("run_snapshot_leak_check: fails when leaked", test_run_snapshot_leak_check_fails_when_leaked)
    check("--snapshot-check-cmd: wired into parsed args", test_snapshot_leak_check_wired_into_cli_flag)
    check("--snapshot-check-cmd: defaults to None", test_snapshot_leak_check_cli_flag_defaults_to_none)
    check(
        "make_default_list_snapshots_fn: runs command and captures stdout",
        test_make_default_list_snapshots_fn_runs_command_and_captures_stdout,
    )
    check(
        "make_default_list_snapshots_fn: raises (never a silent pass) on nonzero exit",
        test_make_default_list_snapshots_fn_raises_on_nonzero_exit,
    )
    check(
        "make_default_list_snapshots_fn: raises with a placeholder when stderr is empty",
        test_make_default_list_snapshots_fn_raises_with_placeholder_when_no_stderr,
    )
    check(
        "run_snapshot_leak_check: propagates a probe command failure (never swallows it)",
        test_run_snapshot_leak_check_propagates_probe_command_failure,
    )

    print()
    if _FAILURES:
        print(f"{len(_FAILURES)} FAILED: {', '.join(_FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
