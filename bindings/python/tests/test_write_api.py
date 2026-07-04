"""Tests for the Python write API (Issue #390).

Covers:
- INSERT / UPDATE / DELETE via db.execute() on a writable Database
- flush_run() producing a real SSTable Data.db file
- maintenance_step() respecting time budget
- write_stats reflecting memtable growth and zeroing after flush
- Read-only mode raises RuntimeError on write operations
- writable=True validation (write_dir and schema required)
"""

import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

import cqlite

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def write_schema(tmp_path):
    """Write a minimal CQL schema to a temp file and return its path."""
    schema_text = """\
CREATE KEYSPACE IF NOT EXISTS write_test
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE write_test;

CREATE TABLE IF NOT EXISTS items (
    id    INT PRIMARY KEY,
    name  TEXT,
    value INT
);
"""
    schema_file = tmp_path / "write-schema.cql"
    schema_file.write_text(schema_text)
    return schema_file


@pytest.fixture()
def writable_db(tmp_path, write_schema):
    """Open a writable in-memory database backed by tmp_path.

    The data_dir is set to an (otherwise) empty directory so that reads
    succeed without any pre-existing SSTables; writes land in write_dir.
    """
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()
    write_dir = tmp_path / "write_dir"

    with cqlite.open(
        str(data_dir),
        schema=str(write_schema),
        writable=True,
        write_dir=str(write_dir),
    ) as db:
        yield db


# ---------------------------------------------------------------------------
# Argument validation tests (do not need actual SSTable data)
# ---------------------------------------------------------------------------


def test_open_writable_requires_write_dir(tmp_path, write_schema):
    """writable=True without write_dir raises ValueError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with pytest.raises(ValueError, match="write_dir is required"):
        cqlite.open(str(data_dir), schema=str(write_schema), writable=True)


def test_open_writable_requires_schema(tmp_path):
    """writable=True without schema raises ValueError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with pytest.raises(ValueError, match="schema is required"):
        cqlite.open(str(data_dir), writable=True, write_dir=str(tmp_path / "wd"))


def test_open_readonly_repr():
    """Read-only database repr contains 'open' but not 'writable'."""
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        with cqlite.open(td) as db:
            r = repr(db)
            assert "open" in r
            assert "writable" not in r


def test_open_writable_repr(writable_db):
    """Writable database repr contains 'writable'."""
    assert "writable" in repr(writable_db)


# ---------------------------------------------------------------------------
# Read-only mode raises RuntimeError on write methods
# ---------------------------------------------------------------------------


def test_readonly_execute_dml_raises(tmp_path):
    """Executing a DML statement on a read-only db raises RuntimeError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with cqlite.open(str(data_dir)) as db:
        with pytest.raises(RuntimeError, match="read-only"):
            db.execute("INSERT INTO t (id) VALUES (1)")


def test_readonly_flush_run_raises(tmp_path):
    """Calling flush_run() on a read-only db raises RuntimeError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with cqlite.open(str(data_dir)) as db:
        with pytest.raises(RuntimeError, match="read-only"):
            db.flush_run()


def test_readonly_maintenance_step_raises(tmp_path):
    """Calling maintenance_step() on a read-only db raises RuntimeError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with cqlite.open(str(data_dir)) as db:
        with pytest.raises(RuntimeError, match="read-only"):
            db.maintenance_step(budget_ms=100)


def test_readonly_write_stats_raises(tmp_path):
    """Accessing write_stats on a read-only db raises RuntimeError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with cqlite.open(str(data_dir)) as db:
        with pytest.raises(RuntimeError, match="read-only"):
            _ = db.write_stats


# ---------------------------------------------------------------------------
# INSERT → QueryResult
# ---------------------------------------------------------------------------


def test_insert_returns_query_result(writable_db):
    """INSERT returns a QueryResult with rows_affected=1 and empty rows."""
    result = writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (1, 'alpha', 10)"
    )
    assert isinstance(result, cqlite.QueryResult)
    assert result.rows_affected == 1
    assert len(result.rows) == 0
    assert result.execution_time_ms >= 0


def test_insert_multiple_rows(writable_db):
    """Multiple INSERTs each return rows_affected=1."""
    for i in range(5):
        result = writable_db.execute(
            f"INSERT INTO write_test.items (id, name, value) VALUES ({i}, 'item{i}', {i * 100})"
        )
        assert result.rows_affected == 1


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------


def test_update_returns_query_result(writable_db):
    """UPDATE returns a QueryResult with rows_affected=1."""
    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (10, 'original', 1)"
    )
    result = writable_db.execute(
        "UPDATE write_test.items SET name = 'updated' WHERE id = 10"
    )
    assert isinstance(result, cqlite.QueryResult)
    assert result.rows_affected == 1
    assert len(result.rows) == 0


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------


def test_delete_returns_query_result(writable_db):
    """DELETE returns a QueryResult with rows_affected=1."""
    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (20, 'tobedeleted', 99)"
    )
    result = writable_db.execute("DELETE FROM write_test.items WHERE id = 20")
    assert isinstance(result, cqlite.QueryResult)
    assert result.rows_affected == 1
    assert len(result.rows) == 0


# ---------------------------------------------------------------------------
# flush_run()
# ---------------------------------------------------------------------------


def test_flush_run_produces_sstable(writable_db, tmp_path):
    """flush_run() returns a non-empty string path to a Data.db file."""
    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (100, 'flush_test', 1)"
    )
    path_str = writable_db.flush_run()
    assert isinstance(path_str, str)
    assert len(path_str) > 0
    p = Path(path_str)
    assert p.exists(), f"Expected SSTable file to exist: {path_str}"
    assert p.name.endswith("Data.db"), f"Expected Data.db file, got: {p.name}"
    assert p.stat().st_size > 0, "Data.db file is empty"


def test_flush_run_empty_memtable(writable_db):
    """flush_run() on an empty memtable returns empty string."""
    path_str = writable_db.flush_run()
    assert path_str == ""


def test_flush_run_clears_memtable(writable_db):
    """After flush_run(), memtable_rows drops to 0."""
    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (200, 'x', 1)"
    )
    stats_before = writable_db.write_stats
    assert stats_before.memtable_rows > 0

    writable_db.flush_run()

    stats_after = writable_db.write_stats
    assert stats_after.memtable_rows == 0


# ---------------------------------------------------------------------------
# maintenance_step()
# ---------------------------------------------------------------------------


def test_maintenance_step_returns_report(writable_db):
    """maintenance_step() returns a MaintenanceReport."""
    report = writable_db.maintenance_step(budget_ms=100)
    assert isinstance(report, cqlite.MaintenanceReport)
    assert report.time_spent_ms >= 0
    assert report.rows_merged >= 0
    assert report.bytes_written >= 0
    assert isinstance(report.completed_merges, list)
    assert isinstance(report.pending_compaction, bool)


def test_maintenance_step_respects_budget(writable_db):
    """maintenance_step() completes within budget_ms * 1.2 (20% tolerance)."""
    budget_ms = 200
    t0 = time.monotonic()
    report = writable_db.maintenance_step(budget_ms=budget_ms)
    elapsed_ms = (time.monotonic() - t0) * 1000

    # maintenance_step guarantees at most 10% over budget internally.
    # Allow 20% in the Python test to account for scheduling variance.
    assert elapsed_ms <= budget_ms * 1.2 + 50, (
        f"maintenance_step took {elapsed_ms:.1f} ms, "
        f"expected <= {budget_ms * 1.2 + 50:.1f} ms"
    )
    # Report should also reflect reasonable timing
    assert report.time_spent_ms <= budget_ms * 1.2 + 50


def test_maintenance_step_no_pending_when_no_data(writable_db):
    """maintenance_step() reports no pending work when nothing has been flushed."""
    report = writable_db.maintenance_step(budget_ms=100)
    # No SSTables to compact → not pending
    assert not report.pending_compaction


def test_maintenance_step_repr(writable_db):
    """MaintenanceReport has a sensible repr."""
    report = writable_db.maintenance_step(budget_ms=50)
    r = repr(report)
    assert "MaintenanceReport" in r
    assert "time_spent_ms" in r


def test_maintenance_step_to_dict(writable_db):
    """MaintenanceReport.to_dict() contains expected keys."""
    report = writable_db.maintenance_step(budget_ms=50)
    d = report.to_dict()
    assert "time_spent_ms" in d
    assert "rows_merged" in d
    assert "bytes_written" in d
    assert "completed_merges" in d
    assert "pending_compaction" in d


# ---------------------------------------------------------------------------
# write_stats
# ---------------------------------------------------------------------------


def test_write_stats_initial(writable_db):
    """write_stats initially has zero memtable_rows."""
    stats = writable_db.write_stats
    assert isinstance(stats, cqlite.WriteStats)
    assert stats.memtable_size >= 0
    assert stats.memtable_rows == 0
    assert stats.wal_size >= 0
    assert stats.l0_count >= 0
    assert stats.total_written >= 0


def test_write_stats_grows_after_inserts(writable_db):
    """write_stats.memtable_rows increases after INSERTs."""
    stats_before = writable_db.write_stats
    rows_before = stats_before.memtable_rows

    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (300, 'stat_test', 1)"
    )
    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (301, 'stat_test2', 2)"
    )

    stats_after = writable_db.write_stats
    assert stats_after.memtable_rows > rows_before


def test_write_stats_resets_after_flush(writable_db):
    """write_stats.memtable_rows resets to 0 after flush_run()."""
    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (400, 'flush_stat', 5)"
    )
    assert writable_db.write_stats.memtable_rows > 0

    writable_db.flush_run()

    assert writable_db.write_stats.memtable_rows == 0


def test_write_stats_repr(writable_db):
    """WriteStats has a sensible repr."""
    stats = writable_db.write_stats
    r = repr(stats)
    assert "WriteStats" in r
    assert "memtable_size" in r


def test_write_stats_to_dict(writable_db):
    """WriteStats.to_dict() contains expected keys."""
    stats = writable_db.write_stats
    d = stats.to_dict()
    assert "memtable_size" in d
    assert "memtable_rows" in d
    assert "wal_size" in d
    assert "l0_count" in d
    assert "total_written" in d


# ---------------------------------------------------------------------------
# Context manager + close
# ---------------------------------------------------------------------------


def test_writable_db_context_manager(tmp_path, write_schema):
    """Writable database closes cleanly via context manager."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_dir = tmp_path / "wd"

    with cqlite.open(
        str(data_dir),
        schema=str(write_schema),
        writable=True,
        write_dir=str(write_dir),
    ) as db:
        db.execute(
            "INSERT INTO write_test.items (id, name, value) VALUES (999, 'ctx', 1)"
        )
        assert not db.is_closed

    assert db.is_closed


def test_close_flushes_memtable(tmp_path, write_schema):
    """Closing a writable database flushes remaining memtable data."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_dir = tmp_path / "wd"

    db = cqlite.open(
        str(data_dir),
        schema=str(write_schema),
        writable=True,
        write_dir=str(write_dir),
    )
    db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (500, 'close_flush', 7)"
    )
    db.close()

    # After close the engine flushed the memtable; at least one Data.db file
    # must exist under write_dir/data/.
    wd_data = write_dir / "data"
    assert db.is_closed
    assert any(wd_data.rglob("*-Data.db")), (
        f"Expected at least one flushed SSTable (*-Data.db) under {wd_data}, "
        f"but found: {list(wd_data.rglob('*'))}"
    )


# ---------------------------------------------------------------------------
# Issue #486 — l0_count and total_written non-placeholder behaviour
# ---------------------------------------------------------------------------


def test_l0_count_increments_after_flush(writable_db):
    """l0_count increases after flush_run(), proving it is not a hardcoded zero."""
    stats_before = writable_db.write_stats
    assert stats_before.l0_count == 0, "l0_count should start at 0"

    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (600, 'l0_test', 1)"
    )
    writable_db.flush_run()

    stats_after = writable_db.write_stats
    assert stats_after.l0_count == 1, (
        f"l0_count must be 1 after one flush, got {stats_after.l0_count}"
    )

    # A second insert + flush must push l0_count to 2
    writable_db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (601, 'l0_test2', 2)"
    )
    writable_db.flush_run()

    stats_final = writable_db.write_stats
    assert stats_final.l0_count == 2, (
        f"l0_count must be 2 after two flushes, got {stats_final.l0_count}"
    )


def test_total_written_survives_flush(writable_db):
    """total_written > memtable_rows after a flush — proves it is not a proxy."""
    # Write 3 rows
    for i in range(3):
        writable_db.execute(
            f"INSERT INTO write_test.items (id, name, value) VALUES ({700 + i}, 'tw{i}', {i})"
        )

    stats_before_flush = writable_db.write_stats
    assert stats_before_flush.total_written >= 3, (
        f"total_written should be >= 3 before flush, got {stats_before_flush.total_written}"
    )

    # Flush — memtable_rows drops to 0 but total_written must remain >= 3
    writable_db.flush_run()

    stats_after_flush = writable_db.write_stats
    assert stats_after_flush.memtable_rows == 0, "memtable_rows should be 0 after flush"
    assert stats_after_flush.total_written >= 3, (
        f"total_written must survive flush, got {stats_after_flush.total_written}"
    )
    assert stats_after_flush.total_written > stats_after_flush.memtable_rows, (
        "total_written must exceed memtable_rows after flush — "
        f"total_written={stats_after_flush.total_written}, "
        f"memtable_rows={stats_after_flush.memtable_rows}"
    )


def test_total_written_accumulates_across_flushes(writable_db):
    """total_written accumulates across multiple flush cycles."""
    # First batch: 2 rows + flush
    for i in range(2):
        writable_db.execute(
            f"INSERT INTO write_test.items (id, name, value) VALUES ({800 + i}, 'acc{i}', {i})"
        )
    writable_db.flush_run()
    stats_mid = writable_db.write_stats
    assert stats_mid.total_written >= 2

    # Second batch: 3 more rows + flush
    for i in range(3):
        writable_db.execute(
            f"INSERT INTO write_test.items (id, name, value) VALUES ({810 + i}, 'acc2_{i}', {i})"
        )
    writable_db.flush_run()
    stats_final = writable_db.write_stats
    assert stats_final.total_written >= 5, (
        f"total_written must accumulate across flushes, got {stats_final.total_written}"
    )
    assert stats_final.l0_count >= 2, (
        f"l0_count must be >= 2 after two flushes, got {stats_final.l0_count}"
    )


# ---------------------------------------------------------------------------
# Issue #1619 — auto_compaction off-switch via the public config surface
# ---------------------------------------------------------------------------
#
# The config bridge (`config_from_dict`) deserializes into the FULL
# `cqlite_core::Config`, which is not `#[serde(default)]`, so a partial dict
# like ``{"storage": {"compaction": {"auto_compaction": False}}}`` is rejected.
# The supported way to flip just this switch is to obtain a COMPLETE config
# dict from a preset (``cqlite.performance_optimized()`` returns a full
# round-tripped config dict), toggle
# ``["storage"]["compaction"]["auto_compaction"]``, then pass it to
# ``cqlite.open(config=...)``. These tests exercise that public path
# end-to-end (open -> write -> flush x4 -> maintenance_step).


def _full_config_dict(auto_compaction: bool) -> dict:
    """Return a COMPLETE config dict with the compaction switch flipped.

    Starts from the ``performance_optimized`` preset (a full config dict) so
    the config bridge accepts it, then sets the one field under test.
    """
    cfg = cqlite.performance_optimized()
    assert "storage" in cfg and "compaction" in cfg["storage"], (
        "preset config dict must expose storage.compaction"
    )
    cfg["storage"]["compaction"]["auto_compaction"] = auto_compaction
    return cfg


def _open_flush_n_maintain(tmp_path, write_schema, config, n=4):
    """Open writable with `config`, flush `n` distinct L0 SSTables, then run
    one maintenance_step with a generous budget.

    Returns ``(report, l0_flushes)`` where ``l0_flushes`` is the per-engine
    flush counter (``write_stats.l0_count``). Note that ``l0_count`` is a
    monotonic count of successful flushes — it is NOT decremented by a merge —
    so merge evidence comes from the MaintenanceReport (``rows_merged`` /
    ``completed_merges``), the authoritative STCS-ran signal.
    """
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()
    write_dir = tmp_path / "write_dir"

    with cqlite.open(
        str(data_dir),
        schema=str(write_schema),
        writable=True,
        write_dir=str(write_dir),
        config=config,
    ) as db:
        for i in range(n):
            # Distinct partition key per flush -> one L0 SSTable each.
            db.execute(
                f"INSERT INTO write_test.items (id, name, value) "
                f"VALUES ({1900 + i}, 'compact{i}', {i})"
            )
            db.flush_run()
        l0_flushes = db.write_stats.l0_count
        report = db.maintenance_step(budget_ms=60000)
        return report, l0_flushes


def test_compaction_off_switch_disables_merges(tmp_path, write_schema):
    """A full config with auto_compaction=False makes maintenance_step a no-op.

    Proves the off-switch is reachable through the public
    ``open(config=...)`` surface (issue #1619).
    """
    config = _full_config_dict(auto_compaction=False)
    report, l0_flushes = _open_flush_n_maintain(
        tmp_path, write_schema, config, n=4
    )
    assert l0_flushes == 4, f"expected 4 flushed SSTables, got {l0_flushes}"
    assert report.rows_merged == 0, "off-switch: no rows may be merged"
    assert not report.pending_compaction, "off-switch: no pending compaction"
    assert report.completed_merges == [], "off-switch: no merges completed"


def test_compaction_default_config_enables_merges(tmp_path, write_schema):
    """A full config with auto_compaction=True (default) merges >=4 SSTables.

    Companion to the off-switch test: proves the default STCS policy is
    genuinely active through the same public config surface.
    """
    config = _full_config_dict(auto_compaction=True)
    report, l0_flushes = _open_flush_n_maintain(
        tmp_path, write_schema, config, n=4
    )
    assert l0_flushes == 4, f"expected 4 flushed SSTables, got {l0_flushes}"
    assert report.rows_merged > 0, "default STCS policy must merge rows"
    assert len(report.completed_merges) > 0, "at least one merge must complete"


def test_compaction_partial_config_dict_rejected(tmp_path, write_schema):
    """A PARTIAL config dict is rejected (documents the full-config
    requirement). This guards against the doc ever again implying partial
    dicts work (issue #1619)."""
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()
    with pytest.raises(ValueError):
        cqlite.open(
            str(data_dir),
            schema=str(write_schema),
            writable=True,
            write_dir=str(tmp_path / "write_dir"),
            config={"storage": {"compaction": {"auto_compaction": False}}},
        )


# ---------------------------------------------------------------------------
# Issue #1444 — write path releases the GIL during blocking I/O
# ---------------------------------------------------------------------------
#
# These tests generate their own SSTables in a tmp dir, so they do NOT touch
# the fixture corpus (no dataset skip / _require_fixtures_strict needed). They
# use a self-calibrated free-run rate so the floor tolerates a heavily-loaded
# machine (this repo runs many concurrent gates) rather than a tight
# wall-clock number that would flake.


def _spin_counter():
    """Start a daemon thread spinning a pure-Python counter.

    Returns ``(counter, stop, thread)`` where ``counter[0]`` is the live count.
    A pure-Python loop can only advance while it holds the GIL, so its progress
    during another thread's C call is a direct probe of whether that call
    released the GIL.
    """
    counter = [0]
    stop = threading.Event()
    ready = threading.Event()

    def run():
        ready.set()
        c = 0
        while not stop.is_set():
            c += 1
            counter[0] = c

    t = threading.Thread(target=run, daemon=True)
    t.start()
    ready.wait()
    return counter, stop, t


@pytest.mark.slow
def test_flush_run_releases_gil(tmp_path, write_schema):
    """flush_run() releases the GIL for the duration of its blocking I/O.

    Thread B spins a pure-Python counter; the main thread (A) calls
    flush_run(). On ``main`` (pre-#1444) the write engine held the GIL for the
    whole flush, starving thread B; after the fix thread B advances at close to
    its free-run rate. We compare the spinner's *rate* during the flush to a
    self-calibrated free-run rate, which is duration- and load-independent.
    """
    # A short GIL switch interval shrinks the boundary "leakage" so the
    # held-GIL bug shows near-zero progress (makes the test discriminating).
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(0.001)

    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()
    write_dir = tmp_path / "write_dir"
    db = cqlite.open(
        str(data_dir),
        schema=str(write_schema),
        writable=True,
        write_dir=str(write_dir),
    )
    try:
        # Enough rows that the flush does real, measurable serialize + fsync I/O.
        for i in range(20000):
            db.execute(
                f"INSERT INTO write_test.items (id, name, value) "
                f"VALUES ({i}, 'gil_row_{i}', {i})"
            )

        counter, stop, t = _spin_counter()
        try:
            # Calibrate the spinner's free-run rate: the main thread sleeps,
            # which (like the fix) releases the GIL, so the spinner runs freely.
            c0 = counter[0]
            time.sleep(0.05)
            free_rate = (counter[0] - c0) / 0.05
            assert free_rate > 0, "spinner made no progress during calibration"

            # Measure the spinner's rate DURING the flush.
            before = counter[0]
            t_start = time.monotonic()
            path = db.flush_run()
            flush_secs = time.monotonic() - t_start
            during_rate = (counter[0] - before) / flush_secs if flush_secs else 0.0
        finally:
            stop.set()
            t.join(timeout=5)

        assert path and Path(path).exists(), "flush must produce a Data.db"
        # With the GIL released the spinner runs concurrently at a large fraction
        # of its free rate; with the GIL held it is starved (rate ~0). 15% cleanly
        # separates the two while tolerating a saturated machine.
        assert during_rate >= 0.15 * free_rate, (
            f"spinner starved during flush: {during_rate:.0f}/s vs free "
            f"{free_rate:.0f}/s over {flush_secs*1000:.1f} ms — GIL was likely "
            "held for the flush (issue #1444 regression)."
        )
    finally:
        db.close()
        sys.setswitchinterval(old_interval)


@pytest.mark.slow
def test_execute_dml_releases_gil(tmp_path, write_schema):
    """A DML batch that triggers auto-flushes keeps the GIL released.

    Companion to the flush test: DML routes through the same Send-guard
    ``allow_threads`` path (issue #1444). A small flush_threshold makes the
    inserts cross it repeatedly so ``execute()`` performs real flush I/O.
    """
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(0.001)

    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()
    write_dir = tmp_path / "write_dir"
    db = cqlite.open(
        str(data_dir),
        schema=str(write_schema),
        writable=True,
        write_dir=str(write_dir),
        flush_threshold=4096,
    )
    try:
        counter, stop, t = _spin_counter()
        try:
            c0 = counter[0]
            time.sleep(0.05)
            free_rate = (counter[0] - c0) / 0.05
            assert free_rate > 0, "spinner made no progress during calibration"

            before = counter[0]
            t_start = time.monotonic()
            for i in range(5000):
                db.execute(
                    f"INSERT INTO write_test.items (id, name, value) "
                    f"VALUES ({i}, 'dml_{i}', {i})"
                )
            secs = time.monotonic() - t_start
            during_rate = (counter[0] - before) / secs if secs else 0.0
        finally:
            stop.set()
            t.join(timeout=5)

        assert during_rate >= 0.10 * free_rate, (
            f"spinner starved during DML batch: {during_rate:.0f}/s vs free "
            f"{free_rate:.0f}/s — DML did not release the GIL (issue #1444)."
        )
    finally:
        db.close()
        sys.setswitchinterval(old_interval)


def test_flush_readback_after_send_handle_change(tmp_path, write_schema):
    """Single-writer correctness holds after the #1444 Send-handle change.

    Proves moving the write engine to an Arc<tokio::Mutex> and running the flush
    under allow_threads did not corrupt the write: the flushed Data.db is real,
    non-empty, and every inserted row reads back with its exact values.
    """
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()
    write_dir = tmp_path / "write_dir"
    db = cqlite.open(
        str(data_dir),
        schema=str(write_schema),
        writable=True,
        write_dir=str(write_dir),
    )
    try:
        for i in range(50):
            db.execute(
                f"INSERT INTO write_test.items (id, name, value) "
                f"VALUES ({i}, 'row_{i}', {i * 2})"
            )
        path = db.flush_run()
        assert path and Path(path).exists(), "flush must produce a Data.db"
        assert Path(path).stat().st_size > 0, "Data.db must be non-empty"
    finally:
        db.close()

    with cqlite.open(str(write_dir / "data"), schema=str(write_schema)) as rd:
        rows = [r.to_dict() for r in rd.execute("SELECT * FROM write_test.items")]

    assert len(rows) == 50, f"expected 50 rows on read-back, got {len(rows)}"
    by_id = {r["id"]: r for r in rows}
    for i in range(50):
        assert by_id[i]["name"] == f"row_{i}", f"name mismatch for id={i}: {by_id[i]}"
        assert by_id[i]["value"] == i * 2, f"value mismatch for id={i}: {by_id[i]}"
