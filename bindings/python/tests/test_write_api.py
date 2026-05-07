"""Tests for the Python write API (Issue #390).

Covers:
- INSERT / UPDATE / DELETE via db.execute() on a writable Database
- flush_run() producing a real SSTable Data.db file
- maintenance_step() respecting time budget
- write_stats reflecting memtable growth and zeroing after flush
- Read-only mode raises RuntimeError on write operations
- writable=True validation (write_dir and schema required)
"""

import tempfile
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
