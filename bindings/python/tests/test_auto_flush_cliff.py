"""Auto-flush cliff wiring evidence (Issue #1620, N2).

The Python binding routes DML through the write engine inside a Tokio runtime
(``block_on``), where the engine's sync auto-flush is intentionally skipped. On
main that meant the memtable grew unbounded and NO SSTable was written until an
explicit ``flush_run()``. This test proves the fix end-to-end: with a tiny
``flush_threshold`` open option, a loop of ``db.execute(...)`` inserts triggers a
REAL async flush on its own — with NO explicit ``flush_run`` — so on-disk
``*-Data.db`` generation files appear.

Named-public-surface → call-chain → e2e evidence:
    flush_threshold open kwarg  →  Database.execute (DML)
        →  PyWriteEngine.execute → WriteEngine::execute_flushing (async flush)
        →  *-Data.db on disk

On main this test is red: 0 Data.db files (auto-flush never fires).

Generates its own SSTables in a tmp dir; no fixture-corpus dependency.
"""

from pathlib import Path

import pytest

import cqlite

SCHEMA_TEXT = """\
CREATE KEYSPACE IF NOT EXISTS flush_test
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flush_test;

CREATE TABLE IF NOT EXISTS items (
    id    INT PRIMARY KEY,
    name  TEXT,
    value INT
);
"""


@pytest.fixture()
def schema_file(tmp_path):
    path = tmp_path / "schema.cql"
    path.write_text(SCHEMA_TEXT)
    return path


def _count_data_db(write_dir):
    # Flushed SSTables land under data/<keyspace>/<table>/nb-*-big-Data.db, so
    # recurse rather than globbing the top level (issue #1620).
    data_path = Path(write_dir) / "data"
    if not data_path.exists():
        return 0
    return len(list(data_path.rglob("*-Data.db")))


def test_tiny_flush_threshold_auto_flushes_during_execute(tmp_path, schema_file):
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir(exist_ok=True)
    write_dir = tmp_path / "write_dir"

    db = cqlite.open(
        str(data_dir),
        schema=str(schema_file),
        writable=True,
        write_dir=str(write_dir),
        flush_threshold=4096,  # 4 KB — crossed after a handful of inserts
    )
    try:
        total = 2000
        for i in range(total):
            result = db.execute(
                f"INSERT INTO flush_test.items (id, name, value) "
                f"VALUES ({i}, 'user{i}', {i})"
            )
            assert result.rows_affected == 1

        # A real auto-flush must have fired: on-disk generation files exist.
        # On main this is 0 because the runtime-present sync path never flushes.
        assert _count_data_db(write_dir) >= 1

        # The memtable was cleared by the flush(es), so its residual row count is
        # far below the total inserted.
        assert db.write_stats.memtable_rows < total
    finally:
        db.close()


def test_flush_threshold_zero_rejected(tmp_path, schema_file):
    # 0 would make should_flush(0) true after every write (flush-per-write).
    with pytest.raises(ValueError):
        cqlite.open(
            str(tmp_path / "data_dir"),
            schema=str(schema_file),
            writable=True,
            write_dir=str(tmp_path / "write_dir"),
            flush_threshold=0,
        )


def test_flush_threshold_above_hard_limit_rejected(tmp_path, schema_file):
    # A threshold above the 256 MB memtable hard limit would never trigger an
    # auto-flush (writes dead-end at the hard limit first) — issue #1620.
    with pytest.raises(ValueError):
        cqlite.open(
            str(tmp_path / "data_dir"),
            schema=str(schema_file),
            writable=True,
            write_dir=str(tmp_path / "write_dir"),
            flush_threshold=300 * 1024 * 1024,  # 300 MB > 256 MB hard limit
        )
