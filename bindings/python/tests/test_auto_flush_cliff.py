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


# ── Issue #1697 (roborev round 1): the ceiling is the CALLER's, not the default ──
#
# The `flush_threshold` ceiling check used to compare against
# `Config::default().storage.memtable_hard_limit` — a hardcoded 256 MB — because
# it ran BEFORE `config=` was parsed AND before the threshold was folded onto the
# public config. Once #1697 made `memtable_hard_limit` a public, settable knob,
# reading its DEFAULT stopped being equivalent to reading its VALUE, so the check
# accepted and rejected the wrong configs in both directions.


def _preset_with_limits(hard_limit: int, threshold: int) -> dict:
    """A COMPLETE config dict (required by the serde bridge) with both memtable
    knobs set.

    Both are set together so the dict is INTERNALLY VALID: ``Config::validate``
    rejects ``hard_limit < memtable_size_threshold`` outright (a wedged engine),
    and that rejection would mask the ``flush_threshold`` ceiling check these
    tests are actually about.
    """
    cfg = cqlite.performance_optimized()
    assert "memtable_hard_limit" in cfg["storage"], (
        "issue #1697 exposed memtable_hard_limit on the public storage config"
    )
    cfg["storage"]["memtable_hard_limit"] = hard_limit
    cfg["storage"]["memtable_size_threshold"] = threshold
    return cfg


def test_flush_threshold_above_callers_low_hard_limit_rejected(tmp_path, schema_file):
    """A threshold under the 256 MB DEFAULT but over the CALLER's ceiling must raise.

    This is the documented happy path (take a preset dict, mutate it), and the
    wedge it produces is permanent: auto-flush never fires at 200 MB while
    admission rejects every write at 64 MB.
    """
    cfg = _preset_with_limits(64 * 1024 * 1024, 16 * 1024 * 1024)
    with pytest.raises(ValueError, match="hard limit") as excinfo:
        cqlite.open(
            str(tmp_path / "data_dir"),
            schema=str(schema_file),
            writable=True,
            write_dir=str(tmp_path / "write_dir"),
            config=cfg,
            flush_threshold=200 * 1024 * 1024,  # < 256MB default, > 64MB caller limit
        )
    # The message must quote the CALLER's ceiling and the offending threshold —
    # not the 256 MB default, which would misdirect the operator entirely.
    message = str(excinfo.value)
    assert str(64 * 1024 * 1024) in message, message
    assert str(200 * 1024 * 1024) in message, message


def test_flush_threshold_above_default_but_under_callers_high_hard_limit_accepted(
    tmp_path, schema_file
):
    """A threshold ABOVE the 256 MB default but under the CALLER's ceiling is valid.

    This is the half that actually catches the bug: with the old check the 512 MB
    threshold was compared against the 256 MB default and wrongly REJECTED, even
    though the caller had raised the ceiling to 1 GB.
    """
    cfg = _preset_with_limits(1024 * 1024 * 1024, 128 * 1024 * 1024)
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir(exist_ok=True)
    db = cqlite.open(
        str(data_dir),
        schema=str(schema_file),
        writable=True,
        write_dir=str(tmp_path / "write_dir"),
        config=cfg,
        flush_threshold=512 * 1024 * 1024,  # > 256MB default, < 1GB caller limit
    )
    try:
        # And the accepted threshold really reached the engine: at 512 MB a
        # handful of tiny inserts must NOT cross the flush cliff, so nothing is
        # on disk yet. This is the fold's wiring evidence, not just acceptance.
        for i in range(20):
            assert (
                db.execute(
                    f"INSERT INTO flush_test.items (id, name, value) "
                    f"VALUES ({i}, 'user{i}', {i})"
                ).rows_affected
                == 1
            )
        assert _count_data_db(tmp_path / "write_dir") == 0
    finally:
        db.close()
