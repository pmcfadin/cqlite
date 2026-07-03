"""End-to-end tests for ``Database.refresh()`` (issue #1749).

These drive the full stale -> refresh -> fresh cycle through the Python public
API (`cqlite.open`, `db.execute`, `db.refresh`) against REAL SSTable binaries.

The fixtures are built in-test with CQLite's own write path (the natural Python
mirror of the ``WriteEngine`` fixture construction in
``cqlite-core/tests/issue_1749_sstable_freshness_refresh.rs``): two single-flush
generations, ``nb-1-big-*`` holding only partition ``id=1`` and ``nb-2-big-*``
holding only ``id=2``. Because the generations are generated here rather than
fetched, there is **no skip path** — a write-path failure fails the test, every
row-set assertion is exact (never ``>= 0``), and each copy asserts it moved at
least one component file. A 0-rows-on-present-data regression therefore fails
loudly instead of silently passing.
"""

import os
import shutil
import tempfile
from pathlib import Path

import cqlite

KEYSPACE = "test_freshness"
TABLE = "users"

_SCHEMA = f"CREATE TABLE {KEYSPACE}.{TABLE} (id int PRIMARY KEY, value text);"


def _write_schema(root: Path) -> Path:
    """Write the single-table schema used by the write engine and readers."""
    schema_path = root / "users.cql"
    schema_path.write_text(_SCHEMA)
    return schema_path


def _build_two_generations(root: Path, schema: Path) -> Path:
    """Build a source table dir with two SSTable generations.

    ``nb-1-big-*`` contains only partition ``id=1`` and ``nb-2-big-*`` only
    ``id=2`` — each ``flush_run()`` of a single writable database advances the
    generation. Returns the ``.../<keyspace>/<table>`` directory containing both
    generations.
    """
    # The read side of a writable open still requires an existing data dir.
    read_dir = root / "src_read"
    read_dir.mkdir()
    write_dir = root / "src_write"

    with cqlite.open(
        read_dir, schema=schema, writable=True, write_dir=write_dir
    ) as db:
        for gen_id in (1, 2):
            db.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} (id, value) "
                f"VALUES ({gen_id}, 'v{gen_id}')"
            )
            path = db.flush_run()
            assert path, f"flush for id={gen_id} produced no SSTable"

    table_dir = write_dir / "data" / KEYSPACE / TABLE
    assert (table_dir / "nb-1-big-Data.db").exists(), "gen-1 must exist in source"
    assert (table_dir / "nb-2-big-Data.db").exists(), "gen-2 must exist in source"
    return table_dir


def _copy_generation(src_table_dir: Path, dst_table_dir: Path, gen: int) -> int:
    """Copy every ``nb-<gen>-big-*`` component into ``dst_table_dir``.

    Returns the number of component files copied (asserted > 0 so a build/path
    regression fails rather than silently copying nothing).
    """
    dst_table_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"nb-{gen}-big-"
    copied = 0
    for name in os.listdir(src_table_dir):
        if name.startswith(prefix):
            shutil.copy(src_table_dir / name, dst_table_dir / name)
            copied += 1
    assert copied > 0, f"expected to copy generation {gen} components"
    return copied


def _delete_generation(table_dir: Path, gen: int) -> int:
    """Delete every ``nb-<gen>-big-*`` component (simulated compaction)."""
    prefix = f"nb-{gen}-big-"
    removed = 0
    for name in os.listdir(table_dir):
        if name.startswith(prefix):
            (table_dir / name).unlink()
            removed += 1
    assert removed > 0, f"expected to remove generation {gen} components"
    return removed


def _select_all_ids(db) -> set:
    """The set of ``id`` partition-key values in ``SELECT *``."""
    res = db.execute(f"SELECT * FROM {KEYSPACE}.{TABLE}")
    return {row.to_dict()["id"] for row in res}


def test_added_generation_invisible_until_refresh_then_visible():
    """Spec: new generation invisible until refresh, visible after;
    report ``readers_added == 1`` and ``readers_removed == 0``."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        schema = _write_schema(root)
        src_table_dir = _build_two_generations(root, schema)

        # Live directory starts with ONLY generation 1 (partition id=1).
        live = root / "live"
        live_table_dir = live / KEYSPACE / TABLE
        _copy_generation(src_table_dir, live_table_dir, 1)

        with cqlite.open(live, schema=schema) as db:
            before = _select_all_ids(db)
            assert before == {1}, "only gen-1 partition visible at open"

            # Copy in generation 2 (partition id=2) but do NOT refresh yet.
            _copy_generation(src_table_dir, live_table_dir, 2)
            assert _select_all_ids(db) == {1}, (
                "stale-until-refresh: same result before refresh() "
                "despite new file on disk"
            )

            report = db.refresh()
            assert report.readers_added == 1, "one generation added"
            assert report.readers_removed == 0, "none removed"
            assert report.tables_scanned >= 1, "at least the users table scanned"

            assert _select_all_ids(db) == {1, 2}, (
                "new generation's partition visible after refresh"
            )


def test_removed_generation_dropped_on_refresh():
    """Spec: removed generation dropped safely — ``readers_removed == 1`` and
    the subsequent SELECT returns only the remaining generation."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        schema = _write_schema(root)
        src_table_dir = _build_two_generations(root, schema)

        live = root / "live"
        live_table_dir = live / KEYSPACE / TABLE
        _copy_generation(src_table_dir, live_table_dir, 1)
        _copy_generation(src_table_dir, live_table_dir, 2)

        with cqlite.open(live, schema=schema) as db:
            assert _select_all_ids(db) == {1, 2}, "both partitions visible at open"

            _delete_generation(live_table_dir, 2)
            report = db.refresh()
            assert report.readers_removed == 1, "one generation removed"
            assert report.readers_added == 0, "none added"

            assert _select_all_ids(db) == {1}, (
                "only the remaining generation's partition after removal"
            )


def test_unchanged_directory_is_zero_delta_noop():
    """Spec: unchanged directory is a cheap no-op — zero-delta report and an
    unchanged result set."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        schema = _write_schema(root)
        src_table_dir = _build_two_generations(root, schema)

        live = root / "live"
        live_table_dir = live / KEYSPACE / TABLE
        _copy_generation(src_table_dir, live_table_dir, 1)

        with cqlite.open(live, schema=schema) as db:
            before = _select_all_ids(db)

            report = db.refresh()
            assert report.readers_added == 0, "no-op: nothing added"
            assert report.readers_removed == 0, "no-op: nothing removed"

            assert _select_all_ids(db) == before, "result unchanged by no-op"


def test_refresh_report_repr_and_to_dict():
    """The ``RefreshReport`` exposes readable ``__repr__`` and ``to_dict()``."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        schema = _write_schema(root)
        src_table_dir = _build_two_generations(root, schema)

        live = root / "live"
        live_table_dir = live / KEYSPACE / TABLE
        _copy_generation(src_table_dir, live_table_dir, 1)

        with cqlite.open(live, schema=schema) as db:
            _copy_generation(src_table_dir, live_table_dir, 2)
            report = db.refresh()

            text = repr(report)
            assert "RefreshReport" in text
            assert "readers_added=1" in text

            as_dict = report.to_dict()
            assert as_dict == {
                "tables_scanned": report.tables_scanned,
                "readers_added": 1,
                "readers_removed": 0,
            }
