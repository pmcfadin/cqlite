"""Content-asserting write→read round-trips through the public Python API (Issue #1231).

Unlike ``test_write_api.py`` (which asserts only ``rows_affected`` / file-exists /
stat counters and never reopens), every test here drives the FULL public chain:

    db.execute("INSERT/UPDATE/DELETE")  →  db.flush_run()  →  real SSTable
        →  cqlite.open(<write_dir>/data)  (independent reopen)
        →  db.execute("SELECT ...")  →  assert decoded VALUES

A write-format/encoding regression that emits a structurally-present but
semantically-WRONG Data.db will turn these red — the shape-only tests could not
(the "CI blind to the write path" hazard, epic #1227).

These tests generate their own SSTables in a tmp dir, so they do NOT depend on
the fixture corpus.
"""

from pathlib import Path

import pytest

import cqlite

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def schema_file(tmp_path):
    """A single-table schema (no-heuristics mandate: one unambiguous target)."""
    text = """\
CREATE KEYSPACE IF NOT EXISTS write_test
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE write_test;

CREATE TABLE IF NOT EXISTS items (
    id    INT PRIMARY KEY,
    name  TEXT,
    value INT
);
"""
    path = tmp_path / "schema.cql"
    path.write_text(text)
    return path


def _open_writable(tmp_path, schema_file):
    """Open a writable database; writes land under ``write_dir``."""
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir(exist_ok=True)
    write_dir = tmp_path / "write_dir"
    db = cqlite.open(
        str(data_dir),
        schema=str(schema_file),
        writable=True,
        write_dir=str(write_dir),
    )
    return db, write_dir


def _read_back(write_dir, schema_file, query):
    """Reopen the flushed SSTable directory read-only and return row dicts."""
    with cqlite.open(str(Path(write_dir) / "data"), schema=str(schema_file)) as rd:
        return [row.to_dict() for row in rd.execute(query)]


def _row_with_id(rows, target):
    return next((r for r in rows if r.get("id") == target), None)


# ---------------------------------------------------------------------------
# INSERT: values survive the round-trip unchanged
# ---------------------------------------------------------------------------


def test_insert_flush_reopen_asserts_values(tmp_path, schema_file):
    db, write_dir = _open_writable(tmp_path, schema_file)
    try:
        db.execute(
            "INSERT INTO write_test.items (id, name, value) VALUES (1, 'alpha', 10)"
        )
        path = db.flush_run()
        assert path and Path(path).exists(), "flush must produce a real Data.db"
    finally:
        db.close()

    rows = _read_back(write_dir, schema_file, "SELECT * FROM write_test.items")
    assert len(rows) == 1, f"exactly one row expected, got {rows}"
    row = rows[0]
    assert row["id"] == 1, f"id value mismatch: {row}"
    assert row["name"] == "alpha", f"name value mismatch: {row}"
    assert row["value"] == 10, f"value value mismatch: {row}"


# ---------------------------------------------------------------------------
# UPDATE: a later write overwrites the earlier value (last-write-wins)
# ---------------------------------------------------------------------------


def test_update_overwrite_wins_on_readback(tmp_path, schema_file):
    db, write_dir = _open_writable(tmp_path, schema_file)
    try:
        db.execute(
            "INSERT INTO write_test.items (id, name, value) VALUES (1, 'alpha', 10)"
        )
        db.execute("UPDATE write_test.items SET name = 'ALPHA', value = 11 WHERE id = 1")
        db.flush_run()
    finally:
        db.close()

    rows = _read_back(write_dir, schema_file, "SELECT * FROM write_test.items")
    assert len(rows) == 1, f"exactly one row expected, got {rows}"
    row = rows[0]
    assert row["name"] == "ALPHA", f"UPDATE did not win for name: {row}"
    assert row["value"] == 11, f"UPDATE did not win for value: {row}"


# ---------------------------------------------------------------------------
# DELETE: the tombstone makes the row absent on read-back
# ---------------------------------------------------------------------------


def test_delete_tombstone_absent_on_readback(tmp_path, schema_file):
    db, write_dir = _open_writable(tmp_path, schema_file)
    try:
        db.execute(
            "INSERT INTO write_test.items (id, name, value) VALUES (1, 'alpha', 10)"
        )
        db.execute(
            "INSERT INTO write_test.items (id, name, value) VALUES (2, 'beta', 20)"
        )
        db.execute("DELETE FROM write_test.items WHERE id = 2")
        db.flush_run()
    finally:
        db.close()

    rows = _read_back(write_dir, schema_file, "SELECT * FROM write_test.items")
    assert _row_with_id(rows, 2) is None, f"deleted row id=2 must be absent: {rows}"
    survivor = _row_with_id(rows, 1)
    assert survivor is not None, f"surviving row id=1 must be present: {rows}"
    assert survivor["name"] == "alpha", f"survivor corrupted: {survivor}"
    assert survivor["value"] == 10, f"survivor corrupted: {survivor}"
