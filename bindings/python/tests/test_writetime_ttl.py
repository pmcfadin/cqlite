"""Tests for WRITETIME()/TTL() output in Python bindings (Issue #693).

Verifies that:
- WRITETIME(col) and TTL(col) expressions execute end-to-end through the
  Python bindings.
- Column names containing parentheses are preserved in row dicts and column
  metadata.
- WRITETIME returns a non-null integer (BigInt) for stored columns.
- TTL returns None when no TTL is set.
- export_parquet correctly round-trips WRITETIME/TTL columns, including the
  parenthesised field names (the streaming path must materialise via the full
  executor so the per-cell metadata is available).
"""

import pytest

import cqlite

from conftest import (
    SCHEMA_BASIC_TYPES,
    require_test_data,
)

try:
    import pyarrow.parquet as pq

    HAVE_PYARROW = True
except ImportError:
    HAVE_PYARROW = False


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def db():
    require_test_data(SCHEMA_BASIC_TYPES)
    import os
    from pathlib import Path

    root = os.environ.get("CQLITE_DATASETS_ROOT")
    if root:
        p = Path(root)
        datasets = p if p.name == "sstables" else p / "sstables"
    else:
        datasets = Path(__file__).resolve().parents[3] / "test-data" / "datasets" / "sstables"

    if not datasets.exists():
        pytest.skip(f"Test data not found: {datasets}")

    with cqlite.open(datasets, schema=SCHEMA_BASIC_TYPES) as database:
        yield database


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

QUERY = "SELECT id, WRITETIME(name), TTL(name) FROM test_basic.simple_table LIMIT 5"


class TestWritetimeTtlExecution:
    """WRITETIME()/TTL() end-to-end execution through the Python bindings."""

    def test_writetime_ttl_query_returns_rows(self, db):
        """Query with WRITETIME/TTL must return at least one row."""
        result = db.execute(QUERY)
        assert len(result.rows) > 0, "Expected rows from WRITETIME/TTL query"

    def test_column_names_contain_parentheses(self, db):
        """Column names must be 'writetime(name)' and 'ttl(name)'."""
        result = db.execute(QUERY)
        col_names = [c.name for c in result.columns]
        assert "writetime(name)" in col_names, (
            f"Expected 'writetime(name)' in columns, got {col_names}"
        )
        assert "ttl(name)" in col_names, (
            f"Expected 'ttl(name)' in columns, got {col_names}"
        )

    def test_writetime_column_accessible_in_row_dict(self, db):
        """Row.to_dict() must expose 'writetime(name)' key."""
        result = db.execute(QUERY)
        assert len(result.rows) > 0, "No rows returned"
        row_dict = result.rows[0].to_dict()
        assert "writetime(name)" in row_dict, (
            f"'writetime(name)' key missing from row dict; keys={list(row_dict.keys())}"
        )

    def test_writetime_is_non_null_integer(self, db):
        """WRITETIME(name) must return a non-null integer for stored rows."""
        result = db.execute(QUERY)
        found_non_null = False
        for row in result.rows:
            wt = row.to_dict().get("writetime(name)")
            if wt is not None:
                assert isinstance(wt, int), (
                    f"writetime(name) should be int, got {type(wt).__name__}: {wt!r}"
                )
                assert wt > 0, f"writetime(name) should be positive micros, got {wt}"
                found_non_null = True
                break
        assert found_non_null, "All writetime(name) values were None"

    def test_ttl_is_none_when_no_ttl_set(self, db):
        """TTL(name) must return None when no TTL was set at write time."""
        result = db.execute(QUERY)
        assert len(result.rows) > 0, "No rows returned"
        # simple_table rows have no TTL; all TTL values should be None.
        for row in result.rows:
            ttl = row.to_dict().get("ttl(name)")
            assert ttl is None, (
                f"Expected ttl(name) = None for rows without TTL, got {ttl!r}"
            )

    def test_writetime_column_data_type_is_bigint(self, db):
        """Metadata must report writetime(name) as BigInt."""
        result = db.execute(QUERY)
        for col in result.columns:
            if col.name == "writetime(name)":
                assert "BigInt" in col.data_type or "bigint" in col.data_type.lower(), (
                    f"writetime column data_type should be BigInt, got {col.data_type!r}"
                )
                return
        pytest.fail("writetime(name) column not found in metadata")

    def test_ttl_column_data_type_is_integer(self, db):
        """Metadata must report ttl(name) as Integer."""
        result = db.execute(QUERY)
        for col in result.columns:
            if col.name == "ttl(name)":
                assert "Integer" in col.data_type or "int" in col.data_type.lower(), (
                    f"ttl column data_type should be Integer, got {col.data_type!r}"
                )
                return
        pytest.fail("ttl(name) column not found in metadata")


class TestWritetimeTtlParquet:
    """Parquet export round-trip for WRITETIME()/TTL() columns (Issue #693)."""

    @pytest.mark.skipif(not HAVE_PYARROW, reason="pyarrow not installed")
    def test_parquet_export_preserves_writetime_column_name(self, db, tmp_path):
        """Parquet field name must be 'writetime(name)' (parentheses preserved)."""
        out = tmp_path / "writetime_ttl.parquet"
        rows = db.export_parquet(QUERY, str(out))
        assert rows > 0, "export_parquet returned 0 rows"

        table = pq.read_table(out)
        assert "writetime(name)" in table.schema.names, (
            f"Expected 'writetime(name)' in Parquet schema, got {table.schema.names}"
        )
        assert "ttl(name)" in table.schema.names, (
            f"Expected 'ttl(name)' in Parquet schema, got {table.schema.names}"
        )

    @pytest.mark.skipif(not HAVE_PYARROW, reason="pyarrow not installed")
    def test_parquet_export_writetime_values_non_null(self, db, tmp_path):
        """writetime(name) column values must be non-null integers in Parquet."""
        out = tmp_path / "writetime_ttl.parquet"
        db.export_parquet(QUERY, str(out))

        table = pq.read_table(out)
        wt_values = table.column("writetime(name)").to_pylist()
        non_null = [v for v in wt_values if v is not None]
        assert len(non_null) > 0, (
            f"All writetime(name) values were null in Parquet; values={wt_values}"
        )
        for v in non_null:
            assert isinstance(v, int), f"writetime value should be int, got {type(v)}: {v!r}"
            assert v > 0, f"writetime value should be positive, got {v}"

    @pytest.mark.skipif(not HAVE_PYARROW, reason="pyarrow not installed")
    def test_parquet_export_ttl_null_for_rows_without_ttl(self, db, tmp_path):
        """ttl(name) column must contain only nulls when no TTL is set."""
        out = tmp_path / "writetime_ttl.parquet"
        db.export_parquet(QUERY, str(out))

        table = pq.read_table(out)
        ttl_values = table.column("ttl(name)").to_pylist()
        for v in ttl_values:
            assert v is None, f"Expected ttl(name) = null, got {v!r}"
