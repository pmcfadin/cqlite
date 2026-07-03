"""Tests for Database.execute() - Issue #293.

TDD tests for synchronous query execution returning all rows.

Tests verify:
1. execute() returns QueryResult with rows
2. Rows accessible via iteration and indexing
3. Column metadata available
4. Dict-like row access
"""

import pytest

import cqlite

from conftest import (
    DATASETS,
    SCHEMA_BASIC_TYPES,
    skip_if_no_datasets,
    skip_if_no_schema,
)


class TestExecuteImports:
    """Test that execute-related types are importable."""

    def test_query_result_importable(self):
        """QueryResult class should be importable from cqlite."""
        assert hasattr(cqlite, "QueryResult")
        assert cqlite.QueryResult is not None

    def test_row_importable(self):
        """Row class should be importable from cqlite."""
        assert hasattr(cqlite, "Row")
        assert cqlite.Row is not None

    def test_column_info_importable(self):
        """ColumnInfo class should be importable from cqlite."""
        assert hasattr(cqlite, "ColumnInfo")
        assert cqlite.ColumnInfo is not None

    def test_types_in_all(self):
        """Result types should be in __all__."""
        assert "QueryResult" in cqlite.__all__
        assert "Row" in cqlite.__all__
        assert "ColumnInfo" in cqlite.__all__


class TestExecuteBasic:
    """Basic execute() functionality."""

    def test_execute_returns_query_result(self, db):
        """execute() should return QueryResult object."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 5")
        assert isinstance(result, cqlite.QueryResult)

    def test_execute_select_with_limit(self, db):
        """execute() should respect LIMIT clause."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 5")
        assert len(result.rows) <= 5
        assert result.execution_time_ms >= 0

    def test_execute_returns_rows(self, db):
        """execute() should return rows when data exists."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 3")
        # May be 0 if no data, but should be list
        assert isinstance(result.rows, list)

    def test_execute_invalid_query_raises_error(self, db):
        """Invalid SQL should raise QueryError or ParseError."""
        # Note: The exact error type depends on how the core handles it
        # Currently "SELEKT" is treated as unsupported query type (QueryError)
        with pytest.raises((cqlite.ParseError, cqlite.QueryError)):
            db.execute("SELEKT * FORM users")

    def test_execute_nonexistent_table_returns_empty(self, db):
        """Query on nonexistent table returns empty result (SSTable is read-only)."""
        # CQLite is a read-only SSTable reader, so nonexistent tables
        # simply return empty results rather than raising errors
        result = db.execute("SELECT * FROM nonexistent_keyspace.nonexistent_table")
        assert len(result) == 0

    def test_execute_on_closed_db_raises(self, db):
        """execute() on closed database should raise RuntimeError."""
        db.close()
        with pytest.raises(RuntimeError):
            db.execute("SELECT * FROM test_basic.simple_table")


class TestQueryResultIteration:
    """Test QueryResult iteration."""

    def test_result_is_iterable(self, db):
        """QueryResult should be iterable."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 3")
        rows = list(result)
        assert len(rows) == len(result.rows)

    def test_result_for_loop(self, db):
        """Should work in for loop."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 3")
        count = 0
        for row in result:
            count += 1
            assert isinstance(row, cqlite.Row)
        assert count == len(result.rows)

    def test_result_len(self, db):
        """len() should work on QueryResult."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 5")
        assert len(result) == len(result.rows)


class TestRowDictAccess:
    """Test Row dict-like access - Issue #293 TDD requirement."""

    def test_row_getitem(self, db):
        """Row should support dict-style access: row["column"]."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) > 0:
            row = result.rows[0]
            keys = row.keys()
            assert len(keys) > 0
            # Access should work for existing columns
            first_key = keys[0]
            _ = row[first_key]  # Should not raise

    def test_row_keys(self, db):
        """Row.keys() should return column names."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) > 0:
            row = result.rows[0]
            keys = row.keys()
            assert isinstance(keys, list)
            assert all(isinstance(k, str) for k in keys)

    def test_row_contains(self, db):
        """Row should support 'in' operator."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) > 0:
            row = result.rows[0]
            keys = row.keys()
            if keys:
                assert keys[0] in row
            assert "nonexistent_column_xyz_123" not in row

    def test_row_get_with_default(self, db):
        """Row.get() should support default value."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) > 0:
            row = result.rows[0]
            value = row.get("nonexistent_column_xyz", "default_value")
            assert value == "default_value"

    def test_row_to_dict(self, db):
        """Row.to_dict() should return Python dict."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) > 0:
            row = result.rows[0]
            d = row.to_dict()
            assert isinstance(d, dict)

    def test_row_missing_key_raises(self, db):
        """Accessing missing key should raise KeyError."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) > 0:
            row = result.rows[0]
            with pytest.raises(KeyError):
                _ = row["nonexistent_column_xyz_123"]

    def test_row_values(self, db):
        """Row.values() should return column values."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) > 0:
            row = result.rows[0]
            values = row.values()
            assert isinstance(values, list)
            assert len(values) == len(row.keys())

    def test_row_items(self, db):
        """Row.items() should return (key, value) pairs."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) > 0:
            row = result.rows[0]
            items = row.items()
            assert isinstance(items, list)
            for key, value in items:
                assert isinstance(key, str)


class TestQueryResultMetadata:
    """Test QueryResult metadata attributes."""

    def test_rows_affected(self, db):
        """rows_affected should be an integer >= 0."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        assert isinstance(result.rows_affected, int)
        assert result.rows_affected >= 0

    def test_execution_time_ms(self, db):
        """execution_time_ms should be non-negative."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        assert isinstance(result.execution_time_ms, int)
        assert result.execution_time_ms >= 0

    def test_columns_list(self, db):
        """columns should be list of ColumnInfo."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        assert isinstance(result.columns, list)

    def test_column_info_attributes(self, db):
        """ColumnInfo should have expected attributes."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if result.columns:
            col = result.columns[0]
            assert hasattr(col, "name")
            assert hasattr(col, "data_type")
            assert hasattr(col, "nullable")
            assert hasattr(col, "position")
            assert isinstance(col.name, str)
            assert isinstance(col.data_type, str)
            assert isinstance(col.nullable, bool)
            assert isinstance(col.position, int)


class TestQueryResultToDict:
    """Test QueryResult.to_dict() method."""

    def test_to_dict_returns_dict(self, db):
        """to_dict() should return Python dict."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 3")
        d = result.to_dict()
        assert isinstance(d, dict)

    def test_to_dict_has_rows(self, db):
        """to_dict() should include rows."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 3")
        d = result.to_dict()
        assert "rows" in d
        assert isinstance(d["rows"], list)

    def test_to_dict_has_metadata(self, db):
        """to_dict() should include metadata."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        d = result.to_dict()
        assert "rows_affected" in d
        assert "execution_time_ms" in d


class TestRepr:
    """Test string representations."""

    def test_query_result_repr(self, db):
        """QueryResult should have meaningful repr."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        repr_str = repr(result)
        assert "QueryResult" in repr_str

    def test_row_repr(self, db):
        """Row should have meaningful repr."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if result.rows:
            repr_str = repr(result.rows[0])
            # Should be either "Row(...)" or dict-like "{...}"
            assert "Row" in repr_str or "{" in repr_str

    def test_column_info_repr(self, db):
        """ColumnInfo should have meaningful repr."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if result.columns:
            repr_str = repr(result.columns[0])
            assert "ColumnInfo" in repr_str or result.columns[0].name in repr_str


class TestRowSelectOrder:
    """Row must return columns in SELECT order, not hash order (issue #1445)."""

    def test_row_keys_match_select_order(self, db):
        """keys()/to_dict()/items() must all follow result.columns order."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 5")
        assert len(result) > 0, (
            "fixture present but returned 0 rows - datasets unreadable/empty"
        )
        expected = [c.name for c in result.columns]
        assert expected, "SELECT * must expose column metadata"
        for row in result.rows:
            assert list(row.keys()) == expected
            assert list(row.to_dict().keys()) == expected
            assert [k for k, _ in row.items()] == expected
            # values() must align positionally with keys()
            assert list(row.values()) == [row[k] for k in expected]

    def test_row_keys_match_explicit_select_order(self, db):
        """An explicit column list that differs from storage order is honored."""
        # Storage/definition order is (id, name, age, ...); request a reorder.
        result = db.execute(
            "SELECT name, id, age FROM test_basic.simple_table LIMIT 5"
        )
        assert len(result) > 0, (
            "fixture present but returned 0 rows - datasets unreadable/empty"
        )
        expected = ["name", "id", "age"]
        assert [c.name for c in result.columns] == expected
        for row in result.rows:
            assert list(row.keys()) == expected
            assert list(row.to_dict().keys()) == expected
            assert [k for k, _ in row.items()] == expected

    def test_row_getitem_and_contains_still_work(self, db):
        """O(1) lookups by name remain correct after the ordered rewrite."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        assert len(result) > 0
        row = result.rows[0]
        assert "id" in row
        assert "definitely_not_a_column" not in row
        # get() with default falls back for a missing column
        sentinel = object()
        assert row.get("definitely_not_a_column", sentinel) is sentinel
        # __getitem__ raises KeyError for a missing column
        with pytest.raises(KeyError):
            _ = row["definitely_not_a_column"]
        assert len(row) == len(result.columns)


class TestSchemaLessStreamingSelectOrder:
    """Schema-less streaming SELECT * must not drop values (issue #1445).

    Regression: in the streaming path, core's ``get_result_columns()`` can
    return an EMPTY column list for a schema-less ``SELECT *`` even though the
    streamed rows carry values. The per-stream ``RowShape`` was built only from
    those (empty) metadata columns, so every streamed row lost ALL its values
    and every column lookup failed. The fix builds the shape from the first
    row's value keys (sorted, mirroring the materialized/core path) when
    metadata columns are empty.
    """

    def test_schemaless_streaming_rows_nonempty_and_ordered(self):
        """Streamed schema-less rows expose columns/values in sorted order."""
        skip_if_no_datasets()
        # Open WITHOUT a schema so core has no authoritative column metadata.
        with cqlite.open(DATASETS) as db:
            rows = list(
                db.execute_streaming(
                    "SELECT * FROM test_basic.simple_table LIMIT 5"
                )
            )
            assert rows, (
                "fixture present but streaming returned 0 rows - "
                "datasets unreadable/empty"
            )
            for row in rows:
                keys = list(row.keys())
                # The regression made every row expose ZERO columns.
                assert keys, "schema-less streamed row lost all its columns"
                # Materialized/core ordering for schema-less SELECT * is the
                # first row's value keys sorted alphabetically (issue #129).
                assert keys == sorted(keys), (
                    "streamed columns must be in sorted (materialized) order"
                )
                # Every advertised column must be a working lookup with a value
                # (at least one non-None, i.e. values were actually surfaced).
                assert [k for k, _ in row.items()] == keys
                assert list(row.to_dict().keys()) == keys
                assert list(row.values()) == [row[k] for k in keys]
                assert any(row[k] is not None for k in keys), (
                    "schema-less streamed row surfaced no values"
                )

    def test_schemaless_streaming_matches_materialized(self):
        """Streaming and materialized schema-less SELECT * agree column-wise."""
        skip_if_no_datasets()
        with cqlite.open(DATASETS) as db:
            materialized = db.execute(
                "SELECT * FROM test_basic.simple_table LIMIT 5"
            )
            assert len(materialized) > 0
            mat_keys = list(materialized.rows[0].keys())

        with cqlite.open(DATASETS) as db:
            streamed = list(
                db.execute_streaming(
                    "SELECT * FROM test_basic.simple_table LIMIT 5"
                )
            )
        assert streamed, "streaming returned 0 rows"
        assert list(streamed[0].keys()) == mat_keys, (
            "streaming column order must match materialized output"
        )

    def test_schemaless_materialized_point_lookup_keeps_values(self):
        """Legacy materialized point-lookup must not drop values (issue #1445).

        A simple ``WHERE id = <literal>`` SELECT is intentionally kept on the
        legacy `QueryExecutor`, which wraps rows via `QueryResult::with_rows(..)`
        and leaves `metadata.columns` empty. Before the fix, the materialized
        `Row` shape was built solely from those empty columns, so the returned
        row exposed ZERO columns and dropped every value.
        """
        skip_if_no_datasets()
        skip_if_no_schema(SCHEMA_BASIC_TYPES)
        # Grab a real partition key via a schema-full read (the schema-less
        # scan omits the partition key column, so we need the schema here).
        with cqlite.open(DATASETS, schema=SCHEMA_BASIC_TYPES) as db:
            seed = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
            assert len(seed) > 0, "fixture present but returned 0 rows"
            id_value = seed.rows[0]["id"]

        # Schema-less point lookup routes through the legacy executor's
        # empty-metadata `with_rows` path.
        with cqlite.open(DATASETS) as db:
            result = db.execute(
                f"SELECT * FROM test_basic.simple_table WHERE id = {id_value}"
            )
            assert len(result) > 0, "point lookup returned no row"
            row = result.rows[0]
            keys = list(row.keys())
            assert keys, "legacy materialized point lookup dropped all columns"
            assert keys == sorted(keys), "columns must be in sorted order"
            assert list(row.to_dict().keys()) == keys
            assert list(row.values()) == [row[k] for k in keys]
            assert any(row[k] is not None for k in keys), (
                "legacy materialized point lookup surfaced no values"
            )

    def test_aggregate_value_not_dropped(self):
        """COUNT(*) value must be surfaced, not dropped (issue #1445).

        The aggregate row is keyed by its alias (e.g. ``Count(*)``) while
        ``metadata.columns`` carries a positional placeholder (``col_0``). The
        positional row rewrite must NOT drop values whose key is absent from the
        metadata columns; before the fix, ``SELECT COUNT(*)`` returned only
        ``{col_0: None}`` and lost the actual count.
        """
        skip_if_no_datasets()
        skip_if_no_schema(SCHEMA_BASIC_TYPES)
        with cqlite.open(DATASETS, schema=SCHEMA_BASIC_TYPES) as db:
            baseline = db.execute("SELECT * FROM test_basic.simple_table")
            expected_count = len(baseline)
            assert expected_count > 0, "fixture present but returned 0 rows"

            agg = db.execute("SELECT COUNT(*) FROM test_basic.simple_table")
            assert len(agg) == 1
            row = agg.rows[0]
            d = row.to_dict()
            # The actual count must appear as a value somewhere in the row and
            # equal the number of rows in the table (no data loss).
            assert expected_count in d.values(), (
                f"COUNT(*) value {expected_count} was dropped from row {d}"
            )
