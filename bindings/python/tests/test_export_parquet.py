"""Tests for Database.export_parquet (Issue #686, Epic #682).

Validates that the Python bindings can export query results to Parquet
files using the core writer (cqlite-core `parquet` feature), covering a
scalar table and a collections table.  Files are read back with pyarrow
when available; otherwise structural checks (magic bytes, row count via
re-query) are used so the tests still validate behavior without the
optional dependency.

Tests skip when datasets are absent (see conftest.require_test_data).
"""

import pytest

from conftest import (
    DATASETS,
    SCHEMA_BASIC_TYPES,
    SCHEMA_COLLECTIONS,
    require_test_data,
)

try:
    import pyarrow.parquet as pq

    HAVE_PYARROW = True
except ImportError:
    HAVE_PYARROW = False


def _assert_parquet_magic(path):
    data = path.read_bytes()
    assert len(data) > 8, "Parquet file too small"
    assert data[:4] == b"PAR1", "file must start with PAR1 magic"
    assert data[-4:] == b"PAR1", "file must end with PAR1 magic"


class TestExportParquetScalars:
    """Scalar table export (test_basic.simple_table)."""

    QUERY = "SELECT * FROM test_basic.simple_table"

    def test_export_creates_valid_file(self, db, tmp_path):
        out = tmp_path / "simple_table.parquet"
        rows = db.export_parquet(self.QUERY, str(out))

        assert out.exists(), "export must create the file"
        _assert_parquet_magic(out)

        expected = len(db.execute(self.QUERY).rows)
        assert rows == expected, "returned row count must match query result"

    @pytest.mark.skipif(not HAVE_PYARROW, reason="pyarrow not installed")
    def test_export_readable_by_pyarrow(self, db, tmp_path):
        out = tmp_path / "simple_table.parquet"
        rows = db.export_parquet(self.QUERY, str(out))

        table = pq.read_table(out)
        assert table.num_rows == rows

        # High-fidelity mapping (epic #673): uuid → FixedSizeBinary(16)
        # with the Arrow UUID extension; text → utf8.
        schema = table.schema
        names = set(schema.names)
        assert "id" in names and "name" in names

        import pyarrow as pa

        id_type = schema.field("id").type
        # uuid is either the registered uuid extension or FixedSizeBinary(16)
        assert (
            id_type == pa.binary(16)
            or getattr(id_type, "extension_name", "") == "arrow.uuid"
        ), f"uuid column should be FixedSizeBinary(16)/uuid extension, got {id_type}"
        assert schema.field("name").type == pa.string()

    def test_export_row_group_size(self, db, tmp_path):
        out = tmp_path / "small_groups.parquet"
        rows = db.export_parquet(self.QUERY, str(out), row_group_size=2)
        assert rows > 0
        _assert_parquet_magic(out)

        if HAVE_PYARROW:
            pf = pq.ParquetFile(out)
            assert pf.metadata.num_row_groups >= rows // 2

    def test_export_zstd_compression(self, db, tmp_path):
        out = tmp_path / "zstd.parquet"
        rows = db.export_parquet(self.QUERY, str(out), compression="zstd")
        assert rows > 0
        _assert_parquet_magic(out)

        if HAVE_PYARROW:
            table = pq.read_table(out)
            assert table.num_rows == rows


class TestExportParquetCollections:
    """Collections table export (test_collections.collection_table)."""

    QUERY = "SELECT * FROM test_collections.collection_table"

    def test_export_creates_valid_file(self, db_collections, tmp_path):
        out = tmp_path / "collection_table.parquet"
        rows = db_collections.export_parquet(self.QUERY, str(out))
        assert rows > 0
        _assert_parquet_magic(out)

    @pytest.mark.skipif(not HAVE_PYARROW, reason="pyarrow not installed")
    def test_collections_export_typed_columns(self, db_collections, tmp_path):
        import pyarrow as pa

        out = tmp_path / "collection_table.parquet"
        rows = db_collections.export_parquet(self.QUERY, str(out))

        table = pq.read_table(out)
        assert table.num_rows == rows

        schema = table.schema
        # list/set columns must be Arrow List (not stringified), map columns
        # must be Arrow Map — the epic #673 typed mapping. Column names per
        # test-data/schemas/collections.cql collection_table.
        for name in ("tags", "scores", "numbers_set", "ordered_values"):
            field = schema.field(name)
            assert pa.types.is_list(
                field.type
            ), f"{name} should be an Arrow List, got {field.type}"
        for name in ("properties", "metadata_map"):
            field = schema.field(name)
            assert pa.types.is_map(
                field.type
            ), f"{name} should be an Arrow Map, got {field.type}"


class TestExportParquetErrors:
    """Error mapping for export_parquet."""

    def test_invalid_compression_raises_value_error(self, db, tmp_path):
        with pytest.raises(ValueError, match="compression"):
            db.export_parquet(
                "SELECT * FROM test_basic.simple_table",
                str(tmp_path / "x.parquet"),
                compression="lz77",
            )

    def test_zero_row_group_size_raises_value_error(self, db, tmp_path):
        with pytest.raises(ValueError, match="row_group_size"):
            db.export_parquet(
                "SELECT * FROM test_basic.simple_table",
                str(tmp_path / "x.parquet"),
                row_group_size=0,
            )

    def test_unwritable_path_raises_io_error(self, db):
        with pytest.raises(IOError):
            db.export_parquet(
                "SELECT * FROM test_basic.simple_table",
                "/nonexistent-dir/definitely/missing/out.parquet",
            )

    def test_closed_database_raises_runtime_error(self, tmp_path):
        require_test_data(SCHEMA_BASIC_TYPES)
        import cqlite

        database = cqlite.open(DATASETS, schema=SCHEMA_BASIC_TYPES)
        database.close()
        with pytest.raises(RuntimeError):
            database.export_parquet(
                "SELECT * FROM test_basic.simple_table",
                str(tmp_path / "x.parquet"),
            )
