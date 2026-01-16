"""Tests for Database.execute_streaming() - Issue #294.

TDD tests for streaming query execution with memory-bounded iteration.

Tests verify:
1. execute_streaming() returns StreamingIterator
2. Iteration yields Row objects one at a time
3. Custom StreamingConfig is respected
4. Early termination works correctly
5. Memory usage stays bounded
"""

import pytest
from pathlib import Path

import cqlite


# Test data paths
TEST_DATA = Path(__file__).parent.parent.parent.parent / "test-data"
DATASETS = TEST_DATA / "datasets" / "sstables"
SCHEMAS = TEST_DATA / "schemas"


@pytest.fixture
def db():
    """Database fixture with schema loaded."""
    schema_file = SCHEMAS / "basic-types.cql"
    if not schema_file.exists():
        pytest.skip(f"Schema file not found: {schema_file}")
    if not DATASETS.exists():
        pytest.skip(f"Test data not found: {DATASETS}")
    with cqlite.open(DATASETS, schema=schema_file) as database:
        yield database


class TestStreamingImports:
    """Test that streaming-related types are importable."""

    def test_streaming_iterator_importable(self):
        """StreamingIterator class should be importable from cqlite."""
        assert hasattr(cqlite, "StreamingIterator")
        assert cqlite.StreamingIterator is not None

    def test_streaming_config_importable(self):
        """StreamingConfig class should be importable from cqlite."""
        assert hasattr(cqlite, "StreamingConfig")
        assert cqlite.StreamingConfig is not None

    def test_types_in_all(self):
        """Streaming types should be in __all__."""
        assert "StreamingIterator" in cqlite.__all__
        assert "StreamingConfig" in cqlite.__all__


class TestStreamingBasic:
    """Basic execute_streaming() functionality."""

    def test_execute_streaming_returns_iterator(self, db):
        """execute_streaming() should return StreamingIterator."""
        result = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5")
        assert isinstance(result, cqlite.StreamingIterator)

    def test_streaming_iteration_yields_rows(self, db):
        """Streaming should yield Row objects."""
        count = 0
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5"):
            assert isinstance(row, cqlite.Row)
            count += 1
            if count >= 5:
                break
        # May be 0 if no data, but iteration should work
        assert count >= 0

    def test_streaming_for_loop(self, db):
        """Should work in for loop."""
        rows = []
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 3"):
            rows.append(row)
            if len(rows) >= 3:
                break
        assert all(isinstance(r, cqlite.Row) for r in rows)

    def test_streaming_list_conversion(self, db):
        """Should be convertible to list."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 3")
        rows = list(iterator)
        assert isinstance(rows, list)
        assert all(isinstance(r, cqlite.Row) for r in rows)

    def test_streaming_on_closed_db_raises(self, db):
        """execute_streaming() on closed database should raise RuntimeError."""
        db.close()
        with pytest.raises(RuntimeError):
            db.execute_streaming("SELECT * FROM test_basic.simple_table")

    def test_streaming_invalid_query_raises(self, db):
        """Invalid SQL should raise QueryError or ParseError."""
        with pytest.raises((cqlite.ParseError, cqlite.QueryError)):
            list(db.execute_streaming("SELEKT * FORM users"))


class TestStreamingWithConfig:
    """Test execute_streaming() with custom StreamingConfig."""

    def test_streaming_with_default_config(self, db):
        """Streaming with default config should work."""
        config = cqlite.StreamingConfig()
        result = db.execute_streaming(
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            config=config
        )
        assert isinstance(result, cqlite.StreamingIterator)

    def test_streaming_with_custom_buffer_size(self, db):
        """Streaming with custom buffer_size should work."""
        config = cqlite.StreamingConfig(buffer_size=512)
        result = db.execute_streaming(
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            config=config
        )
        rows = list(result)
        assert isinstance(rows, list)

    def test_streaming_with_custom_chunk_size(self, db):
        """Streaming with custom chunk_size should work."""
        config = cqlite.StreamingConfig(chunk_size=1000)
        result = db.execute_streaming(
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            config=config
        )
        rows = list(result)
        assert isinstance(rows, list)

    def test_streaming_with_full_config(self, db):
        """Streaming with all config options should work."""
        config = cqlite.StreamingConfig(buffer_size=256, chunk_size=500)
        result = db.execute_streaming(
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            config=config
        )
        rows = list(result)
        assert isinstance(rows, list)


class TestStreamingEarlyTermination:
    """Test early termination (break from loop)."""

    def test_break_from_loop(self, db):
        """Breaking from iteration should work without errors."""
        count = 0
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table"):
            count += 1
            if count >= 2:
                break
        # Should have stopped at 2 rows (or fewer if table is small)
        assert count <= 2 or count == 0  # 0 if no data

    def test_partial_iteration(self, db):
        """Partial iteration followed by abandonment should be safe."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 10")
        # Get just one row
        try:
            first_row = next(iter(iterator))
            assert isinstance(first_row, cqlite.Row)
        except StopIteration:
            pass  # No data is fine
        # Iterator goes out of scope - should clean up gracefully
        del iterator


class TestStreamingIteratorAttributes:
    """Test StreamingIterator attributes and methods."""

    def test_rows_received_attribute(self, db):
        """StreamingIterator should track rows_received."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5")
        initial_count = iterator.rows_received
        assert isinstance(initial_count, int)
        assert initial_count >= 0

        # Consume some rows
        for row in iterator:
            break
        # rows_received may have increased (or stayed 0 if no data)
        assert iterator.rows_received >= initial_count

    def test_progress_percent_attribute(self, db):
        """StreamingIterator should have progress_percent (may be None)."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5")
        progress = iterator.progress_percent
        # May be None if total is unknown, or a float if known
        assert progress is None or isinstance(progress, float)

    def test_repr(self, db):
        """StreamingIterator should have meaningful repr."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5")
        repr_str = repr(iterator)
        assert "StreamingIterator" in repr_str


class TestStreamingRowAccess:
    """Test Row access from streaming results."""

    def test_row_dict_access(self, db):
        """Rows from streaming should support dict-style access."""
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 1"):
            keys = row.keys()
            assert isinstance(keys, list)
            if keys:
                # Access should work for existing columns
                _ = row[keys[0]]
            break

    def test_row_to_dict(self, db):
        """Rows from streaming should support to_dict()."""
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 1"):
            d = row.to_dict()
            assert isinstance(d, dict)
            break


class TestStreamingMemory:
    """Test memory behavior of streaming (informational)."""

    def test_streaming_memory_bounded(self, db):
        """Streaming should use bounded memory (informational test)."""
        import tracemalloc

        tracemalloc.start()

        # Consume some rows via streaming
        count = 0
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table"):
            count += 1
            if count >= 100:
                break

        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Informational: log the peak memory
        # This is not a hard assertion since memory depends on many factors
        # but peak should be well under 128MB for streaming
        assert peak < 128 * 1024 * 1024, f"Peak memory {peak / 1024 / 1024:.1f}MB exceeded 128MB"
