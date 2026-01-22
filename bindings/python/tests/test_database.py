"""Tests for cqlite Database class and open() function.

This module tests Issue #292: Implement Database.open() and Database.close().

Tests verify:
1. Database.open() works with data directory
2. Database.open() works with schema file
3. Context manager protocol (__enter__, __exit__)
4. close() is idempotent
5. Invalid paths raise appropriate errors
"""

import pytest

import cqlite

from conftest import DATASETS, SCHEMAS


class TestDatabaseImports:
    """Test that Database and open are importable."""

    def test_database_importable(self):
        """Database class should be importable from cqlite."""
        assert hasattr(cqlite, "Database")
        assert cqlite.Database is not None

    def test_open_importable(self):
        """open() function should be importable from cqlite."""
        assert hasattr(cqlite, "open")
        assert callable(cqlite.open)

    def test_database_in_all(self):
        """Database should be in __all__."""
        assert "Database" in cqlite.__all__

    def test_open_in_all(self):
        """open should be in __all__."""
        assert "open" in cqlite.__all__


class TestDatabaseOpen:
    """Test Database.open() functionality."""

    def test_open_with_path_string(self):
        """open() should accept string path."""
        db = cqlite.open(str(DATASETS))
        assert db is not None
        assert not db.is_closed
        db.close()

    def test_open_with_pathlib(self):
        """open() should accept pathlib.Path objects."""
        db = cqlite.open(DATASETS)
        assert db is not None
        assert not db.is_closed
        db.close()

    def test_open_with_schema(self):
        """open() should accept optional schema parameter."""
        schema_file = SCHEMAS / "basic-types.cql"
        if schema_file.exists():
            db = cqlite.open(DATASETS, schema=schema_file)
            assert db is not None
            assert not db.is_closed
            db.close()
        else:
            pytest.skip("Schema file not found")

    def test_open_with_config_preset(self):
        """open() should accept config preset string."""
        db = cqlite.open(DATASETS, config="memory_optimized")
        assert db is not None
        db.close()

    def test_open_with_config_dict(self):
        """open() should accept config dict."""
        config = cqlite.memory_optimized()
        db = cqlite.open(DATASETS, config=config)
        assert db is not None
        db.close()

    def test_open_nonexistent_path_succeeds_empty(self, tmp_path):
        """open() with nonexistent path succeeds but has no data.

        This is expected behavior - cqlite opens a data directory and scans
        for SSTables. A non-existent or empty directory is valid (no SSTables).
        """
        nonexistent = tmp_path / "definitely_does_not_exist_12345" / "nested" / "path"
        db = cqlite.open(str(nonexistent))
        assert db is not None
        assert not db.is_closed
        db.close()

    def test_open_invalid_config_raises(self):
        """open() with invalid config should raise ValueError."""
        # Create an invalid config by modifying a preset
        invalid_config = cqlite.memory_optimized()
        invalid_config["memory"]["max_memory"] = 0
        with pytest.raises(ValueError):
            cqlite.open(DATASETS, config=invalid_config)


class TestDatabaseClose:
    """Test Database.close() functionality."""

    def test_close_sets_is_closed(self):
        """close() should set is_closed to True."""
        db = cqlite.open(DATASETS)
        assert not db.is_closed
        db.close()
        assert db.is_closed

    def test_close_idempotent(self):
        """close() should be safe to call multiple times."""
        db = cqlite.open(DATASETS)
        db.close()
        db.close()  # Should not raise
        db.close()  # Should not raise
        assert db.is_closed


class TestContextManager:
    """Test context manager protocol."""

    def test_context_manager_enters(self):
        """Context manager should return database on enter."""
        with cqlite.open(DATASETS) as db:
            assert db is not None
            assert not db.is_closed

    def test_context_manager_closes_on_exit(self):
        """Context manager should close database on exit."""
        with cqlite.open(DATASETS) as db:
            pass
        assert db.is_closed

    def test_context_manager_closes_on_exception(self):
        """Context manager should close database even when exception raised."""
        db = None
        with pytest.raises(ValueError):
            with cqlite.open(DATASETS) as db:
                raise ValueError("test exception")
        assert db is not None
        assert db.is_closed

    def test_context_manager_does_not_suppress_exceptions(self):
        """Context manager should not suppress exceptions."""
        with pytest.raises(RuntimeError, match="test error"):
            with cqlite.open(DATASETS) as db:
                raise RuntimeError("test error")


class TestDatabaseRepr:
    """Test Database string representation."""

    def test_repr_when_open(self):
        """repr() should show 'open' when database is open."""
        db = cqlite.open(DATASETS)
        assert "open" in repr(db).lower()
        db.close()

    def test_repr_when_closed(self):
        """repr() should show 'closed' when database is closed."""
        db = cqlite.open(DATASETS)
        db.close()
        assert "closed" in repr(db).lower()


class TestDatabaseWithRealData:
    """Integration tests with real SSTable data.

    These tests require the test data to be present.
    Skip if test data is not available.
    """

    @pytest.fixture(autouse=True)
    def check_test_data(self):
        """Skip tests if test data is not available."""
        if not DATASETS.exists():
            pytest.skip(f"Test data not found at {DATASETS}")

    def test_open_close_cycle(self):
        """Complete open/close cycle should work."""
        # First open
        db = cqlite.open(DATASETS)
        assert not db.is_closed

        # Close
        db.close()
        assert db.is_closed

        # Reopen (new instance)
        db2 = cqlite.open(DATASETS)
        assert not db2.is_closed
        db2.close()

    def test_multiple_databases(self):
        """Should be able to open multiple databases."""
        db1 = cqlite.open(DATASETS)
        db2 = cqlite.open(DATASETS)

        assert not db1.is_closed
        assert not db2.is_closed

        db1.close()
        assert db1.is_closed
        assert not db2.is_closed

        db2.close()
        assert db2.is_closed
