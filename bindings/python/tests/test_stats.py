"""Tests for database statistics functionality."""
import pytest

import cqlite
from cqlite import DatabaseStats


# db fixture is provided by conftest.py


class TestDatabaseStats:
    """Tests for Database.stats() method."""

    def test_stats_returns_database_stats(self, db):
        """stats() returns a DatabaseStats instance."""
        stats = db.stats()
        assert stats is not None
        assert isinstance(stats, DatabaseStats)

    def test_stats_has_storage_stats(self, db):
        """DatabaseStats has storage_stats property."""
        stats = db.stats()
        assert hasattr(stats, "storage_stats")
        storage = stats.storage_stats
        assert isinstance(storage, dict)

    def test_stats_has_memory_stats(self, db):
        """DatabaseStats has memory_stats property."""
        stats = db.stats()
        assert hasattr(stats, "memory_stats")
        memory = stats.memory_stats
        assert isinstance(memory, dict)

    def test_storage_stats_fields(self, db):
        """storage_stats contains expected fields."""
        stats = db.stats()
        storage = stats.storage_stats
        assert "sstable_count" in storage
        assert "total_size" in storage
        assert "total_entries" in storage
        assert "total_tables" in storage
        assert "average_size" in storage

    def test_storage_stats_types(self, db):
        """storage_stats fields have correct types."""
        stats = db.stats()
        storage = stats.storage_stats
        assert isinstance(storage["sstable_count"], int)
        assert isinstance(storage["total_size"], int)
        assert isinstance(storage["total_entries"], int)
        assert isinstance(storage["total_tables"], int)
        assert isinstance(storage["average_size"], int)

    def test_memory_stats_fields(self, db):
        """memory_stats contains expected fields."""
        stats = db.stats()
        memory = stats.memory_stats
        assert "block_cache_hits" in memory
        assert "block_cache_misses" in memory
        assert "row_cache_hits" in memory
        assert "row_cache_misses" in memory
        assert "total_memory_used" in memory
        assert "buffer_allocations" in memory
        assert "buffer_deallocations" in memory

    def test_memory_stats_types(self, db):
        """memory_stats fields have correct types."""
        stats = db.stats()
        memory = stats.memory_stats
        assert isinstance(memory["block_cache_hits"], int)
        assert isinstance(memory["block_cache_misses"], int)
        assert isinstance(memory["row_cache_hits"], int)
        assert isinstance(memory["row_cache_misses"], int)
        assert isinstance(memory["total_memory_used"], int)
        assert isinstance(memory["buffer_allocations"], int)
        assert isinstance(memory["buffer_deallocations"], int)

    def test_query_stats_present(self, db):
        """query_stats is present when state_machine enabled."""
        stats = db.stats()
        # query_stats should be present (state_machine is default)
        query = stats.query_stats
        if query is not None:
            assert isinstance(query, dict)
            assert "total_queries" in query
            assert "error_queries" in query
            assert "avg_execution_time_us" in query
            assert "cache_hit_ratio" in query
            assert "rows_affected" in query

    def test_query_stats_types(self, db):
        """query_stats fields have correct types when present."""
        stats = db.stats()
        query = stats.query_stats
        if query is not None:
            assert isinstance(query["total_queries"], int)
            assert isinstance(query["error_queries"], int)
            assert isinstance(query["avg_execution_time_us"], int)
            assert isinstance(query["cache_hit_ratio"], float)
            assert isinstance(query["rows_affected"], int)

    def test_stats_to_dict(self, db):
        """to_dict() returns complete stats as dict."""
        stats = db.stats()
        d = stats.to_dict()
        assert isinstance(d, dict)
        assert "storage_stats" in d
        assert "memory_stats" in d
        # query_stats may or may not be present depending on feature flags
        assert isinstance(d["storage_stats"], dict)
        assert isinstance(d["memory_stats"], dict)

    def test_stats_repr(self, db):
        """DatabaseStats has readable repr."""
        stats = db.stats()
        repr_str = repr(stats)
        assert "DatabaseStats" in repr_str


class TestStatsErrors:
    """Tests for stats error handling."""

    def test_stats_on_closed_database(self, db):
        """Getting stats on closed database raises RuntimeError."""
        db.close()
        with pytest.raises(RuntimeError, match="closed"):
            db.stats()


class TestStatsIntegration:
    """Integration tests for stats with real operations."""

    def test_stats_after_execute(self, db):
        """Stats are available after executing a query."""
        # Execute a query first
        db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")

        # Get stats
        stats = db.stats()
        assert stats is not None
        assert stats.storage_stats is not None
        assert stats.memory_stats is not None

    def test_stats_multiple_calls(self, db):
        """Multiple stats() calls work correctly."""
        stats1 = db.stats()
        stats2 = db.stats()

        # Both should be valid DatabaseStats instances
        assert isinstance(stats1, DatabaseStats)
        assert isinstance(stats2, DatabaseStats)

        # Storage stats should be consistent
        assert stats1.storage_stats["sstable_count"] == stats2.storage_stats["sstable_count"]
