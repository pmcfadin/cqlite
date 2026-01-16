"""Tests for prepared statement functionality."""
import pytest
from pathlib import Path

import cqlite
from cqlite import PreparedStatement

# Test data paths - same as other test files
BINDINGS_DIR = Path(__file__).parent.parent
PROJECT_ROOT = BINDINGS_DIR.parent.parent
DATASETS = PROJECT_ROOT / "test-data" / "datasets" / "sstables"
SCHEMAS = PROJECT_ROOT / "test-data" / "schemas"


@pytest.fixture
def db():
    """Database fixture with schema loaded."""
    schema_file = SCHEMAS / "basic-types.cql"
    with cqlite.open(DATASETS, schema=schema_file) as database:
        yield database


class TestPrepareStatement:
    """Tests for Database.prepare() method."""

    def test_prepare_returns_prepared_statement(self, db):
        """Prepare returns a PreparedStatement instance."""
        stmt = db.prepare("SELECT * FROM test_basic.simple_table")
        assert stmt is not None
        assert isinstance(stmt, PreparedStatement)

    def test_prepared_statement_query_property(self, db):
        """PreparedStatement.query returns the original SQL."""
        query = "SELECT * FROM test_basic.simple_table LIMIT 10"
        stmt = db.prepare(query)
        assert stmt.query == query

    def test_prepared_statement_parameter_count(self, db):
        """PreparedStatement.parameter_count returns count."""
        stmt = db.prepare("SELECT * FROM test_basic.simple_table")
        assert stmt.parameter_count >= 0
        assert isinstance(stmt.parameter_count, int)

    def test_prepared_statement_stats(self, db):
        """PreparedStatement.stats() returns statistics dict."""
        stmt = db.prepare("SELECT * FROM test_basic.simple_table")
        stats = stmt.stats()
        assert isinstance(stats, dict)
        assert "parameter_count" in stats
        assert "plan_type" in stats
        assert "estimated_cost" in stats
        assert "estimated_rows" in stats
        assert "cache_friendly" in stats

    def test_prepared_statement_stats_types(self, db):
        """PreparedStatement.stats() returns correct types."""
        stmt = db.prepare("SELECT * FROM test_basic.simple_table")
        stats = stmt.stats()
        assert isinstance(stats["parameter_count"], int)
        assert isinstance(stats["plan_type"], str)
        assert isinstance(stats["estimated_cost"], float)
        assert isinstance(stats["estimated_rows"], int)
        assert isinstance(stats["cache_friendly"], bool)

    def test_prepared_statement_repr(self, db):
        """PreparedStatement has readable repr."""
        stmt = db.prepare("SELECT * FROM test_basic.simple_table")
        repr_str = repr(stmt)
        assert "PreparedStatement" in repr_str
        assert "SELECT" in repr_str


class TestPrepareErrors:
    """Tests for prepare error handling."""

    def test_prepare_on_closed_database(self, db):
        """Preparing on closed database raises RuntimeError."""
        db.close()
        with pytest.raises(RuntimeError, match="closed"):
            db.prepare("SELECT * FROM test_basic.simple_table")

    def test_prepare_invalid_syntax(self, db):
        """Preparing invalid SQL raises QueryError or ParseError."""
        # Note: Core raises QueryError for unsupported query types
        with pytest.raises((cqlite.QueryError, cqlite.ParseError)):
            db.prepare("SELEKT * FORM invalid")
