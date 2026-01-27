"""Tests for cqlite exception types and error handling.

This module tests Issue #288: Error Mapping Layer.

Tests verify:
1. All custom exceptions can be imported
2. Custom exceptions inherit from CqliteError base
3. Exception hierarchy is correct
"""

import pytest

import cqlite


class TestExceptionImports:
    """Test that all exception types are importable."""

    def test_cqlite_error_importable(self):
        """Test CqliteError base exception is importable."""
        assert hasattr(cqlite, "CqliteError")
        assert cqlite.CqliteError is not None

    def test_schema_error_importable(self):
        """Test SchemaError is importable."""
        assert hasattr(cqlite, "SchemaError")
        assert cqlite.SchemaError is not None

    def test_query_error_importable(self):
        """Test QueryError is importable."""
        assert hasattr(cqlite, "QueryError")
        assert cqlite.QueryError is not None

    def test_parse_error_importable(self):
        """Test ParseError is importable."""
        assert hasattr(cqlite, "ParseError")
        assert cqlite.ParseError is not None


class TestExceptionHierarchy:
    """Test exception class hierarchy."""

    def test_schema_error_inherits_from_cqlite_error(self):
        """SchemaError should inherit from CqliteError."""
        assert issubclass(cqlite.SchemaError, cqlite.CqliteError)

    def test_query_error_inherits_from_cqlite_error(self):
        """QueryError should inherit from CqliteError."""
        assert issubclass(cqlite.QueryError, cqlite.CqliteError)

    def test_parse_error_inherits_from_cqlite_error(self):
        """ParseError should inherit from CqliteError."""
        assert issubclass(cqlite.ParseError, cqlite.CqliteError)

    def test_cqlite_error_inherits_from_exception(self):
        """CqliteError should inherit from Exception."""
        assert issubclass(cqlite.CqliteError, Exception)


class TestExceptionInstantiation:
    """Test that exceptions can be raised and caught."""

    def test_raise_cqlite_error(self):
        """Test CqliteError can be raised."""
        try:
            raise cqlite.CqliteError("test error")
        except cqlite.CqliteError as e:
            assert "test error" in str(e)

    def test_raise_schema_error(self):
        """Test SchemaError can be raised."""
        try:
            raise cqlite.SchemaError("schema problem")
        except cqlite.SchemaError as e:
            assert "schema problem" in str(e)

    def test_raise_query_error(self):
        """Test QueryError can be raised."""
        try:
            raise cqlite.QueryError("query failed")
        except cqlite.QueryError as e:
            assert "query failed" in str(e)

    def test_raise_parse_error(self):
        """Test ParseError can be raised."""
        try:
            raise cqlite.ParseError("parse failed")
        except cqlite.ParseError as e:
            assert "parse failed" in str(e)


class TestCatchAllBehavior:
    """Test catch-all exception handling."""

    def test_catch_schema_error_as_cqlite_error(self):
        """SchemaError should be catchable as CqliteError."""
        try:
            raise cqlite.SchemaError("schema issue")
        except cqlite.CqliteError as e:
            assert "schema issue" in str(e)

    def test_catch_query_error_as_cqlite_error(self):
        """QueryError should be catchable as CqliteError."""
        try:
            raise cqlite.QueryError("query issue")
        except cqlite.CqliteError as e:
            assert "query issue" in str(e)

    def test_catch_parse_error_as_cqlite_error(self):
        """ParseError should be catchable as CqliteError."""
        try:
            raise cqlite.ParseError("parse issue")
        except cqlite.CqliteError as e:
            assert "parse issue" in str(e)

    def test_catch_all_cqlite_exceptions(self):
        """All CQLite exceptions should be catchable with base class."""
        exceptions = [
            cqlite.SchemaError("schema"),
            cqlite.QueryError("query"),
            cqlite.ParseError("parse"),
            cqlite.CqliteError("base"),
        ]

        for exc in exceptions:
            with pytest.raises(cqlite.CqliteError):
                raise exc


class TestExceptionInAllExports:
    """Test exceptions are in __all__."""

    def test_exceptions_in_all(self):
        """All exception types should be in __all__."""
        all_exports = cqlite.__all__
        assert "CqliteError" in all_exports
        assert "SchemaError" in all_exports
        assert "QueryError" in all_exports
        assert "ParseError" in all_exports
