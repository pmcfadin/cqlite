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


class TestSharedErrorContract:
    """The shared FFI error contract (issue #1451).

    ``cqlite_ffi_common::error_contract`` is the ONE authoritative
    variant -> (python class, node code, category, recoverable, prefix) table.
    Before it, Python mapped by ``Error`` variant while Node derived its code
    from ``category()``, so the same core error had a different identity in each
    binding. These cases pin the Python half of the pinned rows; the Node half
    lives in ``bindings/node/__test__/error.test.js``.

    Each case goes through the PRODUCTION ``to_py_err`` path via the
    ``_raise_mapped_core_error`` probe, which is the only way to reach a variant
    (``Timeout``, ``Memory``) that no query can provoke.
    """

    # (core Error variant, expected Python exception class, expected Node code)
    #
    # The Node code is asserted in the Node suite; it is named here so the two
    # halves of the contract are readable in one place.
    PINNED_ROWS = [
        ("CqlParse", cqlite.ParseError, "PARSE"),
        ("InvalidInput", ValueError, "INVALID_INPUT"),
        ("Timeout", TimeoutError, "TIMEOUT"),
        ("Memory", MemoryError, "MEMORY"),
        ("Corruption", cqlite.CqliteError, "PARSE"),
    ]

    @pytest.mark.parametrize(
        "variant,expected_class,node_code",
        PINNED_ROWS,
        ids=[row[0] for row in PINNED_ROWS],
    )
    def test_pinned_row_raises_mapped_class(self, variant, expected_class, node_code):
        """The core variant surfaces as exactly the contract's Python class."""
        with pytest.raises(expected_class) as excinfo:
            cqlite._raise_mapped_core_error(variant)
        assert isinstance(excinfo.value, expected_class), (
            f"{variant} (Node code {node_code}) must raise "
            f"{expected_class.__name__}, got {type(excinfo.value).__name__}"
        )
        # The original core message survives the mapping.
        assert str(excinfo.value)

    def test_corruption_is_the_base_class_not_a_subclass(self):
        """``Corruption`` maps to the BASE ``CqliteError``.

        ``pytest.raises(CqliteError)`` alone would also pass for any subclass,
        so the base-class row is only really pinned by excluding them.
        """
        with pytest.raises(cqlite.CqliteError) as excinfo:
            cqlite._raise_mapped_core_error("Corruption")
        err = excinfo.value
        assert type(err) is cqlite.CqliteError
        assert not isinstance(err, (cqlite.SchemaError, cqlite.QueryError, cqlite.ParseError))

    def test_timeout_and_memory_keep_distinct_identities(self):
        """Issue #1451: these two used to collapse into the I/O identity.

        Python already raised ``TimeoutError``/``MemoryError`` while Node
        reported ``IO`` for both. Pinned by EXACT class here (note that Python's
        builtin ``TimeoutError`` is itself an ``OSError`` subclass, so an
        ``isinstance`` check against ``OSError`` could not tell them apart) so
        the shared row cannot regress toward the plain I/O identity.
        """
        exact = {"Timeout": TimeoutError, "Memory": MemoryError, "Io": OSError}
        for variant, expected in exact.items():
            with pytest.raises(Exception) as excinfo:
                cqlite._raise_mapped_core_error(variant)
            assert type(excinfo.value) is expected, (
                f"{variant} must raise exactly {expected.__name__}, "
                f"got {type(excinfo.value).__name__}"
            )

    def test_cql_parse_is_parse_error_not_query_error(self):
        """``CqlParse`` is a ParseError, and specifically not a QueryError."""
        with pytest.raises(cqlite.ParseError) as excinfo:
            cqlite._raise_mapped_core_error("CqlParse")
        assert not isinstance(excinfo.value, cqlite.QueryError)

    def test_unknown_variant_is_fail_closed(self):
        """An unrecognized variant name raises, never a substituted default."""
        with pytest.raises(ValueError, match="unknown core Error variant"):
            cqlite._raise_mapped_core_error("NoSuchVariant")

    def test_every_contract_variant_is_reachable(self):
        """Every variant named in the pinned rows resolves in the shared table.

        A row renamed in core (or dropped from the table) makes the probe raise
        ``ValueError`` for a name that is supposed to exist — which the pinned
        cases above would report as the WRONG exception class rather than as a
        missing row, so it is asserted explicitly here.
        """
        for variant, expected_class, _ in self.PINNED_ROWS:
            with pytest.raises(Exception) as excinfo:
                cqlite._raise_mapped_core_error(variant)
            assert not (
                isinstance(excinfo.value, ValueError)
                and "unknown core Error variant" in str(excinfo.value)
            ), f"contract row '{variant}' is missing from the shared table"
            assert isinstance(excinfo.value, expected_class)


class TestCqlParseErrorFromRealQuery:
    """End-to-end: a genuinely invalid CQL statement raises ``ParseError``.

    Exercises the wired path (``Database.execute`` -> core -> shared contract ->
    ``to_py_err``) rather than the probe. Uses the real dataset corpus; a
    present-but-empty dataset root FAILS LOUDLY under strict fixture mode via
    ``require_test_data`` instead of silently passing.
    """

    def test_truncated_cql_raises_parse_error(self, db):
        """A statement that reaches the CQL parser and fails there is ParseError.

        The statement must actually reach the parser: one whose first token is
        not a known verb is rejected earlier as a core ``QueryExecution`` error
        ("Unsupported query type"), which is a ``QueryError`` — asserted
        separately below so the two identities stay distinguishable.
        """
        with pytest.raises(cqlite.ParseError) as excinfo:
            db.execute("SELECT * FROM")
        assert not isinstance(excinfo.value, cqlite.QueryError)

    def test_unrecognized_statement_type_raises_query_error(self, db):
        """The other side of the fix: an unsupported statement type is a
        ``QueryError``, and must not borrow the parse identity."""
        with pytest.raises(cqlite.QueryError) as excinfo:
            db.execute("THIS IS NOT VALID CQL!!!")
        assert not isinstance(excinfo.value, cqlite.ParseError)
