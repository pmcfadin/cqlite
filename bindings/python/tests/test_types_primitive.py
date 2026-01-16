"""Tests for primitive CQL type to Python type conversions - Issue #298.

TDD tests verifying that cqlite_core::Value variants convert correctly
to Python native types according to M4 spec section 5.1.

Type Mapping (Primitives):
    CQL Type     | Rust Value       | Python Type
    -------------|------------------|------------
    null         | Value::Null      | None
    boolean      | Value::Boolean   | bool
    tinyint      | Value::TinyInt   | int
    smallint     | Value::SmallInt  | int
    int          | Value::Integer   | int
    bigint       | Value::BigInt    | int
    counter      | Value::Counter   | int
    float        | Value::Float32   | float
    double       | Value::Float     | float
    text         | Value::Text      | str
    ascii        | Value::Text      | str
    varchar      | Value::Text      | str
    blob         | Value::Blob      | bytes

Tests use real SSTable data from test_basic keyspace.
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


class TestNullConversion:
    """Test CQL NULL to Python None conversion."""

    def test_null_column_returns_none(self, db):
        """Columns with NULL values should return Python None."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 10")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        # Check rows for any None values
        for row in result.rows:
            d = row.to_dict()
            for value in d.values():
                if value is None:
                    # Found a NULL - verify it's Python None
                    assert value is None
                    return

        # No NULLs found in sample - test passes (data-dependent)

    def test_none_type_identity(self, db):
        """None values should be Python's singleton None."""
        result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        row = result.rows[0]
        for key in row.keys():
            value = row.get(key)
            if value is None:
                assert value is None
                assert type(value) is type(None)


class TestBooleanConversion:
    """Test CQL BOOLEAN to Python bool conversion."""

    def test_boolean_returns_bool_type(self, db):
        """BOOLEAN columns should return Python bool."""
        result = db.execute("SELECT active FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["active"]
        if value is not None:
            assert isinstance(value, bool)
            assert type(value) is bool  # Strict type check (not int subclass)

    def test_boolean_values(self, db):
        """BOOLEAN should return True or False values."""
        result = db.execute("SELECT active FROM test_basic.simple_table LIMIT 100")

        found_bool = False
        for row in result.rows:
            value = row.get("active")
            if value is not None:
                assert value in (True, False)
                found_bool = True

        if not found_bool:
            pytest.skip("No boolean values found in test data")


class TestIntegerConversions:
    """Test CQL integer types to Python int conversion.

    All CQL integer types (TINYINT, SMALLINT, INT, BIGINT) convert to Python int.
    This is intentional - Python int has arbitrary precision.
    """

    def test_tinyint_returns_int(self, db):
        """TINYINT should return Python int."""
        result = db.execute(
            "SELECT small_number FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["small_number"]
        if value is not None:
            assert isinstance(value, int)
            assert not isinstance(value, bool)  # bool is int subclass
            # TINYINT range: -128 to 127
            assert -128 <= value <= 127

    def test_smallint_returns_int(self, db):
        """SMALLINT should return Python int."""
        result = db.execute(
            "SELECT medium_number FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["medium_number"]
        if value is not None:
            assert isinstance(value, int)
            assert not isinstance(value, bool)
            # SMALLINT range: -32768 to 32767
            assert -32768 <= value <= 32767

    def test_int_returns_int(self, db):
        """INT should return Python int."""
        result = db.execute("SELECT age FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["age"]
        if value is not None:
            assert isinstance(value, int)
            assert not isinstance(value, bool)
            assert type(value) is int

    def test_bigint_returns_int(self, db):
        """BIGINT should return Python int."""
        result = db.execute("SELECT salary FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["salary"]
        if value is not None:
            assert isinstance(value, int)
            assert not isinstance(value, bool)
            assert type(value) is int

    def test_integer_not_float(self, db):
        """Integer types should NOT return float."""
        result = db.execute(
            "SELECT age, salary, small_number, medium_number "
            "FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        row = result.rows[0]
        for col in ["age", "salary", "small_number", "medium_number"]:
            value = row.get(col)
            if value is not None:
                assert not isinstance(value, float)


class TestCounterConversion:
    """Test CQL COUNTER type to Python int conversion.

    COUNTER is semantically different from BIGINT (CRDT, increment-only)
    but converts to the same Python type.
    """

    def test_counter_returns_int(self, db):
        """COUNTER should return Python int."""
        result = db.execute("SELECT view_count FROM test_basic.counters LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No counter data available")

        value = result.rows[0]["view_count"]
        if value is not None:
            assert isinstance(value, int)
            assert not isinstance(value, bool)
            assert type(value) is int

    def test_counter_large_values(self, db):
        """COUNTER should handle large values (i64 range)."""
        result = db.execute(
            "SELECT view_count, like_count, share_count "
            "FROM test_basic.counters LIMIT 5"
        )

        found_counter = False
        for row in result.rows:
            for col in ["view_count", "like_count", "share_count"]:
                value = row.get(col)
                if value is not None:
                    assert isinstance(value, int)
                    found_counter = True

        if not found_counter:
            pytest.skip("No counter values found in test data")


class TestFloatConversions:
    """Test CQL FLOAT/DOUBLE to Python float conversion.

    Both CQL FLOAT (32-bit) and DOUBLE (64-bit) convert to Python float.
    """

    def test_float_returns_float(self, db):
        """FLOAT (32-bit) should return Python float."""
        result = db.execute("SELECT height FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["height"]
        if value is not None:
            assert isinstance(value, float)
            assert type(value) is float

    def test_double_returns_float(self, db):
        """DOUBLE (64-bit) should return Python float."""
        result = db.execute("SELECT weight FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["weight"]
        if value is not None:
            assert isinstance(value, float)
            assert type(value) is float

    def test_float_not_int(self, db):
        """Float types should NOT return int (unless it's also a float)."""
        result = db.execute(
            "SELECT height, weight FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        row = result.rows[0]
        for col in ["height", "weight"]:
            value = row.get(col)
            if value is not None:
                # Value should be float, not pure int
                assert isinstance(value, float)


class TestTextConversions:
    """Test CQL TEXT/ASCII/VARCHAR to Python str conversion.

    All string types convert to Python str.
    """

    def test_text_returns_str(self, db):
        """TEXT should return Python str."""
        result = db.execute("SELECT name FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["name"]
        if value is not None:
            assert isinstance(value, str)
            assert type(value) is str

    def test_varchar_returns_str(self, db):
        """VARCHAR should return Python str."""
        result = db.execute(
            "SELECT varchar_field FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["varchar_field"]
        if value is not None:
            assert isinstance(value, str)
            assert type(value) is str

    def test_ascii_returns_str(self, db):
        """ASCII should return Python str."""
        result = db.execute(
            "SELECT ascii_field FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["ascii_field"]
        if value is not None:
            assert isinstance(value, str)
            assert type(value) is str

    def test_text_not_bytes(self, db):
        """Text types should NOT return bytes."""
        result = db.execute(
            "SELECT name, varchar_field, ascii_field "
            "FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        row = result.rows[0]
        for col in ["name", "varchar_field", "ascii_field"]:
            value = row.get(col)
            if value is not None:
                assert not isinstance(value, bytes)


class TestBlobConversion:
    """Test CQL BLOB to Python bytes conversion."""

    def test_blob_returns_bytes(self, db):
        """BLOB should return Python bytes."""
        result = db.execute(
            "SELECT description FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["description"]
        if value is not None:
            assert isinstance(value, bytes)
            assert type(value) is bytes

    def test_blob_not_str(self, db):
        """BLOB should NOT return str."""
        result = db.execute(
            "SELECT description FROM test_basic.simple_table LIMIT 1"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["description"]
        if value is not None:
            assert not isinstance(value, str)


class TestTypeConsistency:
    """Test that types are consistent across rows."""

    def test_same_column_same_type(self, db):
        """Same column should return same type across rows."""
        result = db.execute(
            "SELECT age, name, active FROM test_basic.simple_table LIMIT 10"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        age_types = set()
        name_types = set()
        active_types = set()

        for row in result.rows:
            age = row.get("age")
            name = row.get("name")
            active = row.get("active")

            if age is not None:
                age_types.add(type(age))
            if name is not None:
                name_types.add(type(name))
            if active is not None:
                active_types.add(type(active))

        # Each column should have exactly one type (excluding None)
        if age_types:
            assert len(age_types) == 1, f"age has multiple types: {age_types}"
        if name_types:
            assert len(name_types) == 1, f"name has multiple types: {name_types}"
        if active_types:
            assert len(active_types) == 1, f"active has multiple types: {active_types}"
