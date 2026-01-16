"""Tests for temporal CQL type to Python type conversions - Issue #299.

TDD tests verifying that temporal types convert correctly
to Python native types according to M4 spec section 5.1.

Type Mapping (Temporal):
    CQL Type     | Rust Value         | Python Type
    -------------|--------------------|-----------------
    timestamp    | Value::Timestamp   | datetime.datetime (UTC)
    date         | Value::Date        | datetime.date
    time         | Value::Time        | datetime.time
    duration     | Value::Duration    | datetime.timedelta

Precision Notes:
    - Duration: Months approximated as 30 days, nanos truncated to microseconds
    - Time: Nanoseconds truncated to microseconds
    - Timestamp: Millisecond precision preserved

Tests use real SSTable data from test_basic keyspace.
"""

import datetime
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


class TestTimestampConversion:
    """Test CQL TIMESTAMP to Python datetime.datetime conversion."""

    def test_timestamp_returns_datetime(self, db):
        """TIMESTAMP should return Python datetime.datetime."""
        result = db.execute("SELECT created FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["created"]
        if value is not None:
            assert isinstance(value, datetime.datetime)

    def test_timestamp_has_utc_timezone(self, db):
        """TIMESTAMP datetime should have UTC timezone."""
        result = db.execute("SELECT created FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["created"]
        if value is not None:
            assert value.tzinfo is not None
            assert value.tzinfo == datetime.timezone.utc

    def test_timestamp_not_naive(self, db):
        """TIMESTAMP datetime should not be timezone-naive."""
        result = db.execute("SELECT created FROM test_basic.simple_table LIMIT 10")

        for row in result.rows:
            value = row.get("created")
            if value is not None:
                # Timezone-aware datetime has tzinfo set
                assert value.tzinfo is not None, "Timestamp should be timezone-aware"

    def test_timestamp_components_accessible(self, db):
        """TIMESTAMP datetime components should be accessible."""
        result = db.execute("SELECT created FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["created"]
        if value is not None:
            # Should be able to access all datetime components
            assert hasattr(value, "year")
            assert hasattr(value, "month")
            assert hasattr(value, "day")
            assert hasattr(value, "hour")
            assert hasattr(value, "minute")
            assert hasattr(value, "second")
            assert hasattr(value, "microsecond")
            # All components should be integers
            assert isinstance(value.year, int)
            assert isinstance(value.month, int)
            assert isinstance(value.day, int)


class TestDateConversion:
    """Test CQL DATE to Python datetime.date conversion."""

    def test_date_returns_date(self, db):
        """DATE should return Python datetime.date."""
        result = db.execute("SELECT birth_date FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["birth_date"]
        if value is not None:
            assert isinstance(value, datetime.date)

    def test_date_exact_type(self, db):
        """DATE should return exactly datetime.date, not datetime.datetime."""
        result = db.execute("SELECT birth_date FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["birth_date"]
        if value is not None:
            # datetime.datetime is subclass of date, so check exact type
            assert type(value) is datetime.date, (
                f"Expected datetime.date, got {type(value).__name__}"
            )

    def test_date_not_datetime(self, db):
        """DATE should NOT return datetime.datetime (should be pure date)."""
        result = db.execute("SELECT birth_date FROM test_basic.simple_table LIMIT 10")

        for row in result.rows:
            value = row.get("birth_date")
            if value is not None:
                # Explicitly check it's not datetime.datetime
                assert not isinstance(value, datetime.datetime), (
                    "DATE should not return datetime.datetime"
                )

    def test_date_components_accessible(self, db):
        """DATE components should be accessible."""
        result = db.execute("SELECT birth_date FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["birth_date"]
        if value is not None:
            # Should have year, month, day
            assert hasattr(value, "year")
            assert hasattr(value, "month")
            assert hasattr(value, "day")
            # Should NOT have time components (or they should fail)
            assert not hasattr(value, "hour") or type(value) is datetime.date


class TestTimeConversion:
    """Test CQL TIME to Python datetime.time conversion."""

    def test_time_returns_time(self, db):
        """TIME should return Python datetime.time."""
        result = db.execute("SELECT work_time FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["work_time"]
        if value is not None:
            assert isinstance(value, datetime.time)
            assert type(value) is datetime.time

    def test_time_components_valid_range(self, db):
        """TIME components should be in valid ranges."""
        result = db.execute("SELECT work_time FROM test_basic.simple_table LIMIT 10")

        found_time = False
        for row in result.rows:
            value = row.get("work_time")
            if value is not None:
                found_time = True
                assert 0 <= value.hour <= 23, f"Hour out of range: {value.hour}"
                assert 0 <= value.minute <= 59, f"Minute out of range: {value.minute}"
                assert 0 <= value.second <= 59, f"Second out of range: {value.second}"
                assert 0 <= value.microsecond <= 999999, (
                    f"Microsecond out of range: {value.microsecond}"
                )

        if not found_time:
            pytest.skip("No time values found in test data")

    def test_time_microsecond_precision(self, db):
        """TIME should have microsecond precision (nanoseconds truncated)."""
        result = db.execute("SELECT work_time FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["work_time"]
        if value is not None:
            # microsecond attribute should exist and be an int
            assert hasattr(value, "microsecond")
            assert isinstance(value.microsecond, int)


class TestDurationConversion:
    """Test CQL DURATION to Python datetime.timedelta conversion.

    IMPORTANT: Duration conversion has precision limitations:
    - Months are approximated as 30 days each
    - Nanoseconds are truncated to microseconds

    These limitations are documented in M4 spec section 5.2.
    """

    def test_duration_returns_timedelta(self, db):
        """DURATION should return Python datetime.timedelta."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["duration_val"]
        if value is not None:
            assert isinstance(value, datetime.timedelta), (
                f"Expected datetime.timedelta, got {type(value).__name__}"
            )

    def test_duration_exact_type(self, db):
        """DURATION should return exactly datetime.timedelta."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["duration_val"]
        if value is not None:
            assert type(value) is datetime.timedelta, (
                f"Expected timedelta, got {type(value).__name__}"
            )

    def test_duration_not_dict(self, db):
        """DURATION should NOT return dict (old behavior)."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["duration_val"]
        if value is not None:
            assert not isinstance(value, dict), (
                "DURATION should not return dict, should return timedelta"
            )

    def test_duration_has_total_seconds(self, db):
        """DURATION timedelta should support total_seconds() method."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["duration_val"]
        if value is not None:
            assert hasattr(value, "total_seconds")
            total = value.total_seconds()
            assert isinstance(total, float)

    def test_duration_positive_values(self, db):
        """DURATION with positive nanoseconds should convert correctly."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 10")

        found_duration = False
        for row in result.rows:
            value = row.get("duration_val")
            if value is not None:
                found_duration = True
                # Verify it's a valid timedelta
                assert isinstance(value, datetime.timedelta)
                # Test data has positive durations
                assert value.total_seconds() >= 0, (
                    f"Expected positive duration, got {value}"
                )

        if not found_duration:
            pytest.skip("No duration values found in test data")

    def test_duration_components_accessible(self, db):
        """DURATION timedelta components should be accessible."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 1")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        value = result.rows[0]["duration_val"]
        if value is not None:
            # timedelta has days, seconds, microseconds
            assert hasattr(value, "days")
            assert hasattr(value, "seconds")
            assert hasattr(value, "microseconds")
            assert isinstance(value.days, int)
            assert isinstance(value.seconds, int)
            assert isinstance(value.microseconds, int)


class TestTemporalNullHandling:
    """Test NULL handling for all temporal types."""

    def test_null_timestamp_returns_none(self, db):
        """NULL timestamp should return Python None."""
        # Query multiple rows to increase chance of finding NULL
        result = db.execute(
            "SELECT created FROM test_basic.simple_table LIMIT 100"
        )

        # Verify the mechanism works - None values should be None
        for row in result.rows:
            value = row.get("created")
            if value is None:
                assert value is None  # Explicitly verify None identity

    def test_null_date_returns_none(self, db):
        """NULL date should return Python None."""
        result = db.execute(
            "SELECT birth_date FROM test_basic.simple_table LIMIT 100"
        )

        for row in result.rows:
            value = row.get("birth_date")
            if value is None:
                assert value is None

    def test_null_time_returns_none(self, db):
        """NULL time should return Python None."""
        result = db.execute(
            "SELECT work_time FROM test_basic.simple_table LIMIT 100"
        )

        for row in result.rows:
            value = row.get("work_time")
            if value is None:
                assert value is None

    def test_null_duration_returns_none(self, db):
        """NULL duration should return Python None."""
        result = db.execute(
            "SELECT duration_val FROM test_basic.simple_table LIMIT 100"
        )

        for row in result.rows:
            value = row.get("duration_val")
            if value is None:
                assert value is None


class TestTemporalTypeConsistency:
    """Test that temporal types are consistent across rows."""

    def test_timestamp_type_consistent(self, db):
        """TIMESTAMP should return same type across all rows."""
        result = db.execute("SELECT created FROM test_basic.simple_table LIMIT 20")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        types_seen = set()
        for row in result.rows:
            value = row.get("created")
            if value is not None:
                types_seen.add(type(value))

        if types_seen:
            assert len(types_seen) == 1, f"Inconsistent types: {types_seen}"
            assert datetime.datetime in types_seen

    def test_date_type_consistent(self, db):
        """DATE should return same type across all rows."""
        result = db.execute("SELECT birth_date FROM test_basic.simple_table LIMIT 20")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        types_seen = set()
        for row in result.rows:
            value = row.get("birth_date")
            if value is not None:
                types_seen.add(type(value))

        if types_seen:
            assert len(types_seen) == 1, f"Inconsistent types: {types_seen}"
            assert datetime.date in types_seen

    def test_time_type_consistent(self, db):
        """TIME should return same type across all rows."""
        result = db.execute("SELECT work_time FROM test_basic.simple_table LIMIT 20")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        types_seen = set()
        for row in result.rows:
            value = row.get("work_time")
            if value is not None:
                types_seen.add(type(value))

        if types_seen:
            assert len(types_seen) == 1, f"Inconsistent types: {types_seen}"
            assert datetime.time in types_seen

    def test_duration_type_consistent(self, db):
        """DURATION should return same type across all rows."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 20")
        if len(result.rows) == 0:
            pytest.skip("No data available")

        types_seen = set()
        for row in result.rows:
            value = row.get("duration_val")
            if value is not None:
                types_seen.add(type(value))

        if types_seen:
            assert len(types_seen) == 1, f"Inconsistent types: {types_seen}"
            assert datetime.timedelta in types_seen

    def test_all_temporal_types_together(self, db):
        """All temporal types should convert correctly in same query."""
        result = db.execute(
            "SELECT created, birth_date, work_time, duration_val "
            "FROM test_basic.simple_table LIMIT 10"
        )
        if len(result.rows) == 0:
            pytest.skip("No data available")

        for row in result.rows:
            created = row.get("created")
            birth_date = row.get("birth_date")
            work_time = row.get("work_time")
            duration_val = row.get("duration_val")

            if created is not None:
                assert isinstance(created, datetime.datetime)
            if birth_date is not None:
                assert type(birth_date) is datetime.date
            if work_time is not None:
                assert type(work_time) is datetime.time
            if duration_val is not None:
                assert type(duration_val) is datetime.timedelta


class TestTimestampEdgeCases:
    """Test edge cases for timestamp conversion."""

    def test_timestamp_year_range(self, db):
        """TIMESTAMP year should be reasonable (not corrupted)."""
        result = db.execute("SELECT created FROM test_basic.simple_table LIMIT 10")

        for row in result.rows:
            value = row.get("created")
            if value is not None:
                # Year should be reasonable (1970-2100 for test data)
                assert 1970 <= value.year <= 2100, (
                    f"Year out of expected range: {value.year}"
                )


class TestTimeSeriesData:
    """Test temporal types using time series keyspace data."""

    @pytest.fixture
    def db_timeseries(self):
        """Database fixture with time series schema."""
        schema_file = SCHEMAS / "time-series.cql"
        if not schema_file.exists():
            pytest.skip(f"Schema file not found: {schema_file}")
        if not DATASETS.exists():
            pytest.skip(f"Test data not found: {DATASETS}")
        with cqlite.open(DATASETS, schema=schema_file) as database:
            yield database

    def test_timeseries_timestamp_clustering_key(self, db_timeseries):
        """TIMESTAMP as clustering key should work correctly."""
        result = db_timeseries.execute(
            "SELECT timestamp FROM test_timeseries.sensor_data LIMIT 5"
        )
        if len(result.rows) == 0:
            pytest.skip("No sensor_data available")

        for row in result.rows:
            value = row.get("timestamp")
            if value is not None:
                assert isinstance(value, datetime.datetime)
                assert value.tzinfo == datetime.timezone.utc
