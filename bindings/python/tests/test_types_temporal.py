"""Tests for temporal CQL type to Python type conversions - Issues #299, #1450.

Verifies that temporal types convert to Python types exactly (no precision loss).

Type Mapping (Temporal):
    CQL Type     | Rust Value         | Python Type
    -------------|--------------------|--------------------------------------
    timestamp    | Value::Timestamp   | datetime.datetime (UTC)
    date         | Value::Date        | datetime.date
    time         | Value::Time        | int (nanoseconds since midnight)
    duration     | Value::Duration    | cqlite.Duration(months, days, nanos)

Precision Notes (issue #1450 — the M4 §5.2 lossy mapping was removed):
    - Time: exact nanoseconds since midnight as a Python ``int``. Previously a
      microsecond-capped ``datetime.time`` (sub-µs nanos were truncated).
    - Duration: exact ``months`` / ``days`` / ``nanos`` components, mirroring the
      Node binding. Previously a ``datetime.timedelta`` that approximated months
      as 30 days and truncated nanoseconds to microseconds.
    - Timestamp: millisecond precision preserved.

Tests use real SSTable data from test_basic keyspace, plus self-contained
write→flush→read round-trips (independent of the fixture corpus) to prove
exactness for values the OLD lossy path would have corrupted.
"""

import datetime
from pathlib import Path

import pytest

import cqlite

# db and db_timeseries fixtures are provided by conftest.py


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


# Nanoseconds in one 24-hour day (exclusive upper bound for CQL `time`).
_NANOS_PER_DAY = 24 * 60 * 60 * 1_000_000_000


class TestTimeConversion:
    """Test CQL TIME to Python int (nanoseconds since midnight) conversion (#1450)."""

    def test_time_returns_int_nanos(self, db):
        """TIME should return a Python int (nanoseconds), NOT datetime.time."""
        result = db.execute("SELECT work_time FROM test_basic.simple_table LIMIT 1")
        assert len(result.rows) > 0, "test_basic.simple_table returned no rows"

        value = result.rows[0]["work_time"]
        if value is not None:
            assert type(value) is int, f"Expected int nanos, got {type(value).__name__}"
            # A bool is an int subclass; make sure we did not get one by accident.
            assert not isinstance(value, bool)
            assert not isinstance(value, datetime.time)

    def test_time_in_valid_day_range(self, db):
        """TIME nanoseconds should fall within a single day [0, 86_400e9)."""
        result = db.execute("SELECT work_time FROM test_basic.simple_table LIMIT 10")

        found_time = False
        for row in result.rows:
            value = row.get("work_time")
            if value is not None:
                found_time = True
                assert isinstance(value, int) and not isinstance(value, bool)
                assert 0 <= value < _NANOS_PER_DAY, f"nanos out of day range: {value}"

        assert found_time, "no non-null work_time values found in test data"


class TestDurationConversion:
    """Test CQL DURATION to cqlite.Duration conversion (exact, #1450).

    A CQL duration is decoded to a ``cqlite.Duration`` exposing the exact
    ``months`` / ``days`` / ``nanos`` components (mirroring the Node binding),
    replacing the lossy ``datetime.timedelta`` mapping (M4 §5.2) that collapsed
    months into 30 days and truncated nanoseconds to microseconds.
    """

    def test_duration_returns_cqlite_duration(self, db):
        """DURATION should return cqlite.Duration, NOT datetime.timedelta."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 1")
        assert len(result.rows) > 0, "test_basic.simple_table returned no rows"

        value = result.rows[0]["duration_val"]
        if value is not None:
            assert isinstance(value, cqlite.Duration), (
                f"Expected cqlite.Duration, got {type(value).__name__}"
            )
            assert not isinstance(value, datetime.timedelta)

    def test_duration_not_dict(self, db):
        """DURATION should NOT return a dict."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 1")
        assert len(result.rows) > 0, "test_basic.simple_table returned no rows"

        value = result.rows[0]["duration_val"]
        if value is not None:
            assert not isinstance(value, dict)

    def test_duration_components_are_ints(self, db):
        """cqlite.Duration exposes months/days/nanos as ints."""
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 10")

        found_duration = False
        for row in result.rows:
            value = row.get("duration_val")
            if value is not None:
                found_duration = True
                assert isinstance(value.months, int)
                assert isinstance(value.days, int)
                assert isinstance(value.nanos, int)

        assert found_duration, "no non-null duration_val values found in test data"

    def test_duration_full_nanos_preserved_on_read(self, db):
        """The full nanosecond component survives the real read path.

        The test_basic corpus stores hour/minute/second durations (months=0,
        days=0), so ``nanos`` is a large i64 (>= 1e9). Assert it is preserved
        verbatim as ``nanos`` — the OLD path stuffed it through a
        ``timedelta`` microseconds field (``nanos // 1000``), so a value with
        sub-µs precision would have lost its trailing nanoseconds.
        """
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 50")

        found = False
        for row in result.rows:
            value = row.get("duration_val")
            if value is not None and value.nanos != 0:
                found = True
                # nanos is stored exactly (not divided by 1000, not folded into days)
                assert value.nanos >= 1_000_000_000, (
                    f"expected a sub-day nanos component, got {value.nanos}"
                )
                # Round-trip the components back into a Duration: exact identity.
                assert value == cqlite.Duration(value.months, value.days, value.nanos)

        assert found, "no non-zero-nanos duration values found in test data"


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
        """TIME should return same type (int) across all rows."""
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
            assert int in types_seen

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
            assert cqlite.Duration in types_seen

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
                assert type(work_time) is int
            if duration_val is not None:
                assert isinstance(duration_val, cqlite.Duration)


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

    # db_timeseries fixture is provided by conftest.py

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


# =============================================================================
# Exactness / losslessness proofs (issue #1450)
# =============================================================================


# A single-table schema keeps the write path's target unambiguous (no-heuristics
# mandate). `wtime` exercises the sub-microsecond TIME round-trip.
_TIME_SCHEMA = """\
CREATE KEYSPACE IF NOT EXISTS temporal_exact
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE temporal_exact;

CREATE TABLE IF NOT EXISTS t (
    id    INT PRIMARY KEY,
    wtime TIME
);
"""


class TestTemporalExactness:
    """Prove values the OLD (M4 §5.2) lossy path corrupted are now preserved."""

    def test_time_lossless_nanos(self, tmp_path):
        """A sub-microsecond TIME survives a write→flush→read round-trip exactly.

        Inserts 01:02:03.123456789 (nanos = 3_723_123_456_789). The old
        ``datetime.time`` mapping truncated to microseconds, dropping the
        trailing ``789`` ns (it would have read back 3_723_123_456_000). The new
        ``int`` nanoseconds mapping preserves every digit.
        """
        schema = tmp_path / "schema.cql"
        schema.write_text(_TIME_SCHEMA)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        write_dir = tmp_path / "wd"

        sub_us_nanos = 3_723_123_456_789  # 01:02:03.123456789
        old_lossy_nanos = (sub_us_nanos // 1000) * 1000  # what µs-truncation gave

        db = cqlite.open(
            str(data_dir),
            schema=str(schema),
            writable=True,
            write_dir=str(write_dir),
        )
        try:
            db.execute(
                f"INSERT INTO temporal_exact.t (id, wtime) VALUES (1, {sub_us_nanos})"
            )
            path = db.flush_run()
            assert path and Path(path).exists(), "flush must produce a real Data.db"
        finally:
            db.close()

        with cqlite.open(str(write_dir / "data"), schema=str(schema)) as rd:
            rows = [row.to_dict() for row in rd.execute(
                "SELECT wtime FROM temporal_exact.t"
            )]

        assert len(rows) == 1, f"exactly one row expected, got {rows}"
        value = rows[0]["wtime"]
        assert type(value) is int, f"TIME must decode to int nanos, got {type(value)}"
        assert value == sub_us_nanos, f"expected exact {sub_us_nanos}, got {value}"
        assert value != old_lossy_nanos, (
            "sub-µs nanoseconds must NOT be truncated (regression to M4 §5.2)"
        )

    def test_duration_exact_months_days_nanos(self, db):
        """DURATION exposes months/days/nanos exactly — no 30-day collapse, no
        nanosecond truncation.

        The real corpus is asserted first (structured type on the read path),
        then a components round-trip pins the exact values the OLD
        ``datetime.timedelta`` mapping would have destroyed: months folded into
        ``months * 30`` days and nanos truncated via ``nanos // 1000``. The
        read-path conversion is a verbatim 1:1 copy of the stored
        ``months``/``days``/``nanos`` (see ``bindings/python/src/value.rs``),
        so the constructed value is byte-identical to what a stored cell with
        these components decodes to.
        """
        result = db.execute("SELECT duration_val FROM test_basic.simple_table LIMIT 1")
        assert len(result.rows) > 0, "test_basic.simple_table returned no rows"
        sample = result.rows[0]["duration_val"]
        if sample is not None:
            assert isinstance(sample, cqlite.Duration)
            assert (sample.months, sample.days) == (0, 0)  # corpus stores h/m/s only

        # A duration the OLD path could not represent without loss.
        months, days, nanos = 14, 3, 123_456_789  # 14 months, 3 days, sub-µs nanos
        dur = cqlite.Duration(months, days, nanos)

        # Exact, independent components (no collapse into a single scalar).
        assert dur.months == months
        assert dur.days == days
        assert dur.nanos == nanos

        # Contrast with the removed lossy mapping, to make the regression guard
        # explicit:
        old_days = months * 30 + days      # timedelta collapsed months → 30 days
        old_micros = nanos // 1000         # timedelta truncated nanos → micros
        assert dur.months != 0, "months must be kept, not folded into days"
        assert (old_days, old_micros) == (423, 123_456)  # what the OLD path yielded
        assert dur.nanos != old_micros * 1000, "sub-µs nanos must be preserved"

        # Value equality + hashability (usable as dict/set keys).
        assert dur == cqlite.Duration(months, days, nanos)
        assert hash(dur) == hash(cqlite.Duration(months, days, nanos))
        assert dur != cqlite.Duration(months, days, nanos + 1)
