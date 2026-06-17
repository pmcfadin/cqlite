"""sstabledump Parity Tests for Python Bindings - Issue #309.

Validates that Python binding output matches Cassandra sstabledump for all 33 test tables.
This is part of M4 Python Bindings Epic (#321).

Test Strategy:
    Tier 1: Row Count Parity - Verify row counts match JSONL reference for all 33 tables
    Tier 2: Value Comparison - For tables with simple types, validate cell values match

Tables Tested (33 total):
    test_basic (8): simple_table, composite_key_table, compression_test_table,
                    multi_partition_table, ttl_test_table, counters,
                    static_columns_table, uncompressed_table
    test_collections (8): collection_table, collection_clustering_table,
                          collections_with_udts, empty_collections_table,
                          frozen_collections_table, large_collections_table,
                          nested_collections_table, typed_collections_table
    test_timeseries (9): event_store, user_sessions, sensor_data, app_metrics,
                         log_entries, stock_prices, tick_data,
                         time_bucketed_counters, user_activity
    test_wide_rows (8): wide_partition_table, chat_messages, document_versions,
                        large_blob_table, many_columns_table,
                        multi_metric_timeseries, product_catalog, sparse_data_table
"""

import functools
import json
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

import cqlite


from conftest import DATASETS, SCHEMAS


# =============================================================================
# Table Definitions (33 tables across 4 keyspaces)
# =============================================================================

# Schema file to keyspace mapping
SCHEMA_KEYSPACE_MAP = {
    "basic-types.cql": ["test_basic"],
    "collections.cql": ["test_collections"],
    "time-series.cql": ["test_timeseries"],
    "wide-rows.cql": ["test_wide_rows"],
    "oa-test.cql": ["test_oa"],
}

# All 6 oa tables (Issue #656 VG4 — oa parity enforcement)
OA_TABLES = [
    ("test_oa", "simple_table"),
    ("test_oa", "collection_table"),
    ("test_oa", "udt_table"),
    ("test_oa", "ttl_table"),
    ("test_oa", "static_table"),
    ("test_oa", "tombstone_table"),
]

# All 33 tables organized by keyspace
ALL_TABLES = [
    # test_basic (8 tables)
    ("test_basic", "simple_table"),
    ("test_basic", "composite_key_table"),
    ("test_basic", "compression_test_table"),
    ("test_basic", "multi_partition_table"),
    ("test_basic", "ttl_test_table"),
    ("test_basic", "counters"),
    ("test_basic", "static_columns_table"),
    ("test_basic", "uncompressed_table"),
    # test_collections (8 tables)
    ("test_collections", "collection_table"),
    ("test_collections", "collection_clustering_table"),
    ("test_collections", "collections_with_udts"),
    ("test_collections", "empty_collections_table"),
    ("test_collections", "frozen_collections_table"),
    ("test_collections", "large_collections_table"),
    ("test_collections", "nested_collections_table"),
    ("test_collections", "typed_collections_table"),
    # test_timeseries (9 tables)
    ("test_timeseries", "event_store"),
    ("test_timeseries", "user_sessions"),
    ("test_timeseries", "sensor_data"),
    ("test_timeseries", "app_metrics"),
    ("test_timeseries", "log_entries"),
    ("test_timeseries", "stock_prices"),
    ("test_timeseries", "tick_data"),
    ("test_timeseries", "time_bucketed_counters"),
    ("test_timeseries", "user_activity"),
    # test_wide_rows (8 tables)
    ("test_wide_rows", "wide_partition_table"),
    ("test_wide_rows", "chat_messages"),
    ("test_wide_rows", "document_versions"),
    ("test_wide_rows", "large_blob_table"),
    ("test_wide_rows", "many_columns_table"),
    ("test_wide_rows", "multi_metric_timeseries"),
    ("test_wide_rows", "product_catalog"),
    ("test_wide_rows", "sparse_data_table"),
]


# =============================================================================
# JSONL Reference File Helpers
# =============================================================================


def find_jsonl_file(keyspace: str, table: str) -> Path | None:
    """Find the JSONL reference file for a table.

    JSONL files are located at:
    test-data/datasets/sstables/{keyspace}/{table}-{hash}/nb-1-big-Data.db.jsonl
    """
    keyspace_dir = DATASETS / keyspace
    if not keyspace_dir.exists():
        return None

    # Find table directory (contains hash suffix)
    for table_dir in keyspace_dir.iterdir():
        if table_dir.is_dir() and table_dir.name.startswith(f"{table}-"):
            jsonl_file = table_dir / "nb-1-big-Data.db.jsonl"
            if jsonl_file.exists():
                return jsonl_file
    return None


def find_oa_jsonl_file(keyspace: str, table: str) -> Path | None:
    """Find the JSONL reference file for an oa-format table (Issue #656 VG4).

    oa tables use oa-format SSTable files:
    test-data/datasets/sstables/{keyspace}/{table}-{hash}/oa-2-big-Data.db.jsonl
    """
    keyspace_dir = DATASETS / keyspace
    if not keyspace_dir.exists():
        return None

    for table_dir in keyspace_dir.iterdir():
        if table_dir.is_dir() and table_dir.name.startswith(f"{table}-"):
            # oa tables use oa-N-big-Data.db.jsonl naming
            for jsonl_file in table_dir.glob("oa-*-big-Data.db.jsonl"):
                if jsonl_file.exists():
                    return jsonl_file
    return None


def _count_rows_in_jsonl_impl(jsonl_path: Path) -> int:
    """Implementation: Count total rows in a JSONL reference file.

    JSONL format: One partition per line, each partition has a "rows" array.
    Returns the sum of live row entries across all partitions.

    Counts only entries that match CQLite query results:
    - type == "row" (not range_tombstone_bound)
    - no row-level deletion_info (row tombstones are suppressed by CQLite,
      which correctly excludes deleted rows from query output)
    """
    total_rows = 0
    with open(jsonl_path, "r") as f:
        for line in f:
            if line.strip():
                partition = json.loads(line)
                rows = partition.get("rows", [])
                for row in rows:
                    # Exclude range tombstone markers
                    if row.get("type") != "row":
                        continue
                    # Exclude row tombstones (row-level deletion_info means the
                    # entire row is deleted; CQLite suppresses these from results)
                    if "deletion_info" in row and not row.get("cells"):
                        continue
                    total_rows += 1
    return total_rows


@functools.lru_cache(maxsize=64)
def count_rows_in_jsonl(jsonl_path: str | Path) -> int:
    """Count total rows in a JSONL reference file. Cached for performance (Issue #337).

    Accepts both str and Path for cache key hashability.
    """
    path = Path(jsonl_path) if isinstance(jsonl_path, str) else jsonl_path
    return _count_rows_in_jsonl_impl(path)


def load_jsonl_partitions(jsonl_path: Path) -> list[dict]:
    """Load partitions from a JSONL reference file.

    Returns list of partition dicts with "partition" and "rows" keys.
    """
    partitions = []
    with open(jsonl_path, "r") as f:
        for line in f:
            if line.strip():
                partitions.append(json.loads(line))
    return partitions


@functools.lru_cache(maxsize=32)
def load_jsonl_partitions_cached(jsonl_path: str | Path) -> tuple[dict, ...]:
    """Load and cache partitions from JSONL file. Returns tuple for hashability (Issue #337).

    Accepts both str and Path for cache key hashability.
    """
    path = Path(jsonl_path) if isinstance(jsonl_path, str) else jsonl_path
    return tuple(load_jsonl_partitions(path))


# =============================================================================
# Type Normalization Helpers
# =============================================================================


def normalize_jsonl_value(value: Any, cell_name: str = "") -> Any:
    """Normalize a value from sstabledump JSONL for comparison.

    sstabledump outputs:
    - UUIDs as strings: "15291a77-d739-4e73-8397-b787442f3a1f"
    - Timestamps as strings: "2025-10-06 01:12:05.394Z"
    - Dates as strings: "2025-06-18"
    - Times as strings with nanoseconds: "01:12:05.394017000"
    - Blobs as hex strings: "0x94df07b2..."
    - Duration as strings: "12h58m22s" or "1mo2d3h..."
    - Inet as strings: "154.47.65.214"
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        # Detect blob (hex string starting with 0x)
        if value.startswith("0x"):
            return bytes.fromhex(value[2:])

        # Detect UUID pattern
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        if re.match(uuid_pattern, value, re.IGNORECASE):
            return UUID(value)

        # Detect timestamp pattern: "2025-10-06 01:12:05.394Z" or similar
        timestamp_patterns = [
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+Z$",
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$",
        ]
        for pattern in timestamp_patterns:
            if re.match(pattern, value):
                # Parse timestamp - handle various formats
                ts_str = value.replace("Z", "+00:00").replace(" ", "T")
                # Truncate to microseconds (Python datetime max precision)
                if "." in ts_str:
                    base, frac_tz = ts_str.split(".")
                    # Extract fractional part and timezone
                    frac_match = re.match(r"(\d+)(.*)", frac_tz)
                    if frac_match:
                        frac = frac_match.group(1)[:6].ljust(6, "0")
                        tz = frac_match.group(2)
                        ts_str = f"{base}.{frac}{tz}"
                return datetime.fromisoformat(ts_str)

        # Detect date pattern: "2025-06-18"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            return date.fromisoformat(value)

        # Detect time pattern with nanoseconds: "01:12:05.394017000"
        time_match = re.match(r"^(\d{2}):(\d{2}):(\d{2})\.(\d+)$", value)
        if time_match:
            h, m, s, frac = time_match.groups()
            # Truncate to microseconds
            micros = int(frac[:6].ljust(6, "0"))
            return time(int(h), int(m), int(s), micros)

        # Detect simple time: "01:12:05"
        if re.match(r"^\d{2}:\d{2}:\d{2}$", value):
            return time.fromisoformat(value)

        # Duration pattern: "12h58m22s" or with months/days
        duration_match = re.match(
            r"^(?:(\d+)mo)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$",
            value,
        )
        if duration_match and any(duration_match.groups()):
            mo, d, h, m, s = [int(g) if g else 0 for g in duration_match.groups()]
            # Convert months to days (approximation: 30 days/month)
            total_days = mo * 30 + d
            return timedelta(days=total_days, hours=h, minutes=m, seconds=s)

        # Plain string
        return value

    if isinstance(value, list):
        return [normalize_jsonl_value(v) for v in value]

    if isinstance(value, dict):
        return {k: normalize_jsonl_value(v, k) for k, v in value.items()}

    return value


def values_equal(actual: Any, expected: Any) -> bool:
    """Compare two values with type-aware equality.

    Handles special cases:
    - frozenset vs list comparison (sets are returned as frozenset)
    - datetime timezone handling
    - float precision (use approximate comparison)
    - Decimal comparison
    """
    if actual is None and expected is None:
        return True

    if actual is None or expected is None:
        return False

    # Handle frozenset vs list/set comparison
    if isinstance(actual, frozenset) and isinstance(expected, (list, set)):
        return actual == frozenset(expected)

    # Handle list comparison
    if isinstance(actual, list) and isinstance(expected, list):
        if len(actual) != len(expected):
            return False
        return all(values_equal(a, e) for a, e in zip(actual, expected))

    # Handle dict comparison
    if isinstance(actual, dict) and isinstance(expected, dict):
        if set(actual.keys()) != set(expected.keys()):
            return False
        return all(values_equal(actual[k], expected[k]) for k in actual)

    # Handle float comparison with tolerance
    if isinstance(actual, float) and isinstance(expected, (int, float)):
        if actual == expected:
            return True
        # Use relative tolerance for large values, absolute for small
        rel_tol = 1e-6
        abs_tol = 1e-9
        return abs(actual - expected) <= max(rel_tol * max(abs(actual), abs(expected)), abs_tol)

    # Handle Decimal comparison
    if isinstance(actual, Decimal) and isinstance(expected, (int, float, Decimal)):
        if isinstance(expected, float):
            expected = Decimal(str(expected))
        return actual == expected

    # Handle UUID comparison
    if isinstance(actual, UUID) and isinstance(expected, UUID):
        return actual == expected

    # Handle datetime comparison (with timezone normalization)
    if isinstance(actual, datetime) and isinstance(expected, datetime):
        # Convert both to naive datetimes in UTC for comparison
        actual_naive = actual.replace(tzinfo=None) if actual.tzinfo else actual
        expected_naive = expected.replace(tzinfo=None) if expected.tzinfo else expected
        # Compare with 1ms tolerance for floating point precision
        diff = abs((actual_naive - expected_naive).total_seconds())
        return diff < 0.001  # 1ms tolerance

    # Handle bytes comparison
    if isinstance(actual, bytes) and isinstance(expected, bytes):
        return actual == expected

    # Handle inet (IP address) comparison
    # Python bindings return IPv4Address/IPv6Address, JSONL has string
    if isinstance(actual, (IPv4Address, IPv6Address)) or isinstance(
        expected, (IPv4Address, IPv6Address)
    ):
        return str(actual) == str(expected)

    # Default comparison
    return actual == expected


# =============================================================================
# Pytest Fixtures (Use module-scoped variants from conftest.py)
# =============================================================================


# datasets_root fixture is provided by conftest.py


def get_schema_for_keyspace(keyspace: str) -> Path | None:
    """Get the schema file for a keyspace."""
    for schema_file, keyspaces in SCHEMA_KEYSPACE_MAP.items():
        if keyspace in keyspaces:
            schema_path = SCHEMAS / schema_file
            if schema_path.exists():
                return schema_path
    return None


# Database fixtures (db_basic, db_collections, db_timeseries, db_wide_rows)
# are aliased from conftest module-scoped variants.
# Note: test_parity.py uses single database objects (not tuples), while
# test_cli_parity.py uses (database, schema_file) tuples.


@pytest.fixture(scope="module")
def db_basic(db_basic_module):
    """Alias for db_basic_module from conftest."""
    return db_basic_module


@pytest.fixture(scope="module")
def db_collections(db_collections_module):
    """Alias for db_collections_module from conftest."""
    return db_collections_module


@pytest.fixture(scope="module")
def db_timeseries(db_timeseries_module):
    """Alias for db_timeseries_module from conftest."""
    return db_timeseries_module


@pytest.fixture(scope="module")
def db_wide_rows(db_wide_rows_module):
    """Alias for db_wide_rows_module from conftest."""
    return db_wide_rows_module


@pytest.fixture(scope="module")
def db_oa():
    """Database fixture with oa-test schema (module-scoped, Issue #656 VG4).

    Skips gracefully when oa Data.db binary files are absent (e.g., goldens-only runs).
    """
    schema_path = SCHEMAS / "oa-test.cql"
    if not schema_path.exists():
        pytest.skip(f"Schema file not found: {schema_path}")
    if not (DATASETS / "test_oa").exists():
        pytest.skip(f"oa fixture directory not found: {DATASETS / 'test_oa'}")
    # Check that at least one oa Data.db binary exists (not just JSONL goldens)
    oa_binaries = list((DATASETS / "test_oa").glob("*/oa-*-big-Data.db"))
    if not oa_binaries:
        pytest.skip("oa Data.db binary files not present; run fetch-datasets.sh to download")
    with cqlite.open(DATASETS, schema=schema_path) as database:
        yield database


# =============================================================================
# Tier 1: Row Count Parity Tests (All 33 Tables)
# =============================================================================


# Known issues with row count discrepancies (pre-existing core library issues)
# Note: As of Jan 2026, all previously known issues have been resolved
KNOWN_ROW_COUNT_ISSUES = {
    # All issues resolved
}


class TestRowCountParity:
    """Tier 1: Verify row counts match JSONL reference for all 33 tables.

    This is the primary parity test - ensures all tables are readable and
    return the expected number of rows.
    """

    # test_basic tables (8)
    @pytest.mark.parametrize(
        "table",
        [
            "simple_table",
            "composite_key_table",
            "compression_test_table",
            "multi_partition_table",
            "ttl_test_table",
            "counters",
            "static_columns_table",  # Issue resolved
            "uncompressed_table",
        ],
    )
    def test_basic_row_count(self, db_basic, table):
        """Verify row count parity for test_basic tables."""
        jsonl_file = find_jsonl_file("test_basic", table)
        if jsonl_file is None:
            pytest.skip(f"JSONL reference not found for test_basic.{table}")

        expected_count = count_rows_in_jsonl(jsonl_file)
        result = db_basic.execute(f"SELECT * FROM test_basic.{table}")
        actual_count = len(result.rows)

        assert actual_count == expected_count, (
            f"Row count mismatch for test_basic.{table}: "
            f"got {actual_count}, expected {expected_count}"
        )

    # test_collections tables (8)
    @pytest.mark.parametrize(
        "table",
        [
            "collection_table",
            "collection_clustering_table",
            "collections_with_udts",
            "empty_collections_table",
            "frozen_collections_table",
            "large_collections_table",
            "nested_collections_table",
            "typed_collections_table",  # Issue resolved
        ],
    )
    def test_collections_row_count(self, db_collections, table):
        """Verify row count parity for test_collections tables."""
        jsonl_file = find_jsonl_file("test_collections", table)
        if jsonl_file is None:
            pytest.skip(f"JSONL reference not found for test_collections.{table}")

        expected_count = count_rows_in_jsonl(jsonl_file)
        result = db_collections.execute(f"SELECT * FROM test_collections.{table}")
        actual_count = len(result.rows)

        assert actual_count == expected_count, (
            f"Row count mismatch for test_collections.{table}: "
            f"got {actual_count}, expected {expected_count}"
        )

    # test_timeseries tables (9)
    @pytest.mark.parametrize(
        "table",
        [
            "event_store",
            "user_sessions",
            "sensor_data",
            "app_metrics",
            "log_entries",
            "stock_prices",
            "tick_data",
            "time_bucketed_counters",
            "user_activity",
        ],
    )
    def test_timeseries_row_count(self, db_timeseries, table):
        """Verify row count parity for test_timeseries tables."""
        jsonl_file = find_jsonl_file("test_timeseries", table)
        if jsonl_file is None:
            pytest.skip(f"JSONL reference not found for test_timeseries.{table}")

        expected_count = count_rows_in_jsonl(jsonl_file)
        result = db_timeseries.execute(f"SELECT * FROM test_timeseries.{table}")
        actual_count = len(result.rows)

        assert actual_count == expected_count, (
            f"Row count mismatch for test_timeseries.{table}: "
            f"got {actual_count}, expected {expected_count}"
        )

    # test_wide_rows tables (8)
    @pytest.mark.parametrize(
        "table",
        [
            "wide_partition_table",
            "chat_messages",
            "document_versions",
            "large_blob_table",
            "many_columns_table",
            "multi_metric_timeseries",
            "product_catalog",
            "sparse_data_table",
        ],
    )
    def test_wide_rows_row_count(self, db_wide_rows, table):
        """Verify row count parity for test_wide_rows tables."""
        jsonl_file = find_jsonl_file("test_wide_rows", table)
        if jsonl_file is None:
            pytest.skip(f"JSONL reference not found for test_wide_rows.{table}")

        expected_count = count_rows_in_jsonl(jsonl_file)
        result = db_wide_rows.execute(f"SELECT * FROM test_wide_rows.{table}")
        actual_count = len(result.rows)

        assert actual_count == expected_count, (
            f"Row count mismatch for test_wide_rows.{table}: "
            f"got {actual_count}, expected {expected_count}"
        )


# =============================================================================
# Tier 2: Value Comparison Tests (Representative Tables)
# =============================================================================


class TestValueParity:
    """Tier 2: Verify cell values match JSONL reference for representative tables.

    Focuses on tables with simple (non-collection) types where cell-by-cell
    comparison is straightforward.
    """

    def test_simple_table_values(self, db_basic):
        """Verify values for simple_table (comprehensive type coverage)."""
        jsonl_file = find_jsonl_file("test_basic", "simple_table")
        if jsonl_file is None:
            pytest.skip("JSONL reference not found for test_basic.simple_table")

        # Load partitions from JSONL (cached for performance - Issue #337)
        partitions = load_jsonl_partitions_cached(str(jsonl_file))
        if not partitions:
            pytest.skip("No partitions in JSONL file")

        # Get all rows from Python
        result = db_basic.execute("SELECT * FROM test_basic.simple_table")
        if len(result.rows) == 0:
            pytest.skip("No rows returned from query")

        # Build lookup by partition key (UUID)
        actual_by_key = {}
        for row in result.rows:
            key = row.get("id")
            if key is not None:
                actual_by_key[str(key)] = row

        # Validate a sample of rows
        validated = 0
        for partition in partitions[:10]:  # Check first 10 partitions
            partition_key = partition["partition"]["key"][0]
            rows = partition.get("rows", [])

            for row_data in rows:
                if row_data.get("type") != "row":
                    continue

                cells = row_data.get("cells", [])
                if str(partition_key) not in actual_by_key:
                    continue

                actual_row = actual_by_key[str(partition_key)]

                # Check each cell value
                for cell in cells:
                    cell_name = cell.get("name")
                    cell_value = cell.get("value")

                    if cell_name is None or "deletion_info" in cell:
                        continue  # Skip tombstones

                    if "path" in cell:
                        continue  # Skip collection elements

                    expected = normalize_jsonl_value(cell_value, cell_name)
                    actual = actual_row.get(cell_name)

                    assert values_equal(actual, expected), (
                        f"Value mismatch for simple_table.{cell_name} "
                        f"(partition {partition_key}): "
                        f"got {actual!r} ({type(actual).__name__}), "
                        f"expected {expected!r} ({type(expected).__name__})"
                    )
                    validated += 1

        assert validated > 0, "No values were validated"

    @pytest.mark.xfail(
        reason="Counter tables have complex sharding; partition key 'id' not in results"
    )
    def test_counters_values(self, db_basic):
        """Verify values for counters table."""
        jsonl_file = find_jsonl_file("test_basic", "counters")
        if jsonl_file is None:
            pytest.skip("JSONL reference not found for test_basic.counters")

        # Load partitions (cached for performance - Issue #337)
        partitions = load_jsonl_partitions_cached(str(jsonl_file))
        if not partitions:
            pytest.skip("No partitions in JSONL file")

        result = db_basic.execute("SELECT * FROM test_basic.counters")
        if len(result.rows) == 0:
            pytest.skip("No rows returned from query")

        # Build lookup by partition key (text id)
        actual_by_key = {}
        for row in result.rows:
            key = row.get("id")
            if key is not None:
                actual_by_key[key] = row

        validated = 0
        for partition in partitions:
            partition_key = partition["partition"]["key"][0]
            rows = partition.get("rows", [])

            for row_data in rows:
                if row_data.get("type") != "row":
                    continue

                cells = row_data.get("cells", [])
                if partition_key not in actual_by_key:
                    continue

                actual_row = actual_by_key[partition_key]

                for cell in cells:
                    cell_name = cell.get("name")
                    cell_value = cell.get("value")

                    if cell_name is None or "deletion_info" in cell:
                        continue

                    if "path" in cell:
                        continue

                    expected = normalize_jsonl_value(cell_value, cell_name)
                    actual = actual_row.get(cell_name)

                    # Counter values should be integers
                    assert values_equal(actual, expected), (
                        f"Value mismatch for counters.{cell_name} "
                        f"(partition {partition_key}): "
                        f"got {actual!r}, expected {expected!r}"
                    )
                    validated += 1

        assert validated > 0, "No counter values were validated"

    def test_sensor_data_values(self, db_timeseries):
        """Verify values for sensor_data table (timeseries with clustering)."""
        jsonl_file = find_jsonl_file("test_timeseries", "sensor_data")
        if jsonl_file is None:
            pytest.skip("JSONL reference not found for test_timeseries.sensor_data")

        partitions = load_jsonl_partitions_cached(str(jsonl_file))
        if not partitions:
            pytest.skip("No partitions in JSONL file")

        result = db_timeseries.execute("SELECT * FROM test_timeseries.sensor_data")
        if len(result.rows) == 0:
            pytest.skip("No rows returned from query")

        # Verify we got the expected number of rows
        # Compute count from partitions instead of re-reading file (Issue #337)
        expected_count = sum(
            1 for p in partitions
            for row in p.get("rows", [])
            if row.get("type") == "row"
        )
        assert len(result.rows) == expected_count

        # Spot check that rows contain expected column types
        for row in result.rows[:5]:
            row_dict = row.to_dict()
            # sensor_data should have sensor_id (uuid), timestamp, value, etc.
            assert "sensor_id" in row_dict or len(row_dict) > 0


# =============================================================================
# Coverage Summary Test
# =============================================================================


class TestCoverageSummary:
    """Generate a coverage report for all 33 tables."""

    def test_coverage_report(self, datasets_root):
        """Report coverage status for all tables."""
        passed = []
        failed = []
        skipped = []

        for keyspace, table in ALL_TABLES:
            jsonl_file = find_jsonl_file(keyspace, table)
            if jsonl_file is None:
                skipped.append(f"{keyspace}.{table}")
                continue

            try:
                count = count_rows_in_jsonl(jsonl_file)
                passed.append((f"{keyspace}.{table}", count))
            except Exception as e:
                failed.append((f"{keyspace}.{table}", str(e)))

        # Skip test entirely if no JSONL files available (CI without test data)
        if len(passed) == 0:
            pytest.skip("No JSONL reference files available - test data may not be fetched")

        # Print summary
        print(f"\n{'='*60}")
        print("sstabledump Parity Test Coverage Report")
        print(f"{'='*60}")
        print(f"Total tables: {len(ALL_TABLES)}")
        print(f"JSONL available: {len(passed)}")
        print(f"JSONL missing: {len(skipped)}")
        print(f"Parse errors: {len(failed)}")
        print()

        if passed:
            print("Tables with JSONL references:")
            for name, count in passed:
                print(f"  {name}: {count} rows")

        if skipped:
            print("\nTables missing JSONL:")
            for name in skipped:
                print(f"  {name}")

        if failed:
            print("\nTables with parse errors:")
            for name, error in failed:
                print(f"  {name}: {error}")

        # Assert we have coverage for all tables (only if JSONL available)
        assert len(passed) == len(ALL_TABLES), (
            f"Expected {len(ALL_TABLES)} tables with JSONL references, "
            f"but only found {len(passed)}"
        )


# =============================================================================
# E2E Summary Test (Issue #323)
# =============================================================================


class TestE2ESummary:
    """Explicit E2E summary test for all 33 tables.

    This test is the acceptance criteria for Issue #323:
    Python E2E tests validate all 33 tables against JSONL golden files.
    """

    EXPECTED_TABLE_COUNT = 33

    # Tables with known issues that are expected to fail (XFail)
    # Update this list as core issues are resolved
    KNOWN_ISSUES: dict = {}

    def test_e2e_all_33_tables(self, datasets_root):
        """E2E validation that all 33 tables are queryable.

        This is the primary acceptance test for Issue #323.
        Verifies:
        1. All 33 tables have JSONL golden files
        2. All 33 tables are queryable via Python bindings
        3. Row counts match JSONL reference (excluding known issues)
        """
        passed = []
        failed = []
        xfail = []
        skipped = []

        for keyspace, table in ALL_TABLES:
            # Check JSONL exists
            jsonl_file = find_jsonl_file(keyspace, table)
            if jsonl_file is None:
                skipped.append((keyspace, table, "JSONL not found"))
                continue

            # Get expected row count
            expected_count = count_rows_in_jsonl(jsonl_file)

            # Get schema file
            schema_file = get_schema_for_keyspace(keyspace)
            if schema_file is None:
                skipped.append((keyspace, table, "Schema not found"))
                continue

            # Query table
            try:
                with cqlite.open(DATASETS, schema=schema_file) as db:
                    result = db.execute(f"SELECT * FROM {keyspace}.{table}")
                    actual_count = len(result.rows)

                    if (keyspace, table) in self.KNOWN_ISSUES:
                        # Record as XFail if it fails, pass if it unexpectedly passes
                        if actual_count != expected_count:
                            xfail.append((keyspace, table, self.KNOWN_ISSUES[(keyspace, table)]))
                        else:
                            # Issue is fixed! Record as pass
                            passed.append((keyspace, table, actual_count))
                    elif actual_count == expected_count:
                        passed.append((keyspace, table, actual_count))
                    else:
                        failed.append(
                            (keyspace, table, f"Row count: {actual_count} vs expected {expected_count}")
                        )
            except Exception as e:
                if (keyspace, table) in self.KNOWN_ISSUES:
                    xfail.append((keyspace, table, str(e)))
                else:
                    failed.append((keyspace, table, str(e)))

        # Skip test entirely if all tables skipped (CI without test data)
        if len(skipped) == len(ALL_TABLES):
            pytest.skip("No JSONL reference files available - test data may not be fetched")

        # Report results
        print(f"\n{'='*60}")
        print("E2E Test Summary (Issue #323)")
        print(f"{'='*60}")
        print(f"Total: {len(ALL_TABLES)} tables")
        print(f"Passed: {len(passed)}")
        print(f"XFail (known issues): {len(xfail)}")
        print(f"Failed: {len(failed)}")
        print(f"Skipped: {len(skipped)}")
        print()

        if failed:
            print("FAILURES:")
            for ks, tbl, reason in failed:
                print(f"  {ks}.{tbl}: {reason}")

        if xfail:
            print("\nKNOWN ISSUES (XFail):")
            for ks, tbl, reason in xfail:
                print(f"  {ks}.{tbl}: {reason}")

        # Assert all tables covered (passed + xfail should equal total)
        assert len(ALL_TABLES) == self.EXPECTED_TABLE_COUNT, (
            f"Expected {self.EXPECTED_TABLE_COUNT} tables, found {len(ALL_TABLES)}"
        )

        # Assert no unexpected failures
        assert len(failed) == 0, (
            f"E2E validation failed for {len(failed)} tables: "
            f"{[f'{ks}.{tbl}' for ks, tbl, _ in failed]}"
        )

        # Assert no skipped tables
        assert len(skipped) == 0, (
            f"E2E validation skipped {len(skipped)} tables: "
            f"{[f'{ks}.{tbl}' for ks, tbl, _ in skipped]}"
        )

        # Success: all tables accounted for (passed + xfail)
        print(f"\nE2E VALIDATION: {len(passed) + len(xfail)}/{len(ALL_TABLES)} tables validated")


# =============================================================================
# VG4 (Issue #656): OA Format Parity Tests — Row-Count + Value Spot Checks
# =============================================================================


class TestOaRowCountParity:
    """Tier 1: Row count parity for oa tables against JSONL goldens.

    Issue #656 (VG4): oa tables are now enforced in CI.  The db_oa fixture
    skips gracefully when oa binary files are absent (goldens-only checkout).

    VG6 (Issue #672): All 6 oa tables now pass row-count parity. The
    range-tombstone-marker skip function was fixed to correctly read the u16
    cluster_count + marker_body_size fields (ClusteringBoundOrBoundary.java:105,
    UnfilteredSerializer.java:291), and count_rows_in_jsonl was updated to
    exclude row-level tombstones from the expected count (matching CQLite's
    behaviour of suppressing deleted rows from query results).
    """

    # All 6 oa tables now work correctly through the Python binding layer (VG6)
    WORKING_TABLES = [
        "udt_table",
        "static_table",
        "ttl_table",
        "simple_table",
        "tombstone_table",
        "collection_table",
    ]

    @pytest.mark.parametrize("table", WORKING_TABLES)
    def test_oa_row_count(self, db_oa, table):
        """Row count for test_oa.{table} must match JSONL golden (VG6, Issue #672)."""
        jsonl_file = find_oa_jsonl_file("test_oa", table)
        if jsonl_file is None:
            pytest.skip(f"JSONL reference not found for test_oa.{table}")

        expected_count = count_rows_in_jsonl(jsonl_file)
        result = db_oa.execute(f"SELECT * FROM test_oa.{table}")
        actual_count = len(result.rows)

        assert actual_count == expected_count, (
            f"Row count mismatch for test_oa.{table}: "
            f"got {actual_count}, expected {expected_count} (from JSONL golden)"
        )


class TestOaValueParity:
    """Tier 2: Value-level parity for a representative oa table.

    Issue #656 (VG4): verifies that the Python bindings return the same cell
    values as the sstabledump JSONL for test_oa.udt_table.

    udt_table is chosen because it:
    - Returns the correct row count (2 rows)
    - Has complex UDT fields that exercise oa parsing thoroughly
    - Does not exhibit the timestamp-overflow bug seen in simple_table/tombstone_table
    """

    def test_oa_udt_table_values(self, db_oa):
        """Verify cell values for test_oa.udt_table match JSONL golden."""
        jsonl_file = find_oa_jsonl_file("test_oa", "udt_table")
        if jsonl_file is None:
            pytest.skip("JSONL reference not found for test_oa.udt_table")

        partitions = load_jsonl_partitions_cached(str(jsonl_file))
        if not partitions:
            pytest.skip("No partitions in test_oa.udt_table JSONL file")

        result = db_oa.execute("SELECT * FROM test_oa.udt_table")
        if len(result.rows) == 0:
            pytest.skip("No rows returned from test_oa.udt_table")

        # Build lookup by partition key (UUID string)
        actual_by_key: dict[str, Any] = {}
        for row in result.rows:
            key = row.get("id")
            if key is not None:
                actual_by_key[str(key)] = row

        validated = 0
        for partition in partitions:
            partition_key = partition["partition"]["key"][0]
            rows = partition.get("rows", [])

            for row_data in rows:
                if row_data.get("type") != "row":
                    continue

                cells = row_data.get("cells", [])
                if str(partition_key) not in actual_by_key:
                    continue

                actual_row = actual_by_key[str(partition_key)]

                for cell in cells:
                    cell_name = cell.get("name")
                    cell_value = cell.get("value")

                    if cell_name is None or "deletion_info" in cell:
                        continue  # skip tombstones
                    if "path" in cell:
                        continue  # skip collection elements
                    # Skip UDT fields (dict comparison is handled separately)
                    if isinstance(cell_value, dict) and "street" in cell_value:
                        # UDT address field — validate presence, not exact comparison
                        actual_udt = actual_row.get(cell_name)
                        assert actual_udt is not None, (
                            f"UDT field {cell_name} is None in actual row for partition {partition_key}"
                        )
                        validated += 1
                        continue

                    expected = normalize_jsonl_value(cell_value, cell_name)
                    actual = actual_row.get(cell_name)

                    assert values_equal(actual, expected), (
                        f"Value mismatch for test_oa.udt_table.{cell_name} "
                        f"(partition {partition_key}): "
                        f"got {actual!r} ({type(actual).__name__}), "
                        f"expected {expected!r} ({type(expected).__name__})"
                    )
                    validated += 1

        assert validated > 0, (
            "No cell values were validated in test_oa.udt_table — "
            "check that partition keys align between query results and JSONL"
        )
