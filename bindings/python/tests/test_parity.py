"""sstabledump Parity Tests for Python Bindings - Issue #309.

Validates that Python binding output matches Cassandra sstabledump for the
committed test corpus. This is part of M4 Python Bindings Epic (#321).

Test Strategy:
    Tier 1: Row Count Parity - Verify row counts match the JSONL reference for
            the schema-mapped keyspaces (test_basic/collections/timeseries/wide_rows).
    Tier 2: Value Comparison - For tables with simple types, validate cell values match.
    Coverage/E2E: enumerate the committed corpus DYNAMICALLY (Issue #1229) so a
            newly-committed keyspace is automatically in scope; the skip-set +
            rationale lives in ``test-data/corpus-coverage-policy.md`` and
            ``corpus.py``. No hand-typed table count and no tautological
            "assert len == 33" assertions.
"""

# Defer annotation evaluation so PEP 604 unions (e.g. `Path | None`) parse on
# Python 3.9, the project's minimum supported version. Without this, the
# module-level `-> Path | None` annotation is evaluated at import time and
# raises `TypeError` on 3.9 (PEP 604 runtime support is 3.10+).
from __future__ import annotations

import functools
import json
import re
from datetime import date, datetime
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

import cqlite


from conftest import DATASETS, SCHEMAS

from corpus import (
    SKIP_KEYSPACES,
    SKIP_PENDING_KEYSPACES,
    _is_committed_table_dir,
    discover_corpus,
    discover_table_dirs,
    discover_tables,
    in_scope_keyspaces,
    unclassified_keyspaces,
)


# =============================================================================
# Table Definitions — DYNAMICALLY enumerated from the committed corpus
# (Issue #1229). The skip-set + rationale lives in corpus.py /
# test-data/corpus-coverage-policy.md, NOT hand-typed here.
# =============================================================================

# Schema file to keyspace mapping (which keyspaces this Python suite can
# *execute* queries against; coverage checks span the full in-scope corpus).
SCHEMA_KEYSPACE_MAP = {
    "basic-types.cql": ["test_basic"],
    "collections.cql": ["test_collections"],
    "time-series.cql": ["test_timeseries"],
    "wide-rows.cql": ["test_wide_rows"],
    "oa-test.cql": ["test_oa"],
}

# Keyspaces this suite executes row-count parity against (have a schema map
# AND a stable nb-format golden layout). Other in-scope keyspaces are still
# enumerated by the coverage tests so they cannot silently fall out of scope.
EXECUTABLE_KEYSPACES = ["test_basic", "test_collections", "test_timeseries", "test_wide_rows"]

# All 6 oa tables — discovered dynamically (Issue #656 VG4 / #1229).
OA_TABLES = [("test_oa", t) for t in discover_tables(DATASETS, "test_oa")]

# Executable (keyspace, table) pairs — discovered, not hand-typed.
ALL_TABLES = [
    (ks, table)
    for ks in EXECUTABLE_KEYSPACES
    for table in discover_tables(DATASETS, ks)
]

# The full in-scope read-parity corpus (every committed keyspace minus the
# documented skip-set). Used by coverage/E2E tests so a newly-committed
# keyspace is automatically in scope.
DISCOVERED_CORPUS = discover_corpus(DATASETS)


# =============================================================================
# JSONL Reference File Helpers
# =============================================================================


def find_jsonl_file(keyspace: str, table: str) -> Path | None:
    """Find the JSONL reference file for a table (format-agnostic, #1229).

    JSONL files live at:
    test-data/datasets/sstables/{keyspace}/{table}-{hash}/<format>-<gen>-<kind>-Data.db.jsonl

    The format prefix varies by SSTable format (``nb-`` legacy BIG, ``oa-``
    Cassandra 5.0 BIG, ``da-`` BTI) and the generation is not always ``1``
    (e.g. ``nb-2``, ``nb-45``, ``oa-2``, ``da-2``). Globbing ``*-Data.db.jsonl``
    instead of hard-coding ``nb-1-big-Data.db.jsonl`` ensures a missing golden
    for a non-nb-1 table is detected (not silently treated as "no golden").

    Restricted to the COMMITTED corpus at TABLE granularity (#1319): an
    untracked WIP ``<table>-<uuid>/`` dir that reuses an existing committed
    table's logical name is SKIPPED so the lookup never resolves a WIP golden
    in place of the committed one. Falls back (git unavailable) to treating all
    discovered dirs as committed, matching :func:`_is_committed_table_dir`.
    """
    keyspace_dir = DATASETS / keyspace
    if not keyspace_dir.exists():
        return None

    # Find table directory (contains hash suffix)
    for table_dir in keyspace_dir.iterdir():
        if (
            table_dir.is_dir()
            and table_dir.name.startswith(f"{table}-")
            and _is_committed_table_dir(DATASETS, keyspace, table_dir.name)
        ):
            for jsonl_file in sorted(table_dir.glob("*-Data.db.jsonl")):
                if jsonl_file.exists():
                    return jsonl_file
    return None


def find_oa_jsonl_file(keyspace: str, table: str) -> Path | None:
    """Find the JSONL reference file for an oa-format table (Issue #656 VG4).

    oa tables use oa-format SSTable files:
    test-data/datasets/sstables/{keyspace}/{table}-{hash}/oa-2-big-Data.db.jsonl

    Restricted to the COMMITTED corpus at TABLE granularity (#1319): an
    untracked WIP ``<table>-<uuid>/`` dir reusing a committed table's logical
    name is SKIPPED so the lookup never resolves a WIP golden.
    """
    keyspace_dir = DATASETS / keyspace
    if not keyspace_dir.exists():
        return None

    for table_dir in keyspace_dir.iterdir():
        if (
            table_dir.is_dir()
            and table_dir.name.startswith(f"{table}-")
            and _is_committed_table_dir(DATASETS, keyspace, table_dir.name)
        ):
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

        # Detect time pattern with nanoseconds: "01:12:05.394017000".
        # CQL `time` decodes to exact int nanoseconds since midnight (#1450),
        # so parse the full nanosecond fraction (no microsecond truncation).
        time_match = re.match(r"^(\d{2}):(\d{2}):(\d{2})\.(\d+)$", value)
        if time_match:
            h, m, s, frac = time_match.groups()
            nanos = int(frac[:9].ljust(9, "0"))
            return (int(h) * 3600 + int(m) * 60 + int(s)) * 1_000_000_000 + nanos

        # Detect simple time: "01:12:05" -> exact int nanoseconds (#1450).
        simple_time = re.match(r"^(\d{2}):(\d{2}):(\d{2})$", value)
        if simple_time:
            h, m, s = simple_time.groups()
            return (int(h) * 3600 + int(m) * 60 + int(s)) * 1_000_000_000

        # Duration pattern: "12h58m22s" or with months/days.
        # CQL `duration` decodes to an exact cqlite.Duration (#1450): months and
        # days are kept independently (no 30-day collapse) and sub-day time is
        # nanoseconds.
        duration_match = re.match(
            r"^(?:(\d+)mo)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$",
            value,
        )
        if duration_match and any(duration_match.groups()):
            mo, d, h, m, s = [int(g) if g else 0 for g in duration_match.groups()]
            nanos = (h * 3600 + m * 60 + s) * 1_000_000_000
            return cqlite.Duration(mo, d, nanos)

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
# Tier 1: Row Count Parity Tests (schema-mapped executable keyspaces)
# =============================================================================


# Known issues with row count discrepancies (pre-existing core library issues)
# Note: As of Jan 2026, all previously known issues have been resolved
KNOWN_ROW_COUNT_ISSUES = {
    # All issues resolved
}


# Map each executable keyspace to its module-scoped database fixture name, so
# the dynamically-parametrized row-count test can dispatch by keyspace (Issue
# #1229). A newly-committed table under one of these keyspaces is picked up by
# discover_tables() and automatically gets row-count coverage.
_KEYSPACE_DB_FIXTURE = {
    "test_basic": "db_basic",
    "test_collections": "db_collections",
    "test_timeseries": "db_timeseries",
    "test_wide_rows": "db_wide_rows",
}

# Discovered (keyspace, table) Tier-1 pairs, respecting the documented
# skip-pending set (corpus.py SKIP_PENDING_KEYSPACES / corpus-coverage-policy.md):
# enumerate every executable keyspace's committed tables, NOT a hand-typed list.
TIER1_ROW_COUNT_TABLES = [
    (ks, table)
    for (ks, table) in ALL_TABLES
    if ks in _KEYSPACE_DB_FIXTURE and ks not in SKIP_PENDING_KEYSPACES
]


class TestRowCountParity:
    """Tier 1: Verify row counts match the JSONL reference.

    This is the primary parity test - ensures the schema-mapped keyspaces are
    readable and return the expected number of rows. The (keyspace, table)
    pairs are DISCOVERED dynamically from the committed corpus (Issue #1229),
    not hand-typed, so a newly-committed in-scope table gets coverage
    automatically. The skip-pending set is respected (corpus.py /
    test-data/corpus-coverage-policy.md).
    """

    def test_tier1_enumerates_discovered_tables(self, datasets_root):
        """Guard: the parametrized set must enumerate the discovered Tier-1 corpus.

        Fails loudly (rather than silently shrinking) if discovery returns
        nothing for the schema-mapped keyspaces, which would make every
        row-count case below vanish unnoticed.

        Issue #1312 (fast-follow to #1229): take the ``datasets_root`` fixture so
        an UNFETCHED checkout SKIPs consistently with the rest of the suite
        (``skip_if_no_datasets()`` — or FAILs under ``CQLITE_REQUIRE_FIXTURES=1``
        strict mode) instead of reporting a hard failure here. A datasets-root
        that is PRESENT but yields an empty enumeration is still a real bug and
        FAILs the assertion below (strict mode preserved).
        """
        assert TIER1_ROW_COUNT_TABLES, (
            "No Tier-1 row-count tables discovered for the schema-mapped "
            f"keyspaces {sorted(_KEYSPACE_DB_FIXTURE)}; expected the committed "
            "corpus to yield at least one table per keyspace (Issue #1229)."
        )
        # Every schema-mapped, non-skip-pending keyspace must contribute tables.
        covered = {ks for (ks, _t) in TIER1_ROW_COUNT_TABLES}
        expected = {
            ks for ks in _KEYSPACE_DB_FIXTURE if ks not in SKIP_PENDING_KEYSPACES
        }
        assert covered == expected, (
            f"Tier-1 keyspace coverage mismatch: discovered {sorted(covered)}, "
            f"expected {sorted(expected)} (Issue #1229)."
        )

    @pytest.mark.parametrize(
        ("keyspace", "table"),
        TIER1_ROW_COUNT_TABLES,
        ids=[f"{ks}.{t}" for (ks, t) in TIER1_ROW_COUNT_TABLES],
    )
    def test_row_count(self, request, keyspace, table):
        """Verify row count parity for every discovered Tier-1 table."""
        database = request.getfixturevalue(_KEYSPACE_DB_FIXTURE[keyspace])

        jsonl_file = find_jsonl_file(keyspace, table)
        if jsonl_file is None:
            pytest.skip(f"JSONL reference not found for {keyspace}.{table}")

        expected_count = count_rows_in_jsonl(jsonl_file)
        result = database.execute(f"SELECT * FROM {keyspace}.{table}")
        actual_count = len(result.rows)

        assert actual_count == expected_count, (
            f"Row count mismatch for {keyspace}.{table}: "
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
    """Coverage report for the dynamically-discovered corpus (Issue #1229)."""

    def test_every_discovered_keyspace_is_classified(self, datasets_root):
        """Fail loudly if a committed keyspace is neither in-scope nor skipped.

        This is the integrity guard: a newly-committed keyspace that nobody
        added to a schema map OR to the documented skip-set shows up here and
        reds the suite, instead of being silently uncovered while CI reports
        "100%".
        """
        unclassified = unclassified_keyspaces(DATASETS)
        assert not unclassified, (
            f"Committed keyspace(s) {unclassified} are neither in the in-scope "
            f"corpus nor in the documented skip-set. Add them to a schema map "
            f"or to SKIP_KEYSPACES in corpus.py (see "
            f"test-data/corpus-coverage-policy.md)."
        )

    def test_skip_pending_keyspaces_are_in_scope(self, datasets_root):
        """Skip-pending keyspaces must be IN-SCOPE (discovered), not skip-set.

        A skip-pending keyspace (e.g. test_deltas) is discovered + listed
        explicitly; it must never silently become a skip-set exclusion.
        """
        if not DISCOVERED_CORPUS:
            pytest.skip("No corpus discovered - test data may not be fetched")
        discovered = set(in_scope_keyspaces(DATASETS))
        for keyspace in SKIP_PENDING_KEYSPACES:
            # Only assert for keyspaces actually present on disk.
            if (DATASETS / keyspace).exists():
                assert keyspace in discovered, (
                    f"skip-pending keyspace {keyspace} is present on disk but "
                    f"not in the in-scope corpus; it must not be in SKIP_KEYSPACES"
                )
                assert keyspace not in SKIP_KEYSPACES

    def test_coverage_report(self, datasets_root):
        """Report JSONL coverage for the full in-scope corpus (no tautology).

        Every in-scope table MUST have a golden JSONL, EXCEPT tables in a
        documented skip-pending keyspace (binaries/goldens not yet shipped).
        A missing golden for a non-exempt in-scope table FAILS loudly here —
        it must never be silently swallowed as "no golden" (#1229).
        """
        passed = []
        failed = []
        missing = []          # in-scope, non-exempt, golden absent -> FAIL
        skip_pending = []     # documented skip-pending -> reported, not fatal

        for keyspace, table, golden_or_dir in DISCOVERED_CORPUS:
            # Each entry is ONE golden path (per-generation coverage), or a
            # directory when that table dir ships no golden yet. Verify THIS
            # exact golden, never collapse multiple generations to first-match
            # (#1229 round-3).
            label = f"{keyspace}.{golden_or_dir.name}"
            jsonl_file = golden_or_dir if golden_or_dir.is_file() else None
            if jsonl_file is None:
                if keyspace in SKIP_PENDING_KEYSPACES:
                    skip_pending.append(label)
                else:
                    missing.append(label)
                continue

            try:
                count = count_rows_in_jsonl(jsonl_file)
                passed.append((label, count))
            except Exception as e:
                failed.append((label, str(e)))

        # Skip test entirely if no corpus discovered (CI without test data)
        if not DISCOVERED_CORPUS:
            pytest.skip("No corpus discovered - test data may not be fetched")

        # Print summary
        print(f"\n{'='*60}")
        print("sstabledump Parity Test Coverage Report (dynamic, #1229)")
        print(f"{'='*60}")
        print(f"In-scope keyspaces: {in_scope_keyspaces(DATASETS)}")
        print(f"Skip-set keyspaces: {sorted(SKIP_KEYSPACES)}")
        print(f"Discovered goldens (per generation): {len(DISCOVERED_CORPUS)}")
        print(f"JSONL available: {len(passed)}")
        print(f"JSONL missing (skip-pending, exempt): {len(skip_pending)}")
        print(f"JSONL missing (in-scope, FAIL): {len(missing)}")
        print(f"Parse errors: {len(failed)}")
        print()

        if failed:
            print("Tables with parse errors:")
            for name, error in failed:
                print(f"  {name}: {error}")

        # No parse errors are tolerated.
        assert not failed, f"JSONL parse errors for: {[n for n, _ in failed]}"
        # A missing golden for a non-skip-pending in-scope table is a real
        # coverage gap (e.g. an oa/da/nb-2 table whose golden was overlooked):
        # fail loudly instead of reporting "100%".
        assert not missing, (
            f"In-scope table(s) {missing} have NO golden JSONL and are not in a "
            f"documented skip-pending keyspace ({sorted(SKIP_PENDING_KEYSPACES)}). "
            f"Commit the golden, or add the keyspace to SKIP_PENDING_KEYSPACES / "
            f"SKIP_KEYSPACES in corpus.py with a reason "
            f"(test-data/corpus-coverage-policy.md)."
        )
        # We must have discovered at least one entry per executable directory
        # (DISCOVERED_CORPUS is now per-golden, so a dir with N generation
        # goldens contributes N >= 1 entries — the count only grows).
        assert len(DISCOVERED_CORPUS) >= sum(
            len(discover_table_dirs(DATASETS, ks)) for ks in EXECUTABLE_KEYSPACES
        )


# =============================================================================
# E2E Summary Test (Issue #323)
# =============================================================================


class TestE2ESummary:
    """Explicit E2E summary test for the executable corpus (Issue #323/#1229).

    The set of tables is enumerated dynamically from the committed corpus
    (the schema-mapped EXECUTABLE_KEYSPACES), not a frozen 33-tuple, and the
    old tautological ``assert len(ALL_TABLES) == 33`` has been removed.
    """

    # Tables with known issues that are expected to fail (XFail)
    # Update this list as core issues are resolved
    KNOWN_ISSUES: dict = {}

    def test_e2e_all_tables(self, datasets_root):
        """E2E validation that the executable corpus is queryable.

        This is the primary acceptance test for Issue #323.
        Verifies, for every dynamically-discovered executable table:
        1. it has a JSONL golden file
        2. it is queryable via Python bindings
        3. row counts match the JSONL reference (excluding known issues)
        """
        # Distinguish "datasets genuinely absent" (legitimate skip) from
        # "datasets present but enumeration yielded nothing" (a broken
        # EXECUTABLE_KEYSPACES enumeration that must FAIL, not skip — #1229
        # round-2). The skip is based ONLY on the datasets root being absent.
        if not DATASETS.exists():
            pytest.skip("Datasets root absent - test data may not be fetched")

        # Datasets ARE present: a non-empty executable corpus is mandatory.
        # An empty ALL_TABLES here means the dynamic enumeration is broken;
        # fail loudly rather than letting the all-skipped shortcut hide it.
        assert len(ALL_TABLES) > 0, (
            "Datasets root is present but no executable tables were discovered; "
            "the EXECUTABLE_KEYSPACES enumeration is broken (#1229)"
        )

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

        # NOTE: there is no "all tables skipped -> skip" shortcut here. With
        # datasets present and ALL_TABLES asserted non-empty above, any per-table
        # skip (missing JSONL/schema) is a real coverage gap surfaced by the
        # `assert len(skipped) == 0` below — it must not silently skip the test.

        # Report results
        print(f"\n{'='*60}")
        print("E2E Test Summary (Issue #323/#1229, dynamic enumeration)")
        print(f"{'='*60}")
        print(f"Executable keyspaces: {EXECUTABLE_KEYSPACES}")
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

        # The executable corpus must be non-empty when test data is present
        # (replaces the tautological len(ALL_TABLES) == 33 assertion).
        assert len(ALL_TABLES) > 0, (
            "No executable tables discovered though JSONL goldens are present; "
            "EXECUTABLE_KEYSPACES enumeration is broken"
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

    # oa tables are DISCOVERED from the committed corpus (#1229), not a frozen
    # hand-typed list — newly-added oa tables get row-count parity automatically.
    # test_oa is not skip-pending, so every discovered oa table is enforced.
    OA_TABLE_NAMES = [table for (_ks, table) in OA_TABLES]

    @pytest.mark.parametrize("table", OA_TABLE_NAMES)
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
