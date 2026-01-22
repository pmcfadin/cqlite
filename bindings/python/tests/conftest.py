"""Shared pytest fixtures and helpers for CQLite Python bindings test suite.

Issue #330: Centralized test configuration to eliminate duplication across 16 test files.
Issue #331: CLI binary caching and slow test markers for CI optimization.

This module provides:
- Centralized path constants (TEST_DATA, DATASETS, SCHEMAS, PROJECT_ROOT)
- Database fixtures for all schema variants with proper scoping
- Skip helpers for graceful handling of missing test data
- CLI binary fixture for parity tests (builds once per session)
- Slow test skipping unless RUN_SLOW_TESTS=1 is set
"""

import os
import subprocess
from pathlib import Path
from typing import Optional

import pytest

import cqlite


# =============================================================================
# Path Constants
# =============================================================================

# Calculate paths relative to conftest.py location
# conftest.py is at: bindings/python/tests/conftest.py
# test-data is at: test-data/ (project root level)
TESTS_DIR = Path(__file__).parent
BINDINGS_DIR = TESTS_DIR.parent
PROJECT_ROOT = BINDINGS_DIR.parent.parent
TEST_DATA = PROJECT_ROOT / "test-data"

# Support CQLITE_DATASETS_ROOT environment variable override
_ENV_DATASETS_ROOT = os.environ.get("CQLITE_DATASETS_ROOT")
if _ENV_DATASETS_ROOT:
    DATASETS = Path(_ENV_DATASETS_ROOT)
else:
    DATASETS = TEST_DATA / "datasets" / "sstables"

SCHEMAS = TEST_DATA / "schemas"

# Schema file paths (for convenience)
SCHEMA_BASIC_TYPES = SCHEMAS / "basic-types.cql"
SCHEMA_COLLECTIONS = SCHEMAS / "collections.cql"
SCHEMA_TIME_SERIES = SCHEMAS / "time-series.cql"
SCHEMA_WIDE_ROWS = SCHEMAS / "wide-rows.cql"


# =============================================================================
# Skip Condition Helpers
# =============================================================================


def skip_if_no_datasets():
    """Skip test if datasets directory doesn't exist."""
    if not DATASETS.exists():
        pytest.skip(f"Test data not found: {DATASETS}")


def skip_if_no_schema(schema_path: Path):
    """Skip test if schema file doesn't exist."""
    if not schema_path.exists():
        pytest.skip(f"Schema file not found: {schema_path}")


def require_test_data(schema_path: Optional[Path] = None):
    """Combined check for datasets and optional schema.

    Args:
        schema_path: Optional path to schema file to verify exists.
    """
    skip_if_no_datasets()
    if schema_path:
        skip_if_no_schema(schema_path)


# =============================================================================
# Database Fixtures - Function Scoped (Default)
# =============================================================================


@pytest.fixture
def db():
    """Database fixture with basic-types schema (function-scoped).

    Use for tests that need isolated database state per test.
    Default fixture used by most test files.
    """
    require_test_data(SCHEMA_BASIC_TYPES)
    with cqlite.open(DATASETS, schema=SCHEMA_BASIC_TYPES) as database:
        yield database


@pytest.fixture
def db_collections():
    """Database fixture with collections schema (function-scoped)."""
    require_test_data(SCHEMA_COLLECTIONS)
    with cqlite.open(DATASETS, schema=SCHEMA_COLLECTIONS) as database:
        yield database


@pytest.fixture
def db_timeseries():
    """Database fixture with time-series schema (function-scoped)."""
    require_test_data(SCHEMA_TIME_SERIES)
    with cqlite.open(DATASETS, schema=SCHEMA_TIME_SERIES) as database:
        yield database


@pytest.fixture
def db_wide_rows():
    """Database fixture with wide-rows schema (function-scoped)."""
    require_test_data(SCHEMA_WIDE_ROWS)
    with cqlite.open(DATASETS, schema=SCHEMA_WIDE_ROWS) as database:
        yield database


# =============================================================================
# Database Fixtures - Module Scoped (For Parity/Performance Tests)
# =============================================================================


@pytest.fixture(scope="module")
def db_basic_module():
    """Database fixture with basic-types schema (module-scoped).

    Use for tests that can share database state across a module for performance.
    """
    require_test_data(SCHEMA_BASIC_TYPES)
    with cqlite.open(DATASETS, schema=SCHEMA_BASIC_TYPES) as database:
        yield database


@pytest.fixture(scope="module")
def db_collections_module():
    """Database fixture with collections schema (module-scoped)."""
    require_test_data(SCHEMA_COLLECTIONS)
    with cqlite.open(DATASETS, schema=SCHEMA_COLLECTIONS) as database:
        yield database


@pytest.fixture(scope="module")
def db_timeseries_module():
    """Database fixture with time-series schema (module-scoped)."""
    require_test_data(SCHEMA_TIME_SERIES)
    with cqlite.open(DATASETS, schema=SCHEMA_TIME_SERIES) as database:
        yield database


@pytest.fixture(scope="module")
def db_wide_rows_module():
    """Database fixture with wide-rows schema (module-scoped)."""
    require_test_data(SCHEMA_WIDE_ROWS)
    with cqlite.open(DATASETS, schema=SCHEMA_WIDE_ROWS) as database:
        yield database


# =============================================================================
# Utility Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def datasets_root() -> Path:
    """Return the path to the datasets root directory."""
    skip_if_no_datasets()
    return DATASETS


# =============================================================================
# CLI Parity Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def check_prerequisites():
    """Check that test prerequisites are met for CLI parity tests.

    Verifies:
    - Datasets directory exists
    - Schemas directory exists
    - Cargo is available for running CLI
    """
    import subprocess

    skip_if_no_datasets()
    if not SCHEMAS.exists():
        pytest.skip(f"Schemas not found: {SCHEMAS}")

    # Check if cargo is available
    try:
        subprocess.run(
            ["cargo", "--version"],
            capture_output=True,
            timeout=10,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pytest.skip("Cargo not available - cannot run CLI tests")
