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


# =============================================================================
# CLI Binary Fixture (Issue #331)
# =============================================================================


@pytest.fixture(scope="session")
def cli_binary() -> Path:
    """Build CLI binary once per session and return path.

    If release binary already exists, skip build for faster test startup.
    This fixture is used by CLI parity tests.

    Returns:
        Path to the cqlite binary.

    Raises:
        pytest.skip: If build fails or cargo is unavailable.
    """
    # Note: Binary is named "cqlite" (from [[bin]] name in Cargo.toml), not "cqlite-cli"
    binary_name = "cqlite.exe" if os.name == "nt" else "cqlite"
    release_binary = PROJECT_ROOT / "target" / "release" / binary_name

    # Skip build if release binary already exists (CI pre-build optimization)
    if release_binary.exists():
        return release_binary

    # Build release binary
    try:
        result = subprocess.run(
            ["cargo", "build", "--package", "cqlite-cli", "--release"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout for build
        )
        if result.returncode != 0:
            pytest.skip(f"Failed to build CLI: {result.stderr}")
    except subprocess.TimeoutExpired:
        pytest.skip("CLI build timed out after 5 minutes")
    except FileNotFoundError:
        pytest.skip("Cargo not available - cannot build CLI")

    if not release_binary.exists():
        pytest.skip(f"CLI binary not found after build: {release_binary}")

    return release_binary


# =============================================================================
# Pytest Hooks (Issue #331)
# =============================================================================


def pytest_collection_modifyitems(config, items):
    """Skip slow tests unless RUN_SLOW_TESTS=1 is set.

    Slow tests include:
    - CLI parity tests (spawn external process)
    - Performance/memory tests (timing-sensitive)

    To run slow tests:
        RUN_SLOW_TESTS=1 pytest tests/
    Or explicitly:
        pytest tests/ -m slow
    """
    run_slow = os.environ.get("RUN_SLOW_TESTS", "0") == "1"

    if run_slow:
        # User wants slow tests, don't skip anything
        return

    # Check if user explicitly requested slow tests via -m
    markexpr = config.getoption("-m", default="")
    if markexpr and "slow" in markexpr:
        # User explicitly wants slow tests via marker expression
        return

    skip_slow = pytest.mark.skip(reason="Slow test (set RUN_SLOW_TESTS=1 to run)")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
