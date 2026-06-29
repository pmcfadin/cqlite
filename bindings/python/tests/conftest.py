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
# The documented convention is CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
# (the parent of sstables/).  But we also accept a path that already ends in
# sstables/ for backwards compatibility.  Resolution rule:
#   1. If <root>/sstables/ exists, use that (documented convention).
#   2. Otherwise use <root> directly (already pointing at sstables/).
_ENV_DATASETS_ROOT = os.environ.get("CQLITE_DATASETS_ROOT")
if _ENV_DATASETS_ROOT:
    _root = Path(_ENV_DATASETS_ROOT)
    _candidate = _root / "sstables"
    DATASETS = _candidate if _candidate.exists() else _root
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


def _require_fixtures_strict() -> bool:
    """True when strict fixture mode is requested (issue #1230).

    Mirrors the Rust ``require_fixtures_strict`` helper: either
    ``CQLITE_REQUIRE_FIXTURES`` or ``CQLITE_PARITY_REQUIRE_DATASETS`` set to a
    truthy value flips the dataset-dependent pytest lane FAIL-CLOSED — a missing
    dataset becomes a hard failure instead of a silent skip, so a dropped table
    or a path regression reds CI rather than false-greening. Local dev without
    the binaries (neither flag set) still skips.
    """
    return os.environ.get("CQLITE_REQUIRE_FIXTURES") in ("1", "true") or os.environ.get(
        "CQLITE_PARITY_REQUIRE_DATASETS"
    ) in ("1", "true")


def skip_if_no_datasets():
    """Skip (or, under strict mode, FAIL) when the datasets dir is absent.

    Issue #1230: under ``CQLITE_REQUIRE_FIXTURES=1`` (used by CI) a missing
    dataset is a hard failure, never a silent skip.
    """
    if not DATASETS.exists():
        msg = f"Test data not found: {DATASETS}"
        if _require_fixtures_strict():
            pytest.fail(
                f"{msg} (CQLITE_REQUIRE_FIXTURES=1 — fetch with "
                "bash test-data/scripts/fetch-datasets.sh)",
                pytrace=False,
            )
        pytest.skip(msg)


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


def _get_source_files_for_staleness_check() -> list[Path]:
    """Get all source files that could affect the CLI binary.

    Includes:
    - cqlite-cli/src/**/*.rs (CLI source)
    - cqlite-core/src/**/*.rs (core library source)
    - Cargo.toml, Cargo.lock (workspace dependencies)
    - cqlite-cli/Cargo.toml, cqlite-core/Cargo.toml (per-crate dependencies)
    - build.rs in workspace crates (excludes target/ to avoid noise)
    """
    files = []

    # Rust source files in cqlite-cli and cqlite-core
    for crate_dir in ["cqlite-cli", "cqlite-core"]:
        src_dir = PROJECT_ROOT / crate_dir / "src"
        if src_dir.exists():
            files.extend(src_dir.glob("**/*.rs"))

    # Workspace Cargo manifest and lock files
    for cargo_file in ["Cargo.toml", "Cargo.lock"]:
        cargo_path = PROJECT_ROOT / cargo_file
        if cargo_path.exists():
            files.append(cargo_path)

    # Per-crate Cargo manifests (dependency or feature changes affect binary)
    for crate_dir in ["cqlite-cli", "cqlite-core"]:
        cargo_toml = PROJECT_ROOT / crate_dir / "Cargo.toml"
        if cargo_toml.exists():
            files.append(cargo_toml)

    # Build scripts in workspace crates (exclude target/ to avoid noise)
    for crate_dir in ["cqlite-cli", "cqlite-core", "bindings/python"]:
        build_rs = PROJECT_ROOT / crate_dir / "build.rs"
        if build_rs.exists():
            files.append(build_rs)
    # Also check workspace root
    root_build_rs = PROJECT_ROOT / "build.rs"
    if root_build_rs.exists():
        files.append(root_build_rs)

    return files


@pytest.fixture(scope="session")
def cli_binary() -> Path:
    """Build CLI binary once per session and return path.

    Performs stale binary detection:
    - If binary doesn't exist, builds it
    - If binary exists but sources are newer, rebuilds

    Sources checked for staleness:
    - cqlite-cli/src/**/*.rs
    - cqlite-core/src/**/*.rs
    - Cargo.toml, Cargo.lock (workspace)
    - cqlite-cli/Cargo.toml, cqlite-core/Cargo.toml (per-crate)
    - build.rs in workspace crates

    This fixture is used by CLI parity tests.

    Returns:
        Path to the cqlite binary.

    Raises:
        pytest.skip: If build fails or cargo is unavailable.
    """
    # Note: Binary is named "cqlite" (from [[bin]] name in Cargo.toml), not "cqlite-cli"
    binary_name = "cqlite.exe" if os.name == "nt" else "cqlite"
    release_binary = PROJECT_ROOT / "target" / "release" / binary_name

    # Check if rebuild is needed
    needs_rebuild = False
    if not release_binary.exists():
        needs_rebuild = True
    else:
        # Check if any source file is newer than the binary
        binary_mtime = release_binary.stat().st_mtime
        source_files = _get_source_files_for_staleness_check()
        if source_files:
            newest_source_mtime = max(path.stat().st_mtime for path in source_files)
            if newest_source_mtime > binary_mtime:
                needs_rebuild = True

    if not needs_rebuild:
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
    """Skip slow tests unless explicitly requested.

    Slow tests include:
    - CLI parity tests (spawn external process)
    - Performance/memory tests (timing-sensitive)

    To run slow tests, use either:
        RUN_SLOW_TESTS=1 pytest tests/
        pytest tests/ -m slow

    To exclude slow tests:
        pytest tests/ -m "not slow"
    """
    # Check if user wants slow tests via environment variable
    run_slow = os.environ.get("RUN_SLOW_TESTS", "0") == "1"
    if run_slow:
        return

    # Check if user explicitly requested slow tests via -m marker expression
    # Use satisfiability check: can expression be True with slow=True?
    markexpr = config.getoption("markexpr", default="")
    if markexpr:
        import itertools
        import re
        from _pytest.mark.expression import Expression

        try:
            expr = Expression.compile(markexpr)

            # Extract marker names from expression (identifiers only)
            names = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", markexpr))
            names -= {"and", "or", "not"}

            # If expression doesn't mention "slow", skip slow tests by default
            if "slow" not in names:
                pass  # Fall through to add skip markers
            else:
                names.discard("slow")
                # Enumerate all combinations of other markers (SAT check with slow=True)
                # Search space is tiny in practice (expressions rarely have >8 markers)
                could_select_slow = False
                if len(names) <= 12:  # Safety cap for huge expressions
                    for combo in itertools.product([False, True], repeat=len(names)):
                        env = dict(zip(names, combo))
                        env["slow"] = True
                        if expr.evaluate(lambda n: env.get(n, False)):
                            could_select_slow = True
                            break
                else:
                    # Fallback for huge expressions: if slow is mentioned, assume wanted
                    could_select_slow = True

                if could_select_slow:
                    return  # Don't skip slow tests
        except Exception:
            # If expression parsing fails, fall back to conservative behavior
            pass

    skip_slow = pytest.mark.skip(reason="Slow test (set RUN_SLOW_TESTS=1 or use -m slow)")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


# =============================================================================
# Whole-session no-op floor (issue #1230)
# =============================================================================

# Count of tests that actually PASSED their call phase. Under strict mode a
# session in which 0 tests pass (everything skipped, or nothing collected) is a
# failure, not a green run.
#
# SCOPE (be honest): this is a whole-session no-op guard ONLY. It fires solely
# when the ENTIRE session has zero passing call-phase tests. Because this lane
# also runs many passing NON-dataset tests, the floor does NOT catch "the
# dataset tests all skipped while the rest passed" — i.e. it will NOT catch a
# dropped/renamed table or a #773-class path regression on its own. Those are
# covered by check-dataset-manifest.sh (hard-fails on a partial corpus) and by
# skip_if_no_datasets() failing closed under strict mode.
_PASSED_CALLS = 0


def pytest_runtest_logreport(report):
    global _PASSED_CALLS
    if report.when == "call" and report.passed:
        _PASSED_CALLS += 1


def pytest_sessionfinish(session, exitstatus):
    """Fail the session under strict mode if no test passed (issue #1230)."""
    if not _require_fixtures_strict():
        return
    if _PASSED_CALLS == 0:
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_line(
                "ERROR (issue #1230): CQLITE_REQUIRE_FIXTURES=1 but 0 tests passed "
                "— the dataset lane ran nothing or everything skipped (fail-closed).",
                red=True,
            )
        # Override exit status so CI reds even though no test technically failed.
        session.exitstatus = 1
