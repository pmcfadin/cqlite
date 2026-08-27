/**
 * Jest setup file for CQLite Node.js bindings tests.
 *
 * Issue #306: Centralized test configuration matching Python conftest.py patterns.
 *
 * This module provides:
 * - Centralized path constants
 * - Dataset availability detection
 * - Slow test handling (RUN_SLOW_TESTS environment variable)
 */
const path = require('path');
const fs = require('fs');

// =============================================================================
// Path Constants (matching Python conftest.py pattern)
// =============================================================================

const TESTS_DIR = __dirname;
const BINDINGS_DIR = path.dirname(TESTS_DIR);
const PROJECT_ROOT = path.resolve(BINDINGS_DIR, '..', '..');

// Support CQLITE_DATASETS_ROOT environment variable (used in CI)
const TEST_DATA_ROOT = process.env.CQLITE_DATASETS_ROOT ||
  path.join(PROJECT_ROOT, 'test-data', 'datasets');
const SSTABLES_DIR = path.join(TEST_DATA_ROOT, 'sstables');
const SCHEMAS_DIR = path.join(PROJECT_ROOT, 'test-data', 'schemas');

// Schema file paths for different test keyspaces
const SCHEMA_BASIC_TYPES = path.join(SCHEMAS_DIR, 'basic-types.cql');
const SCHEMA_COLLECTIONS = path.join(SCHEMAS_DIR, 'collections.cql');
const SCHEMA_TIME_SERIES = path.join(SCHEMAS_DIR, 'time-series.cql');
const SCHEMA_WIDE_ROWS = path.join(SCHEMAS_DIR, 'wide-rows.cql');
// Issue #656 (VG4): oa test schema for test_oa keyspace
const SCHEMA_OA_TEST = path.join(SCHEMAS_DIR, 'oa-test.cql');

// =============================================================================
// Dataset Availability Detection
// =============================================================================

/**
 * Recursively check whether `dir` contains at least one `*-Data.db` SSTable
 * binary (issue #1458).
 *
 * Directory existence alone is NOT evidence of a corpus: a present-but-EMPTY
 * sstables dir is the exact shape of the original #773 failure and used to
 * count as "available", false-greening a broken fixture setup.
 *
 * @param {string} dir
 * @returns {boolean}
 */
function hasDataDbFile(dir) {
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (err) {
    // Unreadable/absent dir contributes no fixtures.
    return false;
  }
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    let isDirectory = entry.isDirectory();
    if (entry.isSymbolicLink()) {
      // withFileTypes does not follow symlinks; a symlinked keyspace dir is
      // still a legitimate corpus layout.
      try {
        isDirectory = fs.statSync(full).isDirectory();
      } catch (err) {
        continue; // broken symlink
      }
    }
    if (isDirectory) {
      if (hasDataDbFile(full)) return true;
    } else if (entry.name.endsWith('-Data.db')) {
      return true;
    }
  }
  return false;
}

// Issue #1458: content-aware, not directory-only.
const DATASETS_AVAILABLE = fs.existsSync(SSTABLES_DIR) && hasDataDbFile(SSTABLES_DIR);

// Strict fixture mode (issue #1230/#1458). Mirrors the Python
// _require_fixtures_strict() helper: same two env var names, and the same
// accepted truthy spellings ('1', 'true'). No other names are recognised.
const REQUIRE_FIXTURES = ['1', 'true'].includes(process.env.CQLITE_REQUIRE_FIXTURES) ||
  ['1', 'true'].includes(process.env.CQLITE_PARITY_REQUIRE_DATASETS);

// =============================================================================
// Slow Test Handling (matching Python conftest.py RUN_SLOW_TESTS pattern)
// =============================================================================

const SHOULD_RUN_SLOW_TESTS = process.env.RUN_SLOW_TESTS === '1';

// =============================================================================
// Export to global scope for test files
// =============================================================================

global.testPaths = {
  PROJECT_ROOT,
  TEST_DATA_ROOT,
  SSTABLES_DIR,
  SCHEMAS_DIR,
  SCHEMA_BASIC_TYPES,
  SCHEMA_COLLECTIONS,
  SCHEMA_TIME_SERIES,
  SCHEMA_WIDE_ROWS,
  SCHEMA_OA_TEST,
};

global.DATASETS_AVAILABLE = DATASETS_AVAILABLE;
global.REQUIRE_FIXTURES = REQUIRE_FIXTURES;
global.SHOULD_RUN_SLOW_TESTS = SHOULD_RUN_SLOW_TESTS;

// Log test configuration (only in verbose mode)
if (process.env.DEBUG_TESTS) {
  console.log('Test Configuration:');
  console.log(`  PROJECT_ROOT: ${PROJECT_ROOT}`);
  console.log(`  SSTABLES_DIR: ${SSTABLES_DIR}`);
  console.log(`  DATASETS_AVAILABLE: ${DATASETS_AVAILABLE}`);
  console.log(`  REQUIRE_FIXTURES: ${REQUIRE_FIXTURES}`);
  console.log(`  SHOULD_RUN_SLOW_TESTS: ${SHOULD_RUN_SLOW_TESTS}`);
}

// Issue #1458: under strict mode a missing corpus is a hard failure of the
// WHOLE suite -- throwing here beats leaving DATASETS_AVAILABLE=false for
// describe.skip, which would report a green run over zero real assertions.
if (REQUIRE_FIXTURES && !DATASETS_AVAILABLE) {
  throw new Error(
    `No SSTable fixtures found: ${SSTABLES_DIR} is absent or contains 0 *-Data.db files ` +
    '(CQLITE_REQUIRE_FIXTURES=1 — fetch with bash test-data/scripts/fetch-datasets.sh)'
  );
}
