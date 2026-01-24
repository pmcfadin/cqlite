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

// =============================================================================
// Dataset Availability Detection
// =============================================================================

const DATASETS_AVAILABLE = fs.existsSync(SSTABLES_DIR) &&
  fs.existsSync(path.join(SSTABLES_DIR, 'test_basic'));

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
};

global.DATASETS_AVAILABLE = DATASETS_AVAILABLE;
global.SHOULD_RUN_SLOW_TESTS = SHOULD_RUN_SLOW_TESTS;

// Log test configuration (only in verbose mode)
if (process.env.DEBUG_TESTS) {
  console.log('Test Configuration:');
  console.log(`  PROJECT_ROOT: ${PROJECT_ROOT}`);
  console.log(`  SSTABLES_DIR: ${SSTABLES_DIR}`);
  console.log(`  DATASETS_AVAILABLE: ${DATASETS_AVAILABLE}`);
  console.log(`  SHOULD_RUN_SLOW_TESTS: ${SHOULD_RUN_SLOW_TESTS}`);
}
