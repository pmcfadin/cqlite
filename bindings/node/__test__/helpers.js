/**
 * Test helpers for CQLite Node.js bindings tests.
 *
 * Issue #306: Centralized test utilities matching Python conftest.py patterns.
 */
const path = require('path');
const { Database } = require('../lib/index.js');

/**
 * Get a path that is guaranteed to not exist on any platform.
 * On Windows, uses a nonexistent drive letter path.
 * On Unix, uses a path under /nonexistent.
 *
 * @returns {string} A path that definitely does not exist
 */
function getNonexistentPath() {
  if (process.platform === 'win32') {
    // Use a drive letter that almost certainly doesn't exist
    return 'Z:\\nonexistent\\path\\that\\does\\not\\exist';
  }
  return '/nonexistent/path/that/does/not/exist';
}

/**
 * Require test datasets to be available.
 * Use this in beforeAll() or at the start of tests that require real data.
 *
 * Note: This throws an error rather than skipping to match Python test behavior.
 * Missing test data is considered a setup failure, not a skippable condition.
 * This ensures CI failures are visible when test data is not properly configured.
 *
 * @throws {Error} If test data is not available
 * @example
 * beforeAll(() => {
 *   skipIfNoDatasets();
 * });
 */
function skipIfNoDatasets() {
  if (!global.DATASETS_AVAILABLE) {
    throw new Error('Test data not available. Set CQLITE_DATASETS_ROOT or run fetch-datasets.sh');
  }
}

/**
 * Create a database instance with the specified schema.
 * Caller is responsible for closing the database.
 *
 * @param {string} [schemaPath] - Path to schema file (defaults to basic-types.cql)
 * @returns {Promise<Database>}
 */
async function openDatabase(schemaPath) {
  const schema = schemaPath || global.testPaths.SCHEMA_BASIC_TYPES;
  return Database.open(global.testPaths.SSTABLES_DIR, { schema });
}

/**
 * Create a database instance that auto-closes after the callback.
 * Use this for tests that need a database for a single operation.
 *
 * @param {(db: Database) => Promise<void>} callback
 * @param {string} [schemaPath]
 *
 * @example
 * await withDatabase(async (db) => {
 *   const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');
 *   expect(result.rowCount).toBe(1);
 * });
 */
async function withDatabase(callback, schemaPath) {
  const db = await openDatabase(schemaPath);
  try {
    await callback(db);
  } finally {
    await db.close();
  }
}

module.exports = {
  skipIfNoDatasets,
  openDatabase,
  withDatabase,
  getNonexistentPath,
};
