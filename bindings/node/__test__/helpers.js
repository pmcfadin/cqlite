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
 * Assert that the SSTable test corpus is available, THROWING if it is not.
 * Use this in beforeAll() or at the start of tests that require real data.
 *
 * IT DOES NOT SKIP, AND THE NAME IS THE POINT (issue #3641). This helper was
 * called `skipIfNoDatasets()` for its whole life while throwing on every call,
 * and the misnomer shaped a merge gate's design: a reader who trusted the name
 * expected a reduced-coverage run to be available over an absent corpus, so the
 * gate's `node-bindings` component looked like it could run "leniently". It
 * cannot -- measured on an empty root, `prepared.test.js` reports 16 FAILED of
 * 16 total, not 16 skipped -- which is why that component SKIPs wholesale under
 * `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` instead.
 *
 * The throwing BEHAVIOUR is deliberate and stays (issue #1458): a silent skip
 * over an absent corpus is how #646-class holes hide, and this repo's Node
 * convention is to fail loudly instead. Renaming this back to a `skip`-word, or
 * making it actually skip, reintroduces that hole and needs #1458 re-argued.
 *
 * NOT `requireDatasets()`, deliberately: `require` is already this suite's
 * STRICT-MODE vocabulary (`CQLITE_REQUIRE_FIXTURES` /
 * `CQLITE_PARITY_REQUIRE_DATASETS` -> `global.REQUIRE_FIXTURES` in setup.js),
 * and this assertion is INDEPENDENT of strict mode -- it fires on an absent
 * corpus whether or not strict mode is on. A `require`-named helper beside that
 * global invites the reading "this only fires in strict mode", which is the
 * same class of misreading the rename exists to remove.
 *
 * @throws {Error} If test data is not available
 * @example
 * beforeAll(() => {
 *   assertDatasetsAvailable();
 * });
 */
function assertDatasetsAvailable() {
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
  assertDatasetsAvailable,
  openDatabase,
  withDatabase,
  getNonexistentPath,
};
