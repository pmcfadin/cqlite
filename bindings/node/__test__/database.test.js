/**
 * Database wrapper tests for Issue #296.
 *
 * Issue #306: Migrated to Jest format.
 *
 * TDD Requirements from the issue:
 * - [x] Test: Database.open() returns Database instance
 * - [x] Test: Database.open() with invalid path rejects with IoError
 * - [x] Test: Database.execute() returns QueryResult
 * - [x] Test: Database.execute() with invalid SQL rejects with ParseError
 * - [x] Test: Database.close() resolves successfully
 * - [x] Test: Database.getStats() returns valid statistics
 */

const { Database, version } = require('../lib/index.js');
const { skipIfNoDatasets } = require('./helpers.js');

describe('Database Wrapper Tests (Issue #296)', () => {
  beforeAll(() => {
    console.log(`Test data root: ${global.testPaths.TEST_DATA_ROOT}`);
    console.log(`SSTables dir: ${global.testPaths.SSTABLES_DIR}`);
    console.log(`Schema file: ${global.testPaths.SCHEMA_BASIC_TYPES}`);
  });

  test('version() returns semver string', () => {
    const v = version();
    expect(typeof v).toBe('string');
    expect(v).toMatch(/^\d+\.\d+\.\d+/);
  });

  test('Database.open() returns Database instance', async () => {
    skipIfNoDatasets();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    expect(db).not.toBeNull();
    expect(db).not.toBeUndefined();
    expect(typeof db.execute).toBe('function');
    expect(typeof db.close).toBe('function');
    expect(typeof db.getStats).toBe('function');
    await db.close();
  });

  test('Database.open() with invalid path rejects with IoError', async () => {
    expect.assertions(2);
    try {
      await Database.open('/nonexistent/path/that/does/not/exist');
    } catch (e) {
      expect(e).toBeDefined();
      expect(
        e.message.includes('IoError') || e.message.includes('No such file')
      ).toBe(true);
    }
  });

  test('Database.execute() returns QueryResult', async () => {
    skipIfNoDatasets();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      const result = await db.execute(
        'SELECT * FROM test_basic.simple_table LIMIT 5'
      );

      // Verify QueryResult structure
      expect(typeof result).toBe('object');
      expect(Array.isArray(result.rows)).toBe(true);
      expect(typeof result.rowCount).toBe('number');
      expect(typeof result.executionTimeMs).toBe('number');

      // Verify row count matches rows array length
      expect(result.rowCount).toBe(result.rows.length);

      // Log for debugging
      console.log(`    Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
      if (result.rows.length > 0) {
        console.log(`    First row keys: ${Object.keys(result.rows[0]).join(', ')}`);
      }
    } finally {
      await db.close();
    }
  });

  test('Database.execute() with invalid SQL rejects with ParseError or QueryError', async () => {
    skipIfNoDatasets();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      expect.assertions(1);
      await db.execute('THIS IS NOT VALID SQL AT ALL!!!');
    } catch (e) {
      // Note: The parser may successfully parse "THIS" as a token, but the query executor
      // will reject it as an unsupported query type. Both ParseError and QueryError are valid.
      expect(
        e.message.includes('ParseError') ||
          e.message.includes('QueryError') ||
          e.message.includes('parse') ||
          e.message.includes('syntax') ||
          e.message.includes('Unsupported')
      ).toBe(true);
    } finally {
      await db.close();
    }
  });

  test('Database.close() resolves successfully', async () => {
    skipIfNoDatasets();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });

    // First close should succeed
    await db.close();

    // Second close should also succeed (idempotent)
    await db.close();

    // Verify isClosed getter
    expect(db.isClosed).toBe(true);
  });

  test('Database.getStats() returns valid statistics', async () => {
    skipIfNoDatasets();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      const stats = await db.getStats();

      // Verify DatabaseStats structure
      expect(typeof stats).toBe('object');
      expect(typeof stats.totalSstables).toBe('number');
      expect(
        typeof stats.totalRows === 'number' || typeof stats.totalRows === 'bigint'
      ).toBe(true);
      expect(
        typeof stats.memoryUsedBytes === 'number' ||
          typeof stats.memoryUsedBytes === 'bigint'
      ).toBe(true);

      // Log for debugging
      console.log(`    SSTables: ${stats.totalSstables}`);
      console.log(`    Total rows: ${stats.totalRows}`);
      console.log(`    Memory: ${stats.memoryUsedBytes} bytes`);
    } finally {
      await db.close();
    }
  });

  test('Operations on closed database fail', async () => {
    skipIfNoDatasets();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    await db.close();

    // execute should fail
    await expect(
      db.execute('SELECT * FROM test_basic.simple_table LIMIT 1')
    ).rejects.toThrow(/closed/);

    // getStats should fail
    await expect(db.getStats()).rejects.toThrow(/closed/);
  });
});
