/**
 * QueryResult and ColumnInfo tests for Issue #303.
 *
 * Issue #306: Migrated to Jest format.
 *
 * TDD Requirements from the issue:
 * - [x] Test: QueryResult has columns array
 * - [x] Test: ColumnInfo has name property
 * - [x] Test: ColumnInfo has dataType property
 * - [x] Test: ColumnInfo has nullable property
 * - [x] Test: ColumnInfo has position property
 * - [x] Test: Column order matches query SELECT order
 * - [x] Test: Empty result still has column metadata
 */

const { Database } = require('../lib/index.js');
const { skipIfNoDatasets } = require('./helpers.js');

describe('QueryResult and ColumnInfo Tests (Issue #303)', () => {
  let db = null;

  beforeAll(async () => {
    skipIfNoDatasets();
    console.log(`Test data root: ${global.testPaths.TEST_DATA_ROOT}`);
    console.log(`SSTables dir: ${global.testPaths.SSTABLES_DIR}`);
    console.log(`Schema file: ${global.testPaths.SCHEMA_BASIC_TYPES}`);
    db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
  });

  afterAll(async () => {
    if (db) {
      await db.close();
      db = null;
    }
  });

  test('QueryResult has columns array', async () => {
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    expect(result).not.toBeNull();
    expect(result).not.toBeUndefined();
    expect(result).toHaveProperty('columns');
    expect(Array.isArray(result.columns)).toBe(true);
    console.log(`    Found ${result.columns.length} columns`);
  });

  test('ColumnInfo has name property', async () => {
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    expect(result.columns.length).toBeGreaterThan(0);

    for (const col of result.columns) {
      expect(col).toHaveProperty('name');
      expect(typeof col.name).toBe('string');
      expect(col.name.length).toBeGreaterThan(0);
    }

    console.log(`    Column names: ${result.columns.map((c) => c.name).join(', ')}`);
  });

  test('ColumnInfo has dataType property', async () => {
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    expect(result.columns.length).toBeGreaterThan(0);

    for (const col of result.columns) {
      expect(col).toHaveProperty('dataType');
      expect(typeof col.dataType).toBe('string');
      expect(col.dataType.length).toBeGreaterThan(0);
    }

    console.log(
      `    Data types: ${result.columns.map((c) => `${c.name}:${c.dataType}`).join(', ')}`
    );
  });

  test('ColumnInfo has nullable property', async () => {
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    expect(result.columns.length).toBeGreaterThan(0);

    for (const col of result.columns) {
      expect(col).toHaveProperty('nullable');
      expect(typeof col.nullable).toBe('boolean');
    }

    const nullableCount = result.columns.filter((c) => c.nullable).length;
    console.log(`    ${nullableCount}/${result.columns.length} columns are nullable`);
  });

  test('ColumnInfo has position property', async () => {
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    expect(result.columns.length).toBeGreaterThan(0);

    for (const col of result.columns) {
      expect(col).toHaveProperty('position');
      expect(typeof col.position).toBe('number');
      expect(col.position).toBeGreaterThanOrEqual(0);
    }

    // Verify positions are sequential from 0
    const positions = result.columns.map((c) => c.position).sort((a, b) => a - b);
    for (let i = 0; i < positions.length; i++) {
      expect(positions[i]).toBe(i);
    }

    console.log(`    Positions: 0-${result.columns.length - 1}`);
  });

  test('ColumnInfo has tableName property', async () => {
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    expect(result.columns.length).toBeGreaterThan(0);

    for (const col of result.columns) {
      expect(col).toHaveProperty('tableName');
      // tableName can be string or null
      expect(col.tableName === null || typeof col.tableName === 'string').toBe(
        true
      );
    }
  });

  test('Column order matches SELECT order', async () => {
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    expect(result.columns.length).toBeGreaterThan(0);

    // For SELECT *, verify position matches array index
    for (let i = 0; i < result.columns.length; i++) {
      expect(result.columns[i].position).toBe(i);
    }

    console.log(`    Verified ${result.columns.length} columns in order`);
  });

  test('Empty result has column metadata', async () => {
    // Query with impossible WHERE clause to get empty result
    const result = await db.execute(
      "SELECT * FROM test_basic.simple_table WHERE id = 'nonexistent-id-that-does-not-exist-12345' LIMIT 1"
    );

    expect(result.rowCount).toBe(0);
    expect(result.rows.length).toBe(0);

    // Even with no rows, columns property should exist and be an array
    expect(result).toHaveProperty('columns');
    expect(Array.isArray(result.columns)).toBe(true);

    // Column metadata should be populated even for empty results
    // This allows schema discovery without requiring data
    expect(result.columns.length).toBeGreaterThan(0);
    console.log(`    Empty result has ${result.columns.length} column definitions`);
  });

  test('executeNative also returns columns', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    expect(result).toHaveProperty('columns');
    expect(Array.isArray(result.columns)).toBe(true);

    if (result.columns.length > 0) {
      const col = result.columns[0];
      expect(col).toHaveProperty('name');
      expect(col).toHaveProperty('dataType');
      expect(col).toHaveProperty('nullable');
      expect(col).toHaveProperty('position');
    }

    console.log(`    executeNative: ${result.columns.length} columns`);
  });

  test('Columns match between execute methods', async () => {
    const query = 'SELECT * FROM test_basic.simple_table LIMIT 1';
    const jsonResult = await db.execute(query);
    const nativeResult = await db.executeNative(query);

    expect(jsonResult.columns.length).toBe(nativeResult.columns.length);

    for (let i = 0; i < jsonResult.columns.length; i++) {
      expect(jsonResult.columns[i].name).toBe(nativeResult.columns[i].name);
      expect(jsonResult.columns[i].dataType).toBe(nativeResult.columns[i].dataType);
    }

    console.log(`    Both methods return identical column metadata`);
  });
});
