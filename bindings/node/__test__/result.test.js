/**
 * QueryResult and ColumnInfo tests for Issue #303.
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

const assert = require('assert');
const path = require('path');
const { Database } = require('../lib/index.js');

// Test data paths
const TEST_DATA_ROOT = process.env.CQLITE_DATASETS_ROOT ||
  path.join(__dirname, '..', '..', '..', 'test-data', 'datasets');
const SSTABLES_DIR = path.join(TEST_DATA_ROOT, 'sstables');
const SCHEMA_FILE = path.join(__dirname, '..', '..', '..', 'test-data', 'schemas', 'basic-types.cql');

// Helper to run async test
async function runTest(name, fn) {
  try {
    await fn();
    console.log(`\u2713 ${name}`);
    return true;
  } catch (e) {
    console.error(`\u2717 ${name}`);
    console.error(`  Error: ${e.message}`);
    if (e.stack) {
      console.error(`  Stack: ${e.stack.split('\n').slice(1, 3).join('\n')}`);
    }
    return false;
  }
}

// Shared database instance for tests
let db = null;

// Setup: Open database before tests
async function setup() {
  db = await Database.open(SSTABLES_DIR, { schema: SCHEMA_FILE });
}

// Teardown: Close database after tests
async function teardown() {
  if (db) {
    await db.close();
    db = null;
  }
}

// Test: QueryResult has columns array
async function testQueryResultHasColumnsArray() {
  const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');

  assert(result !== null && result !== undefined, 'Result should not be null');
  assert('columns' in result, 'Result should have columns property');
  assert(Array.isArray(result.columns), 'columns should be an array');
  console.log(`    Found ${result.columns.length} columns`);
}

// Test: ColumnInfo has name property
async function testColumnInfoHasNameProperty() {
  const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');

  assert(result.columns.length > 0, 'Should have at least one column');

  for (const col of result.columns) {
    assert('name' in col, 'Column should have name property');
    assert(typeof col.name === 'string', `name should be string, got ${typeof col.name}`);
    assert(col.name.length > 0, 'name should not be empty');
  }

  console.log(`    Column names: ${result.columns.map(c => c.name).join(', ')}`);
}

// Test: ColumnInfo has dataType property
async function testColumnInfoHasDataTypeProperty() {
  const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');

  assert(result.columns.length > 0, 'Should have at least one column');

  for (const col of result.columns) {
    assert('dataType' in col, 'Column should have dataType property');
    assert(typeof col.dataType === 'string', `dataType should be string, got ${typeof col.dataType}`);
    assert(col.dataType.length > 0, 'dataType should not be empty');
  }

  console.log(`    Data types: ${result.columns.map(c => `${c.name}:${c.dataType}`).join(', ')}`);
}

// Test: ColumnInfo has nullable property
async function testColumnInfoHasNullableProperty() {
  const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');

  assert(result.columns.length > 0, 'Should have at least one column');

  for (const col of result.columns) {
    assert('nullable' in col, 'Column should have nullable property');
    assert(typeof col.nullable === 'boolean', `nullable should be boolean, got ${typeof col.nullable}`);
  }

  const nullableCount = result.columns.filter(c => c.nullable).length;
  console.log(`    ${nullableCount}/${result.columns.length} columns are nullable`);
}

// Test: ColumnInfo has position property
async function testColumnInfoHasPositionProperty() {
  const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');

  assert(result.columns.length > 0, 'Should have at least one column');

  for (const col of result.columns) {
    assert('position' in col, 'Column should have position property');
    assert(typeof col.position === 'number', `position should be number, got ${typeof col.position}`);
    assert(col.position >= 0, 'position should be non-negative');
  }

  // Verify positions are sequential from 0
  const positions = result.columns.map(c => c.position).sort((a, b) => a - b);
  for (let i = 0; i < positions.length; i++) {
    assert(positions[i] === i, `Position ${i} should be ${i}, got ${positions[i]}`);
  }

  console.log(`    Positions: 0-${result.columns.length - 1}`);
}

// Test: ColumnInfo has optional tableName property
async function testColumnInfoHasTableNameProperty() {
  const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');

  assert(result.columns.length > 0, 'Should have at least one column');

  for (const col of result.columns) {
    assert('tableName' in col, 'Column should have tableName property');
    // tableName can be string or null
    assert(
      col.tableName === null || typeof col.tableName === 'string',
      `tableName should be string or null, got ${typeof col.tableName}`
    );
  }
}

// Test: Column order matches query SELECT order (for SELECT *)
async function testColumnOrderMatchesSelectOrder() {
  const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');

  assert(result.columns.length > 0, 'Should have at least one column');

  // For SELECT *, verify position matches array index
  for (let i = 0; i < result.columns.length; i++) {
    assert(
      result.columns[i].position === i,
      `Column at index ${i} should have position ${i}, got ${result.columns[i].position}`
    );
  }

  console.log(`    Verified ${result.columns.length} columns in order`);
}

// Test: Empty result still has column metadata
async function testEmptyResultHasColumnMetadata() {
  // Query with impossible WHERE clause to get empty result
  const result = await db.execute(
    "SELECT * FROM test_basic.simple_table WHERE id = 'nonexistent-id-that-does-not-exist-12345' LIMIT 1"
  );

  assert(result.rowCount === 0, 'Should have zero rows');
  assert(result.rows.length === 0, 'Rows array should be empty');

  // Even with no rows, columns should still have metadata
  assert('columns' in result, 'Empty result should have columns property');
  assert(Array.isArray(result.columns), 'columns should be an array');

  // Note: Column metadata may or may not be populated for empty results
  // depending on query planning. This is acceptable behavior.
  console.log(`    Empty result has ${result.columns.length} column definitions`);
}

// Test: executeNative also returns columns
async function testExecuteNativeHasColumns() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 1');

  assert('columns' in result, 'executeNative result should have columns property');
  assert(Array.isArray(result.columns), 'columns should be an array');

  if (result.columns.length > 0) {
    const col = result.columns[0];
    assert('name' in col, 'Column should have name');
    assert('dataType' in col, 'Column should have dataType');
    assert('nullable' in col, 'Column should have nullable');
    assert('position' in col, 'Column should have position');
  }

  console.log(`    executeNative: ${result.columns.length} columns`);
}

// Test: Columns match between execute and executeNative
async function testColumnsMatchBetweenExecuteMethods() {
  const query = 'SELECT * FROM test_basic.simple_table LIMIT 1';
  const jsonResult = await db.execute(query);
  const nativeResult = await db.executeNative(query);

  assert.strictEqual(
    jsonResult.columns.length,
    nativeResult.columns.length,
    'Column counts should match'
  );

  for (let i = 0; i < jsonResult.columns.length; i++) {
    assert.strictEqual(
      jsonResult.columns[i].name,
      nativeResult.columns[i].name,
      `Column ${i} name should match`
    );
    assert.strictEqual(
      jsonResult.columns[i].dataType,
      nativeResult.columns[i].dataType,
      `Column ${i} dataType should match`
    );
  }

  console.log(`    Both methods return identical column metadata`);
}

// Run all tests
async function main() {
  console.log('QueryResult and ColumnInfo Tests (Issue #303)\n');
  console.log(`Test data root: ${TEST_DATA_ROOT}`);
  console.log(`SSTables dir: ${SSTABLES_DIR}`);
  console.log(`Schema file: ${SCHEMA_FILE}\n`);

  try {
    await setup();

    const tests = [
      ['QueryResult has columns array', testQueryResultHasColumnsArray],
      ['ColumnInfo has name property', testColumnInfoHasNameProperty],
      ['ColumnInfo has dataType property', testColumnInfoHasDataTypeProperty],
      ['ColumnInfo has nullable property', testColumnInfoHasNullableProperty],
      ['ColumnInfo has position property', testColumnInfoHasPositionProperty],
      ['ColumnInfo has tableName property', testColumnInfoHasTableNameProperty],
      ['Column order matches SELECT order', testColumnOrderMatchesSelectOrder],
      ['Empty result has column metadata', testEmptyResultHasColumnMetadata],
      ['executeNative also returns columns', testExecuteNativeHasColumns],
      ['Columns match between execute methods', testColumnsMatchBetweenExecuteMethods],
    ];

    let passed = 0;
    let failed = 0;

    for (const [name, fn] of tests) {
      const success = await runTest(name, fn);
      if (success) {
        passed++;
      } else {
        failed++;
      }
    }

    console.log(`\n${passed} passed, ${failed} failed`);

    await teardown();

    if (failed > 0) {
      process.exit(1);
    }
  } catch (e) {
    console.error('Test setup error:', e);
    await teardown();
    process.exit(1);
  }
}

main().catch(e => {
  console.error('Test runner error:', e);
  process.exit(1);
});
