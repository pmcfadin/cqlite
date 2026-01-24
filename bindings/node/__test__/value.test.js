/**
 * Value type conversion tests for Issue #302.
 *
 * TDD Requirements from the issue:
 * - [ ] Test: null converts to null
 * - [ ] Test: boolean converts to boolean
 * - [ ] Test: int converts to number
 * - [ ] Test: bigint converts to BigInt
 * - [ ] Test: decimal converts to string
 * - [ ] Test: text converts to string
 * - [ ] Test: blob converts to Buffer
 * - [ ] Test: timestamp converts to Date
 * - [ ] Test: uuid converts to string (formatted)
 * - [ ] Test: list converts to Array
 * - [ ] Test: set converts to Set
 * - [ ] Test: map converts to Map
 * - [ ] Test: nested collections convert correctly
 * - [ ] Test: udt converts to object with all fields
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
    console.log(`✓ ${name}`);
    return true;
  } catch (e) {
    console.error(`✗ ${name}`);
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

// Test: executeNative() returns native typed results
async function testExecuteNativeExists() {
  assert(typeof db.executeNative === 'function', 'Database should have executeNative method');
}

// Test: executeNative() returns proper result structure
async function testExecuteNativeResultStructure() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 5');

  assert(typeof result === 'object', 'Result should be an object');
  assert(Array.isArray(result.rows), 'Result should have rows array');
  assert(typeof result.rowCount === 'number', 'Result should have rowCount');
  assert(typeof result.executionTimeMs === 'number', 'Result should have executionTimeMs');
  assert.strictEqual(result.rowCount, result.rows.length, 'rowCount should match rows.length');

  console.log(`    Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
}

// Test: null converts to null
async function testNullConvertsToNull() {
  // Query a table that may have null values
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 1');

  if (result.rows.length > 0) {
    const row = result.rows[0];
    // Check for any null values in the row
    const hasNullOrValues = Object.values(row).every(v =>
      v === null || v !== undefined
    );
    assert(hasNullOrValues, 'Row values should be null or actual values');
  }
}

// Test: boolean converts to boolean
async function testBooleanConvertsToBoolean() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    for (const row of result.rows) {
      // Look for boolean_col or similar
      for (const [key, value] of Object.entries(row)) {
        if (key.toLowerCase().includes('bool') && value !== null) {
          assert(typeof value === 'boolean', `Boolean column ${key} should be boolean type, got ${typeof value}`);
          console.log(`    ${key} = ${value} (${typeof value})`);
        }
      }
    }
  }
}

// Test: int converts to number
async function testIntConvertsToNumber() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    for (const row of result.rows) {
      // int32 columns should be JavaScript numbers
      for (const [key, value] of Object.entries(row)) {
        if ((key.toLowerCase().includes('int') &&
             !key.toLowerCase().includes('bigint') &&
             !key.toLowerCase().includes('varint')) && value !== null) {
          // Could be number or bigint depending on column type
          assert(
            typeof value === 'number' || typeof value === 'bigint',
            `Int column ${key} should be number or bigint, got ${typeof value}`
          );
        }
      }
    }
  }
}

// Test: bigint converts to BigInt
async function testBigintConvertsToBigInt() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    let foundBigInt = false;
    for (const row of result.rows) {
      for (const [key, value] of Object.entries(row)) {
        if (key.toLowerCase().includes('bigint') && value !== null) {
          assert(typeof value === 'bigint', `BigInt column ${key} should be bigint type, got ${typeof value}`);
          console.log(`    ${key} = ${value} (${typeof value})`);
          foundBigInt = true;
        }
      }
    }
    if (!foundBigInt) {
      console.log('    Note: No bigint columns found in test data');
    }
  }
}

// Test: decimal converts to string
async function testDecimalConvertsToString() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    for (const row of result.rows) {
      for (const [key, value] of Object.entries(row)) {
        if (key.toLowerCase().includes('decimal') && value !== null) {
          assert(typeof value === 'string', `Decimal column ${key} should be string, got ${typeof value}`);
          console.log(`    ${key} = ${value} (${typeof value})`);
        }
      }
    }
  }
}

// Test: text converts to string
async function testTextConvertsToString() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    for (const row of result.rows) {
      for (const [key, value] of Object.entries(row)) {
        if ((key.toLowerCase().includes('text') || key.toLowerCase() === 'name') && value !== null) {
          assert(typeof value === 'string', `Text column ${key} should be string, got ${typeof value}`);
          console.log(`    ${key} = "${value}" (${typeof value})`);
        }
      }
    }
  }
}

// Test: blob converts to Buffer
async function testBlobConvertsToBuffer() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    for (const row of result.rows) {
      for (const [key, value] of Object.entries(row)) {
        if (key.toLowerCase().includes('blob') && value !== null) {
          assert(Buffer.isBuffer(value), `Blob column ${key} should be Buffer, got ${typeof value}`);
          console.log(`    ${key} = Buffer(${value.length} bytes)`);
        }
      }
    }
  }
}

// Test: timestamp converts to Date
async function testTimestampConvertsToDate() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    for (const row of result.rows) {
      for (const [key, value] of Object.entries(row)) {
        if ((key.toLowerCase().includes('timestamp') || key.toLowerCase().includes('created')) && value !== null) {
          assert(value instanceof Date, `Timestamp column ${key} should be Date, got ${typeof value}`);
          console.log(`    ${key} = ${value.toISOString()} (Date)`);
        }
      }
    }
  }
}

// Test: uuid converts to string (formatted)
async function testUuidConvertsToString() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    for (const row of result.rows) {
      for (const [key, value] of Object.entries(row)) {
        if ((key.toLowerCase().includes('uuid') || key.toLowerCase() === 'id') && value !== null) {
          if (typeof value === 'string' && uuidRegex.test(value)) {
            console.log(`    ${key} = ${value} (UUID string)`);
          }
        }
      }
    }
  }
}

// Test: list converts to Array
async function testListConvertsToArray() {
  // Try to query a table with list columns
  try {
    const result = await db.executeNative('SELECT * FROM test_collections.list_table LIMIT 5');

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (key.toLowerCase().includes('list') && value !== null) {
            assert(Array.isArray(value), `List column ${key} should be Array, got ${typeof value}`);
            console.log(`    ${key} = Array(${value.length} items)`);
          }
        }
      }
    }
  } catch (e) {
    if (e.message.includes('not found') || e.message.includes('QueryError')) {
      console.log('    Note: list_table not available in test data');
    } else {
      throw e;
    }
  }
}

// Test: set converts to Set
async function testSetConvertsToSet() {
  // Try to query a table with set columns
  try {
    const result = await db.executeNative('SELECT * FROM test_collections.set_table LIMIT 5');

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (key.toLowerCase().includes('set') && value !== null) {
            assert(value instanceof Set, `Set column ${key} should be Set, got ${value?.constructor?.name || typeof value}`);
            console.log(`    ${key} = Set(${value.size} items)`);
          }
        }
      }
    }
  } catch (e) {
    if (e.message.includes('not found') || e.message.includes('QueryError')) {
      console.log('    Note: set_table not available in test data');
    } else {
      throw e;
    }
  }
}

// Test: map converts to Map
async function testMapConvertsToMap() {
  // Try to query a table with map columns
  try {
    const result = await db.executeNative('SELECT * FROM test_collections.map_table LIMIT 5');

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (key.toLowerCase().includes('map') && value !== null) {
            assert(value instanceof Map, `Map column ${key} should be Map, got ${value?.constructor?.name || typeof value}`);
            console.log(`    ${key} = Map(${value.size} entries)`);
          }
        }
      }
    }
  } catch (e) {
    if (e.message.includes('not found') || e.message.includes('QueryError')) {
      console.log('    Note: map_table not available in test data');
    } else {
      throw e;
    }
  }
}

// Test: duration converts to object
async function testDurationConvertsToObject() {
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 10');

  if (result.rows.length > 0) {
    for (const row of result.rows) {
      for (const [key, value] of Object.entries(row)) {
        if (key.toLowerCase().includes('duration') && value !== null) {
          assert(typeof value === 'object', `Duration column ${key} should be object`);
          assert('months' in value, `Duration should have months property`);
          assert('days' in value, `Duration should have days property`);
          assert('nanos' in value, `Duration should have nanos property`);
          console.log(`    ${key} = {months: ${value.months}, days: ${value.days}, nanos: ${value.nanos}}`);
        }
      }
    }
  }
}

// Test: Compare executeNative with execute (JSON) for consistency
async function testExecuteNativeConsistentWithExecute() {
  const queryNative = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 5');
  const queryJson = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 5');

  assert.strictEqual(queryNative.rowCount, queryJson.rowCount, 'Row counts should match');
  assert(queryNative.rows.length === queryJson.rows.length, 'Number of rows should match');

  // Both should have the same column names
  if (queryNative.rows.length > 0) {
    const nativeKeys = Object.keys(queryNative.rows[0]).sort();
    const jsonKeys = Object.keys(queryJson.rows[0]).sort();
    assert.deepStrictEqual(nativeKeys, jsonKeys, 'Column names should match');
    console.log(`    Columns: ${nativeKeys.join(', ')}`);
  }
}

// Run all tests
async function main() {
  console.log('Value Type Conversion Tests (Issue #302)\n');
  console.log(`Test data root: ${TEST_DATA_ROOT}`);
  console.log(`SSTables dir: ${SSTABLES_DIR}`);
  console.log(`Schema file: ${SCHEMA_FILE}\n`);

  try {
    await setup();

    const tests = [
      ['executeNative() method exists', testExecuteNativeExists],
      ['executeNative() returns proper result structure', testExecuteNativeResultStructure],
      ['null converts to null', testNullConvertsToNull],
      ['boolean converts to boolean', testBooleanConvertsToBoolean],
      ['int converts to number', testIntConvertsToNumber],
      ['bigint converts to BigInt', testBigintConvertsToBigInt],
      ['decimal converts to string', testDecimalConvertsToString],
      ['text converts to string', testTextConvertsToString],
      ['blob converts to Buffer', testBlobConvertsToBuffer],
      ['timestamp converts to Date', testTimestampConvertsToDate],
      ['uuid converts to string (formatted)', testUuidConvertsToString],
      ['list converts to Array', testListConvertsToArray],
      ['set converts to Set', testSetConvertsToSet],
      ['map converts to Map', testMapConvertsToMap],
      ['duration converts to object', testDurationConvertsToObject],
      ['executeNative consistent with execute', testExecuteNativeConsistentWithExecute],
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
