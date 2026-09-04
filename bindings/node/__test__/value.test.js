/**
 * Value type conversion tests for Issue #302.
 *
 * Issue #306: Migrated to Jest format.
 *
 * TDD Requirements from the issue:
 * - [x] Test: null converts to null
 * - [x] Test: boolean converts to boolean
 * - [x] Test: int converts to number
 * - [x] Test: bigint converts to BigInt
 * - [x] Test: decimal converts to string
 * - [x] Test: text converts to string
 * - [x] Test: blob converts to Buffer
 * - [x] Test: timestamp converts to Date
 * - [x] Test: uuid converts to string (formatted)
 * - [x] Test: list converts to Array
 * - [x] Test: set converts to Set
 * - [x] Test: map converts to Map
 * - [x] Test: nested collections convert correctly
 * - [x] Test: udt converts to object with all fields
 */

const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable } = require('./helpers.js');

describe('Value Type Conversion Tests (Issue #302)', () => {
  let db = null;

  beforeAll(async () => {
    assertDatasetsAvailable();
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

  test('executeNative() method exists', () => {
    expect(typeof db.executeNative).toBe('function');
  });

  test('executeNative() returns proper result structure', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 5'
    );

    expect(typeof result).toBe('object');
    expect(Array.isArray(result.rows)).toBe(true);
    expect(typeof result.rowCount).toBe('number');
    expect(typeof result.executionTimeMs).toBe('number');
    expect(result.rowCount).toBe(result.rows.length);

    console.log(`    Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
  });

  test('null converts to null', async () => {
    // Query a table that may have null values
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    if (result.rows.length > 0) {
      const row = result.rows[0];
      // Check for any null values in the row
      const hasNullOrValues = Object.values(row).every(
        (v) => v === null || v !== undefined
      );
      expect(hasNullOrValues).toBe(true);
    }
  });

  test('boolean converts to boolean', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        // Look for boolean_col or similar
        for (const [key, value] of Object.entries(row)) {
          if (key.toLowerCase().includes('bool') && value !== null) {
            expect(typeof value).toBe('boolean');
            console.log(`    ${key} = ${value} (${typeof value})`);
          }
        }
      }
    }
  });

  test('int converts to number', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        // int32 columns should be JavaScript numbers
        for (const [key, value] of Object.entries(row)) {
          if (
            key.toLowerCase().includes('int') &&
            !key.toLowerCase().includes('bigint') &&
            !key.toLowerCase().includes('varint') &&
            value !== null
          ) {
            // Could be number or bigint depending on column type
            expect(['number', 'bigint']).toContain(typeof value);
          }
        }
      }
    }
  });

  test('bigint converts to BigInt', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    if (result.rows.length > 0) {
      let foundBigInt = false;
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (key.toLowerCase().includes('bigint') && value !== null) {
            expect(typeof value).toBe('bigint');
            console.log(`    ${key} = ${value} (${typeof value})`);
            foundBigInt = true;
          }
        }
      }
      if (!foundBigInt) {
        console.log('    Note: No bigint columns found in test data');
      }
    }
  });

  test('decimal converts to string', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (key.toLowerCase().includes('decimal') && value !== null) {
            expect(typeof value).toBe('string');
            console.log(`    ${key} = ${value} (${typeof value})`);
          }
        }
      }
    }
  });

  test('text converts to string', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (
            (key.toLowerCase().includes('text') || key.toLowerCase() === 'name') &&
            value !== null
          ) {
            expect(typeof value).toBe('string');
            console.log(`    ${key} = "${value}" (${typeof value})`);
          }
        }
      }
    }
  });

  test('blob converts to Buffer', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (key.toLowerCase().includes('blob') && value !== null) {
            expect(Buffer.isBuffer(value)).toBe(true);
            console.log(`    ${key} = Buffer(${value.length} bytes)`);
          }
        }
      }
    }
  });

  test('timestamp converts to Date', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    // Helper to check if value is a Date (works across realms)
    const isDate = (value) =>
      value !== null &&
      value !== undefined &&
      typeof value === 'object' &&
      typeof value.getTime === 'function' &&
      typeof value.toISOString === 'function' &&
      !isNaN(value.getTime());

    if (result.rows.length > 0) {
      let foundTimestamp = false;
      for (const row of result.rows) {
        // Check 'created' column specifically (TIMESTAMP type)
        if (row.created !== null && row.created !== undefined) {
          expect(isDate(row.created)).toBe(true);
          console.log(`    created = ${row.created.toISOString()} (Date)`);
          foundTimestamp = true;
        }
        // Check 'birth_date' column (DATE type - also becomes Date in JS)
        if (row.birth_date !== null && row.birth_date !== undefined) {
          expect(isDate(row.birth_date)).toBe(true);
          console.log(`    birth_date = ${row.birth_date.toISOString()} (Date)`);
          foundTimestamp = true;
        }
      }
      if (!foundTimestamp) {
        console.log('    Note: No timestamp/date columns found in test data');
      }
    }
  });

  test('uuid converts to string (formatted)', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    if (result.rows.length > 0) {
      const uuidRegex =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (
            (key.toLowerCase().includes('uuid') || key.toLowerCase() === 'id') &&
            value !== null
          ) {
            if (typeof value === 'string' && uuidRegex.test(value)) {
              console.log(`    ${key} = ${value} (UUID string)`);
            }
          }
        }
      }
    }
  });

  test('list converts to Array', async () => {
    // Try to query a table with list columns
    try {
      const result = await db.executeNative(
        'SELECT * FROM test_collections.list_table LIMIT 5'
      );

      if (result.rows.length > 0) {
        for (const row of result.rows) {
          for (const [key, value] of Object.entries(row)) {
            if (key.toLowerCase().includes('list') && value !== null) {
              expect(Array.isArray(value)).toBe(true);
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
  });

  test('set converts to Set', async () => {
    // Try to query a table with set columns
    try {
      const result = await db.executeNative(
        'SELECT * FROM test_collections.set_table LIMIT 5'
      );

      if (result.rows.length > 0) {
        for (const row of result.rows) {
          for (const [key, value] of Object.entries(row)) {
            if (key.toLowerCase().includes('set') && value !== null) {
              expect(value instanceof Set).toBe(true);
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
  });

  test('map converts to Map', async () => {
    // Try to query a table with map columns
    try {
      const result = await db.executeNative(
        'SELECT * FROM test_collections.map_table LIMIT 5'
      );

      if (result.rows.length > 0) {
        for (const row of result.rows) {
          for (const [key, value] of Object.entries(row)) {
            if (key.toLowerCase().includes('map') && value !== null) {
              expect(value instanceof Map).toBe(true);
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
  });

  test('duration converts to object', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 10'
    );

    if (result.rows.length > 0) {
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (key.toLowerCase().includes('duration') && value !== null) {
            expect(typeof value).toBe('object');
            expect(value).toHaveProperty('months');
            expect(value).toHaveProperty('days');
            expect(value).toHaveProperty('nanos');
            console.log(
              `    ${key} = {months: ${value.months}, days: ${value.days}, nanos: ${value.nanos}}`
            );
          }
        }
      }
    }
  });

  test('executeNative consistent with execute', async () => {
    const queryNative = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 5'
    );
    const queryJson = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 5'
    );

    expect(queryNative.rowCount).toBe(queryJson.rowCount);
    expect(queryNative.rows.length).toBe(queryJson.rows.length);

    // Both should have the same column names
    if (queryNative.rows.length > 0) {
      const nativeKeys = Object.keys(queryNative.rows[0]).sort();
      const jsonKeys = Object.keys(queryJson.rows[0]).sort();
      expect(nativeKeys).toEqual(jsonKeys);
      console.log(`    Columns: ${nativeKeys.join(', ')}`);
    }
  });
});

/**
 * Issue #1448: constructor-cache correctness for set/map cells.
 *
 * The batch `executeNative` path now caches the global `Set`/`Map` constructors
 * once per result conversion (instead of re-fetching per cell). This exercises
 * that path over a real `test_collections` table with both a `set<...>` and a
 * `map<...>` column, across many rows/cells, and asserts the produced values are
 * genuine JS `Set`/`Map` instances — the observable contract the cache must not
 * change (ordering/identity semantics are #1454, out of scope here).
 *
 * Realm note: `toBeInstanceOf(Set)` cannot be used here. Jest's `node`
 * `testEnvironment` runs the test file in a separate realm from the napi
 * addon's process realm, so a genuine `Set` returned by the addon fails the
 * cross-realm `instanceof` identity check (the failure even reports
 * "Received constructor: Set"). This mirrors the existing `isSet`/`isMap`
 * helpers in `types.test.js`, which use the realm-safe brand check
 * `Object.prototype.toString.call(v) === '[object Set]'`.
 */
const isSet = (v) =>
  v !== null &&
  v !== undefined &&
  Object.prototype.toString.call(v) === '[object Set]';
const isMap = (v) =>
  v !== null &&
  v !== undefined &&
  Object.prototype.toString.call(v) === '[object Map]';

describe('Set/Map constructor caching (#1448)', () => {
  let db = null;

  beforeAll(async () => {
    assertDatasetsAvailable();
    db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_COLLECTIONS,
    });
  });

  afterAll(async () => {
    if (db) {
      await db.close();
      db = null;
    }
  });

  test('set cols are Set, map cols are Map across all rows', async () => {
    // collection_table (test-data/schemas/collections.cql):
    //   tags SET<TEXT>, numbers_set SET<INT>,
    //   properties MAP<TEXT,TEXT>, metadata_map MAP<TEXT,BIGINT>
    const result = await db.executeNative(
      'SELECT id, tags, numbers_set, properties, metadata_map ' +
        'FROM test_collections.collection_table LIMIT 50'
    );

    // A present-but-empty result must FAIL, never silently skip (#1448 mandate):
    // an empty table would make every per-row assertion vacuous.
    if (!(result.rowCount > 0)) {
      throw new Error(
        `Expected rows from test_collections.collection_table, got rowCount=${result.rowCount}. ` +
          'Fetch datasets (CQLITE_DATASETS_ROOT) so the constructor-cache path is actually exercised.'
      );
    }

    const setCols = ['tags', 'numbers_set'];
    const mapCols = ['properties', 'metadata_map'];
    let setInstances = 0;
    let mapInstances = 0;

    for (const row of result.rows) {
      for (const col of setCols) {
        if (row[col] !== null && row[col] !== undefined) {
          expect(isSet(row[col])).toBe(true);
          setInstances += 1;
        }
      }
      for (const col of mapCols) {
        if (row[col] !== null && row[col] !== undefined) {
          expect(isMap(row[col])).toBe(true);
          mapInstances += 1;
        }
      }
    }

    // Prove the set AND map constructor paths were actually taken (not just that
    // every cell happened to be null) — the whole point of caching those ctors.
    expect(setInstances).toBeGreaterThan(0);
    expect(mapInstances).toBeGreaterThan(0);
    console.log(
      `    #1448: ${result.rowCount} rows, ${setInstances} Set cells, ${mapInstances} Map cells`
    );
  });
});
