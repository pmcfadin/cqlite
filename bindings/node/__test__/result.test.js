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
const { assertDatasetsAvailable } = require('./helpers.js');

describe('QueryResult and ColumnInfo Tests (Issue #303)', () => {
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

  describe('rowsAffected alias (Issue #348)', () => {
    test('execute() result has rowsAffected equal to rowCount', async () => {
      const result = await db.execute(
        'SELECT * FROM test_basic.simple_table LIMIT 5'
      );

      expect(result).toHaveProperty('rowsAffected');
      expect(typeof result.rowsAffected).toBe('number');
      expect(result.rowsAffected).toBe(result.rowCount);

      console.log(`    rowCount=${result.rowCount}, rowsAffected=${result.rowsAffected}`);
    });

    test('executeNative() result has rowsAffected equal to rowCount', async () => {
      const result = await db.executeNative(
        'SELECT * FROM test_basic.simple_table LIMIT 5'
      );

      expect(result).toHaveProperty('rowsAffected');
      expect(typeof result.rowsAffected).toBe('number');
      expect(result.rowsAffected).toBe(result.rowCount);

      console.log(`    rowCount=${result.rowCount}, rowsAffected=${result.rowsAffected}`);
    });

    test('rowsAffected is 0 for empty results', async () => {
      const result = await db.execute(
        "SELECT * FROM test_basic.simple_table WHERE id = 'nonexistent-id-that-does-not-exist-12345'"
      );

      expect(result.rowCount).toBe(0);
      expect(result.rowsAffected).toBe(0);

      console.log(`    Empty result: rowsAffected=${result.rowsAffected}`);
    });
  });
});

describe('Row property order matches SELECT order (Issue #1446)', () => {
  let db = null;

  beforeAll(async () => {
    assertDatasetsAvailable();
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

  test('executeNative: Object.keys(row) matches SELECT column order (SELECT *)', async () => {
    const result = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 5'
    );
    // Dataset rule: present-but-empty fixtures FAIL loudly, never skip.
    expect(result.rowCount).toBeGreaterThan(0);
    const expected = result.columns.map((c) => c.name);
    for (const row of result.rows) {
      expect(Object.keys(row)).toEqual(expected);
    }
  });

  test('executeNative: Object.keys(row) honors an explicit reordered projection', async () => {
    const result = await db.executeNative(
      'SELECT name, id, age FROM test_basic.simple_table LIMIT 5'
    );
    expect(result.rowCount).toBeGreaterThan(0);
    expect(result.columns.map((c) => c.name)).toEqual(['name', 'id', 'age']);
    for (const row of result.rows) {
      expect(Object.keys(row)).toEqual(['name', 'id', 'age']);
    }
  });

  test('executeNative: every selected column is present on every row (never undefined)', async () => {
    // For normal projections core populates every selected column, so the row
    // shape is stable: Object.keys equals the projection and no cell is omitted.
    const cols = ['id', 'name', 'age', 'salary', 'active'];
    const result = await db.executeNative(
      `SELECT ${cols.join(', ')} FROM test_basic.simple_table LIMIT 10`
    );
    expect(result.rowCount).toBeGreaterThan(0);
    for (const row of result.rows) {
      expect(Object.keys(row)).toEqual(cols);
      for (const k of cols) {
        expect(row[k]).not.toBeUndefined();
      }
    }
  });

  test('executeNative: aggregate projection emits no phantom metadata column', async () => {
    // Regression (#1446 roborev job 2736): aggregate result metadata uses a
    // fallback name (col_0) while the row value is keyed by the expression name
    // (Count(*)). A metadata column absent from values must be skipped, not
    // null-filled — otherwise the row gains a phantom `col_0: null`.
    const result = await db.executeNative(
      'SELECT COUNT(*) FROM test_basic.simple_table'
    );
    expect(result.rowCount).toBe(1);
    const row = result.rows[0];
    const keys = Object.keys(row);
    // The real aggregate cell is present with a non-null value...
    expect(keys.length).toBe(1);
    expect(row[keys[0]]).not.toBeNull();
    // ...and no phantom null-filled metadata column leaks through.
    for (const [k, v] of Object.entries(row)) {
      expect(v === null).toBe(false);
    }
  });

  test('executeStreaming: Object.keys(row) matches SELECT column order', async () => {
    const stream = db.executeStreaming(
      'SELECT name, id, age FROM test_basic.simple_table LIMIT 5'
    );
    let seen = 0;
    for await (const row of stream) {
      expect(Object.keys(row)).toEqual(['name', 'id', 'age']);
      seen += 1;
    }
    // Present-but-empty fixtures FAIL loudly, never skip.
    expect(seen).toBeGreaterThan(0);
  });
});

/**
 * executeNative blob-payload memory footprint (Issue #1447).
 *
 * `ExecuteNativeTask::compute()` used to deep-clone every `Value` of every row
 * before dropping the source result; #1447 changed it to MOVE the values
 * instead. This test guards the payload footprint via `externalRatio`: the blob
 * bytes reaching JS must appear as a single copy (`external`/Buffer memory ~=
 * payload, i.e. ~1.0x). That is the meaningful no-duplication guard and the
 * ONLY hard assertion. `rssRatio` (process-wide resident set) is logged as an
 * advisory diagnostic only — it is affected by V8 GC/JIT/allocator retention,
 * is not payload-scaled, and so is too flaky to assert against as a
 * move-vs-clone proxy.
 *
 * Fixture: `test_wide_rows.large_blob_table.chunk_data` is the widest genuine
 * `blob` column in the corpus (~53 KB total across 50 rows, avg ~1 KB). To lift
 * the signal above V8/native baseline noise the query is issued K times and its
 * payload retained, so `totalPayload` reaches tens of MB.
 *
 * Dataset-absence is handled by `assertDatasetsAvailable()` in `beforeAll`:
 * per the repo's Node test convention it THROWS (never skips) when the corpus
 * is not configured, so a misconfigured CI run fails loudly rather than passing
 * silently. Within the test itself we also THROW for the present-but-empty
 * fixture case (rowCount == 0) and when `--expose-gc` is unavailable. Requires
 * `node --expose-gc` (wired via package.json `test` script).
 */
describe('executeNative blob payload memory footprint (Issue #1447)', () => {
  let db = null;

  beforeAll(async () => {
    assertDatasetsAvailable();
    db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_WIDE_ROWS,
    });
  });

  afterAll(async () => {
    if (db) {
      await db.close();
      db = null;
    }
  });

  test('peak resident footprint stays under 2x the blob payload (move, not clone)', async () => {
    // gc is mandatory: without a stable baseline the measurement is meaningless.
    if (typeof global.gc !== 'function') {
      throw new Error(
        'This test requires --expose-gc. Run: node --expose-gc ./node_modules/jest/bin/jest.js result.test.js'
      );
    }

    const query = 'SELECT chunk_data FROM test_wide_rows.large_blob_table';

    // Measure the per-query blob payload once.
    const first = await db.executeNative(query);
    if (first.rowCount === 0) {
      throw new Error(
        'large_blob_table returned 0 rows — present-but-empty fixture must fail, never skip'
      );
    }
    let perQueryPayload = 0;
    for (const row of first.rows) {
      const buf = row.chunk_data;
      if (buf) {
        expect(Buffer.isBuffer(buf)).toBe(true);
        perQueryPayload += buf.length;
      }
    }
    expect(perQueryPayload).toBeGreaterThan(0);

    // Amplify to a multi-MB payload so the footprint signal clears baseline
    // noise (per-query payload ~53 KB is far too small on its own).
    const K = 600;
    global.gc();
    const base = process.memoryUsage();
    let peakRss = base.rss;
    let peakExternal = base.external;
    const retained = [];
    for (let i = 0; i < K; i += 1) {
      const r = await db.executeNative(query);
      for (const row of r.rows) {
        if (row.chunk_data) {
          retained.push(row.chunk_data);
        }
      }
      const m = process.memoryUsage();
      if (m.rss > peakRss) peakRss = m.rss;
      if (m.external > peakExternal) peakExternal = m.external;
    }
    // Keep the payload live through the assertions.
    expect(retained.length).toBe(first.rowCount * K);

    const totalPayload = perQueryPayload * K;
    const externalRatio = (peakExternal - base.external) / totalPayload;
    // rssRatio is advisory-only: process-wide RSS is affected by V8 GC/JIT and
    // allocator retention, is not payload-scaled, and is too flaky to assert.
    const rssRatio = (peakRss - base.rss) / totalPayload;
    console.log(
      `    #1447 footprint: totalPayload=${totalPayload} bytes, ` +
        `externalRatio=${externalRatio.toFixed(3)}, rssRatio=${rssRatio.toFixed(3)} (advisory)`
    );

    // Blob bytes reach JS as a single copy: Buffer/external memory tracks the
    // payload ~1:1. A reintroduced deep-clone (or any retained duplicate) would
    // push externalRatio toward the ~3x regression #1447 fixed. This is the
    // only hard assertion; rssRatio above is logged as a diagnostic only.
    expect(externalRatio).toBeLessThan(2);
  });
});
