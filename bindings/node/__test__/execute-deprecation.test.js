/**
 * Deprecation + precision-loss pinning tests for the legacy `execute()` method
 * (Issue #1457).
 *
 * These tests pin two properties of the DEPRECATED `execute()` path so a future
 * change cannot silently alter them:
 *   1. Calling `execute()` emits a one-time Node `DeprecationWarning`
 *      (code `CQLITE_DEP0001`) steering callers to `executeNative()`.
 *   2. `execute()` loses precision for `bigint` above 2^53 and returns `blob`
 *      as a base64 string, whereas `executeNative()` is the precision-safe path
 *      (exact `BigInt`, `Buffer` for blob).
 *
 * The DB is generated in a tmp dir (write-support), so these tests do NOT depend
 * on the fixture corpus. Setup writes use `executeNative()` so they do not
 * consume the one-time deprecation warning before the warning test runs.
 *
 * NOTE (issue #1457 finding): the issue was filed on the premise that `execute()`
 * silently rounds `bigint` above 2^53. That was NOT reproducible on the current
 * napi build — napi's serde-json conversion returns the i64 as an exact JS
 * `BigInt`, so `execute()` currently preserves the value. These tests therefore
 * pin the OBSERVED behavior (exact BigInt via both paths) so a future napi/serde
 * change that reintroduces f64 rounding turns them red, and pin the genuinely
 * lossy behavior (blob → base64 string vs Buffer).
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('../lib/index.js');

// 2^53 + 1 — the smallest integer NOT exactly representable as a JS `number`
// (f64). If `execute()` ever downcast bigint to a JS number, this value would
// round to 2^53 (= 9007199254740992) and the pinning test below would catch it.
const BIG_UNSAFE = 9007199254740993n;

const SCHEMA_TEXT = `
CREATE KEYSPACE IF NOT EXISTS dep_test
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dep_test;

CREATE TABLE IF NOT EXISTS items (
    id   INT PRIMARY KEY,
    big  BIGINT,
    data BLOB
);
`;

describe('execute() deprecation + precision loss (Issue #1457)', () => {
  let dir;
  let readDb;

  beforeAll(async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-exec-dep-'));
    const schema = path.join(dir, 'schema.cql');
    fs.writeFileSync(schema, SCHEMA_TEXT);
    const dataDir = path.join(dir, 'data_dir');
    fs.mkdirSync(dataDir);
    const writeDir = path.join(dir, 'write_dir');

    // Write via executeNative() so the one-time deprecation warning is NOT
    // consumed before the warning test below.
    const wdb = await Database.open(dataDir, { schema, writable: true, writeDir });
    await wdb.executeNative(
      `INSERT INTO dep_test.items (id, big, data) VALUES (1, ${BIG_UNSAFE.toString()}, 0xdeadbeef)`
    );
    await wdb.flushRun();
    await wdb.close();

    // Reopen the flushed SSTable directory read-only for the assertions.
    readDb = await Database.open(path.join(writeDir, 'data'), { schema });
  });

  afterAll(async () => {
    if (readDb) {
      await readDb.close();
      readDb = null;
    }
    if (dir) {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  test('execute() emits a one-time DeprecationWarning steering to executeNative()', async () => {
    const spy = jest.spyOn(process, 'emitWarning').mockImplementation(() => {});
    try {
      // Two calls: the warning must fire at most once for the whole process.
      await readDb.execute('SELECT * FROM dep_test.items');
      await readDb.execute('SELECT * FROM dep_test.items');

      const depCalls = spy.mock.calls.filter(
        (c) => c[1] && typeof c[1] === 'object' && c[1].code === 'CQLITE_DEP0001'
      );
      expect(depCalls).toHaveLength(1);
      expect(depCalls[0][1].type).toBe('DeprecationWarning');
      expect(depCalls[0][0]).toMatch(/deprecated/i);
      expect(depCalls[0][0]).toMatch(/executeNative/);
    } finally {
      spy.mockRestore();
    }
  });

  test('bigint > 2^53 is exact via executeNative() AND (currently) via execute()', async () => {
    const nativeRows = (await readDb.executeNative('SELECT big FROM dep_test.items')).rows;
    expect(nativeRows).toHaveLength(1);
    // Supported path: exact BigInt.
    expect(typeof nativeRows[0].big).toBe('bigint');
    expect(nativeRows[0].big).toBe(BIG_UNSAFE);

    // Issue #1457 finding: contrary to the issue's premise, the current napi
    // build returns the i64 as an exact JS BigInt from execute() too — it is
    // NOT rounded to an f64. Pin that so a regression to f64 rounding reds here.
    const jsonRows = (await readDb.execute('SELECT big FROM dep_test.items')).rows;
    expect(jsonRows).toHaveLength(1);
    expect(typeof jsonRows[0].big).toBe('bigint');
    expect(jsonRows[0].big).toBe(BIG_UNSAFE);
  });

  test('execute() returns blob as base64 string while executeNative() returns a Buffer', async () => {
    const nativeRows = (await readDb.executeNative('SELECT data FROM dep_test.items')).rows;
    expect(Buffer.isBuffer(nativeRows[0].data)).toBe(true);
    expect(nativeRows[0].data.equals(Buffer.from('deadbeef', 'hex'))).toBe(true);

    const jsonRows = (await readDb.execute('SELECT data FROM dep_test.items')).rows;
    expect(typeof jsonRows[0].data).toBe('string');
    // Base64 of 0xdeadbeef.
    expect(jsonRows[0].data).toBe(Buffer.from('deadbeef', 'hex').toString('base64'));
  });
});
