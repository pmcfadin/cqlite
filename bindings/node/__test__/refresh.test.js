/**
 * End-to-end tests for `Database.refresh()` (issue #1749).
 *
 * These drive the full stale -> refresh -> fresh cycle through the Node public
 * API (`Database.open`, `db.execute`, `db.executeNative`, `db.refresh`) against
 * REAL SSTable binaries.
 *
 * Fixture strategy (mirrors the Python binding test and the Rust integration
 * test): the two SSTable generations are built IN-TEST with CQLite's own write
 * path (writable `Database.open` + INSERT + `flushRun`) — `nb-1-big-*` holds
 * only partition `id=1` and `nb-2-big-*` holds only `id=2`. Because the
 * generations are generated here rather than fetched, there is NO skip path: a
 * write-path failure fails the test, every id-set assertion is exact (never
 * `>= 0`), and each copy asserts it moved at least one component file. A
 * 0-rows-on-present-data regression therefore fails loudly instead of silently
 * passing.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('../lib/index.js');

const KEYSPACE = 'test_freshness';
const TABLE = 'users';
const SCHEMA = `CREATE TABLE ${KEYSPACE}.${TABLE} (id int PRIMARY KEY, value text);`;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

/** Create a fresh temp root and register it for cleanup. */
function makeRoot(cleanups) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-refresh-test-'));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  return root;
}

/** Write the single-table schema used by the write engine and readers. */
function writeSchema(root) {
  const schemaPath = path.join(root, 'users.cql');
  fs.writeFileSync(schemaPath, SCHEMA);
  return schemaPath;
}

/**
 * Build a source table dir with two SSTable generations.
 *
 * Each `flushRun()` of a single writable database advances the generation:
 * `nb-1-big-*` contains only partition `id=1` and `nb-2-big-*` only `id=2`.
 * Returns the `.../data/<keyspace>/<table>` directory containing both.
 */
async function buildTwoGenerations(root, schema) {
  // The read side of a writable open still requires an existing data dir.
  const readDir = path.join(root, 'src_read');
  fs.mkdirSync(readDir);
  const writeDir = path.join(root, 'src_write');

  const db = await Database.open(readDir, {
    schema,
    writable: true,
    writeDir,
  });
  try {
    for (const genId of [1, 2]) {
      await db.execute(
        `INSERT INTO ${KEYSPACE}.${TABLE} (id, value) VALUES (${genId}, 'v${genId}')`
      );
      const flushed = await db.flushRun();
      expect(flushed).toBeTruthy(); // flush produced an SSTable path
    }
  } finally {
    await db.close();
  }

  const tableDir = path.join(writeDir, 'data', KEYSPACE, TABLE);
  expect(fs.existsSync(path.join(tableDir, 'nb-1-big-Data.db'))).toBe(true);
  expect(fs.existsSync(path.join(tableDir, 'nb-2-big-Data.db'))).toBe(true);
  return tableDir;
}

/**
 * Copy every `nb-<gen>-big-*` component into `dstTableDir`.
 * Returns the count copied (asserted > 0 so a path regression fails loudly).
 */
function copyGeneration(srcTableDir, dstTableDir, gen) {
  fs.mkdirSync(dstTableDir, { recursive: true });
  const prefix = `nb-${gen}-big-`;
  let copied = 0;
  for (const name of fs.readdirSync(srcTableDir)) {
    if (name.startsWith(prefix)) {
      fs.copyFileSync(
        path.join(srcTableDir, name),
        path.join(dstTableDir, name)
      );
      copied += 1;
    }
  }
  expect(copied).toBeGreaterThan(0);
  return copied;
}

/** Delete every `nb-<gen>-big-*` component (simulated compaction). */
function deleteGeneration(tableDir, gen) {
  const prefix = `nb-${gen}-big-`;
  let removed = 0;
  for (const name of fs.readdirSync(tableDir)) {
    if (name.startsWith(prefix)) {
      fs.unlinkSync(path.join(tableDir, name));
      removed += 1;
    }
  }
  expect(removed).toBeGreaterThan(0);
  return removed;
}

/** The set of `id` partition-key values in `SELECT *` (native int -> number). */
async function selectAllIds(db) {
  const res = await db.executeNative(`SELECT * FROM ${KEYSPACE}.${TABLE}`);
  return new Set(res.rows.map((row) => Number(row.id)));
}

/** Assert two Sets contain exactly the same numeric members. */
function expectIds(actual, expected) {
  expect([...actual].sort((a, b) => a - b)).toEqual(
    [...expected].sort((a, b) => a - b)
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('Database.refresh() end-to-end (issue #1749)', () => {
  let cleanups;

  beforeEach(() => {
    cleanups = [];
  });

  afterEach(async () => {
    for (const fn of cleanups.reverse()) {
      try {
        await fn();
      } catch (_) {
        // ignore cleanup errors
      }
    }
  });

  test('new generation invisible until refresh, visible after (readersAdded === 1)', async () => {
    const root = makeRoot(cleanups);
    const schema = writeSchema(root);
    const srcTableDir = await buildTwoGenerations(root, schema);

    // Live directory starts with ONLY generation 1 (partition id=1).
    const live = path.join(root, 'live');
    const liveTableDir = path.join(live, KEYSPACE, TABLE);
    copyGeneration(srcTableDir, liveTableDir, 1);

    const db = await Database.open(live, { schema });
    cleanups.push(() => db.close());

    const before = await selectAllIds(db);
    expectIds(before, new Set([1])); // only gen-1 partition visible at open

    // Copy in generation 2 (partition id=2) but do NOT refresh yet.
    copyGeneration(srcTableDir, liveTableDir, 2);
    expectIds(await selectAllIds(db), new Set([1])); // stale-until-refresh

    const report = await db.refresh();
    expect(report.readersAdded).toBe(1);
    expect(report.readersRemoved).toBe(0);
    expect(report.tablesScanned).toBeGreaterThanOrEqual(1);

    // New generation's partition is now visible.
    expectIds(await selectAllIds(db), new Set([1, 2]));
  });

  test('removed generation dropped on refresh (readersRemoved === 1)', async () => {
    const root = makeRoot(cleanups);
    const schema = writeSchema(root);
    const srcTableDir = await buildTwoGenerations(root, schema);

    const live = path.join(root, 'live');
    const liveTableDir = path.join(live, KEYSPACE, TABLE);
    copyGeneration(srcTableDir, liveTableDir, 1);
    copyGeneration(srcTableDir, liveTableDir, 2);

    const db = await Database.open(live, { schema });
    cleanups.push(() => db.close());

    expectIds(await selectAllIds(db), new Set([1, 2])); // both visible at open

    deleteGeneration(liveTableDir, 2);
    const report = await db.refresh();
    expect(report.readersRemoved).toBe(1);
    expect(report.readersAdded).toBe(0);

    // Only the remaining generation's partition after removal.
    expectIds(await selectAllIds(db), new Set([1]));
  });

  test('unchanged directory is a zero-delta no-op', async () => {
    const root = makeRoot(cleanups);
    const schema = writeSchema(root);
    const srcTableDir = await buildTwoGenerations(root, schema);

    const live = path.join(root, 'live');
    const liveTableDir = path.join(live, KEYSPACE, TABLE);
    copyGeneration(srcTableDir, liveTableDir, 1);

    const db = await Database.open(live, { schema });
    cleanups.push(() => db.close());

    const before = await selectAllIds(db);

    const report = await db.refresh();
    expect(report.readersAdded).toBe(0);
    expect(report.readersRemoved).toBe(0);

    expectIds(await selectAllIds(db), before); // result unchanged by no-op
  });

  test('RefreshReport exposes camelCase count fields', async () => {
    const root = makeRoot(cleanups);
    const schema = writeSchema(root);
    const srcTableDir = await buildTwoGenerations(root, schema);

    const live = path.join(root, 'live');
    const liveTableDir = path.join(live, KEYSPACE, TABLE);
    copyGeneration(srcTableDir, liveTableDir, 1);

    const db = await Database.open(live, { schema });
    cleanups.push(() => db.close());

    copyGeneration(srcTableDir, liveTableDir, 2);
    const report = await db.refresh();

    // Exactly the camelCase surface documented in index.d.ts.
    expect(Object.keys(report).sort()).toEqual([
      'readersAdded',
      'readersRemoved',
      'tablesScanned',
    ]);
    expect(typeof report.readersAdded).toBe('number');
    expect(report.readersAdded).toBe(1);
    expect(report.readersRemoved).toBe(0);
  });

  test('refresh() on a closed database rejects', async () => {
    const root = makeRoot(cleanups);
    const schema = writeSchema(root);
    const srcTableDir = await buildTwoGenerations(root, schema);

    const live = path.join(root, 'live');
    const liveTableDir = path.join(live, KEYSPACE, TABLE);
    copyGeneration(srcTableDir, liveTableDir, 1);

    const db = await Database.open(live, { schema });
    await db.close();

    await expect(db.refresh()).rejects.toThrow(/closed/i);
  });
});
