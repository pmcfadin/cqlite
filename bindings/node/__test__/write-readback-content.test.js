/**
 * Content-asserting write→read round-trips through the public Node API (Issue #1231).
 *
 * Unlike write.test.js (whose one post-write SELECT tolerates 0 rows and asserts
 * no content), every test here drives the FULL public chain:
 *
 *   db.execute("INSERT/UPDATE/DELETE")  →  db.flushRun()  →  real SSTable
 *     →  Database.open(<writeDir>/data)  (independent reopen)
 *     →  db.executeNative("SELECT ...")  →  assert decoded VALUES
 *
 * A write-format/encoding regression that emits a structurally-present but
 * semantically-WRONG Data.db will turn these red — shape-only tests could not
 * (the "CI blind to the write path" hazard, epic #1227).
 *
 * These tests generate their own SSTables in a tmp dir, so they do NOT depend on
 * the fixture corpus.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('../lib/index.js');

// A single-table schema (no-heuristics mandate: one unambiguous write target).
const SCHEMA_TEXT = `
CREATE KEYSPACE IF NOT EXISTS write_test
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE write_test;

CREATE TABLE IF NOT EXISTS items (
    id    INT PRIMARY KEY,
    name  TEXT,
    value INT
);
`;

/** Create a tmp work dir containing the schema file. */
function setup() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-write-content-'));
  const schema = path.join(dir, 'schema.cql');
  fs.writeFileSync(schema, SCHEMA_TEXT);
  const dataDir = path.join(dir, 'data_dir');
  fs.mkdirSync(dataDir);
  const writeDir = path.join(dir, 'write_dir');
  return {
    dir,
    schema,
    dataDir,
    writeDir,
    cleanup() {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch (_) {
        /* ignore */
      }
    },
  };
}

/** Reopen the flushed SSTable directory read-only and return the decoded rows. */
async function readBack(writeDir, schema, query) {
  const rd = await Database.open(path.join(writeDir, 'data'), { schema });
  try {
    const res = await rd.executeNative(query);
    return res.rows;
  } finally {
    await rd.close();
  }
}

function rowWithId(rows, id) {
  return rows.find((r) => Number(r.id) === id);
}

describe('Write→read content round-trip (Issue #1231)', () => {
  test('INSERT → flush → reopen → SELECT asserts decoded values', async () => {
    const env = setup();
    try {
      const db = await Database.open(env.dataDir, {
        schema: env.schema,
        writable: true,
        writeDir: env.writeDir,
      });
      await db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (1, 'alpha', 10)"
      );
      const sstable = await db.flushRun();
      expect(sstable.length).toBeGreaterThan(0);
      expect(fs.existsSync(sstable)).toBe(true);
      await db.close();

      const rows = await readBack(env.writeDir, env.schema, 'SELECT * FROM write_test.items');
      expect(rows.length).toBe(1);
      const row = rows[0];
      expect(Number(row.id)).toBe(1);
      expect(row.name).toBe('alpha');
      expect(Number(row.value)).toBe(10);
    } finally {
      env.cleanup();
    }
  });

  test('UPDATE overwrite wins on read-back', async () => {
    const env = setup();
    try {
      const db = await Database.open(env.dataDir, {
        schema: env.schema,
        writable: true,
        writeDir: env.writeDir,
      });
      await db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (1, 'alpha', 10)"
      );
      await db.execute(
        "UPDATE write_test.items SET name = 'ALPHA', value = 11 WHERE id = 1"
      );
      await db.flushRun();
      await db.close();

      const rows = await readBack(env.writeDir, env.schema, 'SELECT * FROM write_test.items');
      expect(rows.length).toBe(1);
      const row = rows[0];
      expect(row.name).toBe('ALPHA'); // UPDATE won, not the INSERT value
      expect(Number(row.value)).toBe(11);
    } finally {
      env.cleanup();
    }
  });

  test('DELETE tombstone makes the row absent on read-back', async () => {
    const env = setup();
    try {
      const db = await Database.open(env.dataDir, {
        schema: env.schema,
        writable: true,
        writeDir: env.writeDir,
      });
      await db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (1, 'alpha', 10)"
      );
      await db.execute(
        "INSERT INTO write_test.items (id, name, value) VALUES (2, 'beta', 20)"
      );
      await db.execute('DELETE FROM write_test.items WHERE id = 2');
      await db.flushRun();
      await db.close();

      const rows = await readBack(env.writeDir, env.schema, 'SELECT * FROM write_test.items');
      expect(rowWithId(rows, 2)).toBeUndefined(); // deleted row absent
      const survivor = rowWithId(rows, 1);
      expect(survivor).toBeDefined();
      expect(survivor.name).toBe('alpha');
      expect(Number(survivor.value)).toBe(10);
    } finally {
      env.cleanup();
    }
  });
});
