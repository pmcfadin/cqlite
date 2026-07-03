/**
 * Auto-flush cliff wiring evidence (Issue #1620, N2).
 *
 * The Node binding routes DML through the write engine inside a `spawn_blocking`
 * where a Tokio runtime handle IS present. On main the engine's sync auto-flush
 * is intentionally skipped in that topology, so the memtable grew unbounded and
 * NO SSTable was ever written until an explicit `flushRun()`. This test proves
 * the fix end-to-end: with a tiny `flushThreshold`, a loop of `db.execute(...)`
 * inserts triggers a REAL async flush on its own — with NO explicit flushRun —
 * and on-disk `*-Data.db` generation files appear.
 *
 * Named-public-surface → call-chain → e2e evidence:
 *   { flushThreshold } open option  →  Database.execute (DML)
 *     →  WriteEngine::execute_flushing (async flush)  →  *-Data.db on disk
 *
 * On main this test is red: 0 Data.db files (auto-flush never fires).
 *
 * Generates its own SSTables in a tmp dir; no fixture-corpus dependency.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('../lib/index.js');

const SCHEMA_TEXT = `
CREATE KEYSPACE IF NOT EXISTS flush_test
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flush_test;

CREATE TABLE IF NOT EXISTS items (
    id    INT PRIMARY KEY,
    name  TEXT,
    value INT
);
`;

function setup() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-autoflush-'));
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

/** Recursively count `*-Data.db` files anywhere under `dir`. */
function countDataDbUnder(dir) {
  if (!fs.existsSync(dir)) return 0;
  let count = 0;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      count += countDataDbUnder(full);
    } else if (entry.name.endsWith('-Data.db')) {
      count += 1;
    }
  }
  return count;
}

/**
 * Count `*-Data.db` files under the write dir's `data/` directory, recursively.
 * Flushed SSTables land under data/<keyspace>/<table>/nb-*-big-Data.db, so a
 * flat readdir of `data/` would miss them (issue #1620).
 */
function countDataDbFiles(writeDir) {
  return countDataDbUnder(path.join(writeDir, 'data'));
}

describe('Auto-flush cliff wiring (Issue #1620)', () => {
  test('tiny flushThreshold triggers a real flush during execute() (no manual flush)', async () => {
    const env = setup();
    try {
      const db = await Database.open(env.dataDir, {
        schema: env.schema,
        writable: true,
        writeDir: env.writeDir,
        flushThreshold: 4096, // 4 KB — crossed after a handful of inserts
      });

      const TOTAL = 2000;
      for (let i = 0; i < TOTAL; i++) {
        const res = await db.execute(
          `INSERT INTO flush_test.items (id, name, value) VALUES (${i}, 'user${i}', ${i})`
        );
        expect(res.rowsAffected).toBe(1);
      }

      // A real auto-flush must have fired: on-disk generation files exist.
      // On main this is 0 because the runtime-present sync path never flushes.
      const dataDbCount = countDataDbFiles(env.writeDir);
      expect(dataDbCount).toBeGreaterThanOrEqual(1);

      // The memtable was cleared by the flush(es), so its residual row count is
      // far below the total inserted. `writeStats` is a synchronous getter.
      const stats = db.writeStats;
      expect(Number(stats.memtableRows)).toBeLessThan(TOTAL);

      await db.close();
    } finally {
      env.cleanup();
    }
  });

  test('flushThreshold below 1 byte is rejected', async () => {
    const env = setup();
    try {
      await expect(
        Database.open(env.dataDir, {
          schema: env.schema,
          writable: true,
          writeDir: env.writeDir,
          flushThreshold: 0,
        })
      ).rejects.toThrow(/flushThreshold/);
    } finally {
      env.cleanup();
    }
  });

  test('flushThreshold above the memtable hard limit is rejected', async () => {
    // A threshold above the 256 MB hard limit would never trigger an auto-flush
    // (writes dead-end at the hard limit first) — issue #1620.
    const env = setup();
    try {
      await expect(
        Database.open(env.dataDir, {
          schema: env.schema,
          writable: true,
          writeDir: env.writeDir,
          flushThreshold: 300 * 1024 * 1024, // 300 MB > 256 MB hard limit
        })
      ).rejects.toThrow(/hard limit/);
    } finally {
      env.cleanup();
    }
  });
});
