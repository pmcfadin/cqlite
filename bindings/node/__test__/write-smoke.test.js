/**
 * Release-artifact write smoke test (Issue #1460).
 *
 * The Node binding's entire write capability hangs on one build flag:
 * `--features write-support` (bindings/node/package.json build script). If that
 * flag is ever dropped from the release build, the DML routing is `#[cfg]`ed out
 * and an INSERT does not error — historically it *silently* fell through to the
 * read path, returned a read-shaped empty result, and never persisted the row.
 *
 * This test exercises a DML write end-to-end through the BUILT `lib/index.js` →
 * `.node` artifact (the same module CI ships) and proves:
 *   1. the INSERT is routed to the write engine (result is NOT read-shaped), and
 *   2. `flushRun()` produces a non-empty `*-Data.db` on disk, and
 *   3. `writeStats` advanced.
 *
 * If `--features write-support` is stripped from the build, the fix in
 * database.rs makes the DML fail closed (an explicit error is thrown), so this
 * test STILL reds — it can never pass against a write-support-stripped build.
 * (TDD proof: `napi build --platform --release` without the flag → this test
 * throws on the INSERT; rebuild with the flag → passes.)
 *
 * This test does NOT depend on the external SSTable datasets — it writes into a
 * fresh tmp dir and needs only a single-table schema file. It must FAIL LOUDLY
 * (throw) if the write engine cannot open; it must never skip.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('../lib/index.js');

// Single-table schema (no-heuristics mandate, Issue #28): unambiguous write target.
const SCHEMA_PATH = path.join(
  __dirname,
  '..',
  '..',
  '..',
  'test-data',
  'schemas',
  'write-test.cql'
);

/** Recursively find the first file whose name ends with `-Data.db`. */
function findDataDb(root) {
  const stack = [root];
  while (stack.length > 0) {
    const dir = stack.pop();
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
      } else if (entry.isFile() && entry.name.endsWith('-Data.db')) {
        return full;
      }
    }
  }
  return null;
}

describe('release-artifact write smoke (Issue #1460)', () => {
  beforeAll(() => {
    if (!fs.existsSync(SCHEMA_PATH)) {
      throw new Error(
        `Schema file not found at ${SCHEMA_PATH}. Cannot run write smoke.`
      );
    }
  });

  test('test_release_artifact_can_write', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-write-smoke-'));
    let db;
    try {
      // Open the built artifact writable against a fresh, empty tmp dir.
      db = await Database.open(tmp, {
        schema: SCHEMA_PATH,
        writable: true,
        writeDir: tmp,
      });

      // Execute a DML write through the released artifact's native path.
      // A literal UUID is used (not uuid()) so the assertion is deterministic and
      // does not depend on server-side function support in the write parser.
      const wr = await db.executeNative(
        "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655441460, 'Smoke')"
      );

      // Negative assertion: the write branch returns rows:[], rowCount:0,
      // rowsAffected:1 (bindings/node/src/database.rs). A read fall-through
      // (feature stripped, pre-fix) would instead have returned a SELECT-shaped
      // result where rowsAffected === rowCount and the write dir stayed empty.
      // Post-fix, a stripped build throws before reaching here.
      expect(wr.rowCount).toBe(0);
      expect(wr.rowsAffected).toBeGreaterThanOrEqual(1);
      expect(wr.rowsAffected).not.toBe(wr.rowCount);

      // Flush the memtable to an SSTable on disk.
      const flushedPath = await db.flushRun();
      expect(typeof flushedPath).toBe('string');
      expect(flushedPath.length).toBeGreaterThan(0);

      // writeStats must show the write + flush advanced counters.
      const stats = db.writeStats;
      expect(stats.l0Count).toBeGreaterThan(0);
      expect(stats.totalWritten).toBeGreaterThan(0);

      // A non-empty *-Data.db must now exist somewhere under the write dir.
      const dataDb = findDataDb(tmp);
      expect(dataDb).not.toBeNull();
      expect(fs.statSync(dataDb).size).toBeGreaterThan(0);
    } finally {
      if (db) {
        await db.close();
      }
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
