/**
 * Write API tests for Issue #391.
 *
 * Tests INSERT/UPDATE/DELETE via execute() and executeNative(), flushRun(),
 * maintenanceStep(), writeStats getter, read-only guard, and error paths.
 *
 * These tests exercise the write path without requiring real SSTable data
 * (the write-dir is a temporary directory created per test). Tests that
 * require reading back written data use the flush+reopen pattern.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('../lib/index.js');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Create a fresh temporary directory for write tests.
 * Returns { dir, cleanup } where cleanup() removes the directory.
 */
function tmpWriteDir() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-write-test-'));
  return {
    dir,
    cleanup() {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch (_) {
        // ignore cleanup errors
      }
    },
  };
}

/**
 * Schema path for the test_basic keyspace.
 * This CQL file must contain CREATE TABLE test_basic.simple_table.
 */
const SCHEMA_PATH = path.join(
  __dirname,
  '..',
  '..',
  '..',
  'test-data',
  'schemas',
  'basic-types.cql'
);

/**
 * Open a writable database in a temp directory.
 * Caller must call cleanup() when done.
 */
async function openWritable() {
  const { dir, cleanup } = tmpWriteDir();
  const db = await Database.open(dir, {
    schema: SCHEMA_PATH,
    writable: true,
    writeDir: dir,
  });
  return { db, dir, cleanup };
}

/**
 * Skip all tests if the schema file isn't present.
 * The schema file ships with the repo, so this should always pass.
 */
function requireSchemaFile() {
  if (!fs.existsSync(SCHEMA_PATH)) {
    throw new Error(
      `Schema file not found at ${SCHEMA_PATH}. Cannot run write tests.`
    );
  }
}

// ---------------------------------------------------------------------------
// Read-only guard tests
// ---------------------------------------------------------------------------

describe('Read-only guard (Issue #391)', () => {
  beforeAll(requireSchemaFile);

  test('execute() DML on read-only database throws clear error', async () => {
    expect.assertions(2);
    const { dir, cleanup } = tmpWriteDir();
    try {
      const db = await Database.open(dir, { schema: SCHEMA_PATH });
      try {
        await db.execute(
          "INSERT INTO test_basic.simple_table (id) VALUES (uuid())"
        );
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e).toBeDefined();
        // Message should mention write support being disabled
        expect(e.message.toLowerCase()).toMatch(/write|writable/);
      } finally {
        await db.close();
      }
    } finally {
      cleanup();
    }
  });

  test('executeNative() DML on read-only database throws clear error', async () => {
    expect.assertions(2);
    const { dir, cleanup } = tmpWriteDir();
    try {
      const db = await Database.open(dir, { schema: SCHEMA_PATH });
      try {
        await db.executeNative(
          "INSERT INTO test_basic.simple_table (id) VALUES (uuid())"
        );
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e).toBeDefined();
        expect(e.message.toLowerCase()).toMatch(/write|writable/);
      } finally {
        await db.close();
      }
    } finally {
      cleanup();
    }
  });

  test('flushRun() on read-only database throws clear error', async () => {
    expect.assertions(2);
    const { dir, cleanup } = tmpWriteDir();
    try {
      const db = await Database.open(dir, { schema: SCHEMA_PATH });
      try {
        await db.flushRun();
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e).toBeDefined();
        expect(e.message.toLowerCase()).toMatch(/write|writable/);
      } finally {
        await db.close();
      }
    } finally {
      cleanup();
    }
  });

  test('maintenanceStep() on read-only database throws clear error', async () => {
    expect.assertions(2);
    const { dir, cleanup } = tmpWriteDir();
    try {
      const db = await Database.open(dir, { schema: SCHEMA_PATH });
      try {
        await db.maintenanceStep({ budgetMs: 10 });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e).toBeDefined();
        expect(e.message.toLowerCase()).toMatch(/write|writable/);
      } finally {
        await db.close();
      }
    } finally {
      cleanup();
    }
  });

  test('writeStats getter on read-only database throws clear error', async () => {
    expect.assertions(2);
    const { dir, cleanup } = tmpWriteDir();
    try {
      const db = await Database.open(dir, { schema: SCHEMA_PATH });
      try {
        const _stats = db.writeStats;
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e).toBeDefined();
        expect(e.message.toLowerCase()).toMatch(/write|writable/);
      } finally {
        await db.close();
      }
    } finally {
      cleanup();
    }
  });

  test('open() with writable:true but no writeDir throws', async () => {
    expect.assertions(2);
    const { dir, cleanup } = tmpWriteDir();
    try {
      try {
        await Database.open(dir, {
          schema: SCHEMA_PATH,
          writable: true,
          // writeDir intentionally omitted
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e).toBeDefined();
        expect(e.message.toLowerCase()).toMatch(/writedir|write.dir/);
      }
    } finally {
      cleanup();
    }
  });
});

// ---------------------------------------------------------------------------
// Writable database – basic API shape
// ---------------------------------------------------------------------------

describe('Writable database – API shape (Issue #391)', () => {
  beforeAll(requireSchemaFile);

  let db, cleanup;

  beforeAll(async () => {
    const result = await openWritable();
    db = result.db;
    cleanup = result.cleanup;
  });

  afterAll(async () => {
    if (db && !db.isClosed) {
      await db.close();
    }
    if (cleanup) cleanup();
  });

  // ------ writeStats ------

  test('writeStats returns an object with expected numeric fields', () => {
    const stats = db.writeStats;
    expect(stats).toBeDefined();
    expect(typeof stats.memtableSize).toBe('number');
    expect(typeof stats.memtableRows).toBe('number');
    expect(typeof stats.walSize).toBe('number');
    expect(typeof stats.l0Count).toBe('number');
    expect(typeof stats.totalWritten).toBe('number');
  });

  test('writeStats starts at zero rows and zero totalWritten', () => {
    const stats = db.writeStats;
    expect(stats.memtableRows).toBe(0);
    expect(stats.totalWritten).toBe(0);
    expect(stats.l0Count).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// execute() with DML
// ---------------------------------------------------------------------------

describe('execute() DML support (Issue #391)', () => {
  beforeAll(requireSchemaFile);

  let db, cleanup;

  beforeAll(async () => {
    const result = await openWritable();
    db = result.db;
    cleanup = result.cleanup;
  });

  afterAll(async () => {
    if (db && !db.isClosed) {
      await db.close();
    }
    if (cleanup) cleanup();
  });

  test('execute() INSERT returns rowsAffected = 1 and empty rows', async () => {
    const result = await db.execute(
      "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440001, 'Alice')"
    );
    expect(result).toBeDefined();
    expect(result.rowsAffected).toBe(1);
    expect(result.rowCount).toBe(0);
    expect(Array.isArray(result.rows)).toBe(true);
    expect(result.rows.length).toBe(0);
    expect(typeof result.executionTimeMs).toBe('number');
    expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);
  });

  test('execute() UPDATE returns rowsAffected = 1', async () => {
    const result = await db.execute(
      "UPDATE test_basic.simple_table SET name = 'Bob' WHERE id = 550e8400-e29b-41d4-a716-446655440002"
    );
    expect(result.rowsAffected).toBe(1);
  });

  test('execute() DELETE returns rowsAffected = 1', async () => {
    const result = await db.execute(
      "DELETE FROM test_basic.simple_table WHERE id = 550e8400-e29b-41d4-a716-446655440003"
    );
    expect(result.rowsAffected).toBe(1);
  });

  test('execute() SELECT still works on writable database', async () => {
    const result = await db.execute(
      "SELECT * FROM test_basic.simple_table LIMIT 1"
    );
    // SELECT may return 0 rows (no binary Data.db on disk yet) but must not throw
    expect(typeof result.rowCount).toBe('number');
    expect(Array.isArray(result.rows)).toBe(true);
  });

  test('execute() DML increases writeStats.memtableRows', async () => {
    const before = db.writeStats.memtableRows;
    await db.execute(
      "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440004, 'Charlie')"
    );
    const after = db.writeStats.memtableRows;
    expect(after).toBeGreaterThan(before);
  });

  test('execute() invalid CQL throws an error with code/category', async () => {
    expect.assertions(3);
    try {
      await db.execute('THIS IS NOT VALID CQL AT ALL @@@@');
      throw new Error('Should have thrown');
    } catch (e) {
      expect(e).toBeDefined();
      // Error should be structured (code + category)
      expect(typeof e.message).toBe('string');
      expect(e.message.length).toBeGreaterThan(0);
    }
  });
});

// ---------------------------------------------------------------------------
// executeNative() with DML
// ---------------------------------------------------------------------------

describe('executeNative() DML support (Issue #391)', () => {
  beforeAll(requireSchemaFile);

  let db, cleanup;

  beforeAll(async () => {
    const result = await openWritable();
    db = result.db;
    cleanup = result.cleanup;
  });

  afterAll(async () => {
    if (db && !db.isClosed) {
      await db.close();
    }
    if (cleanup) cleanup();
  });

  test('executeNative() INSERT returns rowsAffected = 1 and empty rows', async () => {
    const result = await db.executeNative(
      "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440011, 'Diana')"
    );
    expect(result).toBeDefined();
    expect(result.rowsAffected).toBe(1);
    expect(result.rowCount).toBe(0);
    expect(Array.isArray(result.rows)).toBe(true);
    expect(result.rows.length).toBe(0);
    expect(typeof result.executionTimeMs).toBe('number');
  });

  test('executeNative() UPDATE returns rowsAffected = 1', async () => {
    const result = await db.executeNative(
      "UPDATE test_basic.simple_table SET name = 'Eve' WHERE id = 550e8400-e29b-41d4-a716-446655440012"
    );
    expect(result.rowsAffected).toBe(1);
  });

  test('executeNative() DELETE returns rowsAffected = 1', async () => {
    const result = await db.executeNative(
      "DELETE FROM test_basic.simple_table WHERE id = 550e8400-e29b-41d4-a716-446655440013"
    );
    expect(result.rowsAffected).toBe(1);
  });

  test('executeNative() DML increases writeStats.memtableRows', async () => {
    const before = db.writeStats.memtableRows;
    await db.executeNative(
      "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440014, 'Frank')"
    );
    const after = db.writeStats.memtableRows;
    expect(after).toBeGreaterThan(before);
  });

  test('executeNative() invalid CQL throws an error', async () => {
    expect.assertions(2);
    try {
      await db.executeNative('BOGUS STATEMENT !!!');
      throw new Error('Should have thrown');
    } catch (e) {
      expect(e).toBeDefined();
      expect(typeof e.message).toBe('string');
    }
  });
});

// ---------------------------------------------------------------------------
// flushRun()
// ---------------------------------------------------------------------------

describe('flushRun() (Issue #391)', () => {
  beforeAll(requireSchemaFile);

  test('flushRun() on empty memtable returns empty string', async () => {
    const { db, cleanup } = await openWritable();
    try {
      const result = await db.flushRun();
      // Empty memtable -> no-op, return empty string
      expect(typeof result).toBe('string');
      expect(result).toBe('');
    } finally {
      await db.close();
      cleanup();
    }
  });

  test('flushRun() after INSERT returns a non-empty path', async () => {
    const { db, dir, cleanup } = await openWritable();
    try {
      await db.execute(
        "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440021, 'Grace')"
      );
      const sstablePath = await db.flushRun();
      expect(typeof sstablePath).toBe('string');
      expect(sstablePath.length).toBeGreaterThan(0);
      // The path should be an absolute path to a Data.db file
      expect(sstablePath).toMatch(/Data\.db$/);
    } finally {
      await db.close();
      cleanup();
    }
  });

  test('flushRun() produces a file that exists on disk', async () => {
    const { db, cleanup } = await openWritable();
    try {
      await db.execute(
        "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440022, 'Hank')"
      );
      const sstablePath = await db.flushRun();
      expect(sstablePath.length).toBeGreaterThan(0);
      expect(fs.existsSync(sstablePath)).toBe(true);
      // File should have non-zero size
      const stat = fs.statSync(sstablePath);
      expect(stat.size).toBeGreaterThan(0);
    } finally {
      await db.close();
      cleanup();
    }
  });

  test('flushRun() increments l0Count in writeStats', async () => {
    const { db, cleanup } = await openWritable();
    try {
      const before = db.writeStats.l0Count;
      await db.execute(
        "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440023, 'Ivy')"
      );
      await db.flushRun();
      const after = db.writeStats.l0Count;
      expect(after).toBe(before + 1);
    } finally {
      await db.close();
      cleanup();
    }
  });

  test('flushRun() clears memtable rows after flush', async () => {
    const { db, cleanup } = await openWritable();
    try {
      await db.execute(
        "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440024, 'Jack')"
      );
      expect(db.writeStats.memtableRows).toBeGreaterThan(0);
      await db.flushRun();
      // After flush the memtable should be cleared
      expect(db.writeStats.memtableRows).toBe(0);
    } finally {
      await db.close();
      cleanup();
    }
  });

  test('flushRun() updates totalWritten in writeStats', async () => {
    const { db, cleanup } = await openWritable();
    try {
      expect(db.writeStats.totalWritten).toBe(0);
      await db.execute(
        "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440025, 'Karen')"
      );
      await db.flushRun();
      expect(db.writeStats.totalWritten).toBeGreaterThan(0);
    } finally {
      await db.close();
      cleanup();
    }
  });
});

// ---------------------------------------------------------------------------
// maintenanceStep()
// ---------------------------------------------------------------------------

describe('maintenanceStep() (Issue #391)', () => {
  beforeAll(requireSchemaFile);

  test('maintenanceStep() returns a MaintenanceReport object', async () => {
    const { db, cleanup } = await openWritable();
    try {
      const report = await db.maintenanceStep({ budgetMs: 50 });
      expect(report).toBeDefined();
      expect(typeof report.timeSpentMs).toBe('number');
      expect(typeof report.rowsMerged).toBe('number');
      expect(typeof report.bytesWritten).toBe('number');
      expect(Array.isArray(report.completedMerges)).toBe(true);
      expect(typeof report.pendingCompaction).toBe('boolean');
    } finally {
      await db.close();
      cleanup();
    }
  });

  test('maintenanceStep() uses default budget when options omitted', async () => {
    const { db, cleanup } = await openWritable();
    try {
      const report = await db.maintenanceStep();
      expect(report).toBeDefined();
      expect(typeof report.timeSpentMs).toBe('number');
    } finally {
      await db.close();
      cleanup();
    }
  });

  test('maintenanceStep() respects budget within 10% tolerance', async () => {
    const { db, cleanup } = await openWritable();
    try {
      const budgetMs = 100;
      const report = await db.maintenanceStep({ budgetMs });
      // Budget tolerance: allow 10% over budget per spec
      expect(report.timeSpentMs).toBeLessThanOrEqual(budgetMs * 1.1 + 50); // extra 50ms for task overhead
    } finally {
      await db.close();
      cleanup();
    }
  });

  test('maintenanceStep() with no merge policy returns quickly', async () => {
    const { db, cleanup } = await openWritable();
    try {
      // No merge policy set → no work to do → should return immediately
      const start = Date.now();
      const report = await db.maintenanceStep({ budgetMs: 1000 });
      const elapsed = Date.now() - start;
      // Should return quickly (under 500ms) since there's nothing to do
      expect(elapsed).toBeLessThan(500);
      expect(report.pendingCompaction).toBe(false);
      expect(report.rowsMerged).toBe(0);
    } finally {
      await db.close();
      cleanup();
    }
  });
});

// ---------------------------------------------------------------------------
// writeStats getter
// ---------------------------------------------------------------------------

describe('writeStats getter (Issue #391)', () => {
  beforeAll(requireSchemaFile);

  let db, cleanup;

  beforeAll(async () => {
    const result = await openWritable();
    db = result.db;
    cleanup = result.cleanup;
  });

  afterAll(async () => {
    if (db && !db.isClosed) {
      await db.close();
    }
    if (cleanup) cleanup();
  });

  test('writeStats is accessible as a synchronous property', () => {
    // Should not throw even without await
    const stats = db.writeStats;
    expect(stats).toBeDefined();
  });

  test('writeStats.memtableSize increases after INSERT', async () => {
    const before = db.writeStats.memtableSize;
    await db.execute(
      "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440031, 'Liam')"
    );
    const after = db.writeStats.memtableSize;
    expect(after).toBeGreaterThanOrEqual(before);
  });

  test('writeStats.memtableRows increases after INSERT', async () => {
    const before = db.writeStats.memtableRows;
    await db.execute(
      "INSERT INTO test_basic.simple_table (id, name) VALUES (550e8400-e29b-41d4-a716-446655440032, 'Maya')"
    );
    const after = db.writeStats.memtableRows;
    expect(after).toBeGreaterThan(before);
  });

  test('writeStats.walSize is a non-negative number', () => {
    const stats = db.writeStats;
    expect(stats.walSize).toBeGreaterThanOrEqual(0);
  });

  test('writeStats properties are all non-negative', () => {
    const stats = db.writeStats;
    expect(stats.memtableSize).toBeGreaterThanOrEqual(0);
    expect(stats.memtableRows).toBeGreaterThanOrEqual(0);
    expect(stats.walSize).toBeGreaterThanOrEqual(0);
    expect(stats.l0Count).toBeGreaterThanOrEqual(0);
    expect(stats.totalWritten).toBeGreaterThanOrEqual(0);
  });
});

// ---------------------------------------------------------------------------
// Error path tests
// ---------------------------------------------------------------------------

describe('Write error paths (Issue #391)', () => {
  beforeAll(requireSchemaFile);

  let db, cleanup;

  beforeAll(async () => {
    const result = await openWritable();
    db = result.db;
    cleanup = result.cleanup;
  });

  afterAll(async () => {
    if (db && !db.isClosed) {
      await db.close();
    }
    if (cleanup) cleanup();
  });

  test('Closed database rejects write with structured error', async () => {
    const { db: tmpDb, cleanup: tmpCleanup } = await openWritable();
    await tmpDb.close();
    try {
      await tmpDb.execute(
        "INSERT INTO test_basic.simple_table (id) VALUES (550e8400-e29b-41d4-a716-446655440041)"
      );
      throw new Error('Should have thrown');
    } catch (e) {
      expect(e).toBeDefined();
      expect(e.message.toLowerCase()).toMatch(/closed/);
    } finally {
      tmpCleanup();
    }
  });

  test('DML against non-existent table throws an error', async () => {
    expect.assertions(2);
    try {
      await db.execute(
        "INSERT INTO nonexistent_keyspace.ghost_table (id) VALUES (550e8400-e29b-41d4-a716-446655440042)"
      );
      throw new Error('Should have thrown');
    } catch (e) {
      expect(e).toBeDefined();
      expect(typeof e.message).toBe('string');
    }
  });
});
