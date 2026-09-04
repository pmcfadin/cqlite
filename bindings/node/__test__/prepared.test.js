/**
 * Tests for prepared statement functionality.
 *
 * Issue #338: Implement Database.prepare() method for Node.js bindings.
 */
const { assertDatasetsAvailable, openDatabase, withDatabase } = require('./helpers');
const { Database, PreparedStatement } = require('../lib/index.js');

describe('PreparedStatement', () => {
  beforeAll(() => {
    assertDatasetsAvailable();
  });

  describe('PreparedStatement export (Issue #352)', () => {
    test('PreparedStatement is exported and defined', () => {
      expect(PreparedStatement).toBeDefined();
      expect(typeof PreparedStatement).toBe('function');
    });

    test('PreparedStatement is a class constructor', () => {
      // Verify it's a class (function with prototype)
      expect(PreparedStatement.prototype).toBeDefined();
      expect(PreparedStatement.prototype.constructor).toBe(PreparedStatement);
    });

    test('db.prepare() returns instance of PreparedStatement', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        expect(stmt instanceof PreparedStatement).toBe(true);
      });
    });
  });

  describe('Internal functions not exported (Issue #353)', () => {
    test('wrapPreparedStatement is NOT exported (internal only)', () => {
      // wrapPreparedStatement is an internal function used to wrap native
      // PreparedStatement objects. It should NOT be exported to consumers.
      // Issue #353: This was previously exported, but should remain internal.
      const mod = require('../lib/index.js');
      expect(mod.wrapPreparedStatement).toBeUndefined();
    });

    // Both surfaces are pinned by NAME, and each list is an independent
    // expectation the module can actually violate.
    //
    // The PUBLIC surface is exactly three names. The internal test-support
    // exports are underscore-prefixed by convention — `_errorContractProbe` and
    // `_errorContractNodeCodes` (issue #1451), `_ffiCommonRenderVectors`
    // (issue #1452) and `_jsonNumberFromText` (issue #3505) — and are
    // deliberately not part of the public surface. They are enumerated rather
    // than merely counted so that adding another internal export is a
    // deliberate edit here, not a silent widening. That is exactly what
    // happened for `_jsonNumberFromText`: this assertion caught the widening,
    // and this comment is the deliberate acknowledgement it demands.
    test('only Database, PreparedStatement, and version are exported publicly', () => {
      const mod = require('../lib/index.js');
      const allKeys = Object.keys(mod).sort();
      expect(allKeys.filter((key) => !key.startsWith('_'))).toEqual([
        'Database',
        'PreparedStatement',
        'version',
      ]);
      expect(allKeys.filter((key) => key.startsWith('_'))).toEqual([
        '_errorContractNodeCodes',
        '_errorContractProbe',
        '_ffiCommonRenderVectors',
        '_jsonNumberFromText',
      ]);
    });
  });

  describe('Database.prepare()', () => {
    test('prepare() returns a PreparedStatement instance', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        expect(stmt).not.toBeNull();
        expect(stmt).toBeDefined();
        // Check it has the expected properties
        expect(typeof stmt.query).toBe('string');
        expect(typeof stmt.parameterCount).toBe('number');
        expect(typeof stmt.stats).toBe('function');
      });
    });

    test('query property returns the original SQL', async () => {
      await withDatabase(async (db) => {
        const query = 'SELECT * FROM test_basic.simple_table LIMIT 10';
        const stmt = await db.prepare(query);
        expect(stmt.query).toBe(query);
      });
    });

    test('parameterCount property returns count', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        expect(stmt.parameterCount).toBeGreaterThanOrEqual(0);
        expect(Number.isInteger(stmt.parameterCount)).toBe(true);
      });
    });
  });

  describe('PreparedStatement.stats()', () => {
    test('stats() returns statistics object', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        const stats = stmt.stats();

        expect(stats).not.toBeNull();
        expect(typeof stats).toBe('object');
        expect('parameterCount' in stats).toBe(true);
        expect('planType' in stats).toBe(true);
        expect('estimatedCost' in stats).toBe(true);
        expect('estimatedRows' in stats).toBe(true);
        expect('cacheFriendly' in stats).toBe(true);
      });
    });

    test('stats() returns correct types', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        const stats = stmt.stats();

        expect(typeof stats.parameterCount).toBe('number');
        expect(Number.isInteger(stats.parameterCount)).toBe(true);

        expect(typeof stats.planType).toBe('string');
        expect(stats.planType.length).toBeGreaterThan(0);

        expect(typeof stats.estimatedCost).toBe('number');

        // Issue #351: estimatedRows must be bigint, not number
        expect(typeof stats.estimatedRows).toBe('bigint');

        expect(typeof stats.cacheFriendly).toBe('boolean');
      });
    });

    test('stats() estimatedRows is non-negative', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        const stats = stmt.stats();
        expect(stats.estimatedRows).toBeGreaterThanOrEqual(0n);
      });
    });

    test('stats() estimatedRows supports BigInt operations (Issue #351)', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        const stats = stmt.stats();

        // These operations would throw TypeError if value was a number
        expect(stats.estimatedRows & 0xFFFFFFFFn).toBeDefined();
        expect(BigInt.asIntN(64, stats.estimatedRows)).toBeDefined();

        // Arithmetic with bigint literals should work
        expect(stats.estimatedRows + 1n).toBeGreaterThan(0n);
      });
    });
  });

  describe('PreparedStatement.toString()', () => {
    test('toString() contains PreparedStatement and query text', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        const repr = stmt.toString();

        expect(repr).toContain('PreparedStatement');
        expect(repr).toContain('SELECT');
      });
    });
  });

  describe('Error handling', () => {
    test('prepare() on closed database throws error', async () => {
      const db = await openDatabase();
      await db.close();

      await expect(db.prepare('SELECT * FROM test_basic.simple_table'))
        .rejects.toThrow(/closed/i);
    });

    test('prepare() with invalid syntax throws error', async () => {
      await withDatabase(async (db) => {
        // Invalid SQL syntax
        await expect(db.prepare('SELEKT * FORM invalid'))
          .rejects.toThrow();
      });
    });
  });

  describe('Multiple prepared statements', () => {
    test('can prepare multiple different queries', async () => {
      await withDatabase(async (db) => {
        const stmt1 = await db.prepare('SELECT * FROM test_basic.simple_table');
        const stmt2 = await db.prepare('SELECT * FROM test_basic.simple_table LIMIT 5');

        expect(stmt1.query).not.toBe(stmt2.query);
        expect(stmt1.stats()).toBeDefined();
        expect(stmt2.stats()).toBeDefined();
      });
    });
  });
});
