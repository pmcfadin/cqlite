/**
 * Tests for prepared statement functionality.
 *
 * Issue #338: Implement Database.prepare() method for Node.js bindings.
 */
const { skipIfNoDatasets, openDatabase, withDatabase } = require('./helpers');
const { Database, PreparedStatement } = require('../lib/index.js');

describe('PreparedStatement', () => {
  beforeAll(() => {
    skipIfNoDatasets();
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

        // estimatedRows can be number or bigint (napi-rs returns i64 as number)
        expect(
          typeof stats.estimatedRows === 'number' ||
          typeof stats.estimatedRows === 'bigint'
        ).toBe(true);

        expect(typeof stats.cacheFriendly).toBe('boolean');
      });
    });

    test('stats() estimatedRows is non-negative', async () => {
      await withDatabase(async (db) => {
        const stmt = await db.prepare('SELECT * FROM test_basic.simple_table');
        const stats = stmt.stats();
        expect(Number(stats.estimatedRows)).toBeGreaterThanOrEqual(0);
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
