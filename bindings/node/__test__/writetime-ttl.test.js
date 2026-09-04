/**
 * WRITETIME()/TTL() output for Node.js bindings (Issue #693).
 *
 * Verifies end-to-end behaviour of WRITETIME() and TTL() expressions through
 * the @cqlite/node bindings:
 *
 * - Column names containing parentheses are preserved in row objects and in
 *   result.columns metadata.
 * - execute() returns the writetime value as a JSON-serialisable number.
 * - executeNative() returns the writetime value as a BigInt.
 * - TTL returns null when no TTL is set on the rows.
 * - Column names are accessible via bracket notation (row['writetime(col)']).
 */

'use strict';

const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable } = require('./helpers.js');

const QUERY =
  'SELECT id, WRITETIME(name), TTL(name) FROM test_basic.simple_table LIMIT 5';

describe('WRITETIME/TTL output (Issue #693)', () => {
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

  // --------------------------------------------------------------------------
  // execute() path (JSON values)
  // --------------------------------------------------------------------------

  describe('execute() path', () => {
    test('query returns rows', async () => {
      const result = await db.execute(QUERY);
      expect(result.rowCount).toBeGreaterThan(0);
    });

    test('writetime(name) key is present in each row', async () => {
      const result = await db.execute(QUERY);
      expect(result.rows.length).toBeGreaterThan(0);
      for (const row of result.rows) {
        // Parenthesised names require bracket notation.
        expect(Object.prototype.hasOwnProperty.call(row, 'writetime(name)')).toBe(true);
      }
    });

    test('writetime(name) is a non-null number for stored rows', async () => {
      const result = await db.execute(QUERY);
      let foundNonNull = false;
      for (const row of result.rows) {
        const wt = row['writetime(name)'];
        if (wt !== null && wt !== undefined) {
          expect(typeof wt).toBe('number');
          expect(wt).toBeGreaterThan(0);
          foundNonNull = true;
        }
      }
      expect(foundNonNull).toBe(true);
    });

    test('ttl(name) is null when no TTL is set', async () => {
      const result = await db.execute(QUERY);
      expect(result.rows.length).toBeGreaterThan(0);
      for (const row of result.rows) {
        expect(row['ttl(name)']).toBeNull();
      }
    });
  });

  // --------------------------------------------------------------------------
  // executeNative() path (native JS types)
  // --------------------------------------------------------------------------

  describe('executeNative() path', () => {
    test('query returns rows', async () => {
      const result = await db.executeNative(QUERY);
      expect(result.rowCount).toBeGreaterThan(0);
    });

    test('writetime(name) key is present in each row', async () => {
      const result = await db.executeNative(QUERY);
      expect(result.rows.length).toBeGreaterThan(0);
      for (const row of result.rows) {
        expect(Object.prototype.hasOwnProperty.call(row, 'writetime(name)')).toBe(true);
      }
    });

    test('writetime(name) is a BigInt for stored rows', async () => {
      const result = await db.executeNative(QUERY);
      let foundNonNull = false;
      for (const row of result.rows) {
        const wt = row['writetime(name)'];
        if (wt !== null && wt !== undefined) {
          expect(typeof wt).toBe('bigint');
          expect(wt).toBeGreaterThan(0n);
          foundNonNull = true;
        }
      }
      expect(foundNonNull).toBe(true);
    });

    test('ttl(name) is null when no TTL is set', async () => {
      const result = await db.executeNative(QUERY);
      expect(result.rows.length).toBeGreaterThan(0);
      for (const row of result.rows) {
        expect(row['ttl(name)']).toBeNull();
      }
    });
  });

  // --------------------------------------------------------------------------
  // Column metadata
  // --------------------------------------------------------------------------

  describe('column metadata', () => {
    test('columns include writetime(name) and ttl(name)', async () => {
      const result = await db.execute(QUERY);
      const colNames = result.columns.map((c) => c.name);
      expect(colNames).toContain('writetime(name)');
      expect(colNames).toContain('ttl(name)');
    });
  });
});
