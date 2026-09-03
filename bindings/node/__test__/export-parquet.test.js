/**
 * Parquet export tests (Issue #687, Epic #682).
 *
 * Validates Database.exportParquet() against a scalar table and a
 * collections table:
 * - file creation with valid Parquet magic bytes (PAR1 header/footer)
 * - returned row count matches the equivalent execute() result
 * - rowGroupSize and compression options are accepted
 * - errors carry the standard code/category/isRecoverable properties
 *
 * Content-level validation (typed Arrow schemas, value round-trips) is
 * covered by the Rust read-back tests in cqlite-core and cqlite-cli;
 * these tests verify the binding surface without adding a JS Parquet
 * reader dependency.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { assertDatasetsAvailable, withDatabase } = require('./helpers.js');

const SCALAR_QUERY = 'SELECT * FROM test_basic.simple_table';
const COLLECTIONS_QUERY = 'SELECT * FROM test_collections.collection_table';

let tmpDir;

beforeAll(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-parquet-'));
});

afterAll(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function assertParquetMagic(filePath) {
  const data = fs.readFileSync(filePath);
  expect(data.length).toBeGreaterThan(8);
  expect(data.subarray(0, 4).toString('ascii')).toBe('PAR1');
  expect(data.subarray(data.length - 4).toString('ascii')).toBe('PAR1');
}

describe('exportParquet - scalar table', () => {
  test('creates a valid Parquet file with matching row count', async () => {
    assertDatasetsAvailable();
    await withDatabase(async (db) => {
      const out = path.join(tmpDir, 'simple_table.parquet');
      const rows = await db.exportParquet(SCALAR_QUERY, out);

      expect(fs.existsSync(out)).toBe(true);
      assertParquetMagic(out);

      const result = await db.execute(SCALAR_QUERY);
      expect(rows).toBe(result.rowCount);
      expect(rows).toBeGreaterThan(0);
    });
  });

  test('accepts rowGroupSize option', async () => {
    assertDatasetsAvailable();
    await withDatabase(async (db) => {
      const out = path.join(tmpDir, 'small_groups.parquet');
      const rows = await db.exportParquet(SCALAR_QUERY, out, {
        rowGroupSize: 100,
      });
      expect(rows).toBeGreaterThan(0);
      assertParquetMagic(out);
    });
  });

  test('accepts zstd compression', async () => {
    assertDatasetsAvailable();
    await withDatabase(async (db) => {
      const out = path.join(tmpDir, 'zstd.parquet');
      const rows = await db.exportParquet(SCALAR_QUERY, out, {
        compression: 'zstd',
      });
      expect(rows).toBeGreaterThan(0);
      assertParquetMagic(out);
    });
  });
});

describe('exportParquet - collections table', () => {
  test('creates a valid Parquet file with matching row count', async () => {
    assertDatasetsAvailable();
    await withDatabase(async (db) => {
      const out = path.join(tmpDir, 'collection_table.parquet');
      const rows = await db.exportParquet(COLLECTIONS_QUERY, out);

      expect(fs.existsSync(out)).toBe(true);
      assertParquetMagic(out);

      const result = await db.execute(COLLECTIONS_QUERY);
      expect(rows).toBe(result.rowCount);
      expect(rows).toBeGreaterThan(0);
    }, global.testPaths.SCHEMA_COLLECTIONS);
  });
});

describe('exportParquet - error handling', () => {
  test('invalid compression rejects with CONFIG code', async () => {
    assertDatasetsAvailable();
    await withDatabase(async (db) => {
      expect.assertions(3);
      try {
        await db.exportParquet(SCALAR_QUERY, path.join(tmpDir, 'x.parquet'), {
          compression: 'lz77',
        });
      } catch (e) {
        expect(e.code).toBe('CONFIG');
        expect(typeof e.category).toBe('string');
        expect(typeof e.isRecoverable).toBe('boolean');
      }
    });
  });

  test('zero rowGroupSize rejects with CONFIG code', async () => {
    assertDatasetsAvailable();
    await withDatabase(async (db) => {
      expect.assertions(1);
      try {
        await db.exportParquet(SCALAR_QUERY, path.join(tmpDir, 'x.parquet'), {
          rowGroupSize: 0,
        });
      } catch (e) {
        expect(e.code).toBe('CONFIG');
      }
    });
  });

  test('unwritable path rejects with IO code', async () => {
    assertDatasetsAvailable();
    await withDatabase(async (db) => {
      expect.assertions(2);
      try {
        await db.exportParquet(
          SCALAR_QUERY,
          '/nonexistent-dir/definitely/missing/out.parquet'
        );
      } catch (e) {
        expect(e.code).toBe('IO');
        expect(typeof e.isRecoverable).toBe('boolean');
      }
    });
  });

  test('unknown table exports an empty but valid Parquet file', async () => {
    // The query engine is permissive about unknown tables (matching
    // execute(), which returns 0 rows rather than erroring), so export
    // produces a valid empty file.
    assertDatasetsAvailable();
    await withDatabase(async (db) => {
      const out = path.join(tmpDir, 'empty.parquet');
      const rows = await db.exportParquet(
        'SELECT * FROM no_such_ks.no_such_table',
        out
      );
      expect(rows).toBe(0);
      assertParquetMagic(out);
    });
  });
});
