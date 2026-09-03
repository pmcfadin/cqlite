/**
 * Database wrapper tests for Issue #296.
 *
 * Issue #306: Migrated to Jest format.
 *
 * TDD Requirements from the issue:
 * - [x] Test: Database.open() returns Database instance
 * - [x] Test: Database.open() with invalid path rejects with IoError
 * - [x] Test: Database.execute() returns QueryResult
 * - [x] Test: Database.execute() with invalid SQL rejects with ParseError
 * - [x] Test: Database.close() resolves successfully
 * - [x] Test: Database.getStats() returns valid statistics
 */

const { Database, version } = require('../lib/index.js');
const { assertDatasetsAvailable, getNonexistentPath } = require('./helpers.js');

describe('Database Wrapper Tests (Issue #296)', () => {
  beforeAll(() => {
    console.log(`Test data root: ${global.testPaths.TEST_DATA_ROOT}`);
    console.log(`SSTables dir: ${global.testPaths.SSTABLES_DIR}`);
    console.log(`Schema file: ${global.testPaths.SCHEMA_BASIC_TYPES}`);
  });

  test('version() returns semver string', () => {
    const v = version();
    expect(typeof v).toBe('string');
    expect(v).toMatch(/^\d+\.\d+\.\d+/);
  });

  test('Database.open() returns Database instance', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    expect(db).not.toBeNull();
    expect(db).not.toBeUndefined();
    expect(typeof db.execute).toBe('function');
    expect(typeof db.close).toBe('function');
    expect(typeof db.getStats).toBe('function');
    await db.close();
  });

  test('Database.open() with invalid path rejects with IoError', async () => {
    expect.assertions(2);
    try {
      await Database.open(getNonexistentPath());
    } catch (e) {
      expect(e).toBeDefined();
      expect(
        e.message.includes('IoError') || e.message.includes('No such file') || e.message.includes('cannot find')
      ).toBe(true);
    }
  });

  test('Database.execute() returns QueryResult', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      const result = await db.execute(
        'SELECT * FROM test_basic.simple_table LIMIT 5'
      );

      // Verify QueryResult structure
      expect(typeof result).toBe('object');
      expect(Array.isArray(result.rows)).toBe(true);
      expect(typeof result.rowCount).toBe('number');
      expect(typeof result.executionTimeMs).toBe('number');

      // Verify row count matches rows array length
      expect(result.rowCount).toBe(result.rows.length);

      // Log for debugging
      console.log(`    Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
      if (result.rows.length > 0) {
        console.log(`    First row keys: ${Object.keys(result.rows[0]).join(', ')}`);
      }
    } finally {
      await db.close();
    }
  });

  test('Database.execute() with invalid SQL rejects with ParseError or QueryError', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      expect.assertions(1);
      await db.execute('THIS IS NOT VALID SQL AT ALL!!!');
    } catch (e) {
      // Note: The parser may successfully parse "THIS" as a token, but the query executor
      // will reject it as an unsupported query type. Both ParseError and QueryError are valid.
      expect(
        e.message.includes('ParseError') ||
          e.message.includes('QueryError') ||
          e.message.includes('parse') ||
          e.message.includes('syntax') ||
          e.message.includes('Unsupported')
      ).toBe(true);
    } finally {
      await db.close();
    }
  });

  test('Database.close() resolves successfully', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });

    // First close should succeed
    await db.close();

    // Second close should also succeed (idempotent)
    await db.close();

    // Verify isClosed getter
    expect(db.isClosed).toBe(true);
  });

  test('Database.getStats() returns valid statistics', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      const stats = await db.getStats();

      // Verify DatabaseStats structure
      expect(typeof stats).toBe('object');
      expect(typeof stats.totalSstables).toBe('number');
      // Issue #351: Stats fields must be bigint, not number
      expect(typeof stats.totalRows).toBe('bigint');
      expect(typeof stats.memoryUsedBytes).toBe('bigint');

      // Log for debugging
      console.log(`    SSTables: ${stats.totalSstables}`);
      console.log(`    Total rows: ${stats.totalRows}`);
      console.log(`    Memory: ${stats.memoryUsedBytes} bytes`);
    } finally {
      await db.close();
    }
  });

  test('DatabaseStats bigint fields support BigInt operations (Issue #351)', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      const stats = await db.getStats();

      // These operations would throw TypeError if values were numbers
      // (can't mix number and bigint in bitwise operations)
      expect(stats.totalRows & 0xFFFFFFFFn).toBeDefined();
      expect(BigInt.asIntN(64, stats.memoryUsedBytes)).toBeDefined();

      // Arithmetic with bigint literals should work
      expect(stats.totalRows + 1n).toBeGreaterThan(0n);
      expect(stats.memoryUsedBytes * 2n).toBeGreaterThanOrEqual(0n);
    } finally {
      await db.close();
    }
  });

  test('Operations on closed database fail', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    await db.close();

    // execute should fail
    await expect(
      db.execute('SELECT * FROM test_basic.simple_table LIMIT 1')
    ).rejects.toThrow(/closed/);

    // getStats should fail
    await expect(db.getStats()).rejects.toThrow(/closed/);
  });
});

describe('DatabaseOptions Tests (Issue #339)', () => {
  test('Database.open() with memoryLimit option', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
      memoryLimit: 256 * 1024 * 1024, // 256MB
    });
    expect(db).not.toBeNull();
    expect(typeof db.execute).toBe('function');

    // Verify database works with custom memory limit
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );
    expect(result.rowCount).toBeGreaterThanOrEqual(0);

    await db.close();
  });

  test('Database.open() with cacheEnabled=true option', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
      cacheEnabled: true,
    });
    expect(db).not.toBeNull();
    expect(typeof db.execute).toBe('function');

    // Verify database works with caching enabled
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );
    expect(result.rowCount).toBeGreaterThanOrEqual(0);

    await db.close();
  });

  test('Database.open() with cacheEnabled=false option', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
      cacheEnabled: false,
    });
    expect(db).not.toBeNull();
    expect(typeof db.execute).toBe('function');

    // Verify database works with caching disabled
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );
    expect(result.rowCount).toBeGreaterThanOrEqual(0);

    await db.close();
  });

  test('Database.open() with all options combined', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
      memoryLimit: 512 * 1024 * 1024, // 512MB
      cacheEnabled: true,
    });
    expect(db).not.toBeNull();
    expect(typeof db.execute).toBe('function');

    // Verify database works with all options
    const result = await db.execute(
      'SELECT * FROM test_basic.simple_table LIMIT 5'
    );
    expect(result.rowCount).toBeGreaterThanOrEqual(0);

    console.log(
      `    Combined options test: ${result.rowCount} rows in ${result.executionTimeMs}ms`
    );

    await db.close();
  });

  test('Database.open() accepts memoryLimit as BigInt-safe integer', async () => {
    assertDatasetsAvailable();
    // Test with 4GB limit (within safe integer range)
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
      memoryLimit: 4 * 1024 * 1024 * 1024, // 4GB
    });
    expect(db).not.toBeNull();
    await db.close();
  });

  test('Database.open() accepts memoryLimit of 1 byte (minimum valid value)', async () => {
    assertDatasetsAvailable();
    // Test boundary condition: 1 is the minimum valid memoryLimit
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
      memoryLimit: 1,
    });
    expect(db).not.toBeNull();
    await db.close();
  });

  test('DatabaseOptions fields are optional', async () => {
    assertDatasetsAvailable();
    // All fields should be optional - open with just schema
    const db1 = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    expect(db1).not.toBeNull();
    await db1.close();

    // Open with empty options object
    const db2 = await Database.open(global.testPaths.SSTABLES_DIR, {});
    expect(db2).not.toBeNull();
    await db2.close();
  });

  describe('memoryLimit edge cases', () => {
    test('Database.open() with zero memoryLimit should fail', async () => {
      assertDatasetsAvailable();
      await expect(
        Database.open(global.testPaths.SSTABLES_DIR, {
          schema: global.testPaths.SCHEMA_BASIC_TYPES,
          memoryLimit: 0,
        })
      ).rejects.toThrow(/memoryLimit must be at least 1 byte/);
    });

    test('Database.open() with negative memoryLimit should fail', async () => {
      assertDatasetsAvailable();
      await expect(
        Database.open(global.testPaths.SSTABLES_DIR, {
          schema: global.testPaths.SCHEMA_BASIC_TYPES,
          memoryLimit: -1024,
        })
      ).rejects.toThrow(/memoryLimit must be at least 1 byte/);
    });

    test('Database.open() with fractional memoryLimit (0.5) should fail', async () => {
      assertDatasetsAvailable();
      await expect(
        Database.open(global.testPaths.SSTABLES_DIR, {
          schema: global.testPaths.SCHEMA_BASIC_TYPES,
          memoryLimit: 0.5,
        })
      ).rejects.toThrow(/memoryLimit must be at least 1 byte/);
    });

    test('Database.open() with fractional memoryLimit (0.1) should fail', async () => {
      assertDatasetsAvailable();
      await expect(
        Database.open(global.testPaths.SSTABLES_DIR, {
          schema: global.testPaths.SCHEMA_BASIC_TYPES,
          memoryLimit: 0.1,
        })
      ).rejects.toThrow(/memoryLimit must be at least 1 byte/);
    });

    test('Database.open() with fractional memoryLimit (0.99) should fail', async () => {
      assertDatasetsAvailable();
      await expect(
        Database.open(global.testPaths.SSTABLES_DIR, {
          schema: global.testPaths.SCHEMA_BASIC_TYPES,
          memoryLimit: 0.99,
        })
      ).rejects.toThrow(/memoryLimit must be at least 1 byte/);
    });

    test('Database.open() with NaN memoryLimit should fail', async () => {
      assertDatasetsAvailable();
      await expect(
        Database.open(global.testPaths.SSTABLES_DIR, {
          schema: global.testPaths.SCHEMA_BASIC_TYPES,
          memoryLimit: NaN,
        })
      ).rejects.toThrow(/memoryLimit must be a finite number/);
    });

    test('Database.open() with Infinity memoryLimit should fail', async () => {
      assertDatasetsAvailable();
      await expect(
        Database.open(global.testPaths.SSTABLES_DIR, {
          schema: global.testPaths.SCHEMA_BASIC_TYPES,
          memoryLimit: Infinity,
        })
      ).rejects.toThrow(/memoryLimit must be a finite number/);
    });
  });
});
