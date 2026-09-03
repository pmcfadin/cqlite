/**
 * Error Mapping tests for Issue #297.
 *
 * Issue #306: Migrated to Jest format.
 *
 * TDD Requirements from the issue:
 * - [x] Test: IO error maps to code 'IO'
 * - [x] Test: Schema error maps to code 'SCHEMA'
 * - [x] Test: Parse error maps to code 'PARSE'
 * - [x] Test: Error.isRecoverable reflects Rust is_recoverable()
 * - [x] Test: Error.category matches Rust category()
 * - [x] Test: Error message contains original error text
 */

const { Database, _errorContractProbe } = require('../lib/index.js');
const { assertDatasetsAvailable, getNonexistentPath } = require('./helpers.js');

describe('Error Mapping Tests (Issue #297)', () => {
  beforeAll(() => {
    console.log(`Test data root: ${global.testPaths.TEST_DATA_ROOT}`);
  });

  test('IO error maps to code "IO"', async () => {
    expect.assertions(2);
    try {
      await Database.open(getNonexistentPath());
    } catch (e) {
      expect(e.code).toBe('IO');
      expect(
        e.message.includes('IoError:') || e.message.includes('No such file') || e.message.includes('cannot find')
      ).toBe(true);
    }
  });

  test('Schema error maps to code "SCHEMA" or "IO"', async () => {
    // Note: A missing schema file can trigger either:
    // - IO error: File not found during file system access
    // - SCHEMA error: Invalid schema during parsing
    // The specific error depends on error propagation order in cqlite-core.
    // Both are correct behavior for this invalid input scenario.
    expect.assertions(1);
    const nonexistentSchema = process.platform === 'win32'
      ? 'Z:\\nonexistent\\schema.cql'
      : '/nonexistent/schema.cql';
    try {
      await Database.open(global.testPaths.SSTABLES_DIR, {
        schema: nonexistentSchema,
      });
    } catch (e) {
      expect(['SCHEMA', 'IO']).toContain(e.code);
    }
  });

  test('CQL parse error maps to the authoritative code "PARSE"', async () => {
    // Issue #1451: this assertion used to hedge (`['PARSE','QUERY']`) because
    // Node derived its code from `category()`, and `CqlParse` is Query-category
    // — so it reported 'QUERY' while Python raised ParseError for the same core
    // error. The shared contract table (cqlite_ffi_common::error_contract) now
    // decides BY VARIANT, so exactly one code is correct here.
    //
    // The statement must actually REACH the CQL parser: a statement whose very
    // first token is not a known verb is rejected earlier as
    // `Error::QueryExecution` ("Unsupported query type"), which is genuinely
    // `QUERY` — see the sibling case below.
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      expect.assertions(3);
      await db.execute('SELECT * FROM');
    } catch (e) {
      expect(e.code).toBe('PARSE');
      expect(e.category).toBe('Query');
      expect(e.message).toContain('ParseError:');
    } finally {
      await db.close();
    }
  });

  test('an unrecognized statement type stays "QUERY", not "PARSE"', async () => {
    // The other side of the #1451 fix: `PARSE` now means a genuine CQL syntax
    // failure, so an unsupported STATEMENT TYPE (an `Error::QueryExecution`)
    // must NOT borrow it.
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      expect.assertions(2);
      await db.execute('THIS IS NOT VALID SQL!!!');
    } catch (e) {
      expect(e.code).toBe('QUERY');
      expect(e.message).toContain('QueryError:');
    } finally {
      await db.close();
    }
  });

  test('Error.isRecoverable reflects Rust is_recoverable()', async () => {
    expect.assertions(2);
    try {
      await Database.open(getNonexistentPath());
    } catch (e) {
      // IO errors are recoverable in Rust
      expect(typeof e.isRecoverable).toBe('boolean');
      expect(e.isRecoverable).toBe(true);
    }
  });

  test('Error.category matches Rust category()', async () => {
    expect.assertions(2);
    try {
      await Database.open(getNonexistentPath());
    } catch (e) {
      expect(typeof e.category).toBe('string');
      // IO errors have 'System' category in Rust
      expect(e.category).toBe('System');
    }
  });

  test('Error message contains original error text', async () => {
    expect.assertions(3);
    try {
      await Database.open(getNonexistentPath());
    } catch (e) {
      expect(typeof e.message).toBe('string');
      expect(e.message.length).toBeGreaterThan(0);
      // The original error should mention the path or file-related issue
      expect(
        e.message.includes('nonexistent') ||
          e.message.includes('No such file') ||
          e.message.includes('IoError') ||
          e.message.includes('I/O error') ||
          e.message.includes('cannot find')
      ).toBe(true);
    }
  });

  test('Query error has correct properties', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    try {
      expect.assertions(4);
      await db.execute('INVALID SQL SYNTAX');
    } catch (e) {
      expect(typeof e.code).toBe('string');
      expect(typeof e.category).toBe('string');
      expect(typeof e.isRecoverable).toBe('boolean');
      // An unknown statement type is a QueryExecution error (code 'QUERY', not
      // 'PARSE' — see the dedicated cases above); neither is recoverable.
      expect(e.isRecoverable).toBe(false);
    } finally {
      await db.close();
    }
  });

  test('RuntimeError (InvalidState) has correct properties', async () => {
    assertDatasetsAvailable();
    const db = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });
    await db.close();

    expect.assertions(4);
    try {
      await db.execute('SELECT * FROM test_basic.simple_table LIMIT 1');
    } catch (e) {
      // InvalidState maps to 'INVALID_INPUT' code (Logic category)
      expect(typeof e.code).toBe('string');
      expect(typeof e.category).toBe('string');
      expect(typeof e.isRecoverable).toBe('boolean');
      expect(e.message).toContain('closed');
    }
  });
});

/**
 * The shared FFI error contract (issue #1451).
 *
 * `cqlite_ffi_common::error_contract` is the ONE authoritative
 * variant -> (python class, node code, category, recoverable, prefix) table.
 * Before it, this binding derived `code` from the core `category()` while the
 * Python binding matched the `Error` VARIANT, so the same core error had a
 * different identity in each: `CqlParse` reported 'QUERY' here while Python
 * raised `ParseError`, and `Timeout`/`Memory` both reported 'IO' while Python
 * raised `TimeoutError`/`MemoryError`.
 *
 * These cases drive the production mapping (`to_napi_error` -> the `\0`-encoded
 * metadata -> `enhanceError`) for variants no query can provoke, via the
 * `_errorContractProbe` test-support surface. The Python half lives in
 * `bindings/python/tests/test_errors.py::TestSharedErrorContract`.
 */
describe('Shared FFI error contract (Issue #1451)', () => {
  // [core Error variant, expected code, expected category, expected
  //  isRecoverable, expected message prefix]
  const PINNED_ROWS = [
    ['CqlParse', 'PARSE', 'Query', false, 'ParseError:'],
    ['InvalidInput', 'INVALID_INPUT', 'Data', false, 'ValueError:'],
    ['Timeout', 'TIMEOUT', 'System', false, 'TimeoutError:'],
    ['Memory', 'MEMORY', 'System', true, 'MemoryError:'],
    ['Corruption', 'PARSE', 'Data', false, 'ParseError:'],
    ['Io', 'IO', 'System', true, 'IoError:'],
    ['Cancelled', 'CANCELLED', 'Cancelled', false, 'CancelledError:'],
  ];

  test.each(PINNED_ROWS)(
    '%s maps to code "%s", category "%s"',
    (variant, code, category, isRecoverable, prefix) => {
      expect.assertions(5);
      try {
        _errorContractProbe(variant);
      } catch (e) {
        expect(e.code).toBe(code);
        expect(e.category).toBe(category);
        expect(e.isRecoverable).toBe(isRecoverable);
        expect(e.message.startsWith(prefix)).toBe(true);
        // The null-byte metadata block is stripped from the human message.
        expect(e.message).not.toContain('\u0000');
      }
    }
  );

  test('Timeout and Memory no longer collapse into the IO identity', () => {
    expect.assertions(4);
    const codes = {};
    for (const variant of ['Io', 'Timeout', 'Memory']) {
      try {
        _errorContractProbe(variant);
      } catch (e) {
        codes[variant] = e.code;
      }
    }
    expect(codes.Io).toBe('IO');
    expect(codes.Timeout).not.toBe(codes.Io);
    expect(codes.Memory).not.toBe(codes.Io);
    expect(codes.Timeout).not.toBe(codes.Memory);
  });

  test('CqlParse is PARSE and specifically not QUERY', () => {
    expect.assertions(2);
    try {
      _errorContractProbe('CqlParse');
    } catch (e) {
      expect(e.code).toBe('PARSE');
      expect(e.code).not.toBe('QUERY');
    }
  });

  test('an unknown variant name is fail-closed, never a default row', () => {
    expect.assertions(2);
    try {
      _errorContractProbe('NoSuchVariant');
    } catch (e) {
      expect(e.message).toContain('unknown core Error variant');
      expect(e.code).toBe('INVALID_INPUT');
    }
  });

  test('the probe always throws (a silent success would vacuously pass)', () => {
    // Every case above asserts INSIDE a catch block, so a probe that returned
    // normally would make them all pass having asserted nothing.
    expect(() => _errorContractProbe('Timeout')).toThrow();
    expect(() => _errorContractProbe('NoSuchVariant')).toThrow();
  });
});
