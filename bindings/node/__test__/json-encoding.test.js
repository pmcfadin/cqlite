/**
 * JSON Encoding Tests for Issue #343.
 *
 * Documents and validates the deprecated hex encoding format used by execute()
 * for varint and decimal types.
 *
 * These tests serve as executable documentation for the encoding format while
 * verifying that the deprecated path continues to work correctly.
 *
 * Encoding Formats:
 * - Varint: "0x{hex}" - Two's complement big-endian hex encoding
 * - Decimal: "decimal:{scale}:0x{hex}" - Scale + hex-encoded unscaled value
 */

const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable } = require('./helpers.js');

describe('JSON Encoding Format (Issue #343)', () => {
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

  describe('Decimal Hex Encoding', () => {
    test('decimal from execute() returns string', async () => {
      const result = await db.execute(
        'SELECT account_balance FROM test_basic.simple_table LIMIT 10'
      );

      let foundDecimal = false;
      for (const row of result.rows) {
        if (row.account_balance !== null) {
          foundDecimal = true;
          expect(typeof row.account_balance).toBe('string');
          console.log(`  execute() decimal: ${row.account_balance}`);
        }
      }

      if (!foundDecimal) {
        console.log('  No non-null decimal values found in test data');
      }
    });

    test('decimal from executeNative() returns human-readable string', async () => {
      const result = await db.executeNative(
        'SELECT account_balance FROM test_basic.simple_table LIMIT 10'
      );

      let foundDecimal = false;
      for (const row of result.rows) {
        if (row.account_balance !== null) {
          foundDecimal = true;
          expect(typeof row.account_balance).toBe('string');
          // Native path returns decimal notation like "123.45" or "-45.67"
          expect(row.account_balance).toMatch(/^-?\d+\.?\d*$/);
          console.log(`  executeNative() decimal: ${row.account_balance}`);
        }
      }

      if (!foundDecimal) {
        console.log('  No non-null decimal values found in test data');
      }
    });
  });

  describe('BigInt Types', () => {
    test('bigint from execute() returns number or string', async () => {
      const result = await db.execute(
        'SELECT salary FROM test_basic.simple_table LIMIT 10'
      );

      let foundBigint = false;
      for (const row of result.rows) {
        if (row.salary !== null) {
          foundBigint = true;
          // execute() returns number for bigint (may lose precision)
          expect(['number', 'string']).toContain(typeof row.salary);
          console.log(`  execute() bigint: ${row.salary} (${typeof row.salary})`);
        }
      }

      if (!foundBigint) {
        console.log('  No non-null bigint values found in test data');
      }
    });

    test('bigint from executeNative() returns BigInt', async () => {
      const result = await db.executeNative(
        'SELECT salary FROM test_basic.simple_table LIMIT 10'
      );

      let foundBigint = false;
      for (const row of result.rows) {
        if (row.salary !== null) {
          foundBigint = true;
          expect(typeof row.salary).toBe('bigint');
          console.log(`  executeNative() bigint: ${row.salary}n`);
        }
      }

      if (!foundBigint) {
        console.log('  No non-null bigint values found in test data');
      }
    });
  });

  describe('execute() vs executeNative() Comparison', () => {
    test('both methods return same row count', async () => {
      const query = 'SELECT * FROM test_basic.simple_table LIMIT 50';

      const jsonResult = await db.execute(query);
      const nativeResult = await db.executeNative(query);

      expect(jsonResult.rowCount).toBe(nativeResult.rowCount);
      expect(jsonResult.rows.length).toBe(nativeResult.rows.length);
      console.log(`  Both methods returned ${jsonResult.rowCount} rows`);
    });

    test('both methods return same column names', async () => {
      const query = 'SELECT * FROM test_basic.simple_table LIMIT 1';

      const jsonResult = await db.execute(query);
      const nativeResult = await db.executeNative(query);

      if (jsonResult.rows.length > 0 && nativeResult.rows.length > 0) {
        const jsonCols = Object.keys(jsonResult.rows[0]).sort();
        const nativeCols = Object.keys(nativeResult.rows[0]).sort();
        expect(jsonCols).toEqual(nativeCols);
        console.log(`  Columns: ${jsonCols.join(', ')}`);
      }
    });
  });

  describe('Hex Parsing Utilities Validation', () => {
    const { parseVarintHex, parseDecimalHex, isVarintHex, isDecimalHex } = require('./parity-utils');

    test('parseVarintHex handles positive values correctly', () => {
      expect(parseVarintHex('0x00')).toBe(0n);
      expect(parseVarintHex('0x01')).toBe(1n);
      expect(parseVarintHex('0x7f')).toBe(127n);       // Max positive 1-byte
      expect(parseVarintHex('0x0100')).toBe(256n);     // Needs 2 bytes
      expect(parseVarintHex('0x00ff')).toBe(255n);     // 255 requires leading zero
    });

    test('parseVarintHex handles negative values correctly', () => {
      expect(parseVarintHex('0xff')).toBe(-1n);        // All bits set = -1
      expect(parseVarintHex('0x80')).toBe(-128n);      // Min negative 1-byte
      expect(parseVarintHex('0xfe')).toBe(-2n);
    });

    test('parseDecimalHex converts to human-readable format', () => {
      expect(parseDecimalHex('decimal:2:0x7b')).toBe('1.23');
      expect(parseDecimalHex('decimal:0:0x64')).toBe('100');
      expect(parseDecimalHex('decimal:3:0x01')).toBe('0.001');
    });

    test('parseDecimalHex handles negative decimals', () => {
      // -4567 in two's complement (2 bytes) = 0xee29
      expect(parseDecimalHex('decimal:2:0xee29')).toBe('-45.67');
    });

    test('isVarintHex identifies varint format correctly', () => {
      expect(isVarintHex('0x7f')).toBe(true);
      expect(isVarintHex('0xdeadbeef')).toBe(true);
      expect(isVarintHex('decimal:2:0x7b')).toBe(false);  // Decimal, not varint
      expect(isVarintHex('not hex')).toBe(false);
      expect(isVarintHex(123)).toBe(false);
    });

    test('isDecimalHex identifies decimal format correctly', () => {
      expect(isDecimalHex('decimal:2:0x7b')).toBe(true);
      expect(isDecimalHex('decimal:0:0x64')).toBe(true);
      expect(isDecimalHex('0x7f')).toBe(false);          // Varint, not decimal
      expect(isDecimalHex('invalid')).toBe(false);
    });
  });

  describe('Format Documentation', () => {
    /**
     * This test documents the hex encoding format.
     * It serves as executable documentation for users who need to decode
     * varint/decimal values from the deprecated execute() method.
     */
    test('documents varint hex format', () => {
      // Varint encoding format: "0x{hex}"
      // The hex is two's complement big-endian encoding

      const examples = [
        { description: 'Small positive: 127', hex: '0x7f' },
        { description: 'Large positive: 255 (needs leading zero)', hex: '0x00ff' },
        { description: 'Negative: -1', hex: '0xff' },
        { description: 'Negative: -128', hex: '0x80' },
        { description: 'Zero: 0', hex: '0x00' },
        { description: 'Large: 65536', hex: '0x010000' },
      ];

      console.log('  Varint hex encoding examples:');
      for (const { description, hex } of examples) {
        console.log(`    ${description} -> ${hex}`);
      }

      // This test always passes - it's documentation
      expect(true).toBe(true);
    });

    test('documents decimal hex format', () => {
      // Decimal encoding format: "decimal:{scale}:0x{hex}"
      // - scale: number of decimal places
      // - hex: two's complement big-endian encoding of unscaled value

      const examples = [
        { decimal: '1.23', scale: 2, unscaled: 123, hex: 'decimal:2:0x7b' },
        { decimal: '-45.67', scale: 2, unscaled: -4567, hex: 'decimal:2:0xee29' },
        { decimal: '0.001', scale: 3, unscaled: 1, hex: 'decimal:3:0x01' },
        { decimal: '100', scale: 0, unscaled: 100, hex: 'decimal:0:0x64' },
      ];

      console.log('  Decimal hex encoding examples:');
      for (const { decimal, scale, unscaled, hex } of examples) {
        console.log(`    ${decimal} (scale=${scale}, unscaled=${unscaled}) -> ${hex}`);
      }

      // This test always passes - it's documentation
      expect(true).toBe(true);
    });
  });
});
