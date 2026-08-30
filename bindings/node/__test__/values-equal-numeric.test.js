/**
 * `valuesEqual` numeric-comparison contract (issue #3505).
 *
 * Node does NOT have Python's masking bug: the `bigint`↔`number` arms compare
 * with `actual === BigInt(expected)`, which is exact — no float coercion — so an
 * exact integer never silently matches a rounded double. That is asserted here
 * so a future "let's make Node tolerant like Python" edit reds instead of
 * regressing.
 *
 * What Node DOES have is a crash: `BigInt(expected)` throws `RangeError` for a
 * non-integer `number`, so a genuine mismatch (a bigint on one side, a
 * fractional double on the other) took the whole harness down instead of being
 * reported as a mismatch. Hardened here.
 *
 * The Node CEILING is the ORACLE, not the binding: `JSON.parse` reads a
 * golden's `18446744073709551615` into an f64 and loses it before `valuesEqual`
 * is ever called, and JS has no lossless integer JSON parse without a custom
 * reviver. That limit is documented, not coerced away — see the comment on the
 * bigint arms in `parity-utils.js`.
 */

const { valuesEqual } = require('./parity-utils.js');

describe('valuesEqual numeric comparison (issue #3505)', () => {
  test('bigint vs number is EXACT, not float-coerced', () => {
    // 2**53 + 1 is the first integer a double cannot represent.
    const above = 9007199254740993n;
    expect(Number(above)).toBe(9007199254740992); // premise: the double rounds
    expect(valuesEqual(above, Number(above))).toBe(false);
    expect(valuesEqual(Number(above), above)).toBe(false);
  });

  test('bigint vs an exactly-representable number matches', () => {
    expect(valuesEqual(9007199254740992n, 9007199254740992)).toBe(true);
    expect(valuesEqual(9007199254740992, 9007199254740992n)).toBe(true);
    expect(valuesEqual(42n, 42)).toBe(true);
    expect(valuesEqual(42, 42n)).toBe(true);
  });

  test('a non-integer number can never equal a bigint (no RangeError)', () => {
    // BigInt(1.5) throws RangeError; the comparison must report a mismatch.
    expect(valuesEqual(1n, 1.5)).toBe(false);
    expect(valuesEqual(1.5, 1n)).toBe(false);
    expect(valuesEqual(0n, 0.5)).toBe(false);
    expect(valuesEqual(-2n, -1.5)).toBe(false);
  });

  test('NaN and Infinity against a bigint are mismatches, not crashes', () => {
    expect(valuesEqual(1n, NaN)).toBe(false);
    expect(valuesEqual(NaN, 1n)).toBe(false);
    expect(valuesEqual(1n, Infinity)).toBe(false);
    expect(valuesEqual(Infinity, 1n)).toBe(false);
    expect(valuesEqual(1n, -Infinity)).toBe(false);
    expect(valuesEqual(-Infinity, 1n)).toBe(false);
  });

  test('bigint vs bigint stays exact', () => {
    expect(valuesEqual(18446744073709551615n, 18446744073709551615n)).toBe(true);
    expect(valuesEqual(18446744073709551615n, 18446744073709551614n)).toBe(false);
  });

  test('number vs number keeps its tolerance (genuine double columns)', () => {
    expect(valuesEqual(0.1 + 0.2, 0.3)).toBe(true);
    expect(valuesEqual(1.0, 2.0)).toBe(false);
  });
});
