/**
 * Cross-binding shared-vector assertions (issue #1452).
 *
 * `cqlite-ffi-common` holds exactly ONE implementation of CQL `decimal`,
 * `varint` and `inet` rendering, and exports a committed table of
 * `(input, expected outcome)` vectors. This suite renders **every** entry
 * through this binding's production conversion path (`value_to_napi`, the same
 * call `row_to_object` makes for a real result row) and asserts the outcome; the
 * Python binding's `tests/test_shared_vectors.py` asserts the *same committed
 * table* through *its* production path.
 *
 * That is what makes "single implementation" an assertion rather than a comment:
 * a divergence between the two bindings — or a re-introduced private copy in
 * either — fails BOTH suites. Adding an entry to the shared crate covers it in
 * both suites with no per-binding edit.
 *
 * The two comparison rules come from the shared crate's `VectorOutcome`
 * contract:
 *
 *  - `kind === 'value'` → the path must render, and the **full** rendering must
 *    satisfy the entry's exact check: `rendered === expected` for a short
 *    rendering, or `sha256(rendered) === expectedSha256` for a multi-kilobyte
 *    one whose literal form is committed as a digest. `actual` (the digest) is
 *    compared too, but only as the readable half of a failure message — a digest
 *    collapses a long digit run to `{<length>}`, so on its own it would compare
 *    a digit COUNT and pass two bindings that render *different digits of the
 *    same length*.
 *  - `kind === 'error'` → the path must refuse, and `expected` must appear
 *    **verbatim inside** `actual`. Containment only because each binding wraps
 *    the one canonical message in its own typed-error envelope.
 *
 * The hash is SHA-256 over the **UTF-8 bytes** of the rendered string, lower-case
 * hex — the same statement the shared crate's `vectors` module makes, so this
 * suite, the Python suite and the crate's own test cannot disagree about
 * encoding. Each side hashes with its own standard library (`crypto` here,
 * `hashlib` in Python, `sha2` in the crate): three independent implementations
 * over one committed hex string.
 */

const crypto = require('node:crypto');

const { _ffiCommonRenderVectors, _jsonNumberFromText } = require('../lib/index.js');
const { parseErrorMetadata } = require('../lib/error-wrapper.js');

/** Lower-case SHA-256 hex of a string's UTF-8 bytes. */
const sha256Hex = (text) =>
  crypto.createHash('sha256').update(text, 'utf8').digest('hex');

const VECTORS = _ffiCommonRenderVectors();
const byType = (cqlType) => VECTORS.filter((entry) => entry.cqlType === cqlType);

describe('shared cross-binding vector table (issue #1452)', () => {
  // Without this a vacuous pass would be possible: an empty table makes every
  // per-entry assertion below trivially satisfied.
  test('the table is present and covers every type', () => {
    expect(VECTORS.length).toBeGreaterThan(0);
    for (const cqlType of ['decimal', 'varint', 'inet', 'json_number']) {
      expect(byType(cqlType).length).toBeGreaterThan(0);
    }
    const names = VECTORS.map((entry) => entry.name);
    expect(new Set(names).size).toBe(names.length);
    // The refusal path is covered, not just the happy path.
    expect(VECTORS.some((entry) => entry.kind === 'error')).toBe(true);
  });

  test.each(VECTORS.map((entry) => [entry.name, entry]))(
    'every vector renders as the committed table says: %s',
    (_name, entry) => {
      if (entry.kind === 'value') {
        expect(entry.outcome).toBe('ok');
        // The readable half.
        expect(entry.actual).toBe(entry.expected);
        // The EXACT half, on the full rendering this binding produced.
        expect(typeof entry.rendered).toBe('string');
        if (entry.expectedSha256 === null || entry.expectedSha256 === undefined) {
          expect(entry.rendered).toBe(entry.expected);
        } else {
          expect(sha256Hex(entry.rendered)).toBe(entry.expectedSha256);
        }
      } else if (entry.kind === 'error') {
        expect(entry.outcome).toBe('err');
        expect(entry.actual).toContain(entry.expected);
      } else {
        throw new Error(`${entry.name}: unknown expectation kind '${entry.kind}'`);
      }
    },
  );

  // No value entry may be checked by digit count alone: either its expectation
  // is committed verbatim (so equality is exact) or it carries a SHA-256 of the
  // full rendering. Without this, a future long entry could quietly regress to
  // the digest-only comparison this pairing exists to prevent.
  test('every value entry carries an exact oracle, not just a digest', () => {
    const values = VECTORS.filter((entry) => entry.kind === 'value');
    expect(values.length).toBeGreaterThan(0);
    let digested = 0;
    for (const entry of values) {
      const collapsed = entry.expected.includes('{');
      const hasHash = typeof entry.expectedSha256 === 'string';
      expect(collapsed).toBe(hasHash);
      if (hasHash) {
        expect(entry.expectedSha256).toMatch(/^[0-9a-f]{64}$/);
        digested += 1;
      }
    }
    // The multi-kilobyte boundary magnitudes are the reason this exists.
    expect(digested).toBeGreaterThanOrEqual(3);
  });

  // The concrete divergence issue #1452 closed: a 2000-byte well-formed unscaled
  // magnitude with scale 3 rendered here and raised `CqliteError` in Python.
  test('the 2000-byte decimal that used to diverge renders with full precision', () => {
    const entry = VECTORS.find(
      (v) => v.name === 'decimal/large-well-formed-2000-bytes-scale-3',
    );
    expect(entry).toBeDefined();
    expect(entry.outcome).toBe('ok');
    expect(entry.actual).toBe('{4817}e-3');
    // Every one of the 4817 digits, not just how many there are.
    expect(entry.rendered).toMatch(/^[0-9]{4817}e-3$/);
    expect(sha256Hex(entry.rendered)).toBe(
      'e1ec7b41fe833049052e89e01d3cdda36fcfc6dd69ec5deb03d52c116aa55214',
    );
  });

  // Every refusal must carry the shared FFI error contract's identity for a
  // DATA fault (issue #1451): a corrupt cell is a data fault, never an internal
  // bug. `actual` is the raw native `reason`, i.e. exactly the string
  // `lib/error-wrapper.js` parses on a real throw, so this asserts the metadata
  // a caller of `executeNative()` would see.
  //
  // This is the assertion that caught the INET adapter bypassing
  // `to_napi_error()` and mapping with `napi::Error::from_reason` instead: with
  // no `\0code=` metadata in the message, `parseErrorMetadata` fell back to its
  // INTERNAL/Internal defaults and a corrupt inet cell claimed an internal-bug
  // identity. The DECIMAL refusal in the same table always had it right, so a
  // per-type sweep is what makes the two adapters comparable.
  test('every refusal carries the #1451 contract identity, not the INTERNAL default', () => {
    const refusals = VECTORS.filter((entry) => entry.kind === 'error');
    expect(refusals.length).toBeGreaterThan(0);
    const seenTypes = new Set();
    for (const entry of refusals) {
      const metadata = parseErrorMetadata(entry.actual);
      expect({ name: entry.name, ...metadata, message: undefined }).toEqual({
        name: entry.name,
        code: 'PARSE',
        category: 'Data',
        isRecoverable: false,
        message: undefined,
      });
      // The human-readable half survives the metadata suffix.
      expect(metadata.message).toContain(entry.expected);
      seenTypes.add(entry.cqlType);
    }
    // Both refusing adapters are covered, not just one.
    expect([...seenTypes].sort()).toEqual(['decimal', 'inet']);
  });

  // A malformed inet must never come back as raw bytes, hex, or any other
  // passthrough (no-heuristics, issue #28).
  test('a malformed inet length is a typed refusal, never a passthrough', () => {
    const malformed = byType('inet').filter((entry) => entry.kind === 'error');
    expect(malformed.length).toBeGreaterThan(0);
    for (const entry of malformed) {
      expect(entry.outcome).toBe('err');
      expect(entry.actual).toContain('expected 4 or 16');
      expect(entry.actual).not.toMatch(/^0x[0-9a-f]+$/);
    }
  });
});

// =============================================================================
// JSON numbers (issue #3505) — the WIRING evidence for `json_number_to_napi`
// =============================================================================
//
// Why these exist rather than only the shared-crate tests: before this table the
// production adapter `value::json_number_to_napi` had ZERO test callers anywhere
// in the repository. The mutation `JsonNumberClass::U64(u) =>
// env.create_double(u as f64)` reddened NOTHING — not the shared crate's own
// tests (they pin `classify_json_number` only), not
// `values-equal-numeric.test.js` (it pins the comparison harness), not the
// Python suite. So AC1's observable claim ("`u64::MAX` reaches JS as a
// `BigInt`") was asserted by no test at all.
//
// Both halves are needed, and in JS the type half carries more weight than in
// Python: `String(9223372036854775808)` is the SAME text whether the value is a
// lossy double or an exact BigInt, so only `typeof` separates them.
describe('shared JSON-number vectors (issue #3505)', () => {
  const JSON_NUMBERS = byType('json_number');
  // JS integers above this cannot be held exactly by a `number`, so the host
  // type MUST be `bigint` — this is Number.MAX_SAFE_INTEGER's bound, stated as
  // the BigInt it is compared against.
  const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

  test.each(JSON_NUMBERS.map((entry) => [entry.name, entry]))(
    'arrives as the committed host type: %s',
    (_name, entry) => {
      // The table commits no refusals: the only refusing arm (Beyond) is
      // unreachable in a default build.
      expect(entry.kind).toBe('value');
      const text = Buffer.from(entry.bytes).toString('utf8');
      const value = _jsonNumberFromText(text);

      if (entry.hostKind === 'integer') {
        const expected = BigInt(entry.expected);
        // Exact value, whichever host type carries it.
        expect(typeof value === 'bigint' || typeof value === 'number').toBe(true);
        if (typeof value === 'number') {
          // A `number` is an f64: it may only carry this literal if the literal
          // is inside the safe-integer range. Outside it, a `number` IS the
          // #3505 defect, so the type is asserted rather than the value alone.
          expect(Number.isInteger(value)).toBe(true);
          const magnitude = expected < 0n ? -expected : expected;
          expect(magnitude).toBeLessThanOrEqual(MAX_SAFE);
        }
        expect(BigInt(value)).toBe(expected);
      } else if (entry.hostKind === 'float') {
        expect(typeof value).toBe('number');
        expect(value).toBe(Number(entry.expected));
      } else {
        throw new Error(`${entry.name}: unknown hostKind '${entry.hostKind}'`);
      }
    },
  );

  // A guard against the table being silently narrowed to values an f64 can hold,
  // which would leave every assertion above green while covering nothing.
  test('the u64 range is actually covered', () => {
    const I64_MAX = 9223372036854775807n;
    const aboveI64Max = JSON_NUMBERS.filter(
      (entry) => entry.hostKind === 'integer' && BigInt(entry.expected) > I64_MAX,
    );
    expect(aboveI64Max.length).toBeGreaterThan(0);
    // And at least one whose f64 rounding is OBSERVABLE in the rendering, so the
    // text half of the check does not rest on the type half alone.
    expect(
      aboveI64Max.some((entry) => String(Number(entry.expected)) !== entry.expected),
    ).toBe(true);
    // Each of those must arrive as a BigInt — the headline claim, stated
    // directly rather than only through the parametrised loop.
    for (const entry of aboveI64Max) {
      const text = Buffer.from(entry.bytes).toString('utf8');
      expect(typeof _jsonNumberFromText(text)).toBe('bigint');
    }
  });

  test('_jsonNumberFromText is fail-closed on non-numbers', () => {
    for (const bad of ['"18446744073709551615"', 'not-a-number', '', '[1]', '1 2']) {
      expect(() => _jsonNumberFromText(bad)).toThrow();
    }
  });
});
