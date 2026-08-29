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
 *  - `kind === 'value'` → the path must render, and `actual` must equal
 *    `expected` exactly. A multi-thousand-digit rendering is compared as a
 *    *digest* (a digit run longer than 64 collapses to `{<length>}`), which
 *    still pins the exact digit count and the exact surrounding form.
 *  - `kind === 'error'` → the path must refuse, and `expected` must appear
 *    **verbatim inside** `actual`. Containment only because each binding wraps
 *    the one canonical message in its own typed-error envelope.
 */

const { _ffiCommonRenderVectors } = require('../lib/index.js');

const VECTORS = _ffiCommonRenderVectors();
const byType = (cqlType) => VECTORS.filter((entry) => entry.cqlType === cqlType);

describe('shared cross-binding vector table (issue #1452)', () => {
  // Without this a vacuous pass would be possible: an empty table makes every
  // per-entry assertion below trivially satisfied.
  test('the table is present and covers all three types', () => {
    expect(VECTORS.length).toBeGreaterThan(0);
    for (const cqlType of ['decimal', 'varint', 'inet']) {
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
        expect(entry.actual).toBe(entry.expected);
      } else if (entry.kind === 'error') {
        expect(entry.outcome).toBe('err');
        expect(entry.actual).toContain(entry.expected);
      } else {
        throw new Error(`${entry.name}: unknown expectation kind '${entry.kind}'`);
      }
    },
  );

  // The concrete divergence issue #1452 closed: a 2000-byte well-formed unscaled
  // magnitude with scale 3 rendered here and raised `CqliteError` in Python.
  test('the 2000-byte decimal that used to diverge renders with full precision', () => {
    const entry = VECTORS.find(
      (v) => v.name === 'decimal/large-well-formed-2000-bytes-scale-3',
    );
    expect(entry).toBeDefined();
    expect(entry.outcome).toBe('ok');
    expect(entry.actual).toBe('{4817}e-3');
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
