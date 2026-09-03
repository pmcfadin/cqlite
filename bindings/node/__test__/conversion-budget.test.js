/**
 * Per-row heap BUDGET ratchet for the Node binding conversion path (issue #1449,
 * parent #1433).
 *
 * The per-row conversion wins landed in this epic — #1445 (interned/ordered
 * keys), #1446 (ordered Node rows + one-time column-name interning), #1447
 * (clone -> move of row values in executeNative), #1448 (Set/Map constructor
 * caching). Nothing pinned them, so a refactor could silently re-introduce
 * O(rows x columns) per-row allocation with no test noticing. This file makes
 * the per-row JS-heap footprint a ratchet.
 *
 * SCOPE OF THIS TEST (the FFI-call budget lives elsewhere): the #1448
 * constructor-lookup counter is Rust-`#[cfg(test)]` only and is NOT exposed to
 * JS, so its "<= 1 lookup per Set/Map cache per result" budget is asserted in a
 * Rust unit test (bindings/node/src/value.rs ::set_map_ctor_lookups_bounded_per_result).
 * This JS test owns the complementary per-row heap-delta budget: it materializes
 * a wide result and bounds the JS heap growth per row.
 *
 * MEASUREMENT: requires `--expose-gc` (package.json runs jest via
 * `node --expose-gc ...`). We FAIL LOUDLY (never skip) if `global.gc` is absent
 * or datasets are missing/empty — a silent skip would let a regression pass.
 * `process.memoryUsage().heapUsed` is coarse and jittery, so we take the MEDIAN
 * of several measured passes (each after a double gc) rather than a single noisy
 * sample, and set the budget with generous headroom over the observed spread.
 */
const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable } = require('./helpers.js');

const DIR = global.testPaths.SSTABLES_DIR;
const SCHEMA = global.testPaths.SCHEMA_WIDE_ROWS;
// Widest fixture schema (~101 declared columns), consistent with the Python
// per-row alloc budget test. Non-null cells only are materialized per row.
const QUERY = 'SELECT * FROM test_wide_rows.many_columns_table LIMIT 200';
const MEASURE_PASSES = 7;

// MEASUREMENT WINDOW (issue #1449 roborev fix): each pass now wraps
// `executeNative(QUERY)` ITSELF inside the gc'd heap-delta window (gc x2, sample
// heapUsed, executeNative + touch all rows, sample heapUsed again). The earlier
// window sampled only `rows.map(r => ({...r}))` — a shallow spread of rows that
// executeNative had ALREADY materialized — so the per-row conversion allocation
// (the JS value graph built in `resolve()`/`row_to_object`) landed OUTSIDE the
// ratchet. With executeNative inside, the delta captures the actual per-row
// converted-value footprint.
//
// MEASURED BASELINE (2026-07-04, macOS arm64, release-unwind .node):
//   executeNative(QUERY) -> 50 rows, ~8 non-null columns each.
//   Median-of-7 (as this test computes it) stabilized at 1295.2 bytes/row once
//   V8 warmed (a cold experiment measured 1357.6). Single-sample spread over 12
//   warm samples was min=1295.2 / max=1534.9.
// Budget = 2000 bytes/row: baseline measured on macOS arm64 release-unwind
// (~1295 warm median, up to ~1535 single-sample). `process.memoryUsage().heapUsed`
// deltas are platform/build-profile/GC-timing dependent, so 2000 leaves headroom
// for Linux x64 / debug-build / GC-timing drift (heapUsed deltas are noisier than
// Python's tracemalloc, which is why we assert the MEDIAN pass, not a worst-case
// single sample) while staying under the ~2086 doubling-regression signal
// (documented as the biting threshold): a synthetic value-graph-doubling
// regression (converting + retaining each cell's value twice) measured a median
// of 2086.1 bytes/row (min 2047.5), so the ratchet still bites a genuine
// O(rows x columns) regression. Pinned to a measured number per the issue mandate.
//
// WHAT THIS BUDGET DOES AND DOES NOT PIN (issue #1449, measured — honest scope):
// The V8 `heapUsed` delta pins the GROSS per-row JS value-graph footprint, so it
// catches an O(rows x columns) duplication/boxing of converted cell values
// (proven above: the doubling regression trips it). It CANNOT observe two of the
// W1-W4 wins from the binding layer, both confirmed by measurement here:
//   * #1447 (clone -> move of row values in executeNative's `compute()`): the
//     clone is a RUST-heap allocation, dropped before the post-execute V8 sample,
//     and never touches the V8 heap — reverting it moved the median 1335.0 ->
//     1333.8 (noise).
//   * #1445/#1446 (column-key interning): V8 internally dedups property-name
//     strings, so emitting a fresh key string per cell instead of the interned
//     handle moved the median 1335.0 -> 1333.8 (noise) at this table's scale.
// Those two wins are pinned elsewhere: #1448 by the Rust `#[cfg(test)]`
// `set_map_ctor_lookups_bounded_per_result` counter (bindings/node/src/value.rs);
// #1445/#1446/#1447 would need Rust-side allocation counters to ratchet directly
// (none exists yet) — this heap test does not overclaim them.
const BUDGET_BYTES_PER_ROW = 2000;

function median(nums) {
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

describe('conversion per-row heap budget (issue #1449)', () => {
  let db;

  beforeAll(async () => {
    assertDatasetsAvailable();
    // Budget measurement is meaningless without gc control; FAIL, do not skip.
    if (typeof global.gc !== 'function') {
      throw new Error(
        'global.gc is unavailable — run jest via `node --expose-gc ' +
          './node_modules/jest/bin/jest.js` (see package.json "test" script)'
      );
    }
    db = await Database.open(DIR, { schema: SCHEMA });
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  test('per-row JS heap stays under the measured budget', async () => {
    const perRowSamples = [];
    let rowCount = 0;

    for (let pass = 0; pass < MEASURE_PASSES; pass += 1) {
      // Measure the CONVERSION PATH: executeNative() is INSIDE the window
      // (issue #1449 roborev fix) so the per-row value-graph allocation it
      // builds is captured, not just a shallow spread of already-materialized
      // rows.
      global.gc();
      global.gc();
      const before = process.memoryUsage().heapUsed;
      const result = await db.executeNative(QUERY);
      // Touch every row so V8 cannot elide the materialized graph before the
      // post-execute sample.
      const materialized = result.rows.map((r) => ({ ...r }));
      const after = process.memoryUsage().heapUsed;

      rowCount = result.rowCount;
      // FAIL LOUDLY (never skip) on a present-but-empty/unreadable dataset — a
      // zero-row result would divide-by-zero and false-green the ratchet.
      expect(rowCount).toBeGreaterThan(0);
      // Keep the materialized data alive across the sample so nothing is freed
      // before `after` is read.
      expect(materialized.length).toBe(rowCount);

      perRowSamples.push((after - before) / rowCount);
    }

    const perRow = median(perRowSamples);
    expect(perRow).toBeLessThan(BUDGET_BYTES_PER_ROW);
  });
});
