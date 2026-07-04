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

const DIR = global.testPaths.SSTABLES_DIR;
const SCHEMA = global.testPaths.SCHEMA_WIDE_ROWS;
// Widest fixture schema (~101 declared columns), consistent with the Python
// per-row alloc budget test. Non-null cells only are materialized per row.
const QUERY = 'SELECT * FROM test_wide_rows.many_columns_table LIMIT 200';
const MEASURE_PASSES = 7;

// MEASURED BASELINE (2026-07-04, macOS arm64, release-unwind .node):
//   executeNative(QUERY) -> 50 rows, ~8 non-null columns each.
//   Per pass: double gc, read heapUsed, materialize `rows.map(r => ({...r}))`,
//   read heapUsed again; per_row = delta / rowCount.
//   Median-of-7 (as this test computes it) settled at 151.2 bytes/row once V8
//   warmed; a cold first pass measured ~214.6. Single-sample spread over 12
//   runs was min=151.2 / max=221.0.
// Budget = 350 bytes/row: comfortably above the warm 151 and the cold ~215
// medians (~1.6x-2.3x headroom, so it does not flake on V8 GC / hidden-class
// churn — heapUsed deltas are noisier than Python's tracemalloc, which is why we
// assert the MEDIAN pass, not a worst-case single sample), yet well below a
// regression that inflates per-row storage: a synthetic ~2x-properties regression
// measured 1222 bytes/row (see the "Prove the budget bites" note in the issue
// #1449 delivery summary), an order of magnitude over the budget. Pinned to a
// measured number per the issue mandate, never a guess.
const BUDGET_BYTES_PER_ROW = 350;

function median(nums) {
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

describe('conversion per-row heap budget (issue #1449)', () => {
  let db;

  beforeAll(async () => {
    if (!global.DATASETS_AVAILABLE) {
      throw new Error(
        'Test data not available. Set CQLITE_DATASETS_ROOT or run fetch-datasets.sh'
      );
    }
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
      const result = await db.executeNative(QUERY);
      rowCount = result.rowCount;
      // FAIL LOUDLY (never skip) on a present-but-empty/unreadable dataset — a
      // zero-row result would divide-by-zero and false-green the ratchet.
      expect(rowCount).toBeGreaterThan(0);

      global.gc();
      global.gc();
      const before = process.memoryUsage().heapUsed;
      const materialized = result.rows.map((r) => ({ ...r }));
      const after = process.memoryUsage().heapUsed;
      // Keep the materialized data alive across the sample so nothing is freed
      // before `after` is read.
      expect(materialized.length).toBe(rowCount);

      perRowSamples.push((after - before) / rowCount);
    }

    const perRow = median(perRowSamples);
    expect(perRow).toBeLessThan(BUDGET_BYTES_PER_ROW);
  });
});
