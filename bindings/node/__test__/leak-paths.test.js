/**
 * Exception-path and abandoned-iterator LEAK BUDGET tests (issue #1465, parent #1436).
 *
 * Error paths are where leaks hide. When a query rejects, or a streaming
 * iterator is abandoned partway through, the native (Rust) side may have
 * allocated buffers, channel state, or JS objects that never get freed -- and no
 * test noticed steady growth across repeated failures. A long-running Node
 * server hitting errors in a loop would slowly bloat. This file puts a BUDGET on
 * exactly those paths, mirroring the Python tracemalloc budgets in
 * bindings/python/tests/test_leak_paths.py.
 *
 * WHY THE LOAD-BEARING GUARD IS A MEASURED HEAP BUDGET, NOT jest `--detectLeaks`
 * (issue #1465 explicitly allows this fallback and requires the reason to be
 * documented here). `detectLeaks` IS enabled for this file — scoped to it by the
 * `leaks` project in jest.config.js, never globally — and it passes. But it
 * cannot carry the guarantee this issue asks for, for two measured reasons:
 *   1. It answers a DIFFERENT question. jest-leak-detector registers a
 *      FinalizationRegistry on the jest `TestEnvironment` INSTANCE
 *      (jest-runner/build/runTest.js: `new LeakDetector(environment)`), i.e. it
 *      asks "was the whole test environment collected after this FILE finished",
 *      not "does each iteration of this loop retain memory" — which is the
 *      property a leaking error path exhibits.
 *   2. Its liveness here is UNDEMONSTRABLE from test code. The watched value is
 *      the environment instance, which a test cannot reach, so no planted-leak
 *      control can prove the detector would fire. Measured 2026-08-30 (jest
 *      29.7.0, Node v20.20.2): two planted retentions — a required module graph
 *      and the test-environment `global` itself, both pinned on `process` so they
 *      outlive the file — were BOTH reported clean. So a green `detectLeaks` is
 *      not evidence of anything, and this file does not treat it as such; it is
 *      left enabled only because it is stable and costs nothing.
 * `detectOpenHandles: true` is also enabled for this file: an abandoned iterator
 * whose `return()`/`close()` never ran would leave a handle behind, which is a
 * signal that DOES apply to the path under test.
 *
 * WHAT IS ASSERTED (and what is deliberately NOT): the growth of
 * `heapUsed + external` across N iterations must stay under a documented budget.
 * Growth is NEVER asserted to be zero -- V8/GC noise, one-time caches and
 * allocator behaviour make a zero assertion flaky by construction. `external` is
 * summed in ALONGSIDE `heapUsed` because a leaked `Buffer`/native-backed
 * allocation lives OFF the V8 heap and is invisible to `heapUsed` alone
 * (measured: a retained 256-byte Buffer per iteration moved `heapUsed` by ~0 and
 * `heapUsed + external` by ~450 bytes/iteration).
 *
 * NON-VACUITY IS ASSERTED EXPLICITLY (the most likely defect in a budget test):
 * a loop body that silently no-ops -- a "bad" CQL string that resolves instead
 * of rejecting, or a streaming query that yields 0 rows -- would make the budget
 * trivially pass while testing nothing. So every iteration is counted and the
 * counts are asserted, and two separate contract tests pin the shapes: the bad
 * query really rejects, and the broken-out-of stream really was mid-stream and
 * really was closed by `return()`.
 *
 * HOW TO RUN (this lane needs `global.gc`, exactly like conversion-budget.test.js):
 *   cd bindings/node && npm run build && npm run test:leaks
 * A bare `npx jest leak-paths` has no `--expose-gc` and FAILS LOUDLY in
 * `beforeAll` rather than silently measuring GC-deferred garbage.
 *
 * There is deliberately NO wall-clock/elapsed-time assertion in this file: these
 * are MEMORY budgets. A timing threshold in a correctness test is a known flake
 * class (#2642).
 */
const { Database } = require('../lib/index.js');

const DIR = global.testPaths.SSTABLES_DIR;
const SCHEMA = global.testPaths.SCHEMA_WIDE_ROWS;

// Rejected at query-planning time -> QueryError. Chosen deliberately over a
// nonexistent-table SELECT, which resolves with 0 rows WITHOUT rejecting
// (measured 2026-08-30) and would make the error-path loop a silent no-op.
const BAD_CQL = 'THIS IS NOT VALID CQL';

// Widest fixture in the corpus (~101 declared columns, 50 rows), the same table
// the conversion-budget ratchet uses. A wide row means an abandoned stream has
// really built and dropped a non-trivial per-row value graph, so a leak of that
// graph is visible rather than lost in noise.
const STREAM_QUERY = 'SELECT * FROM test_wide_rows.many_columns_table';

const ITERATIONS = 300;
// Warm-up iterations run BEFORE any sample so one-time allocations (V8 code
// caches, first-touch native buffers, the streaming machinery's one-time setup)
// are not counted as growth.
const WARMUP = 20;
// Rows pulled before breaking. Must be < the fixture's row count (50, pinned by
// the contract test below) so the iterator is genuinely abandoned mid-stream.
const STREAM_ROWS = 5;
// `heapUsed`/`external` deltas are far jitterier than Python's tracemalloc:
// individual passes over the SAME clean loop swung from -133 KB to +349 KB (V8
// growing its heap, or collecting an earlier pass's garbage inside a later one).
// So each budget is measured over several passes and asserted on the MINIMUM
// pass -- not the median, and not a single sample.
//
// WHY THE MINIMUM (measured, not aesthetic): a genuine per-iteration leak raises
// EVERY pass -- for all three synthetic leak shapes below, all 9 passes were
// elevated and the MINIMUM pass still sat 1.5x-5.5x above the budget -- whereas
// GC jitter perturbs individual passes in both directions. The minimum therefore
// needs only ONE quiet pass out of MEASURE_PASSES to read fairly, while a median
// needs a majority: a median-based budget was tried first and flaked (1 red in
// 10 runs; medians 616..3200 bytes, with occasional 150-296 KB passes).
// The cost, stated honestly: a leak smaller than the single-pass GC-deferral
// amplitude (~130 KB on the error path) could in principle hide in the minimum.
// Measured, none of the three realistic shapes below does.
const MEASURE_PASSES = 9;

// ---------------------------------------------------------------------------
// BUDGET (issue #1465) -- MEASURED, never guessed. Linux x64, Node v20.20.2,
// release-unwind .node, CQLITE_DATASETS_ROOT=/data/datasets, 300 iterations x 9
// passes; the numbers below are the MINIMUM pass (the asserted statistic), over
// several consecutive runs (2026-08-30):
//   error path:  -133,504 .. +2,496 bytes (a negative minimum means a later pass
//                collected an earlier pass's deferred garbage)
//   stream path:     +424 .. +1,864 bytes (1.4-6.2 bytes/iteration)
// Budget = 32 KiB (109 bytes/iteration at 300 iterations): ~17x the largest clean
// minimum observed on either path, so GC/platform drift cannot red it. Measured
// discrimination, with synthetic leaks injected into these same loop bodies
// (again as the minimum pass):
//   * retain ONE wide row per iteration (~620 B/iter): 171-182 KB -> TRIPS (5.5x)
//   * retain a 256-byte Buffer per iteration:          121-132 KB -> TRIPS (4.0x)
//   * retain a 64-byte Buffer per iteration:            50-80 KB  -> TRIPS (1.5x)
// Those are the realistic shapes of a JS-side or native-buffer leak on these
// paths; the 64-byte case is the smallest leak this test is claimed to catch.
// ---------------------------------------------------------------------------
const BUDGET_BYTES = 32 * 1024;

// Per-test timeout for the two multi-pass budgets (measured ~0.5s and ~4.5s on
// this machine). Declared here rather than as a project-level `testTimeout`,
// which trips a jest 29 config-validation warning.
const BUDGET_TEST_TIMEOUT_MS = 120_000;

/**
 * Assert the measured growth is under the budget, with every per-pass sample in
 * the failure message (jest's own `toBeLessThan` output would show only the
 * single asserted number, and the spread is what tells a real leak from a GC
 * artefact).
 */
function assertUnderBudget(label, growth, samples) {
  if (growth >= BUDGET_BYTES) {
    throw new Error(
      `${label}: tracked memory (heapUsed+external) grew by at least ${growth} ` +
        `bytes in EVERY one of ${MEASURE_PASSES} passes of ${ITERATIONS} ` +
        `iterations (${(growth / ITERATIONS).toFixed(1)} bytes/iteration), ` +
        `exceeding the ${BUDGET_BYTES}-byte budget. ` +
        `Per-pass samples=[${samples.join(', ')}] (issue #1465)`
    );
  }
  expect(growth).toBeLessThan(BUDGET_BYTES);
}

/** Total tracked bytes: V8 heap PLUS off-heap (Buffer/native) allocations. */
function trackedBytes() {
  const usage = process.memoryUsage();
  return usage.heapUsed + usage.external;
}

/** Drive GC to a quiet point: two collections, a macrotask turn, one more. */
async function settle() {
  global.gc();
  global.gc();
  // Let pending microtasks/`setImmediate` callbacks (streaming completions) run
  // so nothing they hold is still reachable when the sample is taken.
  await new Promise((resolve) => setImmediate(resolve));
  global.gc();
}

/**
 * Run `body` WARMUP times, then measure MEASURE_PASSES x ITERATIONS of it.
 *
 * `body(counters)` is responsible for its own non-vacuity counting; this helper
 * only measures. Returns the MINIMUM per-pass growth in tracked bytes (the
 * statistic the budget is asserted on) plus every per-pass sample.
 */
async function measureGrowth(body, counters) {
  for (let i = 0; i < WARMUP; i += 1) {
    await body(counters);
  }

  const samples = [];
  for (let pass = 0; pass < MEASURE_PASSES; pass += 1) {
    await settle();
    const before = trackedBytes();
    for (let i = 0; i < ITERATIONS; i += 1) {
      await body(counters);
    }
    await settle();
    samples.push(trackedBytes() - before);
  }
  // The MINIMUM pass is the asserted statistic -- see MEASURE_PASSES above.
  return { growth: Math.min(...samples), samples };
}

describe('exception-path / abandoned-iterator leak budgets (issue #1465)', () => {
  let db;

  beforeAll(async () => {
    // FAIL LOUDLY, never skip: a missing/empty corpus must red this lane, since
    // a skipped leak budget is indistinguishable from a passing one.
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

  // -------------------------------------------------------------------------
  // Contract pins: the loop bodies below are only meaningful if these hold.
  // -------------------------------------------------------------------------

  test('executeNative rejects on bad CQL (the error path really is an error path)', async () => {
    // The literal shape the issue asks for, run OUTSIDE the measurement window:
    // jest's `expect(...).rejects` machinery allocates matcher state per call,
    // which would be measured noise inside the budgeted loop.
    for (let i = 0; i < 3; i += 1) {
      await expect(db.executeNative(BAD_CQL)).rejects.toThrow();
    }
  });

  test('breaking out of a stream abandons it mid-stream and runs return() -> close()', async () => {
    // The fixture must hold MORE than STREAM_ROWS rows, or "abandoned
    // mid-stream" would silently mean "exhausted".
    let total = 0;
    for await (const row of db.executeStreaming(STREAM_QUERY)) {
      expect(row).toBeDefined();
      total += 1;
    }
    expect(total).toBeGreaterThan(STREAM_ROWS);

    const stream = db.executeStreaming(STREAM_QUERY);
    let pulled = 0;
    for await (const row of stream) {
      expect(row).toBeDefined();
      pulled += 1;
      if (pulled >= STREAM_ROWS) break; // -> iterator.return() -> close()
    }
    expect(pulled).toBe(STREAM_ROWS);

    // Observable proof that `return()` ran and closed the stream: a stream that
    // was NOT closed would keep yielding the remaining rows here.
    let afterBreak = 0;
    for await (const row of stream) {
      expect(row).toBeDefined();
      afterBreak += 1;
    }
    expect(afterBreak).toBe(0);
  });

  // -------------------------------------------------------------------------
  // The budgets.
  // -------------------------------------------------------------------------

  test('repeated query rejections stay under the leak budget', async () => {
    const counters = { rejected: 0, resolved: 0 };
    const { growth, samples } = await measureGrowth(async (c) => {
      try {
        await db.executeNative(BAD_CQL);
        c.resolved += 1;
      } catch (err) {
        c.rejected += 1;
      }
    }, counters);

    // NON-VACUITY: every iteration must have rejected. If BAD_CQL ever stops
    // rejecting, this loop degenerates into a no-op and the budget would pass
    // while measuring nothing.
    const expected = WARMUP + MEASURE_PASSES * ITERATIONS;
    expect(counters.resolved).toBe(0);
    expect(counters.rejected).toBe(expected);

    // BOUNDED, not zero (see file header).
    assertUnderBudget('error path (repeated rejections)', growth, samples);
  }, BUDGET_TEST_TIMEOUT_MS);

  test('abandoned streaming iterators stay under the leak budget', async () => {
    const counters = { rows: 0, iterators: 0 };
    const { growth, samples } = await measureGrowth(async (c) => {
      let pulled = 0;
      for await (const row of db.executeStreaming(STREAM_QUERY)) {
        pulled += 1;
        if (pulled >= STREAM_ROWS) break; // abandoned: NOT exhausted
      }
      c.rows += pulled;
      c.iterators += 1;
    }, counters);

    // NON-VACUITY: a 0-row (or short) stream would make the abandonment a
    // no-op. This is also the FAIL-LOUDLY check for a present-but-unreadable
    // corpus — it fails, it never skips.
    const expectedIterators = WARMUP + MEASURE_PASSES * ITERATIONS;
    expect(counters.iterators).toBe(expectedIterators);
    expect(counters.rows).toBe(STREAM_ROWS * expectedIterators);

    // BOUNDED, not zero (see file header).
    assertUnderBudget('abandoned streaming iterators', growth, samples);
  }, BUDGET_TEST_TIMEOUT_MS);
});
