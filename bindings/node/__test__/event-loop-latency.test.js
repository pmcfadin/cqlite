/**
 * Event-loop latency regression guard for executeNative (issue #1442).
 *
 * executeNative scans OFF the event loop, but every result row is materialized
 * into a JS object on the event-loop thread in `resolve()` (a napi `Env` is
 * thread-bound, so this work CANNOT be moved off-loop). That per-call build is
 * therefore O(rows-in-that-call) of synchronous on-loop work. The fix BOUNDS
 * on-loop work — it does not move it off-loop:
 *
 *   (a) a documented cap (`CQLITE_NODE_MAX_NATIVE_ROWS`, default 100_000)
 *       rejects an oversized single result with a typed error steering the
 *       caller to executeStreaming (verified below), and
 *   (b) callers are documented to prefer executeStreaming for large sets.
 *
 * What the first test actually measures: the shipped fixtures cap a SINGLE-CALL
 * result at ~100 rows, so a single executeNative can never do an O(total)
 * on-loop freeze here. Instead we fire MANY bounded native calls (accumulating
 * >= 50,000 materialized rows total) while a 10ms timer runs, and assert the
 * timer never stalls beyond MAX_GAP_CEILING_MS (issue #4538: a generous regression TRIPWIRE, not a
 * latency SLO). This validates that per-call
 * materialization stays bounded and interleaves cleanly with the event loop —
 * i.e. the loop of many small calls exercises bounded per-call interleaving
 * with the timer. It is NOT a single-call O(total) freeze test (the fixtures
 * cannot produce one); the guard-rejection behavior for a genuinely oversized
 * single result is covered separately by the oversized-rejection test below.
 * The bound is still meaningful: if a change made per-call materialization
 * super-linear or removed the interleaving, the accumulated on-loop time would
 * push the max timer gap past the ceiling.
 *
 * Per the Node test doctrine (no strict-fixtures flag): this test THROWS
 * (does not skip) if data is missing or a query yields 0 rows when rows are
 * expected — a silent skip would let a real regression pass unnoticed.
 */
const path = require('path');
const { spawnSync } = require('child_process');
const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable } = require('./helpers.js');

const ADDON_ENTRY = path.resolve(__dirname, '..', 'lib', 'index.js');
const DIR = global.testPaths.SSTABLES_DIR;
const SCHEMA = global.testPaths.SCHEMA_WIDE_ROWS;
// A wide table with the most rows available in the fixture set.
const QUERY = 'SELECT * FROM test_wide_rows.wide_partition_table';
const TARGET_ROWS = 50000;
// Merge-path TRIPWIRE only (issue #4538, test-oracles lesson 3). A tight latency bound is a wall-clock
// assertion and flakes on a loaded host (a 10ms timer drifts by hundreds of ms under a 150-load box),
// and a ratio-to-run bound is just as load-sensitive. So the ONLY assertion is a deliberately generous
// absolute ceiling: an unbounded O(total) on-loop materialization burst freezes the loop for many
// seconds on this fixture, which is the regression this guards; ordinary scheduler jitter, GC and
// concurrent builds stay orders of magnitude below it. It is NOT a latency SLO — fine-grained latency
// belongs in a perf lane, not on the merge path.
const MAX_GAP_CEILING_MS = 5000;
const TIMER_INTERVAL_MS = 10;
// Keep the child process below jest's 30s worker/test default so a hung child
// fails fast (with diagnostics) instead of blocking the worker forever.
const CHILD_TIMEOUT_MS = 20000;

describe('executeNative event-loop latency (issue #1442)', () => {
  let db;

  beforeAll(async () => {
    assertDatasetsAvailable();
    db = await Database.open(DIR, { schema: SCHEMA });
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  test(`materializing >= 50k rows keeps the event loop responsive (max timer gap < ${MAX_GAP_CEILING_MS}ms)`, async () => {
    // Establish per-call row count and fail loudly if the source is empty.
    const probe = await db.executeNative(QUERY);
    if (probe.rowCount === 0) {
      throw new Error(
        `Expected rows from ${QUERY} but got 0 — fixture data is missing or empty.`
      );
    }
    const perCall = probe.rowCount;
    const iterations = Math.ceil(TARGET_ROWS / perCall);

    const gaps = [];
    let last = performance.now();
    const timer = setInterval(() => {
      const now = performance.now();
      gaps.push(now - last);
      last = now;
    }, TIMER_INTERVAL_MS);

    let processed = 0;
    try {
      // Fire calls in concurrent batches so their resolve() materializations
      // queue back-to-back on the event loop (the realistic burst) while the
      // 10ms timer competes for the loop.
      const batch = 25;
      for (let done = 0; done < iterations; done += batch) {
        const n = Math.min(batch, iterations - done);
        const results = await Promise.all(
          Array.from({ length: n }, () => db.executeNative(QUERY))
        );
        for (const r of results) {
          if (r.rowCount === 0) {
            throw new Error('executeNative unexpectedly returned 0 rows mid-run.');
          }
          processed += r.rowCount;
        }
      }
      // Workload endpoint: record the FINAL gap and stop the timer in the same synchronous step, so
      // a stall at the very end (a tick that never got to fire) is measured against this boundary
      // and is not lost.
      gaps.push(performance.now() - last);
    } finally {
      clearInterval(timer);
    }

    expect(processed).toBeGreaterThanOrEqual(TARGET_ROWS);
    // At least two gap samples must exist (never silently skip).
    if (gaps.length < 2) {
      throw new Error(
        `Expected at least 2 timer gaps to assess responsiveness but recorded ${gaps.length}.`
      );
    }
    // The event loop must not have frozen: the largest gap (including the final one recorded at the
    // workload endpoint) stays under the generous tripwire ceiling.
    const maxGap = Math.max(...gaps);
    // eslint-disable-next-line no-console
    console.log(`event-loop max gap ${maxGap.toFixed(1)}ms (ceiling ${MAX_GAP_CEILING_MS}ms)`);
    expect(maxGap).toBeLessThan(MAX_GAP_CEILING_MS);
  });

  test('oversized executeNative rejects with a typed executeStreaming error, not a freeze', () => {
    // The cap defaults to 100_000 (generous, so normal queries are unaffected),
    // which no shipped single-call fixture reaches. To exercise the guard we
    // lower it via the documented CQLITE_NODE_MAX_NATIVE_ROWS override. That
    // override is read from the OS environment on the JS thread; jest's `node`
    // test environment sandboxes `process.env` writes so they never reach the
    // OS env the native addon reads, so we run the assertion in a child `node`
    // process where the real environment is honored.
    const script = `
      const { Database } = require(${JSON.stringify(ADDON_ENTRY)});
      (async () => {
        const db = await Database.open(${JSON.stringify(DIR)}, { schema: ${JSON.stringify(SCHEMA)} });
        let code = 3;
        try {
          await db.executeNative(${JSON.stringify(QUERY)});
          code = 2; // guard did NOT fire (unexpected)
        } catch (e) {
          const msg = (e && e.message ? e.message : '').split(String.fromCharCode(0))[0];
          process.stdout.write(msg);
          code = /executeStreaming/.test(msg) ? 0 : 5;
        }
        await db.close();
        process.exit(code);
      })().catch((e) => { process.stderr.write(String(e)); process.exit(4); });
    `;
    const res = spawnSync(process.execPath, ['-e', script], {
      encoding: 'utf8',
      // Bound the child below jest's default so a hang in the freeze-detection
      // guard itself fails fast with diagnostics rather than blocking forever.
      timeout: CHILD_TIMEOUT_MS,
      env: {
        ...process.env,
        CQLITE_NODE_MAX_NATIVE_ROWS: '10',
        CQLITE_DATASETS_ROOT: global.testPaths.TEST_DATA_ROOT,
      },
    });
    // Surface child failure/hang details so a timeout, crash, or signal fails
    // with actionable diagnostics instead of a bare status mismatch. (Jest 29
    // ignores expect()'s 2nd-arg message, so we throw explicitly.)
    const diag =
      `child status=${res.status} signal=${res.signal} error=${res.error}\n` +
      `stdout: ${res.stdout}\nstderr: ${res.stderr}`;
    if (res.error || res.signal !== null || res.status !== 0) {
      throw new Error(`oversized-rejection child did not exit cleanly:\n${diag}`);
    }
    // status 0 => rejected with the typed executeStreaming error.
    expect(res.status).toBe(0);
    expect(res.stdout).toMatch(/executeStreaming/);
  });

  test('a normal-sized executeNative is unaffected by the generous default cap', async () => {
    // With the default 100_000 cap, the same query returns rows (no freeze,
    // no rejection) — the guard only trips on genuinely oversized sets.
    const ok = await db.executeNative(QUERY);
    expect(ok.rowCount).toBeGreaterThan(0);
    expect(ok.rowCount).toBeLessThan(100000);
  });
});
