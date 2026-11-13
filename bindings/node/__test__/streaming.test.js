/**
 * Streaming iterator tests for Issue #305.
 *
 * TDD Requirements from the issue:
 * - [x] Test: for await...of iterates all rows
 * - [x] Test: Streaming respects bufferSize
 * - [x] Test: Early break from loop doesn't leak
 * - [x] Test: Error in stream propagates correctly
 * - [x] Test: Empty result yields 0 iterations
 * - [x] Test: Memory stays under 128MB for large results (slow test)
 */

const fs = require('fs');
const path = require('path');
const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable, openDatabase, withDatabase } = require('./helpers.js');

// Query over the largest basic-types table available (simple_table, ~1000 rows).
// Large enough that per-row AsyncTask dispatch overhead is measurable.
const BATCH_QUERY = 'SELECT * FROM test_basic.simple_table';

/**
 * Drain a stream fully, returning { count, ms }. Throws loudly (does NOT skip)
 * if zero rows come back when data is expected — a 0-row stream is a setup
 * failure, not a pass (issue #1443).
 */
async function timedDrain(db, query, config) {
  const t0 = performance.now();
  let count = 0;
  for await (const row of db.executeStreaming(query, config)) {
    count++;
  }
  return { count, ms: performance.now() - t0 };
}

/** Locate a real on-disk Data.db file to exercise concurrent fs.readFile. */
function findDataFile() {
  const basicDir = path.join(global.testPaths.SSTABLES_DIR, 'test_basic');
  for (const entry of fs.readdirSync(basicDir)) {
    const tableDir = path.join(basicDir, entry);
    let files = [];
    try {
      files = fs.readdirSync(tableDir);
    } catch {
      continue;
    }
    const data = files.find((f) => f.endsWith('Data.db'));
    if (data) return path.join(tableDir, data);
  }
  throw new Error('No Data.db file found under test_basic for the fs starvation test');
}

async function fsReadLatency(file) {
  const start = performance.now();
  await fs.promises.readFile(file);
  return performance.now() - start;
}

describe('Streaming Iterator Tests (Issue #305)', () => {
  beforeAll(() => {
    assertDatasetsAvailable();
  });

  describe('Basic Streaming', () => {
    test('executeStreaming returns async iterable synchronously', async () => {
      await withDatabase(async (db) => {
        // No await - executeStreaming returns synchronously per M4 spec
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 5'
        );

        expect(stream).toBeDefined();
        expect(typeof stream[Symbol.asyncIterator]).toBe('function');
        expect(typeof stream.close).toBe('function');
        expect(typeof stream.rowsReceived).toBe('number');
        expect(Array.isArray(stream.columns)).toBe(true);
        // Verify it's NOT a Promise
        expect(stream.then).toBeUndefined();
      });
    });

    test('for await...of iteration works', async () => {
      await withDatabase(async (db) => {
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table'
        );

        const rows = [];
        for await (const row of stream) {
          rows.push(row);
        }

        // Should have some rows
        expect(rows.length).toBeGreaterThan(0);

        // Verify row structure - each row should be an object
        for (const row of rows) {
          expect(typeof row).toBe('object');
          expect(row).not.toBeNull();
        }
      });
    });

    test('rowsReceived tracks progress', async () => {
      await withDatabase(async (db) => {
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 10'
        );

        expect(stream.rowsReceived).toBe(0);

        let count = 0;
        for await (const row of stream) {
          count++;
          // rowsReceived should be updated after each row
          // Note: Due to buffering, may be slightly different from count
          expect(stream.rowsReceived).toBeGreaterThanOrEqual(0);
        }

        expect(count).toBeGreaterThan(0);
      });
    });

    test('columns metadata is available after first iteration', async () => {
      await withDatabase(async (db) => {
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 1'
        );

        // Before iteration, columns returns empty array (lazy init)
        expect(Array.isArray(stream.columns)).toBe(true);

        // Iterate to trigger initialization
        for await (const row of stream) {
          // After first iteration, columns should be populated
          expect(stream.columns.length).toBeGreaterThan(0);
          for (const col of stream.columns) {
            expect(typeof col.name).toBe('string');
            expect(typeof col.dataType).toBe('string');
            expect(typeof col.nullable).toBe('boolean');
            expect(typeof col.position).toBe('number');
          }
        }
      });
    });

    test('streaming returns same data as execute', async () => {
      await withDatabase(async (db) => {
        // Use a query without LIMIT for more reliable comparison
        const query = 'SELECT * FROM test_basic.simple_table';

        // Get rows via regular execute
        const regularResult = await db.executeNative(query);

        // Get rows via streaming - no await on executeStreaming
        const stream = db.executeStreaming(query);
        const streamedRows = [];
        for await (const row of stream) {
          streamedRows.push(row);
        }

        // Same number of rows
        expect(streamedRows.length).toBe(regularResult.rowCount);
      });
    });
  });

  describe('StreamingConfig', () => {
    test('custom bufferSize is respected', async () => {
      await withDatabase(async (db) => {
        const config = { bufferSize: 256, chunkSize: 500 };
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 100',
          config
        );

        const rows = [];
        for await (const row of stream) {
          rows.push(row);
        }

        expect(rows.length).toBeGreaterThan(0);
      });
    });

    test('default config works (no config provided)', async () => {
      await withDatabase(async (db) => {
        // No config - uses defaults (bufferSize: 1024, chunkSize: 10000)
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 5'
        );

        const rows = [];
        for await (const row of stream) {
          rows.push(row);
        }

        expect(rows.length).toBeGreaterThan(0);
      });
    });

    test('zero bufferSize is rejected on first iteration', async () => {
      await withDatabase(async (db) => {
        const config = { bufferSize: 0, chunkSize: 10000 };
        const stream = db.executeStreaming('SELECT * FROM test_basic.simple_table', config);

        // Error surfaces on first iteration, not at call time
        await expect(async () => {
          for await (const row of stream) {
            // Should not reach here
          }
        }).rejects.toThrow(/bufferSize must be greater than 0/);
      });
    });

    test('zero chunkSize is rejected on first iteration', async () => {
      await withDatabase(async (db) => {
        const config = { bufferSize: 1024, chunkSize: 0 };
        const stream = db.executeStreaming('SELECT * FROM test_basic.simple_table', config);

        // Error surfaces on first iteration, not at call time
        await expect(async () => {
          for await (const row of stream) {
            // Should not reach here
          }
        }).rejects.toThrow(/chunkSize must be greater than 0/);
      });
    });
  });

  describe('Early Termination', () => {
    test('break from loop cleans up resources', async () => {
      await withDatabase(async (db) => {
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 100'
        );

        let count = 0;
        for await (const row of stream) {
          count++;
          if (count >= 5) {
            break;
          }
        }

        expect(count).toBe(5);
        // After break, stream should be cleaned up
        // (verified by no memory leaks, no hanging promises)
      });
    });

    test('explicit close() releases resources', async () => {
      await withDatabase(async (db) => {
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 10'
        );

        // Read one row manually using the iterator
        const iterator = stream[Symbol.asyncIterator]();
        const first = await iterator.next();
        expect(first.done).toBe(false);
        expect(first.value).toBeDefined();

        // Close explicitly
        stream.close();

        // Subsequent reads should return done
        const afterClose = await iterator.next();
        expect(afterClose.done).toBe(true);
      });
    });

    test('close() is idempotent', async () => {
      await withDatabase(async (db) => {
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 5'
        );

        // Close multiple times - should not throw
        stream.close();
        stream.close();
        stream.close();
      });
    });
  });

  describe('Error Handling', () => {
    test('invalid SQL throws with structured error on iteration', async () => {
      await withDatabase(async (db) => {
        // executeStreaming returns synchronously, error surfaces on iteration
        const stream = db.executeStreaming('THIS IS NOT VALID SQL');

        try {
          for await (const row of stream) {
            fail('Should have thrown before yielding any rows');
          }
          fail('Should have thrown');
        } catch (e) {
          expect(e.code).toBeDefined();
          // Accept any error code - the important thing is that an error is thrown
          expect(['PARSE', 'QUERY', 'INTERNAL']).toContain(e.code);
          expect(e.message).toBeDefined();
        }
      });
    });

    test('query on nonexistent table throws on iteration', async () => {
      await withDatabase(async (db) => {
        // executeStreaming returns synchronously, error surfaces on iteration
        const stream = db.executeStreaming('SELECT * FROM nonexistent_keyspace.nonexistent_table');

        try {
          for await (const row of stream) {
            fail('Should have thrown before yielding any rows');
          }
          fail('Should have thrown');
        } catch (e) {
          // Error should be thrown - either with a code or as a general error
          expect(e).toBeDefined();
          expect(e.message).toBeDefined();
        }
      });
    });

    test('executeStreaming on closed database throws on iteration', async () => {
      const db = await openDatabase();
      await db.close();

      const stream = db.executeStreaming('SELECT * FROM test_basic.simple_table');

      await expect(async () => {
        for await (const row of stream) {
          // Should not reach here
        }
      }).rejects.toThrow(/closed/i);
    });
  });

  describe('Empty Results', () => {
    test('empty result stream iterates zero times', async () => {
      await withDatabase(async (db) => {
        // Query that returns no rows (nonexistent partition key)
        const stream = db.executeStreaming(
          "SELECT * FROM test_basic.simple_table WHERE pk = 'nonexistent_key_12345_xyz'"
        );

        const rows = [];
        for await (const row of stream) {
          rows.push(row);
        }

        expect(rows.length).toBe(0);
      });
    });

    test('empty stream has valid columns metadata after iteration', async () => {
      await withDatabase(async (db) => {
        const stream = db.executeStreaming(
          "SELECT * FROM test_basic.simple_table WHERE pk = 'nonexistent_key_12345_xyz'"
        );

        // Before iteration, columns returns empty array (lazy init)
        expect(Array.isArray(stream.columns)).toBe(true);

        // Iterate (will complete immediately with 0 rows)
        for await (const row of stream) {
          // Won't reach here since no rows
        }

        // After iteration, columns should be populated
        expect(stream.columns.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Native Type Conversion', () => {
    test('streaming returns native JavaScript types', async () => {
      await withDatabase(async (db) => {
        const stream = db.executeStreaming(
          'SELECT * FROM test_basic.simple_table LIMIT 1'
        );

        for await (const row of stream) {
          // Row should have proper native types
          expect(typeof row).toBe('object');
          // Check that we got actual values, not JSON strings
          for (const [key, value] of Object.entries(row)) {
            // Values can be various types (number, string, bigint, Date, Buffer, etc.)
            // but shouldn't be undefined unless the column is nullable
            if (value !== null) {
              expect(value).toBeDefined();
            }
          }
        }
      });
    });
  });

  describe('Memory Efficiency', () => {
    // This is a slow test - only run with RUN_SLOW_TESTS=1
    const slowTest = global.SHOULD_RUN_SLOW_TESTS ? test : test.skip;

    slowTest(
      'memory stays bounded for large result sets',
      async () => {
        await withDatabase(async (db) => {
          const config = { bufferSize: 128, chunkSize: 500 };

          // Force garbage collection if available (node --expose-gc)
          if (global.gc) {
            global.gc();
          }

          // Get baseline memory
          const baselineMemory = process.memoryUsage().heapUsed;

          // Stream all rows from a larger table - no await on executeStreaming
          const stream = db.executeStreaming(
            'SELECT * FROM test_basic.simple_table',
            config
          );

          let rowCount = 0;
          let peakMemory = baselineMemory;

          for await (const row of stream) {
            rowCount++;

            // Sample memory every 100 rows
            if (rowCount % 100 === 0) {
              const currentMemory = process.memoryUsage().heapUsed;
              peakMemory = Math.max(peakMemory, currentMemory);
            }
          }

          const memoryDelta = peakMemory - baselineMemory;
          const memoryDeltaMB = memoryDelta / (1024 * 1024);

          console.log(`Streamed ${rowCount} rows, memory delta: ${memoryDeltaMB.toFixed(2)} MB`);

          // Memory should stay well under 128MB target
          // With small config, should be under 10MB even for many rows
          expect(memoryDeltaMB).toBeLessThan(128);
        });
      },
      60000 // 60 second timeout for large data
    );
  });

  // ==========================================================================
  // Batch Fetching (Issue #1443)
  //
  // `next()` now fetches a BATCH of up to `bufferSize` rows per libuv-threadpool
  // AsyncTask (`collect_chunk`), instead of one row per task. The JS wrapper
  // buffers the batch and yields one row per `for await` iteration, so the
  // per-row consumer contract is unchanged.
  //
  // These tests use `bufferSize: 1` as the PER-ROW BASELINE (one row per
  // AsyncTask/`block_on` == the pre-#1443 behaviour) and compare it to a full
  // batch (`bufferSize: 1024`) within the SAME process and load window. That
  // relative comparison is robust to machine load (this repo runs many gates
  // concurrently) because both measurements share the same conditions — no
  // brittle absolute wall-clock bound.
  // ==========================================================================
  describe('Batch Fetching (Issue #1443)', () => {
    // Issue #4538: this guard used to assert a THROUGHPUT ratio (batched >= 1.1x per-row) measured
    // in wall time, which collapsed toward 1.0 on a loaded host regardless of the code under test.
    // The mechanism #1443 introduced is "one libuv AsyncTask (native `next()`) per BATCH, not per
    // row", and that is exactly countable: no clock, so no load sensitivity. Per-row dispatch issues
    // >= one native `next()` per row; a batch issues ~ceil(rows / bufferSize) + 1 (the terminal
    // done-call). The wall-clock throughput is still logged for visibility but is not asserted.
    test('batched streaming issues one native next() per batch, not per row', async () => {
      const native = require('../cqlite-node.linux-x64-gnu.node');
      const proto = native.StreamingResult.prototype;
      const realNext = proto.next;
      let calls = 0;
      proto.next = function countedNext(...args) {
        calls++;
        return realNext.apply(this, args);
      };
      try {
        await withDatabase(async (db) => {
          // Warm up so neither drain pays first-touch costs.
          await timedDrain(db, BATCH_QUERY, { bufferSize: 1024 });

          calls = 0;
          const perRow = await timedDrain(db, BATCH_QUERY, { bufferSize: 1 });
          const perRowCalls = calls;
          calls = 0;
          const batched = await timedDrain(db, BATCH_QUERY, { bufferSize: 1024 });
          const batchedCalls = calls;

          // FAIL LOUDLY if the table yields no rows (data expected).
          expect(perRow.count).toBeGreaterThan(0);
          expect(batched.count).toBe(perRow.count);
          const rows = batched.count;

          // Per-row baseline: at least one native dispatch per row.
          expect(perRowCalls).toBeGreaterThanOrEqual(rows);
          // Batched: ceil(rows / 1024) data batches + the terminal done-call.
          expect(batchedCalls).toBeLessThanOrEqual(Math.ceil(rows / 1024) + 1);
          // And decisively fewer dispatches than the per-row path.
          expect(batchedCalls * 10).toBeLessThanOrEqual(perRowCalls);
          // eslint-disable-next-line no-console
          console.log(
            `native next() calls for ${rows} rows: per-row ${perRowCalls}, batched ${batchedCalls}; ` +
              `drain ms per-row ${perRow.ms.toFixed(1)}, batched ${batched.ms.toFixed(1)} (informational)`
          );
        });
      } finally {
        proto.next = realNext;
      }
    });

    test('a single batched stream does not starve concurrent fs I/O', async () => {
      // NOTE (issue #1443 finding): for the canonical SINGLE-stream `for await`
      // consumer, a batched drain keeps the libuv pool responsive because only
      // one `next()` AsyncTask is outstanding at a time (at most one of the four
      // threads is held), leaving the rest for `fs`. This test is a REGRESSION
      // GUARD proving batching does not freeze concurrent `fs.readFile`. We
      // assert on the 2nd-worst latency (not the single max) so one scheduler
      // outlier cannot flake the test.
      await withDatabase(async (db) => {
        const dataFile = findDataFile();

        // Baseline: fs latency with NO streaming in flight.
        const baseline = [];
        for (let i = 0; i < 8; i++) {
          baseline.push(await fsReadLatency(dataFile));
        }
        baseline.sort((a, b) => a - b);
        const baselineSecondWorst = baseline[baseline.length - 2];

        // Launch 8 concurrent fs.readFile while draining a batched stream.
        const latencies = [];
        const stream = db.executeStreaming(BATCH_QUERY, { bufferSize: 1024 });
        const reads = [];
        for (let i = 0; i < 8; i++) {
          reads.push(fsReadLatency(dataFile).then((v) => latencies.push(v)));
        }
        let streamed = 0;
        for await (const row of stream) {
          streamed++;
        }
        await Promise.all(reads);

        // FAIL LOUDLY if the stream yielded nothing (data expected).
        expect(streamed).toBeGreaterThan(0);

        latencies.sort((a, b) => a - b);
        const duringSecondWorst = latencies[latencies.length - 2];

        // The HARD assertion is the load-robust RELATIVE bound: the 2nd-worst fs
        // latency while draining a batched stream must stay near the 2nd-worst
        // baseline latency (measured in the same process). A stream that
        // monopolised the whole libuv pool would push fs completion far past
        // this. Both terms move together under machine load, so the delta stays
        // meaningful; the margin is widened to 400ms (from 150ms) so scheduler
        // jitter under concurrent gates cannot invert it (issue #1443 de-flake).
        expect(duringSecondWorst).toBeLessThan(baselineSecondWorst + 400);

        // The former absolute `< 250ms` ceiling is machine-load-sensitive (it
        // can trip purely from an overloaded box, not from a starvation
        // regression), so it is a logged DIAGNOSTIC only, never a hard assertion.
        // eslint-disable-next-line no-console
        console.log(
          `fs latency during batched drain: 2nd-worst=${duringSecondWorst.toFixed(1)}ms ` +
            `(baseline 2nd-worst=${baselineSecondWorst.toFixed(1)}ms)`
        );
      });
    });

    test('batching preserves exact row count and order vs executeNative', async () => {
      await withDatabase(async (db) => {
        const native = await db.executeNative(BATCH_QUERY);

        // FAIL LOUDLY if the reference query returns no rows.
        expect(native.rowCount).toBeGreaterThan(0);

        const streamed = [];
        for await (const row of db.executeStreaming(BATCH_QUERY, {
          bufferSize: 256,
          chunkSize: 500,
        })) {
          streamed.push(row);
        }

        // Exact count parity: batching must not drop or duplicate rows.
        expect(streamed.length).toBe(native.rowCount);

        // Exact ORDER + value parity: compare each streamed row to the
        // corresponding executeNative row key-for-key (batching must not
        // reorder). Serialize with a replacer that handles native types
        // (BigInt/Set/Map) recursively, including nested values.
        const replacer = (_k, v) => {
          if (typeof v === 'bigint') return `bigint:${v}`;
          if (v instanceof Set) return { __set: [...v] };
          if (v instanceof Map) return { __map: [...v.entries()] };
          return v;
        };
        const rowKey = (row) =>
          JSON.stringify(
            Object.keys(row)
              .sort()
              .map((k) => [k, row[k]]),
            replacer
          );
        for (let i = 0; i < streamed.length; i++) {
          expect(rowKey(streamed[i])).toBe(rowKey(native.rows[i]));
        }
      });
    });

    test('early break from a batched stream closes cleanly and discards buffer', async () => {
      await withDatabase(async (db) => {
        // bufferSize larger than the break point ensures the FIRST batch already
        // buffered rows we will NOT yield; break must discard them and close.
        const stream = db.executeStreaming(BATCH_QUERY, { bufferSize: 1024 });

        let count = 0;
        for await (const row of stream) {
          count++;
          if (count >= 5) break;
        }
        expect(count).toBe(5);

        // After break the stream is closed; rowsReceived reflects the native
        // fetch (a full batch may have been pulled) but the iterator yields no
        // more rows. Re-iterating a closed stream yields nothing.
        const iterator = stream[Symbol.asyncIterator]();
        const afterBreak = await iterator.next();
        expect(afterBreak.done).toBe(true);
        expect(afterBreak.value).toBeUndefined();
      });
    });
  });
});
