/**
 * Error wrapper module for CQLite Node.js bindings.
 *
 * This module provides utilities for parsing error metadata from
 * native error messages and attaching them as properties.
 *
 * Issue #297: Error Mapping Implementation
 */

/**
 * Parse error metadata from a message string.
 *
 * The Rust layer encodes metadata in the message using null-byte separators:
 * "Human-readable message\0code=IO\0category=System\0isRecoverable=true"
 *
 * @param {string} message - The error message from native code
 * @returns {Object} Parsed metadata with code, category, isRecoverable, and message
 */
function parseErrorMetadata(message) {
  if (!message || typeof message !== 'string') {
    return {
      code: 'INTERNAL',
      category: 'Internal',
      isRecoverable: false,
      message: String(message || 'Unknown error'),
    };
  }

  // Split by null bytes
  const parts = message.split('\0');
  const humanMessage = parts[0];

  // Default values
  let code = 'INTERNAL';
  let category = 'Internal';
  let isRecoverable = false;

  // Parse metadata from remaining parts
  for (let i = 1; i < parts.length; i++) {
    const part = parts[i];
    if (part.startsWith('code=')) {
      code = part.slice(5);
    } else if (part.startsWith('category=')) {
      category = part.slice(9);
    } else if (part.startsWith('isRecoverable=')) {
      isRecoverable = part.slice(14) === 'true';
    }
  }

  return {
    code,
    category,
    isRecoverable,
    message: humanMessage,
  };
}

/**
 * Enhance an Error object with CQLite metadata properties.
 *
 * @param {Error} error - The error to enhance
 * @returns {Error} The enhanced error with code, category, and isRecoverable properties
 */
function enhanceError(error) {
  if (!error || typeof error.message !== 'string') {
    return error;
  }

  const metadata = parseErrorMetadata(error.message);

  // Update the message to the human-readable part only
  error.message = metadata.message;

  // Add properties
  error.code = metadata.code;
  error.category = metadata.category;
  error.isRecoverable = metadata.isRecoverable;

  return error;
}

/**
 * Wrap an async function to enhance any thrown errors.
 *
 * @param {Function} fn - The async function to wrap
 * @returns {Function} A wrapped function that enhances errors
 */
function wrapAsync(fn) {
  return async function (...args) {
    try {
      return await fn.apply(this, args);
    } catch (error) {
      throw enhanceError(error);
    }
  };
}

/**
 * Build an AsyncIterator that yields ONE row per `next()` while fetching rows
 * from the native stream in BATCHES (issue #1443).
 *
 * The native `StreamingResult.next()` returns `{ rows: Array<Row>, done }` — a
 * whole batch per AsyncTask/`block_on` (K == the stream's `bufferSize`), instead
 * of one row per task. This wrapper buffers that batch and drains it one row at
 * a time, so the public per-row `for await ... of` contract is UNCHANGED
 * (consumers still see exactly one row per iteration; batching is invisible).
 * Amortising dispatch over K rows both raises throughput and stops a busy stream
 * from monopolising libuv's small (default 4) threadpool and starving concurrent
 * `fs`/`crypto` work.
 *
 * @param {() => Promise<{rows: Array<Object>, done: boolean}>} refill
 *   Fetches the next batch from the native stream. May throw (already enhanced).
 * @param {(yielded: number) => (void|Promise<void>)} onReturn
 *   Invoked on early termination (`return()`/`break`) to close the native
 *   stream. Receives the exact number of rows this iterator YIELDED to the
 *   consumer, so the native span records rows-yielded rather than the whole
 *   fetched batch (the un-yielded tail of the last batch is discarded here on
 *   early break). See issue #1443.
 * @param {() => boolean} [isCancelled]
 *   Optional predicate checked before each `next()`; when it returns true the
 *   iterator discards any buffered rows and reports `done` immediately. This is
 *   how an external `close()` on the stream object takes effect even though the
 *   batch buffer lives in this iterator's closure.
 * @returns {AsyncIterator} Iterator yielding one row per `next()`.
 */
function batchedAsyncIterator(refill, onReturn, isCancelled) {
  let buffer = [];
  let bufIdx = 0;
  let exhausted = false;
  // Exact count of rows YIELDED to the consumer (one per `next()` that returns
  // a value). Passed to `onReturn` on early break so the native span records
  // rows-yielded, not the whole fetched batch (issue #1443).
  let yielded = 0;
  return {
    async next() {
      // Honour an external close(): discard buffered rows and end immediately.
      if (isCancelled && isCancelled()) {
        buffer = [];
        bufIdx = 0;
        exhausted = true;
        return { value: undefined, done: true };
      }
      // Drain the buffered batch first — no native call, no threadpool dispatch.
      if (bufIdx < buffer.length) {
        yielded++;
        return { value: buffer[bufIdx++], done: false };
      }
      if (exhausted) {
        return { value: undefined, done: true };
      }
      // Refill: exactly ONE AsyncTask/block_on fetches a whole batch.
      const batch = await refill();
      buffer = (batch && batch.rows) || [];
      bufIdx = 0;
      // An empty batch always signals exhaustion (native `Done`); a `done` flag
      // is honoured defensively too. A non-empty batch may still be the final
      // one — the next refill then observes the empty batch and ends the stream.
      if (!batch || batch.done || buffer.length === 0) {
        exhausted = true;
      }
      if (bufIdx < buffer.length) {
        yielded++;
        return { value: buffer[bufIdx++], done: false };
      }
      return { value: undefined, done: true };
    },
    async return() {
      // Early `break`/`return()`: discard any un-yielded buffered rows and close
      // the native stream (which terminates the producer and finalises the
      // span). Pass the exact rows yielded so the span is not over-counted by
      // the discarded buffer tail (issue #1443).
      buffer = [];
      bufIdx = 0;
      exhausted = true;
      await onReturn(yielded);
      return { value: undefined, done: true };
    },
  };
}

/**
 * Create an async iterable wrapper around a native StreamingResult.
 *
 * Implements JavaScript's AsyncIterable protocol for use with `for await...of`.
 * Provides automatic resource cleanup on loop completion or early termination.
 *
 * @param {Object} nativeStream - The native StreamingResult from Rust
 * @returns {Object} An async iterable object with streaming functionality
 */
function createAsyncIterator(nativeStream) {
  let closed = false;
  return {
    /**
     * Number of rows received so far.
     * @returns {number}
     */
    get rowsReceived() {
      return nativeStream.rowsReceived;
    },

    /**
     * Column metadata for the result set.
     * @returns {Array<Object>}
     */
    get columns() {
      return nativeStream.columns;
    },

    /**
     * Release resources early.
     * Safe to call multiple times.
     */
    close() {
      closed = true;
      return nativeStream.close();
    },

    /**
     * Implement Symbol.asyncIterator for `for await...of` support.
     *
     * Yields one row per iteration while fetching in batches (issue #1443).
     * @returns {AsyncIterator}
     */
    [Symbol.asyncIterator]() {
      return batchedAsyncIterator(
        async () => {
          try {
            return await nativeStream.next();
          } catch (error) {
            throw enhanceError(error);
          }
        },
        (yielded) => {
          closed = true;
          nativeStream.close(yielded);
        },
        () => closed
      );
    },
  };
}

/**
 * Emit a one-time deprecation warning for the legacy `execute()` method.
 *
 * `execute()` returns lossy legacy JSON encodings — blob comes back as a base64
 * string (not a Buffer), timestamp as an ISO-8601 string (not a Date), and
 * varint/decimal in bespoke `"0x…"` / `"decimal:…"` strings no caller can
 * round-trip (see index.d.ts). It also double-converts (JSON off-loop, then JS
 * on-loop) so it is slower than `executeNative()`. It is deprecated and will be
 * removed in the next major; `executeNative()` returns native types with full
 * fidelity. We emit via `process.emitWarning(..., 'DeprecationWarning')` guarded
 * by a module-level flag so it fires at most once per process, matching Node's
 * own deprecation convention (issue #1457).
 *
 * @private
 */
let executeDeprecationWarned = false;
function warnExecuteDeprecated() {
  if (executeDeprecationWarned) {
    return;
  }
  executeDeprecationWarned = true;
  process.emitWarning(
    "Database.execute() is deprecated and will be removed in the next major. " +
      "It returns lossy legacy JSON encodings: blob becomes a base64 string " +
      "(not a Buffer), timestamp an ISO-8601 string (not a Date), and " +
      "varint/decimal bespoke non-round-trippable strings; it is also slower " +
      "(double conversion). Use executeNative() for native types with full " +
      "fidelity (BigInt, Buffer, Date, Set, Map).",
    {
      type: "DeprecationWarning",
      code: "CQLITE_DEP0001",
    }
  );
}

/**
 * Create a wrapped Database class with enhanced error handling.
 *
 * @param {Function} NativeDatabase - The native Database class
 * @param {Function} wrapPreparedStatement - Function to wrap PreparedStatement for type consistency
 * @returns {Function} A wrapped Database class
 */
function createWrappedDatabase(NativeDatabase, wrapPreparedStatement) {
  class Database {
    constructor(native, preparedStatementWrapper) {
      this._native = native;
      this._wrapPreparedStatement = preparedStatementWrapper;
    }

    static async open(dataDir, options) {
      try {
        const native = await NativeDatabase.open(dataDir, options);
        return new Database(native, wrapPreparedStatement);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async execute(query) {
      warnExecuteDeprecated();
      try {
        const result = await this._native.execute(query);
        // For SELECT: rowsAffected = rowCount (alias, Issue #348).
        // For DML (INSERT/UPDATE/DELETE): rowsAffected is already set by Rust layer to 1;
        // do NOT overwrite it with rowCount (which would be 0 for writes).
        if (result.rowsAffected === undefined || result.rowsAffected === null) {
          result.rowsAffected = result.rowCount;
        }
        return result;
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async executeNative(query) {
      try {
        const result = await this._native.executeNative(query);
        // Same pattern as execute(): preserve rowsAffected from Rust layer.
        if (result.rowsAffected === undefined || result.rowsAffected === null) {
          result.rowsAffected = result.rowCount;
        }
        return result;
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async getStats() {
      try {
        const stats = await this._native.getStats();
        // Coerce to BigInt to ensure TypeScript type guarantees hold (Issue #351)
        // napi-rs returns i64 as number for small values, but TS declares bigint
        return {
          totalSstables: stats.totalSstables,
          totalRows: BigInt(stats.totalRows),
          memoryUsedBytes: BigInt(stats.memoryUsedBytes),
        };
      } catch (error) {
        throw enhanceError(error);
      }
    }

    /**
     * Re-discover the data directory and apply changes to the held reader set.
     *
     * Newly present SSTable generations become queryable, removed generations
     * stop being queried, and unchanged generations keep their warm parsed
     * state. In-flight queries are unaffected; the refresh is atomic and
     * fail-closed. See index.d.ts for the full contract (issue #1749).
     *
     * @returns {Promise<Object>} RefreshReport with tablesScanned, readersAdded, readersRemoved
     * @throws {CqliteError} If the database is closed or a new generation fails to open
     */
    async refresh() {
      try {
        return await this._native.refresh();
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async close() {
      try {
        return await this._native.close();
      } catch (error) {
        throw enhanceError(error);
      }
    }

    get isClosed() {
      return this._native.isClosed;
    }

    /**
     * Execute a CQL query with streaming results.
     *
     * Returns an async iterable that yields rows one at a time for
     * memory-efficient processing of large result sets.
     *
     * @param {string} query - CQL SELECT statement to execute
     * @param {Object} [config] - Optional streaming configuration
     * @param {number} [config.bufferSize=1024] - Rows to buffer in memory
     * @param {number} [config.chunkSize=10000] - Rows per fetch chunk
     * @returns {AsyncIterable<Object>} Async iterable of rows
     * @throws {CqliteError} If the query fails (on first iteration)
     *
     * @example
     * for await (const row of db.executeStreaming('SELECT * FROM large_table')) {
     *   console.log(row.name);
     * }
     */
    executeStreaming(query, config) {
      const self = this;
      let nativeStreamPromise = null;
      let nativeStream = null;
      let initError = null;
      let closed = false;

      // Lazy initialization - called on first iteration
      const ensureInitialized = async () => {
        // Check if stream was closed before initialization
        if (closed) {
          return { next: async () => ({ value: undefined, done: true }) };
        }
        if (initError) throw initError;
        if (nativeStream) return nativeStream;
        if (!nativeStreamPromise) {
          nativeStreamPromise = self._native.executeStreaming(query, config)
            .then(stream => {
              // Check if close() was called while we were initializing
              if (closed) {
                stream.close();
                return { next: async () => ({ value: undefined, done: true }) };
              }
              nativeStream = stream;
              return stream;
            })
            .catch(err => {
              initError = enhanceError(err);
              throw initError;
            });
        }
        return nativeStreamPromise;
      };

      return {
        /**
         * Number of rows received so far.
         * Returns 0 before iteration begins.
         * @returns {number}
         */
        get rowsReceived() {
          return nativeStream?.rowsReceived ?? 0;
        },

        /**
         * Column metadata for the result set.
         * Returns empty array before iteration begins.
         * @returns {Array<Object>}
         */
        get columns() {
          return nativeStream?.columns ?? [];
        },

        /**
         * Release resources early.
         * Safe to call multiple times.
         * If called before initialization completes, prevents initialization
         * from creating a zombie stream.
         */
        close() {
          closed = true;
          // Clear the promise to prevent initialization from completing
          // after close() is called (prevents zombie streams)
          nativeStreamPromise = null;
          nativeStream?.close();
        },

        /**
         * Implement Symbol.asyncIterator for `for await...of` support.
         *
         * Yields one row per iteration while the native stream is fetched in
         * batches (issue #1443): each refill is a single AsyncTask/block_on that
         * returns up to `bufferSize` rows, which are then drained one at a time.
         * The per-row consumer contract is unchanged; batching is invisible.
         * @returns {AsyncIterator}
         */
        [Symbol.asyncIterator]() {
          return batchedAsyncIterator(
            async () => {
              try {
                const stream = await ensureInitialized();
                return await stream.next();
              } catch (error) {
                throw enhanceError(error);
              }
            },
            (yielded) => {
              closed = true;
              nativeStreamPromise = null;
              nativeStream?.close(yielded);
            },
            () => closed
          );
        },
      };
    }

    /**
     * Export the results of a CQL query to a Parquet file.
     *
     * The query runs with streaming, so large result sets are written
     * within bounded memory. The export runs off the JavaScript main
     * thread.
     *
     * @param {string} query - CQL SELECT statement to execute
     * @param {string} path - Destination file path (created or truncated)
     * @param {Object} [options] - Optional export options
     * @param {number} [options.rowGroupSize=10000] - Rows per Parquet row group
     * @param {string} [options.compression='snappy'] - 'snappy', 'zstd', or 'none'
     * @returns {Promise<number>} Number of rows written
     * @throws {CqliteError} If the query fails or the file cannot be written
     *
     * @example
     * const rows = await db.exportParquet(
     *   'SELECT * FROM my_ks.my_table',
     *   '/tmp/out.parquet',
     *   { rowGroupSize: 5000, compression: 'zstd' }
     * );
     */
    async exportParquet(query, path, options) {
      try {
        return await this._native.exportParquet(query, path, options);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    /**
     * Prepare a CQL query for analysis.
     *
     * Returns a PreparedStatement that can be inspected for query plan
     * information and statistics.
     *
     * @param {string} query - CQL SELECT statement to prepare
     * @returns {Promise<PreparedStatement>} PreparedStatement with query plan info
     * @throws {CqliteError} If the query cannot be prepared
     */
    async prepare(query) {
      try {
        const nativeStmt = await this._native.prepare(query);
        // Wrap to ensure type consistency (Issue #351)
        return this._wrapPreparedStatement(nativeStmt);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    /**
     * Flush the in-memory write buffer (memtable) to an SSTable on disk.
     *
     * Returns the path to the created Data.db file.
     * Returns an empty string if the memtable was empty (no-op flush).
     *
     * Requires the database to have been opened with `{ writable: true }`.
     *
     * @returns {Promise<string>} Absolute path to the Data.db file, or "" if nothing flushed
     * @throws {CqliteError} If write support is not enabled or the flush fails
     */
    async flushRun() {
      try {
        return await this._native.flushRun();
      } catch (error) {
        throw enhanceError(error);
      }
    }

    /**
     * Perform time-bounded background maintenance (compaction).
     *
     * @param {Object} [options] - Maintenance options
     * @param {number} [options.budgetMs=100] - Time budget in milliseconds
     * @returns {Promise<Object>} MaintenanceReport with timeSpentMs, rowsMerged, etc.
     * @throws {CqliteError} If write support is not enabled or maintenance fails
     */
    async maintenanceStep(options) {
      try {
        return await this._native.maintenanceStep(options);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    /**
     * Get current write engine statistics (synchronous getter).
     *
     * @returns {Object} WriteStats with memtableSize, memtableRows, walSize, l0Count, totalWritten
     * @throws {CqliteError} If write support is not enabled
     */
    get writeStats() {
      try {
        return this._native.writeStats;
      } catch (error) {
        throw enhanceError(error);
      }
    }
  }

  return Database;
}

module.exports = {
  parseErrorMetadata,
  enhanceError,
  wrapAsync,
  createWrappedDatabase,
};
