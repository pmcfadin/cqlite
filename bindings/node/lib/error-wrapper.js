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
 * Create an async iterable wrapper around a native StreamingResult.
 *
 * Implements JavaScript's AsyncIterable protocol for use with `for await...of`.
 * Provides automatic resource cleanup on loop completion or early termination.
 *
 * @param {Object} nativeStream - The native StreamingResult from Rust
 * @returns {Object} An async iterable object with streaming functionality
 */
function createAsyncIterator(nativeStream) {
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
      return nativeStream.close();
    },

    /**
     * Implement Symbol.asyncIterator for `for await...of` support.
     * @returns {AsyncIterator}
     */
    [Symbol.asyncIterator]() {
      return {
        /**
         * Get the next row from the stream.
         * @returns {Promise<{value: Object|undefined, done: boolean}>}
         */
        async next() {
          try {
            const result = await nativeStream.next();
            return result;
          } catch (error) {
            throw enhanceError(error);
          }
        },

        /**
         * Handle early termination (e.g., break from loop).
         * Called automatically by JavaScript runtime.
         * @returns {Promise<{value: undefined, done: true}>}
         */
        async return() {
          nativeStream.close();
          return { value: undefined, done: true };
        },
      };
    },
  };
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
         * @returns {AsyncIterator}
         */
        [Symbol.asyncIterator]() {
          return {
            async next() {
              try {
                const stream = await ensureInitialized();
                return await stream.next();
              } catch (error) {
                throw enhanceError(error);
              }
            },
            async return() {
              closed = true;
              nativeStreamPromise = null;
              nativeStream?.close();
              return { value: undefined, done: true };
            },
          };
        },
      };
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
