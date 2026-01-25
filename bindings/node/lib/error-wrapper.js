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
 * @returns {Function} A wrapped Database class
 */
function createWrappedDatabase(NativeDatabase) {
  class Database {
    constructor(native) {
      this._native = native;
    }

    static async open(dataDir, options) {
      try {
        const native = await NativeDatabase.open(dataDir, options);
        return new Database(native);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async execute(query) {
      try {
        return await this._native.execute(query);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async executeNative(query) {
      try {
        return await this._native.executeNative(query);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async getStats() {
      try {
        return await this._native.getStats();
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
     * @returns {Promise<AsyncIterable<Object>>} Async iterable of rows
     * @throws {CqliteError} If the query fails
     *
     * @example
     * const stream = await db.executeStreaming('SELECT * FROM large_table');
     * for await (const row of stream) {
     *   console.log(row.name);
     * }
     */
    async executeStreaming(query, config) {
      try {
        const nativeStream = await this._native.executeStreaming(query, config);
        return createAsyncIterator(nativeStream);
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
        return await this._native.prepare(query);
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
