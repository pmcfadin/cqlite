/**
 * CQLite Node.js bindings entry point.
 *
 * This module wraps the native bindings with enhanced error handling
 * that provides structured error properties (code, category, isRecoverable).
 *
 * @example
 * ```javascript
 * const { Database, version } = require('@cqlite/node');
 *
 * try {
 *   const db = await Database.open('/path/to/data', {
 *     schema: '/path/to/schema.cql'
 *   });
 *   const result = await db.execute('SELECT * FROM users LIMIT 10');
 *   console.log(`Got ${result.rowCount} rows`);
 *   await db.close();
 * } catch (e) {
 *   console.log('Error code:', e.code);       // e.g., "IO", "SCHEMA", "QUERY"
 *   console.log('Category:', e.category);     // e.g., "System", "Schema"
 *   console.log('Recoverable:', e.isRecoverable);
 * }
 * ```
 */

const nativeBinding = require('../index.js');
const { createWrappedDatabase } = require('./error-wrapper.js');

/**
 * Wrap a native PreparedStatement to ensure type consistency.
 *
 * Coerces estimatedRows from number to BigInt to match TypeScript declarations.
 * napi-rs returns i64 as number for small values, but TS declares bigint.
 *
 * Issue #351: Stats fields typed as bigint but runtime returns number
 *
 * @param {Object} nativeStmt - The native PreparedStatement from Rust
 * @returns {Object} Wrapped PreparedStatement with consistent types
 */
function wrapPreparedStatement(nativeStmt) {
  return {
    /** The original CQL query text. */
    get query() {
      return nativeStmt.query;
    },

    /** Number of parameters in the query. */
    get parameterCount() {
      return nativeStmt.parameterCount;
    },

    /**
     * Get statistics about the prepared query.
     * @returns {Object} PreparedStatementStats with estimatedRows as bigint
     */
    stats() {
      const nativeStats = nativeStmt.stats();
      return {
        parameterCount: nativeStats.parameterCount,
        planType: nativeStats.planType,
        estimatedCost: nativeStats.estimatedCost,
        estimatedRows: BigInt(nativeStats.estimatedRows),
        cacheFriendly: nativeStats.cacheFriendly,
      };
    },

    /**
     * Return a string representation of this prepared statement.
     * @returns {string} String representation
     */
    toString() {
      return nativeStmt.toString();
    },
  };
}

// Create wrapped Database class with enhanced error handling
const Database = createWrappedDatabase(nativeBinding.Database, wrapPreparedStatement);

// Re-export version function
// Note: PreparedStatement class is not exported directly - it's wrapped via wrapPreparedStatement
const { version } = nativeBinding;

module.exports = {
  Database,
  version,
  // Export wrapper function for internal use and testing
  wrapPreparedStatement,
};
