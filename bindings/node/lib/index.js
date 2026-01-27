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
 * PreparedStatement wraps a native PreparedStatement with type consistency.
 *
 * Ensures estimatedRows in stats() is always BigInt (Issue #351).
 * Users get this via Database.prepare() - direct construction not supported.
 *
 * Issue #351: Stats fields typed as bigint but runtime returns number
 * Issue #352: PreparedStatement not exported at runtime but TypeScript declares it
 */
class PreparedStatement {
  /**
   * @private
   * @param {Object} nativeStmt - The native PreparedStatement from Rust
   */
  constructor(nativeStmt) {
    this._native = nativeStmt;
  }

  /** The original CQL query text. */
  get query() {
    return this._native.query;
  }

  /** Number of parameters in the query. */
  get parameterCount() {
    return this._native.parameterCount;
  }

  /**
   * Get statistics about the prepared query.
   * @returns {Object} PreparedStatementStats with estimatedRows as bigint
   */
  stats() {
    const nativeStats = this._native.stats();
    return {
      parameterCount: nativeStats.parameterCount,
      planType: nativeStats.planType,
      estimatedCost: nativeStats.estimatedCost,
      estimatedRows: BigInt(nativeStats.estimatedRows),
      cacheFriendly: nativeStats.cacheFriendly,
    };
  }

  /**
   * Return a string representation of this prepared statement.
   * @returns {string} String representation
   */
  toString() {
    return this._native.toString();
  }
}

/**
 * Wrap a native PreparedStatement to ensure type consistency.
 *
 * @private
 * @param {Object} nativeStmt - The native PreparedStatement from Rust
 * @returns {PreparedStatement} Wrapped PreparedStatement instance
 */
function wrapPreparedStatement(nativeStmt) {
  return new PreparedStatement(nativeStmt);
}

// Create wrapped Database class with enhanced error handling
const Database = createWrappedDatabase(nativeBinding.Database, wrapPreparedStatement);

// Re-export version function
const { version } = nativeBinding;

module.exports = {
  Database,
  PreparedStatement,
  version,
};
