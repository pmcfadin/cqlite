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
const { createWrappedDatabase, enhanceError } = require('./error-wrapper.js');

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

/**
 * Test-support: throw the JS error the shared FFI error contract maps a named
 * core Rust `Error` variant to (issue #1451).
 *
 * Goes through the production native mapping and the same `enhanceError`
 * wrapper every `Database` method uses, so the thrown error carries the real
 * `code`/`category`/`isRecoverable`. This is how the test suite reaches variants
 * no query can provoke (`Timeout`, `Memory`). The Python binding's twin is
 * `cqlite._raise_mapped_core_error`.
 *
 * Not part of the stable public API — the leading underscore marks it internal
 * test support.
 *
 * @private
 * @param {string} variant - Core `Error` variant identifier, e.g. 'Timeout'
 * @throws {Error} Always; the mapped error for `variant`.
 */
function _errorContractProbe(variant) {
  try {
    return nativeBinding.errorContractProbe(variant);
  } catch (error) {
    throw enhanceError(error);
  }
}

/**
 * Test-support: the distinct `code` values the shared FFI error contract can
 * emit (issue #1451), sorted and deduplicated.
 *
 * The authoritative set comes from the Rust contract table, so the `ErrorCode`
 * union in `index.d.ts` can be asserted against it instead of against a
 * hand-written copy (see `__test__/typescript-definitions.test.js`).
 *
 * Not part of the stable public API — the leading underscore marks it internal
 * test support.
 *
 * @private
 * @returns {string[]} Sorted, deduplicated error codes.
 */
function _errorContractNodeCodes() {
  return nativeBinding.errorContractNodeCodes();
}

/**
 * Test-support: every committed cross-binding vector (issue #1452), rendered
 * through this binding's PRODUCTION conversion path.
 *
 * The tables live in `cqlite_ffi_common::vectors` and the Python binding's twin
 * surface (`cqlite._ffi_common_render_vectors`) reads the same ones, so a
 * divergence between the bindings — or a re-introduced private implementation in
 * either — fails BOTH suites. See `__test__/shared-vectors.test.js`.
 *
 * Not part of the stable public API — the leading underscore marks it internal
 * test support.
 *
 * @private
 * @returns {Array<{cqlType: string, name: string, kind: string, expected: string,
 *                  expectedSha256: (string|null), outcome: string, actual: string,
 *                  rendered: (string|null), scale: number, bytes: Buffer}>}
 *   One entry per committed vector. `expected` is the committed expectation —
 *   collapsed to a digest for a multi-kilobyte rendering, in which case
 *   `expectedSha256` carries the lower-case SHA-256 hex of the UTF-8 bytes of the
 *   FULL expected rendering (`null` when `expected` is itself exact).
 *   `rendered` is the full, un-digested string this binding produced (`null` on a
 *   refusal); `actual` is the readable half — the digest, or the native error's
 *   `reason` — and is never the oracle for a long rendering.
 */
function _ffiCommonRenderVectors() {
  try {
    return nativeBinding.ffiCommonRenderVectors();
  } catch (error) {
    throw enhanceError(error);
  }
}

module.exports = {
  Database,
  PreparedStatement,
  version,
  _errorContractProbe,
  _errorContractNodeCodes,
  _ffiCommonRenderVectors,
};
