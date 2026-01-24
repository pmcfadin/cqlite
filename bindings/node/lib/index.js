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

// Create wrapped Database class with enhanced error handling
const Database = createWrappedDatabase(nativeBinding.Database);

// Re-export version function (doesn't throw errors)
const { version } = nativeBinding;

module.exports = {
  Database,
  version,
};
