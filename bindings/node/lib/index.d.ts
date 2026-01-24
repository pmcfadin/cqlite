/**
 * CQLite Node.js bindings type definitions.
 *
 * This module provides enhanced type definitions that include
 * error handling properties (Issue #297).
 */

// Re-export types from auto-generated definitions
export { QueryResult, DatabaseStats, DatabaseOptions, StreamingConfig } from '../index';

/**
 * Configuration for streaming query execution.
 *
 * Controls memory usage during large result set iteration.
 * Used with `executeStreaming()` for memory-efficient processing
 * of large result sets.
 *
 * ## Memory Budget
 *
 * Default values (~11MB peak usage):
 * - bufferSize: 1024 rows × ~1KB = ~1MB in flight
 * - chunkSize: 10000 rows × ~1KB = ~10MB per chunk
 *
 * For rows with large blobs, reduce buffer sizes proportionally.
 *
 * @example
 * ```typescript
 * const config: StreamingConfig = { bufferSize: 512, chunkSize: 5000 };
 * for await (const row of db.executeStreaming(query, config)) {
 *   console.log(row);
 * }
 * ```
 */
export interface StreamingConfig {
  /**
   * Number of rows to buffer in memory during streaming.
   * Controls backpressure. Default: 1024.
   */
  bufferSize?: number;

  /**
   * Number of rows per fetch chunk from storage.
   * Larger chunks improve throughput, smaller chunks reduce memory.
   * Default: 10000.
   */
  chunkSize?: number;
}

/**
 * Error codes for CQLite errors.
 *
 * These codes map to the ErrorCategory enum in cqlite-core.
 */
export type ErrorCode =
  | 'IO'           // System-level I/O errors (file access, memory, timeout)
  | 'SCHEMA'       // Schema-related errors (table not found, invalid schema)
  | 'QUERY'        // Query execution errors (unsupported queries)
  | 'PARSE'        // Data/parsing errors (CQL syntax, type conversion)
  | 'CONFIG'       // Configuration errors
  | 'STORAGE'      // Storage engine errors
  | 'CONCURRENCY'  // Concurrency/lock errors
  | 'NOT_FOUND'    // Resource not found
  | 'CONFLICT'     // Resource conflicts (already exists)
  | 'INVALID_INPUT' // Logic errors (invalid operation, invalid state)
  | 'CONSTRAINT'   // Constraint violations
  | 'TRANSACTION'  // Transaction errors
  | 'PLATFORM'     // Platform-specific errors (WASM)
  | 'INTERNAL';    // Internal errors

/**
 * Error category names for CQLite errors.
 *
 * These categories map to the ErrorCategory enum in cqlite-core.
 */
export type ErrorCategory =
  | 'System'
  | 'Data'
  | 'Schema'
  | 'Query'
  | 'Configuration'
  | 'Storage'
  | 'Concurrency'
  | 'NotFound'
  | 'Conflict'
  | 'Logic'
  | 'Constraint'
  | 'Transaction'
  | 'Platform'
  | 'Internal';

/**
 * CQLite error interface.
 *
 * All errors thrown by CQLite methods extend the standard Error
 * with additional properties for error categorization.
 *
 * @example
 * ```typescript
 * try {
 *   await db.execute('INVALID SQL');
 * } catch (e) {
 *   const err = e as CqliteError;
 *   if (err.code === 'PARSE') {
 *     console.log('SQL syntax error');
 *   }
 *   if (err.isRecoverable) {
 *     // Can retry the operation
 *   }
 * }
 * ```
 */
export interface CqliteError extends Error {
  /**
   * Error code identifying the type of error.
   *
   * Use this for programmatic error handling.
   */
  code: ErrorCode;

  /**
   * Error category name from the Rust ErrorCategory enum.
   *
   * Provides more context about the error type.
   */
  category: ErrorCategory;

  /**
   * Whether the error is potentially recoverable.
   *
   * Recoverable errors (like I/O or concurrency errors) may succeed
   * if retried. Non-recoverable errors (like parse errors) will
   * always fail with the same input.
   */
  isRecoverable: boolean;
}

/**
 * Returns the version of the cqlite-node binding.
 *
 * @returns The semantic version string (e.g., "0.3.0")
 *
 * @example
 * ```typescript
 * import { version } from '@cqlite/node';
 * console.log(`CQLite version: ${version()}`);
 * ```
 */
export declare function version(): string;

/**
 * A CQLite database handle.
 *
 * Use `Database.open()` to create a Database instance.
 * Always close the database when done to release resources.
 *
 * All methods that can fail throw `CqliteError` with structured
 * error properties (code, category, isRecoverable).
 *
 * @example
 * ```typescript
 * import { Database, CqliteError } from '@cqlite/node';
 *
 * try {
 *   const db = await Database.open('/path/to/data', {
 *     schema: '/path/to/schema.cql'
 *   });
 *   const result = await db.execute('SELECT * FROM users LIMIT 10');
 *   console.log(`Got ${result.rowCount} rows`);
 *   await db.close();
 * } catch (e) {
 *   const err = e as CqliteError;
 *   console.log(`Error [${err.code}]: ${err.message}`);
 *   console.log(`Category: ${err.category}, Recoverable: ${err.isRecoverable}`);
 * }
 * ```
 */
export declare class Database {
  /**
   * Opens a database at the specified data directory.
   *
   * @param dataDir - Path to the SSTable data directory
   * @param options - Optional configuration (schema path, etc.)
   * @returns Promise resolving to a Database instance
   * @throws {CqliteError} If the database cannot be opened
   */
  static open(dataDir: string, options?: { schema?: string }): Promise<Database>;

  /**
   * Execute a CQL query and return results.
   *
   * @param query - CQL SELECT statement to execute
   * @returns Promise resolving to QueryResult with rows and metadata
   * @throws {CqliteError} If the query fails
   */
  execute(query: string): Promise<import('../index').QueryResult>;

  /**
   * Get database statistics.
   *
   * @returns Promise resolving to DatabaseStats
   * @throws {CqliteError} If statistics cannot be retrieved
   */
  getStats(): Promise<import('../index').DatabaseStats>;

  /**
   * Close the database and release resources.
   *
   * This method is idempotent - calling it multiple times is safe.
   *
   * @returns Promise resolving when close is complete
   */
  close(): Promise<void>;

  /**
   * Check if the database is closed.
   */
  get isClosed(): boolean;
}
