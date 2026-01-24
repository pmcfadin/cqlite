/**
 * CQLite Node.js bindings type definitions.
 *
 * This module provides complete type definitions for the CQLite Node.js
 * bindings, enabling full TypeScript support with accurate CQL-to-JavaScript
 * type mappings.
 *
 * @packageDocumentation
 * @module @cqlite/node
 */

/// <reference types="node" />

// ============================================================================
// Value Types
// ============================================================================

/**
 * Duration value representing CQL duration type.
 *
 * CQL durations have three components: months, days, and nanoseconds.
 * Months and days are stored separately because they have variable lengths.
 *
 * @example
 * ```typescript
 * const duration: Duration = { months: 1, days: 15, nanos: 3600000000000n };
 * ```
 */
export interface Duration {
  /** Number of months (-2^31 to 2^31-1) */
  months: number;
  /** Number of days (-2^31 to 2^31-1) */
  days: number;
  /** Number of nanoseconds (uses bigint for full i64 precision) */
  nanos: bigint;
}

/**
 * User-Defined Type (UDT) value.
 *
 * UDTs are returned as plain objects with metadata fields:
 * - `_type`: The UDT type name
 * - `_keyspace`: The keyspace containing the UDT definition
 * - Additional properties for each field in the UDT
 *
 * @example
 * ```typescript
 * const address: UdtValue = {
 *   _type: 'address',
 *   _keyspace: 'my_keyspace',
 *   street: '123 Main St',
 *   city: 'San Francisco',
 *   zip: '94102'
 * };
 * ```
 */
export interface UdtValue {
  /** UDT type name */
  _type: string;
  /** Keyspace containing the UDT definition */
  _keyspace: string;
  /** UDT fields (additional properties) */
  [field: string]: Value;
}

/**
 * All possible JavaScript values returned from CQL queries.
 *
 * This union type represents the complete mapping from CQL types to JavaScript:
 *
 * | CQL Type | JavaScript Type |
 * |----------|-----------------|
 * | null | `null` |
 * | boolean | `boolean` |
 * | tinyint, smallint, int, float, double | `number` |
 * | bigint, counter, time, varint | `bigint` |
 * | text, varchar, ascii | `string` |
 * | uuid, timeuuid | `string` (formatted UUID) |
 * | decimal | `string` (preserves precision) |
 * | inet | `string` (IP address format) |
 * | blob | `Buffer` |
 * | timestamp, date | `Date` |
 * | duration | `Duration` object |
 * | list, tuple | `Value[]` |
 * | set | `Set<Value>` |
 * | map | `Map<Value, Value>` |
 * | udt | `UdtValue` object |
 * | frozen<T> | unwrapped inner type |
 *
 * @example
 * ```typescript
 * // Values from executeNative() are properly typed
 * const result = await db.executeNative('SELECT * FROM users');
 * const row = result.rows[0];
 * const name: Value = row.name;        // string
 * const age: Value = row.age;          // number
 * const balance: Value = row.balance;  // bigint
 * ```
 */
export type Value =
  | null
  | boolean
  | number
  | bigint
  | string
  | Buffer
  | Date
  | Duration
  | Value[]
  | Set<Value>
  | Map<Value, Value>
  | UdtValue;

/**
 * A single row from a query result.
 *
 * Rows are plain JavaScript objects with column names as keys
 * and CQL values converted to JavaScript types.
 *
 * @example
 * ```typescript
 * const result = await db.executeNative('SELECT id, name, age FROM users');
 * for (const row of result.rows) {
 *   console.log(row.id);   // string (UUID)
 *   console.log(row.name); // string
 *   console.log(row.age);  // number
 * }
 * ```
 */
export interface Row {
  [column: string]: Value;
}

// ============================================================================
// Column Metadata
// ============================================================================

/**
 * Column metadata information.
 *
 * Provides information about a column in the query result set,
 * including name, data type, and nullability.
 *
 * @example
 * ```typescript
 * const result = await db.execute('SELECT * FROM users');
 * for (const col of result.columns) {
 *   console.log(`${col.name}: ${col.dataType} (nullable: ${col.nullable})`);
 * }
 * ```
 */
export interface ColumnInfo {
  /** Column name. */
  name: string;

  /**
   * CQL data type as a string.
   *
   * Examples: "Text", "Integer", "BigInt", "Uuid", "Timestamp",
   * "List", "Set", "Map", "Tuple", "Udt"
   */
  dataType: string;

  /** Whether the column can contain null values. */
  nullable: boolean;

  /** Column position in the result set (0-indexed). */
  position: number;

  /**
   * Original table name.
   *
   * Present for queries involving multiple tables (joins).
   * May be null for single-table queries or computed columns.
   */
  tableName: string | null;
}

// ============================================================================
// Query Results
// ============================================================================

/**
 * Query execution result.
 *
 * Contains the query results serialized as JSON values for JavaScript
 * consumption, along with metadata about the execution.
 *
 * For native JavaScript types (BigInt, Buffer, Date, Set, Map),
 * use `Database.executeNative()` instead.
 *
 * @example
 * ```typescript
 * const result = await db.execute('SELECT * FROM users LIMIT 10');
 * console.log(`Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
 * for (const row of result.rows) {
 *   console.log(row.name);
 * }
 * ```
 */
export interface QueryResult {
  /**
   * Result rows as JSON-serializable objects.
   *
   * Values are JSON-serialized versions of CQL types:
   * - BigInt/Counter: number (may lose precision for values > 2^53)
   * - Blob: base64 string
   * - Timestamp: ISO 8601 string
   * - Set/Map: Array representations
   *
   * For native types with full precision, use `executeNative()`.
   */
  rows: Record<string, unknown>[];

  /** Number of rows returned. */
  rowCount: number;

  /** Query execution time in milliseconds. */
  executionTimeMs: number;

  /** Column metadata for the result set. */
  columns: ColumnInfo[];
}

/**
 * Query result with native JavaScript types.
 *
 * Returned by `Database.executeNative()`. Uses native JavaScript types
 * (BigInt, Buffer, Date, Set, Map) instead of JSON-serializable values.
 *
 * @example
 * ```typescript
 * const result = await db.executeNative('SELECT * FROM users');
 * for (const row of result.rows) {
 *   // Proper types preserved
 *   if (typeof row.balance === 'bigint') {
 *     console.log(`Balance: ${row.balance}`);
 *   }
 * }
 * ```
 */
export interface NativeQueryResult {
  /**
   * Result rows with native JavaScript types.
   * Each row is an object with column names as keys.
   */
  rows: Row[];

  /** Number of rows returned. */
  rowCount: number;

  /** Query execution time in milliseconds. */
  executionTimeMs: number;

  /** Column metadata for the result set. */
  columns: ColumnInfo[];
}

// ============================================================================
// Database Statistics
// ============================================================================

/**
 * Database statistics.
 *
 * Provides information about the database state including
 * storage and memory metrics.
 *
 * @example
 * ```typescript
 * const stats = await db.getStats();
 * console.log(`SSTables: ${stats.totalSstables}`);
 * console.log(`Total rows: ${stats.totalRows}`);
 * console.log(`Memory: ${stats.memoryUsedBytes} bytes`);
 * ```
 */
export interface DatabaseStats {
  /** Total number of SSTable files. */
  totalSstables: number;

  /** Total number of rows across all SSTables. */
  totalRows: bigint;

  /** Memory currently used by the database in bytes. */
  memoryUsedBytes: bigint;
}

// ============================================================================
// Configuration
// ============================================================================

/**
 * Database open options.
 *
 * Configuration options for opening a database.
 *
 * @example
 * ```typescript
 * const options: DatabaseOptions = {
 *   schema: '/path/to/schema.cql'
 * };
 * const db = await Database.open('/path/to/data', options);
 * ```
 */
export interface DatabaseOptions {
  /**
   * Path to a CQL schema file (.cql).
   * If provided, the schema will be loaded and used for query execution.
   */
  schema?: string;
}

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
 * - bufferSize: 1024 rows x ~1KB = ~1MB in flight
 * - chunkSize: 10000 rows x ~1KB = ~10MB per chunk
 *
 * For rows with large blobs, reduce buffer sizes proportionally.
 *
 * @example
 * ```typescript
 * const config: StreamingConfig = { bufferSize: 512, chunkSize: 5000 };
 * // Future: for await (const row of db.executeStreaming(query, config)) { ... }
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

// ============================================================================
// Error Handling
// ============================================================================

/**
 * Error codes for CQLite errors.
 *
 * These codes map to the ErrorCategory enum in cqlite-core and can be used
 * for programmatic error handling.
 *
 * @example
 * ```typescript
 * try {
 *   await db.execute('INVALID SQL');
 * } catch (e) {
 *   const err = e as CqliteError;
 *   switch (err.code) {
 *     case 'PARSE':
 *       console.log('SQL syntax error');
 *       break;
 *     case 'IO':
 *       console.log('I/O error, may be retryable');
 *       break;
 *   }
 * }
 * ```
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
 * These categories map to the ErrorCategory enum in cqlite-core and
 * provide semantic grouping for error types.
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
 * with additional properties for error categorization and recovery.
 *
 * @example
 * ```typescript
 * try {
 *   await db.execute('INVALID SQL');
 * } catch (e) {
 *   const err = e as CqliteError;
 *   console.log(`Error [${err.code}]: ${err.message}`);
 *   console.log(`Category: ${err.category}`);
 *   if (err.isRecoverable) {
 *     console.log('This error may succeed on retry');
 *   }
 * }
 * ```
 */
export interface CqliteError extends Error {
  /**
   * Error code identifying the type of error.
   *
   * Use this for programmatic error handling with switch/case.
   */
  code: ErrorCode;

  /**
   * Error category name from the Rust ErrorCategory enum.
   *
   * Provides semantic grouping for error types.
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

// ============================================================================
// Database Class
// ============================================================================

/**
 * A CQLite database handle.
 *
 * Use `Database.open()` to create a Database instance.
 * Always close the database when done to release resources.
 *
 * All methods that can fail throw `CqliteError` with structured
 * error properties (code, category, isRecoverable).
 *
 * ## Thread Safety
 *
 * Database handles are thread-safe and can be shared across worker threads.
 * The `close()` method is idempotent - calling it multiple times is safe.
 *
 * @example
 * ```typescript
 * import { Database, CqliteError } from '@cqlite/node';
 *
 * try {
 *   const db = await Database.open('/path/to/data', {
 *     schema: '/path/to/schema.cql'
 *   });
 *
 *   const result = await db.execute('SELECT * FROM users LIMIT 10');
 *   console.log(`Got ${result.rowCount} rows`);
 *
 *   // For native types (BigInt, Buffer, Date, etc.)
 *   const native = await db.executeNative('SELECT * FROM users LIMIT 10');
 *   for (const row of native.rows) {
 *     console.log(row.name, typeof row.balance);
 *   }
 *
 *   await db.close();
 * } catch (e) {
 *   const err = e as CqliteError;
 *   console.log(`Error [${err.code}]: ${err.message}`);
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
   *
   * @example
   * ```typescript
   * // Basic open
   * const db = await Database.open('/path/to/sstables');
   *
   * // With schema file
   * const db = await Database.open('/path/to/sstables', {
   *   schema: '/path/to/schema.cql'
   * });
   * ```
   */
  static open(dataDir: string, options?: DatabaseOptions): Promise<Database>;

  /**
   * Execute a CQL query and return results as JSON-serializable values.
   *
   * Use this method when you need JSON-compatible output or don't need
   * native JavaScript types. For native types with full precision,
   * use `executeNative()` instead.
   *
   * @param query - CQL SELECT statement to execute
   * @returns Promise resolving to QueryResult with rows and metadata
   * @throws {CqliteError} If the query fails
   *
   * @example
   * ```typescript
   * const result = await db.execute('SELECT * FROM users LIMIT 10');
   * console.log(`Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
   * for (const row of result.rows) {
   *   console.log(row.name);
   * }
   * ```
   */
  execute(query: string): Promise<QueryResult>;

  /**
   * Execute a CQL query and return results with native JavaScript types.
   *
   * This method returns native JavaScript types instead of JSON-serializable values:
   * - `bigint` for CQL bigint/counter/varint/time (preserves 64-bit precision)
   * - `Buffer` for CQL blob
   * - `Date` for CQL timestamp/date
   * - `Set` for CQL set
   * - `Map` for CQL map
   * - `Duration` object for CQL duration
   *
   * Use this method when you need:
   * - Full precision for large integers (> Number.MAX_SAFE_INTEGER)
   * - Native Buffer handling for binary data
   * - Native Set/Map operations
   *
   * @param query - CQL SELECT statement to execute
   * @returns Promise resolving to NativeQueryResult with native typed rows
   * @throws {CqliteError} If the query fails
   *
   * @example
   * ```typescript
   * const result = await db.executeNative('SELECT * FROM users LIMIT 10');
   * for (const row of result.rows) {
   *   // row.id is bigint if column is CQL bigint
   *   // row.created_at is Date if column is timestamp
   *   // row.data is Buffer if column is blob
   *   console.log(row.name, typeof row.id);
   * }
   * ```
   */
  executeNative(query: string): Promise<NativeQueryResult>;

  /**
   * Get database statistics.
   *
   * Returns information about storage, memory usage, and other metrics.
   *
   * @returns Promise resolving to DatabaseStats
   * @throws {CqliteError} If statistics cannot be retrieved
   *
   * @example
   * ```typescript
   * const stats = await db.getStats();
   * console.log(`SSTables: ${stats.totalSstables}`);
   * console.log(`Total rows: ${stats.totalRows}`);
   * console.log(`Memory: ${stats.memoryUsedBytes} bytes`);
   * ```
   */
  getStats(): Promise<DatabaseStats>;

  /**
   * Close the database and release resources.
   *
   * This method is idempotent - calling it multiple times is safe.
   * After closing, any operations on the database will throw an error.
   *
   * @returns Promise resolving when close is complete
   *
   * @example
   * ```typescript
   * const db = await Database.open('/path/to/data');
   * // ... use database ...
   * await db.close();
   * await db.close(); // Safe to call again
   * ```
   */
  close(): Promise<void>;

  /**
   * Check if the database is closed.
   *
   * @returns True if the database has been closed, false otherwise
   */
  get isClosed(): boolean;
}

// ============================================================================
// Functions
// ============================================================================

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
