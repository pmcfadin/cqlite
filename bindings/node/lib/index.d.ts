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
   * - Varint: Hex string `"0x{hex}"` (e.g., `"0x7f"` for 127)
   * - Decimal: String `"decimal:{scale}:0x{hex}"` (e.g., `"decimal:2:0x7b"` for 1.23)
   *
   * For native types with full precision, use `executeNative()`.
   *
   * @deprecated The execute() method uses legacy JSON encoding. Use executeNative() for proper type fidelity.
   */
  rows: Record<string, unknown>[];

  /** Number of rows returned. */
  rowCount: number;

  /**
   * Number of rows affected by the write (INSERT/UPDATE/DELETE).
   * 0 for SELECT queries.
   */
  rowsAffected: number;

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

  /**
   * Number of rows affected by the write (INSERT/UPDATE/DELETE).
   * 0 for SELECT queries.
   */
  rowsAffected: number;

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
 * // Read-only
 * const options: DatabaseOptions = {
 *   schema: '/path/to/schema.cql',
 *   memoryLimit: 256 * 1024 * 1024, // 256MB
 *   cacheEnabled: true
 * };
 * const db = await Database.open('/path/to/data', options);
 *
 * // Read-write
 * const db = await Database.open('/path/to/data', {
 *   schema: '/path/to/schema.cql',
 *   writable: true,
 *   writeDir: '/tmp/cqlite-writes',
 * });
 * ```
 */
export interface DatabaseOptions {
  /**
   * Path to a CQL schema file (.cql).
   * If provided, the schema will be loaded and used for query execution.
   * Required when `writable` is true.
   */
  schema?: string;

  /**
   * Maximum memory usage in bytes.
   * Minimum: 1 byte. Values less than 1 will be rejected.
   * Default: 1GB (1073741824 bytes).
   * Controls the overall memory budget for caches and internal buffers.
   *
   * @example
   * ```typescript
   * { memoryLimit: 256 * 1024 * 1024 } // 256MB
   * ```
   */
  memoryLimit?: number;

  /**
   * Enable or disable all caches (block, row, query).
   * Default: true (caches enabled).
   * Set to false to minimize memory usage at the cost of performance.
   */
  cacheEnabled?: boolean;

  /**
   * Enable write support (INSERT, UPDATE, DELETE).
   * When true, `writeDir` must also be provided and a `schema` is required.
   * Default: false (read-only mode).
   */
  writable?: boolean;

  /**
   * Directory for write-engine data (memtable flush targets and WAL files).
   * Required when `writable` is true.
   * Sub-directories `data/` and `wal/` are created automatically.
   *
   * @example
   * ```typescript
   * { writable: true, writeDir: '/tmp/cqlite-writes' }
   * ```
   */
  writeDir?: string;

  /**
   * Enable automatic (STCS) size-tiered compaction for the write engine.
   * Default: true. Set false to disable compaction — `maintenanceStep`
   * then performs no merges (issue #1619).
   */
  autoCompaction?: boolean;

  /**
   * Memtable flush threshold in bytes for the write engine (issue #1620).
   * When the in-memory memtable grows past this size, the write path
   * (`execute`) awaits a real async flush to a new SSTable generation.
   * Only meaningful when `writable` is true. Default: 64 MB (67108864 bytes).
   */
  flushThreshold?: number;

  /**
   * OpenTelemetry export options (epic #1031, issue #1040).
   *
   * When omitted, the `CQLITE_OTEL_*` environment variables are consulted.
   * Telemetry stays disabled unless `enabled: true` is set (here or via env)
   * AND the native addon was built with the `observability` Cargo feature.
   *
   * Observability is initialised **once per process** on the first
   * `Database.open()`, so passing different `otel` options to a later open has
   * no effect.
   *
   * @example
   * ```typescript
   * const db = await Database.open('/data', {
   *   schema: 'schema.cql',
   *   otel: { enabled: true, endpoint: 'http://collector:4317', protocol: 'grpc' },
   * });
   * ```
   */
  otel?: OtelOptions;

  /**
   * Incoming W3C `traceparent` header to parent this handle's per-call and
   * per-stream spans under a remote trace (distributed-tracing propagation).
   *
   * Applied as the default parent for every `execute`, `executeNative`, and
   * `executeStreaming` on the returned handle. Invalid/empty values are
   * ignored. Only meaningful when telemetry is enabled and the addon was built
   * with the `observability` feature.
   *
   * @example
   * ```typescript
   * const db = await Database.open('/data', {
   *   schema: 'schema.cql',
   *   otel: { enabled: true },
   *   traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
   * });
   * ```
   */
  traceparent?: string;
}

/**
 * OpenTelemetry export options for the Node.js bindings (epic #1031, issue
 * #1040).
 *
 * Any field left unset falls back to the corresponding `CQLITE_OTEL_*`
 * environment variable, then to the foundation default. Exporters are only
 * installed when the effective config has `enabled: true` AND the native addon
 * was built with the `observability` feature.
 */
export interface OtelOptions {
  /**
   * Master enable switch. Unset defers to `CQLITE_OTEL_ENABLED`, then `false`.
   */
  enabled?: boolean;

  /**
   * OTLP collector endpoint: a gRPC endpoint or HTTP base URL.
   * Unset defers to `CQLITE_OTEL_ENDPOINT`, then `http://localhost:4317`.
   */
  endpoint?: string;

  /**
   * Wire protocol: `"grpc"` (default) or `"http"`. Unrecognised values are
   * ignored (the default/env value is kept).
   */
  protocol?: string;

  /**
   * `service.name` resource attribute.
   * Unset defers to `CQLITE_OTEL_SERVICE_NAME`, then `cqlite`.
   */
  serviceName?: string;

  /**
   * `service.version` resource attribute.
   * Unset defers to `CQLITE_OTEL_SERVICE_VERSION`, then the crate version.
   */
  serviceVersion?: string;

  /**
   * Trace-ID-ratio sampling probability in `[0.0, 1.0]` (clamped; non-finite
   * values fall back to full sampling).
   * Unset defers to `CQLITE_OTEL_SAMPLING_RATIO`, then `1.0`.
   */
  samplingRatio?: number;

  /**
   * Exporter export timeout in milliseconds.
   * Unset defers to `CQLITE_OTEL_TIMEOUT_MS`, then `10000`.
   */
  timeoutMs?: number;
}

// ============================================================================
// Write Support
// ============================================================================

/**
 * Write engine statistics.
 *
 * Returned synchronously by `Database.writeStats`.
 * Reflects the current state of the in-memory write buffer and WAL.
 *
 * @example
 * ```typescript
 * const stats = db.writeStats;
 * console.log(`Memtable: ${stats.memtableSize} bytes, ${stats.memtableRows} rows`);
 * console.log(`L0 files: ${stats.l0Count}`);
 * ```
 */
export interface WriteStats {
  /** Current memtable size in bytes. */
  memtableSize: number;

  /** Current number of rows in the memtable. */
  memtableRows: number;

  /** Current write-ahead log (WAL) size in bytes. */
  walSize: number;

  /**
   * Number of L0 SSTable files flushed during this session.
   * Increases by 1 for each `flushRun()` call that produced data.
   */
  l0Count: number;

  /** Total bytes written to SSTables across all flushes in this session. */
  totalWritten: number;
}

/**
 * Options for `Database.maintenanceStep()`.
 *
 * Controls time-bounded background compaction.
 *
 * @example
 * ```typescript
 * const report = await db.maintenanceStep({ budgetMs: 200 });
 * ```
 */
export interface MaintenanceOptions {
  /**
   * Maximum time to spend in this maintenance step, in milliseconds.
   * Default: 100.
   */
  budgetMs?: number;
}

/**
 * Report returned by `Database.maintenanceStep()`.
 *
 * Describes progress made during one time-bounded compaction step.
 *
 * @example
 * ```typescript
 * const report = await db.maintenanceStep({ budgetMs: 100 });
 * console.log(`Merged ${report.rowsMerged} rows in ${report.timeSpentMs}ms`);
 * if (report.pendingCompaction) {
 *   console.log('More compaction work pending');
 * }
 * ```
 */
export interface MaintenanceReport {
  /** Time actually spent in the maintenance step, in milliseconds. */
  timeSpentMs: number;

  /** Number of rows merged during this step. */
  rowsMerged: number;

  /** Number of bytes written during this step. */
  bytesWritten: number;

  /**
   * Paths of SSTables produced by merges completed in this step.
   * Empty array when no merge was completed (step was partial progress).
   */
  completedMerges: string[];

  /** Whether there is more compaction work pending after this step. */
  pendingCompaction: boolean;
}

/**
 * Report returned by `Database.refresh()`.
 *
 * Describes what an explicit directory refresh applied to the database's held
 * SSTable reader set: newly present generations become queryable, removed
 * generations stop being queried, and unchanged generations keep their warm
 * parsed state (they are not re-parsed).
 *
 * @example
 * ```typescript
 * const report = await db.refresh();
 * console.log(
 *   `scanned ${report.tablesScanned} tables, ` +
 *   `+${report.readersAdded}/-${report.readersRemoved} readers`
 * );
 * ```
 */
export interface RefreshReport {
  /** Number of distinct logical tables present after the refresh. */
  tablesScanned: number;

  /** Number of SSTable generations newly opened and made queryable. */
  readersAdded: number;

  /** Number of SSTable generations dropped from the reader set. */
  readersRemoved: number;
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
 * Options for `Database.exportParquet()`.
 *
 * @example
 * ```typescript
 * await db.exportParquet(query, '/tmp/out.parquet', {
 *   rowGroupSize: 5000,
 *   compression: 'zstd',
 * });
 * ```
 */
export interface ParquetExportOptions {
  /**
   * Rows per Parquet row group.
   * Smaller groups reduce memory at some I/O cost. Default: 10000.
   */
  rowGroupSize?: number;

  /**
   * Compression codec: 'snappy' (default), 'zstd', or 'none'.
   */
  compression?: 'snappy' | 'zstd' | 'none';
}

/**
 * Streaming query result for memory-efficient processing.
 *
 * Implements `AsyncIterable<Row>` for use with `for await...of` loops.
 * Memory stays bounded by StreamingConfig settings (default ~11MB peak).
 *
 * ## Resource Cleanup
 *
 * Resources are automatically cleaned up when:
 * 1. All rows are consumed (iteration completes)
 * 2. `break` exits the loop early (calls `return()` automatically)
 * 3. `close()` is called explicitly
 * 4. An error occurs during iteration
 *
 * @example
 * ```typescript
 * // Basic streaming - no await on executeStreaming
 * const stream = db.executeStreaming('SELECT * FROM large_table');
 * for await (const row of stream) {
 *   console.log(row.name);
 *   // Memory stays bounded - only bufferSize rows in flight
 * }
 *
 * // Or use directly in for-await loop
 * for await (const row of db.executeStreaming('SELECT * FROM large_table')) {
 *   if (row.id === targetId) {
 *     break; // Resources cleaned up automatically
 *   }
 * }
 *
 * // Access metadata during streaming (available after first iteration)
 * console.log(`Received ${stream.rowsReceived} rows so far`);
 * console.log(`Columns: ${stream.columns.map(c => c.name).join(', ')}`);
 * ```
 */
export interface StreamingResult extends AsyncIterable<Row> {
  /**
   * Number of rows received so far.
   *
   * This counter increases as rows are yielded from the stream.
   * Useful for progress tracking and debugging.
   */
  readonly rowsReceived: number;

  /**
   * Column metadata for the result set.
   *
   * Contains information about each column's name, type, and nullability.
   * Returns an empty array before iteration begins. Columns are populated
   * after the first row is fetched.
   *
   * @example
   * ```typescript
   * const stream = db.executeStreaming('SELECT * FROM table');
   * console.log(stream.columns); // [] - empty before iteration
   *
   * for await (const row of stream) {
   *   console.log(stream.columns); // ColumnInfo[] - populated during iteration
   *   break;
   * }
   * ```
   */
  readonly columns: ColumnInfo[];

  /**
   * Release resources early.
   *
   * Called automatically when the iterator is exhausted or the loop exits.
   * Call explicitly to release resources before consuming all rows.
   * Safe to call multiple times - subsequent calls are no-ops.
   */
  close(): void;

  /**
   * Async iterator protocol implementation.
   *
   * Prefer using `for await...of` over calling this directly.
   */
  [Symbol.asyncIterator](): AsyncIterator<Row>;
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
// Prepared Statements
// ============================================================================

/**
 * Statistics about a prepared statement.
 *
 * Contains query plan information useful for optimization
 * and debugging query performance.
 */
export interface PreparedStatementStats {
  /** Number of parameters in the query. */
  parameterCount: number;

  /** Type of execution plan (TableScan, IndexScan, PointLookup). */
  planType: string;

  /** Estimated execution cost (relative metric for comparing plans). */
  estimatedCost: number;

  /** Estimated number of rows to be returned. */
  estimatedRows: bigint;

  /** Whether the query is cache-friendly. */
  cacheFriendly: boolean;
}

/**
 * A prepared CQL statement.
 *
 * PreparedStatement holds a pre-parsed and planned query that can be
 * inspected for metadata and statistics. Created via Database.prepare().
 */
export declare class PreparedStatement {
  /** The original CQL query text. */
  readonly query: string;

  /** Number of parameters in the query. */
  readonly parameterCount: number;

  /** Get statistics about this prepared statement. */
  stats(): PreparedStatementStats;

  /** String representation of the prepared statement. */
  toString(): string;
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
   * ## Varint and Decimal Encoding
   *
   * This method uses hex-based encoding for arbitrary precision numbers:
   * - **Varint**: `"0x{hex}"` - Two's complement big-endian hex encoding
   *   - Example: 127 -> `"0x7f"`, -1 -> `"0xff"`, 256 -> `"0x0100"`
   * - **Decimal**: `"decimal:{scale}:0x{hex}"` - Scale + hex-encoded unscaled value
   *   - Example: 1.23 (scale=2, unscaled=123) -> `"decimal:2:0x7b"`
   *
   * @param query - CQL SELECT statement to execute
   * @returns Promise resolving to QueryResult with rows and metadata
   * @throws {CqliteError} If the query fails
   * @deprecated Since 0.4.0. Use `executeNative()` for proper type fidelity.
   *
   * @example
   * ```typescript
   * // JSON path - hex encoding for varint/decimal
   * const result = await db.execute('SELECT * FROM users LIMIT 10');
   * console.log(`Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
   * // Varint column: "0x7f" for 127
   * // Decimal column: "decimal:2:0x7b" for 1.23
   *
   * // Native path (recommended) - proper types
   * const native = await db.executeNative('SELECT * FROM users');
   * // Varint: BigInt(127)
   * // Decimal: "1.23"
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
   * Re-discover the data directory and apply changes to the held reader set.
   *
   * A `Database` is a snapshot at `open()`: a Cassandra flush/compaction (or a
   * CQLite `--flush`) may add or remove SSTable generations under a warm handle,
   * and those changes become queryable only after an explicit `refresh()`. This
   * re-runs the same TOC/filename-based discovery `open()` used (no content
   * sniffing, no heuristics) and applies the diff:
   * - newly present generations become queryable,
   * - removed generations stop being queried,
   * - unchanged generations keep their warm parsed Index/Statistics/bloom state.
   *
   * In-flight queries are never affected: a scan already running completes
   * against the pre-refresh set; a query issued after this Promise resolves sees
   * the post-refresh set. The refresh is atomic and fail-closed — if any newly
   * discovered generation fails to open (e.g. a corrupt `Statistics.db`), the
   * Promise rejects and the previously held reader set is left unchanged.
   *
   * @returns Promise resolving to a RefreshReport with the applied counts
   * @throws {CqliteError} If the database is closed or a new generation fails to open
   *
   * @example
   * ```typescript
   * const report = await db.refresh();
   * console.log(`+${report.readersAdded}/-${report.readersRemoved} readers`);
   * ```
   */
  refresh(): Promise<RefreshReport>;

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

  /**
   * Execute a CQL query with streaming results.
   *
   * Returns an async iterable that yields rows one at a time, keeping memory
   * usage bounded by the `StreamingConfig` settings. Use with `for await...of`.
   *
   * Memory stays bounded by configuration (default ~11MB peak):
   * - `bufferSize`: 1024 rows in flight (~1MB)
   * - `chunkSize`: 10,000 rows per fetch chunk (~10MB)
   *
   * @param query - CQL SELECT statement to execute
   * @param config - Optional StreamingConfig for buffer/chunk sizes
   * @returns StreamingResult async iterable (iteration triggers query execution)
   * @throws {CqliteError} If the query fails (on first iteration)
   *
   * @example
   * ```typescript
   * // Basic streaming - no await needed on executeStreaming itself
   * for await (const row of db.executeStreaming('SELECT * FROM large_table')) {
   *   console.log(row.name);
   * }
   *
   * // With custom config for memory constraints
   * const config: StreamingConfig = { bufferSize: 256, chunkSize: 2500 };
   * for await (const row of db.executeStreaming(query, config)) {
   *   process(row);
   * }
   *
   * // Early termination is safe
   * for await (const row of db.executeStreaming('SELECT * FROM huge_table')) {
   *   if (row.id === targetId) {
   *     break; // Resources cleaned up automatically
   *   }
   * }
   * ```
   */
  executeStreaming(query: string, config?: StreamingConfig): StreamingResult;

  /**
   * Export the results of a CQL query to a Parquet file.
   *
   * The query runs with streaming, so arbitrarily large result sets are
   * written within bounded memory (rows are flushed to Parquet row groups
   * as they arrive). The export runs as an async task off the JavaScript
   * main thread.
   *
   * Types use the high-fidelity schema-driven Arrow mapping: date -> Date32,
   * time -> Time64(ns), decimal/varint -> Decimal128, uuid ->
   * FixedSizeBinary(16) with the Arrow UUID extension, list/set -> List,
   * map -> Map, UDT/tuple -> Struct. CQLite produces Parquet files only;
   * committing files to Iceberg/Delta table formats is out of scope.
   *
   * @param query - CQL SELECT statement to execute
   * @param path - Destination file path (created or truncated)
   * @param options - Optional row group size and compression
   * @returns Promise resolving to the number of rows written
   * @throws {CqliteError} code "CONFIG" for invalid options, "IO" for
   *         file/encoding failures, "QUERY"/"PARSE" for query failures
   *
   * @example
   * ```typescript
   * const rows = await db.exportParquet(
   *   'SELECT * FROM my_ks.my_table',
   *   '/tmp/out.parquet',
   *   { rowGroupSize: 5000, compression: 'zstd' }
   * );
   * console.log(`Exported ${rows} row(s)`);
   * ```
   */
  exportParquet(
    query: string,
    path: string,
    options?: ParquetExportOptions
  ): Promise<number>;

  /**
   * Prepare a CQL query for analysis.
   *
   * Returns a PreparedStatement that can be inspected for query plan
   * information and statistics.
   *
   * @param query - CQL SELECT statement to prepare
   * @returns Promise resolving to PreparedStatement with query plan info
   * @throws {CqliteError} If the query cannot be prepared
   */
  prepare(query: string): Promise<PreparedStatement>;

  /**
   * Flush the in-memory write buffer (memtable) to an SSTable on disk.
   *
   * Returns the path to the created Data.db file.
   * If the memtable is empty, an empty string is returned (no-op flush).
   *
   * Requires the database to have been opened with `{ writable: true }`.
   *
   * @returns Promise resolving to the Data.db path, or "" if nothing was flushed
   * @throws {CqliteError} If write support is not enabled or the flush fails
   *
   * @example
   * ```typescript
   * const db = await Database.open('/data', {
   *   schema: 'schema.cql',
   *   writable: true,
   *   writeDir: '/tmp/writes',
   * });
   * await db.execute("INSERT INTO t (id, name) VALUES (uuid(), 'Alice')");
   * const sstablePath = await db.flushRun();
   * console.log(`Flushed to: ${sstablePath}`);
   * ```
   */
  flushRun(): Promise<string>;

  /**
   * Perform time-bounded background maintenance (compaction).
   *
   * Runs incremental compaction work within the provided time budget.
   * Can be called repeatedly to drain pending compaction work.
   *
   * Requires the database to have been opened with `{ writable: true }`.
   *
   * @param options - Optional maintenance options (default budgetMs: 100)
   * @returns Promise resolving to a MaintenanceReport
   * @throws {CqliteError} If write support is not enabled or maintenance fails
   *
   * @example
   * ```typescript
   * let report: MaintenanceReport;
   * do {
   *   report = await db.maintenanceStep({ budgetMs: 100 });
   *   console.log(`Merged ${report.rowsMerged} rows`);
   * } while (report.pendingCompaction);
   * ```
   */
  maintenanceStep(options?: MaintenanceOptions): Promise<MaintenanceReport>;

  /**
   * Get current write engine statistics (synchronous getter).
   *
   * Returns a snapshot of the in-memory write buffer (memtable) and WAL state.
   *
   * Requires the database to have been opened with `{ writable: true }`.
   *
   * @returns WriteStats snapshot
   * @throws {CqliteError} If write support is not enabled
   *
   * @example
   * ```typescript
   * const stats = db.writeStats;
   * console.log(`Memtable: ${stats.memtableSize} bytes, ${stats.memtableRows} rows`);
   * console.log(`WAL: ${stats.walSize} bytes`);
   * console.log(`L0 files: ${stats.l0Count}`);
   * console.log(`Total written: ${stats.totalWritten} bytes`);
   * ```
   */
  get writeStats(): WriteStats;
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
