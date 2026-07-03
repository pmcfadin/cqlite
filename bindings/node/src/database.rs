//! Database wrapper for Node.js bindings.
//!
//! This module provides the `Database` class for Node.js access to
//! CQLite's SSTable reading and writing capabilities.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use napi_derive::napi;

use crate::error::{runtime_init_error, simple_error, to_napi_error};

#[cfg(feature = "write-support")]
use std::sync::Mutex;

/// Column metadata information.
///
/// Provides information about a column in the query result set,
/// including name, data type, and nullability.
#[derive(Clone)]
#[napi(object)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,

    /// CQL data type as a string (e.g., "Text", "Integer", "List").
    #[napi(js_name = "dataType")]
    pub data_type: String,

    /// Whether the column can contain null values.
    pub nullable: bool,

    /// Column position in the result set (0-indexed).
    pub position: u32,

    /// Original table name (for joined queries).
    #[napi(js_name = "tableName")]
    pub table_name: Option<String>,
}

impl ColumnInfo {
    /// Create ColumnInfo from core library's ColumnInfo.
    fn from_core(col: &cqlite_core::query::result::ColumnInfo) -> Self {
        Self {
            name: col.name.clone(),
            data_type: format!("{:?}", col.data_type),
            nullable: col.nullable,
            position: col.position as u32,
            table_name: col.table_name.clone(),
        }
    }
}

/// Query execution result.
///
/// Contains the query results serialized as JSON values for JavaScript
/// consumption, along with metadata about the execution.
#[napi(object)]
pub struct QueryResult {
    /// Result rows as JSON objects.
    /// Each row is a JSON object with column names as keys.
    pub rows: Vec<serde_json::Value>,

    /// Number of rows returned.
    pub row_count: u32,

    /// Number of rows affected by a write statement (INSERT/UPDATE/DELETE).
    /// For SELECT queries this equals row_count.
    pub rows_affected: u32,

    /// Query execution time in milliseconds.
    pub execution_time_ms: u32,

    /// Column metadata for the result set.
    /// Contains information about each column's name, type, and nullability.
    pub columns: Vec<ColumnInfo>,
}

/// Write engine statistics.
///
/// Returned by `Database.writeStats` (synchronous getter).
/// Reflects the current state of the in-memory write buffer and WAL.
#[napi(object)]
pub struct WriteStats {
    /// Current memtable size in bytes.
    pub memtable_size: f64,

    /// Current number of rows in the memtable.
    pub memtable_rows: u32,

    /// Current WAL (write-ahead log) size in bytes.
    pub wal_size: f64,

    /// Number of L0 SSTable files (generation count proxy).
    pub l0_count: u32,

    /// Total bytes written to SSTables since engine was opened.
    pub total_written: f64,
}

/// Maintenance step options.
///
/// Controls time-bounded background compaction behaviour.
#[napi(object)]
pub struct MaintenanceOptions {
    /// Maximum time to spend in this maintenance step, in milliseconds.
    /// Default: 100.
    pub budget_ms: Option<u32>,
}

/// Report returned by `Database.maintenanceStep()`.
#[napi(object)]
pub struct MaintenanceReport {
    /// Time actually spent in the step, in milliseconds.
    pub time_spent_ms: f64,

    /// Number of rows merged during this step.
    pub rows_merged: f64,

    /// Number of bytes written during this step.
    pub bytes_written: f64,

    /// Paths of SSTables produced by completed merges (as strings).
    pub completed_merges: Vec<String>,

    /// Whether there is pending compaction work remaining.
    pub pending_compaction: bool,
}

/// Database statistics.
///
/// Provides information about the database state including
/// storage and memory metrics.
#[napi(object)]
pub struct DatabaseStats {
    /// Total number of SSTable files.
    pub total_sstables: u32,

    /// Total number of rows across all SSTables.
    #[napi(ts_type = "bigint")]
    pub total_rows: i64,

    /// Memory currently used by the database in bytes.
    #[napi(ts_type = "bigint")]
    pub memory_used_bytes: i64,
}

/// Database open options.
///
/// Configuration options for opening a database.
#[napi(object)]
pub struct DatabaseOptions {
    /// Path to a CQL schema file (.cql).
    /// If provided, the schema will be loaded and used for query execution.
    pub schema: Option<String>,

    /// Maximum memory usage in bytes.
    /// Default: 1GB (1073741824 bytes).
    /// Controls the overall memory budget for caches and internal buffers.
    /// JavaScript numbers can safely represent up to 2^53 bytes (~9 petabytes).
    #[napi(js_name = "memoryLimit")]
    pub memory_limit: Option<f64>,

    /// Enable or disable all caches (block, row, query).
    /// Default: true (caches enabled).
    /// Set to false to minimize memory usage at the cost of performance.
    #[napi(js_name = "cacheEnabled")]
    pub cache_enabled: Option<bool>,

    /// Enable write support.
    /// When true, INSERT/UPDATE/DELETE statements will be accepted and `writeDir`
    /// must also be provided.  Default: false.
    pub writable: Option<bool>,

    /// Directory for write-engine data (memtable flush targets and WAL files).
    /// Required when `writable` is true.
    /// Sub-directories `data/` and `wal/` are created automatically.
    #[napi(js_name = "writeDir")]
    pub write_dir: Option<String>,

    /// Enable automatic (STCS) size-tiered compaction for the write engine.
    /// Default: true. Set false to disable compaction — `maintenanceStep`
    /// then performs no merges (issue #1619).
    #[napi(js_name = "autoCompaction")]
    pub auto_compaction: Option<bool>,

    /// Memtable flush threshold in bytes for the write engine (issue #1620).
    /// When the in-memory memtable grows past this size, the binding write path
    /// (`execute`) awaits a real async flush to a new SSTable generation.
    /// Only meaningful when `writable` is true. Default: 64 MB (67108864 bytes).
    /// JavaScript numbers safely represent up to 2^53 bytes.
    #[napi(js_name = "flushThreshold")]
    pub flush_threshold: Option<f64>,

    /// OpenTelemetry export options (epic #1031, issue #1040).
    ///
    /// When omitted, the `CQLITE_OTEL_*` environment variables are consulted;
    /// telemetry stays disabled unless `enabled: true` is set (here or via env)
    /// AND the binding was built with the `observability` feature. The
    /// foundation initialises ONCE per process on the first `open()`, so passing
    /// `otel` on a later open has no effect.
    pub otel: Option<crate::observability::OtelOptions>,

    /// Incoming W3C `traceparent` header to parent this database's per-call and
    /// per-stream spans under a remote trace (distributed-tracing propagation).
    ///
    /// Applied as the default parent for every `execute`/`executeNative`/
    /// `executeStreaming` issued on this handle. Invalid/empty values are
    /// ignored. Only meaningful when telemetry is enabled and the
    /// `observability` feature is built.
    pub traceparent: Option<String>,
}

/// Configuration for streaming query execution.
///
/// Controls memory usage during large result set iteration.
/// Used with `executeStreaming()` for memory-efficient processing
/// of large result sets.
///
/// ## Example
///
/// ```javascript
/// const config = { bufferSize: 512, chunkSize: 5000 };
/// for await (const row of db.executeStreaming(query, config)) {
///   console.log(row);
/// }
/// ```
///
/// ## Memory Budget
///
/// Default values (~11MB peak usage):
/// - bufferSize: 1024 rows × ~1KB = ~1MB in flight
/// - chunkSize: 10000 rows × ~1KB = ~10MB per chunk
///
/// For rows with large blobs, reduce buffer sizes proportionally.
#[napi(object)]
pub struct StreamingConfig {
    /// Number of rows to buffer in memory during streaming.
    /// Controls backpressure. Default: 1024.
    #[napi(js_name = "bufferSize")]
    pub buffer_size: Option<u32>,

    /// Number of rows per fetch chunk from storage.
    /// Larger chunks improve throughput, smaller chunks reduce memory.
    /// Default: 10000.
    #[napi(js_name = "chunkSize")]
    pub chunk_size: Option<u32>,
}

impl StreamingConfig {
    /// Convert to core StreamingConfig with validation.
    ///
    /// Applies default values and validates that both buffer_size
    /// and chunk_size are greater than 0.
    pub fn to_core(&self) -> napi::Result<cqlite_core::query::result::StreamingConfig> {
        let buffer_size = self.buffer_size.unwrap_or(1024);
        let chunk_size = self.chunk_size.unwrap_or(10_000);

        if buffer_size == 0 {
            return Err(napi::Error::from_reason(
                "bufferSize must be greater than 0",
            ));
        }
        if chunk_size == 0 {
            return Err(napi::Error::from_reason("chunkSize must be greater than 0"));
        }

        Ok(cqlite_core::query::result::StreamingConfig {
            buffer_size: buffer_size as usize,
            chunk_size: chunk_size as usize,
        })
    }

    /// Create a StreamingConfig with default values.
    pub fn with_defaults() -> Self {
        StreamingConfig {
            buffer_size: Some(1024),
            chunk_size: Some(10_000),
        }
    }
}

/// Options for `exportParquet()`.
///
/// ## Example
///
/// ```javascript
/// await db.exportParquet(query, '/tmp/out.parquet', {
///   rowGroupSize: 5000,
///   compression: 'zstd',
/// });
/// ```
#[napi(object)]
#[derive(Default)]
pub struct ParquetExportOptions {
    /// Rows per Parquet row group. Default: 10000.
    #[napi(js_name = "rowGroupSize")]
    pub row_group_size: Option<u32>,

    /// Compression codec: "snappy" (default), "zstd", or "none".
    pub compression: Option<String>,
}

impl ParquetExportOptions {
    /// Convert to core export options with validation.
    ///
    /// Validation failures map to CONFIG-coded errors (ValueError prefix)
    /// via the standard error metadata channel.
    fn to_core(&self) -> napi::Result<cqlite_core::export::parquet::ParquetExportOptions> {
        use cqlite_core::export::parquet::ParquetCompression;

        let row_group_size = self.row_group_size.unwrap_or(10_000);
        if row_group_size == 0 {
            return Err(to_napi_error(cqlite_core::Error::Configuration(
                "rowGroupSize must be greater than 0".to_string(),
            )));
        }

        let compression = match self.compression.as_deref() {
            None => ParquetCompression::Snappy,
            Some(c) => match c.to_ascii_lowercase().as_str() {
                "snappy" => ParquetCompression::Snappy,
                "zstd" => ParquetCompression::Zstd,
                "none" | "uncompressed" => ParquetCompression::Uncompressed,
                other => {
                    return Err(to_napi_error(cqlite_core::Error::Configuration(format!(
                        "unknown compression '{other}'; expected 'snappy', 'zstd', or 'none'"
                    ))))
                }
            },
        };

        Ok(cqlite_core::export::parquet::ParquetExportOptions {
            row_limit: None,
            row_group_size: row_group_size as usize,
            compression,
        })
    }
}

/// A CQLite database handle.
///
/// Use `Database.open()` to create a Database instance.
/// Always close the database when done to release resources.
///
/// ## Example
///
/// ```javascript
/// const db = await Database.open('/path/to/data', { schema: '/path/to/schema.cql' });
/// try {
///   const result = await db.execute('SELECT * FROM users LIMIT 10');
///   console.log(`Got ${result.rowCount} rows`);
/// } finally {
///   await db.close();
/// }
/// ```
///
/// ## Write Support
///
/// Pass `{ writable: true, writeDir: '/path/to/write-dir' }` to enable writes:
///
/// ```javascript
/// const db = await Database.open('/path/to/data', {
///   schema: '/path/to/schema.cql',
///   writable: true,
///   writeDir: '/tmp/cqlite-writes',
/// });
/// await db.execute("INSERT INTO users (id, name) VALUES (uuid(), 'Alice')");
/// const path = await db.flushRun();
/// ```
///
/// ## Thread Safety
///
/// Database handles are thread-safe and can be shared across worker threads.
/// The `close()` method is idempotent - calling it multiple times is safe.
/// The write engine is protected by an Arc<Mutex> and only one write can proceed at a time.
#[napi]
pub struct Database {
    pub(crate) inner: Arc<cqlite_core::Database>,
    closed: AtomicBool,
    /// Default incoming W3C `traceparent` for this handle's per-call spans
    /// (issue #1040). `None` when not supplied or invalid.
    traceparent: Option<String>,
    /// Write engine, present only when `writable: true` was supplied to `open()`.
    /// Wrapped in Arc so it can be shared with async tasks.
    #[cfg(feature = "write-support")]
    write_engine: Option<Arc<Mutex<cqlite_core::storage::write_engine::WriteEngine>>>,
}

impl Database {
    /// Check if database is open, returning error if closed.
    pub(crate) fn ensure_open(&self) -> napi::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            Err(simple_error("Database is closed"))
        } else {
            Ok(())
        }
    }

    /// Check that write support is enabled, returning a clear error if not.
    #[cfg(feature = "write-support")]
    fn ensure_writable(&self) -> napi::Result<()> {
        if self.write_engine.is_none() {
            Err(simple_error(
                "Write support not enabled. \
                 Open the database with { writable: true, writeDir: '<path>' } to enable write operations.",
            ))
        } else {
            Ok(())
        }
    }

    /// Determine whether a CQL statement is a write operation.
    ///
    /// Deliberately NOT feature-gated: the read-path entry points must be able to
    /// detect DML even when the `write-support` feature is compiled out, so they
    /// can fail closed (return an explicit error) instead of silently handing an
    /// `INSERT`/`UPDATE`/`DELETE` to the read engine — which would return a
    /// read-shaped, empty result and never persist the row (issue #1460).
    fn is_dml_statement(query: &str) -> bool {
        let upper = query.trim_start().to_uppercase();
        upper.starts_with("INSERT")
            || upper.starts_with("UPDATE")
            || upper.starts_with("DELETE")
            || upper.starts_with("BEGIN")
    }

    /// Error returned when a DML statement is issued against a binary that was
    /// built WITHOUT the `write-support` feature. Failing closed here is the
    /// whole point of issue #1460: without this guard the DML string falls
    /// through to the read engine and silently no-ops (no write, no error).
    #[cfg(not(feature = "write-support"))]
    fn dml_unsupported_error() -> napi::Error {
        simple_error(
            "Write support is not compiled into this build of @cqlite/node. \
             DML statements (INSERT/UPDATE/DELETE) cannot be executed and will \
             NOT be silently ignored. Rebuild the native module with \
             `--features write-support`.",
        )
    }
}

#[napi]
impl Database {
    /// Opens a database at the specified data directory.
    ///
    /// @param dataDir - Path to the SSTable data directory
    /// @param options - Optional configuration (schema path, writable, writeDir, etc.)
    /// @returns Promise resolving to a Database instance
    ///
    /// @example
    /// ```javascript
    /// // Basic open (read-only)
    /// const db = await Database.open('/path/to/sstables');
    ///
    /// // With schema file
    /// const db = await Database.open('/path/to/sstables', {
    ///   schema: '/path/to/schema.cql'
    /// });
    ///
    /// // With write support enabled
    /// const db = await Database.open('/path/to/sstables', {
    ///   schema: '/path/to/schema.cql',
    ///   writable: true,
    ///   writeDir: '/tmp/cqlite-writes',
    /// });
    /// ```
    #[napi(factory)]
    pub async fn open(
        data_dir: String,
        options: Option<DatabaseOptions>,
    ) -> napi::Result<Database> {
        let path = PathBuf::from(&data_dir);

        // Initialise observability ONCE per process from the open options (or the
        // CQLITE_OTEL_* env fallback). Later opens are no-ops; the first wins.
        // This must run before any span is created below so the OTel layer is
        // composed in. Safe + inert when telemetry is disabled or the feature is
        // off (issue #1040).
        crate::observability::init_once(options.as_ref().and_then(|o| o.otel.as_ref()));

        // Eagerly build the shared tokio runtime so a resource-starved host
        // (out of threads/FDs/memory) surfaces a catchable error AT open()
        // rather than only later on the first executeNative/streaming next/flush
        // block_on (issue #1438). On success the runtime is memoized and reused;
        // this call is a cheap OnceLock fast-path on every subsequent open.
        crate::runtime::try_get_runtime().map_err(runtime_init_error)?;

        // Per-handle default traceparent for this database's per-call spans.
        let traceparent = options
            .as_ref()
            .and_then(|o| o.traceparent.clone())
            .filter(|t| !t.trim().is_empty());

        // Extract all options and build config
        let (schema_path, core_config, writable, write_dir, flush_threshold) = if let Some(opts) =
            options
        {
            let mut config = cqlite_core::Config::default();

            if let Some(limit) = opts.memory_limit {
                if !limit.is_finite() {
                    return Err(napi::Error::from_reason(
                        "memoryLimit must be a finite number",
                    ));
                }
                if limit < 1.0 {
                    return Err(napi::Error::from_reason(
                        "memoryLimit must be at least 1 byte",
                    ));
                }
                config.memory.max_memory = limit as u64;
            }

            if let Some(enabled) = opts.cache_enabled {
                config.memory.block_cache.enabled = enabled;
                config.memory.row_cache.enabled = enabled;
                config.memory.query_cache.enabled = enabled;
            }

            if let Some(ac) = opts.auto_compaction {
                config.storage.compaction.auto_compaction = ac;
            }

            let writable = opts.writable.unwrap_or(false);
            let write_dir = opts.write_dir.map(PathBuf::from);

            // Validate the optional flush threshold (issue #1620). Bytes as an
            // f64 (napi-idiomatic; matches `memoryLimit`). Must be finite, at
            // least 1 byte, and no greater than the memtable hard limit;
            // converted to `usize` at config-build time.
            let flush_threshold = match opts.flush_threshold {
                Some(v) => {
                    if !v.is_finite() {
                        return Err(napi::Error::from_reason(
                            "flushThreshold must be a finite number",
                        ));
                    }
                    if v < 1.0 {
                        return Err(napi::Error::from_reason(
                            "flushThreshold must be at least 1 byte",
                        ));
                    }
                    // A threshold above the hard limit would never trigger an
                    // auto-flush: the memtable hits the hard limit and rejects
                    // writes first, dead-ending the binding write path
                    // (roborev jobs 2885/2890, issue #1620). Gated on
                    // `write-support`, matching every other `write_engine`
                    // reference in this file — `WriteEngineConfig` is
                    // `#[cfg(feature = "write-support")]` in core, and the
                    // threshold is only meaningful when the write engine exists.
                    #[cfg(feature = "write-support")]
                    {
                        let hard_limit =
                            cqlite_core::storage::write_engine::WriteEngineConfig::DEFAULT_HARD_LIMIT;
                        if v > hard_limit as f64 {
                            return Err(napi::Error::from_reason(format!(
                                "flushThreshold ({v} bytes) must not exceed the memtable hard limit ({hard_limit} bytes)"
                            )));
                        }
                    }
                    Some(v)
                }
                None => None,
            };

            (
                opts.schema.map(PathBuf::from),
                config,
                writable,
                write_dir,
                flush_threshold,
            )
        } else {
            (None, cqlite_core::Config::default(), false, None, None)
        };

        // Clone schema path before it is potentially consumed by db open, so the
        // write engine initializer can also read the same CQL file.
        #[cfg(feature = "write-support")]
        let schema_path_for_write: Option<PathBuf> = schema_path.clone();

        // Capture the compaction settings before `core_config` is moved into
        // ingestion / Database::open, so `Config.storage.compaction` is
        // authoritative for the write path (issue #1619) rather than decorative.
        #[cfg(feature = "write-support")]
        let compaction_config = core_config.storage.compaction.clone();

        // Validate write options
        #[cfg(feature = "write-support")]
        if writable && write_dir.is_none() {
            return Err(napi::Error::from_reason(
                "writeDir is required when writable is true",
            ));
        }

        let db = if let Some(schema) = schema_path {
            // Use ingestion module for schema + SSTable discovery
            let ingestion_config = cqlite_core::ingestion::IngestionConfig {
                schema_paths: vec![schema],
                data_dir: path,
                version_hint: None,
                core_config,
                table_directory_filter: None,
            };

            let result = cqlite_core::ingestion::ingest(ingestion_config)
                .await
                .map_err(to_napi_error)?;

            result.database
        } else {
            // Simple open without schema
            cqlite_core::Database::open(&path, core_config)
                .await
                .map_err(to_napi_error)?
        };

        // Build write engine if requested
        #[cfg(feature = "write-support")]
        let write_engine_opt: Option<
            Arc<Mutex<cqlite_core::storage::write_engine::WriteEngine>>,
        > = if writable {
            let wd = write_dir
                .as_ref()
                .expect("write_dir validated above to be Some when writable");

            // We need a TableSchema for the write engine.
            // Use the ingestion result schema registry if available, otherwise require
            // schema_path to be provided and parse it directly (mirrors CLI write-only mode).
            let schema = if let Some(ref sp) = schema_path_for_write {
                // Parse schema directly from CQL file (same as CLI write-only mode).
                // Mirrors the logic in cqlite-cli/src/main.rs that extracts keyspace
                // from USE/CREATE KEYSPACE statements before applying to table schemas.
                use cqlite_core::schema::cql_parser::{
                    classify_statement, parse_create_table, split_cql_statements, StatementType,
                };
                let content = std::fs::read_to_string(sp).map_err(|e| {
                    napi::Error::from_reason(format!(
                        "Failed to read schema file '{}': {}",
                        sp.display(),
                        e
                    ))
                })?;
                let statements = split_cql_statements(&content);

                // Pass 1: collect keyspace from USE / CREATE KEYSPACE statements
                // and all table schemas (with keyspace applied).
                let mut file_keyspace: Option<String> = None;
                let mut table_schemas: Vec<cqlite_core::schema::TableSchema> = Vec::new();

                for stmt in &statements {
                    match classify_statement(stmt) {
                        StatementType::Other(ref kind) if kind == "use" => {
                            // Extract keyspace from USE <keyspace>;
                            let name = stmt
                                .trim()
                                .strip_prefix("USE")
                                .or_else(|| stmt.trim().strip_prefix("use"))
                                .unwrap_or("")
                                .trim()
                                .trim_end_matches(';')
                                .trim()
                                .to_string();
                            if !name.is_empty() {
                                file_keyspace = Some(name);
                            }
                        }
                        StatementType::Other(ref kind) if kind == "create" => {
                            // Extract keyspace from CREATE KEYSPACE IF NOT EXISTS <name>
                            let lower = stmt.to_lowercase();
                            if lower.contains("create keyspace") {
                                let after = if let Some(pos) = lower.find("exists") {
                                    &stmt[pos + 6..]
                                } else if let Some(pos) = lower.find("keyspace") {
                                    &stmt[pos + 8..]
                                } else {
                                    ""
                                };
                                let name = after
                                    .trim()
                                    .split(|c: char| c.is_whitespace() || c == '{' || c == ';')
                                    .next()
                                    .unwrap_or("")
                                    .trim()
                                    .to_string();
                                if !name.is_empty() {
                                    file_keyspace = Some(name);
                                }
                            }
                        }
                        StatementType::CreateTable => {
                            if let Ok((_remaining, mut ts)) = parse_create_table(stmt) {
                                // Apply file-level keyspace if table doesn't have one yet
                                if ts.keyspace.is_empty()
                                    || ts.keyspace == "unknown"
                                    || ts.keyspace == "default"
                                {
                                    if let Some(ref ks) = file_keyspace {
                                        ts.keyspace = ks.clone();
                                    }
                                }
                                table_schemas.push(ts);
                            }
                        }
                        _ => {}
                    }
                }

                // Enforce single-table write target (Issue #28 no-heuristics mandate).
                // Silently picking one table would hide ambiguity; require callers to
                // provide a schema file with exactly one CREATE TABLE statement.
                match table_schemas.len() {
                    0 => {
                        return Err(napi::Error::from_reason(format!(
                            "No CREATE TABLE statement found in schema file '{}'",
                            sp.display()
                        )));
                    }
                    1 => table_schemas.into_iter().next().expect("length is 1"),
                    count => {
                        return Err(napi::Error::from_reason(format!(
                            "Schema file '{}' contains {} CREATE TABLE statements. \
                             The Node bindings currently support a single-table write \
                             target. Specify a schema with exactly one CREATE TABLE.",
                            sp.display(),
                            count
                        )));
                    }
                }
            } else {
                return Err(napi::Error::from_reason(
                    "A schema file (option `schema`) is required when `writable` is true.",
                ));
            };

            let mut config = cqlite_core::storage::write_engine::WriteEngineConfig::new(
                wd.join("data"),
                wd.join("wal"),
                schema,
            )
            .with_compaction_config(&compaction_config);

            // Apply the optional flush threshold (issue #1620); default is the
            // engine's 64 MB when not provided.
            if let Some(v) = flush_threshold {
                config = config.with_flush_threshold(v as usize);
            }

            let engine = cqlite_core::storage::write_engine::WriteEngine::new(config)
                .map_err(to_napi_error)?;
            Some(Arc::new(Mutex::new(engine)))
        } else {
            None
        };

        #[cfg(not(feature = "write-support"))]
        let _ = (writable, write_dir, flush_threshold); // suppress unused warning when feature off

        // Eagerly build (and memoize) the shared async runtime so a resource-
        // starved host surfaces the failure HERE at open() time as a catchable
        // napi::Error, rather than deferring it to the first executeNative() /
        // flushRun() / stream iteration that reaches `block_on` (issue #1438).
        // Idempotent — later calls reuse the memoized runtime.
        crate::runtime::try_get_runtime().map_err(runtime_init_error)?;

        Ok(Database {
            inner: Arc::new(db),
            closed: AtomicBool::new(false),
            traceparent,
            #[cfg(feature = "write-support")]
            write_engine: write_engine_opt,
        })
    }

    /// Execute a CQL query or write statement and return results.
    ///
    /// For SELECT queries, returns matching rows.
    /// For INSERT/UPDATE/DELETE, executes the write and returns `rowsAffected`.
    /// For large result sets, consider using streaming via `executeStreaming()`.
    ///
    /// @param query - CQL statement to execute
    /// @returns Promise resolving to QueryResult with rows and metadata
    ///
    /// @example
    /// ```javascript
    /// // Read
    /// const result = await db.execute('SELECT * FROM users LIMIT 10');
    /// console.log(`Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
    ///
    /// // Write (requires writable: true in open options)
    /// const wr = await db.execute("INSERT INTO users (id, name) VALUES (uuid(), 'Alice')");
    /// console.log(`Rows affected: ${wr.rowsAffected}`);
    /// ```
    #[napi]
    pub async fn execute(&self, query: String) -> napi::Result<QueryResult> {
        use tracing::Instrument;

        self.ensure_open()?;

        // Per-call span (issue #1040), parented under the handle's traceparent
        // when one was supplied. We never hold a span guard across `.await`; the
        // async work is `.instrument(span)`-ed instead.
        let span = crate::observability::execute_span("execute", self.traceparent.as_deref());
        let span_for_record = span.clone();

        async move {
            // Route DML statements to write engine when write support is compiled
            // in. Use spawn_blocking so the Mutex lock + synchronous
            // engine.execute() call does not stall the napi async executor thread.
            #[cfg(feature = "write-support")]
            if Self::is_dml_statement(&query) {
                self.ensure_writable()?;
                let we_clone = Arc::clone(
                    self.write_engine
                        .as_ref()
                        .expect("ensure_writable verified write_engine is Some"),
                );
                let (elapsed_ms, applied) = tokio::task::spawn_blocking(move || {
                    let start = std::time::Instant::now();
                    let mut engine = we_clone
                        .lock()
                        .map_err(|_| simple_error("Write engine lock poisoned"))?;
                    // Drive the async-flushing write path to completion while the
                    // engine Mutex is held (issue #1620). This restores auto-flush
                    // in the runtime-present binding topology; the plain sync
                    // `execute()` skips it and would grow the memtable to the hard
                    // limit. Returns the number of mutations applied.
                    let n = crate::runtime::block_on(engine.execute_flushing(&query))
                        .map_err(to_napi_error)?;
                    Ok::<(u32, u64), napi::Error>((start.elapsed().as_millis() as u32, n))
                })
                .await
                .map_err(|e| simple_error(format!("execute DML task panicked: {e}")))??;
                crate::observability::record_rows(&span_for_record, 0);
                return Ok(QueryResult {
                    rows: vec![],
                    row_count: 0,
                    rows_affected: applied as u32,
                    execution_time_ms: elapsed_ms,
                    columns: vec![],
                });
            }

            // Fail closed: without the write-support feature, a DML statement must
            // NOT fall through to the read engine (issue #1460).
            #[cfg(not(feature = "write-support"))]
            if Self::is_dml_statement(&query) {
                return Err(Self::dml_unsupported_error());
            }

            let core_result = self.inner.execute(&query).await.map_err(|e| {
                // Boundary error: record once here (subsystem = "node"), not in
                // nested helpers, to avoid double counting with core.
                crate::observability::record_boundary_error(&e);
                to_napi_error(e)
            })?;

            // Convert rows to JSON values
            let rows: Vec<serde_json::Value> = core_result
                .rows
                .iter()
                .map(|row| {
                    #[allow(deprecated)]
                    let obj: serde_json::Map<String, serde_json::Value> = row
                        .values
                        .iter()
                        .map(|(k, v)| (k.to_string(), value_to_json(v)))
                        .collect();
                    serde_json::Value::Object(obj)
                })
                .collect();

            // Convert column metadata
            let columns: Vec<ColumnInfo> = core_result
                .metadata
                .columns
                .iter()
                .map(ColumnInfo::from_core)
                .collect();

            let row_count = rows.len() as u32;
            crate::observability::record_rows(&span_for_record, row_count as u64);
            Ok(QueryResult {
                rows_affected: row_count,
                row_count,
                rows,
                execution_time_ms: core_result.execution_time_ms as u32,
                columns,
            })
        }
        .instrument(span)
        .await
    }

    /// Get database statistics.
    ///
    /// Returns information about storage, memory usage, and other metrics.
    ///
    /// @returns Promise resolving to DatabaseStats
    ///
    /// @example
    /// ```javascript
    /// const stats = await db.getStats();
    /// console.log(`SSTables: ${stats.totalSstables}`);
    /// console.log(`Total rows: ${stats.totalRows}`);
    /// console.log(`Memory: ${stats.memoryUsedBytes} bytes`);
    /// ```
    #[napi(js_name = "getStats")]
    pub async fn get_stats(&self) -> napi::Result<DatabaseStats> {
        self.ensure_open()?;

        let core_stats = self.inner.stats().await.map_err(to_napi_error)?;

        Ok(DatabaseStats {
            total_sstables: core_stats.storage_stats.sstables.sstable_count as u32,
            total_rows: core_stats.storage_stats.sstables.total_entries as i64,
            memory_used_bytes: core_stats.memory_stats.total_memory_used as i64,
        })
    }

    /// Close the database and release resources.
    ///
    /// This method is idempotent - calling it multiple times is safe.
    /// After closing, any operations on the database will throw an error.
    ///
    /// @returns Promise resolving when close is complete
    ///
    /// @example
    /// ```javascript
    /// const db = await Database.open('/path/to/data');
    /// // ... use database ...
    /// await db.close();
    /// await db.close(); // Safe to call again
    /// ```
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        // Atomically set closed flag, return early if already closed
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Shutdown the storage engine to release resources
        self.inner.shutdown().await.map_err(to_napi_error)?;

        // Flush buffered telemetry promptly on a graceful close (issue #1040)
        // rather than waiting for the process-exit Drop of the global guard.
        // No-op when telemetry is disabled / the feature is off.
        crate::observability::flush();

        Ok(())
    }

    /// Check if the database is closed.
    ///
    /// @returns True if the database has been closed, false otherwise
    #[napi(getter)]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Execute a CQL query with streaming results.
    ///
    /// Returns a `StreamingResult` that yields rows one at a time for memory-efficient
    /// processing of large result sets. Use with JavaScript's `for await...of` loop.
    ///
    /// Memory stays bounded by `StreamingConfig` settings (default ~11MB peak):
    /// - `bufferSize`: 1024 rows in flight
    /// - `chunkSize`: 10,000 rows per fetch chunk
    ///
    /// @param query - CQL SELECT statement to execute
    /// @param config - Optional StreamingConfig for buffer/chunk sizes
    /// @returns StreamingResult async iterable (JS wrapper makes this sync)
    ///
    /// Note: The native Rust layer returns a Promise, but the JavaScript wrapper
    /// in error-wrapper.js converts this to a synchronous return of AsyncIterable,
    /// per M4 spec requirement (Issue #347).
    ///
    /// @example
    /// ```javascript
    /// // No await on executeStreaming - returns AsyncIterable directly
    /// for await (const row of db.executeStreaming('SELECT * FROM large_table')) {
    ///   console.log(row.name);
    /// }
    ///
    /// // With custom config for memory constraints
    /// const config = { bufferSize: 256, chunkSize: 2500 };
    /// for await (const row of db.executeStreaming(query, config)) {
    ///   process(row);
    /// }
    ///
    /// // Early termination is safe - resources cleaned up automatically
    /// for await (const row of db.executeStreaming('SELECT * FROM huge_table')) {
    ///   if (row.id === targetId) {
    ///     break;
    ///   }
    /// }
    /// ```
    #[napi(js_name = "executeStreaming")]
    pub async fn execute_streaming(
        &self,
        query: String,
        config: Option<StreamingConfig>,
    ) -> napi::Result<crate::streaming::StreamingResult> {
        use tracing::Instrument;

        self.ensure_open()?;

        // Per-stream span (issue #1040). It is handed to the StreamingResult so
        // rows yielded across `next()` iterations accumulate onto it, and it is
        // finalised when iteration ends or the result is closed.
        let span = crate::observability::streaming_span(self.traceparent.as_deref());

        // Convert config or use defaults
        let core_config = match config {
            Some(c) => c.to_core()?,
            None => cqlite_core::query::result::StreamingConfig::default(),
        };

        // Execute streaming query via core library. The setup is instrumented by
        // the stream span (no guard held across `.await`).
        let span_for_iter = span.clone();
        let iter = async move {
            self.inner
                .execute_streaming(&query, core_config)
                .await
                .map_err(|e| {
                    crate::observability::record_boundary_error(&e);
                    to_napi_error(e)
                })
        }
        .instrument(span)
        .await?;

        // Create StreamingResult with shared runtime, carrying the stream span.
        crate::streaming::StreamingResult::new(iter, span_for_iter)
    }

    /// Export the results of a CQL query to a Parquet file.
    ///
    /// The query runs with streaming, so arbitrarily large result sets are
    /// written within bounded memory (rows are flushed to Parquet row groups
    /// as they arrive). The export runs as an async task off the JavaScript
    /// main thread.
    ///
    /// Types use the high-fidelity schema-driven mapping (Date32, Time64,
    /// Decimal128, FixedSizeBinary(16) + UUID extension, typed List/Map,
    /// Struct for UDTs/tuples). CQLite produces Parquet files only;
    /// committing files to Iceberg/Delta is an external committer's job.
    ///
    /// @param query - CQL SELECT statement to execute
    /// @param path - Destination file path (created or truncated)
    /// @param options - Optional rowGroupSize (default 10000) and
    ///                  compression ("snappy" | "zstd" | "none")
    /// @returns Promise resolving to the number of rows written
    ///
    /// @example
    /// ```javascript
    /// const rows = await db.exportParquet(
    ///   'SELECT * FROM my_ks.my_table',
    ///   '/tmp/out.parquet',
    ///   { rowGroupSize: 5000, compression: 'zstd' }
    /// );
    /// console.log(`Exported ${rows} row(s)`);
    /// ```
    #[napi(js_name = "exportParquet")]
    pub async fn export_parquet(
        &self,
        query: String,
        path: String,
        options: Option<ParquetExportOptions>,
    ) -> napi::Result<i64> {
        use cqlite_core::export::parquet::StreamingParquetWriter;

        self.ensure_open()?;

        let core_options = options.unwrap_or_default().to_core()?;
        let row_group_size = core_options.row_group_size;

        let mut iter = self
            .inner
            .execute_streaming(
                &query,
                cqlite_core::query::result::StreamingConfig::default(),
            )
            .await
            .map_err(to_napi_error)?;

        // Writer failures map through cqlite_core::Error::Io so they carry
        // the standard code/category/isRecoverable metadata (code = "IO"),
        // matching the CLI's historical mapping of Parquet errors.
        let map_writer_err = |e: cqlite_core::export::parquet::ParquetExportError| {
            to_napi_error(cqlite_core::Error::Io(std::io::Error::other(e.to_string())))
        };

        let file =
            std::fs::File::create(&path).map_err(|e| to_napi_error(cqlite_core::Error::Io(e)))?;

        let mut writer = StreamingParquetWriter::new(file, &iter.metadata, &core_options)
            .map_err(map_writer_err)?;

        let mut chunk: Vec<cqlite_core::query::QueryRow> =
            Vec::with_capacity(row_group_size.min(10_000));
        while let Some(row) = iter.next_async().await {
            chunk.push(row.map_err(to_napi_error)?);
            if chunk.len() >= row_group_size {
                writer.write_chunk(&chunk).map_err(map_writer_err)?;
                chunk.clear();
            }
        }
        if !chunk.is_empty() {
            writer.write_chunk(&chunk).map_err(map_writer_err)?;
        }
        writer.finalize().map_err(map_writer_err)?;

        Ok(writer.rows_written() as i64)
    }

    /// Execute a CQL query or write statement and return results with native JavaScript types.
    ///
    /// This method returns native JavaScript types instead of JSON:
    /// - BigInt for bigint/counter columns (preserves 64-bit precision)
    /// - Buffer for blob columns
    /// - Date for timestamp/date columns
    /// - Set for set columns
    /// - Map for map columns
    ///
    /// For INSERT/UPDATE/DELETE, `rowsAffected` is set to 1 and `rows` is empty.
    ///
    /// @param query - CQL statement to execute
    /// @returns Promise resolving to NativeQueryResult with native typed rows
    ///
    /// @example
    /// ```javascript
    /// const result = await db.executeNative('SELECT * FROM users LIMIT 10');
    /// console.log(`Got ${result.rowCount} rows`);
    /// for (const row of result.rows) {
    ///   // row.id is a BigInt if the column is bigint type
    ///   // row.created_at is a Date if the column is timestamp
    ///   // row.data is a Buffer if the column is blob
    ///   console.log(row.name, typeof row.id);
    /// }
    ///
    /// // Write (requires writable: true in open options)
    /// const wr = await db.executeNative("INSERT INTO users (id, name) VALUES (uuid(), 'Alice')");
    /// console.log(`Rows affected: ${wr.rowsAffected}`);
    /// ```
    #[napi(
        js_name = "executeNative",
        ts_return_type = "Promise<{rows: object[], rowCount: number, rowsAffected: number, executionTimeMs: number, columns: ColumnInfo[]}>"
    )]
    pub fn execute_native(
        &self,
        query: String,
    ) -> napi::Result<napi::bindgen_prelude::AsyncTask<ExecuteNativeTask>> {
        self.ensure_open()?;

        // For DML, check write engine availability before creating the task
        #[cfg(feature = "write-support")]
        if Self::is_dml_statement(&query) {
            self.ensure_writable()?;
        }

        // Fail closed: without the write-support feature, a DML statement must
        // NOT fall through to the read engine (issue #1460).
        #[cfg(not(feature = "write-support"))]
        if Self::is_dml_statement(&query) {
            return Err(Self::dml_unsupported_error());
        }

        Ok(napi::bindgen_prelude::AsyncTask::new(ExecuteNativeTask {
            inner: self.inner.clone(),
            query,
            traceparent: self.traceparent.clone(),
            #[cfg(feature = "write-support")]
            write_engine: self.write_engine.clone(),
        }))
    }

    /// Prepare a CQL query for analysis.
    ///
    /// Returns a PreparedStatement that can be inspected for query plan
    /// information and statistics.
    #[napi]
    pub async fn prepare(&self, query: String) -> napi::Result<crate::prepared::PreparedStatement> {
        self.ensure_open()?;
        let prepared = self.inner.prepare(&query).await.map_err(to_napi_error)?;
        Ok(crate::prepared::PreparedStatement::new(prepared))
    }

    /// Flush the in-memory write buffer (memtable) to an SSTable on disk.
    ///
    /// Returns the path to the created Data.db file.  If the memtable is empty
    /// an empty string is returned (no-op flush).
    ///
    /// Requires the database to have been opened with `{ writable: true }`.
    ///
    /// @returns Promise resolving to the Data.db path, or "" if nothing was flushed
    /// @throws {CqliteError} If write support is not enabled or the flush fails
    ///
    /// @example
    /// ```javascript
    /// const db = await Database.open('/data', { schema: 'schema.cql', writable: true, writeDir: '/tmp/w' });
    /// await db.execute("INSERT INTO t (id) VALUES (1)");
    /// const sstablePath = await db.flushRun();
    /// console.log(`Flushed to: ${sstablePath}`);
    /// ```
    #[napi(js_name = "flushRun")]
    pub async fn flush_run(&self) -> napi::Result<String> {
        self.ensure_open()?;

        #[cfg(feature = "write-support")]
        {
            self.ensure_writable()?;

            let we = self
                .write_engine
                .as_ref()
                .expect("ensure_writable verified write_engine is Some");

            // `flush()` is async and takes &mut self on the engine.
            // We hold the Mutex lock and block_on inside a spawn_blocking to avoid
            // blocking the napi async executor thread.
            let we_clone = Arc::clone(we);

            let result = tokio::task::spawn_blocking(move || {
                let mut engine = we_clone
                    .lock()
                    .map_err(|_| simple_error("Write engine lock poisoned"))?;
                crate::runtime::block_on(engine.flush())
                    .map_err(runtime_init_error)?
                    .map_err(to_napi_error)
            })
            .await
            .map_err(|e| simple_error(format!("flush_run task panicked: {e}")))??;

            // Flush statistics (l0Count, totalWritten) are read straight from the
            // engine's own counters in `writeStats` (issue #1620), so there are no
            // Node-side counters to update here.
            match result {
                Some(info) => Ok(info.data_path.to_string_lossy().into_owned()),
                None => Ok(String::new()),
            }
        }

        #[cfg(not(feature = "write-support"))]
        Err(simple_error(
            "Write support not enabled. Build with --features write-support to enable write operations.",
        ))
    }

    /// Perform time-bounded background maintenance (compaction).
    ///
    /// Runs incremental compaction work within the provided time budget.
    /// Can be called repeatedly to drain pending compaction work.
    ///
    /// Requires the database to have been opened with `{ writable: true }`.
    ///
    /// @param options - Optional maintenance options (default budgetMs: 100)
    /// @returns Promise resolving to a MaintenanceReport
    /// @throws {CqliteError} If write support is not enabled or maintenance fails
    ///
    /// @example
    /// ```javascript
    /// const report = await db.maintenanceStep({ budgetMs: 100 });
    /// console.log(`Merged ${report.rowsMerged} rows in ${report.timeSpentMs}ms`);
    /// if (report.pendingCompaction) {
    ///   console.log('More compaction work pending');
    /// }
    /// ```
    #[napi(js_name = "maintenanceStep")]
    pub async fn maintenance_step(
        &self,
        options: Option<MaintenanceOptions>,
    ) -> napi::Result<MaintenanceReport> {
        self.ensure_open()?;

        #[cfg(feature = "write-support")]
        {
            self.ensure_writable()?;

            let budget_ms = options.as_ref().and_then(|o| o.budget_ms).unwrap_or(100) as u64;

            let we = self
                .write_engine
                .as_ref()
                .expect("ensure_writable verified write_engine is Some");
            let we_clone = Arc::clone(we);

            let report = tokio::task::spawn_blocking(move || {
                let mut engine = we_clone
                    .lock()
                    .map_err(|_| simple_error("Write engine lock poisoned"))?;
                let budget = std::time::Duration::from_millis(budget_ms);
                engine.maintenance_step(budget).map_err(to_napi_error)
            })
            .await
            .map_err(|e| simple_error(format!("maintenanceStep task panicked: {e}")))??;

            Ok(MaintenanceReport {
                time_spent_ms: report.time_spent.as_secs_f64() * 1000.0,
                rows_merged: report.rows_merged as f64,
                bytes_written: report.bytes_written as f64,
                completed_merges: report
                    .completed_merges
                    .iter()
                    .map(|p| p.to_string_lossy().into_owned())
                    .collect(),
                pending_compaction: report.pending_compaction,
            })
        }

        #[cfg(not(feature = "write-support"))]
        {
            let _ = options;
            Err(simple_error(
                "Write support not enabled. Build with --features write-support to enable write operations.",
            ))
        }
    }

    /// Get current write engine statistics (synchronous).
    ///
    /// Returns statistics about the in-memory write buffer (memtable) and WAL.
    /// All sizes are in bytes.
    ///
    /// Requires the database to have been opened with `{ writable: true }`.
    ///
    /// @returns WriteStats snapshot
    /// @throws {CqliteError} If write support is not enabled
    ///
    /// @example
    /// ```javascript
    /// const stats = db.writeStats;
    /// console.log(`Memtable: ${stats.memtableSize} bytes, ${stats.memtableRows} rows`);
    /// console.log(`L0 files: ${stats.l0Count}`);
    /// ```
    #[napi(getter, js_name = "writeStats")]
    pub fn write_stats(&self) -> napi::Result<WriteStats> {
        self.ensure_open()?;

        #[cfg(feature = "write-support")]
        {
            self.ensure_writable()?;

            let we = self
                .write_engine
                .as_ref()
                .expect("ensure_writable verified write_engine is Some");
            let engine = we
                .lock()
                .map_err(|_| simple_error("Write engine lock poisoned"))?;

            // Read L0 count and cumulative flushed bytes from the engine's own
            // authoritative counters (issue #1620). The engine increments these
            // on EVERY flush — including the automatic flushes the `execute()`
            // path now performs via `execute_flushing` — so the stats stay
            // accurate for auto-flushes, not just explicit `flushRun()` calls.
            Ok(WriteStats {
                memtable_size: engine.memtable_size() as f64,
                memtable_rows: engine.memtable_row_count() as u32,
                wal_size: engine.wal_size() as f64,
                l0_count: engine.l0_count() as u32,
                total_written: engine.total_flushed_bytes() as f64,
            })
        }

        #[cfg(not(feature = "write-support"))]
        Err(simple_error(
            "Write support not enabled. Build with --features write-support to enable write operations.",
        ))
    }
}

/// Async task for executing queries with native type conversion.
pub struct ExecuteNativeTask {
    inner: Arc<cqlite_core::Database>,
    query: String,
    /// Per-handle default traceparent for the per-call span (issue #1040).
    traceparent: Option<String>,
    /// Write engine handle, present only when write support is compiled and writable=true.
    #[cfg(feature = "write-support")]
    write_engine: Option<Arc<Mutex<cqlite_core::storage::write_engine::WriteEngine>>>,
}

/// Intermediate result from async query execution.
pub struct QueryResultData {
    rows: Vec<std::collections::HashMap<String, cqlite_core::types::Value>>,
    execution_time_ms: u32,
    columns: Vec<cqlite_core::query::result::ColumnInfo>,
    /// Non-zero when the statement was a DML write.
    rows_affected: u32,
}

impl napi::Task for ExecuteNativeTask {
    type Output = QueryResultData;
    type JsValue = napi::JsObject;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        use tracing::Instrument;

        // Per-call span (issue #1040), parented under the handle's traceparent.
        let span = crate::observability::execute_span("executeNative", self.traceparent.as_deref());

        // Route DML to write engine when write support is present. This path is
        // fully synchronous, so a span guard is correct (no `.await`).
        #[cfg(feature = "write-support")]
        if Database::is_dml_statement(&self.query) {
            if let Some(ref we) = self.write_engine {
                let _entered = span.enter();
                let start = std::time::Instant::now();
                let mut engine = we
                    .lock()
                    .map_err(|_| simple_error("Write engine lock poisoned"))?;
                engine.execute(&self.query).map_err(to_napi_error)?;
                let elapsed_ms = start.elapsed().as_millis() as u32;
                crate::observability::record_rows(&span, 0);
                return Ok(QueryResultData {
                    rows: vec![],
                    execution_time_ms: elapsed_ms,
                    columns: vec![],
                    rows_affected: 1,
                });
            }
        }

        // Fail closed: without the write-support feature, a DML statement must
        // NOT fall through to the read engine (issue #1460). The public
        // `execute_native` entry point already rejects this before creating the
        // task; this is defense-in-depth so the task can never silently no-op.
        #[cfg(not(feature = "write-support"))]
        if Database::is_dml_statement(&self.query) {
            return Err(Database::dml_unsupported_error());
        }

        // Use global runtime for async execution. The future is `.instrument`-ed
        // by the span rather than holding a guard across the runtime boundary.
        let span_for_record = span.clone();
        let query = &self.query;
        let inner = &self.inner;
        let result = crate::runtime::block_on(
            async move {
                inner.execute(query).await.map_err(|e| {
                    crate::observability::record_boundary_error(&e);
                    to_napi_error(e)
                })
            }
            .instrument(span),
        )
        .map_err(runtime_init_error)??;

        let row_count = result.rows.len() as u32;
        crate::observability::record_rows(&span_for_record, row_count as u64);
        Ok(QueryResultData {
            rows: result
                .rows
                .iter()
                .map(|r| {
                    r.values
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect()
                })
                .collect(),
            execution_time_ms: result.execution_time_ms as u32,
            columns: result.metadata.columns.clone(),
            rows_affected: row_count,
        })
    }

    fn resolve(&mut self, env: napi::Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        let mut result_obj = env.create_object()?;

        // Create rows array with native types
        let mut rows_arr = env.create_array_with_length(output.rows.len())?;
        // Issue #1446: intern SELECT-order column-name keys ONCE per result (not
        // per row) and reuse them so every row's properties are emitted in
        // authoritative column order rather than HashMap hash order.
        let col_names: Vec<String> = output.columns.iter().map(|c| c.name.clone()).collect();
        let col_keys = crate::value::intern_column_keys(&env, &col_names)?;
        for (i, row_values) in output.rows.iter().enumerate() {
            let row_obj = crate::value::row_to_object(&env, &col_keys, row_values)?;
            rows_arr.set_element(i as u32, row_obj)?;
        }

        result_obj.set_named_property("rows", rows_arr)?;
        result_obj.set_named_property("rowCount", env.create_uint32(output.rows.len() as u32)?)?;
        result_obj.set_named_property("rowsAffected", env.create_uint32(output.rows_affected)?)?;
        result_obj.set_named_property(
            "executionTimeMs",
            env.create_uint32(output.execution_time_ms)?,
        )?;

        // Create columns array with metadata
        let mut columns_arr = env.create_array_with_length(output.columns.len())?;
        for (i, col) in output.columns.iter().enumerate() {
            let mut col_obj = env.create_object()?;
            col_obj.set_named_property("name", env.create_string(&col.name)?)?;
            col_obj.set_named_property(
                "dataType",
                env.create_string(&format!("{:?}", col.data_type))?,
            )?;
            col_obj.set_named_property("nullable", env.get_boolean(col.nullable)?)?;
            col_obj.set_named_property("position", env.create_uint32(col.position as u32)?)?;
            match &col.table_name {
                Some(name) => col_obj.set_named_property("tableName", env.create_string(name)?)?,
                None => col_obj.set_named_property("tableName", env.get_null()?)?,
            }
            columns_arr.set_element(i as u32, col_obj)?;
        }
        result_obj.set_named_property("columns", columns_arr)?;

        Ok(result_obj)
    }
}

/// Convert a CQL Value to a JSON value.
///
/// This provides basic type conversion for Phase 2.
/// For native JavaScript types, use `executeNative()` instead.
#[deprecated(
    since = "0.4.0",
    note = "Use executeNative() for native JavaScript types"
)]
#[allow(deprecated)]
fn value_to_json(value: &cqlite_core::types::Value) -> serde_json::Value {
    use cqlite_core::types::Value;

    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i as i64).into()),
        Value::BigInt(i) => serde_json::Value::Number((*i).into()),
        Value::TinyInt(i) => serde_json::Value::Number((*i as i64).into()),
        Value::SmallInt(i) => serde_json::Value::Number((*i as i64).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Blob(b) => {
            // Convert blob to base64 string
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            serde_json::Value::String(encoded)
        }
        Value::Timestamp(ts) => {
            // Use from_timestamp_millis to correctly handle pre-epoch timestamps
            // (Issue #341: truncating division was incorrect for negative values)
            if let Some(dt) = chrono::DateTime::from_timestamp_millis(*ts) {
                serde_json::Value::String(dt.to_rfc3339())
            } else {
                serde_json::Value::Number((*ts).into())
            }
        }
        Value::Date(d) => {
            // Days since epoch as number (Cassandra format)
            serde_json::Value::Number((*d as i64).into())
        }
        Value::Time(t) => {
            // Nanoseconds since midnight as number
            serde_json::Value::Number((*t).into())
        }
        Value::Uuid(bytes) => {
            // Format as UUID string
            let uuid = uuid::Uuid::from_bytes(*bytes);
            serde_json::Value::String(uuid.to_string())
        }
        Value::Varint(bytes) => {
            // Convert to hex string for large integers
            let hex_str = hex::encode(bytes);
            serde_json::Value::String(format!("0x{hex_str}"))
        }
        Value::Decimal { scale, unscaled } => {
            // Represent as string to preserve precision
            let hex_str = hex::encode(unscaled);
            serde_json::Value::String(format!("decimal:{scale}:0x{hex_str}"))
        }
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            serde_json::json!({
                "months": months,
                "days": days,
                "nanos": nanos
            })
        }
        Value::Inet(bytes) => {
            // Format as IP address string
            match bytes.len() {
                4 => {
                    let ip = std::net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
                    serde_json::Value::String(ip.to_string())
                }
                16 => {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    let ip = std::net::Ipv6Addr::from(arr);
                    serde_json::Value::String(ip.to_string())
                }
                _ => serde_json::Value::Null,
            }
        }
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Set(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(pairs) => {
            // Convert map to object if keys are strings, otherwise array of pairs
            let all_string_keys = pairs.iter().all(|(k, _)| matches!(k, Value::Text(_)));

            if all_string_keys {
                let obj: serde_json::Map<String, serde_json::Value> = pairs
                    .iter()
                    .filter_map(|(k, v)| {
                        if let Value::Text(s) = k {
                            Some((s.clone(), value_to_json(v)))
                        } else {
                            None
                        }
                    })
                    .collect();
                serde_json::Value::Object(obj)
            } else {
                serde_json::Value::Array(
                    pairs
                        .iter()
                        .map(|(k, v)| {
                            serde_json::json!({
                                "key": value_to_json(k),
                                "value": value_to_json(v)
                            })
                        })
                        .collect(),
                )
            }
        }
        Value::Tuple(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Udt(udt) => {
            let obj: serde_json::Map<String, serde_json::Value> = udt
                .fields
                .iter()
                .map(|field| {
                    let value = field
                        .value
                        .as_ref()
                        .map(value_to_json)
                        .unwrap_or(serde_json::Value::Null);
                    (field.name.clone(), value)
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Frozen(inner) => value_to_json(inner),
        Value::Json(json_value) => {
            // Value::Json contains serde_json::Value, return it directly
            json_value.clone()
        }
        Value::Tombstone(_) => serde_json::Value::Null,
        Value::Counter(c) => serde_json::Value::Number((*c).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_closed_state() {
        // Test that AtomicBool correctly tracks closed state
        let closed = AtomicBool::new(false);
        assert!(!closed.load(Ordering::SeqCst));

        // First swap should return false (was not closed)
        let was_closed = closed.swap(true, Ordering::SeqCst);
        assert!(!was_closed);
        assert!(closed.load(Ordering::SeqCst));

        // Second swap should return true (was already closed)
        let was_closed = closed.swap(true, Ordering::SeqCst);
        assert!(was_closed);
    }

    #[test]
    #[allow(deprecated)]
    fn test_value_to_json_primitives() {
        use cqlite_core::types::Value;

        assert_eq!(value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(
            value_to_json(&Value::Boolean(true)),
            serde_json::Value::Bool(true)
        );
        assert_eq!(value_to_json(&Value::Integer(42)), serde_json::json!(42));
        assert_eq!(
            value_to_json(&Value::Text("hello".to_string())),
            serde_json::json!("hello")
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_value_to_json_uuid() {
        use cqlite_core::types::Value;

        let uuid_bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let result = value_to_json(&Value::Uuid(uuid_bytes));

        if let serde_json::Value::String(s) = result {
            assert!(s.contains('-')); // UUID format with hyphens
        } else {
            panic!("Expected string for UUID");
        }
    }

    #[test]
    #[allow(deprecated)]
    fn test_value_to_json_collections() {
        use cqlite_core::types::Value;

        // List
        let list = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
        assert_eq!(value_to_json(&list), serde_json::json!([1, 2]));

        // Map with string keys
        let map = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Integer(1)),
            (Value::Text("b".to_string()), Value::Integer(2)),
        ]);
        let result = value_to_json(&map);
        assert!(result.is_object());
    }

    // StreamingConfig tests (Issue #304)

    #[test]
    fn test_streaming_config_to_core_default_values() {
        let config = StreamingConfig {
            buffer_size: None,
            chunk_size: None,
        };
        let core = config.to_core().unwrap();
        assert_eq!(core.buffer_size, 1024);
        assert_eq!(core.chunk_size, 10_000);
    }

    #[test]
    fn test_streaming_config_to_core_custom_values() {
        let config = StreamingConfig {
            buffer_size: Some(512),
            chunk_size: Some(5000),
        };
        let core = config.to_core().unwrap();
        assert_eq!(core.buffer_size, 512);
        assert_eq!(core.chunk_size, 5000);
    }

    #[test]
    fn test_streaming_config_to_core_zero_buffer_size_fails() {
        let config = StreamingConfig {
            buffer_size: Some(0),
            chunk_size: Some(10000),
        };
        let result = config.to_core();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.reason.contains("bufferSize must be greater than 0"));
    }

    #[test]
    fn test_streaming_config_to_core_zero_chunk_size_fails() {
        let config = StreamingConfig {
            buffer_size: Some(1024),
            chunk_size: Some(0),
        };
        let result = config.to_core();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.reason.contains("chunkSize must be greater than 0"));
    }

    #[test]
    fn test_streaming_config_with_defaults() {
        let config = StreamingConfig::with_defaults();
        assert_eq!(config.buffer_size, Some(1024));
        assert_eq!(config.chunk_size, Some(10_000));

        // Should also convert to core correctly
        let core = config.to_core().unwrap();
        assert_eq!(core.buffer_size, 1024);
        assert_eq!(core.chunk_size, 10_000);
    }
}
