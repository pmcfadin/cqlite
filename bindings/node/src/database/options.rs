//! napi object DTOs for the Node.js `Database` surface.
//!
//! Split out of `database.rs` under the campsite rule (epic #1116, issue
//! #1464). Pure code motion: every type, field, attribute and doc comment is
//! unchanged from the single-file layout.

use napi_derive::napi;

use crate::error::to_napi_error;

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
    pub(super) fn from_core(col: &cqlite_core::query::result::ColumnInfo) -> Self {
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
    pub(super) fn to_core(
        &self,
    ) -> napi::Result<cqlite_core::export::parquet::ParquetExportOptions> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
