//! Query result types for CQLite
//!
//! This module provides result types and utilities for query execution results.
//! It includes result set management, row iteration, and result metadata.

use crate::util::udt_json::udt_to_json_object;
use crate::{schema::CqlType, RowKey, Value};
// Re-export cell metadata types that now live in crate::types so the storage
// layer can use them without a cyclic dependency.
pub use crate::types::{CellExpiration, CellWriteMetadata};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Encode bytes as standard base64 (used across JSON serializers below).
fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// True if `RowMetadata` carries anything worth serializing.
fn row_metadata_is_populated(meta: &RowMetadata) -> bool {
    meta.version.is_some() || meta.ttl.is_some() || !meta.tags.is_empty()
}

// ============================================================================
// Per-cell write metadata (Issue #691)
// ============================================================================
//
// CellWriteMetadata and CellExpiration are defined in crate::types and
// re-exported above.  All usages of these types within this module
// and its callers are unchanged — the re-export makes the move transparent.

/// Projection-level flags that control opt-in metadata collection.
///
/// Created during query planning and threaded to the scan/build path.
/// When all flags are `false` (the default), the hot path allocates nothing
/// extra — `QueryRow::cell_metadata` stays `None`.
#[derive(Debug, Clone, Default)]
pub struct ProjectionFlags {
    /// Set when the SELECT list contains at least one `WRITETIME(col)` or
    /// `TTL(col)` expression.  Causes per-cell metadata to be attached to
    /// each `QueryRow` produced by the scan.
    ///
    /// **Wired by**: issue #692 (executor evaluation).  Until then, callers
    /// can set this flag manually in tests or driver code.
    pub include_cell_metadata: bool,
}

/// Query result containing rows and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Result rows
    pub rows: Vec<QueryRow>,
    /// Number of rows affected (for INSERT/UPDATE/DELETE)
    pub rows_affected: u64,
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
    /// Query metadata
    pub metadata: QueryMetadata,
}

/// Individual row in query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRow {
    /// Column values mapped by column name.
    ///
    /// Keyed by a shared `Arc<str>` column-name handle (issue #1334) interned
    /// once by the decoder, so carrying a decoded cell's name into the row is a
    /// reference-count bump rather than a per-cell heap `String` allocation.
    /// `Arc<str>: Borrow<str>`, so all name-based reads (`get(&str)`, `keys()`,
    /// iteration) are source-compatible with the prior `String` key. serde's
    /// `rc` feature makes this (de)serialize with the identical name→value JSON
    /// object shape.
    pub values: HashMap<Arc<str>, Value>,
    /// Original row key
    pub key: RowKey,
    /// Row metadata
    pub metadata: RowMetadata,
    /// Per-cell write metadata (Issue #691).
    ///
    /// Populated **only** when `ProjectionFlags::include_cell_metadata` is
    /// `true` during query planning.  `None` on the hot path — no allocation
    /// is performed unless metadata is explicitly requested.
    ///
    /// Map key = column name; value = write timestamp + optional expiration.
    /// Columns absent from this map either had no individual cell header in
    /// the SSTable (e.g. partition-key columns) or were not decoded under the
    /// current flag setting.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub cell_metadata: Option<HashMap<String, CellWriteMetadata>>,
}

/// Metadata for query results
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryMetadata {
    /// Column information
    pub columns: Vec<ColumnInfo>,
    /// Total row count (may be different from returned rows due to LIMIT)
    pub total_rows: Option<u64>,
    /// Query execution plan information
    pub plan_info: Option<PlanInfo>,
    /// Performance metrics
    pub performance: PerformanceMetrics,
    /// Warnings generated during execution
    pub warnings: Vec<String>,
    /// Access path selected by the SSTable-scan step (Issue #960).
    ///
    /// `Some` when a SELECT ran through the modern `SelectExecutor` (materializing
    /// or streaming); `None` for surfaces that do not yet report a path (e.g. the
    /// legacy executor and non-SELECT queries). This is the result-attached half
    /// of the access-path signal; the test-accessible probe is
    /// `crate::query::access_path::last()`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub access_path: Option<crate::query::access_path::AccessPath>,
}

/// Information about a column in the result set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Column data type (flat, kept for backward compatibility)
    pub data_type: crate::types::DataType,
    /// Whether column can be null
    pub nullable: bool,
    /// Column position in result set
    pub position: usize,
    /// Original table name (for joined queries)
    pub table_name: Option<String>,
    /// Full schema-sourced CQL type (populated when a schema is available).
    ///
    /// This field expresses element types for collections (`list<int>`,
    /// `map<text, bigint>`), and carries variants absent from the flat
    /// `DataType` enum (`date`, `time`, `decimal`, `varint`, `counter`,
    /// `duration`, `inet`). Downstream writers MUST use this over
    /// `data_type` when it is `Some` — the no-heuristics mandate (Issue #28)
    /// requires authoritative-schema metadata rather than runtime inference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cql_type: Option<CqlType>,
}

/// Row metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RowMetadata {
    /// Row version/timestamp
    pub version: Option<u64>,
    /// Row TTL (time to live)
    pub ttl: Option<u64>,
    /// Row tags or labels
    pub tags: HashMap<String, String>,
}

/// Query execution plan information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanInfo {
    /// Plan type used
    pub plan_type: String,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Actual cost
    pub actual_cost: f64,
    /// Indexes used
    pub indexes_used: Vec<String>,
    /// Steps executed
    pub steps: Vec<String>,
    /// Parallelization information
    pub parallelization: Option<ParallelizationInfo>,
}

/// Parallelization information for query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelizationInfo {
    /// Number of threads used
    pub threads_used: usize,
    /// Whether parallelization was effective
    pub effective: bool,
    /// Partition information
    pub partitions: Vec<PartitionInfo>,
}

/// Information about a partition processed in parallel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// Partition ID
    pub id: usize,
    /// Rows processed by this partition
    pub rows_processed: u64,
    /// Processing time for this partition
    pub processing_time_ms: u64,
}

/// Performance metrics for query execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Parse time in microseconds
    pub parse_time_us: u64,
    /// Planning time in microseconds
    pub planning_time_us: u64,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Total time in microseconds
    pub total_time_us: u64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// I/O operations performed
    pub io_operations: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
}

// ============================================================================
// Streaming Query Results (Issue #280)
// ============================================================================

/// Configuration for streaming query results
///
/// Controls buffer sizes and chunk sizes for memory-efficient processing
/// of large result sets.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Channel buffer size (controls backpressure)
    /// Default: 1024 rows in flight
    pub buffer_size: usize,
    /// Chunk size hint for writers (rows per chunk)
    /// Default: 10,000 rows (matches Parquet row group size)
    pub chunk_size: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1024,  // 1K rows in flight
            chunk_size: 10_000, // 10K rows per chunk (matches Parquet row group)
        }
    }
}

impl StreamingConfig {
    /// Create a new streaming config with custom settings
    pub fn new(buffer_size: usize, chunk_size: usize) -> Self {
        Self {
            buffer_size,
            chunk_size,
        }
    }

    /// Create a config optimized for Parquet output
    pub fn for_parquet() -> Self {
        Self {
            buffer_size: 1024,
            chunk_size: 10_000, // Row group size
        }
    }

    /// Create a config optimized for CSV/JSON output
    pub fn for_text_formats() -> Self {
        Self {
            buffer_size: 512,
            chunk_size: 5_000, // Smaller chunks for text formats
        }
    }
}

/// Streaming query result iterator for memory-efficient processing
///
/// Instead of materializing all rows into a `Vec`, this iterator yields rows
/// lazily via a channel, allowing processing of arbitrarily large result sets
/// within the 128MB memory budget.
///
/// # Memory Budget
///
/// To stay within the 128MB target, callers MUST create a bounded channel
/// with capacity from `StreamingConfig::buffer_size`. Assuming average row
/// size of 1KB:
/// - `buffer_size: 1024` = ~1MB in flight
/// - `chunk_size: 10_000` = ~10MB per chunk
/// - Total peak usage: ~11MB (well within 128MB budget)
///
/// For rows with large blobs/text, reduce buffer sizes proportionally.
///
/// # Contract
///
/// 1. The caller MUST create a bounded channel with `mpsc::channel(config.buffer_size)`
/// 2. The iterator does NOT own the sender; the caller must spawn a task to send rows
/// 3. The iterator is consumed once; create a new one for subsequent queries
///
/// # Example
///
/// ```ignore
/// let config = StreamingConfig::default();
/// let (tx, rx) = tokio::sync::mpsc::channel(config.buffer_size);
///
/// // Spawn producer
/// tokio::spawn(async move {
///     for row in rows {
///         if tx.send(Ok(row)).await.is_err() {
///             break; // Consumer dropped
///         }
///     }
/// });
///
/// // Create iterator from receiver
/// let mut iterator = QueryResultIterator::new(rx, metadata);
///
/// while let Some(row_result) = iterator.next_async().await {
///     let row = row_result?;
///     writer.write_row(&row)?;
/// }
/// ```
pub struct QueryResultIterator {
    /// Channel receiver for rows
    receiver: mpsc::Receiver<Result<QueryRow, crate::Error>>,
    /// Query metadata (columns, etc.)
    pub metadata: QueryMetadata,
    /// Total rows hint (if known from query planning)
    pub total_rows_hint: Option<u64>,
    /// Count of rows received so far
    rows_received: u64,
}

impl QueryResultIterator {
    /// Create a new streaming result iterator
    pub fn new(
        receiver: mpsc::Receiver<Result<QueryRow, crate::Error>>,
        metadata: QueryMetadata,
    ) -> Self {
        Self {
            receiver,
            metadata,
            total_rows_hint: None,
            rows_received: 0,
        }
    }

    /// Create with a known total row count hint
    pub fn with_total_hint(mut self, total: u64) -> Self {
        self.total_rows_hint = Some(total);
        self
    }

    /// Receive next row (async)
    ///
    /// Returns `None` when all rows have been received.
    pub async fn next_async(&mut self) -> Option<Result<QueryRow, crate::Error>> {
        let result = self.receiver.recv().await?;
        if result.is_ok() {
            self.rows_received += 1;
        }
        Some(result)
    }

    /// Maximum allowed chunk size to prevent OOM
    const MAX_CHUNK_SIZE: usize = 100_000;

    /// Collect into chunks of specified size
    ///
    /// Returns a chunk of rows up to `size`. May return fewer rows if the
    /// stream ends or an error occurs.
    ///
    /// # Arguments
    ///
    /// * `size` - Maximum number of rows to collect. Limited to MAX_CHUNK_SIZE
    ///   (100,000) to prevent unbounded memory allocation.
    ///
    /// # Returns
    ///
    /// A vector of rows, which may be smaller than `size` if the stream ends
    /// or an error occurs.
    pub async fn collect_chunk(&mut self, size: usize) -> Result<Vec<QueryRow>, crate::Error> {
        let safe_size = size.min(Self::MAX_CHUNK_SIZE);
        // Grow the Vec lazily; a requested `safe_size` is only an upper bound.
        let mut chunk = Vec::new();
        while chunk.len() < safe_size {
            match self.receiver.recv().await {
                Some(Ok(row)) => {
                    self.rows_received += 1;
                    chunk.push(row);
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        Ok(chunk)
    }

    /// Get count of rows received so far
    pub fn rows_received(&self) -> u64 {
        self.rows_received
    }

    /// Get progress as a percentage (if total is known)
    pub fn progress_percent(&self) -> Option<f64> {
        self.total_rows_hint.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.rows_received as f64 / total as f64) * 100.0
            }
        })
    }
}

impl QueryResult {
    /// Create a new empty query result
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            rows_affected: 0,
            execution_time_ms: 0,
            metadata: QueryMetadata::default(),
        }
    }

    /// Create a result with rows
    pub fn with_rows(rows: Vec<QueryRow>) -> Self {
        Self {
            rows,
            ..Self::new()
        }
    }

    /// Create a result for DML operations (INSERT/UPDATE/DELETE)
    pub fn with_affected_rows(rows_affected: u64) -> Self {
        Self {
            rows_affected,
            ..Self::new()
        }
    }

    /// Get the number of rows in the result
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get a specific row by index
    pub fn get_row(&self, index: usize) -> Option<&QueryRow> {
        self.rows.get(index)
    }

    /// Get column information
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.metadata.columns
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<String> {
        self.metadata
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect()
    }

    /// Get execution time in milliseconds
    pub fn execution_time(&self) -> u64 {
        self.execution_time_ms
    }

    /// Get performance metrics
    pub fn performance(&self) -> &PerformanceMetrics {
        &self.metadata.performance
    }

    /// Get warnings
    pub fn warnings(&self) -> &[String] {
        &self.metadata.warnings
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: String) {
        self.metadata.warnings.push(warning);
    }

    /// Convert to JSON representation
    ///
    /// Note: `execution_time_ms` is intentionally excluded to keep snapshot
    /// output deterministic; use `execution_time()` to read it separately.
    pub fn to_json(&self) -> serde_json::Value {
        let rows: Vec<_> = self
            .rows
            .iter()
            .map(|row| self.row_to_json_deterministic(row))
            .collect();
        let columns: Vec<_> = self
            .metadata
            .columns
            .iter()
            .map(ColumnInfo::to_json)
            .collect();
        let warnings: Vec<_> = self
            .metadata
            .warnings
            .iter()
            .cloned()
            .map(serde_json::Value::String)
            .collect();

        json!({
            "rows": rows,
            "rows_affected": self.rows_affected,
            "row_count": self.rows.len(),
            "columns": columns,
            "performance": self.metadata.performance.to_json(),
            "warnings": warnings,
        })
    }

    /// Create result iterator
    pub fn iter(&self) -> std::slice::Iter<'_, QueryRow> {
        self.rows.iter()
    }

    /// Convert a single row to JSON with deterministic field ordering.
    ///
    /// When `metadata.columns` is populated, fields appear in that order; otherwise
    /// HashMap keys are emitted in sorted order so snapshots stay stable.
    fn row_to_json_deterministic(&self, row: &QueryRow) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        if !self.metadata.columns.is_empty() {
            for col in &self.metadata.columns {
                let value_json = row
                    .values
                    .get(col.name.as_str())
                    .map_or(serde_json::Value::Null, ToJson::to_json);
                result.insert(col.name.clone(), value_json);
            }
        } else {
            let mut sorted_keys: Vec<&Arc<str>> = row.values.keys().collect();
            sorted_keys.sort();
            for key in sorted_keys {
                if let Some(value) = row.values.get(key.as_ref()) {
                    result.insert(key.to_string(), value.to_json());
                }
            }
        }

        result.insert(
            "_key".to_string(),
            serde_json::Value::String(format!("{:?}", row.key)),
        );

        if row_metadata_is_populated(&row.metadata) {
            result.insert("_metadata".to_string(), row.metadata.to_json());
        }

        serde_json::Value::Object(result)
    }
}

impl QueryRow {
    /// Create a new query row
    pub fn new(key: RowKey) -> Self {
        Self {
            values: HashMap::new(),
            key,
            metadata: RowMetadata::default(),
            cell_metadata: None,
        }
    }

    /// Create a row with values.
    ///
    /// Concrete over `HashMap<String, Value>` so existing callers (including
    /// `QueryRow::with_values(key, HashMap::new())`) infer the key type without
    /// annotations (issue #1334); the string keys are interned into shared
    /// `Arc<str>` handles once here. Callers that already hold interned
    /// `Arc<str>` keys should use [`QueryRow::with_interned_values`] to skip the
    /// re-allocation.
    pub fn with_values(key: RowKey, values: HashMap<String, Value>) -> Self {
        Self {
            values: values.into_iter().map(|(k, v)| (Arc::from(k), v)).collect(),
            key,
            metadata: RowMetadata::default(),
            cell_metadata: None,
        }
    }

    /// Create a row from already-interned `Arc<str>` column-name handles.
    ///
    /// The interned-key counterpart of [`QueryRow::with_values`] (issue #1334):
    /// the storage/scan path already carries interned `Arc<str>` names, so the
    /// handles move straight in with only a reference-count bump — no per-cell
    /// `String` allocation.
    pub fn with_interned_values(key: RowKey, values: HashMap<Arc<str>, Value>) -> Self {
        Self {
            values,
            key,
            metadata: RowMetadata::default(),
            cell_metadata: None,
        }
    }

    /// Create a row from a column name → value map, using a synthetic empty key.
    ///
    /// Convenience constructor used by CLI utilities that do not track a raw
    /// partition key.  The key is set to an empty byte vector. Concrete over
    /// `HashMap<String, Value>` so `HashMap::new()` callers infer without
    /// annotations (issue #1334); the string keys are interned here.
    pub fn from_map(values: HashMap<String, Value>) -> Self {
        Self {
            values: values.into_iter().map(|(k, v)| (Arc::from(k), v)).collect(),
            key: RowKey::new(vec![]),
            metadata: RowMetadata::default(),
            cell_metadata: None,
        }
    }

    /// Get a value by column name
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.values.get(column)
    }

    /// Set a value for a column.
    ///
    /// Accepts anything convertible into the shared `Arc<str>` key (issue
    /// #1334): a `String`, `&str`, or an existing `Arc<str>` handle.
    pub fn set(&mut self, column: impl Into<Arc<str>>, value: Value) {
        self.values.insert(column.into(), value);
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<String> {
        self.values.keys().map(|k| k.to_string()).collect()
    }

    /// Get the row key
    pub fn key(&self) -> &RowKey {
        &self.key
    }

    /// Get row metadata
    pub fn metadata(&self) -> &RowMetadata {
        &self.metadata
    }

    /// Set row metadata
    pub fn set_metadata(&mut self, metadata: RowMetadata) {
        self.metadata = metadata;
    }

    // ---- Per-cell metadata (Issue #691) ----

    /// Attach per-cell write metadata to this row.
    ///
    /// Replaces any previously attached map. Intended to be called by the
    /// scan/build path when `ProjectionFlags::include_cell_metadata` is set.
    pub fn set_cell_metadata(&mut self, map: HashMap<String, CellWriteMetadata>) {
        self.cell_metadata = Some(map);
    }

    /// Insert a single column's write metadata.
    ///
    /// Initialises the map on first call; subsequent calls insert into the
    /// existing map.  No-op when called on the hot path that never enables
    /// metadata (the caller guards on the flag).
    pub fn insert_cell_metadata(&mut self, column: String, meta: CellWriteMetadata) {
        self.cell_metadata
            .get_or_insert_with(HashMap::new)
            .insert(column, meta);
    }

    /// Return the write metadata for `column`, if present.
    pub fn get_cell_metadata(&self, column: &str) -> Option<&CellWriteMetadata> {
        self.cell_metadata.as_ref()?.get(column)
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        for (column, value) in &self.values {
            result.insert(column.to_string(), value.to_json());
        }

        result.insert(
            "_key".to_string(),
            serde_json::Value::String(format!("{:?}", self.key)),
        );

        if row_metadata_is_populated(&self.metadata) {
            result.insert("_metadata".to_string(), self.metadata.to_json());
        }

        serde_json::Value::Object(result)
    }
}

impl ColumnInfo {
    /// Create new column info
    pub fn new(
        name: String,
        data_type: crate::types::DataType,
        nullable: bool,
        position: usize,
    ) -> Self {
        Self {
            name,
            data_type,
            nullable,
            position,
            table_name: None,
            cql_type: None,
        }
    }

    /// Set table name
    pub fn with_table_name(mut self, table_name: String) -> Self {
        self.table_name = Some(table_name);
        self
    }

    /// Attach a schema-sourced [`CqlType`] to this column.
    ///
    /// The `data_type` field is left unchanged so existing consumers remain
    /// unaffected; downstream writers that need full type fidelity (e.g. the
    /// Parquet/Arrow writer) should prefer `cql_type` when it is `Some`.
    pub fn with_cql_type(mut self, cql_type: CqlType) -> Self {
        self.cql_type = Some(cql_type);
        self
    }

    /// Convert to JSON representation
    ///
    /// The `data_type` and all pre-existing keys are preserved unchanged for
    /// backward compatibility. The new `cql_type` key is only emitted when
    /// the field is `Some` (it is marked `skip_serializing_if` in the struct
    /// derive, but we also reflect it here for the hand-rolled JSON path).
    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("name".to_string(), json!(self.name));
        map.insert(
            "data_type".to_string(),
            json!(format!("{:?}", self.data_type)),
        );
        map.insert("nullable".to_string(), json!(self.nullable));
        map.insert("position".to_string(), json!(self.position));
        if let Some(table_name) = &self.table_name {
            map.insert("table_name".to_string(), json!(table_name));
        }
        // New additive key: only present when schema CqlType is known.
        if let Some(cql_type) = &self.cql_type {
            map.insert("cql_type".to_string(), json!(format_cql_type(cql_type)));
        }
        serde_json::Value::Object(map)
    }
}

/// Map a schema-level [`CqlType`] to the flat [`crate::types::DataType`] enum.
///
/// This is used in the `SELECT *` path and explicit projection paths to derive
/// a backward-compatible `data_type` from authoritative schema metadata rather
/// than hard-coding `DataType::Text` (Issue #674 / no-heuristics mandate #28).
pub fn cql_type_to_data_type(cql_type: &CqlType) -> crate::types::DataType {
    use crate::types::DataType;
    match cql_type {
        CqlType::Boolean => DataType::Boolean,
        CqlType::TinyInt => DataType::TinyInt,
        CqlType::SmallInt => DataType::SmallInt,
        CqlType::Int => DataType::Integer,
        CqlType::BigInt | CqlType::Varint | CqlType::Counter => DataType::BigInt,
        CqlType::Float => DataType::Float32,
        CqlType::Double | CqlType::Decimal => DataType::Float,
        CqlType::Text | CqlType::Varchar | CqlType::Ascii => DataType::Text,
        CqlType::Blob => DataType::Blob,
        CqlType::Timestamp => DataType::Timestamp,
        CqlType::Date | CqlType::Time | CqlType::Duration | CqlType::Inet => DataType::BigInt,
        CqlType::Uuid | CqlType::TimeUuid => DataType::Uuid,
        CqlType::List(_) | CqlType::Vector(_, _) => DataType::List,
        CqlType::Set(_) => DataType::Set,
        CqlType::Map(_, _) => DataType::Map,
        CqlType::Tuple(_) => DataType::Tuple,
        CqlType::Udt(_, _) => DataType::Udt,
        CqlType::Frozen(inner) => cql_type_to_data_type(inner),
        CqlType::Custom(_) => DataType::Blob,
    }
}

/// Format a [`CqlType`] as a human-readable CQL type string.
///
/// Used for the `cql_type` field in `ColumnInfo::to_json`.
fn format_cql_type(cql_type: &CqlType) -> String {
    match cql_type {
        CqlType::Boolean => "boolean".to_string(),
        CqlType::TinyInt => "tinyint".to_string(),
        CqlType::SmallInt => "smallint".to_string(),
        CqlType::Int => "int".to_string(),
        CqlType::BigInt => "bigint".to_string(),
        CqlType::Counter => "counter".to_string(),
        CqlType::Float => "float".to_string(),
        CqlType::Double => "double".to_string(),
        CqlType::Decimal => "decimal".to_string(),
        CqlType::Text => "text".to_string(),
        CqlType::Varchar => "varchar".to_string(),
        CqlType::Ascii => "ascii".to_string(),
        CqlType::Blob => "blob".to_string(),
        CqlType::Timestamp => "timestamp".to_string(),
        CqlType::Date => "date".to_string(),
        CqlType::Time => "time".to_string(),
        CqlType::Uuid => "uuid".to_string(),
        CqlType::TimeUuid => "timeuuid".to_string(),
        CqlType::Inet => "inet".to_string(),
        CqlType::Duration => "duration".to_string(),
        CqlType::Varint => "varint".to_string(),
        CqlType::List(inner) => format!("list<{}>", format_cql_type(inner)),
        CqlType::Set(inner) => format!("set<{}>", format_cql_type(inner)),
        CqlType::Map(k, v) => format!("map<{}, {}>", format_cql_type(k), format_cql_type(v)),
        CqlType::Tuple(types) => {
            let inner: Vec<_> = types.iter().map(format_cql_type).collect();
            format!("tuple<{}>", inner.join(", "))
        }
        CqlType::Udt(name, _) => name.clone(),
        CqlType::Frozen(inner) => format!("frozen<{}>", format_cql_type(inner)),
        CqlType::Vector(e, n) => format!("vector<{}, {n}>", format_cql_type(e)),
        CqlType::Custom(name) => name.clone(),
    }
}

impl RowMetadata {
    /// Create new row metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Set version
    pub fn with_version(mut self, version: u64) -> Self {
        self.version = Some(version);
        self
    }

    /// Set TTL
    pub fn with_ttl(mut self, ttl: u64) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Add tag
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key, value);
        self
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        if let Some(version) = self.version {
            map.insert("version".to_string(), json!(version));
        }
        if let Some(ttl) = self.ttl {
            map.insert("ttl".to_string(), json!(ttl));
        }
        if !self.tags.is_empty() {
            map.insert("tags".to_string(), json!(self.tags));
        }
        serde_json::Value::Object(map)
    }
}

impl PerformanceMetrics {
    /// Create new performance metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total time in milliseconds
    pub fn total_time_ms(&self) -> u64 {
        self.total_time_us / 1000
    }

    /// Get cache hit ratio
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let cache_hit_ratio = serde_json::Number::from_f64(self.cache_hit_ratio())
            .map(serde_json::Value::Number)
            .unwrap_or(json!(0));
        json!({
            "parse_time_us": self.parse_time_us,
            "planning_time_us": self.planning_time_us,
            "execution_time_us": self.execution_time_us,
            "total_time_us": self.total_time_us,
            "memory_usage_bytes": self.memory_usage_bytes,
            "io_operations": self.io_operations,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_ratio": cache_hit_ratio,
        })
    }
}

/// Write one horizontal border row using the supplied left/middle/right glyphs.
fn write_border(
    f: &mut fmt::Formatter<'_>,
    widths: &[usize],
    left: char,
    sep: char,
    right: char,
) -> fmt::Result {
    write!(f, "{}", left)?;
    for (i, width) in widths.iter().enumerate() {
        write!(f, "{}", "─".repeat(width + 2))?;
        if i < widths.len() - 1 {
            write!(f, "{}", sep)?;
        }
    }
    writeln!(f, "{}", right)
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.rows.is_empty() {
            return write!(f, "Empty result set ({} rows affected)", self.rows_affected);
        }

        let column_names = self.column_names();
        if column_names.is_empty() {
            return write!(f, "No columns in result set");
        }

        // Column widths = max(header, longest value) for each column.
        let col_widths: Vec<usize> = column_names
            .iter()
            .map(|col_name| {
                self.rows
                    .iter()
                    .filter_map(|row| row.values.get(col_name.as_str()))
                    .map(|v| format!("{}", v).len())
                    .max()
                    .unwrap_or(0)
                    .max(col_name.len())
            })
            .collect();

        write_border(f, &col_widths, '┌', '┬', '┐')?;

        write!(f, "│")?;
        for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
            write!(f, " {:width$} ", col_name, width = width)?;
            if i < column_names.len() - 1 {
                write!(f, "│")?;
            }
        }
        writeln!(f, "│")?;

        write_border(f, &col_widths, '├', '┼', '┤')?;

        for row in &self.rows {
            write!(f, "│")?;
            for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
                let value = row
                    .values
                    .get(col_name.as_str())
                    .map(|v| format!("{}", v))
                    .unwrap_or_else(|| "NULL".to_string());
                write!(f, " {:width$} ", value, width = width)?;
                if i < column_names.len() - 1 {
                    write!(f, "│")?;
                }
            }
            writeln!(f, "│")?;
        }

        write_border(f, &col_widths, '└', '┴', '┘')?;

        writeln!(
            f,
            "{} rows returned in {}ms",
            self.rows.len(),
            self.execution_time_ms
        )?;

        if !self.metadata.warnings.is_empty() {
            writeln!(f, "\nWarnings:")?;
            for warning in &self.metadata.warnings {
                writeln!(f, "  - {}", warning)?;
            }
        }

        Ok(())
    }
}

impl Default for QueryResult {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for QueryResult {
    type Item = QueryRow;
    type IntoIter = std::vec::IntoIter<QueryRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a QueryResult {
    type Item = &'a QueryRow;
    type IntoIter = std::slice::Iter<'a, QueryRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

// Helper trait for converting values to JSON
trait ToJson {
    fn to_json(&self) -> serde_json::Value;
}

impl ToJson for Value {
    fn to_json(&self) -> serde_json::Value {
        // Non-finite floats have no JSON representation; we emit null for those.
        fn float_to_json(x: f64) -> serde_json::Value {
            serde_json::Number::from_f64(x)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }

        match self {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(b) => json!(*b),
            Value::Integer(i) => json!(*i),
            Value::BigInt(i) => json!(*i),
            Value::Counter(c) => json!(*c),
            Value::TinyInt(i) => json!(*i as i64),
            Value::SmallInt(i) => json!(*i as i64),
            Value::Date(d) => json!(*d),
            Value::Time(t) => json!(*t),
            Value::Timestamp(ts) => json!(*ts),
            Value::Float(f) => float_to_json(*f),
            Value::Float32(f) => float_to_json(*f as f64),
            Value::Text(s) => json!(std::str::from_utf8(s).unwrap_or_default()),
            Value::Json(value) => (**value).clone(),
            Value::Blob(bytes) | Value::Varint(bytes) | Value::Inet(bytes) => json!(b64(bytes)),
            Value::Uuid(uuid) => json!(b64(uuid)),
            Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
                let json_list: Vec<_> = items.iter().map(ToJson::to_json).collect();
                serde_json::Value::Array(json_list)
            }
            Value::Map(entries) => {
                let json_map: serde_json::Map<String, serde_json::Value> = entries
                    .iter()
                    .map(|(k, v)| (format!("{}", k), v.to_json()))
                    .collect();
                serde_json::Value::Object(json_map)
            }
            // Declared fields and NOTHING else — no injected `_type` (issue
            // #3629): type identity must not share the user's field namespace.
            // One shared rule, each writer keeping its own field-value renderer.
            Value::Udt(udt) => udt_to_json_object(udt, ToJson::to_json),
            Value::Frozen(boxed) => boxed.to_json(),
            Value::Decimal { scale, unscaled } => json!({
                "scale": *scale,
                "unscaled": b64(unscaled),
            }),
            Value::Duration {
                months,
                days,
                nanos,
            } => json!({
                "months": *months,
                "days": *days,
                "nanos": *nanos,
            }),
            Value::Tombstone(info) => {
                let mut json_obj = serde_json::Map::new();
                json_obj.insert("type".to_string(), json!("tombstone"));
                json_obj.insert("deletion_time".to_string(), json!(info.deletion_time));
                json_obj.insert(
                    "tombstone_type".to_string(),
                    json!(format!("{:?}", info.tombstone_type)),
                );
                if let Some(ttl) = info.ttl {
                    json_obj.insert("ttl".to_string(), json!(ttl));
                }
                serde_json::Value::Object(json_obj)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn test_query_result_creation() {
        let result = QueryResult::new();
        assert!(result.is_empty());
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.execution_time(), 0);
    }

    #[test]
    fn test_query_result_with_rows() {
        let mut row1 = QueryRow::new(RowKey::new(vec![1]));
        row1.set("id".to_string(), Value::Integer(1));
        row1.set("name".to_string(), Value::text("Alice".to_string()));

        let mut row2 = QueryRow::new(RowKey::new(vec![2]));
        row2.set("id".to_string(), Value::Integer(2));
        row2.set("name".to_string(), Value::text("Bob".to_string()));

        let result = QueryResult::with_rows(vec![row1, row2]);
        assert_eq!(result.row_count(), 2);
        assert!(!result.is_empty());

        let first_row = result.get_row(0).unwrap();
        assert_eq!(first_row.get("id"), Some(&Value::Integer(1)));
        assert_eq!(
            first_row.get("name"),
            Some(&Value::text("Alice".to_string()))
        );
    }

    #[test]
    fn test_query_row_operations() {
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set("id".to_string(), Value::Integer(42));
        row.set("active".to_string(), Value::Boolean(true));

        assert_eq!(row.get("id"), Some(&Value::Integer(42)));
        assert_eq!(row.get("active"), Some(&Value::Boolean(true)));
        assert_eq!(row.get("nonexistent"), None);

        let column_names = row.column_names();
        assert_eq!(column_names.len(), 2);
        assert!(column_names.contains(&"id".to_string()));
        assert!(column_names.contains(&"active".to_string()));
    }

    #[test]
    fn test_column_info() {
        let column = ColumnInfo::new(
            "user_id".to_string(),
            crate::types::DataType::Integer,
            false,
            0,
        )
        .with_table_name("users".to_string());

        assert_eq!(column.name, "user_id");
        assert_eq!(column.data_type, crate::types::DataType::Integer);
        assert!(!column.nullable);
        assert_eq!(column.position, 0);
        assert_eq!(column.table_name, Some("users".to_string()));
        assert!(column.cql_type.is_none());
    }

    #[test]
    fn test_column_info_with_cql_type_scalar() {
        use crate::schema::CqlType;
        let column = ColumnInfo::new("ts".to_string(), crate::types::DataType::Timestamp, true, 1)
            .with_cql_type(CqlType::Timestamp);

        assert_eq!(column.name, "ts");
        assert_eq!(column.cql_type, Some(CqlType::Timestamp));
        // data_type is unaffected
        assert_eq!(column.data_type, crate::types::DataType::Timestamp);
    }

    #[test]
    fn test_column_info_with_cql_type_list() {
        use crate::schema::CqlType;
        let list_type = CqlType::List(Box::new(CqlType::Int));
        let column = ColumnInfo::new("items".to_string(), crate::types::DataType::List, true, 2)
            .with_cql_type(list_type.clone());

        assert_eq!(column.cql_type, Some(list_type));
    }

    #[test]
    fn test_column_info_with_cql_type_map() {
        use crate::schema::CqlType;
        let map_type = CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::BigInt));
        let column = ColumnInfo::new("props".to_string(), crate::types::DataType::Map, true, 3)
            .with_cql_type(map_type.clone());

        assert_eq!(column.cql_type, Some(map_type));
    }

    #[test]
    fn test_column_info_with_cql_type_udt() {
        use crate::schema::CqlType;
        let udt_type = CqlType::Udt("address".to_string(), vec![]);
        let column = ColumnInfo::new("addr".to_string(), crate::types::DataType::Udt, true, 4)
            .with_cql_type(udt_type.clone());

        assert_eq!(column.cql_type, Some(udt_type));
    }

    #[test]
    fn test_cql_type_to_data_type_scalars() {
        use super::cql_type_to_data_type;
        use crate::schema::CqlType;
        use crate::types::DataType;

        assert_eq!(cql_type_to_data_type(&CqlType::Boolean), DataType::Boolean);
        assert_eq!(cql_type_to_data_type(&CqlType::Int), DataType::Integer);
        assert_eq!(cql_type_to_data_type(&CqlType::BigInt), DataType::BigInt);
        assert_eq!(cql_type_to_data_type(&CqlType::Text), DataType::Text);
        assert_eq!(cql_type_to_data_type(&CqlType::Blob), DataType::Blob);
        assert_eq!(cql_type_to_data_type(&CqlType::Uuid), DataType::Uuid);
        assert_eq!(
            cql_type_to_data_type(&CqlType::Timestamp),
            DataType::Timestamp
        );
    }

    #[test]
    fn test_cql_type_to_data_type_collections() {
        use super::cql_type_to_data_type;
        use crate::schema::CqlType;
        use crate::types::DataType;

        assert_eq!(
            cql_type_to_data_type(&CqlType::List(Box::new(CqlType::Int))),
            DataType::List
        );
        assert_eq!(
            cql_type_to_data_type(&CqlType::Set(Box::new(CqlType::Text))),
            DataType::Set
        );
        assert_eq!(
            cql_type_to_data_type(&CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::BigInt)
            )),
            DataType::Map
        );
    }

    #[test]
    fn test_cql_type_to_data_type_frozen() {
        use super::cql_type_to_data_type;
        use crate::schema::CqlType;
        use crate::types::DataType;

        // frozen<list<int>> should unwrap to DataType::List
        assert_eq!(
            cql_type_to_data_type(&CqlType::Frozen(Box::new(CqlType::List(Box::new(
                CqlType::Int
            ))))),
            DataType::List
        );
    }

    #[test]
    fn test_column_info_to_json_includes_cql_type() {
        use crate::schema::CqlType;
        let column = ColumnInfo::new("items".to_string(), crate::types::DataType::List, true, 0)
            .with_cql_type(CqlType::List(Box::new(CqlType::Int)));

        let json = column.to_json();
        let obj = json.as_object().unwrap();

        // Existing keys unchanged
        assert_eq!(obj["name"], "items");
        assert!(obj.contains_key("data_type"));
        assert!(obj.contains_key("nullable"));
        assert!(obj.contains_key("position"));

        // New key present
        assert!(obj.contains_key("cql_type"));
        assert_eq!(obj["cql_type"], "list<int>");
    }

    #[test]
    fn test_column_info_to_json_no_cql_type() {
        // Without cql_type, the JSON must not include the key
        let column = ColumnInfo::new("id".to_string(), crate::types::DataType::Integer, false, 0);

        let json = column.to_json();
        let obj = json.as_object().unwrap();

        assert!(!obj.contains_key("cql_type"));
    }

    #[test]
    fn test_row_metadata() {
        let metadata = RowMetadata::new()
            .with_version(123)
            .with_ttl(3600)
            .with_tag("source".to_string(), "import".to_string());

        assert_eq!(metadata.version, Some(123));
        assert_eq!(metadata.ttl, Some(3600));
        assert_eq!(metadata.tags.get("source"), Some(&"import".to_string()));
    }

    #[test]
    fn test_performance_metrics() {
        let mut metrics = PerformanceMetrics::new();
        metrics.cache_hits = 8;
        metrics.cache_misses = 2;
        metrics.total_time_us = 5000;

        assert_eq!(metrics.cache_hit_ratio(), 0.8);
        assert_eq!(metrics.total_time_ms(), 5);
    }

    #[test]
    fn test_json_serialization() {
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set("id".to_string(), Value::Integer(1));
        row.set("name".to_string(), Value::text("test".to_string()));

        let json = row.to_json();
        assert!(json.is_object());

        let obj = json.as_object().unwrap();
        assert_eq!(obj.get("id"), Some(&serde_json::Value::Number(1.into())));
        assert_eq!(
            obj.get("name"),
            Some(&serde_json::Value::String("test".to_string()))
        );
    }

    #[test]
    fn test_result_iteration() {
        let row1 = QueryRow::new(RowKey::new(vec![1]));
        let row2 = QueryRow::new(RowKey::new(vec![2]));
        let result = QueryResult::with_rows(vec![row1, row2]);

        let mut count = 0;
        for _row in &result {
            count += 1;
        }
        assert_eq!(count, 2);

        let mut count = 0;
        for _row in result {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    // =========================================================================
    // Issue #691: per-cell writetime/TTL metadata plumbing tests
    // =========================================================================

    /// Hot-path guarantee: a row constructed without setting cell metadata
    /// must have `cell_metadata == None` — no allocation.
    #[test]
    fn test_cell_metadata_absent_by_default() {
        let row = QueryRow::new(RowKey::new(vec![1]));
        assert!(
            row.cell_metadata.is_none(),
            "cell_metadata must be None when no metadata is attached (hot-path, zero allocation)"
        );

        let row2 = QueryRow::with_values(RowKey::new(vec![2]), HashMap::new());
        assert!(row2.cell_metadata.is_none());

        let row3 = QueryRow::from_map(HashMap::new());
        assert!(row3.cell_metadata.is_none());
    }

    /// ProjectionFlags::default() must not request cell metadata.
    #[test]
    fn test_projection_flags_default_no_metadata() {
        let flags = ProjectionFlags::default();
        assert!(
            !flags.include_cell_metadata,
            "include_cell_metadata must default to false"
        );
    }

    /// Single SSTable scenario: attach metadata to a row and read it back.
    #[test]
    fn test_cell_metadata_single_sstable_single_cell() {
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set("name".to_string(), Value::text("Alice".to_string()));

        let meta = CellWriteMetadata {
            write_timestamp_micros: 1_700_000_000_000_000, // ~2023 epoch in µs
            expiration: None,
        };
        row.insert_cell_metadata("name".to_string(), meta.clone());

        // cell_metadata map must now be Some
        assert!(row.cell_metadata.is_some());
        // Values are unchanged
        assert_eq!(row.get("name"), Some(&Value::text("Alice".to_string())));
        // Metadata round-trips correctly
        let got = row
            .get_cell_metadata("name")
            .expect("metadata must be present");
        assert_eq!(got.write_timestamp_micros, meta.write_timestamp_micros);
        assert!(got.expiration.is_none());
    }

    /// TTL / expiration path: metadata includes expiry info.
    #[test]
    fn test_cell_metadata_with_ttl_expiration() {
        let mut row = QueryRow::new(RowKey::new(vec![2]));
        row.set("score".to_string(), Value::Integer(42));

        let ttl_seconds = 3600_i32;
        let write_ts_micros = 1_700_000_000_000_000_i64;
        // expires_at = write_ts / 1_000_000 + ttl
        let expires_at = (write_ts_micros / 1_000_000) + ttl_seconds as i64;

        let meta = CellWriteMetadata {
            write_timestamp_micros: write_ts_micros,
            expiration: Some(CellExpiration {
                ttl_seconds,
                expires_at_seconds: expires_at,
            }),
        };
        row.insert_cell_metadata("score".to_string(), meta);

        let got = row.get_cell_metadata("score").unwrap();
        assert_eq!(got.write_timestamp_micros, write_ts_micros);
        let exp = got.expiration.as_ref().unwrap();
        assert_eq!(exp.ttl_seconds, 3600);
        assert_eq!(exp.expires_at_seconds, expires_at);
    }

    /// Null cell: metadata is absent for columns not decoded (e.g. partition-key
    /// columns reconstructed from the raw key bytes, not from cells).
    #[test]
    fn test_cell_metadata_absent_for_null_cells() {
        let mut row = QueryRow::new(RowKey::new(vec![3]));
        row.set("id".to_string(), Value::Null);
        // We do NOT insert metadata for "id" — simulating a null/missing cell.
        // Even if metadata is enabled for other columns, this one should be absent.
        row.insert_cell_metadata(
            "name".to_string(),
            CellWriteMetadata {
                write_timestamp_micros: 42,
                expiration: None,
            },
        );

        assert!(
            row.get_cell_metadata("id").is_none(),
            "no metadata for null column"
        );
        assert!(row.get_cell_metadata("name").is_some());
    }

    /// Two SSTables with the same key: the SURVIVING (newer) cell's metadata must
    /// be the one carried.  This test simulates the LWW merge decision by
    /// constructing the two candidate rows (as would be produced by two SSTable
    /// reads), selecting the winner by timestamp, and asserting the winner's
    /// metadata is the one present.
    ///
    /// The merge itself (tombstone_merger.rs) is tested at the unit level in that
    /// module; here we only verify that the metadata carrier (`QueryRow`) can
    /// hold the winning metadata and that callers can correctly replace it.
    #[test]
    fn test_cell_metadata_lww_winner_carries_newer_timestamp() {
        // Older SSTable: timestamp 1_000_000 µs
        let mut older_row = QueryRow::new(RowKey::new(b"partition1".to_vec()));
        older_row.set("value".to_string(), Value::Integer(10));
        older_row.insert_cell_metadata(
            "value".to_string(),
            CellWriteMetadata {
                write_timestamp_micros: 1_000_000,
                expiration: None,
            },
        );

        // Newer SSTable: timestamp 2_000_000 µs — this one wins the LWW merge.
        let mut newer_row = QueryRow::new(RowKey::new(b"partition1".to_vec()));
        newer_row.set("value".to_string(), Value::Integer(20));
        newer_row.insert_cell_metadata(
            "value".to_string(),
            CellWriteMetadata {
                write_timestamp_micros: 2_000_000,
                expiration: None,
            },
        );

        // Simulate LWW merge: pick the row with the higher write timestamp.
        let winner = if newer_row
            .get_cell_metadata("value")
            .map(|m| m.write_timestamp_micros)
            .unwrap_or(0)
            > older_row
                .get_cell_metadata("value")
                .map(|m| m.write_timestamp_micros)
                .unwrap_or(0)
        {
            newer_row
        } else {
            older_row
        };

        assert_eq!(
            winner.get("value"),
            Some(&Value::Integer(20)),
            "value from the newer SSTable must be present"
        );
        assert_eq!(
            winner
                .get_cell_metadata("value")
                .map(|m| m.write_timestamp_micros),
            Some(2_000_000),
            "metadata must reflect the winning (newer) cell's timestamp"
        );
    }

    /// `set_cell_metadata` replaces the entire map atomically.
    #[test]
    fn test_set_cell_metadata_replaces_map() {
        let mut row = QueryRow::new(RowKey::new(vec![4]));
        row.insert_cell_metadata(
            "a".to_string(),
            CellWriteMetadata {
                write_timestamp_micros: 1,
                expiration: None,
            },
        );

        let mut new_map = HashMap::new();
        new_map.insert(
            "b".to_string(),
            CellWriteMetadata {
                write_timestamp_micros: 99,
                expiration: None,
            },
        );
        row.set_cell_metadata(new_map);

        // Old key "a" gone; new key "b" present.
        assert!(row.get_cell_metadata("a").is_none());
        assert_eq!(
            row.get_cell_metadata("b").map(|m| m.write_timestamp_micros),
            Some(99)
        );
    }

    /// Serde round-trip: `cell_metadata` serialises and deserialises correctly.
    #[test]
    fn test_cell_metadata_serde_round_trip() {
        let mut row = QueryRow::new(RowKey::new(vec![5]));
        row.set("x".to_string(), Value::Integer(7));
        row.insert_cell_metadata(
            "x".to_string(),
            CellWriteMetadata {
                write_timestamp_micros: 123_456_789,
                expiration: Some(CellExpiration {
                    ttl_seconds: 60,
                    expires_at_seconds: 9999,
                }),
            },
        );

        let json = serde_json::to_string(&row).expect("serialise");
        let back: QueryRow = serde_json::from_str(&json).expect("deserialise");

        let meta = back
            .get_cell_metadata("x")
            .expect("metadata present after round-trip");
        assert_eq!(meta.write_timestamp_micros, 123_456_789);
        let exp = meta.expiration.as_ref().unwrap();
        assert_eq!(exp.ttl_seconds, 60);
        assert_eq!(exp.expires_at_seconds, 9999);
    }

    /// When `cell_metadata` is `None`, the JSON output must not include the field
    /// (the `skip_serializing_if` attribute ensures backward compatibility).
    #[test]
    fn test_cell_metadata_none_omitted_from_json() {
        let row = QueryRow::new(RowKey::new(vec![6]));
        let json = serde_json::to_string(&row).expect("serialise");
        assert!(
            !json.contains("cell_metadata"),
            "cell_metadata must be absent from JSON when None (backward compat)"
        );
    }

    // =========================================================================
    // Issue #1334: interned Arc<str> column-name keys in QueryRow.values
    // =========================================================================

    /// Cells are addressable by `&str` even though the key type is now
    /// `Arc<str>` (via `Arc<str>: Borrow<str>`), and values are unchanged.
    #[test]
    fn test_row_values_addressable_by_str_key() {
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set("id", Value::Integer(7)); // &str key
        row.set("name".to_string(), Value::text("Zoe".to_string())); // String key

        // `get(&str)` works unchanged.
        assert_eq!(row.get("id"), Some(&Value::Integer(7)));
        assert_eq!(row.get("name"), Some(&Value::text("Zoe".to_string())));
        assert_eq!(row.get("absent"), None);

        // The key type is a shared Arc<str> handle.
        let key: &Arc<str> = row.values.keys().next().expect("a key");
        let _: &str = key; // Deref/Borrow to str compiles.
    }

    /// serde round-trip of a `QueryResult` (requires serde's `rc` feature for
    /// the `Arc<str>` key): names and values survive and the JSON object shape
    /// (name→value) is unchanged.
    #[test]
    fn test_query_result_serde_round_trip_preserves_names_and_values() {
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set("id", Value::Integer(42));
        row.set("name", Value::text("Alice".to_string()));
        let result = QueryResult::with_rows(vec![row]);

        // Serialize → the values object is keyed by the plain column-name string
        // (an Arc<str> serializes as its str, requiring serde's `rc` feature),
        // NOT by any Arc-wrapper shape. The per-value encoding is serde's derived
        // enum form (unchanged by this change); we only assert the name→value
        // OBJECT SHAPE here and rely on the round-trip below for value equality.
        let json = serde_json::to_value(&result).expect("serialise QueryResult");
        let values_obj = json["rows"][0]["values"]
            .as_object()
            .expect("values is a JSON object keyed by column name");
        assert!(values_obj.contains_key("id"), "object keyed by column name");
        assert!(
            values_obj.contains_key("name"),
            "object keyed by column name"
        );

        // Deserialize back → identical names and values.
        let back: QueryResult = serde_json::from_value(json).expect("deserialise QueryResult");
        let r = &back.rows[0];
        assert_eq!(r.get("id"), Some(&Value::Integer(42)));
        assert_eq!(r.get("name"), Some(&Value::text("Alice".to_string())));
    }
}
