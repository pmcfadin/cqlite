//! Query result types for CQLite
//!
//! This module provides result types and utilities for query execution results.
//! It includes result set management, row iteration, and result metadata.

use crate::{RowKey, Value};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt;
use tokio::sync::mpsc;

/// Encode bytes as standard base64 (used across JSON serializers below).
fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// True if `RowMetadata` carries anything worth serializing.
fn row_metadata_is_populated(meta: &RowMetadata) -> bool {
    meta.version.is_some() || meta.ttl.is_some() || !meta.tags.is_empty()
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
    /// Column values mapped by column name
    pub values: HashMap<String, Value>,
    /// Original row key
    pub key: RowKey,
    /// Row metadata
    pub metadata: RowMetadata,
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
}

/// Information about a column in the result set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: crate::types::DataType,
    /// Whether column can be null
    pub nullable: bool,
    /// Column position in result set
    pub position: usize,
    /// Original table name (for joined queries)
    pub table_name: Option<String>,
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
                    .get(&col.name)
                    .map_or(serde_json::Value::Null, ToJson::to_json);
                result.insert(col.name.clone(), value_json);
            }
        } else {
            let mut sorted_keys: Vec<&String> = row.values.keys().collect();
            sorted_keys.sort();
            for key in sorted_keys {
                if let Some(value) = row.values.get(key) {
                    result.insert(key.clone(), value.to_json());
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
        }
    }

    /// Create a row with values
    pub fn with_values(key: RowKey, values: HashMap<String, Value>) -> Self {
        Self {
            values,
            key,
            metadata: RowMetadata::default(),
        }
    }

    /// Get a value by column name
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.values.get(column)
    }

    /// Set a value for a column
    pub fn set(&mut self, column: String, value: Value) {
        self.values.insert(column, value);
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
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

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        for (column, value) in &self.values {
            result.insert(column.clone(), value.to_json());
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
        }
    }

    /// Set table name
    pub fn with_table_name(mut self, table_name: String) -> Self {
        self.table_name = Some(table_name);
        self
    }

    /// Convert to JSON representation
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
        serde_json::Value::Object(map)
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
                    .filter_map(|row| row.values.get(col_name))
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
                    .get(col_name)
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
            Value::Text(s) => json!(s),
            Value::Json(value) => value.clone(),
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
            Value::Udt(udt) => {
                let mut json_obj = serde_json::Map::new();
                json_obj.insert("_type".to_string(), json!(udt.type_name));
                for field in &udt.fields {
                    let field_json = field
                        .value
                        .as_ref()
                        .map_or(serde_json::Value::Null, ToJson::to_json);
                    json_obj.insert(field.name.clone(), field_json);
                }
                serde_json::Value::Object(json_obj)
            }
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
        row1.set("name".to_string(), Value::Text("Alice".to_string()));

        let mut row2 = QueryRow::new(RowKey::new(vec![2]));
        row2.set("id".to_string(), Value::Integer(2));
        row2.set("name".to_string(), Value::Text("Bob".to_string()));

        let result = QueryResult::with_rows(vec![row1, row2]);
        assert_eq!(result.row_count(), 2);
        assert!(!result.is_empty());

        let first_row = result.get_row(0).unwrap();
        assert_eq!(first_row.get("id"), Some(&Value::Integer(1)));
        assert_eq!(
            first_row.get("name"),
            Some(&Value::Text("Alice".to_string()))
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
        row.set("name".to_string(), Value::Text("test".to_string()));

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
}
