//! Database wrapper for Node.js bindings.
//!
//! This module provides the `Database` class for Node.js access to
//! CQLite's SSTable reading capabilities.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use napi_derive::napi;

use crate::error::{simple_error, to_napi_error};

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

    /// Query execution time in milliseconds.
    pub execution_time_ms: u32,

    /// Column metadata for the result set.
    /// Contains information about each column's name, type, and nullability.
    pub columns: Vec<ColumnInfo>,
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
/// ## Thread Safety
///
/// Database handles are thread-safe and can be shared across worker threads.
/// The `close()` method is idempotent - calling it multiple times is safe.
#[napi]
pub struct Database {
    inner: Arc<cqlite_core::Database>,
    closed: AtomicBool,
}

impl Database {
    /// Check if database is open, returning error if closed.
    fn ensure_open(&self) -> napi::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            Err(simple_error("Database is closed"))
        } else {
            Ok(())
        }
    }
}

#[napi]
impl Database {
    /// Opens a database at the specified data directory.
    ///
    /// @param dataDir - Path to the SSTable data directory
    /// @param options - Optional configuration (schema path, etc.)
    /// @returns Promise resolving to a Database instance
    ///
    /// @example
    /// ```javascript
    /// // Basic open
    /// const db = await Database.open('/path/to/sstables');
    ///
    /// // With schema file
    /// const db = await Database.open('/path/to/sstables', {
    ///   schema: '/path/to/schema.cql'
    /// });
    /// ```
    #[napi(factory)]
    pub async fn open(
        data_dir: String,
        options: Option<DatabaseOptions>,
    ) -> napi::Result<Database> {
        let path = PathBuf::from(&data_dir);

        // Extract all options and build config
        let (schema_path, core_config) = if let Some(opts) = options {
            let mut config = cqlite_core::Config::default();

            if let Some(limit) = opts.memory_limit {
                if !limit.is_finite() {
                    return Err(napi::Error::from_reason(
                        "memoryLimit must be a finite number",
                    ));
                }
                if limit <= 0.0 {
                    return Err(napi::Error::from_reason(
                        "memoryLimit must be greater than 0",
                    ));
                }
                config.memory.max_memory = limit as u64;
            }

            if let Some(enabled) = opts.cache_enabled {
                config.memory.block_cache.enabled = enabled;
                config.memory.row_cache.enabled = enabled;
                config.memory.query_cache.enabled = enabled;
            }

            (opts.schema.map(PathBuf::from), config)
        } else {
            (None, cqlite_core::Config::default())
        };

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

        Ok(Database {
            inner: Arc::new(db),
            closed: AtomicBool::new(false),
        })
    }

    /// Execute a CQL query and return results.
    ///
    /// Executes a query against the database and returns all matching rows.
    /// For large result sets, consider using streaming (future feature).
    ///
    /// @param query - CQL SELECT statement to execute
    /// @returns Promise resolving to QueryResult with rows and metadata
    ///
    /// @example
    /// ```javascript
    /// const result = await db.execute('SELECT * FROM users LIMIT 10');
    /// console.log(`Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
    /// for (const row of result.rows) {
    ///   console.log(row.name);
    /// }
    /// ```
    #[napi]
    pub async fn execute(&self, query: String) -> napi::Result<QueryResult> {
        self.ensure_open()?;

        let core_result = self.inner.execute(&query).await.map_err(to_napi_error)?;

        // Convert rows to JSON values
        let rows: Vec<serde_json::Value> = core_result
            .rows
            .iter()
            .map(|row| {
                #[allow(deprecated)]
                let obj: serde_json::Map<String, serde_json::Value> = row
                    .values
                    .iter()
                    .map(|(k, v)| (k.clone(), value_to_json(v)))
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

        Ok(QueryResult {
            row_count: rows.len() as u32,
            rows,
            execution_time_ms: core_result.execution_time_ms as u32,
            columns,
        })
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
    /// @returns Promise resolving to StreamingResult async iterator
    ///
    /// @example
    /// ```javascript
    /// const stream = await db.executeStreaming('SELECT * FROM large_table');
    /// for await (const row of stream) {
    ///   console.log(row.name);
    /// }
    ///
    /// // With custom config for memory constraints
    /// const config = { bufferSize: 256, chunkSize: 2500 };
    /// for await (const row of await db.executeStreaming(query, config)) {
    ///   process(row);
    /// }
    ///
    /// // Early termination is safe - resources cleaned up automatically
    /// const stream = await db.executeStreaming('SELECT * FROM huge_table');
    /// for await (const row of stream) {
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
        self.ensure_open()?;

        // Convert config or use defaults
        let core_config = match config {
            Some(c) => c.to_core()?,
            None => cqlite_core::query::result::StreamingConfig::default(),
        };

        // Execute streaming query via core library
        let iter = self
            .inner
            .execute_streaming(&query, core_config)
            .await
            .map_err(to_napi_error)?;

        // Create StreamingResult with shared runtime
        crate::streaming::StreamingResult::new(iter)
    }

    /// Execute a CQL query and return results with native JavaScript types.
    ///
    /// This method returns native JavaScript types instead of JSON:
    /// - BigInt for bigint/counter columns (preserves 64-bit precision)
    /// - Buffer for blob columns
    /// - Date for timestamp/date columns
    /// - Set for set columns
    /// - Map for map columns
    ///
    /// @param query - CQL SELECT statement to execute
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
    /// ```
    #[napi(
        js_name = "executeNative",
        ts_return_type = "Promise<{rows: object[], rowCount: number, executionTimeMs: number, columns: ColumnInfo[]}>"
    )]
    pub fn execute_native(
        &self,
        query: String,
    ) -> napi::Result<napi::bindgen_prelude::AsyncTask<ExecuteNativeTask>> {
        self.ensure_open()?;
        Ok(napi::bindgen_prelude::AsyncTask::new(ExecuteNativeTask {
            inner: self.inner.clone(),
            query,
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
}

/// Async task for executing queries with native type conversion.
pub struct ExecuteNativeTask {
    inner: Arc<cqlite_core::Database>,
    query: String,
}

/// Intermediate result from async query execution.
pub struct QueryResultData {
    rows: Vec<std::collections::HashMap<String, cqlite_core::types::Value>>,
    execution_time_ms: u32,
    columns: Vec<cqlite_core::query::result::ColumnInfo>,
}

impl napi::Task for ExecuteNativeTask {
    type Output = QueryResultData;
    type JsValue = napi::JsObject;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        // Use global runtime for async execution
        let result =
            crate::runtime::block_on(self.inner.execute(&self.query)).map_err(to_napi_error)?;

        Ok(QueryResultData {
            rows: result.rows.iter().map(|r| r.values.clone()).collect(),
            execution_time_ms: result.execution_time_ms as u32,
            columns: result.metadata.columns.clone(),
        })
    }

    fn resolve(&mut self, env: napi::Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        let mut result_obj = env.create_object()?;

        // Create rows array with native types
        let mut rows_arr = env.create_array_with_length(output.rows.len())?;
        for (i, row_values) in output.rows.iter().enumerate() {
            let row_obj = crate::value::row_to_object(&env, row_values)?;
            rows_arr.set_element(i as u32, row_obj)?;
        }

        result_obj.set_named_property("rows", rows_arr)?;
        result_obj.set_named_property("rowCount", env.create_uint32(output.rows.len() as u32)?)?;
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
            serde_json::Value::String(format!("0x{}", hex_str))
        }
        Value::Decimal { scale, unscaled } => {
            // Represent as string to preserve precision
            let hex_str = hex::encode(unscaled);
            serde_json::Value::String(format!("decimal:{}:0x{}", scale, hex_str))
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
