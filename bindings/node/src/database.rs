//! Database wrapper for Node.js bindings.
//!
//! This module provides the `Database` class for Node.js access to
//! CQLite's SSTable reading capabilities.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use napi_derive::napi;

use crate::error::{simple_error, to_napi_error};

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
        let schema_path = options.and_then(|o| o.schema).map(PathBuf::from);

        let db = if let Some(schema) = schema_path {
            // Use ingestion module for schema + SSTable discovery
            let ingestion_config = cqlite_core::ingestion::IngestionConfig {
                schema_paths: vec![schema],
                data_dir: path,
                version_hint: None,
                core_config: cqlite_core::Config::default(),
                table_directory_filter: None,
            };

            let result = cqlite_core::ingestion::ingest(ingestion_config)
                .await
                .map_err(to_napi_error)?;

            result.database
        } else {
            // Simple open without schema
            cqlite_core::Database::open(&path, cqlite_core::Config::default())
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

        Ok(QueryResult {
            row_count: rows.len() as u32,
            rows,
            execution_time_ms: core_result.execution_time_ms as u32,
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
        ts_return_type = "Promise<{rows: object[], rowCount: number, executionTimeMs: number}>"
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
}

impl napi::Task for ExecuteNativeTask {
    type Output = QueryResultData;
    type JsValue = napi::JsObject;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        // Create a new runtime for this blocking task
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| napi::Error::from_reason(format!("Failed to create runtime: {}", e)))?;

        let result = rt
            .block_on(self.inner.execute(&self.query))
            .map_err(to_napi_error)?;

        Ok(QueryResultData {
            rows: result.rows.iter().map(|r| r.values.clone()).collect(),
            execution_time_ms: result.execution_time_ms as u32,
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
            // ISO 8601 timestamp string
            let secs = ts / 1000;
            let nanos = ((ts % 1000).unsigned_abs() * 1_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
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
}
