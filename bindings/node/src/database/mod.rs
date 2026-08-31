//! Database wrapper for Node.js bindings.
//!
//! This module provides the `Database` class for Node.js access to
//! CQLite's SSTable reading and writing capabilities.
//!
//! Split into concern submodules under the campsite rule (epic #1116, issue
//! #1464): `options` holds the napi object DTOs, `open` the factory, `write`
//! the write-path methods, `execute_native_task` the `executeNative()` task and
//! `json_value` the deprecated legacy JSON shaping. Every path that resolved
//! through `database::…` before still resolves, via the re-exports below.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use napi_derive::napi;

use crate::error::{simple_error, to_napi_error};
// `runtime_init_error` is only reachable from the DML branch of `execute()`.
#[cfg(feature = "write-support")]
use crate::error::runtime_init_error;

#[cfg(feature = "write-support")]
use std::sync::Mutex;

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

    /// The write engine, or a typed error if the database is read-only.
    /// Replaces the `ensure_writable()? … .expect(Some)` two-step so the `Some`
    /// unwrap is compiler-enforced (no reachable panic in a write path).
    #[cfg(feature = "write-support")]
    fn writable_engine(
        &self,
    ) -> napi::Result<&Arc<Mutex<cqlite_core::storage::write_engine::WriteEngine>>> {
        self.write_engine.as_ref().ok_or_else(|| {
            simple_error(
                "Write support not enabled. Open with { writable: true, writeDir: '<path>' } \
                 to enable write operations.",
            )
        })
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

// Submodule declarations sit AFTER the `#[napi] struct Database` above, and must
// stay there: napi-derive expands in source order and refuses an `#[napi] impl`
// whose struct it has not parsed yet ("Did not find struct `Database` parsed
// before expand #[napi] for impl"). `open` and `write` both carry `#[napi] impl
// Database` blocks.
mod execute_native_task;
mod json_value;
mod open;
mod options;
mod write;

pub use execute_native_task::ExecuteNativeTask;
pub use options::{
    ColumnInfo, DatabaseOptions, DatabaseStats, MaintenanceOptions, MaintenanceReport,
    ParquetExportOptions, QueryResult, StreamingConfig, WriteStats,
};

#[napi]
impl Database {
    /// Execute a CQL query or write statement and return results.
    ///
    /// **DEPRECATED — removed in the next major. Use `executeNative()` instead.**
    ///
    /// For SELECT queries, returns matching rows as lossy legacy JSON encodings:
    /// blob → base64 string, timestamp → ISO-8601 string, varint → `"0x{hex}"`,
    /// decimal → `"decimal:{scale}:0x{hex}"`, date/time → number. It also
    /// double-converts (JSON off-loop, then JS on-loop) so it is slower than
    /// `executeNative()`. The JS wrapper (`lib/error-wrapper.js`) emits a
    /// one-time `DeprecationWarning` on first call. (BigInt/Counter are
    /// currently returned as an exact JS `BigInt` on this napi build.)
    ///
    /// For INSERT/UPDATE/DELETE, executes the write and returns `rowsAffected`.
    /// For large result sets, consider using streaming via `executeStreaming()`.
    ///
    /// @param query - CQL statement to execute
    /// @returns Promise resolving to QueryResult with rows and metadata
    ///
    /// @example
    /// ```javascript
    /// // Read (recommended: executeNative() for native types with full precision)
    /// const result = await db.executeNative('SELECT * FROM users LIMIT 10');
    /// console.log(`Got ${result.rowCount} rows in ${result.executionTimeMs}ms`);
    ///
    /// // Write (requires writable: true in open options)
    /// const wr = await db.executeNative("INSERT INTO users (id, name) VALUES (uuid(), 'Alice')");
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
                let we_clone = Arc::clone(self.writable_engine()?);
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
                    // Outer `?` folds a runtime-init `io::Error` into napi; inner
                    // `?` propagates the core error (issue #1438).
                    let n = crate::runtime::block_on(engine.execute_flushing(&query))
                        .map_err(runtime_init_error)?
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

            // Convert rows to JSON values. A refusal (e.g. a malformed inet
            // length) propagates out of `execute()` instead of being embedded in
            // the row as a null — see `value_to_json` (issue #1452).
            let mut rows: Vec<serde_json::Value> = Vec::with_capacity(core_result.rows.len());
            for row in &core_result.rows {
                let mut obj = serde_json::Map::with_capacity(row.values.len());
                for (k, v) in &row.values {
                    #[allow(deprecated)]
                    let json = json_value::value_to_json(v)?;
                    obj.insert(k.to_string(), json);
                }
                rows.push(serde_json::Value::Object(obj));
            }

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
        let batch_size = core_config.buffer_size; // per-`next()` batch (#1443), before move

        // Execute streaming query; setup instrumented by the stream span (no guard across `.await`).
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

        // Create StreamingResult with the stream span + per-`next()` batch size.
        crate::streaming::StreamingResult::new(iter, span_for_iter, batch_size)
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

    /// Execute a CQL query or write statement, returning native JavaScript types
    /// (`BigInt`, `Buffer`, `Date`, `Set`, `Map`). For INSERT/UPDATE/DELETE,
    /// `rowsAffected` is 1 and `rows` is empty. See `lib/index.d.ts` for details.
    ///
    /// ## Performance — O(rows) on the event-loop thread
    ///
    /// The result is scanned off the event loop, but each row's JS object is
    /// built on the event-loop thread (napi `Env` is thread-bound) — O(rows) of
    /// on-loop work that cannot be moved off-loop. Use `executeStreaming()` for
    /// result sets beyond ~a few thousand rows; sets larger than
    /// `CQLITE_NODE_MAX_NATIVE_ROWS` (default 100_000) are rejected with a typed
    /// error rather than freezing timers/HTTP handlers (issue #1442).
    ///
    /// @param query - CQL statement to execute
    /// @returns Promise resolving to NativeQueryResult with native typed rows
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
            max_native_rows: crate::error::native_row_limit(),
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
}
