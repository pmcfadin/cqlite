//! Database wrapper for Python bindings.
//!
//! This module provides the `Database` class and `open()` function
//! for Python access to CQLite's SSTable reading and writing capabilities.
//!
//! ## Write Support
//!
//! When `Database.open()` is called with `writable=True` **and** a `write_dir`
//! path, the database initialises a [`WriteEngine`] that backs INSERT/UPDATE/DELETE
//! statements executed through the unified `db.execute()` API.  The caller must
//! also supply `schema=` so that the write engine can resolve column types.
//!
//! ```python
//! import cqlite
//!
//! with cqlite.open(
//!     "test-data/datasets/sstables",
//!     schema="test-data/schemas/basic-types.cql",
//!     writable=True,
//!     write_dir="/tmp/cqlite-writes",
//! ) as db:
//!     db.execute("INSERT INTO test_basic.simple_table ...")
//!     path = db.flush_run()
//! ```
//!
//! Read-only mode (the default) is unchanged; all existing call-sites continue
//! to work without modification.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::config::StreamingConfig;
use crate::error::{runtime_init_to_py_err, to_py_err};
use crate::prepared::PreparedStatement;
use crate::result::{QueryResult, StreamingIterator};
use crate::runtime::block_on;
use crate::stats::DatabaseStats;
use crate::write::PyWriteEngine;

// Submodule declarations sit AFTER the `use` block; `open` is reached under an
// alias because pyo3's `#[pyfunction]` generates a hidden module with the SAME
// name as the function, so `mod open;` + `pub use open::open;` would define
// `open` twice in the type namespace.
#[path = "open.rs"]
mod open_fn;
mod write_methods;

pub use open_fn::open;

/// A CQLite database handle.
///
/// Use `cqlite.open()` to create a Database instance.
/// Always close the database when done, either explicitly with `close()`
/// or by using the context manager protocol.
///
/// # Example
///
/// ```python
/// # Using context manager (recommended)
/// with cqlite.open("/path/to/data") as db:
///     # use database
///     pass
/// # automatically closed
///
/// # Manual management
/// db = cqlite.open("/path/to/data")
/// try:
///     # use database
///     pass
/// finally:
///     db.close()
/// ```
#[pyclass(module = "cqlite")]
pub struct Database {
    pub(crate) inner: Arc<cqlite_core::Database>,
    /// Closed flag, shared (via `Arc`) with any `StreamingIterator` this
    /// database hands out (issue #1462). `Arc<AtomicBool>` derefs to
    /// `AtomicBool`, so every existing `.load(..)`/`.swap(..)` call site is
    /// unchanged; only the field type + initialization change. Sharing the
    /// exact atomic lets an iterator observe `close()` atomically and raise a
    /// clean `RuntimeError` from `__next__` instead of driving a torn-down
    /// engine.
    pub(crate) closed: Arc<AtomicBool>,
    /// Optional write engine — present only when opened with `writable=True`.
    /// A `tokio::sync::Mutex` (not `std`) so its guard is `Send` and survives the
    /// `py.allow_threads` boundary (issue #1444): blocking writes (WAL fsync +
    /// SSTable materialization) run with the GIL released, matching the read path.
    pub(crate) write_engine: Option<Arc<AsyncMutex<PyWriteEngine>>>,
    /// W3C `traceparent` captured at `open` time (issue #1039). When set, every
    /// per-call span is re-parented to this trace unless a per-call traceparent
    /// overrides it, so the bindings' Rust spans correlate with the caller's
    /// Python OpenTelemetry trace.
    default_traceparent: Option<String>,
}

impl Database {
    /// Check if database is open, raising RuntimeError if closed.
    pub(crate) fn ensure_open(&self) -> PyResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            Err(PyRuntimeError::new_err("Database is closed"))
        } else {
            Ok(())
        }
    }

    /// Get a clone of the inner database Arc.
    ///
    /// Returns an Arc clone to allow async operations that may outlive
    /// the borrow of self.
    pub(crate) fn inner(&self) -> Arc<cqlite_core::Database> {
        Arc::clone(&self.inner)
    }

    /// Resolve the effective W3C traceparent for a call: the per-call value when
    /// supplied, otherwise the one captured at `open` time. Returns `None` when
    /// neither is set (the span keeps its natural parent).
    fn resolve_traceparent<'a>(&'a self, per_call: Option<&'a str>) -> Option<&'a str> {
        per_call.or(self.default_traceparent.as_deref())
    }

    /// Detect DML statements (INSERT / UPDATE / DELETE / BEGIN BATCH).
    ///
    /// Delegates to `cqlite_core::cql::is_dml_statement` — the single canonical
    /// implementation shared with the CLI for consistent routing semantics.
    fn is_dml_statement(query: &str) -> bool {
        cqlite_core::cql::is_dml_statement(query)
    }
}

#[pymethods]
impl Database {
    /// Close the database and release resources.
    ///
    /// This method is idempotent - calling it multiple times is safe.
    /// After closing, any operations on the database will raise RuntimeError.
    ///
    /// When the database was opened in writable mode the write engine is closed
    /// first, which flushes any remaining memtable data to disk.
    ///
    /// # Example
    ///
    /// ```python
    /// db = cqlite.open("/path/to/data")
    /// db.close()
    /// db.close()  # Safe to call again
    /// ```
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        // Atomically set closed flag, return early if already closed
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Close the write engine first (flushes remaining memtable). The tokio
        // guard is `Send`, so the flush-on-close I/O runs with the GIL released
        // (issue #1444), the same as every other write op. `blocking_lock` on the
        // synchronous calling thread + `block_on` inside mirrors `with_write_engine`
        // (never nest `block_on`s).
        if let Some(ref engine_arc) = self.write_engine {
            let engine = Arc::clone(engine_arc);
            py.allow_threads(move || {
                let mut guard = engine.blocking_lock();
                block_on(guard.inner.close())
                    .map_err(runtime_init_to_py_err)?
                    .map_err(to_py_err)
            })?;
        }

        // Shutdown the read-side storage engine
        py.allow_threads(|| block_on(self.inner.shutdown()))
            .map_err(runtime_init_to_py_err)?
            .map_err(to_py_err)?;

        // Flush buffered telemetry (issue #1039) so a short-lived script that
        // closes its database before interpreter shutdown still exports its
        // spans/metrics. Process-global and idempotent; the guard itself also
        // flushes on interpreter shutdown via Drop.
        crate::observability::flush();

        Ok(())
    }

    /// Check if the database is closed.
    ///
    /// # Returns
    ///
    /// True if the database has been closed, False otherwise.
    #[getter]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Context manager entry point.
    ///
    /// Returns self for use in `with` statements.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager exit point.
    ///
    /// Ensures the database is closed when exiting the context,
    /// even if an exception occurred.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false) // Don't suppress exceptions
    }

    /// String representation of the database.
    fn __repr__(&self) -> String {
        if self.closed.load(Ordering::SeqCst) {
            "Database(closed)".to_string()
        } else if self.write_engine.is_some() {
            "Database(open, writable)".to_string()
        } else {
            "Database(open)".to_string()
        }
    }

    /// Execute a CQL query or DML statement.
    ///
    /// For SELECT queries this reads from the SSTable data on disk.
    ///
    /// For INSERT, UPDATE, and DELETE statements the operation is routed to the
    /// write engine (requires the database to have been opened with
    /// `writable=True`). The returned `QueryResult` will have an empty `rows`
    /// list and `rows_affected` reflecting the number of mutations applied
    /// (typically 1).
    ///
    /// # Arguments
    ///
    /// * `query` - CQL statement to execute
    ///
    /// # Returns
    ///
    /// QueryResult containing rows/metadata for SELECT, or rows_affected for DML.
    ///
    /// # Raises
    ///
    /// * `QueryError`   – If query execution fails
    /// * `ParseError`   – If CQL syntax is invalid
    /// * `RuntimeError` – If database is closed, or if DML is attempted on a
    ///                    read-only database
    ///
    /// # Example
    ///
    /// ```python
    /// # Read
    /// result = db.execute("SELECT * FROM users LIMIT 10")
    /// print(f"Got {len(result)} rows in {result.execution_time_ms}ms")
    ///
    /// # Write (requires writable=True)
    /// result = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    /// print(f"Affected {result.rows_affected} row(s)")
    /// ```
    #[pyo3(signature = (query, *, traceparent=None))]
    pub fn execute(
        &self,
        py: Python<'_>,
        query: &str,
        traceparent: Option<&str>,
    ) -> PyResult<QueryResult> {
        self.ensure_open()?;

        // Per-call span (issue #1039). Core's query path (#1035) emits the
        // query.duration / query.rows metrics and marks the active span on
        // error, so we intentionally do NOT re-emit those here — we only own the
        // `python.execute` span and re-parent it to the caller's trace.
        let span = crate::observability::call_span("python.execute");
        crate::observability::set_traceparent_parent(&span, self.resolve_traceparent(traceparent));
        let _enter = span.enter();

        // Route DML statements to the write engine when present
        if Self::is_dml_statement(query) {
            let result = self.execute_dml(py, query)?;
            span.record("cqlite.rows", result.rows_affected_value());
            return Ok(result);
        }

        let db = self.inner();
        let query_owned = query.to_string();

        // Release GIL during async execution to allow other Python threads to run
        let core_result = py
            .allow_threads(|| block_on(db.execute(&query_owned)))
            .map_err(runtime_init_to_py_err)?
            .map_err(to_py_err)?;

        let result = QueryResult::from_core(py, core_result)?;
        span.record("cqlite.rows", result.row_count() as i64);
        Ok(result)
    }

    /// Execute a CQL query with streaming results.
    ///
    /// Returns an iterator that yields rows one at a time, keeping memory
    /// usage bounded by the `StreamingConfig` settings. Use this for large
    /// result sets that would not fit in memory.
    ///
    /// # Arguments
    ///
    /// * `query` - CQL SELECT statement to execute
    /// * `config` - Optional StreamingConfig for buffer/chunk sizes
    ///
    /// # Returns
    ///
    /// A StreamingIterator that yields Row objects.
    ///
    /// # Raises
    ///
    /// * `QueryError` - If query execution fails
    /// * `ParseError` - If CQL syntax is invalid
    /// * `RuntimeError` - If database is closed
    ///
    /// # Example
    ///
    /// ```python
    /// # Basic streaming
    /// for row in db.execute_streaming("SELECT * FROM large_table"):
    ///     process(row)
    ///
    /// # With custom config
    /// config = cqlite.StreamingConfig(buffer_size=512, chunk_size=5000)
    /// for row in db.execute_streaming("SELECT * FROM huge_table", config=config):
    ///     process(row)
    ///
    /// # Early termination is safe
    /// for row in db.execute_streaming("SELECT * FROM large_table"):
    ///     if row["id"] == target_id:
    ///         break
    /// ```
    #[pyo3(signature = (query, *, config=None, traceparent=None))]
    pub fn execute_streaming(
        &self,
        py: Python<'_>,
        query: &str,
        config: Option<&StreamingConfig>,
        traceparent: Option<&str>,
    ) -> PyResult<StreamingIterator> {
        self.ensure_open()?;

        // Per-call span (issue #1039). The span outlives this method: it is moved
        // into the StreamingIterator, which records the total rows yielded across
        // iteration when it is exhausted or dropped. Re-parent to the caller's
        // trace so the whole stream correlates with the Python OTel span.
        let span = crate::observability::call_span("python.execute_streaming");
        crate::observability::set_traceparent_parent(&span, self.resolve_traceparent(traceparent));
        let _enter = span.enter();

        let db = self.inner();
        let query_owned = query.to_string();
        let core_config = config.map(|c| c.to_core()).unwrap_or_default();

        // Release GIL during async execution
        let core_iter = py
            .allow_threads(|| block_on(db.execute_streaming(&query_owned, core_config)))
            .map_err(runtime_init_to_py_err)?
            .map_err(to_py_err)?;

        // Share the *same* closed atomic with the iterator (issue #1462) so a
        // `db.close()` that outlives this iterator is observed atomically and
        // `__next__` raises a clean RuntimeError instead of touching the
        // torn-down engine.
        Ok(StreamingIterator::with_span(
            core_iter,
            span.clone(),
            Arc::clone(&self.closed),
        ))
    }

    /// Export the results of a CQL query to a Parquet file.
    ///
    /// The query is executed with streaming, so arbitrarily large result
    /// sets are written within bounded memory (rows are flushed to Parquet
    /// row groups as they arrive). The GIL is released for the duration of
    /// the export.
    ///
    /// Types are mapped using the high-fidelity schema-driven mapping
    /// (Date32, Time64, Decimal128, FixedSizeBinary(16) + UUID extension,
    /// typed List/Map, Struct for UDTs/tuples). CQLite produces Parquet
    /// files only; committing files to Iceberg/Delta is out of scope.
    ///
    /// # Arguments
    ///
    /// * `query` - CQL SELECT statement to execute
    /// * `path` - Destination file path (created or truncated)
    /// * `row_group_size` - Rows per Parquet row group (default: 10000)
    /// * `compression` - "snappy" (default), "zstd", or "none"
    /// * `config` - Optional StreamingConfig for buffer/chunk sizes during the
    ///   underlying streaming scan (defaults to the engine default when unset)
    ///
    /// # Returns
    ///
    /// The number of rows written.
    ///
    /// # Raises
    ///
    /// * `ValueError` - If compression/row_group_size is invalid
    /// * `IOError` - If the file cannot be created or written
    /// * `QueryError` - If query execution fails
    /// * `RuntimeError` - If database is closed
    ///
    /// # Example
    ///
    /// ```python
    /// rows = db.export_parquet(
    ///     "SELECT * FROM my_ks.my_table",
    ///     "/tmp/out.parquet",
    ///     row_group_size=5000,
    ///     compression="zstd",
    /// )
    /// print(f"Exported {rows} row(s)")
    /// ```
    #[pyo3(signature = (query, path, *, row_group_size=10000, compression="snappy", config=None))]
    pub fn export_parquet(
        &self,
        py: Python<'_>,
        query: &str,
        path: &str,
        row_group_size: usize,
        compression: &str,
        config: Option<&StreamingConfig>,
    ) -> PyResult<u64> {
        use cqlite_core::export::parquet::{
            ParquetCompression, ParquetExportOptions, StreamingParquetWriter,
        };
        use pyo3::exceptions::PyIOError;

        self.ensure_open()?;

        let parquet_compression = match compression.to_ascii_lowercase().as_str() {
            "snappy" => ParquetCompression::Snappy,
            "zstd" => ParquetCompression::Zstd,
            "none" | "uncompressed" => ParquetCompression::Uncompressed,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown compression '{other}'; expected 'snappy', 'zstd', or 'none'"
                )))
            }
        };
        if row_group_size == 0 {
            return Err(PyValueError::new_err(
                "row_group_size must be greater than 0",
            ));
        }
        let options = ParquetExportOptions {
            row_limit: None,
            row_group_size,
            compression: parquet_compression,
        };

        let db = self.inner();
        let query_owned = query.to_string();
        let path_owned = PathBuf::from(path);
        // Resolve the streaming config while we still hold the GIL (the pyclass
        // ref cannot cross `allow_threads`); default when unset (issue #1463).
        let core_config = config.map(|c| c.to_core()).unwrap_or_default();

        // Release the GIL for the whole export (query + file writing).
        // Errors are split so each maps to the right Python exception:
        // core errors via to_py_err, writer/file errors to IOError.
        let result: Result<u64, PyErr> = py.allow_threads(|| {
            block_on(async {
                let mut iter = db
                    .execute_streaming(&query_owned, core_config)
                    .await
                    .map_err(to_py_err)?;

                let file = std::fs::File::create(&path_owned).map_err(|e| {
                    PyIOError::new_err(format!("failed to create {}: {e}", path_owned.display()))
                })?;

                let mut writer = StreamingParquetWriter::new(file, &iter.metadata, &options)
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;

                let mut chunk: Vec<cqlite_core::query::QueryRow> =
                    Vec::with_capacity(row_group_size.min(10_000));
                while let Some(row) = iter.next_async().await {
                    chunk.push(row.map_err(to_py_err)?);
                    if chunk.len() >= row_group_size {
                        writer
                            .write_chunk(&chunk)
                            .map_err(|e| PyIOError::new_err(e.to_string()))?;
                        chunk.clear();
                    }
                }
                if !chunk.is_empty() {
                    writer
                        .write_chunk(&chunk)
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;
                }
                writer
                    .finalize()
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;

                Ok(writer.rows_written())
            })
            // Fold a runtime-init failure into the same PyErr channel as the
            // export body so the caller sees a catchable exception (issue #1438).
            .map_err(runtime_init_to_py_err)
            .and_then(|inner| inner)
        });

        result
    }

    /// Prepare a CQL statement for repeated execution.
    ///
    /// Prepares and caches a query plan for the given CQL statement.
    /// Useful for queries that will be executed multiple times.
    ///
    /// # Arguments
    ///
    /// * `query` - CQL SELECT statement to prepare
    ///
    /// # Returns
    ///
    /// A PreparedStatement that can be inspected for statistics.
    ///
    /// # Raises
    ///
    /// * `ParseError` - If CQL syntax is invalid
    /// * `RuntimeError` - If database is closed
    ///
    /// # Example
    ///
    /// ```python
    /// stmt = db.prepare("SELECT * FROM users WHERE id = ?")
    /// print(f"Parameters: {stmt.parameter_count}")
    /// ```
    pub fn prepare(&self, py: Python<'_>, query: &str) -> PyResult<PreparedStatement> {
        self.ensure_open()?;

        let db = self.inner();
        let query_owned = query.to_string();

        let prepared = py
            .allow_threads(|| block_on(db.prepare(&query_owned)))
            .map_err(runtime_init_to_py_err)?
            .map_err(to_py_err)?;

        Ok(PreparedStatement::new(prepared))
    }

    /// Get database statistics.
    ///
    /// Returns comprehensive statistics about storage, memory usage,
    /// and query execution.
    ///
    /// # Returns
    ///
    /// DatabaseStats with storage_stats, memory_stats, and query_stats.
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If database is closed
    ///
    /// # Example
    ///
    /// ```python
    /// stats = db.stats()
    /// print(f"SSTables: {stats.storage_stats['sstable_count']}")
    /// ```
    pub fn stats(&self, py: Python<'_>) -> PyResult<DatabaseStats> {
        self.ensure_open()?;

        let db = self.inner();

        let core_stats = py
            .allow_threads(|| block_on(db.stats()))
            .map_err(runtime_init_to_py_err)?
            .map_err(to_py_err)?;

        DatabaseStats::from_core(py, core_stats)
    }
}

/// Register database types with the Python module.
pub fn register_database(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_function(wrap_pyfunction!(open, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_closed_state() {
        // Test that AtomicBool correctly tracks closed state
        let closed = AtomicBool::new(false);
        assert!(!closed.load(Ordering::SeqCst));

        let was_closed = closed.swap(true, Ordering::SeqCst);
        assert!(!was_closed);
        assert!(closed.load(Ordering::SeqCst));

        let was_closed = closed.swap(true, Ordering::SeqCst);
        assert!(was_closed);
    }

    #[test]
    fn test_is_dml_statement() {
        assert!(Database::is_dml_statement("INSERT INTO t (id) VALUES (1)"));
        assert!(Database::is_dml_statement("insert into t (id) values (1)"));
        assert!(Database::is_dml_statement(
            "UPDATE t SET x = 1 WHERE id = 1"
        ));
        assert!(Database::is_dml_statement("DELETE FROM t WHERE id = 1"));
        assert!(Database::is_dml_statement(
            "BEGIN BATCH INSERT INTO t (id) VALUES (1) APPLY BATCH"
        ));
        assert!(!Database::is_dml_statement("SELECT * FROM t"));
        assert!(!Database::is_dml_statement("  select * from t"));
    }
}
