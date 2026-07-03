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
use std::sync::{Arc, Mutex};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::config::{config_from_py, StreamingConfig};
use crate::error::to_py_err;
use crate::prepared::PreparedStatement;
use crate::result::{QueryResult, StreamingIterator};
use crate::runtime::block_on;
use crate::stats::DatabaseStats;
use crate::write::{MaintenanceReport, PyWriteEngine, WriteStats};

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
    inner: Arc<cqlite_core::Database>,
    closed: AtomicBool,
    /// Optional write engine — present only when opened with `writable=True`.
    write_engine: Option<Mutex<PyWriteEngine>>,
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

    /// Return a clear error when a write method is called on a read-only database.
    fn require_writable(&self) -> PyResult<()> {
        if self.write_engine.is_none() {
            return Err(PyRuntimeError::new_err(
                "Database is read-only. \
                 Open with writable=True and write_dir=<path> to enable write operations. \
                 Example: cqlite.open(path, schema=schema, writable=True, write_dir='/tmp/writes')",
            ));
        }
        Ok(())
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

        // Close the write engine first (flushes remaining memtable)
        if let Some(ref engine_mutex) = self.write_engine {
            let mut engine = engine_mutex
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Write engine lock poisoned during close"))?;
            // close() flushes remaining data. MutexGuard is not Send, so we
            // cannot release the GIL here. The flush on close is typically fast.
            block_on(engine.inner.close()).map_err(to_py_err)?;
        }

        // Shutdown the read-side storage engine
        py.allow_threads(|| block_on(self.inner.shutdown()))
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
            .map_err(to_py_err)?;

        Ok(StreamingIterator::with_span(core_iter, span.clone()))
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
    #[pyo3(signature = (query, path, *, row_group_size=10000, compression="snappy"))]
    pub fn export_parquet(
        &self,
        py: Python<'_>,
        query: &str,
        path: &str,
        row_group_size: usize,
        compression: &str,
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

        // Release the GIL for the whole export (query + file writing).
        // Errors are split so each maps to the right Python exception:
        // core errors via to_py_err, writer/file errors to IOError.
        let result: Result<u64, PyErr> = py.allow_threads(|| {
            block_on(async {
                let mut iter = db
                    .execute_streaming(
                        &query_owned,
                        cqlite_core::query::result::StreamingConfig::default(),
                    )
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
            .map_err(to_py_err)?;

        DatabaseStats::from_core(py, core_stats)
    }

    // -----------------------------------------------------------------------
    // Write API – available only when `writable=True` at open time
    // -----------------------------------------------------------------------

    /// Flush the memtable to a new SSTable.
    ///
    /// Forces a flush of all in-memory writes to disk.  Returns the absolute
    /// path to the newly written `Data.db` file, or an empty string if the
    /// memtable was empty (nothing to flush).
    ///
    /// Requires the database to have been opened with `writable=True`.
    ///
    /// # Returns
    ///
    /// Absolute path string of the flushed SSTable Data.db file, or `""` if
    /// the memtable was empty.
    ///
    /// # Raises
    ///
    /// * `RuntimeError` – If database is closed or opened in read-only mode
    /// * `CqliteError`  – If the flush fails (e.g. I/O error)
    ///
    /// # Example
    ///
    /// ```python
    /// db.execute("INSERT INTO t (id) VALUES (1)")
    /// path = db.flush_run()
    /// assert Path(path).exists()
    /// ```
    pub fn flush_run(&self) -> PyResult<String> {
        self.ensure_open()?;
        self.require_writable()?;

        let engine_mutex = self
            .write_engine
            .as_ref()
            .expect("require_writable() guarantees write_engine is Some");

        let mut engine = engine_mutex
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Write engine lock poisoned"))?;

        // Flush involves substantial I/O. We cannot release the GIL here because
        // MutexGuard is not Send. Flush is typically fast (writes a single SSTable
        // file). This is acceptable for the current implementation.
        engine.flush().map_err(to_py_err)
    }

    /// Perform one incremental maintenance step (compaction).
    ///
    /// Runs background compaction work within the given time budget.  Can be
    /// called repeatedly to make incremental progress.  The actual time spent
    /// may exceed `budget_ms` by up to ~10% (one partition is always processed
    /// to guarantee forward progress).
    ///
    /// Requires the database to have been opened with `writable=True`.
    ///
    /// # Arguments
    ///
    /// * `budget_ms` – Maximum milliseconds to spend in this call
    ///
    /// # Returns
    ///
    /// A `MaintenanceReport` with timing, merge statistics, and a
    /// `pending_compaction` flag.
    ///
    /// # Raises
    ///
    /// * `RuntimeError` – If database is closed or opened in read-only mode
    /// * `CqliteError`  – If maintenance fails
    ///
    /// # Example
    ///
    /// ```python
    /// report = db.maintenance_step(budget_ms=100)
    /// print(f"Merged {report.rows_merged} rows in {report.time_spent_ms:.1f} ms")
    /// ```
    pub fn maintenance_step(&self, budget_ms: u64) -> PyResult<MaintenanceReport> {
        self.ensure_open()?;
        self.require_writable()?;

        let engine_mutex = self
            .write_engine
            .as_ref()
            .expect("require_writable() guarantees write_engine is Some");

        let mut engine = engine_mutex
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Write engine lock poisoned"))?;

        let budget = std::time::Duration::from_millis(budget_ms);

        // MutexGuard is not Send so we cannot release the GIL here.
        // maintenance_step() is time-bounded and typically completes quickly.
        let report = engine.maintenance_step(budget).map_err(to_py_err)?;

        Ok(MaintenanceReport::from_core(report))
    }

    /// Current write engine statistics.
    ///
    /// Returns a snapshot of memtable occupancy, WAL size, and L0 SSTable count.
    ///
    /// Requires the database to have been opened with `writable=True`.
    ///
    /// # Returns
    ///
    /// A `WriteStats` object.
    ///
    /// # Raises
    ///
    /// * `RuntimeError` – If database is closed or opened in read-only mode
    ///
    /// # Example
    ///
    /// ```python
    /// stats = db.write_stats
    /// print(f"Memtable: {stats.memtable_size} bytes, {stats.memtable_rows} rows")
    /// ```
    #[getter]
    pub fn write_stats(&self) -> PyResult<WriteStats> {
        self.ensure_open()?;
        self.require_writable()?;

        let engine_mutex = self
            .write_engine
            .as_ref()
            .expect("require_writable() guarantees write_engine is Some");

        let engine = engine_mutex
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Write engine lock poisoned"))?;

        Ok(engine.write_stats())
    }
}

// -----------------------------------------------------------------------
// Internal helpers (not exposed to Python)
// -----------------------------------------------------------------------

impl Database {
    /// Route a DML statement to the write engine.
    fn execute_dml(&self, py: Python<'_>, query: &str) -> PyResult<QueryResult> {
        use std::time::Instant;

        self.require_writable()?;

        let engine_mutex = self
            .write_engine
            .as_ref()
            .expect("require_writable() guarantees write_engine is Some");

        let mut engine = engine_mutex
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Write engine lock poisoned"))?;

        let query_owned = query.to_string();
        let t0 = Instant::now();

        // MutexGuard is not Send so we cannot release the GIL here.
        // execute() is a fast synchronous in-memory operation (WAL write + memtable insert).
        let rows_affected = engine.execute(&query_owned).map_err(to_py_err)?;

        let elapsed_ms = t0.elapsed().as_millis() as u64;

        QueryResult::from_write(py, rows_affected, elapsed_ms)
    }
}

// -----------------------------------------------------------------------
// open() function
// -----------------------------------------------------------------------

/// Open a CQLite database.
///
/// Opens a database at the specified path, optionally loading a schema file
/// and applying custom configuration.
///
/// # Arguments
///
/// * `path`       – Path to the data directory containing SSTables
/// * `schema`     – Optional path to a CQL schema file (.cql)
/// * `config`     – Optional configuration (dict, JSON string, or preset name)
/// * `writable`   – Enable write support (INSERT / UPDATE / DELETE).  Default `False`.
/// * `write_dir`  – Directory for WAL and flushed SSTables.  Required when
///                  `writable=True`.  Created automatically if it doesn't exist.
///
/// # Caveat: write_dir is not file-locked
///
/// Only one `Database` instance should hold a given `write_dir` at a time.
/// Concurrent instances sharing the same directory are **not** protected by
/// file locks and will corrupt each other's WAL and SSTable files.  File
/// locking is planned for a future release (see issue #485).
///
/// # Returns
///
/// A new Database instance.
///
/// # Raises
///
/// * `IOError`     – If the path doesn't exist or is inaccessible
/// * `SchemaError` – If schema parsing fails
/// * `ValueError`  – If configuration is invalid, or `writable=True` but
///                   `write_dir` or `schema` was not provided
///
/// # Examples
///
/// ```python
/// # Basic read-only open
/// db = cqlite.open("/path/to/sstables")
///
/// # With schema file
/// db = cqlite.open("/path/to/sstables", schema="/path/to/schema.cql")
///
/// # With write support
/// db = cqlite.open(
///     "/path/to/sstables",
///     schema="/path/to/schema.cql",
///     writable=True,
///     write_dir="/tmp/cqlite-write",
/// )
///
/// # Using context manager
/// with cqlite.open("/path/to/sstables") as db:
///     pass
/// ```
#[pyfunction]
#[pyo3(signature = (path, *, schema=None, config=None, writable=false, write_dir=None, otel_config=None, traceparent=None))]
pub fn open(
    py: Python<'_>,
    path: PathBuf,
    schema: Option<PathBuf>,
    config: Option<&Bound<'_, PyAny>>,
    writable: bool,
    write_dir: Option<PathBuf>,
    otel_config: Option<&Bound<'_, PyAny>>,
    traceparent: Option<String>,
) -> PyResult<Database> {
    // Initialise observability once per process (issue #1039). The config is
    // resolved from the optional `otel_config` dict layered over the
    // `CQLITE_OTEL_*` environment. The first `open` wins; later opens reuse the
    // installed exporters. A bad exporter config never blocks the open — it
    // simply yields no telemetry.
    let obs_cfg = crate::observability::config_from_py(py, otel_config)?;
    crate::observability::ensure_initialized(obs_cfg);

    // Validate writable-mode requirements upfront before any I/O.
    if writable {
        if write_dir.is_none() {
            return Err(PyValueError::new_err(
                "write_dir is required when writable=True. \
                 Example: cqlite.open(path, writable=True, write_dir='/tmp/cqlite-writes')",
            ));
        }
        if schema.is_none() {
            return Err(PyValueError::new_err(
                "schema is required when writable=True so that the write engine \
                 can resolve column types.",
            ));
        }
    }

    let core_config = config_from_py(py, config)?;

    // Capture the compaction settings before `core_config` is moved into
    // ingestion / Database::open, so `Config.storage.compaction` is
    // authoritative for the Python write path rather than decorative (issue
    // #1619). Setting `auto_compaction = false` disables STCS, making
    // `maintenance_step` a no-op. NOTE: the config bridge (`config_from_dict`)
    // deserializes into the full `cqlite_core::Config`, which is NOT
    // `#[serde(default)]`, so `config` must be a COMPLETE config — a full dict,
    // a full JSON string, or a preset. A partial dict such as
    // `{"storage": {"compaction": {"auto_compaction": false}}}` is rejected
    // with missing-field errors. To flip only this switch, obtain a full config
    // dict from a preset (e.g. `cqlite.performance_optimized()`), set
    // `["storage"]["compaction"]["auto_compaction"] = False`, then pass it.
    let compaction_config = core_config.storage.compaction.clone();

    // Open the read-side database and capture the schema registry when present.
    // We always use ingestion when a schema file is provided because that path
    // performs the SSTable discovery and populates the schema registry.
    let (db, schema_registry_opt) = if let Some(schema_path) = schema.clone() {
        let ingestion_config = cqlite_core::ingestion::IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir: path.clone(),
            version_hint: None,
            core_config,
            table_directory_filter: None,
        };

        py.allow_threads(|| {
            block_on(async {
                let result = cqlite_core::ingestion::ingest(ingestion_config).await?;
                let registry = result.schema_registry;
                Ok::<_, cqlite_core::Error>((result.database, Some(registry)))
            })
        })
        .map_err(to_py_err)?
    } else {
        let db = py
            .allow_threads(|| block_on(cqlite_core::Database::open(&path, core_config)))
            .map_err(to_py_err)?;
        (db, None)
    };

    // Build WriteEngine when writable=True.
    let write_engine: Option<Mutex<PyWriteEngine>> = if writable {
        let wd = write_dir.expect("validated above");
        let schema_path_display = schema.clone().expect("validated above");

        // Retrieve the first TableSchema from the registry loaded during ingestion.
        let table_schema = py
            .allow_threads(|| {
                block_on(async {
                    let registry = schema_registry_opt.ok_or_else(|| {
                        cqlite_core::Error::Schema(
                            "Internal error: schema registry unavailable. \
                             This should not happen when schema= is provided."
                                .to_string(),
                        )
                    })?;

                    let schemas = registry
                        .read()
                        .await
                        .list_schemas(None)
                        .await
                        .map_err(|e| {
                            cqlite_core::Error::Schema(format!("Failed to list schemas: {}", e))
                        })?;

                    schemas.into_iter().next().ok_or_else(|| {
                        cqlite_core::Error::Schema(format!(
                            "No table schema found in {:?}. \
                             Verify the schema file contains at least one CREATE TABLE statement.",
                            schema_path_display
                        ))
                    })
                })
            })
            .map_err(to_py_err)?;

        let engine_config = cqlite_core::storage::write_engine::WriteEngineConfig::new(
            wd.join("data"),
            wd.join("wal"),
            table_schema,
        )
        .with_compaction_config(&compaction_config);

        let engine = cqlite_core::storage::write_engine::WriteEngine::new(engine_config)
            .map_err(to_py_err)?;

        Some(Mutex::new(PyWriteEngine::new(engine)))
    } else {
        // Silence unused-variable warning
        let _ = (write_dir, schema_registry_opt, compaction_config);
        None
    };

    Ok(Database {
        inner: Arc::new(db),
        closed: AtomicBool::new(false),
        write_engine,
        default_traceparent: traceparent.filter(|s| !s.trim().is_empty()),
    })
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
