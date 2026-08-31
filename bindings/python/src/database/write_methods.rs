//! Write-path methods of the Python `Database` surface: the write-engine
//! accessors, the `flush_run`/`maintenance_step`/`write_stats` pymethods and the
//! DML route used by `execute()`.
//!
//! Split out of `database.rs` under the campsite rule (epic #1116, issue
//! #1464). Pure code motion: signatures, pyo3 attributes, GIL handling and doc
//! comments are unchanged from the single-file layout.

use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::error::to_py_err;
use crate::result::QueryResult;
use crate::write::{MaintenanceReport, PyWriteEngine, WriteStats};

use tokio::sync::Mutex as AsyncMutex;

use super::Database;

impl Database {
    /// The write engine handle, or a typed error if the database is read-only.
    /// Replaces the `require_writable()? … .expect(Some)` two-step. Returns the
    /// `&Arc<..>` so callers can `Arc::clone` a `Send` handle into an
    /// `allow_threads` closure (issue #1444).
    fn writable_engine(&self) -> PyResult<&Arc<AsyncMutex<PyWriteEngine>>> {
        self.write_engine.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err(
                "Database is read-only. Open with writable=True and write_dir=<path> \
                 to enable write operations.",
            )
        })
    }

    /// Run `f` with the write-engine guard held, releasing the GIL for the
    /// duration (issue #1444). The `tokio::sync::Mutex` guard is `Send`, so the
    /// blocking write executes inside `py.allow_threads` while the lock still
    /// serializes writes (single-writer: no two flushes/DML overlap).
    ///
    /// The guard is taken with `blocking_lock()` on the (synchronous) Python
    /// calling thread — NOT inside a `block_on`. `PyWriteEngine::flush`/`execute`
    /// bridge their own async work via `block_on`; acquiring the lock via
    /// `.lock().await` here would make this thread a runtime driver and nest
    /// `block_on`s ("cannot start a runtime from within a runtime").
    fn with_write_engine<T, F>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        F: FnOnce(&mut PyWriteEngine) -> PyResult<T> + Send,
        T: Send,
    {
        let engine = Arc::clone(self.writable_engine()?);
        py.allow_threads(move || {
            let mut guard = engine.blocking_lock();
            f(&mut guard)
        })
    }

    /// Route a DML statement to the write engine.
    ///
    /// `pub(super)` only because `execute()` stayed in the parent module when
    /// this file was split out (issue #1464); it is not exposed to Python.
    pub(super) fn execute_dml(&self, py: Python<'_>, query: &str) -> PyResult<QueryResult> {
        use std::time::Instant;

        let query_owned = query.to_string();
        let t0 = Instant::now();

        // Release the GIL for the write (issue #1620, #1444). `execute()` routes
        // through `WriteEngine::execute_flushing`, which performs a REAL async
        // SSTable flush (disk I/O) once the memtable crosses the flush threshold;
        // the `Send` tokio guard runs it entirely inside `allow_threads`.
        let rows_affected =
            self.with_write_engine(py, move |e| e.execute(&query_owned).map_err(to_py_err))?;

        let elapsed_ms = t0.elapsed().as_millis() as u64;

        QueryResult::from_write(py, rows_affected, elapsed_ms)
    }
}

#[pymethods]
impl Database {
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
    pub fn flush_run(&self, py: Python<'_>) -> PyResult<String> {
        self.ensure_open()?;
        // Flush is substantial I/O (WAL fsync + SSTable materialization). The
        // `Send` tokio guard lets it run under `allow_threads`, so a large-memtable
        // flush no longer freezes every other Python thread (issue #1444).
        self.with_write_engine(py, |e| e.flush().map_err(to_py_err))
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
    pub fn maintenance_step(&self, py: Python<'_>, budget_ms: u64) -> PyResult<MaintenanceReport> {
        self.ensure_open()?;
        let budget = std::time::Duration::from_millis(budget_ms);
        // GIL released for the duration; the guard serializes against flush/DML.
        let report =
            self.with_write_engine(py, move |e| e.maintenance_step(budget).map_err(to_py_err))?;
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
    pub fn write_stats(&self, py: Python<'_>) -> PyResult<WriteStats> {
        self.ensure_open()?;
        // Same `Send`-guard path so acquiring the lock never blocks the GIL
        // against a concurrent flush/DML holding it (issue #1444).
        self.with_write_engine(py, |e| Ok(e.write_stats()))
    }
}
