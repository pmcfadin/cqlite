//! Write engine wrapper and related PyO3 classes.
//!
//! This module exposes:
//! - [`WriteStats`] – memtable / WAL / L0 metrics
//! - [`MaintenanceReport`] – result of `Database.maintenance_step()`
//! - [`PyWriteEngine`] – thin wrapper around `WriteEngine`
//!
//! The `write-support` feature is unconditionally enabled for the Python
//! bindings crate (see `Cargo.toml`), so there are no conditional compilation
//! guards in this file.

use cqlite_core::storage::write_engine::{MaintenanceReport as CoreReport, WriteEngine};
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// WriteStats
// ---------------------------------------------------------------------------

/// Write engine statistics.
///
/// Exposes memtable occupancy, WAL size, and L0 SSTable count.
/// Retrieve via the `db.write_stats` property.
///
/// # Attributes
///
/// * `memtable_size`  – Memtable size in bytes
/// * `memtable_rows`  – Number of rows currently in the memtable
/// * `wal_size`       – Write-ahead log size in bytes
/// * `l0_count`       – Number of Level-0 SSTables (zero until tracking added)
/// * `total_written`  – Rows written since engine was opened (equals `memtable_rows`
///                      until flushed-row tracking is implemented)
///
/// # Example
///
/// ```python
/// stats = db.write_stats
/// print(f"Memtable: {stats.memtable_size} bytes, {stats.memtable_rows} rows")
/// print(f"WAL size: {stats.wal_size} bytes")
/// ```
#[pyclass(module = "cqlite")]
pub struct WriteStats {
    /// Current memtable size in bytes.
    #[pyo3(get)]
    pub memtable_size: usize,
    /// Current memtable row count.
    #[pyo3(get)]
    pub memtable_rows: usize,
    /// WAL file size in bytes.
    #[pyo3(get)]
    pub wal_size: u64,
    /// Number of L0 SSTables in the write directory.
    #[pyo3(get)]
    pub l0_count: usize,
    /// Total rows written (memtable + flushed).
    #[pyo3(get)]
    pub total_written: usize,
}

#[pymethods]
impl WriteStats {
    fn __repr__(&self) -> String {
        format!(
            "WriteStats(memtable_size={}, memtable_rows={}, wal_size={}, l0_count={})",
            self.memtable_size, self.memtable_rows, self.wal_size, self.l0_count
        )
    }

    /// Convert to a plain Python dict.
    ///
    /// Returns a dict with keys: `memtable_size`, `memtable_rows`, `wal_size`,
    /// `l0_count`, `total_written`.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        use pyo3::types::PyDict;
        let d = PyDict::new(py);
        d.set_item("memtable_size", self.memtable_size)?;
        d.set_item("memtable_rows", self.memtable_rows)?;
        d.set_item("wal_size", self.wal_size)?;
        d.set_item("l0_count", self.l0_count)?;
        d.set_item("total_written", self.total_written)?;
        Ok(d.into_any().unbind())
    }
}

// ---------------------------------------------------------------------------
// MaintenanceReport
// ---------------------------------------------------------------------------

/// Result returned by `Database.maintenance_step(budget_ms)`.
///
/// # Attributes
///
/// * `time_spent_ms`     – Actual time spent in the maintenance step (ms, float)
/// * `rows_merged`       – Number of rows processed/merged
/// * `bytes_written`     – Bytes written to output SSTable(s)
/// * `completed_merges`  – List of output SSTable Data.db paths completed
/// * `pending_compaction`– `True` if more compaction work remains
///
/// # Example
///
/// ```python
/// report = db.maintenance_step(budget_ms=200)
/// print(f"Merged {report.rows_merged} rows in {report.time_spent_ms:.1f} ms")
/// if report.pending_compaction:
///     print("More compaction work remains")
/// ```
#[pyclass(module = "cqlite")]
pub struct MaintenanceReport {
    /// Time spent in this maintenance step (milliseconds).
    #[pyo3(get)]
    pub time_spent_ms: f64,
    /// Number of rows merged in this step.
    #[pyo3(get)]
    pub rows_merged: u64,
    /// Bytes written to output SSTables.
    #[pyo3(get)]
    pub bytes_written: u64,
    /// Paths of SSTable Data.db files completed in this step.
    #[pyo3(get)]
    pub completed_merges: Vec<String>,
    /// Whether there is pending compaction work.
    #[pyo3(get)]
    pub pending_compaction: bool,
}

impl MaintenanceReport {
    /// Convert from the core `MaintenanceReport`.
    pub fn from_core(r: CoreReport) -> Self {
        Self {
            time_spent_ms: r.time_spent.as_secs_f64() * 1000.0,
            rows_merged: r.rows_merged,
            bytes_written: r.bytes_written,
            completed_merges: r
                .completed_merges
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect(),
            pending_compaction: r.pending_compaction,
        }
    }
}

#[pymethods]
impl MaintenanceReport {
    fn __repr__(&self) -> String {
        format!(
            "MaintenanceReport(time_spent_ms={:.1}, rows_merged={}, pending={})",
            self.time_spent_ms, self.rows_merged, self.pending_compaction
        )
    }

    /// Convert to a plain Python dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        use pyo3::types::{PyDict, PyList};
        let d = PyDict::new(py);
        d.set_item("time_spent_ms", self.time_spent_ms)?;
        d.set_item("rows_merged", self.rows_merged)?;
        d.set_item("bytes_written", self.bytes_written)?;
        let merges = PyList::new(py, &self.completed_merges)?;
        d.set_item("completed_merges", merges)?;
        d.set_item("pending_compaction", self.pending_compaction)?;
        Ok(d.into_any().unbind())
    }
}

// ---------------------------------------------------------------------------
// PyWriteEngine – internal, not exported directly to Python
// ---------------------------------------------------------------------------

/// Thin wrapper around [`WriteEngine`].
///
/// Stored inside `Database` as `Option<Mutex<PyWriteEngine>>` when opened in
/// writable mode.  All methods are synchronous; async flush is bridged via
/// `block_on`.
pub struct PyWriteEngine {
    pub(crate) inner: WriteEngine,
}

impl PyWriteEngine {
    /// Create a new `PyWriteEngine` from a configured [`WriteEngine`].
    pub fn new(engine: WriteEngine) -> Self {
        Self { inner: engine }
    }

    /// Execute a DML CQL statement (INSERT / UPDATE / DELETE / BEGIN BATCH).
    ///
    /// Returns the number of mutations applied (typically 1, or N for BATCH).
    ///
    /// Routes through the async-flushing write path (issue #1620): the Python
    /// binding runs inside a Tokio runtime where the sync `execute()` auto-flush
    /// is intentionally skipped, so it would otherwise grow the memtable to the
    /// hard limit. `execute_flushing` awaits a real async flush once the flush
    /// threshold is crossed.
    pub fn execute(&mut self, statement: &str) -> cqlite_core::error::Result<u64> {
        use crate::runtime::block_on;
        // Outer `?` converts a runtime-init `io::Error` via `Error::Io` (#[from]);
        // inner `?` propagates the core error (issue #1438).
        Ok(block_on(self.inner.execute_flushing(statement))??)
    }

    /// Flush the memtable to a new SSTable generation.
    ///
    /// Returns the Data.db path, or an empty string if the memtable was empty.
    pub fn flush(&mut self) -> cqlite_core::error::Result<String> {
        use crate::runtime::block_on;
        // Outer `?` converts a runtime-init `io::Error` via `Error::Io` (#[from]);
        // inner `?` propagates the flush error (issue #1438).
        let info = block_on(self.inner.flush())??;
        Ok(info
            .map(|i| i.data_path.to_string_lossy().into_owned())
            .unwrap_or_default())
    }

    /// Run one maintenance step within the given time budget.
    pub fn maintenance_step(
        &mut self,
        budget: std::time::Duration,
    ) -> cqlite_core::error::Result<CoreReport> {
        self.inner.maintenance_step(budget)
    }

    /// Snapshot current write-engine metrics.
    pub fn write_stats(&self) -> WriteStats {
        WriteStats {
            memtable_size: self.inner.memtable_size(),
            memtable_rows: self.inner.memtable_row_count(),
            wal_size: self.inner.wal_size(),
            l0_count: self.inner.l0_count() as usize,
            total_written: self.inner.total_written() as usize,
        }
    }
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

/// Register write-related PyO3 types with the Python module.
pub fn register_write(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<WriteStats>()?;
    m.add_class::<MaintenanceReport>()?;
    Ok(())
}
