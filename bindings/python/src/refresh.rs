//! `RefreshReport` wrapper for Python bindings (issue #1749).
//!
//! Exposes the result of [`Database.refresh()`], describing how an explicit
//! directory refresh changed the held reader set.

use pyo3::prelude::*;

use crate::database::Database;
use crate::error::{runtime_init_to_py_err, to_py_err};
use crate::runtime::block_on;

/// Result returned by `Database.refresh()`.
///
/// Describes what an explicit directory refresh applied to the database's held
/// SSTable reader set: newly present generations become queryable, removed
/// generations stop being queried, and unchanged generations keep their warm
/// parsed state (they are not re-parsed).
///
/// # Attributes
///
/// * `tables_scanned`  – Number of distinct logical tables present after the refresh.
/// * `readers_added`   – Number of SSTable generations newly opened.
/// * `readers_removed` – Number of SSTable generations dropped.
///
/// # Example
///
/// ```python
/// report = db.refresh()
/// print(f"scanned {report.tables_scanned} tables, "
///       f"+{report.readers_added}/-{report.readers_removed} readers")
/// ```
#[pyclass(module = "cqlite")]
pub struct RefreshReport {
    /// Number of distinct logical tables present after the refresh.
    #[pyo3(get)]
    pub tables_scanned: usize,
    /// Number of SSTable generations newly opened and made queryable.
    #[pyo3(get)]
    pub readers_added: usize,
    /// Number of SSTable generations dropped from the reader set.
    #[pyo3(get)]
    pub readers_removed: usize,
}

impl RefreshReport {
    /// Build the Python report from the core [`cqlite_core::RefreshReport`].
    pub fn from_core(report: cqlite_core::RefreshReport) -> Self {
        Self {
            tables_scanned: report.tables_scanned,
            readers_added: report.readers_added,
            readers_removed: report.readers_removed,
        }
    }
}

#[pymethods]
impl RefreshReport {
    fn __repr__(&self) -> String {
        format!(
            "RefreshReport(tables_scanned={}, readers_added={}, readers_removed={})",
            self.tables_scanned, self.readers_added, self.readers_removed
        )
    }

    /// Convert to a plain Python dict.
    ///
    /// Keys: `tables_scanned`, `readers_added`, `readers_removed`.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        use pyo3::types::PyDict;
        let d = PyDict::new(py);
        d.set_item("tables_scanned", self.tables_scanned)?;
        d.set_item("readers_added", self.readers_added)?;
        d.set_item("readers_removed", self.readers_removed)?;
        Ok(d.into_any().unbind())
    }
}

/// `Database.refresh()` lives here (issue #1749), split from `database.rs` per
/// the campsite file-size doctrine (epic #1116). PyO3's `multiple-pymethods`
/// feature lets this second `#[pymethods] impl Database` block coexist with the
/// primary one in `database.rs`; the public Python surface (`db.refresh()`) is
/// unchanged. This mirrors the Node binding's `bindings/node/src/refresh.rs`.
#[pymethods]
impl Database {
    /// Re-discover the data directory and apply changes to the held reader set.
    ///
    /// A `Database` takes a snapshot of the on-disk SSTables at `open` time and
    /// serves every subsequent query from that snapshot. `refresh()` is the
    /// explicit way to pick up files that appeared or disappeared since then:
    /// it re-runs the same TOC/filename-component discovery `open` uses (no
    /// content sniffing) and applies the diff — newly present generations
    /// become queryable, removed generations stop being queried, and unchanged
    /// generations keep their existing warm reader state (Index/Statistics/bloom
    /// are not re-parsed).
    ///
    /// The refresh is atomic and fail-closed: if any newly discovered generation
    /// fails to open (including a corrupt `Statistics.db`, per the #1626
    /// posture), `refresh()` raises and leaves the previously held reader set
    /// fully unchanged — no partial application. A query already in flight is
    /// unaffected; queries started after `refresh()` returns see the new set.
    ///
    /// # Returns
    ///
    /// A `RefreshReport` with `tables_scanned`, `readers_added`, and
    /// `readers_removed`.
    ///
    /// # Raises
    ///
    /// * `RuntimeError` – If the database is closed
    /// * `CqliteError`  – If a newly discovered generation fails to open (the
    ///                    held reader set is left unchanged)
    ///
    /// # Example
    ///
    /// ```python
    /// # A new SSTable was copied into the data directory after open.
    /// report = db.refresh()
    /// print(f"added {report.readers_added}, removed {report.readers_removed}")
    /// # Subsequent queries now see the new generation.
    /// ```
    pub fn refresh(&self, py: Python<'_>) -> PyResult<RefreshReport> {
        self.ensure_open()?;

        let db = self.inner();

        // Release the GIL during the async re-discovery + reader swap so other
        // Python threads keep running (same pattern as execute).
        let core_report = py
            .allow_threads(|| block_on(db.refresh()))
            .map_err(runtime_init_to_py_err)?
            .map_err(to_py_err)?;

        Ok(RefreshReport::from_core(core_report))
    }
}

/// Register refresh types with the Python module.
pub fn register_refresh(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RefreshReport>()?;
    Ok(())
}
