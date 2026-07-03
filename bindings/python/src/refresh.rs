//! `RefreshReport` wrapper for Python bindings (issue #1749).
//!
//! Exposes the result of [`Database.refresh()`], describing how an explicit
//! directory refresh changed the held reader set.

use pyo3::prelude::*;

/// Result returned by `Database.refresh()`.
///
/// Describes what an explicit directory refresh applied to the database's held
/// SSTable reader set: newly present generations become queryable, removed
/// generations stop being queried, and unchanged generations keep their warm
/// parsed state (they are not re-parsed).
///
/// # Attributes
///
/// * `tables_scanned`  – Number of table directories re-discovered.
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
    /// Number of table directories re-discovered during the refresh.
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

/// Register refresh types with the Python module.
pub fn register_refresh(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RefreshReport>()?;
    Ok(())
}
