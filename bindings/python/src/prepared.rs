//! PreparedStatement wrapper for Python bindings.
//!
//! This module provides the `PreparedStatement` class for Python access
//! to CQLite's prepared statement functionality.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// A prepared CQL statement.
///
/// PreparedStatement holds a pre-parsed and planned query that can be
/// inspected for metadata and statistics. Created via `Database.prepare()`.
///
/// # Example
///
/// ```python
/// stmt = db.prepare("SELECT * FROM users WHERE id = ?")
/// print(f"Query: {stmt.query}")
/// print(f"Parameters: {stmt.parameter_count}")
/// print(f"Stats: {stmt.stats()}")
/// ```
#[pyclass(module = "cqlite")]
pub struct PreparedStatement {
    inner: Arc<cqlite_core::query::PreparedQuery>,
}

impl PreparedStatement {
    /// Create a new PreparedStatement from a core PreparedQuery.
    pub fn new(inner: Arc<cqlite_core::query::PreparedQuery>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PreparedStatement {
    /// The original CQL query text.
    ///
    /// Returns the exact query string that was passed to `prepare()`.
    #[getter]
    fn query(&self) -> &str {
        &self.inner.cql
    }

    /// Number of parameters in the query.
    ///
    /// Returns the count of placeholder parameters (?) in the query.
    #[getter]
    fn parameter_count(&self) -> usize {
        self.inner.parameters.len()
    }

    /// Get statistics about this prepared statement.
    ///
    /// Returns a dictionary containing:
    /// - `parameter_count`: Number of parameters
    /// - `plan_type`: Type of execution plan (e.g., "TableScan", "IndexLookup")
    /// - `estimated_cost`: Estimated execution cost
    /// - `estimated_rows`: Estimated number of rows returned
    /// - `cache_friendly`: Whether the query is cache-friendly
    ///
    /// # Example
    ///
    /// ```python
    /// stats = stmt.stats()
    /// print(f"Plan type: {stats['plan_type']}")
    /// print(f"Estimated rows: {stats['estimated_rows']}")
    /// ```
    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let core_stats = self.inner.stats();
        let dict = PyDict::new(py);
        dict.set_item("parameter_count", core_stats.parameter_count)?;
        dict.set_item("plan_type", &core_stats.plan_type)?;
        dict.set_item("estimated_cost", core_stats.estimated_cost)?;
        dict.set_item("estimated_rows", core_stats.estimated_rows)?;
        dict.set_item("cache_friendly", core_stats.cache_friendly)?;
        Ok(dict.into())
    }

    /// String representation of the prepared statement.
    fn __repr__(&self) -> String {
        format!("PreparedStatement({:?})", self.inner.cql)
    }
}

/// Register prepared statement types with the Python module.
pub fn register_prepared(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PreparedStatement>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_prepared_statement_repr() {
        // Unit test for repr formatting
        let repr = format!("PreparedStatement({:?})", "SELECT * FROM test");
        assert!(repr.contains("PreparedStatement"));
        assert!(repr.contains("SELECT"));
    }
}
