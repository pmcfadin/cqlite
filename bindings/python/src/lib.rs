//! Python bindings for CQLite SSTable reader.
//!
//! This module provides Python access to CQLite's core functionality
//! for reading Cassandra 5.0 SSTables without cluster dependencies.

use pyo3::prelude::*;

/// CQLite version string.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the version of the cqlite library.
#[pyfunction]
fn version() -> &'static str {
    VERSION
}

/// Python module for CQLite.
#[pymodule]
fn _cqlite(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}
