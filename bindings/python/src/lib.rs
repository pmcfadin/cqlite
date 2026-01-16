//! Python bindings for CQLite SSTable reader.
//!
//! This module provides Python access to CQLite's core functionality
//! for reading Cassandra 5.0 SSTables without cluster dependencies.

use pyo3::prelude::*;

mod config;
mod database;
mod error;
mod runtime;

pub use config::{config_from_py, StreamingConfig};
pub use database::{open, Database};
pub use error::{to_py_err, CqliteError, ParseError, QueryError, SchemaError};
pub use runtime::{block_on, get_runtime};

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
    // Register version info
    m.add("__version__", VERSION)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    // Register exception types
    error::register_exceptions(m)?;

    // Register configuration classes and functions
    config::register_config(m)?;

    // Register database class and open function
    database::register_database(m)?;

    Ok(())
}
