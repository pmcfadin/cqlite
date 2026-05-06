//! Python bindings for CQLite SSTable reader.
//!
//! This module provides Python access to CQLite's core functionality
//! for reading Cassandra 5.0 SSTables without cluster dependencies.

use pyo3::prelude::*;

mod config;
mod database;
mod error;
mod prepared;
mod result;
mod runtime;
mod stats;
mod value;
mod write;

pub use config::{config_from_py, StreamingConfig};
pub use database::{open, Database};
pub use error::{to_py_err, CqliteError, ParseError, QueryError, SchemaError};
pub use prepared::PreparedStatement;
pub use result::{ColumnInfo, QueryResult, QueryResultIter, Row, StreamingIterator};
pub use runtime::{block_on, get_runtime};
pub use stats::DatabaseStats;
pub use write::{MaintenanceReport, WriteStats};

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

    // Register result types (QueryResult, Row, ColumnInfo)
    result::register_result(m)?;

    // Register prepared statement types
    prepared::register_prepared(m)?;

    // Register database stats types
    stats::register_stats(m)?;

    // Register write-support types (WriteStats, MaintenanceReport)
    write::register_write(m)?;

    Ok(())
}
