//! Python bindings for CQLite SSTable reader.
//!
//! This module provides Python access to CQLite's core functionality
//! for reading Cassandra 5.0 SSTables without cluster dependencies.

use pyo3::prelude::*;

mod config;
mod database;
mod error;
mod observability;
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

/// Test-support introspection: reports whether this extension was compiled with
/// the `panic = "abort"` strategy.
///
/// This is read-only and derived at compile time from `cfg!(panic = "abort")`,
/// so it reflects the ACTUAL compiled panic strategy of the loaded wheel (which
/// differs between the debug/test profile, `panic = "unwind"`, and the release
/// profile). The abort-safety harness (`tests/test_abort_safety.py`, issue
/// #1437) keys a conditional strict xfail on this value: under `panic = "unwind"`
/// PyO3 contains a core panic as a catchable exception and the survival cases
/// hard-assert; under `panic = "abort"` the same panic is a process abort until
/// issue #1440 flips the release profile to `panic = "unwind"`.
///
/// Not part of the stable public API; the leading underscore marks it as
/// internal test support.
#[pyfunction]
fn _built_with_panic_abort() -> bool {
    cfg!(panic = "abort")
}

/// Python module for CQLite.
#[pymodule]
fn _cqlite(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register version info
    m.add("__version__", VERSION)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    // Test-support introspection (issue #1437). See the fn doc comment.
    m.add_function(wrap_pyfunction!(_built_with_panic_abort, m)?)?;

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
