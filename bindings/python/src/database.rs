//! Database wrapper for Python bindings.
//!
//! This module provides the `Database` class and `open()` function
//! for Python access to CQLite's SSTable reading capabilities.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::config::config_from_py;
use crate::error::to_py_err;
use crate::runtime::block_on;

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
}

impl Database {
    /// Check if database is open, raising RuntimeError if closed.
    #[allow(dead_code)]
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            Err(PyRuntimeError::new_err("Database is closed"))
        } else {
            Ok(())
        }
    }

    /// Get a clone of the inner database Arc (for future execute methods).
    ///
    /// Returns an Arc clone to allow async operations that may outlive
    /// the borrow of self.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> Arc<cqlite_core::Database> {
        Arc::clone(&self.inner)
    }
}

#[pymethods]
impl Database {
    /// Close the database and release resources.
    ///
    /// This method is idempotent - calling it multiple times is safe.
    /// After closing, any operations on the database will raise RuntimeError.
    ///
    /// # Example
    ///
    /// ```python
    /// db = cqlite.open("/path/to/data")
    /// db.close()
    /// db.close()  # Safe to call again
    /// ```
    pub fn close(&self) -> PyResult<()> {
        // Atomically set closed flag, return early if already closed
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Shutdown the storage engine to release resources
        block_on(self.inner.shutdown()).map_err(to_py_err)?;
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
    ///
    /// # Arguments
    ///
    /// * `exc_type` - Exception type if an exception was raised
    /// * `exc_val` - Exception value if an exception was raised
    /// * `exc_tb` - Traceback if an exception was raised
    ///
    /// # Returns
    ///
    /// False to indicate exceptions should not be suppressed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false) // Don't suppress exceptions
    }

    /// String representation of the database.
    fn __repr__(&self) -> String {
        if self.closed.load(Ordering::SeqCst) {
            "Database(closed)".to_string()
        } else {
            "Database(open)".to_string()
        }
    }
}

/// Open a CQLite database.
///
/// Opens a database at the specified path, optionally loading a schema file
/// and applying custom configuration.
///
/// # Arguments
///
/// * `path` - Path to the data directory containing SSTables
/// * `schema` - Optional path to a CQL schema file (.cql)
/// * `config` - Optional configuration (dict, JSON string, or preset name)
///
/// # Returns
///
/// A new Database instance.
///
/// # Raises
///
/// * `IOError` - If the path doesn't exist or is inaccessible
/// * `SchemaError` - If schema parsing fails
/// * `ValueError` - If configuration is invalid
///
/// # Examples
///
/// ```python
/// # Basic open
/// db = cqlite.open("/path/to/sstables")
///
/// # With schema file
/// db = cqlite.open("/path/to/sstables", schema="/path/to/schema.cql")
///
/// # With config preset
/// db = cqlite.open("/path/to/sstables", config="memory_optimized")
///
/// # With custom config dict
/// db = cqlite.open("/path/to/sstables", config={"memory": {"max_memory": 134217728}})
///
/// # Using context manager
/// with cqlite.open("/path/to/sstables") as db:
///     # database is automatically closed on exit
///     pass
/// ```
#[pyfunction]
#[pyo3(signature = (path, *, schema=None, config=None))]
pub fn open(
    py: Python<'_>,
    path: PathBuf,
    schema: Option<PathBuf>,
    config: Option<&Bound<'_, PyAny>>,
) -> PyResult<Database> {
    let core_config = config_from_py(py, config)?;

    let db = if let Some(schema_path) = schema {
        // Use ingestion module for schema + SSTable discovery
        let ingestion_config = cqlite_core::ingestion::IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir: path,
            version_hint: None,
            core_config,
            table_directory_filter: None,
        };

        block_on(async {
            let result = cqlite_core::ingestion::ingest(ingestion_config).await?;
            Ok::<_, cqlite_core::Error>(result.database)
        })
        .map_err(to_py_err)?
    } else {
        // Simple open without schema
        block_on(cqlite_core::Database::open(&path, core_config)).map_err(to_py_err)?
    };

    Ok(Database {
        inner: Arc::new(db),
        closed: AtomicBool::new(false),
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

        // First swap should return false (was not closed)
        let was_closed = closed.swap(true, Ordering::SeqCst);
        assert!(!was_closed);
        assert!(closed.load(Ordering::SeqCst));

        // Second swap should return true (was already closed)
        let was_closed = closed.swap(true, Ordering::SeqCst);
        assert!(was_closed);
    }
}
