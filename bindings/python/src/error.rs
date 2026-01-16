//! Error mapping layer for Python bindings.
//!
//! Maps `cqlite_core::Error` variants to Python exceptions.
//!
//! # Exception Hierarchy
//!
//! ```text
//! Exception (Python builtin)
//! ├── CqliteError (base for all CQLite exceptions)
//! │   ├── SchemaError - Schema/Table errors
//! │   ├── QueryError - Query execution errors
//! │   └── ParseError - CQL parsing errors
//! ├── IOError (builtin) - I/O errors
//! ├── ValueError (builtin) - Configuration/input errors
//! ├── TimeoutError (builtin) - Timeout errors
//! └── MemoryError (builtin) - Memory errors
//! ```

use pyo3::exceptions::{PyIOError, PyMemoryError, PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::{create_exception, PyErr};

// Define custom exception hierarchy
// CqliteError is the base exception for all CQLite-specific errors
create_exception!(cqlite, CqliteError, pyo3::exceptions::PyException);

// Schema-related errors (Schema, Table variants)
create_exception!(cqlite, SchemaError, CqliteError);

// Query execution errors (QueryExecution, UnsupportedQuery variants)
create_exception!(cqlite, QueryError, CqliteError);

// CQL parsing errors (CqlParse variant)
create_exception!(cqlite, ParseError, CqliteError);

/// Register all exception types with the Python module.
///
/// This function must be called during module initialization to make
/// the exception types available for Python code to catch.
pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("CqliteError", m.py().get_type::<CqliteError>())?;
    m.add("SchemaError", m.py().get_type::<SchemaError>())?;
    m.add("QueryError", m.py().get_type::<QueryError>())?;
    m.add("ParseError", m.py().get_type::<ParseError>())?;
    Ok(())
}

/// Convert a `cqlite_core::Error` to a Python exception.
///
/// This function is used instead of `impl From<cqlite_core::Error> for PyErr`
/// due to Rust's orphan rules (both types are from external crates).
///
/// # Mapping Table
///
/// | Rust Variant | Python Exception |
/// |--------------|------------------|
/// | `Io` | `IOError` (builtin) |
/// | `Schema`, `Table` | `SchemaError` |
/// | `QueryExecution`, `UnsupportedQuery` | `QueryError` |
/// | `CqlParse` | `ParseError` |
/// | `Configuration`, `InvalidInput` | `ValueError` (builtin) |
/// | `Timeout` | `TimeoutError` (builtin) |
/// | `Memory` | `MemoryError` (builtin) |
/// | All others | `CqliteError` (base) |
pub fn to_py_err(err: cqlite_core::Error) -> PyErr {
    let message = err.to_string();

    match err {
        // I/O errors -> Python IOError (builtin)
        cqlite_core::Error::Io(_) => PyIOError::new_err(message),

        // Schema-related errors -> SchemaError
        cqlite_core::Error::Schema(_) | cqlite_core::Error::Table(_) => {
            SchemaError::new_err(message)
        }

        // Query execution errors -> QueryError
        cqlite_core::Error::QueryExecution(_) | cqlite_core::Error::UnsupportedQuery(_) => {
            QueryError::new_err(message)
        }

        // CQL parsing errors -> ParseError
        cqlite_core::Error::CqlParse(_) => ParseError::new_err(message),

        // Configuration/input errors -> Python ValueError (builtin)
        cqlite_core::Error::Configuration(_) | cqlite_core::Error::InvalidInput(_) => {
            PyValueError::new_err(message)
        }

        // Timeout errors -> Python TimeoutError (builtin)
        cqlite_core::Error::Timeout(_) => PyTimeoutError::new_err(message),

        // Memory errors -> Python MemoryError (builtin)
        cqlite_core::Error::Memory(_) => PyMemoryError::new_err(message),

        // Invalid state errors -> Python RuntimeError (builtin)
        // This covers cases like using a closed database
        cqlite_core::Error::InvalidState(_) => PyRuntimeError::new_err(message),

        // All other errors -> CqliteError (base exception)
        // Explicitly unmapped variants (all map to base CqliteError):
        // - Serialization, Corruption, InvalidFormat, UnsupportedFormat
        // - InvalidPath, TypeConversion, Storage, Concurrency
        // - NotFound, AlreadyExists, InvalidOperation, ConstraintViolation
        // - Transaction, Index, Compaction, Internal, Parse
        // Note: When adding new Error variants to cqlite-core, review this mapping
        _ => CqliteError::new_err(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_core::Error;

    /// Helper to extract error message from PyErr
    fn get_error_message(py: Python<'_>, err: PyErr) -> String {
        err.value(py).to_string()
    }

    #[test]
    fn test_io_error_maps_to_ioerror() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let rust_err = Error::Io(io_err);
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<PyIOError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("file not found"));
        });
    }

    #[test]
    fn test_schema_error_maps_to_schemaerror() {
        let rust_err = Error::Schema("table not found".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<SchemaError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("table not found"));
        });
    }

    #[test]
    fn test_table_error_maps_to_schemaerror() {
        let rust_err = Error::Table("invalid table".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<SchemaError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("invalid table"));
        });
    }

    #[test]
    fn test_query_execution_error_maps_to_queryerror() {
        let rust_err = Error::QueryExecution("query failed".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<QueryError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("query failed"));
        });
    }

    #[test]
    fn test_unsupported_query_maps_to_queryerror() {
        let rust_err = Error::UnsupportedQuery("UPDATE not supported".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<QueryError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("UPDATE not supported"));
        });
    }

    #[test]
    fn test_cql_parse_error_maps_to_parseerror() {
        let rust_err = Error::CqlParse("syntax error".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<ParseError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("syntax error"));
        });
    }

    #[test]
    fn test_configuration_error_maps_to_valueerror() {
        let rust_err = Error::Configuration("invalid config".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<PyValueError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("invalid config"));
        });
    }

    #[test]
    fn test_invalid_input_maps_to_valueerror() {
        let rust_err = Error::InvalidInput("bad input".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<PyValueError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("bad input"));
        });
    }

    #[test]
    fn test_timeout_error_maps_to_timeouterror() {
        let rust_err = Error::Timeout("operation timed out".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<PyTimeoutError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("operation timed out"));
        });
    }

    #[test]
    fn test_memory_error_maps_to_memoryerror() {
        let rust_err = Error::Memory("out of memory".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<PyMemoryError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("out of memory"));
        });
    }

    #[test]
    fn test_invalid_state_maps_to_runtimeerror() {
        let rust_err = Error::InvalidState("database closed".to_string());
        let py_err = to_py_err(rust_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<PyRuntimeError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("database closed"));
        });
    }

    #[test]
    fn test_other_errors_map_to_cqliteerror() {
        // Test several "other" error types that should map to base CqliteError
        let test_cases = vec![
            Error::Corruption("data corrupted".to_string()),
            Error::InvalidFormat("bad format".to_string()),
            Error::Storage("storage error".to_string()),
            Error::NotFound("resource not found".to_string()),
            Error::Internal("internal error".to_string()),
        ];

        for rust_err in test_cases {
            let py_err = to_py_err(rust_err);

            Python::with_gil(|py| {
                assert!(py_err.is_instance_of::<CqliteError>(py));
            });
        }
    }

    #[test]
    fn test_error_messages_preserved() {
        // Verify original error messages are preserved in Python exceptions
        let cases = vec![
            (
                Error::Schema("Schema validation failed: missing primary key".to_string()),
                "Schema validation failed: missing primary key",
            ),
            (
                Error::QueryExecution("Column 'foo' not found in table 'bar'".to_string()),
                "Column 'foo' not found",
            ),
            (
                Error::CqlParse("Unexpected token at position 42".to_string()),
                "Unexpected token",
            ),
        ];

        for (rust_err, expected_substr) in cases {
            let py_err = to_py_err(rust_err);

            Python::with_gil(|py| {
                let msg = get_error_message(py, py_err);
                assert!(
                    msg.contains(expected_substr),
                    "Expected '{}' to contain '{}'",
                    msg,
                    expected_substr
                );
            });
        }
    }
}
