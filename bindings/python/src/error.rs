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

        // Write-dir lock conflict -> CqliteError with clear message
        // The formatted message already contains the path and advice
        cqlite_core::Error::WriteDirLocked { .. } => CqliteError::new_err(message),

        // All other errors -> CqliteError (base exception)
        // See test_error_mapping_completeness() for the complete list of unmapped variants
        _ => CqliteError::new_err(message),
    }
}

/// Map a tokio runtime-initialization failure to a Python exception.
///
/// The shared async runtime is built lazily (see `runtime::try_get_runtime`).
/// If the host is out of threads/file descriptors/memory the build fails with an
/// [`std::io::Error`]; surfacing it here as a catchable Python exception (via the
/// same `Io` → `IOError` mapping as any other I/O failure) lets `open()` raise
/// instead of the process aborting under `panic = "abort"` (issue #1438).
pub fn runtime_init_to_py_err(err: std::io::Error) -> PyErr {
    to_py_err(cqlite_core::Error::Io(err))
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

    /// Compile-time completeness check for error variant mapping.
    ///
    /// This test ensures all `cqlite_core::Error` variants are explicitly handled
    /// in the `to_py_err` function. If a new variant is added to the core Error enum,
    /// this test will fail to compile, forcing a review of the Python error mapping.
    ///
    /// # Error Mapping Table (Complete)
    ///
    /// | Rust Variant | Python Exception | Notes |
    /// |--------------|------------------|-------|
    /// | `Io` | `IOError` (builtin) | I/O operations |
    /// | `Schema` | `SchemaError` | Schema validation |
    /// | `Table` | `SchemaError` | Table-related errors |
    /// | `QueryExecution` | `QueryError` | Query execution |
    /// | `UnsupportedQuery` | `QueryError` | Unsupported operations |
    /// | `CqlParse` | `ParseError` | CQL syntax errors |
    /// | `Configuration` | `ValueError` (builtin) | Config errors |
    /// | `InvalidInput` | `ValueError` (builtin) | Input validation |
    /// | `Timeout` | `TimeoutError` (builtin) | Operation timeouts |
    /// | `Memory` | `MemoryError` (builtin) | Memory allocation |
    /// | `InvalidState` | `RuntimeError` (builtin) | Invalid state (e.g., closed DB) |
    /// | `Serialization` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Corruption` | `CqliteError` (base) | Unmapped - generic error |
    /// | `InvalidFormat` | `CqliteError` (base) | Unmapped - generic error |
    /// | `UnsupportedFormat` | `CqliteError` (base) | Unmapped - generic error |
    /// | `InvalidPath` | `CqliteError` (base) | Unmapped - generic error |
    /// | `TypeConversion` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Storage` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Concurrency` | `CqliteError` (base) | Unmapped - generic error |
    /// | `NotFound` | `CqliteError` (base) | Unmapped - generic error |
    /// | `AlreadyExists` | `CqliteError` (base) | Unmapped - generic error |
    /// | `InvalidOperation` | `CqliteError` (base) | Unmapped - generic error |
    /// | `ConstraintViolation` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Transaction` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Index` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Compaction` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Internal` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Parse` | `CqliteError` (base) | Unmapped - generic error |
    /// | `Wasm` (wasm32 only) | `CqliteError` (base) | Unmapped - generic error |
    #[test]
    fn test_error_mapping_completeness() {
        // This function uses an exhaustive match to ensure all Error variants
        // are accounted for. If a new variant is added to cqlite_core::Error,
        // this will fail to compile until the match is updated.
        //
        // The goal is NOT to test runtime behavior (other tests do that),
        // but to serve as a compile-time check and documentation.

        fn verify_all_variants_documented(err: &Error) {
            match err {
                // === Explicitly mapped to specific Python exceptions ===
                Error::Io(_) => { /* Maps to PyIOError */ }
                Error::Schema(_) => { /* Maps to SchemaError */ }
                Error::Table(_) => { /* Maps to SchemaError */ }
                Error::QueryExecution(_) => { /* Maps to QueryError */ }
                Error::UnsupportedQuery(_) => { /* Maps to QueryError */ }
                Error::CqlParse(_) => { /* Maps to ParseError */ }
                Error::Configuration(_) => { /* Maps to PyValueError */ }
                Error::InvalidInput(_) => { /* Maps to PyValueError */ }
                Error::Timeout(_) => { /* Maps to PyTimeoutError */ }
                Error::Memory(_) => { /* Maps to PyMemoryError */ }
                Error::InvalidState(_) => { /* Maps to PyRuntimeError */ }

                // === Unmapped variants (fall through to base CqliteError) ===
                Error::Serialization { .. } => { /* Maps to CqliteError */ }
                Error::Corruption(_) => { /* Maps to CqliteError */ }
                Error::InvalidFormat(_) => { /* Maps to CqliteError */ }
                Error::UnsupportedFormat(_) => { /* Maps to CqliteError */ }
                Error::UnsupportedVersion { .. } => { /* Maps to CqliteError */ }
                Error::InvalidPath(_) => { /* Maps to CqliteError */ }
                Error::TypeConversion(_) => { /* Maps to CqliteError */ }
                Error::Storage(_) => { /* Maps to CqliteError */ }
                Error::Concurrency(_) => { /* Maps to CqliteError */ }
                Error::NotFound(_) => { /* Maps to CqliteError */ }
                Error::AlreadyExists(_) => { /* Maps to CqliteError */ }
                Error::InvalidOperation(_) => { /* Maps to CqliteError */ }
                Error::ConstraintViolation(_) => { /* Maps to CqliteError */ }
                Error::Transaction(_) => { /* Maps to CqliteError */ }
                Error::Index(_) => { /* Maps to CqliteError */ }
                Error::Compaction(_) => { /* Maps to CqliteError */ }
                Error::Internal(_) => { /* Maps to CqliteError */ }
                Error::Parse(_) => { /* Maps to CqliteError */ }

                // Write-dir lock conflict — maps to CqliteError with clear message
                Error::WriteDirLocked { .. } => { /* Maps to CqliteError */ }

                // Conditional variant (only exists on wasm32)
                #[cfg(target_arch = "wasm32")]
                Error::Wasm(_) => { /* Maps to CqliteError */ }
            }
        }

        // Create sample errors to verify the function compiles
        // (This exercises the exhaustive match at compile time)
        let test_errors = vec![
            Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "test")),
            Error::Schema("test".to_string()),
            Error::Corruption("test".to_string()),
            Error::InvalidState("test".to_string()),
            Error::Serialization {
                message: "test".to_string(),
                source: None,
            },
        ];

        for err in &test_errors {
            verify_all_variants_documented(err);
        }

        // Additionally verify that the to_py_err function handles all these
        // (This is a runtime sanity check, but primarily the compile-time check matters)
        for err in test_errors {
            let _py_err = to_py_err(err);
            // If we got here, the mapping didn't panic
        }
    }

    #[test]
    fn test_runtime_init_error_maps_to_catchable_exception() {
        // A runtime-init failure must surface as a catchable Python exception
        // (IOError), never a process abort (issue #1438).
        let io_err =
            std::io::Error::other("cannot spawn worker threads: resource temporarily unavailable");
        let py_err = runtime_init_to_py_err(io_err);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<PyIOError>(py));
            let msg = get_error_message(py, py_err);
            assert!(msg.contains("cannot spawn worker threads"));
        });
    }

    #[test]
    fn test_unmapped_variants_map_to_base_error() {
        // Verify that all unmapped variants correctly map to base CqliteError
        let unmapped_cases = vec![
            Error::Serialization {
                message: "serialization failed".to_string(),
                source: None,
            },
            Error::Corruption("data corrupted".to_string()),
            Error::InvalidFormat("bad format".to_string()),
            Error::UnsupportedFormat("unsupported format".to_string()),
            Error::InvalidPath("invalid path".to_string()),
            Error::TypeConversion("type conversion failed".to_string()),
            Error::Storage("storage error".to_string()),
            Error::Concurrency("concurrency error".to_string()),
            Error::NotFound("not found".to_string()),
            Error::AlreadyExists("already exists".to_string()),
            Error::InvalidOperation("invalid operation".to_string()),
            Error::ConstraintViolation("constraint violated".to_string()),
            Error::Transaction("transaction error".to_string()),
            Error::Index("index error".to_string()),
            Error::Compaction("compaction error".to_string()),
            Error::Internal("internal error".to_string()),
            Error::Parse("parse error".to_string()),
            Error::WriteDirLocked {
                path: "/tmp/test-write-dir".to_string(),
            },
        ];

        for rust_err in unmapped_cases {
            let py_err = to_py_err(rust_err);

            Python::with_gil(|py| {
                assert!(
                    py_err.is_instance_of::<CqliteError>(py),
                    "Unmapped variant should map to base CqliteError"
                );
            });
        }
    }
}
