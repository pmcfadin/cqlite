//! Error mapping layer for Python bindings.
//!
//! Maps `cqlite_core::Error` variants to Python exceptions **by reading the
//! shared FFI error contract** (`cqlite_ffi_common::error_contract`) — the ONE
//! authoritative variant -> (python class, node code, category, recoverable,
//! prefix) table that `bindings/node` reads too, so a core error has the same
//! identity in every binding (issue #1451). This module owns only the
//! identifier -> concrete PyO3 class step; it never decides WHICH class a
//! variant gets.
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

use cqlite_ffi_common::error_contract::{contract_for, PyExceptionClass};
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

// A cooperative cancellation / abort (issue #2264). Dedicated so a cancelled
// scan is never mislabeled as (or silently folded into) a generic CqliteError
// — callers can `except cqlite.CancelledError` precisely, matching the Node
// binding's dedicated `CANCELLED` code.
create_exception!(cqlite, CancelledError, CqliteError);

/// Register all exception types with the Python module.
///
/// This function must be called during module initialization to make
/// the exception types available for Python code to catch.
pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("CqliteError", m.py().get_type::<CqliteError>())?;
    m.add("SchemaError", m.py().get_type::<SchemaError>())?;
    m.add("QueryError", m.py().get_type::<QueryError>())?;
    m.add("ParseError", m.py().get_type::<ParseError>())?;
    m.add("CancelledError", m.py().get_type::<CancelledError>())?;
    Ok(())
}

/// Convert a `cqlite_core::Error` to a Python exception.
///
/// This function is used instead of `impl From<cqlite_core::Error> for PyErr`
/// due to Rust's orphan rules (both types are from external crates).
///
/// The class is chosen **by variant** from the shared FFI error contract
/// (`cqlite_ffi_common::error_contract`), never re-derived here — that table is
/// also what `bindings/node` reads, so the two bindings cannot drift apart
/// (issue #1451). To change how a variant surfaces, edit the table.
///
/// # Mapping Table (from the shared contract)
///
/// | Rust Variant | Python Exception |
/// |--------------|------------------|
/// | `Io` | `IOError` (builtin) |
/// | `Schema`, `Table` | `SchemaError` |
/// | `QueryExecution`, `ResultTooLarge`, `UnsupportedQuery` | `QueryError` |
/// | `CqlParse` | `ParseError` |
/// | `Configuration`, `InvalidInput` | `ValueError` (builtin) |
/// | `Timeout` | `TimeoutError` (builtin) |
/// | `Memory` | `MemoryError` (builtin) |
/// | `InvalidState` | `RuntimeError` (builtin) |
/// | `Cancelled` | `CancelledError` (issue #2264 — never `IOError`) |
/// | All others (incl. `Corruption`) | `CqliteError` (base) |
pub fn to_py_err(err: cqlite_core::Error) -> PyErr {
    let message = err.to_string();

    // ONE lookup in the shared contract; the match below only turns the
    // class IDENTIFIER into the concrete PyO3 class. It is exhaustive, so a new
    // `PyExceptionClass` fails to compile until it is wired to a real class.
    match contract_for(&err).py_class {
        PyExceptionClass::Io => PyIOError::new_err(message),
        PyExceptionClass::Value => PyValueError::new_err(message),
        PyExceptionClass::Timeout => PyTimeoutError::new_err(message),
        PyExceptionClass::Memory => PyMemoryError::new_err(message),
        PyExceptionClass::Runtime => PyRuntimeError::new_err(message),
        PyExceptionClass::Cqlite => CqliteError::new_err(message),
        PyExceptionClass::Schema => SchemaError::new_err(message),
        PyExceptionClass::Query => QueryError::new_err(message),
        PyExceptionClass::Parse => ParseError::new_err(message),
        PyExceptionClass::Cancelled => CancelledError::new_err(message),
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
    use cqlite_ffi_common::error_contract::FfiErrorVariant;

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

    /// Issue #2264: a cooperative cancellation must surface as a DEDICATED
    /// `CancelledError`, and explicitly NEVER as `IOError` (the pre-fix
    /// behaviour: `Error::Cancelled` fell into `ErrorCategory::System`).
    #[test]
    fn test_cancelled_maps_to_cancellederror_not_ioerror() {
        let py_err = to_py_err(Error::Cancelled);

        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<CancelledError>(py));
            assert!(
                !py_err.is_instance_of::<PyIOError>(py),
                "a cooperative cancellation must never surface as IOError"
            );
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

    /// The Python exception class each core variant is EXPECTED to surface as —
    /// a hand-written restatement of the shared contract's `py_class` column.
    ///
    /// Two guards in one:
    ///
    /// 1. **Compile-time completeness.** The match is exhaustive over
    ///    `cqlite_core::Error`, so adding a variant to the core enum fails to
    ///    compile here until the Python identity is reviewed.
    /// 2. **Content.** `test_error_mapping_completeness` asserts the shared
    ///    table (and the exception `to_py_err` actually raises) agrees with this
    ///    independent statement, so an accidental edit to the table's
    ///    `py_class` column fails HERE instead of reaching users.
    ///
    /// # Error Mapping Table (Complete)
    ///
    /// | Rust Variant | Python Exception | Notes |
    /// |--------------|------------------|-------|
    /// | `Io` | `IOError` (builtin) | I/O operations |
    /// | `Schema`, `Table` | `SchemaError` | Schema/table validation |
    /// | `QueryExecution`, `ResultTooLarge`, `UnsupportedQuery` | `QueryError` | Query execution |
    /// | `CqlParse` | `ParseError` | CQL syntax errors (Node code `PARSE`, #1451) |
    /// | `Configuration`, `InvalidInput` | `ValueError` (builtin) | Config/input validation |
    /// | `Timeout` | `TimeoutError` (builtin) | Node code `TIMEOUT` (#1451) |
    /// | `Memory` | `MemoryError` (builtin) | Node code `MEMORY` (#1451) |
    /// | `InvalidState` | `RuntimeError` (builtin) | Invalid state (e.g. closed DB) |
    /// | `Cancelled` | `CancelledError` | Cooperative abort (#2264) — never `IOError` |
    /// | `Corruption`, `Serialization`, `InvalidFormat`, `UnsupportedFormat` | `CqliteError` (base) | No closer Python class |
    /// | `UnsupportedVersion`, `UnsupportedCommitLogVersion`, `CorruptCommitLogFrame` | `CqliteError` (base) | Format gating |
    /// | `InvalidReadPath`, `ForcedReadPathUnavailable` | `CqliteError` (base) | Read-path knob (#1918) |
    /// | `InvalidPath`, `TypeConversion`, `Storage`, `Concurrency`, `NotFound` | `CqliteError` (base) | |
    /// | `AlreadyExists`, `InvalidOperation`, `ConstraintViolation`, `Transaction` | `CqliteError` (base) | |
    /// | `Index`, `Compaction`, `Internal`, `Parse`, `WriteDirLocked` | `CqliteError` (base) | |
    /// | `Wasm` (wasm32 only) | `CqliteError` (base) | |
    fn expected_py_class(err: &Error) -> PyExceptionClass {
        match err {
            // === Explicitly mapped to specific Python exceptions ===
            Error::Io(_) => PyExceptionClass::Io,
            Error::Schema(_) | Error::Table(_) => PyExceptionClass::Schema,
            Error::QueryExecution(_)
            | Error::ResultTooLarge { .. }
            | Error::UnsupportedQuery(_) => PyExceptionClass::Query,
            Error::CqlParse(_) => PyExceptionClass::Parse,
            Error::Configuration(_) | Error::InvalidInput(_) => PyExceptionClass::Value,
            Error::Timeout(_) => PyExceptionClass::Timeout,
            // Query execution budget elapsed (issue #1695): the SAME builtin
            // `TimeoutError` as its sibling above, so `except TimeoutError:` catches
            // both. Its core `ErrorCategory` is `Query` — a separate axis from the
            // Python class, which is why this is not `PyExceptionClass::Query`. Node
            // gives the same variant code `TIMEOUT`, which is the cross-binding
            // agreement the shared table (#1451) exists to enforce.
            Error::QueryTimeout { .. } => PyExceptionClass::Timeout,
            Error::Memory(_) => PyExceptionClass::Memory,
            Error::InvalidState(_) => PyExceptionClass::Runtime,
            Error::Cancelled => PyExceptionClass::Cancelled,

            // === Variants with no closer Python class: the base exception ===
            Error::Serialization { .. } => PyExceptionClass::Cqlite,
            Error::Corruption(_) => PyExceptionClass::Cqlite,
            Error::InvalidFormat(_) => PyExceptionClass::Cqlite,
            Error::UnsupportedFormat(_) => PyExceptionClass::Cqlite,
            Error::UnsupportedVersion { .. } => PyExceptionClass::Cqlite,
            // CommitLog reader (#2389) — not bound yet (v1 is library+CLI only).
            Error::UnsupportedCommitLogVersion { .. } => PyExceptionClass::Cqlite,
            Error::CorruptCommitLogFrame(_) => PyExceptionClass::Cqlite,
            // Read-path forcing knob errors (#1918).
            Error::InvalidReadPath { .. } => PyExceptionClass::Cqlite,
            Error::ForcedReadPathUnavailable { .. } => PyExceptionClass::Cqlite,
            Error::InvalidPath(_) => PyExceptionClass::Cqlite,
            Error::TypeConversion(_) => PyExceptionClass::Cqlite,
            Error::Storage(_) => PyExceptionClass::Cqlite,
            Error::Concurrency(_) => PyExceptionClass::Cqlite,
            Error::NotFound(_) => PyExceptionClass::Cqlite,
            Error::AlreadyExists(_) => PyExceptionClass::Cqlite,
            Error::InvalidOperation(_) => PyExceptionClass::Cqlite,
            Error::ConstraintViolation(_) => PyExceptionClass::Cqlite,
            Error::Transaction(_) => PyExceptionClass::Cqlite,
            Error::Index(_) => PyExceptionClass::Cqlite,
            Error::Compaction(_) => PyExceptionClass::Cqlite,
            Error::Internal(_) => PyExceptionClass::Cqlite,
            Error::Parse(_) => PyExceptionClass::Cqlite,
            Error::WriteDirLocked { .. } => PyExceptionClass::Cqlite,

            // Conditional variant (only exists on wasm32)
            #[cfg(target_arch = "wasm32")]
            Error::Wasm(_) => PyExceptionClass::Cqlite,
        }
    }

    /// Is the exception `to_py_err` produced an instance of `class`?
    ///
    /// Exhaustive over `PyExceptionClass`, so a new class in the shared contract
    /// fails to compile until this check (and `to_py_err`) handles it.
    fn raised_class_matches(py: Python<'_>, py_err: &PyErr, class: PyExceptionClass) -> bool {
        match class {
            PyExceptionClass::Io => py_err.is_instance_of::<PyIOError>(py),
            PyExceptionClass::Value => py_err.is_instance_of::<PyValueError>(py),
            PyExceptionClass::Timeout => py_err.is_instance_of::<PyTimeoutError>(py),
            PyExceptionClass::Memory => py_err.is_instance_of::<PyMemoryError>(py),
            PyExceptionClass::Runtime => py_err.is_instance_of::<PyRuntimeError>(py),
            // The base class is only correct if NO subclass matched, otherwise
            // "maps to CqliteError" would be satisfied by every subclass too.
            PyExceptionClass::Cqlite => {
                py_err.is_instance_of::<CqliteError>(py)
                    && !py_err.is_instance_of::<SchemaError>(py)
                    && !py_err.is_instance_of::<QueryError>(py)
                    && !py_err.is_instance_of::<ParseError>(py)
                    && !py_err.is_instance_of::<CancelledError>(py)
            }
            PyExceptionClass::Schema => py_err.is_instance_of::<SchemaError>(py),
            PyExceptionClass::Query => py_err.is_instance_of::<QueryError>(py),
            PyExceptionClass::Parse => py_err.is_instance_of::<ParseError>(py),
            PyExceptionClass::Cancelled => py_err.is_instance_of::<CancelledError>(py),
        }
    }

    /// Every core variant maps to the documented Python class — in the shared
    /// contract AND in the exception `to_py_err` actually raises.
    #[test]
    fn test_error_mapping_completeness() {
        let mut checked = 0usize;
        for &variant in FfiErrorVariant::ALL {
            let Some(err) = variant.sample_error() else {
                // Only `Wasm` lacks a representative value off wasm32.
                assert_eq!(variant, FfiErrorVariant::Wasm);
                continue;
            };
            let expected = expected_py_class(&err);
            let row = contract_for(&err);
            assert_eq!(
                row.py_class, expected,
                "shared contract row {} maps to {:?}, this binding documents {:?}",
                row.variant, row.py_class, expected
            );

            let py_err = to_py_err(err);
            Python::with_gil(|py| {
                assert!(
                    raised_class_matches(py, &py_err, expected),
                    "{} must raise {}",
                    row.variant,
                    expected.as_str()
                );
            });
            checked += 1;
        }
        let expected_checked =
            FfiErrorVariant::ALL.len() - if cfg!(target_arch = "wasm32") { 0 } else { 1 };
        assert_eq!(
            checked, expected_checked,
            "every contract row except Wasm (off wasm32) must be exercised"
        );
    }

    /// The four cross-binding divergences issue #1451 fixes, pinned on the
    /// Python side of the contract.
    #[test]
    fn test_pinned_contract_rows() {
        let cases = [
            (
                Error::cql_parse("syntax error"),
                PyExceptionClass::Parse,
                "PARSE",
            ),
            (
                Error::invalid_input("bad input"),
                PyExceptionClass::Value,
                "INVALID_INPUT",
            ),
            (
                Error::Timeout("timed out".to_string()),
                PyExceptionClass::Timeout,
                "TIMEOUT",
            ),
            (
                Error::memory("out of memory"),
                PyExceptionClass::Memory,
                "MEMORY",
            ),
            (
                Error::corruption("data corrupted"),
                PyExceptionClass::Cqlite,
                "PARSE",
            ),
        ];

        for (err, py_class, node_code) in cases {
            let row = contract_for(&err);
            assert_eq!(row.py_class, py_class, "py_class for {}", row.variant);
            // Asserted here too: this binding and Node read the SAME row, so a
            // Node-side code change cannot silently move the Python class.
            assert_eq!(row.node_code, node_code, "node_code for {}", row.variant);
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
