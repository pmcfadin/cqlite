//! Error mapping layer for Node.js bindings.
//!
//! Maps `cqlite_core::Error` variants to JavaScript errors via napi-rs.
//!
//! # Error Categories
//!
//! | Rust Variant | JS Error Message Prefix |
//! |--------------|------------------------|
//! | `Io` | `IoError:` |
//! | `Schema`, `Table` | `SchemaError:` |
//! | `QueryExecution`, `UnsupportedQuery` | `QueryError:` |
//! | `CqlParse` | `ParseError:` |
//! | `Configuration`, `InvalidInput` | `ValueError:` |
//! | `Timeout` | `TimeoutError:` |
//! | `Memory` | `MemoryError:` |
//! | `InvalidState` | `RuntimeError:` |
//! | All others | (original message) |

use cqlite_core::Error;

/// Convert a `cqlite_core::Error` to a `napi::Error`.
///
/// This function maps Rust errors to JavaScript errors with categorized
/// prefixes to allow JavaScript code to distinguish error types.
///
/// # Example
///
/// ```javascript
/// try {
///   await db.execute("INVALID SQL");
/// } catch (e) {
///   if (e.message.startsWith("ParseError:")) {
///     console.log("SQL syntax error");
///   }
/// }
/// ```
pub fn to_napi_error(err: Error) -> napi::Error {
    let message = err.to_string();

    match err {
        // I/O errors - file/path access issues
        Error::Io(_) => napi::Error::new(
            napi::Status::GenericFailure,
            format!("IoError: {}", message),
        ),

        // Schema-related errors
        Error::Schema(_) | Error::Table(_) => napi::Error::new(
            napi::Status::InvalidArg,
            format!("SchemaError: {}", message),
        ),

        // Query execution errors
        Error::QueryExecution(_) | Error::UnsupportedQuery(_) => napi::Error::new(
            napi::Status::GenericFailure,
            format!("QueryError: {}", message),
        ),

        // CQL parsing errors
        Error::CqlParse(_) => {
            napi::Error::new(napi::Status::InvalidArg, format!("ParseError: {}", message))
        }

        // Configuration/input validation errors
        Error::Configuration(_) | Error::InvalidInput(_) => {
            napi::Error::new(napi::Status::InvalidArg, format!("ValueError: {}", message))
        }

        // Timeout errors
        Error::Timeout(_) => napi::Error::new(
            napi::Status::GenericFailure,
            format!("TimeoutError: {}", message),
        ),

        // Memory errors
        Error::Memory(_) => napi::Error::new(
            napi::Status::GenericFailure,
            format!("MemoryError: {}", message),
        ),

        // Invalid state errors (e.g., using closed database)
        Error::InvalidState(_) => napi::Error::new(
            napi::Status::GenericFailure,
            format!("RuntimeError: {}", message),
        ),

        // All other errors - use original message
        _ => napi::Error::new(napi::Status::GenericFailure, message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_mapping() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let rust_err = Error::Io(io_err);
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("IoError:"));
        assert!(napi_err.reason.contains("file not found"));
    }

    #[test]
    fn test_schema_error_mapping() {
        let rust_err = Error::Schema("table not found".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("SchemaError:"));
        assert!(napi_err.reason.contains("table not found"));
    }

    #[test]
    fn test_table_error_mapping() {
        let rust_err = Error::Table("invalid table".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("SchemaError:"));
    }

    #[test]
    fn test_query_execution_error_mapping() {
        let rust_err = Error::QueryExecution("query failed".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("QueryError:"));
    }

    #[test]
    fn test_unsupported_query_error_mapping() {
        let rust_err = Error::UnsupportedQuery("UPDATE not supported".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("QueryError:"));
    }

    #[test]
    fn test_cql_parse_error_mapping() {
        let rust_err = Error::CqlParse("syntax error at position 42".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("ParseError:"));
        assert!(napi_err.reason.contains("syntax error"));
    }

    #[test]
    fn test_configuration_error_mapping() {
        let rust_err = Error::Configuration("invalid config".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("ValueError:"));
    }

    #[test]
    fn test_invalid_input_error_mapping() {
        let rust_err = Error::InvalidInput("bad input".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("ValueError:"));
    }

    #[test]
    fn test_timeout_error_mapping() {
        let rust_err = Error::Timeout("operation timed out".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("TimeoutError:"));
    }

    #[test]
    fn test_memory_error_mapping() {
        let rust_err = Error::Memory("out of memory".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("MemoryError:"));
    }

    #[test]
    fn test_invalid_state_error_mapping() {
        let rust_err = Error::InvalidState("database closed".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("RuntimeError:"));
    }

    #[test]
    fn test_other_errors_use_original_message() {
        let rust_err = Error::Corruption("data corrupted".to_string());
        let napi_err = to_napi_error(rust_err);

        // Should not have a prefix, just the original message
        assert!(napi_err.reason.contains("data corrupted"));
        assert!(!napi_err.reason.starts_with("IoError:"));
        assert!(!napi_err.reason.starts_with("SchemaError:"));
    }

    /// Compile-time completeness check for error variant mapping.
    ///
    /// This test ensures all `cqlite_core::Error` variants are accounted for.
    /// If a new variant is added to the core Error enum, this will fail to compile.
    #[test]
    fn test_error_mapping_completeness() {
        fn verify_all_variants_documented(err: &Error) {
            match err {
                // Explicitly mapped variants
                Error::Io(_) => { /* Maps to IoError */ }
                Error::Schema(_) => { /* Maps to SchemaError */ }
                Error::Table(_) => { /* Maps to SchemaError */ }
                Error::QueryExecution(_) => { /* Maps to QueryError */ }
                Error::UnsupportedQuery(_) => { /* Maps to QueryError */ }
                Error::CqlParse(_) => { /* Maps to ParseError */ }
                Error::Configuration(_) => { /* Maps to ValueError */ }
                Error::InvalidInput(_) => { /* Maps to ValueError */ }
                Error::Timeout(_) => { /* Maps to TimeoutError */ }
                Error::Memory(_) => { /* Maps to MemoryError */ }
                Error::InvalidState(_) => { /* Maps to RuntimeError */ }

                // Unmapped variants (fall through to base error)
                Error::Serialization { .. } => { /* Uses original message */ }
                Error::Corruption(_) => { /* Uses original message */ }
                Error::InvalidFormat(_) => { /* Uses original message */ }
                Error::UnsupportedFormat(_) => { /* Uses original message */ }
                Error::InvalidPath(_) => { /* Uses original message */ }
                Error::TypeConversion(_) => { /* Uses original message */ }
                Error::Storage(_) => { /* Uses original message */ }
                Error::Concurrency(_) => { /* Uses original message */ }
                Error::NotFound(_) => { /* Uses original message */ }
                Error::AlreadyExists(_) => { /* Uses original message */ }
                Error::InvalidOperation(_) => { /* Uses original message */ }
                Error::ConstraintViolation(_) => { /* Uses original message */ }
                Error::Transaction(_) => { /* Uses original message */ }
                Error::Index(_) => { /* Uses original message */ }
                Error::Compaction(_) => { /* Uses original message */ }
                Error::Internal(_) => { /* Uses original message */ }
                Error::Parse(_) => { /* Uses original message */ }

                #[cfg(target_arch = "wasm32")]
                Error::Wasm(_) => { /* Uses original message */ }
            }
        }

        // Exercise the exhaustive match
        let test_errors = vec![
            Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "test")),
            Error::Schema("test".to_string()),
            Error::Corruption("test".to_string()),
        ];

        for err in &test_errors {
            verify_all_variants_documented(err);
        }
    }
}
