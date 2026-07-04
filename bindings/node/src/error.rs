//! Error mapping layer for Node.js bindings.
//!
//! Maps `cqlite_core::Error` variants to JavaScript Error objects with
//! structured metadata properties.
//!
//! # Error Properties (Issue #297)
//!
//! Each error includes:
//! - `code`: String error code (e.g., "IO", "SCHEMA", "QUERY")
//! - `category`: Category name from ErrorCategory (e.g., "System", "Schema")
//! - `isRecoverable`: Boolean indicating if the error is recoverable
//!
//! # Error Code Mapping
//!
//! | Rust Category | JS Code | JS Message Prefix |
//! |---------------|---------|-------------------|
//! | System | `IO` | `IoError:` |
//! | Schema | `SCHEMA` | `SchemaError:` |
//! | Query | `QUERY` | `QueryError:` |
//! | Data | `PARSE` | `ParseError:` |
//! | Configuration | `CONFIG` | `ValueError:` |
//! | Storage | `STORAGE` | (original) |
//! | NotFound | `NOT_FOUND` | (original) |
//! | Logic | `INVALID_INPUT` | `RuntimeError:` |
//! | Concurrency | `CONCURRENCY` | (original) |
//! | Conflict | `CONFLICT` | (original) |
//! | Constraint | `CONSTRAINT` | (original) |
//! | Transaction | `TRANSACTION` | (original) |
//! | Platform | `PLATFORM` | (original) |
//! | Internal | `INTERNAL` | (original) |
//!
//! # Example
//!
//! ```javascript
//! try {
//!   await db.execute("INVALID SQL");
//! } catch (e) {
//!   console.log(e.code);          // "PARSE" or "QUERY"
//!   console.log(e.category);      // "Query" or "Data"
//!   console.log(e.isRecoverable); // false
//!   if (e.code === "PARSE") {
//!     console.log("SQL syntax error");
//!   }
//! }
//! ```

use cqlite_core::error::ErrorCategory;
use cqlite_core::Error;

/// Error metadata extracted from a cqlite_core::Error.
///
/// This struct holds the structured error information that will be
/// attached to JavaScript Error objects.
#[derive(Debug, Clone)]
pub struct ErrorMetadata {
    /// String error code (e.g., "IO", "SCHEMA", "QUERY")
    pub code: &'static str,
    /// Category name (e.g., "System", "Schema", "Query")
    pub category: String,
    /// Whether the error is recoverable
    pub is_recoverable: bool,
    /// Error message with prefix
    pub message: String,
}

/// Convert ErrorCategory to a string code for JavaScript.
///
/// Maps the 14 ErrorCategory variants to simplified string codes
/// matching the M4 spec requirements.
pub fn category_to_code(category: ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::System => "IO",
        ErrorCategory::Data => "PARSE",
        ErrorCategory::Schema => "SCHEMA",
        ErrorCategory::Query => "QUERY",
        ErrorCategory::Configuration => "CONFIG",
        ErrorCategory::Storage => "STORAGE",
        ErrorCategory::Concurrency => "CONCURRENCY",
        ErrorCategory::NotFound => "NOT_FOUND",
        ErrorCategory::Conflict => "CONFLICT",
        ErrorCategory::Logic => "INVALID_INPUT",
        ErrorCategory::Constraint => "CONSTRAINT",
        ErrorCategory::Transaction => "TRANSACTION",
        ErrorCategory::Platform => "PLATFORM",
        ErrorCategory::Internal => "INTERNAL",
    }
}

/// Get the message prefix for an error category.
fn category_to_prefix(category: ErrorCategory) -> Option<&'static str> {
    match category {
        ErrorCategory::System => Some("IoError"),
        ErrorCategory::Schema => Some("SchemaError"),
        ErrorCategory::Query => Some("QueryError"),
        ErrorCategory::Data => Some("ParseError"),
        ErrorCategory::Configuration => Some("ValueError"),
        ErrorCategory::Logic => Some("RuntimeError"),
        // Other categories don't have special prefixes
        _ => None,
    }
}

/// Extract error metadata from a cqlite_core::Error.
///
/// This provides all the structured information needed for the JavaScript error.
pub fn extract_metadata(err: &Error) -> ErrorMetadata {
    let category = err.category();
    let code = category_to_code(category);
    let category_name = category.to_string();
    let is_recoverable = err.is_recoverable();
    let original_message = err.to_string();

    // Format message with prefix if applicable
    let message = match category_to_prefix(category) {
        Some(prefix) => format!("{prefix}: {original_message}"),
        None => original_message,
    };

    ErrorMetadata {
        code,
        category: category_name,
        is_recoverable,
        message,
    }
}

/// Convert a `cqlite_core::Error` to a `napi::Error` with structured properties.
///
/// The returned error will have the following properties accessible from JavaScript:
/// - `code`: String error code
/// - `category`: Category name
/// - `isRecoverable`: Boolean
///
/// # Note
///
/// napi-rs 2.x doesn't directly support adding custom properties to Error objects
/// returned from `napi::Error`. To work around this, we encode the metadata in
/// the error message in a parseable format, and also expose helper functions
/// that can be used to create properly structured errors when an Env is available.
pub fn to_napi_error(err: Error) -> napi::Error {
    let metadata = extract_metadata(&err);

    // Create a structured error using napi's Error with custom message
    // The message format includes metadata that JavaScript can parse:
    // [CODE|CATEGORY|RECOVERABLE] Message
    //
    // However, for better DX, we also provide the metadata directly via
    // a custom approach. Since napi::Error doesn't support custom properties
    // directly, we'll use a wrapper approach in the JavaScript layer.
    //
    // For now, we embed metadata in a machine-parseable format at the end
    // of the message, which the index.js wrapper can extract.
    let message = &metadata.message;
    let code = metadata.code;
    let category = &metadata.category;
    let is_recoverable = metadata.is_recoverable;
    let formatted_message =
        format!("{message}\0code={code}\0category={category}\0isRecoverable={is_recoverable}");

    napi::Error::new(napi::Status::GenericFailure, formatted_message)
}

/// Map a tokio runtime-initialization failure to a `napi::Error`.
///
/// The shared async runtime is built lazily (see `runtime::try_get_runtime`).
/// If the host is out of threads/file descriptors/memory the build fails with an
/// [`std::io::Error`]; surfacing it here as a `napi::Error` (via the same
/// `Io` → System/`IO` mapping as any other I/O failure) lets `open()` reject/throw
/// instead of the process aborting under `panic = "abort"` (issue #1438).
pub fn runtime_init_error(err: std::io::Error) -> napi::Error {
    to_napi_error(Error::Io(err))
}

/// Default cap on the number of rows `executeNative` will materialize into JS
/// objects on the event-loop thread (issue #1442).
///
/// `executeNative` scans off the event loop, but the per-row JS-object build in
/// `resolve()` is O(rows) work that MUST run on the JS thread (napi `Env` is
/// thread-bound), so it cannot be moved off-loop. A very large result set would
/// therefore freeze timers/HTTP handlers for the duration of the burst. This
/// bound rejects such calls with a typed error steering the caller to
/// `executeStreaming`. Kept generous so ordinary queries are unaffected.
pub const DEFAULT_MAX_NATIVE_ROWS: usize = 100_000;

/// Resolve the `executeNative` on-loop row cap (issue #1442).
///
/// Reads `CQLITE_NODE_MAX_NATIVE_ROWS` (a positive integer) as a documented
/// override; falls back to [`DEFAULT_MAX_NATIVE_ROWS`]. Call this on the JS
/// thread (in `execute_native`) and pass the value into the task so `compute()`
/// never reads the process environment from a worker thread.
pub fn native_row_limit() -> usize {
    std::env::var("CQLITE_NODE_MAX_NATIVE_ROWS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MAX_NATIVE_ROWS)
}

/// Typed error for an `executeNative` result set that exceeds the on-loop cap.
///
/// Steers the caller to `executeStreaming` rather than freezing the event loop
/// materializing every row (issue #1442).
pub fn native_rows_exceeded_error(rows: usize, limit: usize) -> napi::Error {
    simple_error(format!(
        "executeNative result set of {rows} rows exceeds the on-event-loop \
         materialization limit of {limit}; use executeStreaming() for large \
         result sets (or raise CQLITE_NODE_MAX_NATIVE_ROWS)"
    ))
}

/// Create a napi::Error with a simple message (no metadata).
///
/// Use this for errors that don't originate from cqlite_core::Error,
/// such as "Database is closed".
pub fn simple_error(message: impl Into<String>) -> napi::Error {
    let msg = message.into();
    // For consistency, add minimal metadata
    let formatted_message =
        format!("{msg}\0code=INVALID_INPUT\0category=Logic\0isRecoverable=false");
    napi::Error::new(napi::Status::GenericFailure, formatted_message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_category_to_code() {
        assert_eq!(category_to_code(ErrorCategory::System), "IO");
        assert_eq!(category_to_code(ErrorCategory::Schema), "SCHEMA");
        assert_eq!(category_to_code(ErrorCategory::Query), "QUERY");
        assert_eq!(category_to_code(ErrorCategory::Data), "PARSE");
        assert_eq!(category_to_code(ErrorCategory::Configuration), "CONFIG");
        assert_eq!(category_to_code(ErrorCategory::Storage), "STORAGE");
        assert_eq!(category_to_code(ErrorCategory::NotFound), "NOT_FOUND");
        assert_eq!(category_to_code(ErrorCategory::Logic), "INVALID_INPUT");
    }

    #[test]
    fn test_io_error_metadata() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let rust_err = Error::Io(io_err);
        let metadata = extract_metadata(&rust_err);

        assert_eq!(metadata.code, "IO");
        assert_eq!(metadata.category, "System");
        assert!(metadata.is_recoverable);
        assert!(metadata.message.contains("IoError:"));
        assert!(metadata.message.contains("file not found"));
    }

    #[test]
    fn test_io_error_mapping() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let rust_err = Error::Io(io_err);
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("IoError:"));
        assert!(napi_err.reason.contains("file not found"));
        assert!(napi_err.reason.contains("code=IO"));
        assert!(napi_err.reason.contains("category=System"));
        assert!(napi_err.reason.contains("isRecoverable=true"));
    }

    #[test]
    fn test_schema_error_metadata() {
        let rust_err = Error::Schema("table not found".to_string());
        let metadata = extract_metadata(&rust_err);

        assert_eq!(metadata.code, "SCHEMA");
        assert_eq!(metadata.category, "Schema");
        assert!(!metadata.is_recoverable);
        assert!(metadata.message.contains("SchemaError:"));
    }

    #[test]
    fn test_schema_error_mapping() {
        let rust_err = Error::Schema("table not found".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("SchemaError:"));
        assert!(napi_err.reason.contains("code=SCHEMA"));
        assert!(napi_err.reason.contains("category=Schema"));
        assert!(napi_err.reason.contains("isRecoverable=false"));
    }

    #[test]
    fn test_table_error_mapping() {
        let rust_err = Error::Table("invalid table".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("SchemaError:"));
        assert!(napi_err.reason.contains("code=SCHEMA"));
    }

    #[test]
    fn test_query_execution_error_mapping() {
        let rust_err = Error::QueryExecution("query failed".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("QueryError:"));
        assert!(napi_err.reason.contains("code=QUERY"));
        assert!(napi_err.reason.contains("isRecoverable=false"));
    }

    #[test]
    fn test_unsupported_query_error_mapping() {
        let rust_err = Error::UnsupportedQuery("UPDATE not supported".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("QueryError:"));
        assert!(napi_err.reason.contains("code=QUERY"));
    }

    #[test]
    fn test_cql_parse_error_mapping() {
        let rust_err = Error::CqlParse("syntax error at position 42".to_string());
        let napi_err = to_napi_error(rust_err);

        // CqlParse has Query category (not Data), so it gets QueryError prefix
        assert!(napi_err.reason.contains("QueryError:"));
        assert!(napi_err.reason.contains("syntax error"));
        assert!(napi_err.reason.contains("code=QUERY"));
        assert!(napi_err.reason.contains("category=Query"));
    }

    #[test]
    fn test_configuration_error_mapping() {
        let rust_err = Error::Configuration("invalid config".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("ValueError:"));
        assert!(napi_err.reason.contains("code=CONFIG"));
    }

    #[test]
    fn test_invalid_input_error_mapping() {
        let rust_err = Error::InvalidInput("bad input".to_string());
        let napi_err = to_napi_error(rust_err);

        // InvalidInput has Data category, which maps to ParseError prefix
        assert!(napi_err.reason.contains("ParseError:"));
        assert!(napi_err.reason.contains("code=PARSE"));
    }

    #[test]
    fn test_timeout_error_mapping() {
        let rust_err = Error::Timeout("operation timed out".to_string());
        let napi_err = to_napi_error(rust_err);

        // Timeout has System category
        assert!(napi_err.reason.contains("IoError:"));
        assert!(napi_err.reason.contains("code=IO"));
    }

    #[test]
    fn test_memory_error_mapping() {
        let rust_err = Error::Memory("out of memory".to_string());
        let napi_err = to_napi_error(rust_err);

        // Memory has System category
        assert!(napi_err.reason.contains("IoError:"));
        assert!(napi_err.reason.contains("code=IO"));
    }

    #[test]
    fn test_invalid_state_error_mapping() {
        let rust_err = Error::InvalidState("database closed".to_string());
        let napi_err = to_napi_error(rust_err);

        // InvalidState has Logic category
        assert!(napi_err.reason.contains("RuntimeError:"));
        assert!(napi_err.reason.contains("code=INVALID_INPUT"));
        assert!(napi_err.reason.contains("category=Logic"));
    }

    #[test]
    fn test_storage_error_mapping() {
        let rust_err = Error::Storage("storage error".to_string());
        let napi_err = to_napi_error(rust_err);

        // Storage category doesn't have a prefix
        assert!(napi_err.reason.contains("storage error"));
        assert!(napi_err.reason.contains("code=STORAGE"));
        assert!(napi_err.reason.contains("category=Storage"));
    }

    #[test]
    fn test_not_found_error_mapping() {
        let rust_err = Error::NotFound("resource not found".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("resource not found"));
        assert!(napi_err.reason.contains("code=NOT_FOUND"));
        assert!(napi_err.reason.contains("isRecoverable=false"));
    }

    #[test]
    fn test_other_errors_use_original_message() {
        let rust_err = Error::Corruption("data corrupted".to_string());
        let napi_err = to_napi_error(rust_err);

        // Corruption has Data category, which maps to ParseError
        assert!(napi_err.reason.contains("data corrupted"));
        assert!(napi_err.reason.contains("code=PARSE"));
    }

    #[test]
    fn test_runtime_init_error_maps_to_napi_error() {
        // A runtime-init failure must surface as a catchable napi::Error
        // (System/IO), never a process abort (issue #1438).
        let io_err =
            std::io::Error::other("cannot spawn worker threads: resource temporarily unavailable");
        let napi_err = runtime_init_error(io_err);

        assert!(napi_err.reason.contains("cannot spawn worker threads"));
        assert!(napi_err.reason.contains("code=IO"));
        assert!(napi_err.reason.contains("category=System"));
    }

    #[test]
    fn test_simple_error() {
        let napi_err = simple_error("Database is closed");

        assert!(napi_err.reason.contains("Database is closed"));
        assert!(napi_err.reason.contains("code=INVALID_INPUT"));
        assert!(napi_err.reason.contains("category=Logic"));
        assert!(napi_err.reason.contains("isRecoverable=false"));
    }

    /// Compile-time completeness check for error variant mapping.
    ///
    /// This test ensures all `cqlite_core::Error` variants are accounted for.
    /// If a new variant is added to the core Error enum, this will fail to compile.
    #[test]
    fn test_error_mapping_completeness() {
        fn verify_all_variants_documented(err: &Error) {
            match err {
                // Explicitly mapped variants with category
                Error::Io(_) => {
                    assert_eq!(err.category(), ErrorCategory::System);
                }
                Error::Schema(_) => {
                    assert_eq!(err.category(), ErrorCategory::Schema);
                }
                Error::Table(_) => {
                    assert_eq!(err.category(), ErrorCategory::Schema);
                }
                Error::QueryExecution(_) => {
                    assert_eq!(err.category(), ErrorCategory::Query);
                }
                Error::UnsupportedQuery(_) => {
                    assert_eq!(err.category(), ErrorCategory::Query);
                }
                Error::CqlParse(_) => {
                    // CqlParse is Query category in cqlite-core
                    assert_eq!(err.category(), ErrorCategory::Query);
                }
                Error::Configuration(_) => {
                    assert_eq!(err.category(), ErrorCategory::Configuration);
                }
                Error::InvalidInput(_) => {
                    assert_eq!(err.category(), ErrorCategory::Data);
                }
                Error::Timeout(_) => {
                    assert_eq!(err.category(), ErrorCategory::System);
                }
                Error::Memory(_) => {
                    assert_eq!(err.category(), ErrorCategory::System);
                }
                Error::InvalidState(_) => {
                    assert_eq!(err.category(), ErrorCategory::Logic);
                }

                // All other variants are handled by category
                Error::Serialization { .. } => {}
                Error::Corruption(_) => {}
                Error::InvalidFormat(_) => {}
                Error::UnsupportedFormat(_) => {}
                Error::UnsupportedVersion { .. } => {}
                Error::InvalidPath(_) => {}
                Error::TypeConversion(_) => {}
                Error::Storage(_) => {}
                Error::Concurrency(_) => {}
                Error::NotFound(_) => {}
                Error::AlreadyExists(_) => {}
                Error::InvalidOperation(_) => {}
                Error::ConstraintViolation(_) => {}
                Error::Transaction(_) => {}
                Error::Index(_) => {}
                Error::Compaction(_) => {}
                Error::Internal(_) => {}
                Error::Parse(_) => {}

                // Write-dir lock conflict — Concurrency category, maps to CONCURRENCY code
                Error::WriteDirLocked { .. } => {
                    assert_eq!(err.category(), ErrorCategory::Concurrency);
                }

                #[cfg(target_arch = "wasm32")]
                Error::Wasm(_) => {}
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
            // Also verify extract_metadata works
            let _ = extract_metadata(err);
        }
    }

    #[test]
    fn test_all_error_categories_have_codes() {
        // Verify every ErrorCategory maps to a code
        let categories = [
            ErrorCategory::System,
            ErrorCategory::Data,
            ErrorCategory::Schema,
            ErrorCategory::Query,
            ErrorCategory::Configuration,
            ErrorCategory::Storage,
            ErrorCategory::Concurrency,
            ErrorCategory::NotFound,
            ErrorCategory::Conflict,
            ErrorCategory::Logic,
            ErrorCategory::Constraint,
            ErrorCategory::Transaction,
            ErrorCategory::Platform,
            ErrorCategory::Internal,
        ];

        for category in categories {
            let code = category_to_code(category);
            assert!(!code.is_empty(), "Category {category:?} should have a code");
        }
    }
}
