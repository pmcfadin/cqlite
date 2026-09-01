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
//! # Error Code Mapping — the shared FFI error contract (issue #1451)
//!
//! `code`/`category`/`isRecoverable`/prefix all come from
//! `cqlite_ffi_common::error_contract`, the ONE authoritative table that
//! `bindings/python` reads too, keyed **by `Error` variant**. This binding used
//! to derive its code from `Error::category()`, which made the same core error
//! a different thing in each binding: `CqlParse` reported `QUERY` here while
//! Python raised `ParseError`, and `Timeout`/`Memory` both collapsed into `IO`
//! while Python raised `TimeoutError`/`MemoryError`.
//!
//! | Rust variant | JS code | JS message prefix |
//! |--------------|---------|-------------------|
//! | `Io`, `InvalidPath` | `IO` | `IoError:` |
//! | `Schema`, `Table` | `SCHEMA` | `SchemaError:` |
//! | `QueryExecution`, `UnsupportedQuery`, `ResultTooLarge`, `ForcedReadPathUnavailable` | `QUERY` | `QueryError:` |
//! | `CqlParse`, `Corruption`, `Serialization`, `Parse`, `TypeConversion`, `InvalidFormat`, `UnsupportedFormat`, `UnsupportedVersion`, `UnsupportedCommitLogVersion`, `CorruptCommitLogFrame` | `PARSE` | `ParseError:` |
//! | `Configuration`, `InvalidReadPath` | `CONFIG` | `ValueError:` |
//! | `InvalidInput` | `INVALID_INPUT` | `ValueError:` |
//! | `InvalidState`, `InvalidOperation` | `INVALID_INPUT` | `RuntimeError:` |
//! | `Timeout` | `TIMEOUT` | `TimeoutError:` |
//! | `Memory` | `MEMORY` | `MemoryError:` |
//! | `Storage`, `Index`, `Compaction` | `STORAGE` | (original) |
//! | `NotFound` | `NOT_FOUND` | (original) |
//! | `Concurrency`, `WriteDirLocked` | `CONCURRENCY` | (original) |
//! | `AlreadyExists` | `CONFLICT` | (original) |
//! | `ConstraintViolation` | `CONSTRAINT` | (original) |
//! | `Transaction` | `TRANSACTION` | (original) |
//! | `Wasm` | `PLATFORM` | (original) |
//! | `Internal` | `INTERNAL` | (original) |
//! | `Cancelled` | `CANCELLED` | `CancelledError:` (issue #2264 — never `IO`) |
//!
//! # Example
//!
//! The statement below is malformed *in the CQL grammar* (a `SELECT` with no
//! table), so it reaches the parser and fails there — that is what `PARSE`
//! means. A statement whose leading token is not a known verb (`"INVALID SQL"`)
//! never reaches the parser: it is rejected earlier as `Error::QueryExecution`
//! and correctly reports `QUERY`, not `PARSE`.
//!
//! ```javascript
//! try {
//!   await db.execute("SELECT * FROM");
//! } catch (e) {
//!   console.log(e.code);          // "PARSE" (a CQL syntax failure)
//!   console.log(e.category);      // "Query"
//!   console.log(e.isRecoverable); // false
//!   if (e.code === "PARSE") {
//!     console.log("CQL syntax error");
//!   }
//! }
//! ```

use cqlite_core::Error;
use cqlite_ffi_common::error_contract::contract_for;

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

/// Extract error metadata from a `cqlite_core::Error`.
///
/// Every field comes from the error's row in the shared FFI error contract
/// (`cqlite_ffi_common::error_contract`), looked up **by variant** — never
/// re-derived from `Error::category()`, which cannot distinguish `CqlParse`
/// from a generic query failure or a `Timeout` from an I/O failure (issue
/// #1451). `bindings/python` reads the same row, so the two bindings cannot
/// drift apart; to change how a variant surfaces, edit the table.
pub fn extract_metadata(err: &Error) -> ErrorMetadata {
    let row = contract_for(err);
    let original_message = err.to_string();

    // Format message with the row's prefix if it has one.
    let message = match row.message_prefix {
        Some(prefix) => format!("{prefix}: {original_message}"),
        None => original_message,
    };

    ErrorMetadata {
        code: row.node_code,
        category: row.category.to_string(),
        is_recoverable: row.recoverable,
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

/// Run `f` on a napi async-worker thread and convert ANY panic it raises into a
/// typed `napi::Error` rather than letting it abort the host process (issue
/// #1754).
///
/// A `napi::Task::compute` body runs on a libuv threadpool thread that has NO
/// unwind boundary above it. Even under the binding cdylib's `panic=unwind`
/// profile (issue #1440), a panic there cannot unwind across the FFI frame — the
/// Rust runtime instead calls `abort()` (`fatal runtime error: failed to
/// initiate panic, error 5, aborting`), killing the whole Node process. Wrapping
/// the worker body in [`std::panic::catch_unwind`] catches the panic ON the
/// worker thread (before it reaches the un-unwindable FFI frame) and turns it
/// into a rejected promise / thrown JS `Error`, preserving the abort-safety
/// guarantee (#1431/#1440) for the raw-parse path.
///
/// The closure is wrapped in [`AssertUnwindSafe`] because the task state it
/// borrows is not used again after a panic (the panic aborts the compute and the
/// error is returned), so the standard unwind-safety concern (observing a
/// broken invariant post-panic) does not apply here.
pub fn catch_unwind_to_napi<T>(what: &str, f: impl FnOnce() -> napi::Result<T>) -> napi::Result<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => Err(to_napi_error(Error::corruption(format!(
            "{what} panicked while decoding a corrupt SSTable: {} — recovered at the \
             napi boundary instead of aborting the process (issue #1754)",
            panic_message(&payload)
        )))),
    }
}

/// Best-effort extraction of a human-readable message from a caught panic
/// payload (`&str` or `String`), falling back to a generic label.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_ffi_common::error_contract::FfiErrorVariant;

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

    /// Issue #1451: a CQL syntax failure is `PARSE`, not the generic `QUERY`
    /// bucket its Query category used to put it in — matching the Python
    /// binding, which has always raised `ParseError` for this variant.
    #[test]
    fn test_cql_parse_error_mapping() {
        let rust_err = Error::CqlParse("syntax error at position 42".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("ParseError:"));
        assert!(napi_err.reason.contains("syntax error"));
        assert!(napi_err.reason.contains("code=PARSE"));
        assert!(napi_err.reason.contains("category=Query"));
        assert!(!napi_err.reason.contains("code=QUERY"));
    }

    #[test]
    fn test_configuration_error_mapping() {
        let rust_err = Error::Configuration("invalid config".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("ValueError:"));
        assert!(napi_err.reason.contains("code=CONFIG"));
    }

    /// Issue #1451: bad caller input is `INVALID_INPUT`, not the `PARSE` code
    /// that belongs to a genuine CQL parse failure (Python raises `ValueError`
    /// for this variant, so the prefix says `ValueError:` too).
    #[test]
    fn test_invalid_input_error_mapping() {
        let rust_err = Error::InvalidInput("bad input".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("ValueError:"));
        assert!(napi_err.reason.contains("code=INVALID_INPUT"));
        assert!(!napi_err.reason.contains("code=PARSE"));
    }

    /// Issue #1451: a deadline gets its OWN code. It used to collapse into `IO`
    /// via the System category, while Python raised `TimeoutError`.
    #[test]
    fn test_timeout_error_mapping() {
        let rust_err = Error::Timeout("operation timed out".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("TimeoutError:"));
        assert!(napi_err.reason.contains("code=TIMEOUT"));
        assert!(napi_err.reason.contains("category=System"));
        assert!(!napi_err.reason.contains("code=IO"));
    }

    /// Issue #1451: an allocation failure gets its OWN code. It used to collapse
    /// into `IO` via the System category, while Python raised `MemoryError`.
    #[test]
    fn test_memory_error_mapping() {
        let rust_err = Error::Memory("out of memory".to_string());
        let napi_err = to_napi_error(rust_err);

        assert!(napi_err.reason.contains("MemoryError:"));
        assert!(napi_err.reason.contains("code=MEMORY"));
        assert!(napi_err.reason.contains("category=System"));
        assert!(!napi_err.reason.contains("code=IO"));
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

        // Corruption keeps the Data-shaped `PARSE` code and `ParseError:` prefix
        // (issue #1451 pins this row: Python has no closer class than the base
        // CqliteError, and Node has no closer code than PARSE).
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

    /// The JS code each core variant is EXPECTED to surface as — a hand-written
    /// restatement of the shared contract's `node_code` column.
    ///
    /// Two guards in one:
    ///
    /// 1. **Compile-time completeness.** The match is exhaustive over
    ///    `cqlite_core::Error`, so adding a variant to the core enum fails to
    ///    compile here until the JS identity is reviewed.
    /// 2. **Content.** `test_error_mapping_completeness` asserts the shared
    ///    table (and the metadata `extract_metadata` actually emits) agrees with
    ///    this independent statement, so an accidental edit to the table's
    ///    `node_code` column fails HERE instead of reaching users.
    ///
    /// Note the codes are decided BY VARIANT (issue #1451), which is why several
    /// variants sharing one `ErrorCategory` no longer have to share one code:
    /// `Timeout`/`Memory`/`Io` are all `System`, and `CqlParse`/`QueryExecution`
    /// are both `Query`.
    fn expected_node_code(err: &Error) -> &'static str {
        match err {
            Error::Io(_) | Error::InvalidPath(_) => "IO",
            Error::Schema(_) | Error::Table(_) => "SCHEMA",
            Error::QueryExecution(_)
            | Error::UnsupportedQuery(_)
            // Byte-bounded result budget (issue #1582) and the forced-read-path
            // fail-closed error (issue #1918) are both query-shaped.
            | Error::ResultTooLarge { .. }
            | Error::ForcedReadPathUnavailable { .. } => "QUERY",
            // A real CQL parse failure — and the data-shaped errors that share
            // the parse identity because JS has no closer code for them.
            Error::CqlParse(_)
            | Error::Corruption(_)
            // Issue #3723: a wrong on-disk fixed-width length is corrupt DATA and
            // takes `Corruption`'s identity, not a query or schema code.
            | Error::FixedWidthLengthMismatch { .. }
            | Error::Serialization { .. }
            | Error::Parse(_)
            | Error::TypeConversion(_)
            | Error::InvalidFormat(_)
            | Error::UnsupportedFormat(_)
            | Error::UnsupportedVersion { .. }
            // CommitLog reader (#2389) — not bound yet (v1 is library+CLI only).
            | Error::UnsupportedCommitLogVersion { .. }
            | Error::CorruptCommitLogFrame(_) => "PARSE",
            // Query execution budget elapsed (issue #1695): the SAME `TIMEOUT` code as
            // its sibling `Timeout`, so a JS caller checking for `TIMEOUT` catches
            // both. Deliberately NOT `QUERY`, even though its `ErrorCategory` IS
            // `Query` — #1451's whole point is that codes are decided BY VARIANT, so
            // the classify category and the JS code are separate axes. Python maps the
            // same variant to the builtin `TimeoutError`, which is the cross-binding
            // agreement the shared table exists to enforce.
            Error::QueryTimeout { .. } => "TIMEOUT",
            Error::Configuration(_) | Error::InvalidReadPath { .. } => "CONFIG",
            Error::InvalidInput(_) | Error::InvalidState(_) | Error::InvalidOperation(_) => {
                "INVALID_INPUT"
            }
            Error::Timeout(_) => "TIMEOUT",
            Error::Memory(_) => "MEMORY",
            Error::Storage(_) | Error::Index(_) | Error::Compaction(_) => "STORAGE",
            Error::NotFound(_) => "NOT_FOUND",
            Error::Concurrency(_) | Error::WriteDirLocked { .. } => "CONCURRENCY",
            Error::AlreadyExists(_) => "CONFLICT",
            Error::ConstraintViolation(_) => "CONSTRAINT",
            Error::Transaction(_) => "TRANSACTION",
            Error::Internal(_) => "INTERNAL",
            // Issue #2264: a cooperative cancellation gets its OWN code — never
            // the misleading "IO" of a genuine transport/filesystem failure.
            Error::Cancelled => "CANCELLED",

            #[cfg(target_arch = "wasm32")]
            Error::Wasm(_) => "PLATFORM",
        }
    }

    /// Every core variant emits the documented code — in the shared contract AND
    /// in the metadata this binding actually attaches to the JS error.
    #[test]
    fn test_error_mapping_completeness() {
        let mut checked = 0usize;
        for &variant in FfiErrorVariant::ALL {
            let Some(err) = variant.sample_error() else {
                // Only `Wasm` lacks a representative value off wasm32.
                assert_eq!(variant, FfiErrorVariant::Wasm);
                continue;
            };
            let expected = expected_node_code(&err);
            let row = contract_for(&err);
            assert_eq!(
                row.node_code, expected,
                "shared contract row {} emits {}, this binding documents {}",
                row.variant, row.node_code, expected
            );

            let metadata = extract_metadata(&err);
            assert_eq!(metadata.code, expected, "metadata code for {}", row.variant);
            assert_eq!(
                metadata.category,
                err.category().to_string(),
                "metadata category for {}",
                row.variant
            );
            assert_eq!(
                metadata.is_recoverable,
                err.is_recoverable(),
                "metadata isRecoverable for {}",
                row.variant
            );
            // The ORIGINAL core message always survives, prefix or not.
            assert!(
                metadata.message.contains(&err.to_string()),
                "metadata message for {} must contain the core message",
                row.variant
            );
            checked += 1;
        }
        let expected_checked =
            FfiErrorVariant::ALL.len() - if cfg!(target_arch = "wasm32") { 0 } else { 1 };
        assert_eq!(
            checked, expected_checked,
            "every contract row except Wasm (off wasm32) must be exercised"
        );
    }

    /// Every row emits a non-empty code, and the metadata encoding the JS
    /// wrapper parses survives for every one of them.
    #[test]
    fn test_every_row_encodes_parseable_metadata() {
        for &variant in FfiErrorVariant::ALL {
            let Some(err) = variant.sample_error() else {
                continue;
            };
            let row = contract_for(&err);
            assert!(!row.node_code.is_empty());
            let napi_err = to_napi_error(err);
            assert!(
                napi_err
                    .reason
                    .contains(&format!("\0code={}", row.node_code)),
                "row {} must encode its code for the JS wrapper",
                row.variant
            );
            assert!(napi_err.reason.contains("\0category="));
            assert!(napi_err.reason.contains("\0isRecoverable="));
        }
    }

    /// The four cross-binding divergences issue #1451 fixes, pinned as a table.
    #[test]
    fn test_pinned_contract_rows() {
        let cases = [
            (Error::cql_parse("bad syntax"), "PARSE", Some("ParseError")),
            (
                Error::invalid_input("bad argument"),
                "INVALID_INPUT",
                Some("ValueError"),
            ),
            (
                Error::Timeout("deadline exceeded".to_string()),
                "TIMEOUT",
                Some("TimeoutError"),
            ),
            (
                Error::memory("allocation failed"),
                "MEMORY",
                Some("MemoryError"),
            ),
            (Error::corruption("torn page"), "PARSE", Some("ParseError")),
        ];

        for (err, code, prefix) in cases {
            let row = contract_for(&err);
            assert_eq!(row.node_code, code, "node_code for {}", row.variant);
            assert_eq!(row.message_prefix, prefix, "prefix for {}", row.variant);
            let metadata = extract_metadata(&err);
            assert_eq!(metadata.code, code);
        }
    }
}
