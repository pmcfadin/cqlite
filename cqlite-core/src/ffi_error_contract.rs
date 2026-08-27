//! The shared FFI error contract (issue #1451).
//!
//! **This module is the single source of truth for how a core [`Error`] variant
//! surfaces in the language bindings.** It is consumed by
//! `bindings/python/src/error.rs` (`to_py_err`) and
//! `bindings/node/src/error.rs` (`extract_metadata`), which previously carried
//! two hand-maintained mappings keyed on *different* things — Python matched the
//! `Error` **variant**, Node derived its code from [`Error::category`] — so the
//! same core error had a different identity in each binding (`CqlParse` was
//! `ParseError` in Python but code `QUERY` in Node; `Timeout`/`Memory` collapsed
//! into `IO`). Both bindings now read the rows below **by variant**.
//!
//! # Relocation
//!
//! The contract lives in `cqlite-core` only because the shared FFI crate does
//! not exist yet. When `cqlite-ffi-common` (issue #1452) lands, this module
//! moves there verbatim and the bindings re-point their import; nothing else
//! changes. It is deliberately a top-level module (a sibling of [`crate::error`])
//! so that move is a file move.
//!
//! # No binding dependency may enter this module
//!
//! The rows are **inert data**. The Python class is carried as an *identifier*
//! ([`PyExceptionClass`]), never a PyO3 type, and the Node code as a plain
//! `&'static str`; `cqlite-core` therefore gains no `pyo3`/`napi` dependency.
//!
//! # Column meanings
//!
//! | Column | Meaning |
//! |--------|---------|
//! | `py_class` | Python exception class the variant is raised as |
//! | `node_code` | JS `error.code` string |
//! | `category` | JS `error.category` (mirrors [`Error::category`]) |
//! | `recoverable` | JS `error.isRecoverable` (mirrors [`Error::is_recoverable`]) |
//! | `message_prefix` | prefix prepended to the Node message (`"IoError: …"`) |
//!
//! `category`/`recoverable` are carried in the row so a binding needs exactly
//! ONE lookup, and a unit test pins them against [`Error::category`] /
//! [`Error::is_recoverable`] so the copy can never drift. Core's `category()`
//! and `is_recoverable()` are untouched by this module and keep their own
//! meaning for their own callers.
//!
//! # Adding an `Error` variant
//!
//! [`variant_of`] is an exhaustive match over [`Error`], so a new core variant
//! **fails to compile** until it is mapped to an [`FfiErrorVariant`]; adding a
//! new `FfiErrorVariant` requires a row in the table below (the macro generates
//! the exhaustive `row()` match from it).

use crate::error::{Error, ErrorCategory};

/// Identifier of the Python exception class a core [`Error`] maps to.
///
/// An *identifier*, not a PyO3 type: `cqlite-core` must not depend on `pyo3`.
/// The Python binding matches on this enum to pick the concrete class, so a new
/// member fails that binding's build until it is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyExceptionClass {
    /// Python builtin `IOError`/`OSError`.
    IoError,
    /// Python builtin `ValueError`.
    ValueError,
    /// Python builtin `TimeoutError`.
    TimeoutError,
    /// Python builtin `MemoryError`.
    MemoryError,
    /// Python builtin `RuntimeError`.
    RuntimeError,
    /// `cqlite.CqliteError` — the base class of the binding's own hierarchy.
    CqliteError,
    /// `cqlite.SchemaError` (subclass of `CqliteError`).
    SchemaError,
    /// `cqlite.QueryError` (subclass of `CqliteError`).
    QueryError,
    /// `cqlite.ParseError` (subclass of `CqliteError`).
    ParseError,
    /// `cqlite.CancelledError` (subclass of `CqliteError`, issue #2264).
    CancelledError,
}

impl PyExceptionClass {
    /// The class name as Python sees it (`"IOError"`, `"ParseError"`, …).
    pub const fn as_str(self) -> &'static str {
        match self {
            PyExceptionClass::IoError => "IOError",
            PyExceptionClass::ValueError => "ValueError",
            PyExceptionClass::TimeoutError => "TimeoutError",
            PyExceptionClass::MemoryError => "MemoryError",
            PyExceptionClass::RuntimeError => "RuntimeError",
            PyExceptionClass::CqliteError => "CqliteError",
            PyExceptionClass::SchemaError => "SchemaError",
            PyExceptionClass::QueryError => "QueryError",
            PyExceptionClass::ParseError => "ParseError",
            PyExceptionClass::CancelledError => "CancelledError",
        }
    }
}

/// One row of the shared FFI error contract: the complete binding identity of a
/// single core [`Error`] variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FfiErrorRow {
    /// The core [`Error`] variant's identifier (e.g. `"CqlParse"`).
    pub variant: &'static str,
    /// Python exception class to raise.
    pub py_class: PyExceptionClass,
    /// JavaScript `error.code`.
    pub node_code: &'static str,
    /// JavaScript `error.category`.
    pub category: ErrorCategory,
    /// JavaScript `error.isRecoverable`.
    pub recoverable: bool,
    /// Prefix prepended to the Node message, if any.
    pub message_prefix: Option<&'static str>,
}

/// Declare the contract table ONCE, generating the variant enum, the full-row
/// lookup, the name lookup and the enumeration of every row from it.
macro_rules! ffi_error_contract_table {
    ($(
        $variant:ident => {
            py: $py:ident,
            code: $code:literal,
            category: $cat:ident,
            recoverable: $rec:literal,
            prefix: $prefix:expr,
        }
    ),+ $(,)?) => {
        /// One member per core [`Error`] variant, named identically to it.
        ///
        /// This is the contract's key. Errors are mapped to it by [`variant_of`],
        /// which is exhaustive over [`Error`].
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum FfiErrorVariant {
            $(
                #[doc = concat!("Contract row for [`Error::", stringify!($variant), "`].")]
                $variant,
            )+
        }

        impl FfiErrorVariant {
            /// Every contract row's key, in table order.
            pub const ALL: &'static [FfiErrorVariant] = &[$(FfiErrorVariant::$variant),+];

            /// The contract row for this variant.
            pub const fn row(self) -> FfiErrorRow {
                match self {
                    $(
                        FfiErrorVariant::$variant => FfiErrorRow {
                            variant: stringify!($variant),
                            py_class: PyExceptionClass::$py,
                            node_code: $code,
                            category: ErrorCategory::$cat,
                            recoverable: $rec,
                            message_prefix: $prefix,
                        },
                    )+
                }
            }

            /// Resolve a variant by its core [`Error`] identifier.
            ///
            /// Returns `None` for an unknown name (fail-closed: a caller must
            /// never silently fall back to some default row).
            pub fn from_name(name: &str) -> Option<Self> {
                match name {
                    $(stringify!($variant) => Some(FfiErrorVariant::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

// ============================================================================
// THE TABLE — the single source of truth for both bindings.
//
// `code`/`prefix` changes marked (#1451) are the cross-binding divergences this
// issue fixes: before it, Node derived these from `category()`, so `CqlParse`
// reported `QUERY`, `Timeout`/`Memory` both reported `IO`, and `InvalidInput`
// took the `PARSE` code that belongs to a genuine CQL parse failure.
// ============================================================================
ffi_error_contract_table! {
    Io => { py: IoError, code: "IO", category: System, recoverable: true, prefix: Some("IoError"), },
    Serialization => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    Corruption => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    Schema => { py: SchemaError, code: "SCHEMA", category: Schema, recoverable: false, prefix: Some("SchemaError"), },
    // (#1451) real PARSE: a CQL syntax failure, not the generic QUERY bucket.
    CqlParse => { py: ParseError, code: "PARSE", category: Query, recoverable: false, prefix: Some("ParseError"), },
    InvalidFormat => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    UnsupportedFormat => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    UnsupportedVersion => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    UnsupportedCommitLogVersion => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    CorruptCommitLogFrame => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    // (#1451) dedicated TIMEOUT code: a deadline is not an I/O failure.
    Timeout => { py: TimeoutError, code: "TIMEOUT", category: System, recoverable: false, prefix: Some("TimeoutError"), },
    InvalidPath => { py: CqliteError, code: "IO", category: System, recoverable: false, prefix: Some("IoError"), },
    InvalidState => { py: RuntimeError, code: "INVALID_INPUT", category: Logic, recoverable: false, prefix: Some("RuntimeError"), },
    QueryExecution => { py: QueryError, code: "QUERY", category: Query, recoverable: false, prefix: Some("QueryError"), },
    ResultTooLarge => { py: QueryError, code: "QUERY", category: Query, recoverable: false, prefix: Some("QueryError"), },
    InvalidReadPath => { py: CqliteError, code: "CONFIG", category: Configuration, recoverable: false, prefix: Some("ValueError"), },
    ForcedReadPathUnavailable => { py: CqliteError, code: "QUERY", category: Query, recoverable: false, prefix: Some("QueryError"), },
    TypeConversion => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    Configuration => { py: ValueError, code: "CONFIG", category: Configuration, recoverable: false, prefix: Some("ValueError"), },
    Storage => { py: CqliteError, code: "STORAGE", category: Storage, recoverable: true, prefix: None, },
    // (#1451) dedicated MEMORY code: an allocation failure is not an I/O failure.
    Memory => { py: MemoryError, code: "MEMORY", category: System, recoverable: true, prefix: Some("MemoryError"), },
    Concurrency => { py: CqliteError, code: "CONCURRENCY", category: Concurrency, recoverable: true, prefix: None, },
    WriteDirLocked => { py: CqliteError, code: "CONCURRENCY", category: Concurrency, recoverable: false, prefix: None, },
    NotFound => { py: CqliteError, code: "NOT_FOUND", category: NotFound, recoverable: false, prefix: None, },
    Table => { py: SchemaError, code: "SCHEMA", category: Schema, recoverable: false, prefix: Some("SchemaError"), },
    AlreadyExists => { py: CqliteError, code: "CONFLICT", category: Conflict, recoverable: false, prefix: None, },
    InvalidOperation => { py: CqliteError, code: "INVALID_INPUT", category: Logic, recoverable: false, prefix: Some("RuntimeError"), },
    ConstraintViolation => { py: CqliteError, code: "CONSTRAINT", category: Constraint, recoverable: false, prefix: None, },
    Transaction => { py: CqliteError, code: "TRANSACTION", category: Transaction, recoverable: true, prefix: None, },
    Index => { py: CqliteError, code: "STORAGE", category: Storage, recoverable: true, prefix: None, },
    Compaction => { py: CqliteError, code: "STORAGE", category: Storage, recoverable: true, prefix: None, },
    Wasm => { py: CqliteError, code: "PLATFORM", category: Platform, recoverable: false, prefix: None, },
    Internal => { py: CqliteError, code: "INTERNAL", category: Internal, recoverable: false, prefix: None, },
    Parse => { py: CqliteError, code: "PARSE", category: Data, recoverable: false, prefix: Some("ParseError"), },
    // (#1451) INVALID_INPUT, not PARSE: bad caller input is not a CQL parse failure.
    InvalidInput => { py: ValueError, code: "INVALID_INPUT", category: Data, recoverable: false, prefix: Some("ValueError"), },
    UnsupportedQuery => { py: QueryError, code: "QUERY", category: Query, recoverable: false, prefix: Some("QueryError"), },
    Cancelled => { py: CancelledError, code: "CANCELLED", category: Cancelled, recoverable: false, prefix: Some("CancelledError"), },
}

/// Map a core [`Error`] to its contract key.
///
/// **This match is the contract's exhaustiveness guard**: it is exhaustive over
/// [`Error`], so adding a variant to the core enum fails to compile until the
/// variant is given an [`FfiErrorVariant`] (and therefore a table row).
pub fn variant_of(err: &Error) -> FfiErrorVariant {
    match err {
        Error::Io(_) => FfiErrorVariant::Io,
        Error::Serialization { .. } => FfiErrorVariant::Serialization,
        Error::Corruption(_) => FfiErrorVariant::Corruption,
        Error::Schema(_) => FfiErrorVariant::Schema,
        Error::CqlParse(_) => FfiErrorVariant::CqlParse,
        Error::InvalidFormat(_) => FfiErrorVariant::InvalidFormat,
        Error::UnsupportedFormat(_) => FfiErrorVariant::UnsupportedFormat,
        Error::UnsupportedVersion { .. } => FfiErrorVariant::UnsupportedVersion,
        Error::UnsupportedCommitLogVersion { .. } => FfiErrorVariant::UnsupportedCommitLogVersion,
        Error::CorruptCommitLogFrame(_) => FfiErrorVariant::CorruptCommitLogFrame,
        Error::Timeout(_) => FfiErrorVariant::Timeout,
        Error::InvalidPath(_) => FfiErrorVariant::InvalidPath,
        Error::InvalidState(_) => FfiErrorVariant::InvalidState,
        Error::QueryExecution(_) => FfiErrorVariant::QueryExecution,
        Error::ResultTooLarge { .. } => FfiErrorVariant::ResultTooLarge,
        Error::InvalidReadPath { .. } => FfiErrorVariant::InvalidReadPath,
        Error::ForcedReadPathUnavailable { .. } => FfiErrorVariant::ForcedReadPathUnavailable,
        Error::TypeConversion(_) => FfiErrorVariant::TypeConversion,
        Error::Configuration(_) => FfiErrorVariant::Configuration,
        Error::Storage(_) => FfiErrorVariant::Storage,
        Error::Memory(_) => FfiErrorVariant::Memory,
        Error::Concurrency(_) => FfiErrorVariant::Concurrency,
        Error::WriteDirLocked { .. } => FfiErrorVariant::WriteDirLocked,
        Error::NotFound(_) => FfiErrorVariant::NotFound,
        Error::Table(_) => FfiErrorVariant::Table,
        Error::AlreadyExists(_) => FfiErrorVariant::AlreadyExists,
        Error::InvalidOperation(_) => FfiErrorVariant::InvalidOperation,
        Error::ConstraintViolation(_) => FfiErrorVariant::ConstraintViolation,
        Error::Transaction(_) => FfiErrorVariant::Transaction,
        Error::Index(_) => FfiErrorVariant::Index,
        Error::Compaction(_) => FfiErrorVariant::Compaction,
        #[cfg(target_arch = "wasm32")]
        Error::Wasm(_) => FfiErrorVariant::Wasm,
        Error::Internal(_) => FfiErrorVariant::Internal,
        Error::Parse(_) => FfiErrorVariant::Parse,
        Error::InvalidInput(_) => FfiErrorVariant::InvalidInput,
        Error::UnsupportedQuery(_) => FfiErrorVariant::UnsupportedQuery,
        Error::Cancelled => FfiErrorVariant::Cancelled,
    }
}

/// The contract row for a core [`Error`] — the ONE lookup a binding performs.
pub fn contract_for(err: &Error) -> FfiErrorRow {
    variant_of(err).row()
}

impl FfiErrorVariant {
    /// A representative [`Error`] value for this contract row.
    ///
    /// Used by the bindings' error-contract conformance probes (and this
    /// module's tests) to exercise the real mapping path for a variant that no
    /// test query can provoke (`Timeout`, `Memory`, …). Returns `None` only for
    /// a variant that does not exist on this build target (`Wasm` off wasm32),
    /// never as a silent fallback.
    pub fn sample_error(self) -> Option<Error> {
        let sample = match self {
            FfiErrorVariant::Io => Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "sample io failure",
            )),
            FfiErrorVariant::Serialization => Error::serialization("sample serialization failure"),
            FfiErrorVariant::Corruption => Error::corruption("sample corruption"),
            FfiErrorVariant::Schema => Error::schema("sample schema failure"),
            FfiErrorVariant::CqlParse => Error::cql_parse("sample CQL syntax failure"),
            FfiErrorVariant::InvalidFormat => Error::invalid_format("sample invalid format"),
            FfiErrorVariant::UnsupportedFormat => {
                Error::unsupported_format("sample unsupported format")
            }
            FfiErrorVariant::UnsupportedVersion => Error::UnsupportedVersion {
                version: "ma".to_string(),
                floor: "na".to_string(),
            },
            FfiErrorVariant::UnsupportedCommitLogVersion => Error::UnsupportedCommitLogVersion {
                version: 5,
                floor: 6,
                ceiling: 8,
            },
            FfiErrorVariant::CorruptCommitLogFrame => {
                Error::CorruptCommitLogFrame("sample corrupt frame".to_string())
            }
            FfiErrorVariant::Timeout => Error::Timeout("sample operation timeout".to_string()),
            FfiErrorVariant::InvalidPath => Error::invalid_path("sample invalid path"),
            FfiErrorVariant::InvalidState => Error::invalid_state("sample invalid state"),
            FfiErrorVariant::QueryExecution => Error::query_execution("sample query failure"),
            FfiErrorVariant::ResultTooLarge => Error::ResultTooLarge {
                budget_bytes: 1,
                estimated_bytes: 2,
                rows: 3,
            },
            FfiErrorVariant::InvalidReadPath => Error::invalid_read_path("sample"),
            FfiErrorVariant::ForcedReadPathUnavailable => {
                Error::forced_read_path_unavailable("point", "sample reason")
            }
            FfiErrorVariant::TypeConversion => Error::type_conversion("sample type conversion"),
            FfiErrorVariant::Configuration => Error::configuration("sample configuration failure"),
            FfiErrorVariant::Storage => Error::storage("sample storage failure"),
            FfiErrorVariant::Memory => Error::memory("sample allocation failure"),
            FfiErrorVariant::Concurrency => Error::concurrency("sample concurrency failure"),
            FfiErrorVariant::WriteDirLocked => Error::write_dir_locked("/sample/write/dir"),
            FfiErrorVariant::NotFound => Error::not_found("sample missing resource"),
            FfiErrorVariant::Table => Error::Table("sample table failure".to_string()),
            FfiErrorVariant::AlreadyExists => Error::already_exists("sample existing resource"),
            FfiErrorVariant::InvalidOperation => {
                Error::invalid_operation("sample invalid operation")
            }
            FfiErrorVariant::ConstraintViolation => {
                Error::constraint_violation("sample constraint violation")
            }
            FfiErrorVariant::Transaction => Error::transaction("sample transaction failure"),
            FfiErrorVariant::Index => Error::index("sample index failure"),
            FfiErrorVariant::Compaction => Error::compaction("sample compaction failure"),
            #[cfg(target_arch = "wasm32")]
            FfiErrorVariant::Wasm => Error::wasm("sample wasm failure"),
            // `Error::Wasm` does not exist off wasm32, so no sample can be
            // constructed. Reported as `None`, never substituted.
            #[cfg(not(target_arch = "wasm32"))]
            FfiErrorVariant::Wasm => return None,
            FfiErrorVariant::Internal => Error::internal("sample internal failure"),
            FfiErrorVariant::Parse => Error::parse("sample parse failure"),
            FfiErrorVariant::InvalidInput => Error::invalid_input("sample invalid input"),
            FfiErrorVariant::UnsupportedQuery => Error::unsupported_query("sample unsupported query"),
            FfiErrorVariant::Cancelled => Error::Cancelled,
        };
        Some(sample)
    }
}
