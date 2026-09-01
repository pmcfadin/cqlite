//! Error types for CQLite

use std::fmt;
use thiserror::Error;

/// Result type alias for CQLite operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for CQLite operations
#[derive(Error, Debug)]
pub enum Error {
    /// I/O related errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization/deserialization errors
    #[error("Serialization error: {message}")]
    Serialization {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Data corruption errors
    #[error("Data corruption: {0}")]
    Corruption(String),

    /// Schema validation errors
    #[error("Schema error: {0}")]
    Schema(String),

    /// CQL parsing errors
    #[error("CQL parse error: {0}")]
    CqlParse(String),

    /// Invalid format error (for SSTable parsing)
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// Unsupported format error
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    /// SSTable version below the supported Cassandra 5.0 floor.
    ///
    /// CQLite targets Cassandra 5.0 (`na`+/`nb` BIG, `oa`/`da` BTI). A
    /// pre-`na` (`ma`–`me`, Cassandra 3.x) BIG version, or a non-`da` BTI
    /// version, is out of scope and rejected at version-parse time.
    #[error("Unsupported SSTable version {version:?}: below supported floor {floor:?}")]
    UnsupportedVersion { version: String, floor: String },

    /// Cassandra CommitLog segment descriptor version outside the supported
    /// Cassandra 5.0-era range (issue #2389).
    ///
    /// The version is read authoritatively from the `CommitLogDescriptor`
    /// header, never inferred from the filename or file size (no-heuristics).
    /// A below/above-range version is rejected before the mutation stream is
    /// touched, mirroring `BigVersionGates`/`BtiVersionGates`.
    #[error("Unsupported CommitLog version {version}: supported range is {floor}..={ceiling}")]
    UnsupportedCommitLogVersion {
        version: i32,
        floor: i32,
        ceiling: i32,
    },

    /// A Cassandra CommitLog per-record frame failed structural validation:
    /// a CRC mismatch (header or payload), an implausible record length, or a
    /// corrupt sync marker (issue #2389).
    ///
    /// Distinct from a *torn tail* (clean truncation), which is reported to the
    /// caller as end-of-segment rather than as an error.
    #[error("Corrupt CommitLog frame: {0}")]
    CorruptCommitLogFrame(String),

    /// Timeout error
    #[error("Operation timeout: {0}")]
    Timeout(String),

    /// A query exceeded the configured `query.max_execution_time` budget
    /// (issue #1695).
    ///
    /// Raised by the SINGLE timeout wrapper at the query-engine chokepoint (see
    /// `crate::query::engine::deadline`), never by an ad-hoc clock check inside a
    /// scan loop. Deliberately a variant of its own — distinct from
    /// [`Error::Timeout`] (an I/O-level timeout) and from every corruption
    /// variant — so an operator-imposed budget can never be mistaken for damaged
    /// data: it classifies as its own bounded telemetry category
    /// (`crate::observability::ObsErrorCategory::Timeout`).
    #[error(
        "query exceeded the configured query.max_execution_time budget of {limit:?} \
         (elapsed {elapsed:?}) at {operation}; raise query.max_execution_time \
         (CLI: performance.query_timeout_ms), narrow the query with a \
         partition-key WHERE or a LIMIT, or set it to 0 for no timeout"
    )]
    QueryTimeout {
        /// The bounded entry point that elapsed (e.g. `query.execute`).
        operation: String,
        /// Time actually spent before the budget was abandoned.
        elapsed: std::time::Duration,
        /// The configured budget (`query.max_execution_time`).
        limit: std::time::Duration,
    },

    /// Invalid path error
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    /// Invalid state error
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Query execution errors
    #[error("Query execution error: {0}")]
    QueryExecution(String),

    /// A materialized result set exceeded the configured byte budget (issue #1582).
    ///
    /// Raised by the SELECT executor *while collecting* a materialized result
    /// set, before it grows large enough to threaten the process's <128MB
    /// memory target. This is the byte-unit successor to the coarse row-count
    /// safety valve: a byte ceiling correctly distinguishes 1M harmless skinny
    /// rows from 100k memory-blowing wide rows. The remedy is in the message —
    /// bound the result with a `LIMIT` clause, or consume it incrementally via
    /// the streaming query API instead of materializing the whole set.
    #[error(
        "result set exceeded the {budget_bytes}-byte materialization budget \
         (estimated {estimated_bytes} bytes across {rows} rows so far); \
         add a LIMIT clause to bound the result, or use the streaming query \
         API (e.g. execute_streaming) to consume rows incrementally"
    )]
    ResultTooLarge {
        /// Configured materialization byte budget that was exceeded.
        budget_bytes: usize,
        /// Estimated logical size (bytes) of the result collected so far.
        estimated_bytes: usize,
        /// Number of rows collected when the budget was exceeded.
        rows: usize,
    },

    /// An unrecognized `CQLITE_READ_PATH` value was supplied (issue #1918).
    ///
    /// Resolving the read-path forcing knob returns this distinct error rather
    /// than silently falling through to `auto`, so a typo'd knob is loud instead
    /// of a no-op. Names the invalid value and the allowed set.
    #[error("invalid CQLITE_READ_PATH value '{value}': expected one of auto, point, full")]
    InvalidReadPath {
        /// The unrecognized value supplied to the knob.
        value: String,
    },

    /// Forced `point` read path could not run a partition-targeted lookup (issue
    /// #1918).
    ///
    /// Raised whenever a forced read path cannot serve a query without silently
    /// diverging from the `auto` result. Under `CQLITE_READ_PATH=point` (or the
    /// equivalent `QueryConfig` field) this fires when the executor would not run
    /// a genuinely partition-targeted lookup — a classification fallback, an
    /// unwired targeted surface (e.g. a metadata `IN` fan-out), or a build/path
    /// that does not actually prune. Under `CQLITE_READ_PATH=full` it fires for a
    /// schema-less sole-pk point lookup, which only the specialized targeted seek
    /// can serve correctly (a full scan would return 0 rows instead of the row
    /// `auto` returns). Either way the query fails closed instead of silently
    /// returning a wrong result; `reason` names the concrete cause.
    #[error(
        "forced read path '{forced}' unavailable: {reason}. This query cannot be \
         served under CQLITE_READ_PATH={forced} without diverging from the 'auto' \
         result; use 'auto' to let CQLite choose the read path"
    )]
    ForcedReadPathUnavailable {
        /// The forced mode that could not be satisfied (`"point"` or `"full"`).
        forced: &'static str,
        /// The concrete fallback reason label (e.g. `partition_key_not_fully_constrained`).
        reason: String,
    },

    /// Type conversion errors
    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Storage engine errors
    #[error("Storage error: {0}")]
    Storage(String),

    /// Memory management errors
    #[error("Memory error: {0}")]
    Memory(String),

    /// Lock/concurrency errors
    #[error("Concurrency error: {0}")]
    Concurrency(String),

    /// Write directory already locked by another process or Database instance
    ///
    /// Returned by `WriteEngine::new` when the advisory lock on `write_dir`
    /// cannot be acquired because another `WriteEngine` (in this or another
    /// process) already holds it.  Only one `Database` instance may hold a
    /// `write_dir` at a time.
    #[error(
        "write_dir '{path}' is already locked by another process. \
         Only one Database instance may hold a write_dir at a time."
    )]
    WriteDirLocked {
        /// The path that could not be locked
        path: String,
    },

    /// Resource not found
    #[error("Not found: {0}")]
    NotFound(String),

    /// Table errors
    #[error("Table error: {0}")]
    Table(String),

    /// Resource already exists
    #[error("Already exists: {0}")]
    AlreadyExists(String),

    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Constraint violation
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    /// Transaction errors
    #[error("Transaction error: {0}")]
    Transaction(String),

    /// Index errors
    #[error("Index error: {0}")]
    Index(String),

    /// Compaction errors
    #[error("Compaction error: {0}")]
    Compaction(String),

    /// WASM-specific errors
    #[cfg(target_arch = "wasm32")]
    #[error("WASM error: {0}")]
    Wasm(String),

    /// Generic internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Parse error
    #[error("Parse error: {0}")]
    Parse(String),

    /// Invalid input error
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Unsupported query error
    #[error("Unsupported query: {0}")]
    UnsupportedQuery(String),

    /// A fixed-width CQL value's declared length is not a length the pinned
    /// Cassandra 5.0.8 serializer for that type admits (issue #3723).
    ///
    /// Raised by the bounded element/field decoder, where the value slice has
    /// already been delimited by its own `[i32 BE len]` prefix, so the slice
    /// length IS the declared length. Reading only the first `expected` bytes
    /// and discarding the rest made two distinct on-disk encodings decode to
    /// the same `Value` — see the issue.
    #[error(
        "Fixed-width length mismatch: CQL type '{cql_type}' at '{context}' \
         admits exactly {expected} byte(s), got {actual}"
    )]
    FixedWidthLengthMismatch {
        /// Canonical CQL short form of the type whose width was violated.
        cql_type: String,
        /// Column / element description the value was read for.
        context: String,
        /// The only byte width this decoder admits for `cql_type`.
        expected: usize,
        /// The declared length actually present on disk.
        actual: usize,
    },

    /// The operation was cooperatively cancelled (issue #2264).
    ///
    /// Raised by a long-running scan (e.g. the compaction streaming read) when
    /// its cancellation token is tripped — a client disconnect propagated from
    /// the Flight `do_get` path. Distinct from a genuine failure so callers can
    /// treat it as a clean, expected abort rather than corruption.
    #[error("Operation cancelled")]
    Cancelled,
}

impl Error {
    /// Create a serialization error
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization {
            message: msg.into(),
            source: None,
        }
    }

    /// Create a corruption error
    pub fn corruption(msg: impl Into<String>) -> Self {
        Self::Corruption(msg.into())
    }

    /// Create a schema error
    pub fn schema(msg: impl Into<String>) -> Self {
        Self::Schema(msg.into())
    }

    /// Create a CQL parse error
    pub fn cql_parse(msg: impl Into<String>) -> Self {
        Self::CqlParse(msg.into())
    }

    /// Create an invalid format error
    pub fn invalid_format(msg: impl Into<String>) -> Self {
        Self::InvalidFormat(msg.into())
    }

    /// Create an unsupported format error
    pub fn unsupported_format(msg: impl Into<String>) -> Self {
        Self::UnsupportedFormat(msg.into())
    }

    /// Create an invalid path error
    pub fn invalid_path(msg: impl Into<String>) -> Self {
        Self::InvalidPath(msg.into())
    }

    /// Create an invalid state error
    pub fn invalid_state(msg: impl Into<String>) -> Self {
        Self::InvalidState(msg.into())
    }

    /// Create a query execution error
    pub fn query_execution(msg: impl Into<String>) -> Self {
        Self::QueryExecution(msg.into())
    }

    /// Create an invalid-read-path error (issue #1918).
    pub fn invalid_read_path(value: impl Into<String>) -> Self {
        Self::InvalidReadPath {
            value: value.into(),
        }
    }

    /// Create a forced-read-path-unavailable error (issue #1918). `forced` is the
    /// forced mode (`"point"` or `"full"`); `reason` is the concrete cause label.
    pub fn forced_read_path_unavailable(forced: &'static str, reason: impl Into<String>) -> Self {
        Self::ForcedReadPathUnavailable {
            forced,
            reason: reason.into(),
        }
    }

    /// Create a type conversion error
    pub fn type_conversion(msg: impl Into<String>) -> Self {
        Self::TypeConversion(msg.into())
    }

    /// Create a configuration error
    pub fn configuration(msg: impl Into<String>) -> Self {
        Self::Configuration(msg.into())
    }

    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }

    /// Create a memory error
    pub fn memory(msg: impl Into<String>) -> Self {
        Self::Memory(msg.into())
    }

    /// Create a concurrency error
    pub fn concurrency(msg: impl Into<String>) -> Self {
        Self::Concurrency(msg.into())
    }

    /// Create a not found error
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    /// Create an already exists error
    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self::AlreadyExists(msg.into())
    }

    /// Create an invalid operation error
    pub fn invalid_operation(msg: impl Into<String>) -> Self {
        Self::InvalidOperation(msg.into())
    }

    /// Create a constraint violation error
    pub fn constraint_violation(msg: impl Into<String>) -> Self {
        Self::ConstraintViolation(msg.into())
    }

    /// Create a transaction error
    pub fn transaction(msg: impl Into<String>) -> Self {
        Self::Transaction(msg.into())
    }

    /// Create an index error
    pub fn index(msg: impl Into<String>) -> Self {
        Self::Index(msg.into())
    }

    /// Create a compaction error
    pub fn compaction(msg: impl Into<String>) -> Self {
        Self::Compaction(msg.into())
    }

    /// Create a WASM error
    #[cfg(target_arch = "wasm32")]
    pub fn wasm(msg: impl Into<String>) -> Self {
        Self::Wasm(msg.into())
    }

    /// Create an internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Create an invalid input error
    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self::InvalidInput(msg.into())
    }

    /// Create a parse error
    pub fn parse(msg: impl Into<String>) -> Self {
        Self::Parse(msg.into())
    }

    /// Create an unsupported query error
    pub fn unsupported_query(msg: impl Into<String>) -> Self {
        Self::UnsupportedQuery(msg.into())
    }

    /// Create a write-dir locked error
    pub fn write_dir_locked(path: impl Into<String>) -> Self {
        Self::WriteDirLocked { path: path.into() }
    }

    /// Create a table not found error
    pub fn table_not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(format!("Table not found: {}", msg.into()))
    }

    /// Create an ambiguous table error
    pub fn ambiguous_table(msg: impl Into<String>) -> Self {
        Self::Table(format!("Ambiguous table reference: {}", msg.into()))
    }

    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            // These errors are typically recoverable with retry
            Error::Io(_) => true,
            Error::Concurrency(_) => true,
            Error::Memory(_) => true,

            // These errors are typically not recoverable
            Error::Corruption(_) => false,
            // A wrong on-disk length is corrupt input, not a transient fault.
            Error::FixedWidthLengthMismatch { .. } => false,
            Error::Schema(_) => false,
            Error::CqlParse(_) => false,
            Error::Configuration(_) => false,

            // Context-dependent errors
            Error::Storage(_) => true,
            Error::QueryExecution(_) => false,
            // Not recoverable by retry: the same query would re-materialize the
            // same oversized result. The user must add LIMIT or stream.
            Error::ResultTooLarge { .. } => false,
            // A knob misconfiguration re-fails identically until the operator fixes it.
            Error::InvalidReadPath { .. } => false,
            Error::ForcedReadPathUnavailable { .. } => false,
            Error::TypeConversion(_) => false,
            Error::NotFound(_) => false,
            Error::AlreadyExists(_) => false,
            Error::InvalidOperation(_) => false,
            Error::ConstraintViolation(_) => false,
            Error::Transaction(_) => true,
            Error::Index(_) => true,
            Error::Compaction(_) => true,

            // New error types
            Error::Table(_) => false,

            // Write-dir lock conflict — not recoverable without releasing the lock
            Error::WriteDirLocked { .. } => false,

            #[cfg(target_arch = "wasm32")]
            Error::Wasm(_) => false,

            Error::Serialization { .. } => false,
            Error::Internal(_) => false,
            Error::Parse(_) => false,
            Error::InvalidInput(_) => false,
            Error::InvalidFormat(_) => false,
            Error::UnsupportedFormat(_) => false,
            Error::UnsupportedVersion { .. } => false,
            Error::UnsupportedCommitLogVersion { .. } => false,
            Error::CorruptCommitLogFrame(_) => false,
            Error::InvalidPath(_) => false,
            Error::InvalidState(_) => false,
            Error::Timeout(_) => false,
            // Issue #1695: re-running the same query under the same budget elapses
            // again — the operator must raise `query.max_execution_time` or narrow
            // the query (same reasoning as `ResultTooLarge`).
            Error::QueryTimeout { .. } => false,
            Error::UnsupportedQuery(_) => false,
            // A cancelled operation is deliberate, not a transient fault: re-running
            // it would just be cancelled again. The caller decides whether to retry.
            Error::Cancelled => false,
        }
    }

    /// Get the error category
    pub fn category(&self) -> ErrorCategory {
        match self {
            Error::Io(_) => ErrorCategory::System,
            Error::Serialization { .. } => ErrorCategory::Data,
            Error::Corruption(_) => ErrorCategory::Data,
            Error::FixedWidthLengthMismatch { .. } => ErrorCategory::Data,
            Error::Schema(_) => ErrorCategory::Schema,
            Error::CqlParse(_) => ErrorCategory::Query,
            Error::QueryExecution(_) => ErrorCategory::Query,
            Error::ResultTooLarge { .. } => ErrorCategory::Query,
            Error::InvalidReadPath { .. } => ErrorCategory::Configuration,
            Error::ForcedReadPathUnavailable { .. } => ErrorCategory::Query,
            Error::TypeConversion(_) => ErrorCategory::Data,
            Error::Configuration(_) => ErrorCategory::Configuration,
            Error::Storage(_) => ErrorCategory::Storage,
            Error::Memory(_) => ErrorCategory::System,
            Error::Concurrency(_) => ErrorCategory::Concurrency,
            Error::NotFound(_) => ErrorCategory::NotFound,
            Error::AlreadyExists(_) => ErrorCategory::Conflict,
            Error::InvalidOperation(_) => ErrorCategory::Logic,
            Error::ConstraintViolation(_) => ErrorCategory::Constraint,
            Error::Transaction(_) => ErrorCategory::Transaction,
            Error::Index(_) => ErrorCategory::Storage,
            Error::Compaction(_) => ErrorCategory::Storage,

            // New error types
            Error::Table(_) => ErrorCategory::Schema,

            // Write-dir lock conflict
            Error::WriteDirLocked { .. } => ErrorCategory::Concurrency,

            #[cfg(target_arch = "wasm32")]
            Error::Wasm(_) => ErrorCategory::Platform,

            Error::Internal(_) => ErrorCategory::Internal,
            Error::Parse(_) => ErrorCategory::Data,
            Error::InvalidInput(_) => ErrorCategory::Data,
            Error::InvalidFormat(_) => ErrorCategory::Data,
            Error::UnsupportedFormat(_) => ErrorCategory::Data,
            Error::UnsupportedVersion { .. } => ErrorCategory::Data,
            Error::UnsupportedCommitLogVersion { .. } => ErrorCategory::Data,
            Error::CorruptCommitLogFrame(_) => ErrorCategory::Data,
            Error::InvalidPath(_) => ErrorCategory::System,
            Error::InvalidState(_) => ErrorCategory::Logic,
            Error::Timeout(_) => ErrorCategory::System,
            // Issue #1695: a query-budget elapse is a QUERY-lifecycle outcome, not
            // a `Data` (corruption/serialization) fault. The developer-facing
            // taxonomy is deliberately left at 14 variants (adding one breaks the
            // bindings' public code mapping); the DISTINCT bucket lives in the
            // telemetry taxonomy — see `observability::ObsErrorCategory::Timeout`.
            Error::QueryTimeout { .. } => ErrorCategory::Query,
            Error::UnsupportedQuery(_) => ErrorCategory::Query,
            Error::Cancelled => ErrorCategory::Cancelled,
        }
    }
}

/// Error categories for grouping related errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// System-level errors (I/O, memory, etc.)
    System,
    /// Data-related errors (corruption, serialization)
    Data,
    /// Schema-related errors
    Schema,
    /// Query-related errors (parsing, execution)
    Query,
    /// Configuration errors
    Configuration,
    /// Storage engine errors
    Storage,
    /// Concurrency-related errors
    Concurrency,
    /// Resource not found
    NotFound,
    /// Resource conflicts
    Conflict,
    /// Logic errors
    Logic,
    /// Constraint violations
    Constraint,
    /// Transaction errors
    Transaction,
    /// Platform-specific errors
    Platform,
    /// Internal errors
    Internal,
    /// A cooperative cancellation / abort (issue #2264). Distinct from
    /// `System` (I/O, memory) so a cancelled operation is never mislabeled as
    /// a transport/IO failure by a consumer that switches on category.
    Cancelled,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ErrorCategory::System => "System",
            ErrorCategory::Data => "Data",
            ErrorCategory::Schema => "Schema",
            ErrorCategory::Query => "Query",
            ErrorCategory::Configuration => "Configuration",
            ErrorCategory::Storage => "Storage",
            ErrorCategory::Concurrency => "Concurrency",
            ErrorCategory::NotFound => "NotFound",
            ErrorCategory::Conflict => "Conflict",
            ErrorCategory::Logic => "Logic",
            ErrorCategory::Constraint => "Constraint",
            ErrorCategory::Transaction => "Transaction",
            ErrorCategory::Platform => "Platform",
            ErrorCategory::Internal => "Internal",
            ErrorCategory::Cancelled => "Cancelled",
        };
        write!(f, "{}", name)
    }
}

/// Convert from bincode errors
impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Error::Serialization {
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

/// Convert from serde_json errors
impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization {
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

/// Convert from nom errors
impl<I> From<nom::Err<nom::error::Error<I>>> for Error
where
    I: std::fmt::Debug,
{
    fn from(err: nom::Err<nom::error::Error<I>>) -> Self {
        Error::CqlParse(format!("Parse error: {:?}", err))
    }
}

// Helper function to create custom parse error type
pub type ParseResult<I, O> = nom::IResult<I, O, Error>;

/// Custom error type for parsing operations
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_from_conversions() {
        // Test bincode error conversion (covers line 399-400)
        let io_err = std::io::Error::other("test error");
        let bincode_err = bincode::Error::new(bincode::ErrorKind::Io(io_err));
        let error = Error::from(bincode_err);
        assert!(matches!(error, Error::Serialization { .. }));

        // Test serde_json error conversion (covers line 406-407)
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let error = Error::from(json_err);
        assert!(matches!(error, Error::Serialization { .. }));

        // Test nom error conversion (covers line 416-417)
        let nom_err = nom::Err::Error(nom::error::Error::new(
            "test input",
            nom::error::ErrorKind::Tag,
        ));
        let error = Error::from(nom_err);
        assert!(matches!(error, Error::CqlParse(_)));
    }

    #[test]
    fn test_parse_error_display() {
        // Test ParseError Display implementation (covers line 431-432)
        let parse_error = ParseError {
            message: "test parse error".to_string(),
        };
        let display_str = format!("{}", parse_error);
        assert_eq!(display_str, "test parse error");
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn test_wasm_error_creation() {
        // Test WASM error creation (covers line 234-235)
        let err = Error::wasm("WASM error");
        assert!(matches!(err, Error::Wasm(_)));
        assert!(!err.is_recoverable());
        assert_eq!(err.category(), ErrorCategory::Platform);
    }

    #[test]
    fn test_new_error_types_coverage() {
        // Test Table error
        let table_err = Error::Table("table error".to_string());
        assert!(!table_err.is_recoverable());
        assert_eq!(table_err.category(), ErrorCategory::Schema);
    }

    #[test]
    fn test_error_creation() {
        let err = Error::storage("test error");
        assert!(matches!(err, Error::Storage(_)));
        assert_eq!(err.to_string(), "Storage error: test error");
    }

    #[test]
    fn test_error_categories() {
        assert_eq!(Error::storage("test").category(), ErrorCategory::Storage);
        assert_eq!(Error::schema("test").category(), ErrorCategory::Schema);
        assert_eq!(Error::cql_parse("test").category(), ErrorCategory::Query);
    }

    #[test]
    fn test_error_recoverability() {
        assert!(Error::concurrency("test").is_recoverable());
        assert!(!Error::corruption("test").is_recoverable());
        assert!(!Error::schema("test").is_recoverable());
    }

    #[test]
    fn test_all_error_constructors() {
        // Test all error constructor methods for coverage
        let _ = Error::serialization("test");
        let _ = Error::corruption("test");
        let _ = Error::schema("test");
        let _ = Error::cql_parse("test");
        let _ = Error::invalid_format("test");
        let _ = Error::unsupported_format("test");
        let _ = Error::invalid_path("test");
        let _ = Error::invalid_state("test");
        let _ = Error::query_execution("test");
        let _ = Error::type_conversion("test");
        let _ = Error::configuration("test");
        let _ = Error::storage("test");
        let _ = Error::memory("test");
        let _ = Error::concurrency("test");
        let _ = Error::not_found("test");
        let _ = Error::already_exists("test");
        let _ = Error::invalid_operation("test");
        let _ = Error::constraint_violation("test");
        let _ = Error::transaction("test");
        let _ = Error::index("test");
        let _ = Error::compaction("test");
        let _ = Error::internal("test");
        let _ = Error::invalid_input("test");
        let _ = Error::parse("test");
        let _ = Error::write_dir_locked("/tmp/test-dir");
    }

    #[test]
    fn test_all_error_categories() {
        // Test all error categories for coverage
        assert_eq!(Error::serialization("test").category(), ErrorCategory::Data);
        assert_eq!(Error::corruption("test").category(), ErrorCategory::Data);
        assert_eq!(Error::cql_parse("test").category(), ErrorCategory::Query);
        assert_eq!(
            Error::invalid_format("test").category(),
            ErrorCategory::Data
        );
        assert_eq!(
            Error::unsupported_format("test").category(),
            ErrorCategory::Data
        );
        assert_eq!(
            Error::invalid_path("test").category(),
            ErrorCategory::System
        );
        assert_eq!(
            Error::invalid_state("test").category(),
            ErrorCategory::Logic
        );
        assert_eq!(
            Error::query_execution("test").category(),
            ErrorCategory::Query
        );
        assert_eq!(
            Error::type_conversion("test").category(),
            ErrorCategory::Data
        );
        assert_eq!(
            Error::configuration("test").category(),
            ErrorCategory::Configuration
        );
        assert_eq!(Error::memory("test").category(), ErrorCategory::System);
        assert_eq!(
            Error::concurrency("test").category(),
            ErrorCategory::Concurrency
        );
        assert_eq!(Error::not_found("test").category(), ErrorCategory::NotFound);
        assert_eq!(
            Error::already_exists("test").category(),
            ErrorCategory::Conflict
        );
        assert_eq!(
            Error::invalid_operation("test").category(),
            ErrorCategory::Logic
        );
        assert_eq!(
            Error::constraint_violation("test").category(),
            ErrorCategory::Constraint
        );
        assert_eq!(
            Error::transaction("test").category(),
            ErrorCategory::Transaction
        );
        assert_eq!(Error::index("test").category(), ErrorCategory::Storage);
        assert_eq!(Error::compaction("test").category(), ErrorCategory::Storage);
        assert_eq!(Error::internal("test").category(), ErrorCategory::Internal);
        assert_eq!(Error::invalid_input("test").category(), ErrorCategory::Data);
        assert_eq!(Error::parse("test").category(), ErrorCategory::Data);
        assert_eq!(
            Error::write_dir_locked("/tmp/test").category(),
            ErrorCategory::Concurrency
        );
        // Issue #2264: a cooperative cancellation must NEVER classify as
        // `System` (which downstream bindings map to an I/O error code).
        assert_eq!(Error::Cancelled.category(), ErrorCategory::Cancelled);
    }

    #[test]
    fn test_all_error_recoverability() {
        // Test recoverability for all error types
        assert!(Error::memory("test").is_recoverable());
        assert!(Error::storage("test").is_recoverable());
        assert!(Error::transaction("test").is_recoverable());
        assert!(Error::index("test").is_recoverable());
        assert!(Error::compaction("test").is_recoverable());

        assert!(!Error::serialization("test").is_recoverable());
        assert!(!Error::cql_parse("test").is_recoverable());
        assert!(!Error::invalid_format("test").is_recoverable());
        assert!(!Error::unsupported_format("test").is_recoverable());
        assert!(!Error::invalid_path("test").is_recoverable());
        assert!(!Error::invalid_state("test").is_recoverable());
        assert!(!Error::query_execution("test").is_recoverable());
        assert!(!Error::type_conversion("test").is_recoverable());
        assert!(!Error::configuration("test").is_recoverable());
        assert!(!Error::not_found("test").is_recoverable());
        assert!(!Error::already_exists("test").is_recoverable());
        assert!(!Error::invalid_operation("test").is_recoverable());
        assert!(!Error::constraint_violation("test").is_recoverable());
        assert!(!Error::internal("test").is_recoverable());
        assert!(!Error::invalid_input("test").is_recoverable());
        assert!(!Error::parse("test").is_recoverable());
        assert!(!Error::write_dir_locked("/tmp/test").is_recoverable());
    }

    #[test]
    fn test_error_category_display() {
        // Test ErrorCategory Display implementation
        assert_eq!(ErrorCategory::System.to_string(), "System");
        assert_eq!(ErrorCategory::Data.to_string(), "Data");
        assert_eq!(ErrorCategory::Schema.to_string(), "Schema");
        assert_eq!(ErrorCategory::Query.to_string(), "Query");
        assert_eq!(ErrorCategory::Configuration.to_string(), "Configuration");
        assert_eq!(ErrorCategory::Storage.to_string(), "Storage");
        assert_eq!(ErrorCategory::Concurrency.to_string(), "Concurrency");
        assert_eq!(ErrorCategory::NotFound.to_string(), "NotFound");
        assert_eq!(ErrorCategory::Conflict.to_string(), "Conflict");
        assert_eq!(ErrorCategory::Logic.to_string(), "Logic");
        assert_eq!(ErrorCategory::Constraint.to_string(), "Constraint");
        assert_eq!(ErrorCategory::Transaction.to_string(), "Transaction");
        assert_eq!(ErrorCategory::Platform.to_string(), "Platform");
        assert_eq!(ErrorCategory::Internal.to_string(), "Internal");
        assert_eq!(ErrorCategory::Cancelled.to_string(), "Cancelled");
    }

    #[test]
    fn test_error_from_io_error() {
        // Test conversion from std::io::Error
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let cqlite_err: Error = io_err.into();
        assert!(matches!(cqlite_err, Error::Io(_)));
        assert_eq!(cqlite_err.category(), ErrorCategory::System);
        assert!(cqlite_err.is_recoverable());
    }

    #[test]
    fn test_result_type_alias() {
        // Test the Result type alias
        let success: Result<i32> = Ok(42);
        let failure: Result<i32> = Err(Error::storage("test error"));

        assert!(success.is_ok());
        if let Ok(value) = success {
            assert_eq!(value, 42);
        }
        assert!(failure.is_err());
    }
}
