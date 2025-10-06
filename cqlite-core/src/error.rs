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

    /// SQL parsing errors
    #[error("SQL parse error: {0}")]
    SqlParse(String),

    /// Invalid format error (for SSTable parsing)
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// Unsupported format error
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    /// Timeout error
    #[error("Operation timeout: {0}")]
    Timeout(String),

    /// Invalid path error
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    /// Invalid state error
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Query execution errors
    #[error("Query execution error: {0}")]
    QueryExecution(String),

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

    /// Create a SQL parse error
    pub fn sql_parse(msg: impl Into<String>) -> Self {
        Self::SqlParse(msg.into())
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

    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            // These errors are typically recoverable with retry
            Error::Io(_) => true,
            Error::Concurrency(_) => true,
            Error::Memory(_) => true,

            // These errors are typically not recoverable
            Error::Corruption(_) => false,
            Error::Schema(_) => false,
            Error::SqlParse(_) => false,
            Error::Configuration(_) => false,

            // Context-dependent errors
            Error::Storage(_) => true,
            Error::QueryExecution(_) => false,
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

            #[cfg(target_arch = "wasm32")]
            Error::Wasm(_) => false,

            Error::Serialization { .. } => false,
            Error::Internal(_) => false,
            Error::Parse(_) => false,
            Error::InvalidInput(_) => false,
            Error::InvalidFormat(_) => false,
            Error::UnsupportedFormat(_) => false,
            Error::InvalidPath(_) => false,
            Error::InvalidState(_) => false,
            Error::Timeout(_) => false,
        }
    }

    /// Get the error category
    pub fn category(&self) -> ErrorCategory {
        match self {
            Error::Io(_) => ErrorCategory::System,
            Error::Serialization { .. } => ErrorCategory::Data,
            Error::Corruption(_) => ErrorCategory::Data,
            Error::Schema(_) => ErrorCategory::Schema,
            Error::SqlParse(_) => ErrorCategory::Query,
            Error::QueryExecution(_) => ErrorCategory::Query,
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

            #[cfg(target_arch = "wasm32")]
            Error::Wasm(_) => ErrorCategory::Platform,

            Error::Internal(_) => ErrorCategory::Internal,
            Error::Parse(_) => ErrorCategory::Data,
            Error::InvalidInput(_) => ErrorCategory::Data,
            Error::InvalidFormat(_) => ErrorCategory::Data,
            Error::UnsupportedFormat(_) => ErrorCategory::Data,
            Error::InvalidPath(_) => ErrorCategory::System,
            Error::InvalidState(_) => ErrorCategory::Logic,
            Error::Timeout(_) => ErrorCategory::System,
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
        Error::SqlParse(format!("Parse error: {:?}", err))
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
        assert!(matches!(error, Error::SqlParse(_)));
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
        assert_eq!(Error::sql_parse("test").category(), ErrorCategory::Query);
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
        let _ = Error::sql_parse("test");
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
    }

    #[test]
    fn test_all_error_categories() {
        // Test all error categories for coverage
        assert_eq!(Error::serialization("test").category(), ErrorCategory::Data);
        assert_eq!(Error::corruption("test").category(), ErrorCategory::Data);
        assert_eq!(Error::sql_parse("test").category(), ErrorCategory::Query);
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
        assert!(!Error::sql_parse("test").is_recoverable());
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
