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
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Data corruption errors
    #[error("Data corruption: {0}")]
    Corruption(String),

    /// Schema validation errors
    #[error("Schema error: {0}")]
    Schema(String),

    /// SQL parsing errors
    #[error("SQL parse error: {0}")]
    SqlParse(String),

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
}

impl Error {
    /// Create a serialization error
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
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
            
            #[cfg(target_arch = "wasm32")]
            Error::Wasm(_) => false,
            
            Error::Serialization(_) => false,
            Error::Internal(_) => false,
        }
    }

    /// Get the error category
    pub fn category(&self) -> ErrorCategory {
        match self {
            Error::Io(_) => ErrorCategory::System,
            Error::Serialization(_) => ErrorCategory::Data,
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
            
            #[cfg(target_arch = "wasm32")]
            Error::Wasm(_) => ErrorCategory::Platform,
            
            Error::Internal(_) => ErrorCategory::Internal,
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
        Error::Serialization(err.to_string())
    }
}

/// Convert from serde_json errors
impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}