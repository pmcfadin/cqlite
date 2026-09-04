//! Unit tests for [`crate::error`], split out under the campsite rule
//! (epic #1135) so the error taxonomy itself stays under the source
//! threshold. Declared from `error.rs` as `#[cfg(test)] mod tests;`.

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
