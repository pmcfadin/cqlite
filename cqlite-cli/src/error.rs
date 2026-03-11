//! Error handling and exit code management for CQLite CLI
//!
//! This module defines standardized exit codes as specified in M2_CLI_SPEC.md
//! and provides utilities for classifying errors and printing helpful messages.

/// CLI exit codes as specified in M2_CLI_SPEC.md line 340
///
/// Exit codes:
/// - 0: success
/// - 2: invalid CLI args
/// - 3: schema errors
/// - 4: data-dir/discovery errors
/// - 5: query execution errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
#[allow(dead_code)] // Success variant used for completeness per spec
pub enum CliExitCode {
    /// Successful execution
    Success = 0,
    /// Invalid CLI arguments or flags
    InvalidCliArgs = 2,
    /// Schema parsing or loading errors
    SchemaError = 3,
    /// Data directory or discovery errors
    DataDirError = 4,
    /// Query execution errors
    QueryExecutionError = 5,
}

impl CliExitCode {
    /// Convert exit code to i32 for process::exit
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Get helpful hint for this error category per M2_CLI_SPEC.md
    ///
    /// These hints follow the spec's guidance to provide concise,
    /// actionable next-step commands.
    pub fn hint(&self) -> &'static str {
        match self {
            Self::Success => "",
            Self::InvalidCliArgs => "Run 'cqlite --help' for usage information",
            Self::SchemaError => "Use ':schema load <file>' or '--schema <path>' to provide schema",
            Self::DataDirError => {
                "Use ':config data-dir <path>' or '--data-dir <path>' to set data directory"
            }
            Self::QueryExecutionError => "Check query syntax and ensure required data is available",
        }
    }
}

/// Classify an error into appropriate exit code category
///
/// Examines the error message and context chain to determine the most
/// appropriate exit code per M2_CLI_SPEC.md exit code policy.
///
/// Classification logic (order matters - most specific first):
/// 1. CLI argument parsing errors -> InvalidCliArgs (2)
/// 2. Schema-related errors -> SchemaError (3)
/// 3. Data directory/SSTable errors -> DataDirError (4)
/// 4. Default: Query execution errors -> QueryExecutionError (5)
///
/// # Examples
///
/// ```
/// use anyhow::anyhow;
/// use cqlite_cli::error::{classify_error, CliExitCode};
///
/// let err = anyhow!("Failed to parse schema file");
/// assert_eq!(classify_error(&err), CliExitCode::SchemaError);
///
/// let err = anyhow!("Data directory not found");
/// assert_eq!(classify_error(&err), CliExitCode::DataDirError);
/// ```
pub fn classify_error(err: &anyhow::Error) -> CliExitCode {
    // First, try to downcast to cqlite_core::Error for precise classification
    if let Some(core_err) = err.downcast_ref::<cqlite_core::Error>() {
        return match core_err {
            // Exit code 3: Schema loading errors
            cqlite_core::Error::Schema(_) | cqlite_core::Error::Table(_) => {
                CliExitCode::SchemaError
            }

            // Exit code 4: Data directory / SSTable discovery errors
            cqlite_core::Error::Io(_)
            | cqlite_core::Error::Storage(_)
            | cqlite_core::Error::InvalidPath(_)
            | cqlite_core::Error::NotFound(_)
            | cqlite_core::Error::Index(_) => CliExitCode::DataDirError,

            // Exit code 5: Query execution / unsupported query failures
            cqlite_core::Error::QueryExecution(_)
            | cqlite_core::Error::CqlParse(_)
            | cqlite_core::Error::UnsupportedQuery(_)
            | cqlite_core::Error::Parse(_)
            | cqlite_core::Error::InvalidInput(_) => CliExitCode::QueryExecutionError,

            // Other errors default to query execution error
            _ => CliExitCode::QueryExecutionError,
        };
    }

    // Fall back to string-based classification for non-core errors
    let err_chain = format!("{:#}", err);
    let err_lower = err_chain.to_lowercase();

    // Check for specific error patterns (order matters - most specific first)

    // Issue #231: Missing --data-dir flag check (MUST be before schema check)
    // This specific error message contains "--schema" but is really about missing --data-dir
    if err_lower.contains("missing required flag: --data-dir") {
        return CliExitCode::DataDirError;
    }

    // CLI argument parsing errors (from clap or manual validation)
    if err_lower.contains("invalid argument")
        || err_lower.contains("unexpected argument")
        || err_lower.contains("required argument")
        || err_chain.contains("clap::Error")
        || err_lower.contains("usage:")
    {
        return CliExitCode::InvalidCliArgs;
    }

    // Schema-related errors
    if err_lower.contains("schema")
        || err_lower.contains("failed to parse cql")
        || err_lower.contains("failed to parse json")
        || err_lower.contains("missing keyspace")
        || err_lower.contains("missing table")
        || err_lower.contains("invalid column")
    {
        return CliExitCode::SchemaError;
    }

    // Data directory and SSTable discovery errors
    if err_lower.contains("data-dir")
        || err_lower.contains("data directory")
        || err_lower.contains("sstable")
        || err_lower.contains("discovery")
        || err_lower.contains("failed to open")
        || err_lower.contains("no such file or directory")
        || err_lower.contains("not found")
        || err_lower.contains("cannot read file")
    {
        return CliExitCode::DataDirError;
    }

    // Default to query execution error for database operations
    CliExitCode::QueryExecutionError
}

/// Print error with appropriate formatting and helpful hint
///
/// Formats error output consistently across the CLI with:
/// - Error message with full context chain
/// - Context-aware hint for resolution
///
/// # Examples
///
/// ```no_run
/// use anyhow::anyhow;
/// use cqlite_cli::error::{print_error, classify_error};
///
/// let err = anyhow!("Unsupported query: JOIN not supported");
/// let exit_code = classify_error(&err);
/// print_error(&err, exit_code);
/// // Output:
/// // Error: Unsupported query: JOIN not supported
/// //
/// // Hint: Supported SELECT features in M2:
/// //   • SELECT with WHERE on partition/primary key
/// //   ...
/// ```
pub fn print_error(err: &anyhow::Error, exit_code: CliExitCode) {
    eprintln!("Error: {:#}", err);

    let hint = get_error_hint(err, exit_code);
    if !hint.is_empty() {
        eprintln!("\nHint: {}", hint);
    }
}

/// Get context-aware hint based on error details and exit code
///
/// Provides enhanced hints for specific error categories, particularly
/// for unsupported query features per M2_CLI_SPEC.md
///
/// # Examples
///
/// ```no_run
/// use anyhow::anyhow;
/// use cqlite_cli::error::{get_error_hint, classify_error};
///
/// let err = anyhow!("Unsupported query: JOIN not supported");
/// let exit_code = classify_error(&err);
/// let hint = get_error_hint(&err, exit_code);
/// assert!(hint.contains("Supported SELECT features"));
/// ```
pub fn get_error_hint(err: &anyhow::Error, exit_code: CliExitCode) -> String {
    // For query execution errors, check if it's an unsupported query
    if matches!(exit_code, CliExitCode::QueryExecutionError) {
        let err_text = format!("{:#}", err).to_lowercase();

        // Detect unsupported query pattern from core Error::UnsupportedQuery
        if err_text.contains("unsupported query") || err_text.contains("not supported") {
            return build_unsupported_query_hint();
        }
    }

    // Fall back to standard hint from CliExitCode
    exit_code.hint().to_string()
}

/// Build detailed hint for unsupported query features
///
/// Provides specific guidance on what SELECT features are supported in M2
/// as defined in M2_CLI_SPEC.md lines 303-306, 328-330
fn build_unsupported_query_hint() -> String {
    let mut hint = String::new();
    hint.push_str("Supported SELECT features in M2:\n");
    hint.push_str("  • SELECT with WHERE on partition/primary key\n");
    hint.push_str("  • LIMIT clause for result pagination\n");
    hint.push_str("  • DESCRIBE/DESC for schema information\n");
    hint.push_str("  • USE for keyspace switching\n\n");
    hint.push_str("Examples:\n");
    hint.push_str("  SELECT * FROM users WHERE id = ? LIMIT 10\n");
    hint.push_str("  DESCRIBE TABLE keyspace.users\n");
    hint.push_str("  USE my_keyspace\n\n");
    hint.push_str("Not supported: JOIN, subqueries, advanced aggregations\n");
    hint.push_str("See: cqlite-cli/CLI_USAGE_EXAMPLES.md");
    hint
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn test_classify_invalid_args() {
        let err = anyhow!("Invalid argument: --foo");
        assert_eq!(classify_error(&err), CliExitCode::InvalidCliArgs);

        let err = anyhow!("Required argument missing");
        assert_eq!(classify_error(&err), CliExitCode::InvalidCliArgs);
    }

    #[test]
    fn test_classify_schema_error() {
        let err = anyhow!("Failed to parse schema file");
        assert_eq!(classify_error(&err), CliExitCode::SchemaError);

        let err = anyhow!("Missing keyspace in schema");
        assert_eq!(classify_error(&err), CliExitCode::SchemaError);
    }

    #[test]
    fn test_classify_data_dir_error() {
        let err = anyhow!("Data directory not found");
        assert_eq!(classify_error(&err), CliExitCode::DataDirError);

        let err = anyhow!("Failed to open SSTable");
        assert_eq!(classify_error(&err), CliExitCode::DataDirError);
    }

    #[test]
    fn test_classify_missing_data_dir_flag() {
        // Issue #231: Missing --data-dir flag should return exit code 4
        let err = anyhow!(
            "Missing required flag: --data-dir\n\n\
             One-shot query execution requires both --schema and --data-dir."
        );
        assert_eq!(classify_error(&err), CliExitCode::DataDirError);
        assert_eq!(classify_error(&err).as_i32(), 4);
    }

    #[test]
    fn test_classify_query_error() {
        let err = anyhow!("Query execution failed");
        assert_eq!(classify_error(&err), CliExitCode::QueryExecutionError);

        let err = anyhow!("Syntax error in SELECT statement");
        assert_eq!(classify_error(&err), CliExitCode::QueryExecutionError);
    }

    #[test]
    fn test_exit_code_hints() {
        assert_eq!(
            CliExitCode::InvalidCliArgs.hint(),
            "Run 'cqlite --help' for usage information"
        );
        assert_eq!(
            CliExitCode::SchemaError.hint(),
            "Use ':schema load <file>' or '--schema <path>' to provide schema"
        );
        assert_eq!(
            CliExitCode::DataDirError.hint(),
            "Use ':config data-dir <path>' or '--data-dir <path>' to set data directory"
        );
        assert_eq!(
            CliExitCode::QueryExecutionError.hint(),
            "Check query syntax and ensure required data is available"
        );
        assert_eq!(CliExitCode::Success.hint(), "");
    }

    #[test]
    fn test_exit_code_as_i32() {
        assert_eq!(CliExitCode::Success.as_i32(), 0);
        assert_eq!(CliExitCode::InvalidCliArgs.as_i32(), 2);
        assert_eq!(CliExitCode::SchemaError.as_i32(), 3);
        assert_eq!(CliExitCode::DataDirError.as_i32(), 4);
        assert_eq!(CliExitCode::QueryExecutionError.as_i32(), 5);
    }

    #[test]
    fn test_unsupported_query_detection() {
        let err = anyhow!("Unsupported query: JOIN operations not supported");
        let exit_code = classify_error(&err);
        assert_eq!(exit_code, CliExitCode::QueryExecutionError);

        let hint = get_error_hint(&err, exit_code);
        assert!(hint.contains("Supported SELECT features"));
        assert!(hint.contains("WHERE on partition/primary key"));
        assert!(hint.contains("LIMIT"));
        assert!(hint.contains("DESCRIBE"));
    }

    #[test]
    fn test_unsupported_query_hint_format() {
        let err = anyhow!("Query feature not supported");
        let exit_code = CliExitCode::QueryExecutionError;
        let hint = get_error_hint(&err, exit_code);

        // Verify hint contains required sections
        assert!(hint.contains("Supported SELECT features"));
        assert!(hint.contains("Examples:"));
        assert!(hint.contains("Not supported"));
        assert!(hint.contains("CLI_USAGE_EXAMPLES.md"));
    }

    #[test]
    fn test_regular_query_error_hint() {
        let err = anyhow!("Query execution failed: timeout");
        let exit_code = classify_error(&err);
        let hint = get_error_hint(&err, exit_code);

        // Should get standard hint, not unsupported query hint
        assert_eq!(
            hint,
            "Check query syntax and ensure required data is available"
        );
    }

    #[test]
    fn test_exit_code_5_for_unsupported_queries() {
        let err = anyhow!("Unsupported query: subquery not allowed");
        let exit_code = classify_error(&err);
        assert_eq!(exit_code.as_i32(), 5);
    }

    #[test]
    fn test_classify_core_schema_errors() {
        // Test direct cqlite_core::Error::Schema mapping
        let core_err = cqlite_core::Error::schema("Invalid schema definition");
        let anyhow_err = anyhow::Error::new(core_err);
        assert_eq!(classify_error(&anyhow_err), CliExitCode::SchemaError);
        assert_eq!(classify_error(&anyhow_err).as_i32(), 3);

        // Test cqlite_core::Error::Table mapping
        let table_err = cqlite_core::Error::Table("Table not found".to_string());
        let anyhow_err = anyhow::Error::new(table_err);
        assert_eq!(classify_error(&anyhow_err), CliExitCode::SchemaError);
        assert_eq!(classify_error(&anyhow_err).as_i32(), 3);
    }

    #[test]
    fn test_classify_core_discovery_errors() {
        // Test cqlite_core::Error::Io mapping
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let core_err = cqlite_core::Error::from(io_err);
        let anyhow_err = anyhow::Error::new(core_err);
        assert_eq!(classify_error(&anyhow_err), CliExitCode::DataDirError);
        assert_eq!(classify_error(&anyhow_err).as_i32(), 4);

        // Test cqlite_core::Error::Storage mapping
        let storage_err = cqlite_core::Error::storage("SSTable not accessible");
        let anyhow_err = anyhow::Error::new(storage_err);
        assert_eq!(classify_error(&anyhow_err), CliExitCode::DataDirError);
        assert_eq!(classify_error(&anyhow_err).as_i32(), 4);

        // Test cqlite_core::Error::InvalidPath mapping
        let path_err = cqlite_core::Error::invalid_path("/nonexistent/path");
        let anyhow_err = anyhow::Error::new(path_err);
        assert_eq!(classify_error(&anyhow_err), CliExitCode::DataDirError);
        assert_eq!(classify_error(&anyhow_err).as_i32(), 4);

        // Test cqlite_core::Error::NotFound mapping
        let not_found_err = cqlite_core::Error::not_found("Resource not found");
        let anyhow_err = anyhow::Error::new(not_found_err);
        assert_eq!(classify_error(&anyhow_err), CliExitCode::DataDirError);
        assert_eq!(classify_error(&anyhow_err).as_i32(), 4);

        // Test cqlite_core::Error::Index mapping
        let index_err = cqlite_core::Error::index("Index read failure");
        let anyhow_err = anyhow::Error::new(index_err);
        assert_eq!(classify_error(&anyhow_err), CliExitCode::DataDirError);
        assert_eq!(classify_error(&anyhow_err).as_i32(), 4);
    }

    #[test]
    fn test_classify_core_query_errors() {
        // Test cqlite_core::Error::QueryExecution mapping
        let query_err = cqlite_core::Error::query_execution("Query failed");
        let anyhow_err = anyhow::Error::new(query_err);
        assert_eq!(
            classify_error(&anyhow_err),
            CliExitCode::QueryExecutionError
        );
        assert_eq!(classify_error(&anyhow_err).as_i32(), 5);

        // Test cqlite_core::Error::CqlParse mapping
        let parse_err = cqlite_core::Error::cql_parse("Invalid SELECT syntax");
        let anyhow_err = anyhow::Error::new(parse_err);
        assert_eq!(
            classify_error(&anyhow_err),
            CliExitCode::QueryExecutionError
        );
        assert_eq!(classify_error(&anyhow_err).as_i32(), 5);

        // Test cqlite_core::Error::UnsupportedQuery mapping
        let unsupported_err = cqlite_core::Error::unsupported_query("JOIN not supported");
        let anyhow_err = anyhow::Error::new(unsupported_err);
        assert_eq!(
            classify_error(&anyhow_err),
            CliExitCode::QueryExecutionError
        );
        assert_eq!(classify_error(&anyhow_err).as_i32(), 5);

        // Test cqlite_core::Error::Parse mapping
        let parse_err = cqlite_core::Error::parse("Parse failure");
        let anyhow_err = anyhow::Error::new(parse_err);
        assert_eq!(
            classify_error(&anyhow_err),
            CliExitCode::QueryExecutionError
        );
        assert_eq!(classify_error(&anyhow_err).as_i32(), 5);

        // Test cqlite_core::Error::InvalidInput mapping
        let invalid_input_err = cqlite_core::Error::invalid_input("Invalid query input");
        let anyhow_err = anyhow::Error::new(invalid_input_err);
        assert_eq!(
            classify_error(&anyhow_err),
            CliExitCode::QueryExecutionError
        );
        assert_eq!(classify_error(&anyhow_err).as_i32(), 5);
    }

    #[test]
    fn test_classify_core_other_errors_default() {
        // Test that other core errors default to QueryExecutionError
        let corruption_err = cqlite_core::Error::corruption("Data corrupted");
        let anyhow_err = anyhow::Error::new(corruption_err);
        assert_eq!(
            classify_error(&anyhow_err),
            CliExitCode::QueryExecutionError
        );
        assert_eq!(classify_error(&anyhow_err).as_i32(), 5);
    }
}
