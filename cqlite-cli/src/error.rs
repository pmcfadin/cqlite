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
    let err_chain = format!("{:#}", err);
    let err_lower = err_chain.to_lowercase();

    // Check for specific error patterns (order matters - most specific first)

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
/// - Category-specific hint for resolution
///
/// # Examples
///
/// ```no_run
/// use anyhow::anyhow;
/// use cqlite_cli::error::{print_error, classify_error};
///
/// let err = anyhow!("Schema file not found");
/// let exit_code = classify_error(&err);
/// print_error(&err, exit_code);
/// // Output:
/// // Error: Schema file not found
/// // Hint: Use ':schema load <file>' or '--schema <path>' to provide schema
/// ```
pub fn print_error(err: &anyhow::Error, exit_code: CliExitCode) {
    eprintln!("Error: {:#}", err);

    let hint = exit_code.hint();
    if !hint.is_empty() {
        eprintln!("Hint: {}", hint);
    }
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
}
