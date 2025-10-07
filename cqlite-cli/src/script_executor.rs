//! Script execution module for handling CQL script files
//!
//! This module provides functionality to execute CQL scripts from files,
//! processing multiple statements sequentially with proper error handling
//! and output formatting.

use anyhow::{Context, Result};
use cqlite_core::Database;
use std::path::Path;

use crate::cli::OutputFormat;
use crate::config::OutputConfig;

/// Execute a CQL script file containing multiple statements
///
/// This function parses a CQL script file and executes each statement
/// sequentially against the provided database. If any statement fails,
/// execution stops immediately and an error is returned.
///
/// # Arguments
///
/// * `file_path` - Path to the CQL script file
/// * `database` - Database instance to execute statements against
/// * `output_config` - Output configuration (color, pagination, etc.)
/// * `format` - Output format (table, json, csv, yaml)
///
/// # Returns
///
/// * `Ok(())` - Script executed successfully
/// * `Err(_)` - Error occurred during script parsing or execution
///
/// # Exit Code
///
/// On query execution errors, this function prints the error and exits
/// with code 5 (as per M2_CLI_SPEC.md line 340).
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use cqlite_core::Database;
/// use cqlite_cli::config::OutputConfig;
/// use cqlite_cli::cli::OutputFormat;
/// use cqlite_cli::script_executor::execute_script_file;
///
/// # async fn example() -> anyhow::Result<()> {
/// let db = Database::open("test.db", Default::default()).await?;
/// let config = OutputConfig::default();
///
/// execute_script_file(
///     Path::new("script.cql"),
///     &db,
///     &config,
///     OutputFormat::Table,
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "state_machine")]
pub async fn execute_script_file(
    file_path: &Path,
    database: &Database,
    output_config: &OutputConfig,
    format: OutputFormat,
) -> Result<()> {
    // Parse the script file
    let statements = load_script(file_path)
        .with_context(|| format!("Failed to parse script file: {}", file_path.display()))?;

    if statements.is_empty() {
        eprintln!("Warning: Script file contains no statements");
        return Ok(());
    }

    println!(
        "Executing {} statement(s) from {}",
        statements.len(),
        file_path.display()
    );

    // Execute each statement sequentially
    for (index, statement) in statements.iter().enumerate() {
        let statement_num = index + 1;

        // Execute the statement using the existing execute_query function
        if let Err(e) = crate::commands::execute_query(
            database,
            statement,
            false, // explain
            false, // timing
            format.clone(),
            output_config,
        )
        .await
        {
            // Print error with statement context
            eprintln!(
                "Error executing statement {} in {}",
                statement_num,
                file_path.display()
            );
            eprintln!("Statement: {}", statement);
            eprintln!("Error: {}", e);

            // Exit with code 5 as per M2_CLI_SPEC.md line 340
            std::process::exit(5);
        }
    }

    println!("\nSuccessfully executed {} statement(s)", statements.len());
    Ok(())
}

/// Stub for execute_script_file when state_machine feature is disabled
#[cfg(not(feature = "state_machine"))]
pub async fn execute_script_file(
    _file_path: &Path,
    _database: &Database,
    _output_config: &OutputConfig,
    _format: OutputFormat,
) -> Result<()> {
    anyhow::bail!(
        "Script execution is not available in M1.\n\
         Build with --features state_machine to enable this feature.\n\
         See CLAUDE.md for M1 API examples."
    )
}

/// Parse a CQL script into individual statements
///
/// Handles:
/// - Semicolon-terminated statements
/// - Line comments (-- comment)
/// - Block comments (/* comment */)
/// - String literals with both single and double quotes
/// - Escaped quotes (doubled quotes like '' or "")
/// - Strings with embedded semicolons
/// - Multi-line statements
/// - Blank lines and whitespace
///
/// Returns a vector of trimmed statement strings (without trailing semicolons)
///
/// # Errors
///
/// Returns an error if:
/// - An unterminated statement is found (missing semicolon)
/// - An unterminated string literal is found
/// - An unterminated block comment is found
pub fn parse_script(script_content: &str) -> Result<Vec<String>> {
    let mut statements = Vec::new();
    let mut current_statement = String::new();
    let mut chars = script_content.chars().peekable();

    let mut in_string = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut string_delimiter = '\0';

    while let Some(ch) = chars.next() {
        // Handle line comments
        if !in_string && !in_block_comment && ch == '-' {
            if chars.peek() == Some(&'-') {
                chars.next(); // consume second '-'
                in_line_comment = true;
                continue;
            }
        }

        // End line comment at newline
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                current_statement.push(ch);
            }
            continue;
        }

        // Handle block comments
        if !in_string && !in_line_comment && ch == '/' {
            if chars.peek() == Some(&'*') {
                chars.next(); // consume '*'
                in_block_comment = true;
                continue;
            }
        }

        // End block comment
        if in_block_comment {
            if ch == '*' && chars.peek() == Some(&'/') {
                chars.next(); // consume '/'
                in_block_comment = false;
            }
            continue;
        }

        // Handle string literals
        if !in_block_comment && !in_line_comment {
            if ch == '\'' || ch == '"' {
                if !in_string {
                    in_string = true;
                    string_delimiter = ch;
                    current_statement.push(ch);
                } else if ch == string_delimiter {
                    // Check for escaped quote (doubled quote)
                    if chars.peek() == Some(&ch) {
                        current_statement.push(ch);
                        current_statement.push(chars.next().unwrap());
                    } else {
                        in_string = false;
                        current_statement.push(ch);
                    }
                } else {
                    current_statement.push(ch);
                }
                continue;
            }
        }

        // Handle statement terminator (semicolon)
        if !in_string && !in_line_comment && !in_block_comment && ch == ';' {
            let trimmed = current_statement.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current_statement.clear();
            continue;
        }

        // Regular character - add to current statement
        if !in_line_comment && !in_block_comment {
            current_statement.push(ch);
        }
    }

    // Check for unterminated constructs
    if in_string {
        anyhow::bail!("Unterminated string literal in script");
    }

    if in_block_comment {
        anyhow::bail!("Unterminated block comment in script");
    }

    // Check for unterminated statement
    let remaining = current_statement.trim();
    if !remaining.is_empty() {
        anyhow::bail!(
            "Unterminated statement found (missing semicolon): {}",
            if remaining.len() > 50 {
                format!("{}...", &remaining[..50])
            } else {
                remaining.to_string()
            }
        );
    }

    Ok(statements)
}

/// Load and parse a CQL script file
pub fn load_script(script_path: &Path) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(script_path)
        .with_context(|| format!("Failed to read script file: {}", script_path.display()))?;

    parse_script(&content)
        .with_context(|| format!("Failed to parse script file: {}", script_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_file() {
        let result = parse_script("");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_parse_single_statement() {
        let content = "SELECT * FROM users;";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT * FROM users");
    }

    #[test]
    fn test_parse_multiple_statements() {
        let content = "SELECT * FROM users;\nINSERT INTO users VALUES (1, 'test');";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SELECT * FROM users");
        assert_eq!(statements[1], "INSERT INTO users VALUES (1, 'test')");
    }

    #[test]
    fn test_parse_line_comments() {
        let content = "-- This is a comment\nSELECT * FROM users; -- inline comment";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT * FROM users");
    }

    #[test]
    fn test_parse_block_comments() {
        let content = "/* block comment */ SELECT * FROM users; /* another comment */";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT * FROM users");
    }

    #[test]
    fn test_parse_multiline_block_comment() {
        let content = "/*\n * Multi-line\n * block comment\n */\nSELECT * FROM users;";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT * FROM users");
    }

    #[test]
    fn test_parse_string_with_semicolon() {
        let content = "INSERT INTO users VALUES (1, 'test;value');";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "INSERT INTO users VALUES (1, 'test;value')");
    }

    #[test]
    fn test_parse_double_quoted_string() {
        let content = r#"INSERT INTO users VALUES (1, "test;value");"#;
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0],
            r#"INSERT INTO users VALUES (1, "test;value")"#
        );
    }

    #[test]
    fn test_parse_escaped_single_quotes() {
        let content = "INSERT INTO users VALUES (1, 'test''s value');";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0],
            "INSERT INTO users VALUES (1, 'test''s value')"
        );
    }

    #[test]
    fn test_parse_escaped_double_quotes() {
        let content = r#"INSERT INTO users VALUES (1, "test""s value");"#;
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0],
            r#"INSERT INTO users VALUES (1, "test""s value")"#
        );
    }

    #[test]
    fn test_parse_multiline_statement() {
        let content = "SELECT *\nFROM users\nWHERE id = 1;";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT *\nFROM users\nWHERE id = 1");
    }

    #[test]
    fn test_parse_blank_lines() {
        let content = "SELECT * FROM users;\n\n\nINSERT INTO users VALUES (1, 'test');";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn test_parse_comments_only() {
        let content = "-- comment 1\n/* comment 2 */\n-- comment 3";
        let result = parse_script(content);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_parse_unterminated_statement() {
        let content = "SELECT * FROM users";
        let result = parse_script(content);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unterminated statement"));
    }

    #[test]
    fn test_parse_unterminated_string() {
        let content = "INSERT INTO users VALUES (1, 'unterminated;";
        let result = parse_script(content);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unterminated string"));
    }

    #[test]
    fn test_parse_unterminated_block_comment() {
        let content = "/* unterminated comment\nSELECT * FROM users;";
        let result = parse_script(content);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unterminated block comment"));
    }

    #[test]
    fn test_parse_complex_script() {
        let content = r#"
-- Create table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT
);

/* Insert test data */
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');

-- Query with string containing semicolon
SELECT * FROM users WHERE email = 'test;email@example.com';
"#;
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 4);
    }

    #[test]
    fn test_parse_empty_statements() {
        let content = ";;; SELECT * FROM users; ;;;";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT * FROM users");
    }

    #[test]
    fn test_parse_mixed_quotes() {
        let content = r#"INSERT INTO users VALUES (1, 'single', "double");"#;
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_parse_comment_in_string() {
        let content = "INSERT INTO users VALUES (1, 'value with -- comment inside');";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0],
            "INSERT INTO users VALUES (1, 'value with -- comment inside')"
        );
    }

    #[test]
    fn test_parse_block_comment_in_string() {
        let content = "INSERT INTO users VALUES (1, 'value with /* comment */ inside');";
        let result = parse_script(content);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0],
            "INSERT INTO users VALUES (1, 'value with /* comment */ inside')"
        );
    }
}
