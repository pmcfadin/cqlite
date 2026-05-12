//! Integration tests for unsupported query error handling (Issue #131)
//!
//! Tests verify that unsupported SELECT queries produce:
//! - Exit code 5 (query execution error)
//! - Helpful error messages with specific guidance
//! - Consistent UX per M2_CLI_SPEC.md

use anyhow::anyhow;
use cqlite_cli::error::{classify_error, get_error_hint, print_error, CliExitCode};

#[test]
fn test_unsupported_join_query() {
    // Simulate error from query engine for JOIN (not supported in Cassandra CQL)
    let err = anyhow!("Unsupported query: JOIN operations are not supported in Cassandra CQL");

    let exit_code = classify_error(&err);
    assert_eq!(exit_code, CliExitCode::QueryExecutionError);
    assert_eq!(exit_code.as_i32(), 5);

    let hint = get_error_hint(&err, exit_code);
    assert!(hint.contains("Supported SELECT features"));
    assert!(hint.contains("WHERE on partition/primary key"));
}

#[test]
fn test_unsupported_subquery() {
    let err = anyhow!("Unsupported query: Subqueries are not supported");

    let exit_code = classify_error(&err);
    assert_eq!(exit_code.as_i32(), 5);

    let hint = get_error_hint(&err, exit_code);
    assert!(hint.contains("Not supported"));
    assert!(hint.contains("subqueries"));
}

#[test]
fn test_unsupported_advanced_aggregation() {
    let err =
        anyhow!("Query feature not supported: Advanced aggregation functions require GROUP BY");

    let exit_code = classify_error(&err);
    assert_eq!(exit_code, CliExitCode::QueryExecutionError);

    let hint = get_error_hint(&err, exit_code);
    assert!(hint.contains("Examples:"));
    assert!(hint.contains("LIMIT"));
}

#[test]
fn test_hint_contains_examples() {
    let err = anyhow!("Unsupported query: Complex WHERE clause not supported");
    let exit_code = classify_error(&err);
    let hint = get_error_hint(&err, exit_code);

    // Verify hint contains concrete examples
    assert!(hint.contains("SELECT * FROM users WHERE id = ?"));
    assert!(hint.contains("DESCRIBE TABLE"));
    assert!(hint.contains("USE my_keyspace"));
}

#[test]
fn test_hint_contains_documentation_link() {
    let err = anyhow!("Unsupported query feature");
    let exit_code = classify_error(&err);
    let hint = get_error_hint(&err, exit_code);

    // Verify hint points to documentation
    assert!(hint.contains("CLI_USAGE_EXAMPLES.md"));
}

#[test]
fn test_supported_features_listed() {
    let err = anyhow!("Unsupported query: Feature X not available");
    let exit_code = classify_error(&err);
    let hint = get_error_hint(&err, exit_code);

    // Verify all supported features are mentioned
    assert!(hint.contains("SELECT with WHERE"));
    assert!(hint.contains("LIMIT"));
    assert!(hint.contains("DESCRIBE"));
    assert!(hint.contains("USE"));
}

#[test]
fn test_regular_query_error_different_hint() {
    // Non-unsupported query errors should get standard hint
    let err = anyhow!("Query execution failed: No data found");
    let exit_code = classify_error(&err);
    let hint = get_error_hint(&err, exit_code);

    // Should NOT get the unsupported query hint
    assert!(!hint.contains("Supported SELECT features"));
    assert_eq!(
        hint,
        "Check query syntax and ensure required data is available"
    );
}

#[test]
fn test_error_message_format() {
    // Test that print_error produces well-formatted output
    let err = anyhow!("Unsupported query: Window functions not supported");
    let exit_code = classify_error(&err);

    // This test primarily verifies compilation and structure
    // Actual output testing would require capturing stderr
    print_error(&err, exit_code);

    // Verify hint structure
    let hint = get_error_hint(&err, exit_code);
    assert!(hint.lines().count() > 5); // Multi-line hint
    assert!(hint.contains("•")); // Uses bullet points
}

#[test]
fn test_exit_code_consistency() {
    // All unsupported query errors should return exit code 5
    let test_cases = vec![
        "Unsupported query: JOIN",
        "Query feature not supported: UNION",
        "Unsupported query: Nested SELECT",
        "Not supported: HAVING clause without GROUP BY",
    ];

    for case in test_cases {
        let err = anyhow!(case);
        let exit_code = classify_error(&err);
        assert_eq!(exit_code.as_i32(), 5, "Expected exit code 5 for: {case}");
    }
}

#[test]
fn test_hint_readability() {
    let err = anyhow!("Unsupported query detected");
    let exit_code = classify_error(&err);
    let hint = get_error_hint(&err, exit_code);

    // Verify hint is well-structured and readable
    assert!(hint.contains("\n")); // Multi-line
    assert!(hint.contains(":")); // Has section headers
    assert!(hint.len() > 100); // Substantial content
    assert!(hint.len() < 1000); // Not overwhelming
}

#[test]
fn test_unsupported_not_supported_pattern() {
    // Test both "unsupported" and "not supported" patterns are detected
    let test_cases = vec![
        ("Unsupported query: GROUP BY", true),
        ("Feature not supported: HAVING", true),
        ("Query execution failed: syntax error", false),
        ("Invalid column name", false),
    ];

    for (error_msg, should_get_unsupported_hint) in test_cases {
        let err = anyhow!(error_msg);
        let exit_code = classify_error(&err);
        let hint = get_error_hint(&err, exit_code);

        if should_get_unsupported_hint {
            assert!(
                hint.contains("Supported SELECT features"),
                "Expected unsupported hint for: {error_msg}"
            );
        } else {
            assert!(
                !hint.contains("Supported SELECT features"),
                "Should not get unsupported hint for: {error_msg}"
            );
        }
    }
}

#[test]
fn test_hint_shows_all_m2_features() {
    let err = anyhow!("Unsupported query");
    let exit_code = classify_error(&err);
    let hint = get_error_hint(&err, exit_code);

    // Verify M2_CLI_SPEC.md features are all listed
    let required_features = vec![
        "SELECT with WHERE",
        "LIMIT",
        "DESCRIBE",
        "USE",
        "partition/primary key",
    ];

    for feature in required_features {
        assert!(
            hint.contains(feature),
            "Hint should mention '{feature}' but doesn't:\n{hint}"
        );
    }
}

#[test]
fn test_hint_shows_unsupported_features() {
    let err = anyhow!("Unsupported query: advanced feature");
    let exit_code = classify_error(&err);
    let hint = get_error_hint(&err, exit_code);

    // Verify unsupported features are clearly listed
    let unsupported_features = vec!["JOIN", "subqueries", "advanced aggregations"];

    for feature in unsupported_features {
        assert!(
            hint.contains(feature),
            "Hint should mention unsupported '{feature}' but doesn't:\n{hint}"
        );
    }
}

#[test]
fn test_exit_code_5_for_all_query_errors() {
    // Verify that query execution errors (both supported and unsupported) get exit code 5
    let test_cases = vec![
        "Unsupported query: feature X",
        "Query execution failed",
        "Syntax error in SELECT",
        "Not supported: operation Y",
    ];

    for case in test_cases {
        let err = anyhow!(case);
        let exit_code = classify_error(&err);
        assert_eq!(
            exit_code,
            CliExitCode::QueryExecutionError,
            "All query errors should be QueryExecutionError"
        );
        assert_eq!(exit_code.as_i32(), 5, "Exit code should be 5 for: {case}");
    }
}

#[test]
fn test_case_insensitive_detection() {
    // Error detection should be case-insensitive
    let test_cases = vec![
        "UNSUPPORTED QUERY: JOIN",
        "unsupported query: join",
        "Unsupported Query: Join",
        "NOT SUPPORTED: FEATURE",
        "not supported: feature",
    ];

    for case in test_cases {
        let err = anyhow!(case);
        let exit_code = classify_error(&err);
        let hint = get_error_hint(&err, exit_code);

        assert!(
            hint.contains("Supported SELECT features"),
            "Case-insensitive detection failed for: {case}"
        );
    }
}

#[test]
fn test_hint_structure_consistency() {
    let err = anyhow!("Unsupported query");
    let exit_code = classify_error(&err);
    let hint = get_error_hint(&err, exit_code);

    // Verify hint has consistent structure
    assert!(hint.contains("Supported SELECT features in M2:"));
    assert!(hint.contains("Examples:"));
    assert!(hint.contains("Not supported:"));
    assert!(hint.contains("See:"));

    // Verify bullet points are used
    let bullet_count = hint.matches("•").count();
    assert!(bullet_count >= 4, "Should have at least 4 bullet points");
}
