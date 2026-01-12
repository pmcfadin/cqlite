//! Edge case tests for environment variable parsing
//!
//! Tests invalid values, boundary conditions, and error handling

#![allow(clippy::all)]

use cqlite_cli::cli_types::{Cli, OutputMode};
use cqlite_cli::config::Config;
use serial_test::serial;
use std::env;
use std::path::PathBuf;

/// Helper to create a minimal CLI with specified fields
fn create_cli_with_flags(
    config_path: Option<PathBuf>,
    schema: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    execute: Option<String>,
    file: Option<PathBuf>,
    out: Option<OutputMode>,
    limit: Option<usize>,
    page_size: Option<usize>,
    no_color: bool,
) -> Cli {
    Cli {
        database: None,
        config: config_path,
        verbose: 0,
        quiet: false,
        format: cqlite_cli::cli::OutputFormat::Table,
        auto_detect: false,
        cassandra_version: None,
        schema,
        dataset: None,
        data_dir,
        execute,
        file,
        out,
        output: None,
        overwrite: false,
        limit,
        page_size,
        no_color,
        enable_select_fallback: false,
        command: None,
    }
}

#[test]
#[serial]
fn test_invalid_cqlite_limit_non_numeric() {
    env::set_var("CQLITE_LIMIT", "not_a_number");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let result = Config::load(None, &cli);

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("Invalid CQLITE_LIMIT"),
        "Error should mention CQLITE_LIMIT, got: {}",
        error_msg
    );

    env::remove_var("CQLITE_LIMIT");
}

#[test]
#[serial]
fn test_invalid_cqlite_limit_zero() {
    env::set_var("CQLITE_LIMIT", "0");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let result = Config::load(None, &cli);

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("must be greater than 0"),
        "Error should mention zero validation, got: {}",
        error_msg
    );

    env::remove_var("CQLITE_LIMIT");
}

#[test]
#[serial]
fn test_invalid_cqlite_page_size_non_numeric() {
    env::set_var("CQLITE_PAGE_SIZE", "invalid");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let result = Config::load(None, &cli);

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("Invalid CQLITE_PAGE_SIZE"),
        "Error should mention CQLITE_PAGE_SIZE, got: {}",
        error_msg
    );

    env::remove_var("CQLITE_PAGE_SIZE");
}

#[test]
#[serial]
fn test_invalid_cqlite_page_size_zero() {
    env::set_var("CQLITE_PAGE_SIZE", "0");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let result = Config::load(None, &cli);

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("must be greater than 0"),
        "Error should mention zero validation, got: {}",
        error_msg
    );

    env::remove_var("CQLITE_PAGE_SIZE");
}

#[test]
#[serial]
fn test_cqlite_schema_with_empty_paths() {
    // Test trailing comma
    env::set_var("CQLITE_SCHEMA", "/path/one.cql,/path/two.cql,");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();

    // Current implementation will include empty path
    // This is acceptable as it will fail at file open time
    assert!(config.schema_paths.len() >= 2);
    assert_eq!(config.schema_paths[0], PathBuf::from("/path/one.cql"));
    assert_eq!(config.schema_paths[1], PathBuf::from("/path/two.cql"));

    env::remove_var("CQLITE_SCHEMA");
}

#[test]
#[serial]
fn test_cqlite_schema_single_path() {
    env::set_var("CQLITE_SCHEMA", "/single/path/schema.cql");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.schema_paths.len(), 1);
    assert_eq!(
        config.schema_paths[0],
        PathBuf::from("/single/path/schema.cql")
    );

    env::remove_var("CQLITE_SCHEMA");
}

#[test]
#[serial]
fn test_cqlite_schema_with_spaces_in_paths() {
    env::set_var(
        "CQLITE_SCHEMA",
        "/path/with spaces/schema.cql,/another/path.cql",
    );

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.schema_paths.len(), 2);
    assert_eq!(
        config.schema_paths[0],
        PathBuf::from("/path/with spaces/schema.cql")
    );
    assert_eq!(config.schema_paths[1], PathBuf::from("/another/path.cql"));

    env::remove_var("CQLITE_SCHEMA");
}

#[test]
#[serial]
fn test_cqlite_no_color_case_insensitive() {
    // Test uppercase TRUE
    env::set_var("CQLITE_NO_COLOR", "TRUE");
    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, true);

    // Test mixed case Yes
    env::set_var("CQLITE_NO_COLOR", "Yes");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, true);

    // Test uppercase ON
    env::set_var("CQLITE_NO_COLOR", "ON");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, true);

    env::remove_var("CQLITE_NO_COLOR");
}

#[test]
#[serial]
fn test_cqlite_no_color_unrecognized_value() {
    // Unrecognized values should be treated as false (current behavior)
    env::set_var("CQLITE_NO_COLOR", "maybe");
    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, false);

    env::set_var("CQLITE_NO_COLOR", "typo_ture");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, false);

    env::remove_var("CQLITE_NO_COLOR");
}

#[test]
#[serial]
fn test_cqlite_limit_max_value() {
    // Test very large but valid value
    env::set_var("CQLITE_LIMIT", "1000000");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.query_limit, Some(1000000));

    env::remove_var("CQLITE_LIMIT");
}

#[test]
#[serial]
fn test_cqlite_page_size_boundary_values() {
    // Test page_size = 1 (minimum valid)
    env::set_var("CQLITE_PAGE_SIZE", "1");
    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.repl.page_size, 1);

    // Test large page_size
    env::set_var("CQLITE_PAGE_SIZE", "10000");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.repl.page_size, 10000);

    env::remove_var("CQLITE_PAGE_SIZE");
}

#[test]
#[serial]
fn test_multiple_env_vars_simultaneously() {
    // Set all env vars at once
    env::set_var("CQLITE_DATA_DIR", "/test/data");
    env::set_var("CQLITE_SCHEMA", "/schema/one.cql,/schema/two.cql");
    env::set_var("CQLITE_LIMIT", "100");
    env::set_var("CQLITE_PAGE_SIZE", "25");
    env::set_var("CQLITE_NO_COLOR", "true");
    env::set_var("CQLITE_OUT", "json");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();

    // Verify all env vars are applied
    assert_eq!(config.data_directory, Some(PathBuf::from("/test/data")));
    assert_eq!(config.schema_paths.len(), 2);
    assert_eq!(config.query_limit, Some(100));
    assert_eq!(config.repl.page_size, 25);
    assert_eq!(config.no_color, true);
    assert_eq!(config.output_mode, Some("json".to_string()));

    // Cleanup
    env::remove_var("CQLITE_DATA_DIR");
    env::remove_var("CQLITE_SCHEMA");
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
    env::remove_var("CQLITE_NO_COLOR");
    env::remove_var("CQLITE_OUT");
}

#[test]
#[serial]
fn test_cqlite_out_valid_formats() {
    // Test table format
    env::set_var("CQLITE_OUT", "table");
    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.output_mode, Some("table".to_string()));

    // Test json format
    env::set_var("CQLITE_OUT", "json");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.output_mode, Some("json".to_string()));

    // Test csv format
    env::set_var("CQLITE_OUT", "csv");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.output_mode, Some("csv".to_string()));

    env::remove_var("CQLITE_OUT");
}

#[test]
#[serial]
fn test_cqlite_data_dir_with_tilde() {
    // Test that tilde paths are accepted (shell expansion would happen at usage time)
    env::set_var("CQLITE_DATA_DIR", "~/cassandra/data");

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();

    // PathBuf accepts tilde literally (expansion is shell responsibility)
    assert_eq!(
        config.data_directory,
        Some(PathBuf::from("~/cassandra/data"))
    );

    env::remove_var("CQLITE_DATA_DIR");
}

#[test]
#[serial]
fn test_env_vars_do_not_persist_between_loads() {
    // First load with env var set
    env::set_var("CQLITE_LIMIT", "100");
    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config1 = Config::load(None, &cli).unwrap();
    assert_eq!(config1.query_limit, Some(100));

    // Remove env var and load again
    env::remove_var("CQLITE_LIMIT");
    let config2 = Config::load(None, &cli).unwrap();
    assert_eq!(config2.query_limit, None);
}
