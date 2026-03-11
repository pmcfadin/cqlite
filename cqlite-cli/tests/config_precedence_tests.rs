//! Integration tests for config precedence validation
//!
//! Tests verify precedence order: flags > env > file > defaults
//! Uses real Config::load with actual CLI parsing

// EMERGENCY M1 FIX: Allow clippy warnings
#![allow(clippy::all)]

use cqlite_cli::cli_types::{Cli, OutputMode};
use cqlite_cli::config::Config;
use serial_test::serial;
use std::env;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper to create a temporary config file
fn create_temp_config(content: &str) -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("test_config.toml");
    fs::write(&config_path, content).unwrap();
    (temp_dir, config_path)
}

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
        writable: false,
        write_dir: None,
        mutation: Vec::new(),
        mutations_file: None,
        flush: false,
        command: None,
    }
}

#[test]
#[serial]
fn test_defaults_when_no_config() {
    // No config file, no env vars, no flags
    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);

    let config = Config::load(None, &cli).unwrap();

    // Should use defaults
    assert_eq!(config.schema_paths, Vec::<PathBuf>::new());
    assert_eq!(config.data_directory, None);
    assert_eq!(config.execution_query, None);
    assert_eq!(config.execution_file, None);
    assert_eq!(config.output_mode, None);
    assert_eq!(config.query_limit, None);
    assert_eq!(config.repl.page_size, 50); // Default from ReplConfig
    assert_eq!(config.no_color, false);
}

#[test]
#[serial]
fn test_config_file_overrides_defaults() {
    let config_content = r#"
        data_directory = "/test/data"
        query_limit = 100

        [repl]
        enable_history = true
        enable_completion = true
        enable_colors = true
        show_timing = false
        page_size = 25
        enable_paging = true
        max_history_size = 1000
        prompt = "cqlite> "
        prompt_continuation = "    -> "

        [output]
        colors = false
        timestamp_format = "%Y-%m-%d %H:%M:%S"
    "#;

    let (_temp_dir, config_path) = create_temp_config(config_content);
    let cli = create_cli_with_flags(
        Some(config_path.clone()),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    );

    let config = Config::load(Some(config_path), &cli).unwrap();

    // Config file values should override defaults
    assert_eq!(config.data_directory, Some(PathBuf::from("/test/data")));
    assert_eq!(config.query_limit, Some(100));
    assert_eq!(config.repl.page_size, 25);
    assert_eq!(config.output.colors, false);
}

#[test]
#[serial]
fn test_env_vars_override_config_file() {
    let config_content = r#"
        data_directory = "/config/data"
        query_limit = 100

        [repl]
        enable_history = true
        enable_completion = true
        enable_colors = true
        show_timing = false
        page_size = 25
        enable_paging = true
        max_history_size = 1000
        prompt = "cqlite> "
        prompt_continuation = "    -> "
    "#;

    let (_temp_dir, config_path) = create_temp_config(config_content);

    // Set environment variables
    env::set_var("CQLITE_DATA_DIR", "/env/data");
    env::set_var("CQLITE_LIMIT", "200");
    env::set_var("CQLITE_PAGE_SIZE", "75");

    let cli = create_cli_with_flags(
        Some(config_path.clone()),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    );

    let config = Config::load(Some(config_path), &cli).unwrap();

    // Env vars should override config file
    assert_eq!(config.data_directory, Some(PathBuf::from("/env/data")));
    assert_eq!(config.query_limit, Some(200));
    assert_eq!(config.repl.page_size, 75);

    // Cleanup
    env::remove_var("CQLITE_DATA_DIR");
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
}

#[test]
#[serial]
fn test_flags_override_env_vars() {
    let config_content = r#"
        data_directory = "/config/data"
        query_limit = 100
    "#;

    let (_temp_dir, config_path) = create_temp_config(config_content);

    // Set environment variables
    env::set_var("CQLITE_DATA_DIR", "/env/data");
    env::set_var("CQLITE_LIMIT", "200");
    env::set_var("CQLITE_PAGE_SIZE", "75");
    env::set_var("CQLITE_NO_COLOR", "true");

    // Create CLI with flags
    let cli = create_cli_with_flags(
        Some(config_path.clone()),
        Some(PathBuf::from("/flag/schema.cql")),
        Some(PathBuf::from("/flag/data")),
        Some("SELECT * FROM test".to_string()),
        None,
        Some(OutputMode::Json),
        Some(50),
        Some(100),
        true,
    );

    let config = Config::load(Some(config_path), &cli).unwrap();

    // Flags should override everything
    assert_eq!(config.data_directory, Some(PathBuf::from("/flag/data")));
    assert_eq!(config.schema_paths, vec![PathBuf::from("/flag/schema.cql")]);
    assert_eq!(
        config.execution_query,
        Some("SELECT * FROM test".to_string())
    );
    assert_eq!(config.output_mode, Some("json".to_string()));
    assert_eq!(config.query_limit, Some(50));
    assert_eq!(config.repl.page_size, 100);
    assert_eq!(config.no_color, true);
    assert_eq!(config.output.colors, false); // Should be synced with no_color

    // Cleanup
    env::remove_var("CQLITE_DATA_DIR");
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
    env::remove_var("CQLITE_NO_COLOR");
}

#[test]
#[serial]
fn test_schema_env_var_comma_separated() {
    env::set_var(
        "CQLITE_SCHEMA",
        "/path/one.cql,/path/two.json, /path/three.cql",
    );

    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);

    let config = Config::load(None, &cli).unwrap();

    // Should parse comma-separated paths
    assert_eq!(
        config.schema_paths,
        vec![
            PathBuf::from("/path/one.cql"),
            PathBuf::from("/path/two.json"),
            PathBuf::from("/path/three.cql"),
        ]
    );

    // Cleanup
    env::remove_var("CQLITE_SCHEMA");
}

#[test]
#[serial]
fn test_no_color_env_var_parsing() {
    // Test "true" value
    env::set_var("CQLITE_NO_COLOR", "true");
    let cli = create_cli_with_flags(None, None, None, None, None, None, None, None, false);
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, true);
    assert_eq!(config.output.colors, false);

    // Test "1" value
    env::set_var("CQLITE_NO_COLOR", "1");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, true);

    // Test "yes" value
    env::set_var("CQLITE_NO_COLOR", "yes");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, true);

    // Test "false" value (should not set no_color)
    env::set_var("CQLITE_NO_COLOR", "false");
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.no_color, false);

    // Cleanup
    env::remove_var("CQLITE_NO_COLOR");
}

#[test]
#[serial]
fn test_complete_precedence_chain() {
    // Setup: Create config file with base values
    let config_content = r#"
        data_directory = "/config/data"
        query_limit = 100
        output_mode = "table"

        [repl]
        enable_history = true
        enable_completion = true
        enable_colors = true
        show_timing = false
        page_size = 25
        enable_paging = true
        max_history_size = 1000
        prompt = "cqlite> "
        prompt_continuation = "    -> "
    "#;

    let (_temp_dir, config_path) = create_temp_config(config_content);

    // Setup: Set env vars (should override config)
    env::set_var("CQLITE_DATA_DIR", "/env/data");
    env::set_var("CQLITE_LIMIT", "200");
    // Don't set PAGE_SIZE - should use config value

    // Setup: Create CLI with some flags (should override env and config)
    let cli = create_cli_with_flags(
        Some(config_path.clone()),
        None,                              // No schema flag
        Some(PathBuf::from("/flag/data")), // Override data_dir
        None,
        None,
        None,      // No out flag - should use config
        None,      // No limit flag - should use env var
        Some(150), // Override page_size
        false,
    );

    let config = Config::load(Some(config_path), &cli).unwrap();

    // Verify precedence chain:
    // - data_directory: flag wins ("/flag/data")
    assert_eq!(config.data_directory, Some(PathBuf::from("/flag/data")));

    // - query_limit: env var wins (200) because no flag set
    assert_eq!(config.query_limit, Some(200));

    // - page_size: flag wins (150)
    assert_eq!(config.repl.page_size, 150);

    // - output_mode: config wins ("table") because no env or flag
    assert_eq!(config.output_mode, Some("table".to_string()));

    // Cleanup
    env::remove_var("CQLITE_DATA_DIR");
    env::remove_var("CQLITE_LIMIT");
}
