use clap::Parser;
use cqlite_cli::cli_types::Cli;
use cqlite_cli::config::Config;
use serial_test::serial;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
#[serial]
fn test_project_config_discovery() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create .cqlite.toml in project
    let toml_content = r#"
        data_directory = "/project/data"
        query_limit = 100
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.data_directory, Some(PathBuf::from("/project/data")));
    assert_eq!(config.query_limit, Some(100));
}

#[test]
#[serial]
fn test_explicit_config_overrides_discovered() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create project config
    fs::write("./.cqlite.toml", "query_limit = 100").unwrap();

    // Create explicit config with different value
    let explicit_path = temp_dir.path().join("my.toml");
    fs::write(&explicit_path, "query_limit = 200").unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(Some(explicit_path), &cli).unwrap();

    assert_eq!(config.query_limit, Some(200)); // Explicit wins
}

#[test]
#[serial]
fn test_env_overrides_file_config() {
    std::env::set_var("CQLITE_LIMIT", "300");

    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();
    fs::write("./.cqlite.toml", "query_limit = 100").unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.query_limit, Some(300)); // Env wins

    std::env::remove_var("CQLITE_LIMIT");
}

#[test]
#[serial]
fn test_cli_flag_highest_precedence() {
    std::env::set_var("CQLITE_LIMIT", "300");

    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();
    fs::write("./.cqlite.toml", "query_limit = 100").unwrap();

    let cli = Cli::parse_from(["cqlite", "--limit", "500"]);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.query_limit, Some(500)); // CLI wins

    std::env::remove_var("CQLITE_LIMIT");
}

#[test]
#[serial]
fn test_project_config_overrides_defaults() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create .cqlite.toml with custom values
    // Note: Due to how TOML works with nested structs, we only set top-level fields
    // or fully specify nested structs
    let toml_content = r#"
        query_limit = 50
        no_color = true
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.query_limit, Some(50));
    assert!(config.no_color);
    // output.colors gets set from no_color flag
    assert!(!config.output.colors);
}

#[test]
#[serial]
fn test_project_config_not_found_uses_defaults() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // No .cqlite.toml file created

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    // Should use defaults
    assert_eq!(config.query_limit, None);
    assert!(!config.no_color);
    assert!(config.output.colors); // Default is true
}

#[test]
#[serial]
fn test_explicit_config_file_not_found_errors() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    let nonexistent_path = temp_dir.path().join("nonexistent.toml");

    let cli = Cli::parse_from(["cqlite"]);
    let result = Config::load(Some(nonexistent_path), &cli);

    // Should error when explicit config is missing
    assert!(result.is_err());
}

#[test]
#[serial]
fn test_invalid_toml_in_project_config_errors() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create invalid TOML
    fs::write("./.cqlite.toml", "invalid toml { content").unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let result = Config::load(None, &cli);

    // Should error on invalid TOML
    assert!(result.is_err());
}

#[test]
#[serial]
fn test_schema_paths_from_config_file() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create config with schema_paths
    let toml_content = r#"
        schema_paths = ["/path/to/schema1.cql", "/path/to/schema2.cql"]
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.schema_paths.len(), 2);
    assert_eq!(
        config.schema_paths[0],
        PathBuf::from("/path/to/schema1.cql")
    );
    assert_eq!(
        config.schema_paths[1],
        PathBuf::from("/path/to/schema2.cql")
    );
}

#[test]
#[serial]
fn test_output_mode_from_config_file() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create config with output_mode
    let toml_content = r#"
        output_mode = "json"
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.output_mode, Some("json".to_string()));
}

#[test]
#[serial]
fn test_complete_precedence_chain() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create project config
    fs::write("./.cqlite.toml", "query_limit = 100").unwrap();

    // Create explicit config
    let explicit_path = temp_dir.path().join("explicit.toml");
    fs::write(&explicit_path, "query_limit = 200").unwrap();

    // Test 1: Project config only (no env, no explicit, no CLI)
    std::env::remove_var("CQLITE_LIMIT");
    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();
    assert_eq!(config.query_limit, Some(100));

    // Test 2: Explicit config overrides project
    std::env::remove_var("CQLITE_LIMIT");
    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(Some(explicit_path.clone()), &cli).unwrap();
    assert_eq!(config.query_limit, Some(200));

    // Test 3: Env var overrides explicit config
    std::env::set_var("CQLITE_LIMIT", "300");
    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(Some(explicit_path.clone()), &cli).unwrap();
    assert_eq!(config.query_limit, Some(300));

    // Test 4: CLI flag overrides env var
    std::env::set_var("CQLITE_LIMIT", "300");
    let cli = Cli::parse_from(["cqlite", "--limit", "500"]);
    let config = Config::load(Some(explicit_path), &cli).unwrap();
    assert_eq!(config.query_limit, Some(500));

    std::env::remove_var("CQLITE_LIMIT");
}

#[test]
#[serial]
fn test_partial_merge_preserves_unset_fields() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create config with only some fields set
    let toml_content = r#"
        query_limit = 100
        # data_directory not set
        # output_mode not set
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    // Set field should be present
    assert_eq!(config.query_limit, Some(100));

    // Unset fields should have defaults
    assert_eq!(config.data_directory, None);
    assert_eq!(config.output_mode, None);
}

#[test]
#[serial]
fn test_no_color_flag_merging() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create config with no_color = true
    let toml_content = r#"
        no_color = true
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    assert!(config.no_color);
    assert!(!config.output.colors);
}

#[test]
#[serial]
fn test_cassandra_version_from_config() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create config with cassandra_version
    // Note: cassandra_version is marked with #[serde(skip)] so it won't be loaded from file
    // This test verifies that behavior
    let toml_content = r#"
        query_limit = 100
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite", "--cassandra-version", "5.0"]);
    let config = Config::load(None, &cli).unwrap();

    // cassandra_version should come from CLI only (it's marked with #[serde(skip)])
    assert_eq!(config.cassandra_version, Some("5.0".to_string()));
}

#[test]
#[serial]
fn test_default_keyspace_from_config() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create config with default_keyspace
    let toml_content = r#"
        default_keyspace = "my_keyspace"
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.default_keyspace, Some("my_keyspace".to_string()));
}

#[test]
#[serial]
fn test_nested_config_structures() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Create config with nested structures
    // Note: Nested structs need all fields when specified in TOML
    let toml_content = r#"
        [connection]
        timeout_ms = 60000
        retry_attempts = 5
        pool_size = 20

        [output]
        max_rows = 500
        colors = false
        timestamp_format = "%Y-%m-%d"

        [repl]
        enable_history = true
        enable_completion = true
        enable_colors = true
        show_timing = true
        page_size = 100
        enable_paging = true
        max_history_size = 1000
        prompt = "cqlite> "
        prompt_continuation = "    -> "
    "#;
    fs::write("./.cqlite.toml", toml_content).unwrap();

    let cli = Cli::parse_from(["cqlite"]);
    let config = Config::load(None, &cli).unwrap();

    assert_eq!(config.connection.timeout_ms, 60000);
    assert_eq!(config.connection.retry_attempts, 5);
    assert_eq!(config.connection.pool_size, 20);

    assert_eq!(config.output.max_rows, Some(500));
    assert!(!config.output.colors);
    assert_eq!(config.output.timestamp_format, "%Y-%m-%d");

    assert_eq!(config.repl.page_size, 100);
    assert!(config.repl.show_timing);
}
