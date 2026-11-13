//! Unit tests for `config.rs` (moved out for the #1116 file-size ratchet; module path unchanged).

use super::*;
use crate::cli_types::Cli;
use clap::Parser;
use serial_test::serial;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_cassandra_version_flag_passed_to_config() {
    // Parse CLI args with cassandra_version flag
    let cli = Cli::parse_from(&[
        "cqlite",
        "--cassandra-version",
        "5.0",
        "--data-dir",
        "/tmp/data",
    ]);

    // Build config
    let config = Config::load(None, &cli).unwrap();

    // Verify cassandra_version was captured
    assert_eq!(config.cassandra_version, Some("5.0".to_string()));
}

#[tokio::test]
async fn test_version_resolution_user_override() {
    let temp_dir = TempDir::new().unwrap();

    // Create CLI with user override
    let cli = Cli::parse_from(&[
        "cqlite",
        "--cassandra-version",
        "5.0",
        "--data-dir",
        temp_dir.path().to_str().unwrap(),
    ]);

    // Build config
    let mut config = Config::load(None, &cli).unwrap();

    // Initialize platform and resolve version
    let core_config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::Platform::new(&core_config).await.unwrap());

    config.resolve_version(platform).await.unwrap();

    // Verify user override takes precedence
    let version_info = config.version_info().unwrap();
    assert_eq!(
        version_info.source,
        cqlite_core::version_hints::VersionSource::UserFlag
    );
    assert_eq!(version_info.version, Some("5.0".to_string()));
}

#[tokio::test]
async fn test_version_resolution_metadata_yml() {
    let temp_dir = TempDir::new().unwrap();

    // Create metadata.yml with version
    let metadata_content = "cassandra_version: \"4.0\"\nkeyspaces: []\n";
    let metadata_path = temp_dir.path().join("metadata.yml");
    std::fs::write(&metadata_path, metadata_content).unwrap();

    // Create CLI without user override
    let cli = Cli::parse_from(&["cqlite", "--data-dir", temp_dir.path().to_str().unwrap()]);

    // Build config
    let mut config = Config::load(None, &cli).unwrap();

    // Initialize platform and resolve version
    let core_config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::Platform::new(&core_config).await.unwrap());

    config.resolve_version(platform).await.unwrap();

    // Verify metadata.yml was used
    let version_info = config.version_info().unwrap();
    assert_eq!(
        version_info.source,
        cqlite_core::version_hints::VersionSource::DatasetMetadata
    );
    assert_eq!(version_info.version, Some("4.0".to_string()));
}

#[tokio::test]
async fn test_version_resolution_unknown() {
    let temp_dir = TempDir::new().unwrap();

    // Create CLI without version and no metadata.yml
    let cli = Cli::parse_from(&["cqlite", "--data-dir", temp_dir.path().to_str().unwrap()]);

    // Build config
    let mut config = Config::load(None, &cli).unwrap();

    // Initialize platform and resolve version
    let core_config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::Platform::new(&core_config).await.unwrap());

    config.resolve_version(platform).await.unwrap();

    // Verify unknown fallback
    let version_info = config.version_info().unwrap();
    assert_eq!(
        version_info.source,
        cqlite_core::version_hints::VersionSource::Unknown
    );
    assert_eq!(version_info.version, None);
    assert_eq!(version_info.version_or_unknown(), "unknown");
}

#[tokio::test]
async fn test_version_precedence_user_overrides_metadata() {
    let temp_dir = TempDir::new().unwrap();

    // Create metadata.yml with version 4.0
    let metadata_content = "cassandra_version: \"4.0\"\nkeyspaces: []\n";
    let metadata_path = temp_dir.path().join("metadata.yml");
    std::fs::write(&metadata_path, metadata_content).unwrap();

    // Create CLI with user override 5.0
    let cli = Cli::parse_from(&[
        "cqlite",
        "--cassandra-version",
        "5.0",
        "--data-dir",
        temp_dir.path().to_str().unwrap(),
    ]);

    // Build config
    let mut config = Config::load(None, &cli).unwrap();

    // Initialize platform and resolve version
    let core_config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::Platform::new(&core_config).await.unwrap());

    config.resolve_version(platform).await.unwrap();

    // Verify user override takes precedence over metadata.yml
    let version_info = config.version_info().unwrap();
    assert_eq!(
        version_info.source,
        cqlite_core::version_hints::VersionSource::UserFlag
    );
    assert_eq!(version_info.version, Some("5.0".to_string()));
}

#[tokio::test]
async fn test_version_string_helpers() {
    let temp_dir = TempDir::new().unwrap();

    // Create CLI with version
    let cli = Cli::parse_from(&[
        "cqlite",
        "--cassandra-version",
        "5.0",
        "--data-dir",
        temp_dir.path().to_str().unwrap(),
    ]);

    // Build config
    let mut config = Config::load(None, &cli).unwrap();

    // Before resolution
    assert_eq!(config.version_string(), "not resolved");
    assert_eq!(config.version_source(), "not resolved");

    // After resolution
    let core_config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::Platform::new(&core_config).await.unwrap());
    config.resolve_version(platform).await.unwrap();

    assert_eq!(config.version_string(), "5.0");
    assert!(config.version_source().contains("User-provided flag"));
}

#[test]
fn test_config_default_includes_version_fields() {
    let config = Config::default();
    assert_eq!(config.cassandra_version, None);
    assert_eq!(config.resolved_version, None);
}

// Observability config tests (Issue #1033, Epic #1031)

#[test]
fn test_observability_default_maps_to_core_disabled_defaults() {
    // An all-None CLI observability section must yield the core defaults:
    // disabled, localhost:4317, grpc, service "cqlite", full sampling.
    let obs = ObservabilityConfig::default();
    let core = obs.to_core();
    assert!(!core.enabled);
    assert_eq!(
        core.endpoint,
        cqlite_core::observability::config::DEFAULT_ENDPOINT
    );
    assert_eq!(
        core.protocol,
        cqlite_core::observability::OtelProtocol::Grpc
    );
    assert_eq!(
        core.service_name,
        cqlite_core::observability::config::DEFAULT_SERVICE_NAME
    );
    assert_eq!(core.sampling_ratio, 1.0);
}

#[test]
fn test_observability_to_core_applies_set_fields() {
    let obs = ObservabilityConfig {
        enabled: Some(true),
        endpoint: Some("http://collector:4318".to_string()),
        protocol: Some("http".to_string()),
        service_name: Some("svc".to_string()),
        service_version: Some("9.9.9".to_string()),
        sampling_ratio: Some(0.5),
        timeout_ms: Some(2500),
    };
    let core = obs.to_core();
    assert!(core.enabled);
    assert_eq!(core.endpoint, "http://collector:4318");
    assert_eq!(
        core.protocol,
        cqlite_core::observability::OtelProtocol::Http
    );
    assert_eq!(core.service_name, "svc");
    assert_eq!(core.service_version, "9.9.9");
    assert_eq!(core.sampling_ratio, 0.5);
    assert_eq!(core.timeout, std::time::Duration::from_millis(2500));
}

#[test]
fn test_observability_to_core_ignores_unparseable_protocol() {
    // A bad protocol string keeps the core default (grpc) rather than erroring.
    let obs = ObservabilityConfig {
        protocol: Some("carrier-pigeon".to_string()),
        ..Default::default()
    };
    let core = obs.to_core();
    assert_eq!(
        core.protocol,
        cqlite_core::observability::OtelProtocol::Grpc
    );
}

#[test]
#[serial]
fn test_observability_flag_overrides_env_and_file_precedence() {
    use std::env;

    // env sets the endpoint; the --otel-endpoint flag must win.
    env::set_var("CQLITE_OTEL_ENABLED", "true");
    env::set_var("CQLITE_OTEL_ENDPOINT", "http://from-env:4317");

    let cli = Cli::parse_from(&["cqlite", "--otel-endpoint", "http://from-flag:4317"]);
    let config = Config::load(None, &cli).unwrap();

    // clap merged env into otel_enabled (no flag) -> Some(true);
    // explicit --otel-endpoint flag wins over the env value.
    assert_eq!(config.observability.enabled, Some(true));
    assert_eq!(
        config.observability.endpoint.as_deref(),
        Some("http://from-flag:4317")
    );

    env::remove_var("CQLITE_OTEL_ENABLED");
    env::remove_var("CQLITE_OTEL_ENDPOINT");
}

/// A malformed `CQLITE_OTEL_SAMPLING_RATIO` / `CQLITE_OTEL_TIMEOUT_MS` env
/// value must NOT abort `Cli::parse()` and must fall back to defaults,
/// matching `ObservabilityConfig::from_env` semantics (issue #1033). These
/// flags are carried as raw strings and parsed leniently in the config
/// layer, so a bad env value can never break the whole CLI at parse time.
#[test]
#[serial]
fn test_observability_malformed_numeric_env_falls_back_to_defaults() {
    use std::env;

    env::set_var("CQLITE_OTEL_SAMPLING_RATIO", "not-a-number");
    env::set_var("CQLITE_OTEL_TIMEOUT_MS", "xyz");

    // The bad env values are surfaced to clap as raw strings; parse must
    // not error.
    let cli = Cli::parse_from(&["cqlite"]);
    assert_eq!(cli.otel_sampling_ratio.as_deref(), Some("not-a-number"));
    assert_eq!(cli.otel_timeout_ms.as_deref(), Some("xyz"));

    // The config layer parses leniently and keeps the defaults (None ->
    // core defaults) on garbage input rather than propagating an error.
    let config = ConfigBuilder::from_defaults().with_flags(&cli).build();
    assert_eq!(config.observability.sampling_ratio, None);
    assert_eq!(config.observability.timeout_ms, None);

    // Non-finite values (NaN / inf) are also rejected — they would survive
    // a naive clamp.
    env::set_var("CQLITE_OTEL_SAMPLING_RATIO", "inf");
    let cli = Cli::parse_from(&["cqlite"]);
    let config = ConfigBuilder::from_defaults().with_flags(&cli).build();
    assert_eq!(config.observability.sampling_ratio, None);

    // A well-formed env value is still honored.
    env::set_var("CQLITE_OTEL_SAMPLING_RATIO", "0.25");
    env::set_var("CQLITE_OTEL_TIMEOUT_MS", "2500");
    let cli = Cli::parse_from(&["cqlite"]);
    let config = ConfigBuilder::from_defaults().with_flags(&cli).build();
    assert_eq!(config.observability.sampling_ratio, Some(0.25));
    assert_eq!(config.observability.timeout_ms, Some(2500));

    // A malformed CQLITE_OTEL_ENABLED must likewise not abort Cli::parse()
    // and must fall back to the default (None) rather than erroring.
    env::set_var("CQLITE_OTEL_ENABLED", "maybe");
    let cli = Cli::parse_from(&["cqlite"]);
    assert_eq!(cli.otel_enabled.as_deref(), Some("maybe"));
    let config = ConfigBuilder::from_defaults().with_flags(&cli).build();
    assert_eq!(config.observability.enabled, None);
    env::remove_var("CQLITE_OTEL_ENABLED");

    env::remove_var("CQLITE_OTEL_SAMPLING_RATIO");
    env::remove_var("CQLITE_OTEL_TIMEOUT_MS");
}

/// An explicit malformed `--otel-sampling-ratio` flag on the command line
/// is tolerated the same way (falls back to default) rather than aborting
/// the CLI.
#[test]
#[serial]
fn test_observability_malformed_numeric_flag_is_tolerated() {
    use std::env;
    env::remove_var("CQLITE_OTEL_SAMPLING_RATIO");
    env::remove_var("CQLITE_OTEL_TIMEOUT_MS");

    let cli = Cli::parse_from(&[
        "cqlite",
        "--otel-sampling-ratio",
        "garbage",
        "--otel-timeout-ms",
        "garbage",
    ]);
    let config = ConfigBuilder::from_defaults().with_flags(&cli).build();
    assert_eq!(config.observability.sampling_ratio, None);
    assert_eq!(config.observability.timeout_ms, None);
}

/// A malformed high-precedence `CQLITE_OTEL_ENABLED` must not leave a
/// lower-precedence config-file `enabled = true` active — the master switch
/// fails safe to the core default (disabled) on garbage input (issue #1033).
#[test]
#[serial]
fn test_observability_malformed_enabled_env_overrides_file_to_default() {
    use std::env;
    env::set_var("CQLITE_OTEL_ENABLED", "maybe");
    let cli = Cli::parse_from(&["cqlite"]);

    // Simulate a lower-precedence config-file value that enabled export.
    let mut builder = ConfigBuilder::from_defaults();
    builder.config.observability.enabled = Some(true);
    let config = builder.with_flags(&cli).build();

    // Garbage env resets the master switch to None so to_core() uses the
    // core default (disabled) instead of the file's `true`.
    assert_eq!(config.observability.enabled, None);
    env::remove_var("CQLITE_OTEL_ENABLED");
}

#[test]
fn test_config_builder_preserves_version_flag() {
    let cli = Cli::parse_from(&["cqlite", "--cassandra-version", "4.0"]);

    let config = ConfigBuilder::from_defaults().with_flags(&cli).build();

    assert_eq!(config.cassandra_version, Some("4.0".to_string()));
}

#[test]
#[serial]
fn test_env_var_replaces_config_file_schema_paths() {
    use std::env;

    // Set up environment variable
    env::set_var("CQLITE_SCHEMA", "/env/path1,/env/path2");

    // Create config with file-based schema paths
    let mut config = Config::default();
    config.schema_paths = vec![PathBuf::from("/file/path1"), PathBuf::from("/file/path2")];

    // Apply env vars
    let builder = ConfigBuilder { config };
    let result = builder.with_env().unwrap();

    // Verify env var REPLACED file paths, not extended
    assert_eq!(result.config.schema_paths.len(), 2);
    assert_eq!(result.config.schema_paths[0], PathBuf::from("/env/path1"));
    assert_eq!(result.config.schema_paths[1], PathBuf::from("/env/path2"));

    // Clean up
    env::remove_var("CQLITE_SCHEMA");
}

#[test]
#[serial]
fn test_env_var_single_schema_path_replaces_multiple() {
    use std::env;

    // Set up environment variable with single path
    env::set_var("CQLITE_SCHEMA", "/env/single/path");

    // Create config with multiple file-based schema paths
    let mut config = Config::default();
    config.schema_paths = vec![
        PathBuf::from("/file/path1"),
        PathBuf::from("/file/path2"),
        PathBuf::from("/file/path3"),
    ];

    // Apply env vars
    let builder = ConfigBuilder { config };
    let result = builder.with_env().unwrap();

    // Verify single env path replaced all file paths
    assert_eq!(result.config.schema_paths.len(), 1);
    assert_eq!(
        result.config.schema_paths[0],
        PathBuf::from("/env/single/path")
    );

    // Clean up
    env::remove_var("CQLITE_SCHEMA");
}

#[test]
#[serial]
fn test_cli_flag_overrides_env_var_schema() {
    use std::env;

    // Set up environment variable
    env::set_var("CQLITE_SCHEMA", "/env/path1,/env/path2");

    // Create config with file paths and apply env
    let mut config = Config::default();
    config.schema_paths = vec![PathBuf::from("/file/path")];

    let builder = ConfigBuilder { config };
    let result = builder.with_env().unwrap();

    // At this point, env var should have replaced file paths
    assert_eq!(result.config.schema_paths.len(), 2);

    // Now apply CLI flag
    let cli = Cli::parse_from(&["cqlite", "--schema", "/cli/path"]);
    let final_config = result.with_flags(&cli).build();

    // Verify CLI flag replaced everything
    assert_eq!(final_config.schema_paths.len(), 1);
    assert_eq!(final_config.schema_paths[0], PathBuf::from("/cli/path"));

    // Clean up
    env::remove_var("CQLITE_SCHEMA");
}

#[test]
#[serial]
fn test_schema_precedence_chain_complete() {
    use std::env;

    // Test: file < env < CLI flag

    // Start with file-based config
    let mut config = Config::default();
    config.schema_paths = vec![PathBuf::from("/file/path")];

    // Apply env var (should replace file)
    env::set_var("CQLITE_SCHEMA", "/env/path");
    let builder = ConfigBuilder { config };
    let with_env = builder.with_env().unwrap();
    assert_eq!(
        with_env.config.schema_paths,
        vec![PathBuf::from("/env/path")]
    );

    // Apply CLI flag (should replace env)
    let cli = Cli::parse_from(&["cqlite", "--schema", "/cli/path"]);
    let final_config = with_env.with_flags(&cli).build();
    assert_eq!(final_config.schema_paths, vec![PathBuf::from("/cli/path")]);

    // Clean up
    env::remove_var("CQLITE_SCHEMA");
}

#[test]
#[serial]
fn test_no_env_var_preserves_file_schema() {
    use std::env;

    // Make sure env var is NOT set
    env::remove_var("CQLITE_SCHEMA");

    // Create config with file-based schema paths
    let mut config = Config::default();
    config.schema_paths = vec![PathBuf::from("/file/path1"), PathBuf::from("/file/path2")];

    // Apply env vars (should not change anything)
    let builder = ConfigBuilder { config };
    let result = builder.with_env().unwrap();

    // Verify file paths are preserved
    assert_eq!(result.config.schema_paths.len(), 2);
    assert_eq!(result.config.schema_paths[0], PathBuf::from("/file/path1"));
    assert_eq!(result.config.schema_paths[1], PathBuf::from("/file/path2"));
}

#[test]
#[serial]
fn test_env_var_with_whitespace_trimming() {
    use std::env;

    // Set up environment variable with whitespace
    env::set_var("CQLITE_SCHEMA", " /path1 , /path2 , /path3 ");

    // Create config
    let config = Config::default();

    // Apply env vars
    let builder = ConfigBuilder { config };
    let result = builder.with_env().unwrap();

    // Verify paths are trimmed
    assert_eq!(result.config.schema_paths.len(), 3);
    assert_eq!(result.config.schema_paths[0], PathBuf::from("/path1"));
    assert_eq!(result.config.schema_paths[1], PathBuf::from("/path2"));
    assert_eq!(result.config.schema_paths[2], PathBuf::from("/path3"));

    // Clean up
    env::remove_var("CQLITE_SCHEMA");
}

// OutputConfig precedence chain tests (Issue #118)

#[test]
#[serial]
fn test_output_config_uses_defaults_when_no_flags_or_env() {
    use std::env;

    // Ensure no env vars are set
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
    env::remove_var("CQLITE_NO_COLOR");

    // Create CLI with no flags
    let cli = Cli::parse_from(&["cqlite"]);

    // Build config (should have defaults)
    let config = Config::load(None, &cli).unwrap();

    // Create OutputConfig with no CLI flags
    let output = OutputConfig::from_cli(&config, false, None, None, None, false);

    // Verify defaults
    assert!(output.color_enabled); // Default is true
    assert_eq!(output.limit, None); // Default is None
    assert_eq!(output.page_size, Some(50)); // Default is 50
}

#[test]
#[serial]
fn test_output_config_env_vars_override_defaults() {
    use std::env;

    // Set env vars
    env::set_var("CQLITE_LIMIT", "100");
    env::set_var("CQLITE_PAGE_SIZE", "25");
    env::set_var("CQLITE_NO_COLOR", "true");

    // Create CLI with no flags
    let cli = Cli::parse_from(&["cqlite"]);

    // Build config (should pick up env vars)
    let config = Config::load(None, &cli).unwrap();

    // Create OutputConfig with no CLI flags
    let output = OutputConfig::from_cli(&config, false, None, None, None, false);

    // Verify env vars were used
    assert!(!output.color_enabled); // CQLITE_NO_COLOR=true
    assert_eq!(output.limit, Some(100)); // CQLITE_LIMIT=100
    assert_eq!(output.page_size, Some(25)); // CQLITE_PAGE_SIZE=25

    // Clean up
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
    env::remove_var("CQLITE_NO_COLOR");
}

#[test]
#[serial]
fn test_output_config_cli_flags_override_env_vars() {
    use std::env;

    // Set env vars
    env::set_var("CQLITE_LIMIT", "100");
    env::set_var("CQLITE_PAGE_SIZE", "25");
    env::set_var("CQLITE_NO_COLOR", "false");

    // Create CLI with flags (should override env)
    let cli = Cli::parse_from(&[
        "cqlite",
        "--limit",
        "200",
        "--page-size",
        "10",
        "--no-color",
    ]);

    // Build config
    let config = Config::load(None, &cli).unwrap();

    // Create OutputConfig with CLI flags
    let output = OutputConfig::from_cli(
        &config,
        cli.no_color,
        cli.limit,
        cli.page_size,
        cli.output.clone(),
        cli.overwrite,
    );

    // Verify CLI flags overrode env vars
    assert!(!output.color_enabled); // --no-color flag
    assert_eq!(output.limit, Some(200)); // --limit 200
    assert_eq!(output.page_size, Some(10)); // --page-size 10

    // Clean up
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
    env::remove_var("CQLITE_NO_COLOR");
}

#[test]
#[serial]
fn test_output_config_partial_cli_flags_preserve_env_vars() {
    use std::env;

    // Set env vars
    env::set_var("CQLITE_LIMIT", "100");
    env::set_var("CQLITE_PAGE_SIZE", "25");

    // Create CLI with only --no-color flag
    let cli = Cli::parse_from(&["cqlite", "--no-color"]);

    // Build config
    let config = Config::load(None, &cli).unwrap();

    // Create OutputConfig with partial CLI flags
    let output = OutputConfig::from_cli(
        &config,
        cli.no_color,
        cli.limit,
        cli.page_size,
        cli.output.clone(),
        cli.overwrite,
    );

    // Verify: --no-color overrides, but env vars are used for limit/page_size
    assert!(!output.color_enabled); // --no-color flag
    assert_eq!(output.limit, Some(100)); // From CQLITE_LIMIT env
    assert_eq!(output.page_size, Some(25)); // From CQLITE_PAGE_SIZE env

    // Clean up
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
}

#[test]
#[serial]
fn test_output_config_no_color_flag_false_preserves_env() {
    use std::env;

    // Set env var to disable colors
    env::set_var("CQLITE_NO_COLOR", "true");

    // Create CLI without --no-color flag
    let cli = Cli::parse_from(&["cqlite"]);

    // Build config (should pick up env var)
    let config = Config::load(None, &cli).unwrap();

    // Create OutputConfig with no_color_flag=false (no flag provided)
    let output = OutputConfig::from_cli(&config, false, None, None, None, false);

    // Verify env var was respected
    assert!(!output.color_enabled); // CQLITE_NO_COLOR=true from env

    // Clean up
    env::remove_var("CQLITE_NO_COLOR");
}

#[test]
#[serial]
fn test_output_config_complete_precedence_chain() {
    use std::env;

    // Test the complete chain: flags > env > defaults

    // Step 1: Defaults only
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
    env::remove_var("CQLITE_NO_COLOR");

    let cli = Cli::parse_from(&["cqlite"]);
    let config = Config::load(None, &cli).unwrap();
    let output = OutputConfig::from_cli(&config, false, None, None, None, false);

    assert!(output.color_enabled);
    assert_eq!(output.limit, None);
    assert_eq!(output.page_size, Some(50));

    // Step 2: Env vars override defaults
    env::set_var("CQLITE_LIMIT", "150");
    env::set_var("CQLITE_PAGE_SIZE", "30");
    env::set_var("CQLITE_NO_COLOR", "true");

    let cli = Cli::parse_from(&["cqlite"]);
    let config = Config::load(None, &cli).unwrap();
    let output = OutputConfig::from_cli(&config, false, None, None, None, false);

    assert!(!output.color_enabled);
    assert_eq!(output.limit, Some(150));
    assert_eq!(output.page_size, Some(30));

    // Step 3: CLI flags override env vars
    let cli = Cli::parse_from(&["cqlite", "--limit", "300", "--page-size", "15"]);
    let config = Config::load(None, &cli).unwrap();
    let output = OutputConfig::from_cli(
        &config,
        cli.no_color,
        cli.limit,
        cli.page_size,
        cli.output.clone(),
        cli.overwrite,
    );

    // CLI flags override env, but --no-color not provided so env var still applies
    assert!(!output.color_enabled); // Still from CQLITE_NO_COLOR env
    assert_eq!(output.limit, Some(300)); // From --limit flag
    assert_eq!(output.page_size, Some(15)); // From --page-size flag

    // Clean up
    env::remove_var("CQLITE_LIMIT");
    env::remove_var("CQLITE_PAGE_SIZE");
    env::remove_var("CQLITE_NO_COLOR");
}
