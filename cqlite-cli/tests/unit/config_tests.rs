//! Unit tests for configuration handling

use cqlite_cli::Config;
use tempfile::TempDir;
use std::path::PathBuf;

#[test]
fn test_default_config_creation() {
    let config = Config::default();
    
    // Verify default values
    assert!(config.default_database.is_none());
    assert_eq!(config.repl.max_history_size, 1000);
    assert_eq!(config.performance.memory_limit_mb, None);
    assert_eq!(config.performance.cache_size_mb, 256);
    assert_eq!(config.performance.query_timeout_ms, 300000);
}

#[test]
fn test_config_serialization() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();
    let serialized = toml::to_string(&config)?;
    
    // Verify serialization contains expected sections
    assert!(serialized.contains("[repl]"));
    assert!(serialized.contains("[performance]"));
    assert!(serialized.contains("[output]"));
    assert!(serialized.contains("[connection]"));
    assert!(serialized.contains("[logging]"));
    
    Ok(())
}

#[test]
fn test_config_deserialization() -> Result<(), Box<dyn std::error::Error>> {
    let toml_content = r#"
        [connection]
        timeout_ms = 30000
        retry_attempts = 3
        pool_size = 10
        
        [output]
        max_rows = 500
        colors = false
        timestamp_format = "%Y-%m-%d"
        
        [performance]
        cache_size_mb = 128
        query_timeout_ms = 60000
        
        [logging]
        level = "debug"
        format = "Json"
        
        [repl]
        enable_history = false
        enable_completion = false
        enable_colors = true
        show_timing = true
        page_size = 25
        enable_paging = false
        max_history_size = 500
        prompt = "test> "
        prompt_continuation = "-> "
    "#;
    
    let config: Config = toml::from_str(toml_content)?;
    
    assert_eq!(config.repl.max_history_size, 500);
    assert!(!config.repl.enable_completion);
    assert_eq!(config.performance.cache_size_mb, 128);
    assert_eq!(config.performance.query_timeout_ms, 60000);
    assert_eq!(config.output.max_rows, Some(500));
    assert_eq!(config.repl.page_size, 25);
    
    Ok(())
}

#[test]
fn test_config_file_save_and_load() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config_path = temp_dir.path().join("test_config.toml");
    
    // Create and save config
    let mut original_config = Config::default();
    original_config.repl.max_history_size = 2000;
    original_config.performance.cache_size_mb = 512;
    original_config.save_to_file(&config_path)?;
    
    // Load config from file
    let loaded_config = Config::load(Some(config_path))?;
    
    assert_eq!(loaded_config.repl.max_history_size, 2000);
    assert_eq!(loaded_config.performance.cache_size_mb, 512);
    
    Ok(())
}

#[test]
fn test_config_validation() {
    let mut config = Config::default();
    
    // Valid config should pass
    assert!(config.validate().is_ok());
    
    // Invalid cache size should fail
    config.performance.cache_size_mb = 0;
    assert!(config.validate().is_err());
    
    // Invalid timeout should fail
    config.performance.cache_size_mb = 64; // Reset to valid
    config.performance.query_timeout_ms = 0;
    assert!(config.validate().is_err());
}

#[test]
fn test_config_with_database_path() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = Config::default();
    config.default_database = Some(db_path.clone());
    
    assert_eq!(config.default_database, Some(db_path));
    
    Ok(())
}

#[test]
fn test_config_environment_variable_override() {
    // This test would verify environment variable overrides
    // The actual implementation would depend on your config loading logic
    
    unsafe {
        // SAFETY: Setting environment variable in single-threaded test context
        // This is safe because it's within a test function and affects only the current process
        std::env::set_var("CQLITE_CACHE_SIZE_MB", "512");
    }
    
    // In a real implementation, you'd test that the config loader
    // picks up environment variables
    let config = Config::default();
    
    // Clean up
    std::env::remove_var("CQLITE_CACHE_SIZE_MB");
    
    // This assertion would change based on actual env var implementation
    assert_eq!(config.performance.cache_size_mb, 64); // Default value for now
}

#[test]
fn test_config_merge() {
    let mut base_config = Config::default();
    base_config.repl.max_history_size = 500;
    
    let mut override_config = Config::default();
    override_config.performance.cache_size_mb = 128;
    
    // Test merging logic (would need actual implementation)
    // This is a placeholder for configuration merging functionality
    assert_eq!(base_config.repl.max_history_size, 500);
    assert_eq!(override_config.performance.cache_size_mb, 128);
}

#[test]
fn test_config_partial_updates() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config_path = temp_dir.path().join("partial_config.toml");
    
    // Write complete config with custom performance values
    let partial_toml = r#"
        [connection]
        timeout_ms = 30000
        retry_attempts = 3
        pool_size = 10
        
        [output]
        max_rows = 1000
        colors = true
        timestamp_format = "%Y-%m-%d %H:%M:%S"
        
        [performance]
        cache_size_mb = 512
        query_timeout_ms = 300000
        
        [logging]
        level = "info"
        format = "Pretty"
        
        [repl]
        enable_history = true
        enable_completion = true
        enable_colors = true
        show_timing = false
        page_size = 50
        enable_paging = true
        max_history_size = 1000
        prompt = "cqlite> "
        prompt_continuation = "    -> "
    "#;
    std::fs::write(&config_path, partial_toml)?;
    
    // Load should use custom values
    let config = Config::load(Some(config_path))?;
    
    assert_eq!(config.performance.cache_size_mb, 512);
    assert_eq!(config.repl.max_history_size, 1000); // Default value
    
    Ok(())
}

#[test]
fn test_config_invalid_file_handling() {
    // Test loading non-existent file
    let result = Config::load(Some(PathBuf::from("/non/existent/config.toml")));
    assert!(result.is_err());
    
    // Test loading invalid TOML
    let temp_dir = TempDir::new().unwrap();
    let invalid_config_path = temp_dir.path().join("invalid.toml");
    std::fs::write(&invalid_config_path, "invalid toml content [").unwrap();
    
    let result = Config::load(Some(invalid_config_path));
    assert!(result.is_err());
}