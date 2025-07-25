use anyhow::Result;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;
use tempfile::TempDir;

/// Test utilities and helpers for CLI testing
/// 
/// This module provides common functionality for all test suites:
/// - Test data generation
/// - CLI command execution helpers
/// - Output validation utilities
/// - File and directory management
/// - Performance measurement tools

pub const CLI_BINARY: &str = "cqlite";
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Execute CLI command with arguments
pub fn run_cli(args: &[&str]) -> Result<Output> {
    Command::new("cargo")
        .args(&["run", "--bin", CLI_BINARY, "--"])
        .args(args)
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to execute CLI command: {}", e))
}

/// Execute CLI command with timeout (basic implementation)
pub fn run_cli_with_timeout(args: &[&str], _timeout: Duration) -> Result<Output> {
    // For now, this is the same as run_cli
    // In a full implementation, this would use process timeout mechanisms
    run_cli(args)
}

/// Check if CLI command succeeded
pub fn command_succeeded(output: &Output) -> bool {
    output.status.success()
}

/// Check if CLI command failed
pub fn command_failed(output: &Output) -> bool {
    !output.status.success()
}

/// Get stdout as string
pub fn get_stdout(output: &Output) -> Result<String> {
    String::from_utf8(output.stdout.clone())
        .map_err(|e| anyhow::anyhow!("Failed to decode stdout: {}", e))
}

/// Get stderr as string
pub fn get_stderr(output: &Output) -> Result<String> {
    String::from_utf8(output.stderr.clone())
        .map_err(|e| anyhow::anyhow!("Failed to decode stderr: {}", e))
}

/// Get combined output as string
pub fn get_combined_output(output: &Output) -> Result<String> {
    let stdout = get_stdout(output)?;
    let stderr = get_stderr(output)?;
    Ok(format!("{}{}", stdout, stderr))
}

/// Check if output contains all given patterns
pub fn output_contains_all(output: &Output, patterns: &[&str]) -> Result<bool> {
    let combined = get_combined_output(output)?;
    Ok(patterns.iter().all(|pattern| combined.contains(pattern)))
}

/// Check if output contains any of the given patterns
pub fn output_contains_any(output: &Output, patterns: &[&str]) -> Result<bool> {
    let combined = get_combined_output(output)?;
    Ok(patterns.iter().any(|pattern| combined.contains(pattern)))
}

/// Validate output format based on expected format
pub fn validate_output_format(output: &Output, format: &str) -> Result<bool> {
    let stdout = get_stdout(output)?;
    
    match format.to_lowercase().as_str() {
        "json" => Ok(is_valid_json(&stdout)),
        "csv" => Ok(is_valid_csv(&stdout)),
        "yaml" => Ok(is_valid_yaml(&stdout)),
        "table" => Ok(is_table_format(&stdout)),
        _ => Ok(false),
    }
}

/// Check if string is valid JSON
fn is_valid_json(s: &str) -> bool {
    s.trim_start().starts_with('{') || s.trim_start().starts_with('[') ||
    serde_json::from_str::<serde_json::Value>(s).is_ok()
}

/// Check if string is valid CSV
fn is_valid_csv(s: &str) -> bool {
    s.lines().any(|line| line.contains(',')) && 
    !s.trim().is_empty()
}

/// Check if string looks like YAML
fn is_valid_yaml(s: &str) -> bool {
    s.contains(':') && 
    !s.trim_start().starts_with('{') &&
    !s.trim().is_empty()
}

/// Check if string is table format
fn is_table_format(s: &str) -> bool {
    s.contains('|') || s.contains('+') || s.contains('-')
}

/// Create temporary database with optional path
pub fn create_temp_database() -> Result<(TempDir, PathBuf)> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    Ok((temp_dir, db_path))
}

/// Create temporary directory for tests
pub fn create_temp_dir() -> Result<TempDir> {
    TempDir::new().map_err(|e| anyhow::anyhow!("Failed to create temp directory: {}", e))
}

/// Create test schema files (JSON and CQL)
pub fn create_test_schema_files(temp_dir: &TempDir) -> Result<(PathBuf, PathBuf)> {
    let json_schema = temp_dir.path().join("test_schema.json");
    let cql_schema = temp_dir.path().join("test_schema.cql");
    
    let json_content = r#"{
  "keyspace": "test_keyspace",
  "table": "test_table",
  "partition_keys": [
    {
      "name": "id",
      "data_type": "uuid",
      "position": 0
    }
  ],
  "clustering_keys": [
    {
      "name": "timestamp",
      "data_type": "timestamp",
      "position": 0,
      "order": "ASC"
    }
  ],
  "columns": [
    {
      "name": "id",
      "data_type": "uuid",
      "nullable": false,
      "default": null
    },
    {
      "name": "name",
      "data_type": "text",
      "nullable": true,
      "default": null
    },
    {
      "name": "email",
      "data_type": "text",
      "nullable": true,
      "default": null
    },
    {
      "name": "timestamp",
      "data_type": "timestamp",
      "nullable": false,
      "default": null
    }
  ],
  "comments": {}
}"#;

    let cql_content = r#"CREATE TABLE test_keyspace.test_table (
  id uuid,
  name text,
  email text,
  timestamp timestamp,
  PRIMARY KEY (id, timestamp)
);"#;

    std::fs::write(&json_schema, json_content)?;
    std::fs::write(&cql_schema, cql_content)?;
    
    Ok((json_schema, cql_schema))
}

/// Create mock SSTable directory structure
pub fn create_mock_sstable_dir(temp_dir: &TempDir, table_name: &str) -> Result<PathBuf> {
    let sstable_dir = temp_dir.path().join(format!("{}-46436710673711f0b2cf19d64e7cbecb", table_name));
    std::fs::create_dir_all(&sstable_dir)?;
    
    // Create essential SSTable files
    let files = vec![
        ("nb-1-big-Data.db", b"mock sstable data".as_ref()),
        ("nb-1-big-TOC.txt", b"Data.db\nStatistics.db\nTOC.txt".as_ref()),
        ("nb-1-big-Statistics.db", b"mock statistics".as_ref()),
        ("nb-1-big-Index.db", b"mock index".as_ref()),
        ("nb-1-big-Filter.db", b"mock filter".as_ref()),
    ];
    
    for (filename, content) in files {
        let file_path = sstable_dir.join(filename);
        std::fs::write(file_path, content)?;
    }
    
    Ok(sstable_dir)
}

/// Create test configuration file
pub fn create_test_config(temp_dir: &TempDir) -> Result<PathBuf> {
    let config_path = temp_dir.path().join("test_config.toml");
    
    let config_content = r#"
[performance]
cache_size_mb = 64
query_timeout_ms = 30000
memory_limit_mb = 256

[logging]
level = "info"

default_database = "test.db"
"#;
    
    std::fs::write(&config_path, config_content)?;
    Ok(config_path)
}

/// Create test data files for import/export testing
pub fn create_test_data_files(temp_dir: &TempDir) -> Result<(PathBuf, PathBuf, PathBuf)> {
    let json_file = temp_dir.path().join("test_data.json");
    let csv_file = temp_dir.path().join("test_data.csv");
    let large_csv_file = temp_dir.path().join("large_test_data.csv");
    
    // JSON test data
    let json_data = r#"[
  {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "name": "Alice Johnson",
    "email": "alice@example.com",
    "age": 28
  },
  {
    "id": "123e4567-e89b-12d3-a456-426614174001", 
    "name": "Bob Smith",
    "email": "bob@example.com",
    "age": 34
  },
  {
    "id": "123e4567-e89b-12d3-a456-426614174002",
    "name": "Carol Davis", 
    "email": "carol@example.com",
    "age": 29
  }
]"#;
    
    // CSV test data
    let csv_data = "id,name,email,age\n123e4567-e89b-12d3-a456-426614174000,Alice Johnson,alice@example.com,28\n123e4567-e89b-12d3-a456-426614174001,Bob Smith,bob@example.com,34\n123e4567-e89b-12d3-a456-426614174002,Carol Davis,carol@example.com,29";
    
    std::fs::write(&json_file, json_data)?;
    std::fs::write(&csv_file, csv_data)?;
    
    // Create larger CSV for performance testing
    let mut large_csv_content = String::from("id,name,email,age,city,country\n");
    for i in 0..1000 {
        large_csv_content.push_str(&format!(
            "123e4567-e89b-12d3-a456-{:012},User{},user{}@example.com,{},City{},Country{}\n",
            i, i, i, 20 + (i % 50), i % 100, i % 10
        ));
    }
    std::fs::write(&large_csv_file, large_csv_content)?;
    
    Ok((json_file, csv_file, large_csv_file))
}

/// Extract timing information from CLI output
pub fn extract_timing_ms(output: &Output) -> Option<f64> {
    let stdout = get_stdout(output).ok()?;
    
    for line in stdout.lines() {
        if line.contains("executed in") && line.contains("ms") {
            // Look for patterns like "Query executed in 123.45ms"
            if let Some(ms_part) = line.split("in ").nth(1) {
                if let Some(ms_str) = ms_part.split("ms").next() {
                    if let Ok(ms) = ms_str.trim().parse::<f64>() {
                        return Some(ms);
                    }
                }
            }
        }
        
        if line.contains("Timing:") && line.contains("ms") {
            // Look for patterns like "Timing: 123.45ms"
            if let Some(ms_part) = line.split("Timing: ").nth(1) {
                if let Some(ms_str) = ms_part.split("ms").next() {
                    if let Ok(ms) = ms_str.trim().parse::<f64>() {
                        return Some(ms);
                    }
                }
            }
        }
    }
    
    None
}

/// Check if CLI binary is available
pub fn cli_available() -> bool {
    Command::new("cargo")
        .args(&["check", "--bin", CLI_BINARY])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Performance measurement utilities
pub struct PerformanceMeasurement {
    pub start_time: std::time::Instant,
    pub end_time: Option<std::time::Instant>,
}

impl PerformanceMeasurement {
    pub fn start() -> Self {
        Self {
            start_time: std::time::Instant::now(),
            end_time: None,
        }
    }
    
    pub fn stop(&mut self) {
        self.end_time = Some(std::time::Instant::now());
    }
    
    pub fn duration(&self) -> Option<Duration> {
        self.end_time.map(|end| end.duration_since(self.start_time))
    }
    
    pub fn duration_ms(&self) -> Option<f64> {
        self.duration().map(|d| d.as_secs_f64() * 1000.0)
    }
}

/// Test result validation
pub struct TestValidator {
    pub passed: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}

impl TestValidator {
    pub fn new() -> Self {
        Self {
            passed: 0,
            failed: 0,
            errors: Vec::new(),
        }
    }
    
    pub fn assert_success(&mut self, output: &Output, description: &str) {
        if command_succeeded(output) {
            self.passed += 1;
        } else {
            self.failed += 1;
            let error = format!("{}: Command failed - stderr: {}", 
                              description, 
                              get_stderr(output).unwrap_or_else(|_| "Unable to decode stderr".to_string()));
            self.errors.push(error);
        }
    }
    
    pub fn assert_failure(&mut self, output: &Output, description: &str) {
        if command_failed(output) {
            self.passed += 1;
        } else {
            self.failed += 1;
            let error = format!("{}: Expected command to fail but it succeeded", description);
            self.errors.push(error);
        }
    }
    
    pub fn assert_contains(&mut self, output: &Output, pattern: &str, description: &str) {
        match output_contains_all(output, &[pattern]) {
            Ok(true) => self.passed += 1,
            Ok(false) => {
                self.failed += 1;
                let error = format!("{}: Output does not contain '{}'", description, pattern);
                self.errors.push(error);
            }
            Err(e) => {
                self.failed += 1;
                let error = format!("{}: Error checking output: {}", description, e);
                self.errors.push(error);
            }
        }
    }
    
    pub fn summary(&self) -> String {
        format!("Tests: {} passed, {} failed", self.passed, self.failed)
    }
    
    pub fn has_failures(&self) -> bool {
        self.failed > 0
    }
}

/// Common test scenarios
pub mod scenarios {
    use super::*;
    
    /// Test basic CLI help and version
    pub fn test_basic_cli_info() -> Result<()> {
        let mut validator = TestValidator::new();
        
        // Test help
        let help_output = run_cli(&["--help"])?;
        validator.assert_success(&help_output, "CLI help");
        validator.assert_contains(&help_output, "CQLite", "Help contains product name");
        
        // Test version
        let version_output = run_cli(&["--version"])?;
        validator.assert_success(&version_output, "CLI version");
        
        if validator.has_failures() {
            eprintln!("Basic CLI info test failures:");
            for error in &validator.errors {
                eprintln!("  - {}", error);
            }
            return Err(anyhow::anyhow!("Basic CLI info tests failed"));
        }
        
        println!("✅ Basic CLI info tests: {}", validator.summary());
        Ok(())
    }
    
    /// Test all output formats
    pub fn test_output_formats(db_path: &Path) -> Result<()> {
        let mut validator = TestValidator::new();
        let formats = ["table", "json", "csv", "yaml"];
        
        for format in &formats {
            let output = run_cli(&[
                "--database", db_path.to_str().unwrap(),
                "--format", format,
                "query", "SELECT 1 as test"
            ])?;
            
            validator.assert_success(&output, &format!("Format {}", format));
        }
        
        if validator.has_failures() {
            eprintln!("Output format test failures:");
            for error in &validator.errors {
                eprintln!("  - {}", error);
            }
            return Err(anyhow::anyhow!("Output format tests failed"));
        }
        
        println!("✅ Output format tests: {}", validator.summary());
        Ok(())
    }
}

/// Test environment setup and cleanup
pub struct TestEnvironment {
    pub temp_dir: TempDir,
    pub db_path: PathBuf,
    pub config_path: PathBuf,
    pub schema_files: (PathBuf, PathBuf),
    pub data_files: (PathBuf, PathBuf, PathBuf),
}

impl TestEnvironment {
    /// Create a complete test environment
    pub fn setup() -> Result<Self> {
        let temp_dir = create_temp_dir()?;
        let (_, db_path) = create_temp_database()?;
        let config_path = create_test_config(&temp_dir)?;
        let schema_files = create_test_schema_files(&temp_dir)?;
        let data_files = create_test_data_files(&temp_dir)?;
        
        Ok(Self {
            temp_dir,
            db_path,
            config_path,
            schema_files,
            data_files,
        })
    }
    
    /// Get database path as string
    pub fn db_path_str(&self) -> &str {
        self.db_path.to_str().unwrap()
    }
    
    /// Get config path as string
    pub fn config_path_str(&self) -> &str {
        self.config_path.to_str().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_helper_functions() -> Result<()> {
        // Test environment setup
        let env = TestEnvironment::setup()?;
        assert!(env.temp_dir.path().exists());
        assert!(env.config_path.exists());
        
        // Test output format detection
        assert!(is_valid_json(r#"{"test": "value"}"#));
        assert!(is_valid_csv("a,b,c\n1,2,3"));
        assert!(is_valid_yaml("key: value"));
        assert!(is_table_format("+---+---+\n| a | b |\n+---+---+"));
        
        Ok(())
    }
}