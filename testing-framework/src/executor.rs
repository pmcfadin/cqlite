//! Test execution engine for running queries on both cqlsh and cqlite

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio::time::timeout;

use crate::config::TestConfig;
use crate::output::QueryOutput;
use crate::test_case::TestCase;

/// Executes tests on both cqlsh and cqlite
pub struct TestExecutor {
    config: TestConfig,
}

impl TestExecutor {
    /// Create a new test executor
    pub fn new(config: &TestConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Execute a test case on cqlsh (via Docker)
    pub async fn execute_cqlsh(&self, test_case: &TestCase) -> Result<QueryOutput> {
        let start_time = Instant::now();
        
        log::debug!("Executing on cqlsh: {}", test_case.name);

        // Build cqlsh command
        let mut cmd = Command::new("docker");
        cmd.args(&[
            "exec",
            &self.config.docker.container_name,
            "cqlsh",
            "-e",
            &test_case.query,
        ]);

        // Add keyspace if specified
        if let Some(keyspace) = &test_case.keyspace {
            cmd.args(&["-k", keyspace]);
        }

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        // Execute with timeout
        let timeout_duration = Duration::from_secs(self.config.cqlite.query_timeout_seconds);
        
        let result = timeout(timeout_duration, async {
            let output = cmd.output()
                .context("Failed to execute cqlsh command")?;

            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if !output.status.success() {
                anyhow::bail!("cqlsh failed with status: {} - stderr: {}", output.status, stderr);
            }

            Ok((stdout, stderr))
        }).await??;

        let execution_time = start_time.elapsed();
        let (stdout, stderr) = result;

        let output = self.parse_cqlsh_output(&stdout, &stderr, execution_time)?;
        
        log::debug!("cqlsh execution completed in {:?}", execution_time);
        Ok(output)
    }

    /// Execute a test case on cqlite
    pub async fn execute_cqlite(&self, test_case: &TestCase) -> Result<QueryOutput> {
        let start_time = Instant::now();
        
        log::debug!("Executing on cqlite: {}", test_case.name);

        // Build cqlite command
        let mut cmd = Command::new(&self.config.cqlite.binary_path);
        cmd.args(&[
            "--database",
            self.config.cqlite.database_path.to_str().unwrap(),
            "--format",
            &self.config.cqlite.output_format,
            "query",
            &test_case.query,
        ]);

        // Add quiet flag to reduce noise
        cmd.arg("--quiet");

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        // Execute with timeout
        let timeout_duration = Duration::from_secs(self.config.cqlite.query_timeout_seconds);
        
        let result = timeout(timeout_duration, async {
            let output = cmd.output()
                .context("Failed to execute cqlite command")?;

            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if !output.status.success() {
                anyhow::bail!("cqlite failed with status: {} - stderr: {}", output.status, stderr);
            }

            Ok((stdout, stderr))
        }).await??;

        let execution_time = start_time.elapsed();
        let (stdout, stderr) = result;

        let output = self.parse_cqlite_output(&stdout, &stderr, execution_time)?;
        
        log::debug!("cqlite execution completed in {:?}", execution_time);
        Ok(output)
    }

    /// Parse cqlsh output into structured format
    fn parse_cqlsh_output(&self, stdout: &str, stderr: &str, execution_time: Duration) -> Result<QueryOutput> {
        let mut output = QueryOutput {
            raw_output: stdout.to_string(),
            stderr: stderr.to_string(),
            execution_time_ms: execution_time.as_millis() as u64,
            ..Default::default()
        };

        // Parse table format (cqlsh default)
        if stdout.contains("---") || stdout.contains("|") {
            output.format = "table".to_string();
            output.rows = self.parse_table_format(stdout)?;
        } else if stdout.trim().is_empty() {
            // Empty result set
            output.format = "empty".to_string();
            output.rows = Vec::new();
        } else {
            // Single value or error
            output.format = "text".to_string();
            output.rows = vec![vec![stdout.trim().to_string()]];
        }

        // Extract row count if available
        if let Some(count_line) = stdout.lines().last() {
            if count_line.contains("rows)") {
                if let Some(count_part) = count_line.split('(').nth(1) {
                    if let Some(count_str) = count_part.split(' ').next() {
                        if let Ok(count) = count_str.parse::<usize>() {
                            output.row_count = Some(count);
                        }
                    }
                }
            }
        }

        // Extract column headers if present
        if let Some(header_line) = stdout.lines().find(|line| !line.trim().is_empty() && !line.contains("---")) {
            if header_line.contains("|") {
                output.column_headers = header_line
                    .split('|')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }

        Ok(output)
    }

    /// Parse cqlite output into structured format  
    fn parse_cqlite_output(&self, stdout: &str, stderr: &str, execution_time: Duration) -> Result<QueryOutput> {
        let mut output = QueryOutput {
            raw_output: stdout.to_string(),
            stderr: stderr.to_string(),
            execution_time_ms: execution_time.as_millis() as u64,
            ..Default::default()
        };

        // Parse based on configured output format
        match self.config.cqlite.output_format.as_str() {
            "table" => {
                output.format = "table".to_string();
                output.rows = self.parse_table_format(stdout)?;
            }
            "json" => {
                output.format = "json".to_string();
                if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(stdout) {
                    output.json_data = Some(json_value);
                    output.rows = self.json_to_rows(&output.json_data.as_ref().unwrap())?;
                }
            }
            "csv" => {
                output.format = "csv".to_string();
                output.rows = self.parse_csv_format(stdout)?;
            }
            _ => {
                output.format = "text".to_string();
                output.rows = vec![vec![stdout.trim().to_string()]];
            }
        }

        // Set row count
        output.row_count = Some(output.rows.len());

        Ok(output)
    }

    /// Parse table format output (both cqlsh and cqlite table format)
    fn parse_table_format(&self, output: &str) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        let mut in_data_section = false;
        let mut header_processed = false;

        for line in output.lines() {
            let line = line.trim();
            
            // Skip empty lines
            if line.is_empty() {
                continue;
            }

            // Skip separator lines
            if line.starts_with("---") || line.starts_with("===") {
                in_data_section = true;
                continue;
            }

            // Skip metadata lines
            if line.starts_with("(") && line.contains("rows)") {
                break;
            }

            // Process header
            if !header_processed && (line.contains("|") || in_data_section) {
                if line.contains("|") {
                    let columns: Vec<String> = line
                        .split('|')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                        
                    if !columns.is_empty() {
                        rows.push(columns);
                        header_processed = true;
                    }
                }
                continue;
            }

            // Process data rows
            if in_data_section || header_processed {
                if line.contains("|") {
                    let columns: Vec<String> = line
                        .split('|')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                        
                    if !columns.is_empty() {
                        rows.push(columns);
                    }
                }
            }
        }

        Ok(rows)
    }

    /// Parse CSV format output
    fn parse_csv_format(&self, output: &str) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        
        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Simple CSV parsing (doesn't handle quoted values with commas)
            let columns: Vec<String> = line
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
                
            rows.push(columns);
        }

        Ok(rows)
    }

    /// Convert JSON data to row format
    fn json_to_rows(&self, json_data: &serde_json::Value) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();

        match json_data {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    if let serde_json::Value::Object(obj) = item {
                        let row: Vec<String> = obj
                            .values()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => "null".to_string(),
                                _ => v.to_string(),
                            })
                            .collect();
                        rows.push(row);
                    }
                }
            }
            serde_json::Value::Object(obj) => {
                let row: Vec<String> = obj
                    .values()
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Null => "null".to_string(),
                        _ => v.to_string(),
                    })
                    .collect();
                rows.push(row);
            }
            _ => {
                rows.push(vec![json_data.to_string()]);
            }
        }

        Ok(rows)
    }

    /// Execute a setup query (schema creation, data insertion, etc.)
    pub async fn execute_setup(&self, query: &str) -> Result<()> {
        log::debug!("Executing setup query: {}", query);

        // First execute on Cassandra (via cqlsh)
        let mut cmd = Command::new("docker");
        cmd.args(&[
            "exec",
            &self.config.docker.container_name,
            "cqlsh",
            "-e",
            query,
        ]);

        let output = cmd.output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            log::warn!("Setup query failed on cqlsh: {}", stderr);
        }

        // Then execute on cqlite (if needed for schema setup)
        if query.to_uppercase().contains("CREATE") || query.to_uppercase().contains("INSERT") {
            let mut cmd = Command::new(&self.config.cqlite.binary_path);
            cmd.args(&[
                "--database",
                self.config.cqlite.database_path.to_str().unwrap(),
                "query",
                query,
                "--quiet",
            ]);

            let output = cmd.output()?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                log::debug!("Setup query on cqlite (may not be needed): {}", stderr);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TestConfig;

    #[test]
    fn test_table_format_parsing() {
        let executor = TestExecutor::new(&TestConfig::default());
        
        let table_output = r#"
 id                                   | name | age
--------------------------------------+------+-----
 550e8400-e29b-41d4-a716-446655440000 | John |  30
 550e8400-e29b-41d4-a716-446655440001 | Jane |  25

(2 rows)
"#;

        let rows = executor.parse_table_format(table_output).unwrap();
        assert_eq!(rows.len(), 3); // header + 2 data rows
        assert_eq!(rows[0], vec!["id", "name", "age"]);
        assert_eq!(rows[1][1], "John");
        assert_eq!(rows[2][1], "Jane");
    }

    #[test]
    fn test_csv_format_parsing() {
        let executor = TestExecutor::new(&TestConfig::default());
        
        let csv_output = "id,name,age\n550e8400-e29b-41d4-a716-446655440000,John,30\n550e8400-e29b-41d4-a716-446655440001,Jane,25";
        
        let rows = executor.parse_csv_format(csv_output).unwrap();
        assert_eq!(rows.len(), 3); // header + 2 data rows
        assert_eq!(rows[0], vec!["id", "name", "age"]);
        assert_eq!(rows[1], vec!["550e8400-e29b-41d4-a716-446655440000", "John", "30"]);
    }

    #[test]
    fn test_json_to_rows() {
        let executor = TestExecutor::new(&TestConfig::default());
        
        let json_data = serde_json::json!([
            {"id": "550e8400-e29b-41d4-a716-446655440000", "name": "John", "age": 30},
            {"id": "550e8400-e29b-41d4-a716-446655440001", "name": "Jane", "age": 25}
        ]);
        
        let rows = executor.json_to_rows(&json_data).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains(&"John".to_string()));
        assert!(rows[1].contains(&"Jane".to_string()));
    }
}