//! Docker integration for automated cqlsh testing
//!
//! This module provides functionality to execute cqlsh commands on a running
//! Cassandra Docker container and capture the results for comparison.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use crate::output::QueryOutput;
use crate::config::{TestConfig, DockerConfig};

/// Docker container information for Cassandra
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassandraContainer {
    pub container_id: String,
    pub container_name: String,
    pub image: String,
    pub status: String,
    pub ports: Vec<String>,
    pub keyspace: Option<String>,
}

/// CQLSH execution configuration
#[derive(Debug, Clone)]
pub struct CqlshConfig {
    pub container_name: String,
    pub keyspace: Option<String>,
    pub timeout_seconds: u64,
    pub host: String,
    pub port: u16,
}

impl Default for CqlshConfig {
    fn default() -> Self {
        Self {
            container_name: "cassandra-node1".to_string(),
            keyspace: None,
            timeout_seconds: 30,
            host: "localhost".to_string(),
            port: 9042,
        }
    }
}

/// Docker integration manager
pub struct DockerManager {
    config: DockerConfig,
}

impl DockerManager {
    /// Create a new Docker manager
    pub fn new(config: DockerConfig) -> Self {
        Self { config }
    }

    /// Ensure Cassandra is ready for testing
    pub async fn ensure_cassandra_ready(&self) -> Result<(), String> {
        let containers = self.find_cassandra_containers()?;
        if containers.is_empty() {
            return Err("No running Cassandra containers found".to_string());
        }

        // Check if the primary container is healthy
        let primary_container = &containers[0];
        if !primary_container.status.contains("healthy") && !primary_container.status.contains("Up") {
            return Err(format!("Cassandra container {} is not healthy: {}", 
                primary_container.container_name, primary_container.status));
        }

        // Test connectivity with a simple query
        let test_query = "SELECT now() FROM system.local;";
        match self.execute_cql_query(test_query) {
            Ok(_) => {
                log::info!("Cassandra container {} is ready for testing", primary_container.container_name);
                Ok(())
            },
            Err(e) => Err(format!("Cassandra connectivity test failed: {}", e))
        }
    }

    /// Cleanup Docker resources
    pub async fn cleanup(&self) -> Result<(), String> {
        log::info!("Cleaning up Docker resources");
        // For now, just log - in a real implementation we might stop test containers
        Ok(())
    }

    /// Find running Cassandra containers
    pub fn find_cassandra_containers(&self) -> Result<Vec<CassandraContainer>, String> {
        let output = Command::new("docker")
            .args(&["ps", "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"])
            .output()
            .map_err(|e| format!("Failed to execute docker ps: {}", e))?;

        if !output.status.success() {
            return Err(format!("Docker ps failed: {}", String::from_utf8_lossy(&output.stderr)));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut containers = Vec::new();

        for line in stdout.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 4 {
                let image = parts[2].to_lowercase();
                if image.contains("cassandra") {
                    containers.push(CassandraContainer {
                        container_id: parts[0].to_string(),
                        container_name: parts[1].to_string(),
                        image: parts[2].to_string(),
                        status: parts[3].to_string(),
                        ports: if parts.len() > 4 {
                            parts[4].split(',').map(|s| s.trim().to_string()).collect()
                        } else {
                            Vec::new()
                        },
                        keyspace: None,
                    });
                }
            }
        }

        Ok(containers)
    }

    /// Test connection to Cassandra container
    pub fn test_connection(&self) -> Result<bool, String> {
        let start_time = Instant::now();
        
        println!("🔍 Testing connection to Cassandra container: {}", self.config.container_name);
        
        let output = Command::new("docker")
            .args(&[
                "exec",
                &self.config.container_name,
                "cqlsh",
                "-e",
                "DESCRIBE KEYSPACES;"
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| format!("Failed to execute docker exec: {}", e))?;

        let execution_time = start_time.elapsed();
        
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            println!("✅ Connection successful ({:?})", execution_time);
            println!("📋 Available keyspaces: {}", stdout.trim());
            Ok(true)
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            println!("❌ Connection failed: {}", stderr);
            Err(format!("Connection test failed: {}", stderr))
        }
    }

    /// Execute a CQL query using cqlsh in the container
    pub fn execute_cql_query(&self, query: &str) -> Result<QueryOutput, String> {
        let start_time = Instant::now();
        
        println!("🚀 Executing CQL query: {}", query);
        
        // Build cqlsh command
        let mut args = vec![
            "exec".to_string(),
            self.config.container_name.clone(),
            "cqlsh".to_string(),
        ];

        // Add keyspace if specified
        if let Some(keyspace) = &self.config.keyspace {
            args.push("-k".to_string());
            args.push(keyspace.clone());
        }

        // Add the query
        args.push("-e".to_string());
        args.push(query.to_string());

        let output = Command::new("docker")
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| format!("Failed to execute docker exec: {}", e))?;

        let execution_time = start_time.elapsed();
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if output.status.success() {
            println!("✅ Query executed successfully ({:?})", execution_time);
            self.parse_cqlsh_output(&stdout, execution_time)
        } else {
            println!("❌ Query failed: {}", stderr);
            Ok(QueryOutput::error(&stderr))
        }
    }

    /// Parse cqlsh output into structured format
    fn parse_cqlsh_output(&self, output: &str, execution_time: Duration) -> Result<QueryOutput, String> {
        let mut query_output = QueryOutput::default();
        query_output.raw_output = output.to_string();
        query_output.execution_time_ms = execution_time.as_millis() as u64;
        query_output.format = "table".to_string();

        // Parse table format output
        let lines: Vec<&str> = output.lines().collect();
        let mut parsing_data = false;
        let mut header_found = false;
        
        for (i, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            
            // Skip empty lines
            if trimmed.is_empty() {
                continue;
            }
            
            // Skip cqlsh prompt and warnings
            if trimmed.starts_with("cqlsh") || 
               trimmed.starts_with("Warning") ||
               trimmed.starts_with("Connected") {
                continue;
            }
            
            // Look for result summary (e.g., "(2 rows)")
            if trimmed.starts_with('(') && trimmed.ends_with(" rows)") {
                if let Some(count_str) = trimmed.strip_prefix('(').and_then(|s| s.strip_suffix(" rows)")) {
                    if let Ok(count) = count_str.parse::<usize>() {
                        query_output.row_count = Some(count);
                    }
                }
                break;
            }
            
            // Look for separator line (dashes and plus signs)
            if trimmed.contains("----") && trimmed.contains('+') {
                parsing_data = true;
                continue;
            }
            
            // Parse header line (contains pipe separators and non-dash characters)
            if !header_found && trimmed.contains('|') && !trimmed.contains('-') {
                let headers: Vec<String> = trimmed
                    .split('|')
                    .map(|h| h.trim().to_string())
                    .filter(|h| !h.is_empty())
                    .collect();
                
                if !headers.is_empty() {
                    query_output.column_headers = headers;
                    header_found = true;
                }
                continue;
            }
            
            // Parse data rows
            if parsing_data && trimmed.contains('|') && !trimmed.contains('-') {
                let row_data: Vec<String> = trimmed
                    .split('|')
                    .map(|cell| cell.trim().to_string())
                    .filter(|cell| !cell.is_empty())
                    .collect();
                
                if !row_data.is_empty() {
                    query_output.rows.push(row_data);
                }
            }
        }

        // Set row count if not found in summary
        if query_output.row_count.is_none() {
            query_output.row_count = Some(query_output.rows.len());
        }

        println!("📊 Parsed {} rows with {} columns", 
                query_output.rows.len(), 
                query_output.column_headers.len());

        Ok(query_output)
    }

    /// Execute multiple queries for comprehensive testing
    pub fn execute_test_queries(&self, keyspace: &str, table: &str) -> Result<Vec<(String, QueryOutput)>, String> {
        let test_queries = vec![
            format!("SELECT * FROM {}.{} LIMIT 5;", keyspace, table),
            format!("SELECT COUNT(*) FROM {}.{};", keyspace, table),
            format!("SELECT * FROM {}.{} WHERE id = a8f167f0-ebe7-4f20-a386-31ff138bec3b;", keyspace, table),
        ];

        let mut results = Vec::new();

        for query in test_queries {
            println!("\n🔧 Executing test query: {}", query);
            match self.execute_cql_query(&query) {
                Ok(output) => {
                    println!("✅ Query successful - {} rows returned", 
                            output.row_count.unwrap_or(0));
                    results.push((query, output));
                }
                Err(e) => {
                    println!("❌ Query failed: {}", e);
                    results.push((query, QueryOutput::error(&e)));
                }
            }
        }

        Ok(results)
    }

    /// Get schema information for a table
    pub fn get_table_schema(&self, keyspace: &str, table: &str) -> Result<QueryOutput, String> {
        let query = format!("DESCRIBE TABLE {}.{};", keyspace, table);
        self.execute_cql_query(&query)
    }

    /// List all keyspaces
    pub fn list_keyspaces(&self) -> Result<QueryOutput, String> {
        self.execute_cql_query("DESCRIBE KEYSPACES;")
    }

    /// List tables in a keyspace
    pub fn list_tables(&self, keyspace: &str) -> Result<QueryOutput, String> {
        let query = format!("USE {}; DESCRIBE TABLES;", keyspace);
        self.execute_cql_query(&query)
    }
}

/// Format cqlsh output for comparison
pub fn format_cqlsh_for_comparison(output: &QueryOutput) -> String {
    if output.has_error() {
        return format!("ERROR: {}", output.stderr);
    }

    if output.rows.is_empty() {
        return "No rows returned".to_string();
    }

    // Calculate column widths (same as cqlsh)
    let mut widths = vec![0; output.column_headers.len().max(
        output.rows.first().map(|r| r.len()).unwrap_or(0)
    )];

    // Calculate width based on headers
    for (i, header) in output.column_headers.iter().enumerate() {
        if i < widths.len() {
            widths[i] = header.len();
        }
    }

    // Expand width based on data
    for row in &output.rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let mut result = String::new();

    // Headers (left-aligned)
    if !output.column_headers.is_empty() {
        result.push(' ');
        for (i, header) in output.column_headers.iter().enumerate() {
            if i > 0 {
                result.push_str(" | ");
            }
            result.push_str(&format!("{:<width$}", header, width = widths[i]));
        }
        result.push('\n');

        // Separator line
        result.push('-');
        for (i, &width) in widths.iter().enumerate() {
            if i > 0 {
                result.push_str("-+-");
            }
            result.push_str(&"-".repeat(width));
        }
        result.push_str("-\n");
    }

    // Data rows (right-aligned)
    for row in &output.rows {
        result.push(' ');
        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                result.push_str(" | ");
            }
            if i < widths.len() {
                result.push_str(&format!("{:>width$}", cell, width = widths[i]));
            } else {
                result.push_str(cell);
            }
        }
        result.push('\n');
    }

    // Row count summary
    if let Some(count) = output.row_count {
        result.push('\n');
        result.push_str(&format!("({} rows)", count));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docker_manager_creation() {
        let config = CqlshConfig::default();
        let manager = DockerManager::new(config);
        assert_eq!(manager.config.container_name, "cassandra-node1");
    }

    #[test]
    fn test_cqlsh_output_parsing() {
        let docker_manager = DockerManager::new(CqlshConfig::default());
        let sample_output = r#"
 id                                   | data
--------------------------------------+--------
 a8f167f0-ebe7-4f20-a386-31ff138bec3b | test

(1 rows)
"#;
        
        let result = docker_manager.parse_cqlsh_output(sample_output, Duration::from_millis(100));
        assert!(result.is_ok());
        
        let output = result.unwrap();
        assert_eq!(output.column_headers.len(), 2);
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.row_count, Some(1));
    }

    #[test]
    fn test_format_cqlsh_for_comparison() {
        let mut output = QueryOutput::default();
        output.column_headers = vec!["id".to_string(), "name".to_string()];
        output.rows = vec![
            vec!["1".to_string(), "John".to_string()],
            vec!["2".to_string(), "Jane".to_string()],
        ];
        output.row_count = Some(2);

        let formatted = format_cqlsh_for_comparison(&output);
        assert!(formatted.contains("id | name"));
        assert!(formatted.contains("---+-----"));
        assert!(formatted.contains("(2 rows)"));
    }
}