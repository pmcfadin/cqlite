//! Docker integration utilities for testing
//!
//! This module provides Docker-based testing infrastructure for CQLite,
//! including Cassandra container management and sstableloader integration.
//!
//! Only compiled when the `docker-integration` feature is enabled.

use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// Output from CQL shell command
#[derive(Debug, Clone)]
pub struct CqlshOutput {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub raw_output: String,
}

/// Docker CQL shell client for executing CQL queries in containers
#[derive(Debug)]
pub struct DockerCqlshClient {
    container: String,
}

impl DockerCqlshClient {
    /// Create a new Docker cqlsh client with a specific container
    pub fn new(container: String) -> Self {
        Self { container }
    }

    /// Find a running Cassandra container by name or image
    pub fn find_cassandra_container() -> io::Result<String> {
        // Try common container names first
        let common_names = [
            "cassandra",
            "cassandra-5-0",
            "cqlite-cassandra",
            "test-cassandra",
        ];

        for name in &common_names {
            let output = Command::new("docker")
                .args(["ps", "--filter", &format!("name={}", name), "--format", "{{.Names}}"])
                .output()?;

            if output.status.success() {
                let container = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .next()
                    .map(|s| s.to_string());

                if let Some(c) = container {
                    if !c.is_empty() {
                        return Ok(c);
                    }
                }
            }
        }

        // Try finding by image
        let output = Command::new("docker")
            .args([
                "ps",
                "--filter",
                "ancestor=cassandra:5.0",
                "--format",
                "{{.Names}}",
            ])
            .output()?;

        if output.status.success() {
            let container = String::from_utf8_lossy(&output.stdout)
                .lines()
                .next()
                .map(|s| s.to_string());

            if let Some(c) = container {
                if !c.is_empty() {
                    return Ok(c);
                }
            }
        }

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "No running Cassandra container found. Start one with: docker-compose -f test-data/docker/docker-compose-cassandra5.yml up -d",
        ))
    }

    /// Wait until Cassandra is ready to accept connections
    pub fn wait_until_ready(&self, timeout_secs: u32) -> io::Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs as u64);

        println!("⏳ Waiting for Cassandra to be ready (max {}s)...", timeout_secs);

        while start.elapsed() < timeout {
            // Check if Cassandra is accepting CQL connections
            let result = self.execute_cql("DESCRIBE KEYSPACES");

            match result {
                Ok(_) => {
                    println!("✅ Cassandra is ready ({:.1}s)", start.elapsed().as_secs_f64());
                    return Ok(());
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_secs(2));
                }
            }
        }

        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "Cassandra not ready after {}s. Container: {}",
                timeout_secs, self.container
            ),
        ))
    }

    /// Execute a CQL statement and return raw output
    pub fn execute_cql(&self, cql: &str) -> io::Result<String> {
        let output = Command::new("docker")
            .args([
                "exec",
                &self.container,
                "cqlsh",
                "-e",
                cql,
            ])
            .output()?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("CQL execution failed: {}", stderr),
            ))
        }
    }

    /// Parse cqlsh output into structured format
    pub fn parse_cqlsh_output(output: &str) -> CqlshOutput {
        let lines: Vec<&str> = output.lines().collect();
        let mut headers = Vec::new();
        let mut rows = Vec::new();

        // Find header line (contains column separators)
        let mut header_idx = None;
        for (i, line) in lines.iter().enumerate() {
            if line.contains('|') && !line.starts_with('-') {
                header_idx = Some(i);
                break;
            }
        }

        if let Some(idx) = header_idx {
            // Parse headers
            headers = lines[idx]
                .split('|')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            // Skip separator line and parse data rows
            for line in lines.iter().skip(idx + 2) {
                if line.starts_with('-') || line.trim().is_empty() {
                    continue;
                }
                if line.starts_with('(') {
                    // End of results (e.g., "(3 rows)")
                    break;
                }
                if line.contains('|') {
                    let row: Vec<String> = line
                        .split('|')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    if !row.is_empty() {
                        rows.push(row);
                    }
                }
            }
        }

        CqlshOutput {
            headers,
            rows,
            raw_output: output.to_string(),
        }
    }
}

/// Cassandra container manager for sstableloader integration tests
#[derive(Debug)]
pub struct CassandraContainer {
    /// Container name/ID
    container: String,
    /// Whether we started the container (vs found existing)
    _started_by_us: bool,
    /// CQL client for executing queries
    client: DockerCqlshClient,
}

impl CassandraContainer {
    /// Start a new Cassandra container or connect to an existing one
    pub fn start() -> io::Result<Self> {
        // First try to find an existing container
        if let Ok(container) = DockerCqlshClient::find_cassandra_container() {
            println!("🔗 Using existing Cassandra container: {}", container);
            let client = DockerCqlshClient::new(container.clone());
            return Ok(Self {
                container,
                _started_by_us: false,
                client,
            });
        }

        // Start a new container using docker-compose
        println!("🚀 Starting new Cassandra container...");

        let compose_file = Path::new("test-data/docker/docker-compose-cassandra5.yml");
        if !compose_file.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Docker compose file not found: {}",
                    compose_file.display()
                ),
            ));
        }

        let output = Command::new("docker-compose")
            .args(["-f", compose_file.to_str().unwrap(), "up", "-d", "cassandra-5-0"])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to start Cassandra: {}", stderr),
            ));
        }

        // Wait for container to start
        std::thread::sleep(Duration::from_secs(5));

        let container =
            DockerCqlshClient::find_cassandra_container().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "Failed to find started container")
            })?;

        let client = DockerCqlshClient::new(container.clone());

        Ok(Self {
            container,
            _started_by_us: true,
            client,
        })
    }

    /// Wait until Cassandra is ready
    pub fn wait_until_ready(&self, timeout_secs: u32) -> io::Result<()> {
        self.client.wait_until_ready(timeout_secs)
    }

    /// Execute CQL statement
    pub fn execute_cql(&self, cql: &str) -> io::Result<CqlshOutput> {
        let output = self.client.execute_cql(cql)?;
        Ok(DockerCqlshClient::parse_cqlsh_output(&output))
    }

    /// Copy SSTable files into the container
    pub fn copy_sstables(&self, local_dir: &Path, keyspace: &str, table: &str) -> io::Result<()> {
        // Target path inside container
        let container_path = format!(
            "/var/lib/cassandra/data/{}/{}/",
            keyspace, table
        );

        // Create target directory
        let mkdir_output = Command::new("docker")
            .args([
                "exec",
                &self.container,
                "mkdir",
                "-p",
                &container_path,
            ])
            .output()?;

        if !mkdir_output.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to create SSTable directory in container",
            ));
        }

        // Copy files
        let copy_output = Command::new("docker")
            .args([
                "cp",
                &format!("{}/*", local_dir.display()),
                &format!("{}:{}", self.container, container_path),
            ])
            .output()?;

        if !copy_output.status.success() {
            let stderr = String::from_utf8_lossy(&copy_output.stderr);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to copy SSTables: {}", stderr),
            ));
        }

        Ok(())
    }

    /// Run sstableloader to import SSTables
    pub fn run_sstableloader(
        &self,
        sstable_dir: &Path,
        keyspace: &str,
    ) -> io::Result<SstableloaderResult> {
        // First copy the SSTables to container
        self.copy_sstables(sstable_dir, keyspace, "temp_load")?;

        let container_path = format!("/var/lib/cassandra/data/{}/temp_load", keyspace);

        // Run sstableloader
        let output = Command::new("docker")
            .args([
                "exec",
                &self.container,
                "sstableloader",
                "-d",
                "localhost",
                &container_path,
            ])
            .output()?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        let result = SstableloaderResult {
            success: output.status.success(),
            stdout,
            stderr,
            exit_code: output.status.code(),
        };

        Ok(result)
    }

    /// Get the container name
    pub fn name(&self) -> &str {
        &self.container
    }
}

/// Result from running sstableloader
#[derive(Debug)]
pub struct SstableloaderResult {
    /// Whether the command succeeded
    pub success: bool,
    /// Standard output
    pub stdout: String,
    /// Standard error
    pub stderr: String,
    /// Exit code if available
    pub exit_code: Option<i32>,
}

impl SstableloaderResult {
    /// Check if the load was successful
    pub fn is_successful(&self) -> bool {
        self.success
    }

    /// Get summary message
    pub fn summary(&self) -> String {
        if self.success {
            format!("sstableloader completed successfully")
        } else {
            format!(
                "sstableloader failed (exit code: {:?}): {}",
                self.exit_code, self.stderr
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cqlsh_output() {
        let output = r#"
 id | name    | age
----+---------+-----
  1 | Alice   |  30
  2 | Bob     |  25
  3 | Charlie |  35

(3 rows)
"#;

        let parsed = DockerCqlshClient::parse_cqlsh_output(output);

        assert_eq!(parsed.headers, vec!["id", "name", "age"]);
        assert_eq!(parsed.rows.len(), 3);
        assert_eq!(parsed.rows[0], vec!["1", "Alice", "30"]);
        assert_eq!(parsed.rows[1], vec!["2", "Bob", "25"]);
        assert_eq!(parsed.rows[2], vec!["3", "Charlie", "35"]);
    }

    #[test]
    fn test_parse_empty_output() {
        let output = "(0 rows)";
        let parsed = DockerCqlshClient::parse_cqlsh_output(output);
        assert!(parsed.headers.is_empty());
        assert!(parsed.rows.is_empty());
    }
}
