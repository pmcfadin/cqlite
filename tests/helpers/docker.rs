//! Docker integration utilities for testing.
//!
//! This module provides Docker-based testing infrastructure for CQLite,
//! including Cassandra container management and `sstableloader` integration.
//!
//! Only compiled when the `docker-integration` feature is enabled.

use std::io;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const NODETOOL_BIN: &str = "/opt/cassandra/bin/nodetool";
const SSTABLELOADER_BIN: &str = "/opt/cassandra/bin/sstableloader";
const SYSTEM_LOG_PATH: &str = "/opt/cassandra/logs/system.log";

/// Output from a `cqlsh` command.
#[derive(Debug, Clone)]
pub struct CqlshOutput {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    #[allow(dead_code)]
    pub raw_output: String,
}

/// Docker-backed `cqlsh` client for executing queries in a Cassandra container.
#[derive(Debug)]
pub struct DockerCqlshClient {
    container: String,
}

impl DockerCqlshClient {
    /// Create a new client for a specific container name or ID.
    pub fn new(container: String) -> Self {
        Self { container }
    }

    /// Find a running Cassandra 5.0 container.
    pub fn find_cassandra_container() -> io::Result<String> {
        if let Ok(container) = std::env::var("CQLITE_CASSANDRA_CONTAINER") {
            let trimmed = container.trim();
            if !trimmed.is_empty() && Self::container_is_running(trimmed)? {
                return Ok(trimmed.to_string());
            }
        }

        let common_names = ["cqlite-cassandra-5-0", "cassandra-5-0"];

        for name in &common_names {
            let output = Self::docker_output(&[
                "ps",
                "--filter",
                &format!("name={name}"),
                "--format",
                "{{.Names}}",
            ])?;

            if output.status.success() {
                if let Some(container) = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .find(|line| !line.trim().is_empty())
                {
                    return Ok(container.trim().to_string());
                }
            }
        }

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "No running Cassandra container found. Start one with: docker compose -f test-data/docker/docker-compose-cassandra5.yml up -d cassandra-5-0",
        ))
    }

    /// Wait until Cassandra is ready to accept CQL queries.
    pub fn wait_until_ready(&self, timeout_secs: u32) -> io::Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs as u64);

        while start.elapsed() < timeout {
            let readiness = Self::docker_output(&[
                "exec",
                &self.container,
                "sh",
                "-lc",
                &format!(
                    "cqlsh -e \"SELECT cluster_name FROM system.local;\" >/dev/null 2>&1 && {NODETOOL_BIN} status | grep -q 'UN'"
                ),
            ]);

            if let Ok(output) = readiness {
                if output.status.success() {
                    return Ok(());
                }
            }

            if !Self::container_is_running(&self.container)? {
                std::thread::sleep(Duration::from_secs(2));
                continue;
            }

            if self.execute_cql("SELECT cluster_name FROM system.local;").is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_secs(2));
        }

        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "Cassandra not ready after {timeout_secs}s in container {}",
                self.container
            ),
        ))
    }

    /// Execute a CQL statement and return raw stdout.
    pub fn execute_cql(&self, cql: &str) -> io::Result<String> {
        let output = Self::docker_output(&["exec", &self.container, "cqlsh", "-e", cql])?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(io::Error::other(format!(
                "CQL execution failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )))
        }
    }

    /// Parse `cqlsh` tabular output into a structured format.
    pub fn parse_cqlsh_output(output: &str) -> CqlshOutput {
        let lines: Vec<&str> = output.lines().collect();
        let mut headers = Vec::new();
        let mut rows = Vec::new();

        let mut header_idx = None;
        for (idx, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            if trimmed.contains('|') && !trimmed.starts_with('-') {
                header_idx = Some(idx);
                break;
            }
        }

        if let Some(idx) = header_idx {
            headers = lines[idx]
                .split('|')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect();

            for line in lines.iter().skip(idx + 2) {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('-') {
                    continue;
                }
                if trimmed.starts_with('(') {
                    break;
                }
                if trimmed.contains('|') {
                    let row = trimmed
                        .split('|')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>();
                    if !row.is_empty() {
                        rows.push(row);
                    }
                }
            }
        } else {
            let non_empty_lines = lines
                .iter()
                .map(|line| line.trim())
                .filter(|line| !line.is_empty())
                .collect::<Vec<_>>();

            if non_empty_lines.len() >= 2
                && non_empty_lines[1].chars().all(|ch| ch == '-')
                && !non_empty_lines[0].starts_with('(')
            {
                headers.push(non_empty_lines[0].to_string());

                for line in non_empty_lines.into_iter().skip(2) {
                    if line.starts_with('(') {
                        break;
                    }
                    rows.push(vec![line.to_string()]);
                }
            }
        }

        CqlshOutput {
            headers,
            rows,
            raw_output: output.to_string(),
        }
    }

    fn docker_output(args: &[&str]) -> io::Result<Output> {
        Command::new("docker").args(args).output()
    }

    fn container_is_running(container: &str) -> io::Result<bool> {
        let output =
            Self::docker_output(&["inspect", "-f", "{{.State.Running}}", container])?;
        Ok(output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "true")
    }
}

/// Cassandra container manager for `sstableloader` integration tests.
#[derive(Debug)]
pub struct CassandraContainer {
    container: String,
    _started_by_us: bool,
    client: DockerCqlshClient,
}

impl CassandraContainer {
    /// Start a new Cassandra container or attach to an existing one.
    pub fn start() -> io::Result<Self> {
        let container = DockerCqlshClient::find_cassandra_container()?;
        let client = DockerCqlshClient::new(container.clone());

        Ok(Self {
            container,
            _started_by_us: false,
            client,
        })
    }

    /// Wait until Cassandra is ready.
    pub fn wait_until_ready(&self, timeout_secs: u32) -> io::Result<()> {
        self.client.wait_until_ready(timeout_secs)
    }

    /// Execute CQL and parse the output.
    pub fn execute_cql(&self, cql: &str) -> io::Result<CqlshOutput> {
        let output = self.client.execute_cql(cql)?;
        Ok(DockerCqlshClient::parse_cqlsh_output(&output))
    }

    /// Resolve Cassandra's on-disk table directory name (`table-<id_without_dashes>`).
    pub fn table_directory_name(&self, keyspace: &str, table: &str) -> io::Result<String> {
        let cql = format!(
            "SELECT id FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{table}';"
        );
        let start = Instant::now();
        let timeout = Duration::from_secs(20);

        let table_id = loop {
            let result = self.execute_cql(&cql)?;
            if let Some(id) = result.rows.first().and_then(|row| row.first()) {
                break id.clone();
            }

            if start.elapsed() >= timeout {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Could not resolve table id for {keyspace}.{table}"),
                ));
            }

            std::thread::sleep(Duration::from_millis(250));
        };

        Ok(format!("{}-{}", table, table_id.replace('-', "")))
    }

    /// Run `sstableloader` against a packaged keyspace/table directory.
    pub fn run_sstableloader(
        &self,
        local_table_dir: &Path,
        keyspace: &str,
        table: &str,
    ) -> io::Result<SstableloaderResult> {
        let table_dir_name = self.table_directory_name(keyspace, table)?;
        let remote_dir = format!("/tmp/cqlite-loader/{keyspace}/{table_dir_name}");
        let contact_host = self.hostname()?;
        self.copy_sstables(local_table_dir, &remote_dir)?;

        let output = DockerCqlshClient::docker_output(&[
            "exec",
            &self.container,
            "sh",
            "-lc",
            &format!(
                "MAX_HEAP_SIZE=128M HEAP_NEWSIZE=32M {SSTABLELOADER_BIN} -d {contact_host} {remote_dir}"
            ),
        ])?;

        Ok(SstableloaderResult {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code(),
        })
    }

    /// Get the container name or ID.
    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.container
    }

    pub fn tail_system_log(&self, lines: usize) -> io::Result<String> {
        let output = DockerCqlshClient::docker_output(&[
            "exec",
            &self.container,
            "sh",
            "-lc",
            &format!("tail -n {lines} {SYSTEM_LOG_PATH}"),
        ])?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(io::Error::other(format!(
                "Failed to tail Cassandra system log: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )))
        }
    }

    pub fn list_table_components(&self, keyspace: &str, table: &str) -> io::Result<String> {
        let table_dir_name = self.table_directory_name(keyspace, table)?;
        let table_dir = format!("/var/lib/cassandra/data/{keyspace}/{table_dir_name}");
        let output = DockerCqlshClient::docker_output(&[
            "exec",
            &self.container,
            "sh",
            "-lc",
            &format!("if [ -d {table_dir} ]; then ls -1 {table_dir} | sort; fi"),
        ])?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(io::Error::other(format!(
                "Failed to list imported table components: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )))
        }
    }

    fn copy_sstables(&self, local_dir: &Path, remote_dir: &str) -> io::Result<()> {
        if !local_dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Local SSTable directory does not exist: {}", local_dir.display()),
            ));
        }

        let cleanup = DockerCqlshClient::docker_output(&[
            "exec",
            &self.container,
            "sh",
            "-lc",
            &format!("rm -rf {remote_dir} && mkdir -p {remote_dir}"),
        ])?;
        if !cleanup.status.success() {
            return Err(io::Error::other(format!(
                "Failed to prepare remote SSTable directory: {}",
                String::from_utf8_lossy(&cleanup.stderr).trim()
            )));
        }

        let source = format!("{}/.", local_dir.display());
        let destination = format!("{}:{}", self.container, remote_dir);
        let copy = DockerCqlshClient::docker_output(&["cp", &source, &destination])?;
        if !copy.status.success() {
            return Err(io::Error::other(format!(
                "Failed to copy SSTables: {}",
                String::from_utf8_lossy(&copy.stderr).trim()
            )));
        }

        Ok(())
    }

    fn hostname(&self) -> io::Result<String> {
        let output =
            DockerCqlshClient::docker_output(&["inspect", "-f", "{{.Config.Hostname}}", &self.container])?;

        if !output.status.success() {
            return Err(io::Error::other(format!(
                "Failed to inspect container hostname: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        let hostname = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if hostname.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Container hostname is empty",
            ));
        }

        Ok(hostname)
    }
}

/// Result from running `sstableloader`.
#[derive(Debug)]
pub struct SstableloaderResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
}

impl SstableloaderResult {
    pub fn is_successful(&self) -> bool {
        self.success
    }

    pub fn summary(&self) -> String {
        if self.success {
            "sstableloader completed successfully".to_string()
        } else {
            format!(
                "sstableloader failed (exit code: {:?}): stdout=`{}` stderr=`{}`",
                self.exit_code,
                self.stdout.trim(),
                self.stderr.trim()
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

    #[test]
    fn test_parse_single_column_output() {
        let output = r#"
 id
--------------------------------------
 a6811200-1b7b-11f1-bc4b-1d58099c4029

(1 rows)
"#;

        let parsed = DockerCqlshClient::parse_cqlsh_output(output);
        assert_eq!(parsed.headers, vec!["id"]);
        assert_eq!(
            parsed.rows,
            vec![vec!["a6811200-1b7b-11f1-bc4b-1d58099c4029".to_string()]]
        );
    }
}
