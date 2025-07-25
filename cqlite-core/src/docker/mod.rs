/// Docker integration module for running cqlsh commands in Cassandra containers
use std::process::{Command, Output};
use std::io::{self, Error, ErrorKind};
use serde::{Deserialize, Serialize};

/// Represents a Docker container
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerContainer {
    pub id: String,
    pub name: String,
    pub image: String,
}

/// Docker cqlsh client for executing queries in Cassandra containers
#[derive(Debug)]
pub struct DockerCqlshClient {
    container: DockerContainer,
}

impl DockerCqlshClient {
    /// Create a new Docker cqlsh client for a specific container
    pub fn new(container: DockerContainer) -> Self {
        Self { container }
    }

    /// Find the running Cassandra container
    pub fn find_cassandra_container() -> io::Result<DockerContainer> {
        let output = Command::new("docker")
            .args(&["ps", "--format", "{{.ID}}|{{.Names}}|{{.Image}}", "--filter", "status=running"])
            .output()?;

        if !output.status.success() {
            return Err(Error::new(
                ErrorKind::Other,
                "Failed to list Docker containers",
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        
        // Find Cassandra container
        for line in stdout.lines() {
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() == 3 {
                let image = parts[2];
                if image.contains("cassandra") {
                    return Ok(DockerContainer {
                        id: parts[0].to_string(),
                        name: parts[1].to_string(),
                        image: image.to_string(),
                    });
                }
            }
        }

        Err(Error::new(
            ErrorKind::NotFound,
            "No running Cassandra container found",
        ))
    }

    /// Execute a CQL query using cqlsh in the container
    pub fn execute_cql(&self, query: &str) -> io::Result<String> {
        let output = Command::new("docker")
            .args(&[
                "exec",
                "-i",
                &self.container.name,
                "cqlsh",
                "-e",
                query,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::new(
                ErrorKind::Other,
                format!("cqlsh command failed: {}", stderr),
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Execute a CQL query with host and port
    pub fn execute_cql_with_host(&self, query: &str, host: &str, port: u16) -> io::Result<String> {
        let output = Command::new("docker")
            .args(&[
                "exec",
                "-i",
                &self.container.name,
                "cqlsh",
                host,
                &port.to_string(),
                "-e",
                query,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::new(
                ErrorKind::Other,
                format!("cqlsh command failed: {}", stderr),
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Execute a CQL file in the container
    pub fn execute_cql_file(&self, file_content: &str) -> io::Result<String> {
        // Create a temporary file in the container
        let temp_file = "/tmp/query.cql";
        
        // Write the content to the container
        let mut child = Command::new("docker")
            .args(&[
                "exec",
                "-i",
                &self.container.name,
                "tee",
                temp_file,
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()?;

        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin.write_all(file_content.as_bytes())?;
        }

        child.wait()?;

        // Execute the file
        let output = Command::new("docker")
            .args(&[
                "exec",
                "-i",
                &self.container.name,
                "cqlsh",
                "-f",
                temp_file,
            ])
            .output()?;

        // Clean up
        let _ = Command::new("docker")
            .args(&[
                "exec",
                &self.container.name,
                "rm",
                temp_file,
            ])
            .output();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::new(
                ErrorKind::Other,
                format!("cqlsh command failed: {}", stderr),
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Check if the Cassandra container is ready
    pub fn is_ready(&self) -> bool {
        match self.execute_cql("SELECT now() FROM system.local;") {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    /// Wait for Cassandra to be ready (with timeout)
    pub fn wait_until_ready(&self, timeout_secs: u64) -> io::Result<()> {
        use std::time::{Duration, Instant};
        use std::thread;

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            if self.is_ready() {
                return Ok(());
            }
            thread::sleep(Duration::from_secs(1));
        }

        Err(Error::new(
            ErrorKind::TimedOut,
            "Cassandra container did not become ready in time",
        ))
    }

    /// Parse cqlsh output into structured format
    pub fn parse_cqlsh_output(output: &str) -> CqlshOutput {
        let lines: Vec<String> = output.lines().map(|s| s.to_string()).collect();
        
        // Find header and data separator
        let mut header_index = None;
        let mut separator_index = None;
        
        for (i, line) in lines.iter().enumerate() {
            if line.contains("---") && line.chars().all(|c| c == '-' || c == '+' || c.is_whitespace()) {
                separator_index = Some(i);
                if i > 0 {
                    header_index = Some(i - 1);
                }
                break;
            }
        }

        let (headers, rows) = if let (Some(h_idx), Some(s_idx)) = (header_index, separator_index) {
            let headers = parse_row(&lines[h_idx]);
            let rows: Vec<Vec<String>> = lines[(s_idx + 1)..]
                .iter()
                .filter(|line| !line.is_empty() && !line.contains("rows)"))
                .map(|line| parse_row(line))
                .collect();
            (headers, rows)
        } else {
            (vec![], vec![])
        };

        CqlshOutput {
            headers,
            rows,
            raw_output: output.to_string(),
        }
    }
}

/// Represents parsed cqlsh output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CqlshOutput {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub raw_output: String,
}

/// Parse a row from cqlsh output
fn parse_row(line: &str) -> Vec<String> {
    line.split('|')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
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
}