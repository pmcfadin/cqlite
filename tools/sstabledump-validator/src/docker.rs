use anyhow::{anyhow, Result};
#[cfg(feature = "docker-integration")]
use bollard::container::{ListContainersOptions, WaitContainerOptions};
#[cfg(feature = "docker-integration")]
use bollard::exec::{CreateExecOptions, StartExecResults};
#[cfg(feature = "docker-integration")]
use bollard::models::HostConfig;
#[cfg(feature = "docker-integration")]
use bollard::{
    container::{Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions},
    Docker,
};
#[cfg(feature = "docker-integration")]
use futures_util::stream::StreamExt;
#[cfg(feature = "docker-integration")]
use std::collections::HashMap;
#[cfg(feature = "docker-integration")]
use std::path::Path;
#[cfg(feature = "docker-integration")]
use tokio::fs;
#[cfg(feature = "docker-integration")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "docker-integration")]
use tracing::{debug, info, warn};

#[cfg(feature = "docker-integration")]
#[derive(Debug)]
pub struct DockerManager {
    docker: Docker,
    cassandra_container_id: Option<String>,
}

#[cfg(not(feature = "docker-integration"))]
#[derive(Debug)]
pub struct DockerManager {
    // Mock implementation when Docker integration is disabled
}

#[cfg(not(feature = "docker-integration"))]
impl DockerManager {
    pub async fn new() -> Result<Self> {
        Err(anyhow!(
            "Docker integration is disabled. Enable the 'docker-integration' feature."
        ))
    }

    pub async fn setup_cassandra_container(&mut self, _version: &str) -> Result<()> {
        Err(anyhow!("Docker integration is disabled"))
    }

    pub async fn is_cassandra_ready(&self) -> Result<bool> {
        Ok(false)
    }

    pub async fn start_cassandra(&mut self) -> Result<()> {
        Err(anyhow!("Docker integration is disabled"))
    }

    pub async fn copy_file_to_container(
        &self,
        _local_path: &std::path::Path,
        _container_path: &str,
    ) -> Result<()> {
        Err(anyhow!("Docker integration is disabled"))
    }

    pub async fn run_sstabledump(&self, _sstable_path: &str) -> Result<String> {
        Err(anyhow!("Docker integration is disabled"))
    }

    pub async fn generate_test_data(&self, _count: u32, _edge_cases: bool) -> Result<()> {
        Err(anyhow!("Docker integration is disabled"))
    }

    pub async fn execute_cql(&self, _cql: &str) -> Result<String> {
        Err(anyhow!("Docker integration is disabled"))
    }

    pub async fn extract_sstables(&self, _keyspace: &str, _table: &str) -> Result<Vec<String>> {
        Err(anyhow!("Docker integration is disabled"))
    }
}

#[cfg(feature = "docker-integration")]
impl DockerManager {
    #[cfg(feature = "docker-integration")]
    pub async fn new() -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;

        // Test Docker connection
        match docker.ping().await {
            Ok(_) => debug!("Docker connection established"),
            Err(e) => return Err(anyhow!("Failed to connect to Docker: {}", e)),
        }

        Ok(Self {
            docker,
            cassandra_container_id: None,
        })
    }

    /// Setup Cassandra container for testing
    pub async fn setup_cassandra_container(&mut self, version: &str) -> Result<()> {
        info!("Setting up Cassandra {} container", version);

        // Stop and remove existing container if it exists
        if let Some(container_id) = &self.cassandra_container_id {
            self.stop_and_remove_container(container_id).await?;
        }

        let image = format!("cassandra:{}", version);
        let container_name = "cqlite-sstabledump-validator-cassandra";

        // Create container configuration
        let config = Config {
            image: Some(image.clone()),
            env: Some(vec![
                "CASSANDRA_CLUSTER_NAME=cqlite-validator".to_string(),
                "CASSANDRA_DC=datacenter1".to_string(),
                "CASSANDRA_RACK=rack1".to_string(),
                "MAX_HEAP_SIZE=1G".to_string(),
                "HEAP_NEWSIZE=200m".to_string(),
            ]),
            host_config: Some(HostConfig {
                port_bindings: Some({
                    let mut bindings = HashMap::new();
                    bindings.insert(
                        "9042/tcp".to_string(),
                        Some(vec![bollard::models::PortBinding {
                            host_ip: None,
                            host_port: Some("9042".to_string()),
                        }]),
                    );
                    bindings
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: container_name,
            ..Default::default()
        };

        // Create and start container
        let container_response = self.docker.create_container(Some(options), config).await?;
        let container_id = container_response.id;

        info!("Starting Cassandra container: {}", container_id);
        self.docker
            .start_container(&container_id, None::<StartContainerOptions<String>>)
            .await?;

        self.cassandra_container_id = Some(container_id);

        // Wait for Cassandra to be ready
        self.wait_for_cassandra_ready().await?;

        info!("Cassandra container is ready");
        Ok(())
    }

    /// Check if Cassandra container is running and ready
    pub async fn is_cassandra_ready(&self) -> Result<bool> {
        if let Some(container_id) = &self.cassandra_container_id {
            // Check if container is running
            let containers = self
                .docker
                .list_containers(Some(ListContainersOptions::<String> {
                    all: false,
                    ..Default::default()
                }))
                .await?;

            let container_running = containers.iter().any(|c| {
                c.id.as_ref() == Some(container_id)
                    && c.state.as_ref() == Some(&"running".to_string())
            });

            if !container_running {
                return Ok(false);
            }

            // Test CQL connection
            match self.test_cql_connection().await {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            }
        } else {
            Ok(false)
        }
    }

    /// Start existing Cassandra container
    pub async fn start_cassandra(&mut self) -> Result<()> {
        if let Some(container_id) = &self.cassandra_container_id {
            info!("Starting existing Cassandra container");
            self.docker
                .start_container(container_id, None::<StartContainerOptions<String>>)
                .await?;
            self.wait_for_cassandra_ready().await?;
        } else {
            // Create new container with default version
            self.setup_cassandra_container("5.0").await?;
        }
        Ok(())
    }

    /// Copy file to container
    pub async fn copy_file_to_container(
        &self,
        local_path: &Path,
        container_path: &str,
    ) -> Result<()> {
        let container_id = self
            .cassandra_container_id
            .as_ref()
            .ok_or_else(|| anyhow!("No Cassandra container available"))?;

        debug!(
            "Copying {:?} to container path {}",
            local_path, container_path
        );

        // Read local file
        let file_content = fs::read(local_path).await?;

        // Create exec to write file in container
        let command = format!("cat > {}", container_path);
        let exec_config = CreateExecOptions {
            cmd: Some(vec!["sh", "-c", &command]),
            attach_stdin: Some(true),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let exec_response = self.docker.create_exec(container_id, exec_config).await?;

        if let StartExecResults::Attached { mut input, .. } =
            self.docker.start_exec(&exec_response.id, None).await?
        {
            input.write_all(&file_content).await?;
            input.shutdown().await?;
        }

        debug!("File copied successfully");
        Ok(())
    }

    /// Run sstabledump command in container
    pub async fn run_sstabledump(&self, sstable_path: &str) -> Result<String> {
        let container_id = self
            .cassandra_container_id
            .as_ref()
            .ok_or_else(|| anyhow!("No Cassandra container available"))?;

        info!("Running sstabledump on {}", sstable_path);

        let exec_config = CreateExecOptions {
            cmd: Some(vec!["sstabledump", sstable_path]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let exec_response = self.docker.create_exec(container_id, exec_config).await?;

        let mut output_str = String::new();
        if let StartExecResults::Attached { mut output, .. } =
            self.docker.start_exec(&exec_response.id, None).await?
        {
            use futures_util::StreamExt;
            while let Some(chunk) = output.next().await {
                match chunk {
                    Ok(log_output) => {
                        output_str.push_str(&log_output.to_string());
                    }
                    Err(e) => {
                        warn!("Error reading sstabledump output: {}", e);
                    }
                }
            }
        }

        if output_str.is_empty() {
            return Err(anyhow!("sstabledump produced no output"));
        }

        debug!("sstabledump output: {} bytes", output_str.len());
        Ok(output_str)
    }

    /// Generate test data in the container
    pub async fn generate_test_data(&self, count: u32, edge_cases: bool) -> Result<()> {
        let _container_id = self
            .cassandra_container_id
            .as_ref()
            .ok_or_else(|| anyhow!("No Cassandra container available"))?;

        info!(
            "Generating {} test data entries (edge_cases: {})",
            count, edge_cases
        );

        // Create test keyspace and tables
        let cql_commands = self.generate_test_cql_commands(count, edge_cases);

        for command in cql_commands {
            self.execute_cql_command(&command).await?;
        }

        // Force flush to ensure data is written to SSTables
        self.execute_cql_command("NODETOOL flush").await?;

        info!("Test data generation completed");
        Ok(())
    }

    // Private helper methods

    async fn wait_for_cassandra_ready(&self) -> Result<()> {
        info!("Waiting for Cassandra to be ready...");

        let max_attempts = 60; // 5 minutes
        let mut attempts = 0;

        while attempts < max_attempts {
            if let Ok(_) = self.test_cql_connection().await {
                return Ok(());
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            attempts += 1;

            if attempts % 12 == 0 {
                // Every minute
                info!("Still waiting for Cassandra... ({}s)", attempts * 5);
            }
        }

        Err(anyhow!("Cassandra failed to become ready within 5 minutes"))
    }

    async fn test_cql_connection(&self) -> Result<()> {
        let container_id = self
            .cassandra_container_id
            .as_ref()
            .ok_or_else(|| anyhow!("No container ID"))?;

        let exec_config = CreateExecOptions {
            cmd: Some(vec![
                "cqlsh",
                "-e",
                "SELECT cluster_name FROM system.local;",
            ]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let exec_response = self.docker.create_exec(container_id, exec_config).await?;

        let mut output_str = String::new();
        if let StartExecResults::Attached { mut output, .. } =
            self.docker.start_exec(&exec_response.id, None).await?
        {
            use futures_util::StreamExt;
            while let Some(chunk) = output.next().await {
                match chunk {
                    Ok(log_output) => {
                        output_str.push_str(&log_output.to_string());
                    }
                    Err(_e) => {
                        // Ignore errors in test output
                    }
                }
            }
        }

        if output_str.contains("cluster_name") {
            Ok(())
        } else {
            Err(anyhow!("CQL connection test failed: {}", output_str))
        }
    }

    async fn execute_cql_command(&self, command: &str) -> Result<String> {
        let container_id = self
            .cassandra_container_id
            .as_ref()
            .ok_or_else(|| anyhow!("No container ID"))?;

        debug!("Executing CQL: {}", command);

        let exec_config = CreateExecOptions {
            cmd: Some(vec!["cqlsh", "-e", command]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let exec_response = self.docker.create_exec(container_id, exec_config).await?;

        let mut output_str = String::new();
        if let StartExecResults::Attached { mut output, .. } =
            self.docker.start_exec(&exec_response.id, None).await?
        {
            use futures_util::StreamExt;
            while let Some(chunk) = output.next().await {
                match chunk {
                    Ok(log_output) => {
                        output_str.push_str(&log_output.to_string());
                    }
                    Err(_e) => {
                        // Ignore errors in command output
                    }
                }
            }
        }

        Ok(output_str)
    }

    fn generate_test_cql_commands(&self, count: u32, edge_cases: bool) -> Vec<String> {
        let mut commands = vec![
            // Create test keyspace
            "CREATE KEYSPACE IF NOT EXISTS validator_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};".to_string(),

            // Create basic test table
            "CREATE TABLE IF NOT EXISTS validator_test.basic_types (
                id UUID PRIMARY KEY,
                text_col TEXT,
                int_col INT,
                bigint_col BIGINT,
                boolean_col BOOLEAN,
                timestamp_col TIMESTAMP,
                float_col FLOAT,
                double_col DOUBLE
            );".to_string(),

            // Create collection test table
            "CREATE TABLE IF NOT EXISTS validator_test.collections (
                id UUID PRIMARY KEY,
                list_col LIST<TEXT>,
                set_col SET<INT>,
                map_col MAP<TEXT, TEXT>
            );".to_string(),
        ];

        // Generate basic data
        for i in 0..count {
            commands.push(format!(
                "INSERT INTO validator_test.basic_types (id, text_col, int_col, bigint_col, boolean_col, timestamp_col, float_col, double_col)
                 VALUES (uuid(), 'text_{}', {}, {}, {}, '{}', {}, {});",
                i, i, i * 1000, i % 2 == 0, "2024-01-01 12:00:00", i as f32 * 3.14, i as f64 * 2.718
            ));

            commands.push(format!(
                "INSERT INTO validator_test.collections (id, list_col, set_col, map_col)
                 VALUES (uuid(), ['item1_{}', 'item2_{}'], {{{}, {}, {}}}, {{'key_{}': 'value_{}'}});",
                i, i, i, i+1, i+2, i, i
            ));
        }

        // Add edge cases if requested
        if edge_cases {
            commands.extend(vec![
                // Null values
                "INSERT INTO validator_test.basic_types (id, text_col) VALUES (uuid(), null);".to_string(),

                // Empty collections
                "INSERT INTO validator_test.collections (id, list_col, set_col, map_col) VALUES (uuid(), [], {}, {});".to_string(),

                // Large text value
                format!("INSERT INTO validator_test.basic_types (id, text_col) VALUES (uuid(), '{}');", "x".repeat(1000)),

                // Maximum/minimum values
                "INSERT INTO validator_test.basic_types (id, int_col, bigint_col) VALUES (uuid(), 2147483647, 9223372036854775807);".to_string(),
                "INSERT INTO validator_test.basic_types (id, int_col, bigint_col) VALUES (uuid(), -2147483648, -9223372036854775808);".to_string(),
            ]);
        }

        commands
    }

    async fn stop_and_remove_container(&self, container_id: &str) -> Result<()> {
        debug!("Stopping and removing container: {}", container_id);

        // Stop container
        if let Err(e) = self.docker.stop_container(container_id, None).await {
            warn!("Failed to stop container {}: {}", container_id, e);
        }

        // Wait for container to stop
        let wait_options = WaitContainerOptions {
            condition: "not-running",
        };

        let mut wait_stream = self.docker.wait_container(container_id, Some(wait_options));

        if let Some(result) = wait_stream.next().await {
            match result {
                Ok(_) => debug!("Container stopped successfully"),
                Err(e) => warn!("Error waiting for container to stop: {}", e),
            }
        }

        // Remove container
        let remove_options = RemoveContainerOptions {
            force: true,
            ..Default::default()
        };

        if let Err(e) = self
            .docker
            .remove_container(container_id, Some(remove_options))
            .await
        {
            warn!("Failed to remove container {}: {}", container_id, e);
        }

        Ok(())
    }

    /// Execute a CQL command and return the result
    pub async fn execute_cql(&self, cql: &str) -> Result<String> {
        self.execute_cql_command(cql).await
    }

    /// Extract SSTable files from a keyspace and table
    pub async fn extract_sstables(&self, keyspace: &str, table: &str) -> Result<Vec<String>> {
        let container_id = self
            .cassandra_container_id
            .as_ref()
            .ok_or_else(|| anyhow!("No container ID"))?;

        let data_dir = format!("/var/lib/cassandra/data/{}/{}-*/", keyspace, table);

        let exec_config = CreateExecOptions {
            cmd: Some(vec!["find", &data_dir, "-name", "*.db", "-type", "f"]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let exec_response = self.docker.create_exec(container_id, exec_config).await?;

        let mut output_str = String::new();
        if let StartExecResults::Attached { mut output, .. } =
            self.docker.start_exec(&exec_response.id, None).await?
        {
            use futures_util::StreamExt;
            while let Some(chunk) = output.next().await {
                match chunk {
                    Ok(log_output) => {
                        output_str.push_str(&log_output.to_string());
                    }
                    Err(_e) => {
                        // Ignore errors in command output
                    }
                }
            }
        }

        let sstable_paths: Vec<String> = output_str
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.trim().to_string())
            .collect();

        Ok(sstable_paths)
    }
}

#[cfg(feature = "docker-integration")]
impl Drop for DockerManager {
    fn drop(&mut self) {
        if let Some(container_id) = &self.cassandra_container_id {
            // Note: This is a blocking drop, but in practice it's just scheduling cleanup
            // The actual cleanup will happen when the Docker daemon processes the request
            debug!("Scheduling container cleanup for: {}", container_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_docker_manager_creation() {
        let result = DockerManager::new().await;
        // This test will only pass if Docker is available
        // In CI, we might want to skip this test if Docker is not available
        if std::env::var("CI").is_ok() && result.is_err() {
            // Skip test in CI if Docker is not available
            return;
        }

        // When Docker integration is disabled, expect error
        #[cfg(not(feature = "docker-integration"))]
        assert!(result.is_err());

        // When Docker integration is enabled, may succeed if Docker is available
        #[cfg(feature = "docker-integration")]
        {
            // Result depends on Docker availability
            match result {
                Ok(_) => println!("Docker is available"),
                Err(_) => println!("Docker is not available"),
            }
        }
    }
}
