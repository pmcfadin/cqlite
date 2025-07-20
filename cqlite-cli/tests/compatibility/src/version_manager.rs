use std::collections::HashMap;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};

/// Manages different Cassandra versions via Docker for compatibility testing
#[derive(Debug, Clone)]
pub struct CassandraVersionManager {
    pub supported_versions: Vec<String>,
    pub running_containers: HashMap<String, String>, // version -> container_id
    pub default_ports: HashMap<String, u16>, // version -> port
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionInfo {
    pub version: String,
    pub docker_image: String,
    pub port: u16,
    pub container_id: Option<String>,
    pub sstable_format: String,
    pub supported_features: Vec<String>,
}

impl CassandraVersionManager {
    pub fn new() -> Self {
        let mut default_ports = HashMap::new();
        default_ports.insert("4.0".to_string(), 9042);
        default_ports.insert("4.1".to_string(), 9043);
        default_ports.insert("5.0".to_string(), 9044);
        default_ports.insert("5.1".to_string(), 9045);
        default_ports.insert("6.0".to_string(), 9046);

        Self {
            supported_versions: vec![
                "4.0".to_string(),
                "4.1".to_string(), 
                "5.0".to_string(),
                "5.1".to_string(),
                "6.0".to_string(),
            ],
            running_containers: HashMap::new(),
            default_ports,
        }
    }

    /// Start a specific Cassandra version
    pub async fn start_version(&mut self, version: &str) -> Result<VersionInfo> {
        if !self.supported_versions.contains(&version.to_string()) {
            return Err(anyhow::anyhow!("Unsupported Cassandra version: {}", version));
        }

        let docker_image = self.get_docker_image(version);
        let port = self.default_ports[version];
        
        println!("🐳 Starting Cassandra {} on port {}", version, port);

        // Stop existing container if running
        if let Some(container_id) = self.running_containers.get(version) {
            self.stop_container(container_id).await?;
        }

        let container_id = self.run_cassandra_container(&docker_image, port, version).await?;
        
        // Wait for Cassandra to be ready
        self.wait_for_cassandra_ready(port).await?;
        
        let version_info = VersionInfo {
            version: version.to_string(),
            docker_image,
            port,
            container_id: Some(container_id.clone()),
            sstable_format: self.detect_sstable_format(version),
            supported_features: self.get_supported_features(version),
        };

        self.running_containers.insert(version.to_string(), container_id);
        
        println!("✅ Cassandra {} ready on port {}", version, port);
        Ok(version_info)
    }

    /// Stop a specific Cassandra version
    pub async fn stop_version(&mut self, version: &str) -> Result<()> {
        if let Some(container_id) = self.running_containers.remove(version) {
            self.stop_container(&container_id).await?;
            println!("🛑 Stopped Cassandra {}", version);
        }
        Ok(())
    }

    /// Stop all running Cassandra containers
    pub async fn stop_all(&mut self) -> Result<()> {
        for (version, container_id) in self.running_containers.drain() {
            if let Err(e) = self.stop_container(&container_id).await {
                eprintln!("⚠️ Failed to stop {}: {}", version, e);
            }
        }
        Ok(())
    }

    /// Run compatibility test across all versions
    pub async fn run_compatibility_matrix(&mut self) -> Result<Vec<CompatibilityResult>> {
        let mut results = Vec::new();
        
        for version in &self.supported_versions.clone() {
            println!("🧪 Testing Cassandra version {}", version);
            
            match self.test_version_compatibility(version).await {
                Ok(result) => {
                    println!("✅ {} compatibility: {}%", version, result.compatibility_score);
                    results.push(result);
                },
                Err(e) => {
                    eprintln!("❌ {} compatibility test failed: {}", version, e);
                    results.push(CompatibilityResult {
                        version: version.clone(),
                        compatibility_score: 0.0,
                        issues: vec![format!("Test failed: {}", e)],
                        sstable_parsing: false,
                        query_compatibility: false,
                        performance_ratio: None,
                    });
                }
            }
        }
        
        Ok(results)
    }

    async fn test_version_compatibility(&mut self, version: &str) -> Result<CompatibilityResult> {
        let version_info = self.start_version(version).await?;
        
        // Generate test data
        self.generate_test_data(&version_info).await?;
        
        // Test SSTable parsing
        let sstable_parsing = self.test_sstable_parsing(&version_info).await?;
        
        // Test query compatibility  
        let query_compatibility = self.test_query_compatibility(&version_info).await?;
        
        // Measure performance
        let performance_ratio = self.measure_performance(&version_info).await?;
        
        // Calculate overall compatibility score
        let mut score = 0.0;
        let mut issues = Vec::new();
        
        if sstable_parsing {
            score += 50.0;
        } else {
            issues.push("SSTable parsing failed".to_string());
        }
        
        if query_compatibility {
            score += 30.0;
        } else {
            issues.push("Query compatibility failed".to_string());
        }
        
        if let Some(ratio) = performance_ratio {
            if ratio > 0.8 { // Performance within 20% of baseline
                score += 20.0;
            } else {
                issues.push(format!("Performance degraded: {}% of baseline", (ratio * 100.0) as u32));
            }
        }
        
        self.stop_version(version).await?;
        
        Ok(CompatibilityResult {
            version: version.to_string(),
            compatibility_score: score,
            issues,
            sstable_parsing,
            query_compatibility,
            performance_ratio,
        })
    }

    async fn run_cassandra_container(&self, image: &str, port: u16, version: &str) -> Result<String> {
        let output = Command::new("docker")
            .args(&[
                "run", "-d",
                "--name", &format!("cassandra-{}", version),
                "-p", &format!("{}:9042", port),
                "-e", "CASSANDRA_START_RPC=true",
                "-e", "CASSANDRA_RPC_ADDRESS=0.0.0.0",
                "-e", "CASSANDRA_LISTEN_ADDRESS=auto",
                "-e", "CASSANDRA_BROADCAST_ADDRESS=127.0.0.1",
                "-e", "CASSANDRA_BROADCAST_RPC_ADDRESS=127.0.0.1",
                image
            ])
            .output()
            .context("Failed to start Cassandra container")?;

        if !output.status.success() {
            return Err(anyhow::anyhow!("Docker run failed: {}", 
                String::from_utf8_lossy(&output.stderr)));
        }

        let container_id = String::from_utf8(output.stdout)?
            .trim()
            .to_string();
        
        Ok(container_id)
    }

    async fn stop_container(&self, container_id: &str) -> Result<()> {
        Command::new("docker")
            .args(&["stop", container_id])
            .output()
            .context("Failed to stop container")?;
            
        Command::new("docker")
            .args(&["rm", container_id])
            .output()
            .context("Failed to remove container")?;
            
        Ok(())
    }

    async fn wait_for_cassandra_ready(&self, port: u16) -> Result<()> {
        println!("⏳ Waiting for Cassandra to be ready...");
        
        for attempt in 1..=30 { // Wait up to 5 minutes
            if self.check_cassandra_health(port).await {
                return Ok(());
            }
            
            if attempt % 5 == 0 {
                println!("⏳ Still waiting... (attempt {}/30)", attempt);
            }
            
            sleep(Duration::from_secs(10)).await;
        }
        
        Err(anyhow::anyhow!("Cassandra failed to start within timeout"))
    }

    async fn check_cassandra_health(&self, port: u16) -> bool {
        Command::new("cqlsh")
            .args(&[
                &format!("127.0.0.1:{}", port),
                "-e", "DESCRIBE KEYSPACES;"
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn get_docker_image(&self, version: &str) -> String {
        match version {
            "4.0" => "cassandra:4.0".to_string(),
            "4.1" => "cassandra:4.1".to_string(),
            "5.0" => "cassandra:5.0".to_string(),
            "5.1" => "cassandra:5.1".to_string(),
            "6.0" => "cassandra:6.0".to_string(),
            _ => format!("cassandra:{}", version),
        }
    }

    fn detect_sstable_format(&self, version: &str) -> String {
        match version {
            v if v.starts_with("4.0") => "big".to_string(),
            v if v.starts_with("4.1") => "big".to_string(),
            v if v.starts_with("5.0") => "big".to_string(),
            v if v.starts_with("5.1") => "big".to_string(),
            v if v.starts_with("6.0") => "big".to_string(), // May change in future
            _ => "unknown".to_string(),
        }
    }

    fn get_supported_features(&self, version: &str) -> Vec<String> {
        let mut features = vec![
            "basic_types".to_string(),
            "collections".to_string(),
            "udts".to_string(),
        ];
        
        match version {
            v if v >= "4.1" => {
                features.push("virtual_tables".to_string());
            },
            v if v >= "5.0" => {
                features.push("sai_indexes".to_string());
                features.push("vector_search".to_string());
            },
            _ => {}
        }
        
        features
    }

    async fn generate_test_data(&self, version_info: &VersionInfo) -> Result<()> {
        // This will be implemented in data_generator.rs
        println!("📊 Generating test data for Cassandra {}", version_info.version);
        Ok(())
    }

    async fn test_sstable_parsing(&self, version_info: &VersionInfo) -> Result<bool> {
        // This will be implemented in format_detective.rs
        println!("🔍 Testing SSTable parsing for Cassandra {}", version_info.version);
        Ok(true) // Placeholder
    }

    async fn test_query_compatibility(&self, version_info: &VersionInfo) -> Result<bool> {
        // This will be implemented in compatibility suite
        println!("🔍 Testing query compatibility for Cassandra {}", version_info.version);
        Ok(true) // Placeholder
    }

    async fn measure_performance(&self, version_info: &VersionInfo) -> Result<Option<f64>> {
        // This will be implemented in performance tests
        println!("⚡ Measuring performance for Cassandra {}", version_info.version);
        Ok(Some(0.95)) // Placeholder - 95% of baseline
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompatibilityResult {
    pub version: String,
    pub compatibility_score: f64, // 0-100
    pub issues: Vec<String>,
    pub sstable_parsing: bool,
    pub query_compatibility: bool,
    pub performance_ratio: Option<f64>, // Ratio compared to baseline version
}

impl CompatibilityResult {
    pub fn is_compatible(&self) -> bool {
        self.compatibility_score >= 95.0
    }
    
    pub fn status_emoji(&self) -> &str {
        match self.compatibility_score {
            score if score >= 95.0 => "✅",
            score if score >= 80.0 => "🟡", 
            _ => "❌",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_manager_creation() {
        let manager = CassandraVersionManager::new();
        assert!(manager.supported_versions.contains(&"4.0".to_string()));
        assert!(manager.supported_versions.contains(&"5.0".to_string()));
    }

    #[tokio::test]
    async fn test_compatibility_result() {
        let result = CompatibilityResult {
            version: "4.0".to_string(),
            compatibility_score: 98.0,
            issues: vec![],
            sstable_parsing: true,
            query_compatibility: true,
            performance_ratio: Some(0.96),
        };
        
        assert!(result.is_compatible());
        assert_eq!(result.status_emoji(), "✅");
    }
}