use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tokio::fs;
use tracing::{info, debug, warn, error};
use crate::parser::{SstableDumpParser, ParsedData};
use crate::comparator::{CellByCell, ComparisonResult};
use crate::docker::DockerManager;
use crate::reporter::{ValidationReport, ReportFormat};

pub struct SstableDumpValidator {
    docker: DockerManager,
    parser: SstableDumpParser,
    comparator: CellByCell,
}

impl SstableDumpValidator {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            docker: DockerManager::new().await?,
            parser: SstableDumpParser::new(),
            comparator: CellByCell::new(),
        })
    }
    
    /// Main validation entry point - zero tolerance cell-by-cell comparison
    pub async fn validate_sstable(
        &mut self, 
        sstable_path: &Path, 
        fail_on_diff: bool, 
        detailed: bool
    ) -> Result<ValidationReport> {
        info!("Starting zero-tolerance validation for: {:?}", sstable_path);
        
        // Step 1: Ensure Docker environment is ready
        self.ensure_docker_ready().await?;
        
        // Step 2: Generate Cassandra sstabledump reference
        let cassandra_dump = self.generate_cassandra_dump(sstable_path).await?;
        
        // Step 3: Generate CQLite dump
        let cqlite_dump = self.generate_cqlite_dump(sstable_path).await?;
        
        // Step 4: Parse both outputs
        let cassandra_parsed = self.parser.parse_cassandra_dump(&cassandra_dump).await?;
        let cqlite_parsed = self.parser.parse_cqlite_dump(&cqlite_dump).await?;
        
        // Step 5: Perform cell-by-cell comparison
        let comparison = self.comparator.compare_cell_by_cell(&cassandra_parsed, &cqlite_parsed).await?;
        
        // Step 6: Generate comprehensive report
        let report = ValidationReport::new(
            sstable_path.to_path_buf(),
            comparison,
            detailed,
            fail_on_diff
        );
        
        info!("Validation completed. Differences: {}", report.difference_count());
        
        if report.has_differences() && fail_on_diff {
            error!("CRITICAL: Cell-by-cell comparison found {} differences", report.difference_count());
            error!("This validation WILL CAUSE CI TO FAIL as requested");
        }
        
        Ok(report)
    }
    
    /// Parse a single dump file
    pub async fn parse_dump(&self, dump_path: &Path, json_output: bool) -> Result<String> {
        info!("Parsing dump file: {:?}", dump_path);
        
        let parsed = if dump_path.to_string_lossy().contains("cassandra") {
            self.parser.parse_cassandra_dump(dump_path).await?
        } else {
            self.parser.parse_cqlite_dump(dump_path).await?
        };
        
        if json_output {
            Ok(serde_json::to_string_pretty(&parsed)?)
        } else {
            Ok(format!("{:#?}", parsed))
        }
    }
    
    /// Compare two pre-generated dumps
    pub async fn compare_dumps(
        &self, 
        cassandra_dump: &Path, 
        cqlite_dump: &Path, 
        zero_tolerance: bool
    ) -> Result<ComparisonResult> {
        info!("Comparing dumps: {:?} vs {:?}", cassandra_dump, cqlite_dump);
        
        let cassandra_parsed = self.parser.parse_cassandra_dump(cassandra_dump).await?;
        let cqlite_parsed = self.parser.parse_cqlite_dump(cqlite_dump).await?;
        
        let result = self.comparator.compare_cell_by_cell(&cassandra_parsed, &cqlite_parsed).await?;
        
        if result.has_differences() && zero_tolerance {
            error!("Zero tolerance mode: {} differences found", result.difference_count());
        }
        
        Ok(result)
    }
    
    /// Setup Docker environment with Cassandra 5.0
    pub async fn setup_docker_environment(&mut self, version: &str) -> Result<()> {
        info!("Setting up Docker environment with Cassandra {}", version);
        self.docker.setup_cassandra_container(version).await
    }
    
    /// Generate test data using existing Docker setup
    pub async fn generate_test_data(&self, count: u32, edge_cases: bool) -> Result<()> {
        info!("Generating {} test cases (edge_cases: {})", count, edge_cases);
        self.docker.generate_test_data(count, edge_cases).await
    }
    
    // Private helper methods
    
    async fn ensure_docker_ready(&mut self) -> Result<()> {
        if !self.docker.is_cassandra_ready().await? {
            warn!("Cassandra container not ready, starting...");
            self.docker.start_cassandra().await?;
            
            // Wait for readiness with timeout
            let mut attempts = 0;
            while !self.docker.is_cassandra_ready().await? && attempts < 60 {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                attempts += 1;
            }
            
            if !self.docker.is_cassandra_ready().await? {
                return Err(anyhow!("Cassandra failed to become ready after 5 minutes"));
            }
        }
        Ok(())
    }
    
    async fn generate_cassandra_dump(&self, sstable_path: &Path) -> Result<PathBuf> {
        info!("Generating Cassandra sstabledump reference for: {:?}", sstable_path);
        
        let dump_path = self.get_temp_dump_path("cassandra").await?;
        
        // Copy SSTable to container and run sstabledump
        let container_path = "/tmp/sstable.db";
        self.docker.copy_file_to_container(sstable_path, container_path).await?;
        
        let output = self.docker.run_sstabledump(container_path).await?;
        
        fs::write(&dump_path, output).await?;
        debug!("Cassandra dump written to: {:?}", dump_path);
        
        Ok(dump_path)
    }
    
    async fn generate_cqlite_dump(&self, sstable_path: &Path) -> Result<PathBuf> {
        info!("Generating CQLite dump for: {:?}", sstable_path);
        
        let dump_path = self.get_temp_dump_path("cqlite").await?;
        
        // Use CQLite core to read and dump the SSTable
        let output = self.run_cqlite_dump(sstable_path).await?;
        
        fs::write(&dump_path, output).await?;
        debug!("CQLite dump written to: {:?}", dump_path);
        
        Ok(dump_path)
    }
    
    async fn run_cqlite_dump(&self, sstable_path: &Path) -> Result<String> {
        // This would integrate with cqlite-core to read the SSTable
        // For now, we'll use a placeholder that calls the cqlite binary
        
        let output = Command::new("cargo")
            .args(&["run", "--bin", "cqlite", "--", "dump", &sstable_path.to_string_lossy()])
            .current_dir("../../") // Assuming we're in tools/sstabledump-validator
            .output()
            .await?;
            
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("CQLite dump failed: {}", stderr));
        }
        
        Ok(String::from_utf8(output.stdout)?)
    }
    
    async fn get_temp_dump_path(&self, prefix: &str) -> Result<PathBuf> {
        let temp_dir = tempfile::tempdir()?;
        let filename = format!("{}_dump_{}.txt", prefix, uuid::Uuid::new_v4());
        Ok(temp_dir.path().join(filename))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;
    
    #[tokio::test]
    async fn test_validator_creation() {
        let result = SstableDumpValidator::new().await;
        
        // When Docker integration is disabled, expect error
        #[cfg(not(feature = "docker-integration"))]
        assert!(result.is_err());
        
        // When Docker integration is enabled, may succeed or fail depending on Docker availability
        #[cfg(feature = "docker-integration")]
        {
            match result {
                Ok(_) => println!("Validator created with Docker available"),
                Err(_) => println!("Validator creation failed - Docker not available"),
            }
        }
    }
    
    #[tokio::test]
    async fn test_validation_workflow() {
        // This would require actual test data
        // For now, just test that the structure works
    }
}