//! Cassandra Compatibility Testing Framework for CQLite
//! 
//! This module provides comprehensive testing capabilities to ensure CQLite
//! remains compatible with different Cassandra versions as they evolve.

pub mod version_manager;
pub mod format_detective;
pub mod data_generator;
pub mod suite;

pub use version_manager::{CassandraVersionManager, VersionInfo, CompatibilityResult};
pub use format_detective::{FormatDetective, FormatDiff, SSTableFormat};
pub use data_generator::{TestDataGenerator, GeneratedDataSet, TestSchema};
pub use suite::{CompatibilityTestSuite, CompatibilityReport, SuiteResult};

use std::path::PathBuf;
use anyhow::Result;

/// Main entry point for running compatibility tests
pub async fn run_compatibility_tests(output_dir: PathBuf) -> Result<CompatibilityReport> {
    let mut suite = CompatibilityTestSuite::new(output_dir);
    suite.run_full_compatibility_suite().await
}

/// Quick compatibility check for a specific version
pub async fn quick_compatibility_check(version: &str, output_dir: PathBuf) -> Result<CompatibilityResult> {
    let mut version_manager = CassandraVersionManager::new();
    version_manager.test_version_compatibility(version).await
}

/// Detect format changes in new SSTable files
pub async fn detect_format_changes(sstable_files: Vec<PathBuf>) -> Result<Vec<String>> {
    let detective = FormatDetective::new();
    let mut changes = Vec::new();
    
    for file in sstable_files {
        let format = detective.analyze_sstable_format(&file).await?;
        changes.push(format.version);
    }
    
    Ok(changes)
}