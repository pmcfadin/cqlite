#!/usr/bin/env cargo
//! cqlite Integration Test
//! 
//! This program attempts to use the actual cqlite-core library to parse
//! real Cassandra SSTable files and validate the parsing results.

use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::Instant;

// We'll try to use cqlite-core once we add it as a dependency
// For now, let's create a simple test that examines file structure

#[derive(Debug, Clone)]
pub struct SSTableFileInfo {
    pub filename: String,
    pub size_bytes: u64,
    pub table_name: String,
    pub files_in_directory: Vec<String>,
}

#[derive(Debug)]
pub struct IntegrationTestResult {
    pub total_files_found: usize,
    pub data_files: Vec<SSTableFileInfo>,
    pub index_files: usize,
    pub statistics_files: usize,
    pub other_files: usize,
    pub total_size_mb: f64,
}

pub struct CqliteIntegrationTester {
    test_data_dir: PathBuf,
}

impl CqliteIntegrationTester {
    pub fn new(test_data_dir: &str) -> Self {
        Self {
            test_data_dir: PathBuf::from(test_data_dir),
        }
    }

    pub async fn run_integration_test(&mut self) -> Result<IntegrationTestResult, Box<dyn std::error::Error>> {
        println!("🧪 Starting cqlite Integration Test");
        println!("📂 Scanning directory: {}", self.test_data_dir.display());
        println!();

        let start_time = Instant::now();
        
        // Discover all SSTable-related files
        let file_info = self.analyze_sstable_structure().await?;
        
        // Analyze file structure
        let result = self.analyze_files(&file_info).await?;
        
        let total_time = start_time.elapsed();
        
        println!("⏱️  Analysis completed in {:.2}s", total_time.as_secs_f64());
        self.print_detailed_analysis(&result);
        
        Ok(result)
    }

    async fn analyze_sstable_structure(&self) -> Result<Vec<SSTableFileInfo>, Box<dyn std::error::Error>> {
        let mut file_infos = Vec::new();
        let mut stack = vec![self.test_data_dir.clone()];
        
        while let Some(current_dir) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&current_dir).await?;
            
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                
                if path.is_dir() {
                    stack.push(path);
                } else if self.is_data_file(&path) {
                    let metadata = tokio::fs::metadata(&path).await?;
                    let parent_dir = path.parent().unwrap();
                    
                    // Get all files in the same directory
                    let mut dir_files = Vec::new();
                    let mut dir_entries = tokio::fs::read_dir(parent_dir).await?;
                    while let Some(dir_entry) = dir_entries.next_entry().await? {
                        if let Some(filename) = dir_entry.file_name().to_str() {
                            dir_files.push(filename.to_string());
                        }
                    }
                    
                    let table_name = self.extract_table_name(&path);
                    
                    file_infos.push(SSTableFileInfo {
                        filename: path.file_name().unwrap().to_string_lossy().to_string(),
                        size_bytes: metadata.len(),
                        table_name,
                        files_in_directory: dir_files,
                    });
                }
            }
        }
        
        Ok(file_infos)
    }

    fn is_data_file(&self, path: &Path) -> bool {
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            filename.ends_with("-Data.db")
        } else {
            false
        }
    }

    fn extract_table_name(&self, path: &Path) -> String {
        if let Some(parent) = path.parent() {
            if let Some(dir_name) = parent.file_name().and_then(|n| n.to_str()) {
                if let Some(dash_pos) = dir_name.find('-') {
                    return dir_name[..dash_pos].to_string();
                }
            }
        }
        "unknown".to_string()
    }

    async fn analyze_files(&self, file_infos: &[SSTableFileInfo]) -> Result<IntegrationTestResult, Box<dyn std::error::Error>> {
        let mut data_files = Vec::new();
        let mut index_files = 0;
        let mut statistics_files = 0;
        let mut other_files = 0;
        let mut total_size = 0u64;
        
        for file_info in file_infos {
            total_size += file_info.size_bytes;
            data_files.push(file_info.clone());
            
            // Count related files in the same directory
            for related_file in &file_info.files_in_directory {
                if related_file.contains("Index") {
                    index_files += 1;
                } else if related_file.contains("Statistics") {
                    statistics_files += 1;
                } else if !related_file.ends_with("-Data.db") {
                    other_files += 1;
                }
            }
        }
        
        Ok(IntegrationTestResult {
            total_files_found: file_infos.len(),
            data_files,
            index_files,
            statistics_files,
            other_files,
            total_size_mb: total_size as f64 / 1_000_000.0,
        })
    }

    fn print_detailed_analysis(&self, result: &IntegrationTestResult) {
        println!();
        println!("📊 INTEGRATION TEST ANALYSIS");
        println!("═══════════════════════════════");
        println!("📁 Total SSTable Data Files: {}", result.total_files_found);
        println!("📄 Index Files: {}", result.index_files);
        println!("📈 Statistics Files: {}", result.statistics_files);
        println!("📝 Other Files: {}", result.other_files);
        println!("💾 Total Size: {:.2} MB", result.total_size_mb);
        println!();

        // Group by table
        let mut table_groups: HashMap<String, Vec<&SSTableFileInfo>> = HashMap::new();
        for file_info in &result.data_files {
            table_groups.entry(file_info.table_name.clone()).or_default().push(file_info);
        }

        println!("📋 TABLES DISCOVERED");
        println!("───────────────────");
        for (table_name, files) in &table_groups {
            let total_size: u64 = files.iter().map(|f| f.size_bytes).sum();
            println!("{}: {} files, {:.2} MB", table_name, files.len(), total_size as f64 / 1_000_000.0);
            
            for file in files {
                println!("  • {} ({:.2} MB)", file.filename, file.size_bytes as f64 / 1_000_000.0);
            }
        }
        println!();

        // File structure analysis
        println!("🔍 FILE STRUCTURE ANALYSIS");
        println!("─────────────────────────");
        if let Some(sample_file) = result.data_files.first() {
            println!("Sample directory structure ({}/):", sample_file.table_name);
            for file in &sample_file.files_in_directory {
                println!("  • {}", file);
            }
        }
        println!();

        // Readiness assessment
        println!("🎯 CQLITE READINESS ASSESSMENT");
        println!("──────────────────────────────");
        
        if result.total_files_found > 0 {
            println!("✅ Test data available ({} tables)", table_groups.len());
        } else {
            println!("❌ No test data found");
        }
        
        if result.total_size_mb > 10.0 {
            println!("✅ Sufficient test data size ({:.1} MB)", result.total_size_mb);
        } else {
            println!("⚠️  Limited test data size ({:.1} MB)", result.total_size_mb);
        }
        
        if table_groups.contains_key("collections_table") {
            println!("✅ Complex types test data available");
        } else {
            println!("⚠️  No complex types test data found");
        }
        
        if table_groups.contains_key("all_types") {
            println!("✅ Comprehensive data types test available");
        } else {
            println!("⚠️  Limited data type coverage");
        }
        
        println!();
        
        // Next steps
        println!("📝 NEXT STEPS FOR CQLITE INTEGRATION");
        println!("───────────────────────────────────");
        println!("1. Add cqlite-core dependency to validation tests");
        println!("2. Implement real SSTable parsing using cqlite-core::parser");
        println!("3. Compare parsed results with expected Cassandra data");
        println!("4. Test complex data types (collections, UDTs, tuples)");
        println!("5. Validate data integrity and format compatibility");
        println!("6. Performance benchmark against native Cassandra tools");
        println!();
    }

    pub async fn test_single_file(&self, file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔍 Testing single file: {}", file_path.display());
        
        let metadata = tokio::fs::metadata(file_path).await?;
        println!("📏 File size: {} bytes", metadata.len());
        
        // Read first few bytes to examine header
        let file_data = tokio::fs::read(file_path).await?;
        if file_data.len() >= 16 {
            println!("🔍 First 16 bytes (hex): {:02x?}", &file_data[..16]);
        }
        
        // This is where we would use cqlite-core to parse the file
        println!("📊 Would parse using cqlite-core::parser here");
        
        // For now, just examine file structure
        self.examine_file_structure(&file_data);
        
        Ok(())
    }

    fn examine_file_structure(&self, data: &[u8]) {
        println!("🔍 File Structure Analysis:");
        println!("  • Total size: {} bytes", data.len());
        
        if data.len() >= 4 {
            let magic_number = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            println!("  • Possible magic number: 0x{:08x}", magic_number);
        }
        
        // Look for patterns that might indicate SSTable structure
        let null_count = data.iter().filter(|&&b| b == 0).count();
        let printable_count = data.iter().filter(|&&b| b >= 32 && b <= 126).count();
        
        println!("  • Null bytes: {} ({:.1}%)", null_count, (null_count as f64 / data.len() as f64) * 100.0);
        println!("  • Printable chars: {} ({:.1}%)", printable_count, (printable_count as f64 / data.len() as f64) * 100.0);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_data_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/Users/patrick/local_projects/cqlite/test-env/cassandra5/sstables".to_string());
    
    let mut tester = CqliteIntegrationTester::new(&test_data_dir);
    
    // Run comprehensive analysis
    let _result = tester.run_integration_test().await?;
    
    // Test a single file if available
    let sstable_path = std::path::Path::new(&test_data_dir)
        .join("all_types-86a52c10669411f0acab47cdf782cef5")
        .join("nb-1-big-Data.db");
    
    if sstable_path.exists() {
        println!();
        println!("═══════════════════════════════");
        tester.test_single_file(&sstable_path).await?;
    }
    
    Ok(())
}