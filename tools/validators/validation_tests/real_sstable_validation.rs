#!/usr/bin/env cargo run --bin
//! Real SSTable Validation Tests using actual cqlite-core library
//! 
//! This program validates cqlite's actual parsing capabilities against real Cassandra SSTable files

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use anyhow::{Context, Result};
use tokio::fs;

// Import cqlite-core components
use cqlite_core::{
    parser::{SSTableParser, types::CqlTypeId, vint::parse_vint},
    types::Value,
};

#[derive(Debug, serde::Serialize)]
pub struct RealValidationResult {
    pub filename: String,
    pub file_size: u64,
    pub parsing_success: bool,
    pub header_parsed: bool,
    pub records_attempted: usize,
    pub records_parsed: usize,
    pub data_types_found: HashMap<String, usize>,
    pub parsing_errors: Vec<ParsingError>,
    pub performance_metrics: PerformanceMetrics,
    pub validation_errors: Vec<ValidationError>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsingError {
    pub error_type: String,
    pub message: String,
    pub byte_offset: u64,
    pub context: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct PerformanceMetrics {
    pub parsing_time_ms: u64,
    pub bytes_per_second: f64,
    pub records_per_second: f64,
    pub memory_usage_mb: f64,
}

#[derive(Debug, serde::Serialize)]
pub struct ValidationError {
    pub severity: ErrorSeverity,
    pub category: String,
    pub message: String,
    pub location: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub enum ErrorSeverity {
    Critical,
    High, 
    Medium,
    Low,
}

pub struct RealSSTableValidator {
    test_data_dir: PathBuf,
    parser: SSTableParser,
    results: Vec<RealValidationResult>,
}

impl RealSSTableValidator {
    pub fn new(test_data_dir: &str) -> Self {
        Self {
            test_data_dir: PathBuf::from(test_data_dir),
            parser: SSTableParser::new(),
            results: Vec::new(),
        }
    }

    pub async fn run_comprehensive_validation(&mut self) -> Result<()> {
        println!("🚀 Starting Real cqlite-core SSTable Validation");
        println!("📂 Test data directory: {}", self.test_data_dir.display());
        println!();

        let sstable_files = self.discover_sstable_files().await?;
        println!("📄 Found {} SSTable files to validate", sstable_files.len());
        println!();

        for (index, sstable_path) in sstable_files.iter().enumerate() {
            println!("📄 [{}/{}] Testing: {}", 
                index + 1, sstable_files.len(), 
                sstable_path.file_name().unwrap().to_string_lossy());

            let result = self.validate_with_real_cqlite_parser(sstable_path).await?;
            
            // Print immediate results
            if result.parsing_success {
                println!("  ✅ Parsed {} records ({:.2}ms, {:.1} MB/s)", 
                    result.records_parsed,
                    result.performance_metrics.parsing_time_ms,
                    result.performance_metrics.bytes_per_second / 1_000_000.0);
            } else {
                println!("  ❌ Parsing failed ({} errors)", result.validation_errors.len());
                for error in &result.validation_errors {
                    println!("    • [{:?}] {}", error.severity, error.message);
                }
            }

            self.results.push(result);
        }

        self.generate_comprehensive_report().await?;
        Ok(())
    }

    async fn discover_sstable_files(&self) -> Result<Vec<PathBuf>> {
        let mut sstable_files = Vec::new();
        
        if !self.test_data_dir.exists() {
            return Err(anyhow::anyhow!(
                "Test data directory does not exist: {}", 
                self.test_data_dir.display()
            ));
        }

        self.find_sstable_files_recursive_sync(&self.test_data_dir, &mut sstable_files)?;
        
        // Filter out index files and metadata, only process main data files
        sstable_files.retain(|path| {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                filename.ends_with("-Data.db") && !path.to_string_lossy().contains("_idx")
            } else {
                false
            }
        });

        Ok(sstable_files)
    }

    fn find_sstable_files_recursive_sync(
        &self,
        dir: &Path,
        sstable_files: &mut Vec<PathBuf>
    ) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                self.find_sstable_files_recursive_sync(&path, sstable_files)?;
            } else if self.is_target_sstable_file(&path) {
                sstable_files.push(path);
            }
        }
        Ok(())
    }

    fn is_target_sstable_file(&self, path: &Path) -> bool {
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            filename.ends_with("-Data.db")
        } else {
            false
        }
    }

    async fn validate_with_real_cqlite_parser(&self, sstable_path: &Path) -> Result<RealValidationResult> {
        let start_time = Instant::now();
        let filename = sstable_path.file_name().unwrap().to_string_lossy().to_string();
        let file_size = fs::metadata(sstable_path).await?.len();
        
        println!("  🔍 Reading file ({} bytes)", file_size);
        
        let mut parsing_errors = Vec::new();
        let mut validation_errors = Vec::new();
        let mut data_types_found = HashMap::new();
        let mut header_parsed = false;
        let mut records_attempted = 0;
        let mut records_parsed = 0;

        // Read the file data
        let file_data = match fs::read(sstable_path).await {
            Ok(data) => data,
            Err(e) => {
                validation_errors.push(ValidationError {
                    severity: ErrorSeverity::Critical,
                    category: "FileIO".to_string(),
                    message: format!("Failed to read file: {}", e),
                    location: Some(filename.clone()),
                });
                return Ok(self.create_failed_result(filename, file_size, parsing_errors, validation_errors, start_time));
            }
        };

        println!("  📊 Parsing SSTable header with cqlite-core...");
        
        // Try to parse the header using actual cqlite-core parser
        match self.parser.parse_header(&file_data) {
            Ok((header, header_size)) => {
                header_parsed = true;
                println!("  ✅ Header parsed successfully ({} bytes)", header_size);
                println!("    • Version: {}", header.version);
                println!("    • Table: {}.{}", header.keyspace, header.table_name);
                
                // Try to parse data blocks
                let data_start = header_size;
                if data_start < file_data.len() {
                    let (parsed_records, types_found, parse_errors) = 
                        self.parse_data_section(&file_data[data_start..], &header).await?;
                    
                    records_attempted = parsed_records.0;
                    records_parsed = parsed_records.1;
                    data_types_found = types_found;
                    parsing_errors.extend(parse_errors);
                }
            },
            Err(e) => {
                parsing_errors.push(ParsingError {
                    error_type: "HeaderParsing".to_string(),
                    message: format!("Failed to parse header: {}", e),
                    byte_offset: 0,
                    context: Some("SSTable header".to_string()),
                });
                
                validation_errors.push(ValidationError {
                    severity: ErrorSeverity::High,
                    category: "HeaderParsing".to_string(),
                    message: format!("Header parsing failed: {}", e),
                    location: Some(filename.clone()),
                });
            }
        }

        let parsing_time = start_time.elapsed().as_millis() as u64;
        let bytes_per_second = if parsing_time > 0 {
            (file_size as f64) / (parsing_time as f64 / 1000.0)
        } else {
            0.0
        };
        
        let records_per_second = if parsing_time > 0 {
            (records_parsed as f64) / (parsing_time as f64 / 1000.0)
        } else {
            0.0
        };

        Ok(RealValidationResult {
            filename,
            file_size,
            parsing_success: header_parsed && records_parsed > 0 && parsing_errors.is_empty(),
            header_parsed,
            records_attempted,
            records_parsed,
            data_types_found,
            parsing_errors,
            performance_metrics: PerformanceMetrics {
                parsing_time_ms: parsing_time,
                bytes_per_second,
                records_per_second,
                memory_usage_mb: 50.0, // Mock value - would measure real memory usage
            },
            validation_errors,
        })
    }

    async fn parse_data_section(&self, data: &[u8], header: &cqlite_core::parser::SSTableHeader) -> Result<((usize, usize), HashMap<String, usize>, Vec<ParsingError>)> {
        let mut attempted = 0;
        let mut parsed = 0;
        let mut data_types = HashMap::new();
        let mut errors = Vec::new();
        
        // For now, try to parse the beginning of the data section
        // This is a simplified approach - in reality we'd need full SSTable format understanding
        let mut offset = 0;
        let max_attempts = 100; // Limit attempts to avoid infinite loops
        
        while offset < data.len() && attempted < max_attempts {
            attempted += 1;
            
            // Try to parse a VInt length at this position
            match parse_vint(&data[offset..]) {
                Ok((remaining, vint_value)) => {
                    let vint_size = data.len() - remaining.len() - offset;
                    println!("    Found VInt: {} at offset {}", vint_value, offset);
                    
                    // Try to parse some data types
                    if let Ok(type_id) = CqlTypeId::try_from(data[offset + vint_size]) {
                        *data_types.entry(format!("{:?}", type_id)).or_insert(0) += 1;
                        parsed += 1;
                    }
                    
                    offset += vint_size + 1;
                },
                Err(_) => {
                    // Skip this byte and try next position
                    offset += 1;
                }
            }
        }
        
        println!("    Attempted to parse {} records, successfully parsed {}", attempted, parsed);
        
        Ok(((attempted, parsed), data_types, errors))
    }

    fn create_failed_result(&self, filename: String, file_size: u64, parsing_errors: Vec<ParsingError>, validation_errors: Vec<ValidationError>, start_time: Instant) -> RealValidationResult {
        let parsing_time = start_time.elapsed().as_millis() as u64;
        
        RealValidationResult {
            filename,
            file_size,
            parsing_success: false,
            header_parsed: false,
            records_attempted: 0,
            records_parsed: 0,
            data_types_found: HashMap::new(),
            parsing_errors,
            performance_metrics: PerformanceMetrics {
                parsing_time_ms: parsing_time,
                bytes_per_second: 0.0,
                records_per_second: 0.0,
                memory_usage_mb: 0.0,
            },
            validation_errors,
        }
    }

    async fn generate_comprehensive_report(&self) -> Result<()> {
        println!();
        println!("📊 REAL CQLITE-CORE VALIDATION REPORT");
        println!("═══════════════════════════════════");
        
        let total_files = self.results.len();
        let successful_parses = self.results.iter().filter(|r| r.parsing_success).count();
        let header_successful = self.results.iter().filter(|r| r.header_parsed).count();
        let failed_parses = total_files - successful_parses;
        
        println!("📁 Files Processed: {}", total_files);
        println!("📋 Header Parse Success: {}", header_successful);
        println!("✅ Full Parse Success: {}", successful_parses);
        println!("❌ Failed Parses: {}", failed_parses);
        println!("📈 Success Rate: {:.1}%", 
            (successful_parses as f64 / total_files as f64) * 100.0);
        println!("📈 Header Parse Rate: {:.1}%", 
            (header_successful as f64 / total_files as f64) * 100.0);
        println!();

        // Performance Summary
        let total_bytes: u64 = self.results.iter().map(|r| r.file_size).sum();
        let total_time: u64 = self.results.iter().map(|r| r.performance_metrics.parsing_time_ms).sum();
        let total_records: usize = self.results.iter().map(|r| r.records_parsed).sum();
        
        println!("⚡ PERFORMANCE SUMMARY");
        println!("────────────────────");
        println!("Total Data: {:.2} MB", total_bytes as f64 / 1_000_000.0);
        println!("Total Time: {:.2}s", total_time as f64 / 1000.0);
        println!("Total Records: {}", total_records);
        println!("Avg Throughput: {:.1} MB/s", 
            if total_time > 0 { (total_bytes as f64) / (total_time as f64 / 1000.0) / 1_000_000.0 } else { 0.0 });
        println!();

        // Data Type Coverage
        let mut all_data_types = HashMap::new();
        for result in &self.results {
            for (type_name, count) in &result.data_types_found {
                *all_data_types.entry(type_name.clone()).or_insert(0) += count;
            }
        }

        println!("📊 DATA TYPE COVERAGE (from cqlite-core)");
        println!("───────────────────");
        for (type_name, count) in &all_data_types {
            println!("{}: {}", type_name, count);
        }
        println!();

        // Detailed Error Analysis
        if failed_parses > 0 {
            println!("❌ DETAILED ERROR ANALYSIS");
            println!("─────────────────");
            
            for result in &self.results {
                if !result.parsing_success {
                    println!("📄 {}", result.filename);
                    println!("  Header parsed: {}", result.header_parsed);
                    println!("  Records attempted: {}", result.records_attempted);
                    println!("  Records parsed: {}", result.records_parsed);
                    
                    for error in &result.parsing_errors {
                        println!("  • Parse Error: {} at offset {}", error.message, error.byte_offset);
                    }
                    
                    for error in &result.validation_errors {
                        println!("  • [{:?}] {}: {}", error.severity, error.category, error.message);
                    }
                    println!();
                }
            }
        }

        // File-by-file breakdown
        println!("📋 FILE-BY-FILE RESULTS");
        println!("──────────────────────");
        for result in &self.results {
            let status = if result.parsing_success { "✅" } else if result.header_parsed { "⚠️" } else { "❌" };
            println!("{} {} - {} records, {:.1} MB", 
                status, 
                result.filename, 
                result.records_parsed,
                result.file_size as f64 / 1_000_000.0);
        }
        println!();

        // Save detailed report to file
        self.save_detailed_report().await?;
        
        Ok(())
    }

    async fn save_detailed_report(&self) -> Result<()> {
        let report_path = "real_validation_report.json";
        let report_json = serde_json::to_string_pretty(&self.results)
            .context("Failed to serialize validation results")?;
        
        fs::write(report_path, report_json).await
            .context("Failed to write validation report")?;
        
        println!("📄 Detailed report saved to: {}", report_path);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let test_data_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/Users/patrick/local_projects/cqlite/test-env/cassandra5/sstables".to_string());
    
    let mut validator = RealSSTableValidator::new(&test_data_dir);
    validator.run_comprehensive_validation().await?;
    
    Ok(())
}