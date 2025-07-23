use std::path::Path;
use cqlite_core::storage::sstable::directory::{test_all_directories, test_directory_validation};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 SSTable Directory Validation Test");
    println!("====================================");
    
    // Test path for Cassandra 5.0 SSTable directories
    let test_path = "test-env/cassandra5/sstables";
    
    if !Path::new(test_path).exists() {
        eprintln!("❌ Test directory not found: {}", test_path);
        eprintln!("Please run from the project root directory.");
        std::process::exit(1);
    }
    
    println!("📂 Testing SSTable directories in: {}", test_path);
    println!();
    
    match test_all_directories(test_path) {
        Ok(results) => {
            println!("📊 Validation Results Summary");
            println!("----------------------------");
            println!("Total directories tested: {}", results.len());
            
            let mut valid_count = 0;
            let mut total_errors = 0;
            let mut total_toc_issues = 0;
            let mut total_header_issues = 0;
            let mut total_corrupted = 0;
            
            for (dir_name, report) in &results {
                println!("\n🗂️  Directory: {}", dir_name);
                println!("   {}", report.summary());
                
                if report.is_valid() {
                    valid_count += 1;
                    println!("   ✅ Status: VALID");
                } else {
                    println!("   ❌ Status: ISSUES FOUND");
                    if !report.validation_errors.is_empty() {
                        println!("      🔴 Validation Errors: {}", report.validation_errors.len());
                        for error in &report.validation_errors {
                            println!("         • {}", error);
                        }
                    }
                    if !report.toc_inconsistencies.is_empty() {
                        println!("      📋 TOC Inconsistencies: {}", report.toc_inconsistencies.len());
                        for inconsistency in &report.toc_inconsistencies {
                            println!("         • {}", inconsistency);
                        }
                    }
                    if !report.header_inconsistencies.is_empty() {
                        println!("      🏷️  Header Issues: {}", report.header_inconsistencies.len());
                        for issue in &report.header_inconsistencies {
                            println!("         • {}", issue);
                        }
                    }
                    if !report.corrupted_files.is_empty() {
                        println!("      💥 Corrupted Files: {}", report.corrupted_files.len());
                        for file in &report.corrupted_files {
                            println!("         • {}", file);
                        }
                    }
                }
                
                total_errors += report.validation_errors.len();
                total_toc_issues += report.toc_inconsistencies.len();
                total_header_issues += report.header_inconsistencies.len();
                total_corrupted += report.corrupted_files.len();
            }
            
            println!("\n📈 Overall Statistics");
            println!("====================");
            println!("Valid directories: {}/{} ({:.1}%)", 
                     valid_count, results.len(),
                     if results.len() > 0 { (valid_count as f64 / results.len() as f64) * 100.0 } else { 0.0 });
            println!("Total validation errors: {}", total_errors);
            println!("Total TOC inconsistencies: {}", total_toc_issues);
            println!("Total header issues: {}", total_header_issues);
            println!("Total corrupted files: {}", total_corrupted);
            
            // Print detailed reports for invalid directories
            println!("\n📋 Detailed Reports for Invalid Directories");
            println!("==========================================");
            for (dir_name, report) in &results {
                if !report.is_valid() {
                    println!("\n{}", report.detailed_report());
                }
            }
            
            // Component analysis summary
            println!("\n🔍 Component Analysis Summary");
            println!("============================");
            for (dir_name, report) in &results {
                for analysis in &report.component_analysis {
                    println!("\n📁 {}/Generation {} ({} format):", 
                             dir_name, analysis.generation, analysis.format);
                    println!("   Required present: {} components", analysis.required_components_present.len());
                    println!("   Required missing: {} components", analysis.required_components_missing.len());
                    println!("   Optional present: {} components", analysis.optional_components_present.len());
                    println!("   Total file size: {} bytes", analysis.file_sizes.values().sum::<u64>());
                    
                    let accessible_count = analysis.accessibility_status.values().filter(|&&v| v).count();
                    let total_files = analysis.accessibility_status.len();
                    println!("   Accessible files: {}/{}", accessible_count, total_files);
                }
            }
            
            if valid_count == results.len() {
                println!("\n🎉 All SSTable directories passed validation!");
                std::process::exit(0);
            } else {
                println!("\n⚠️  Some SSTable directories have validation issues.");
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("❌ Failed to validate directories: {}", e);
            std::process::exit(1);
        }
    }
}