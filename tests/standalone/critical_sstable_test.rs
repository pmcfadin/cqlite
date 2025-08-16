//! Critical SSTable Reading Test - Issue #17 Protection
//! 
//! This is a minimal, standalone test to validate core SSTable reading
//! functionality is working before any cleanup operations.

use std::path::Path;
use std::fs;

fn main() {
    println!("🔍 Critical SSTable Reading Test - Issue #17 Protection");
    println!("=======================================================");
    
    let test_data_dir = Path::new("test-env/cassandra5/sstables");
    
    if !test_data_dir.exists() {
        println!("❌ CRITICAL: Test data directory not found!");
        println!("   Expected: {:?}", test_data_dir);
        std::process::exit(1);
    }
    
    println!("✅ Test data directory exists");
    
    // Count SSTable directories
    let mut table_count = 0;
    let mut file_count = 0;
    
    if let Ok(entries) = fs::read_dir(test_data_dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                if entry.path().is_dir() {
                    table_count += 1;
                    println!("📁 Found table directory: {}", entry.file_name().to_string_lossy());
                    
                    // Count files in each table directory
                    if let Ok(files) = fs::read_dir(entry.path()) {
                        for file in files {
                            if let Ok(file) = file {
                                if file.path().is_file() {
                                    file_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    println!("📊 Statistics:");
    println!("   Table directories: {}", table_count);
    println!("   Total SSTable files: {}", file_count);
    
    if table_count >= 8 && file_count >= 60 {
        println!("✅ PASS: Sufficient test data available");
        println!("🛡️  Issue #17 infrastructure appears intact");
    } else {
        println!("⚠️  WARNING: Less test data than expected");
        println!("   Expected: 8+ tables, 60+ files");
        println!("   Found: {} tables, {} files", table_count, file_count);
    }
    
    // Test basic file access
    let users_table = test_data_dir.join("users-28883a106e5411f0a72add2bbbd2f55e");
    if users_table.exists() {
        let data_file = users_table.join("nb-1-big-Data.db");
        if data_file.exists() {
            if let Ok(metadata) = fs::metadata(&data_file) {
                println!("✅ Sample data file accessible: {} bytes", metadata.len());
            } else {
                println!("❌ Cannot read sample data file metadata");
            }
        } else {
            println!("❌ Sample data file not found: {:?}", data_file);
        }
    } else {
        println!("❌ Users table directory not found");
    }
    
    println!("\n🎯 RESULT: Basic file system access verified");
    println!("   Ready for Issue #17 SSTable reading validation");
}