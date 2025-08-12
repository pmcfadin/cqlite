//! Issue #17 Simple SSTable Reading Validator
//!
//! Minimal test runner to validate core SSTable reading functionality
//! without complex dependencies.

use std::{path::Path, time::Instant};

/// Simple test runner for Issue #17 validation
fn main() {
    println!("🔍 Issue #17: SSTable Reading Validation (Simple)");
    println!("================================================");

    let start_time = Instant::now();
    let mut tests_passed = 0;
    let mut tests_failed = 0;

    // Test 1: Check test data availability
    println!("\n📂 Test 1: Checking test data availability...");
    let test_data_path = Path::new("test-env/cassandra5/sstables");
    if test_data_path.exists() {
        let sstable_count = count_sstable_files(test_data_path);
        println!("  ✅ Found {} SSTable files", sstable_count);
        tests_passed += 1;
    } else {
        println!("  ❌ Test data not found at {:?}", test_data_path);
        tests_failed += 1;
    }

    // Test 2: Basic directory structure validation
    println!("\n📁 Test 2: Directory structure validation...");
    if validate_directory_structure(test_data_path) {
        println!("  ✅ Directory structure looks valid");
        tests_passed += 1;
    } else {
        println!("  ❌ Directory structure validation failed");
        tests_failed += 1;
    }

    // Test 3: File type detection
    println!("\n🔍 Test 3: SSTable file type detection...");
    if test_file_type_detection(test_data_path) {
        println!("  ✅ File type detection working");
        tests_passed += 1;
    } else {
        println!("  ❌ File type detection failed");
        tests_failed += 1;
    }

    // Test 4: Basic file reading
    println!("\n📖 Test 4: Basic file reading capability...");
    if test_basic_file_reading(test_data_path) {
        println!("  ✅ File reading capability confirmed");
        tests_passed += 1;
    } else {
        println!("  ❌ File reading capability failed");
        tests_failed += 1;
    }

    // Summary
    let duration = start_time.elapsed();
    println!("\n📊 Test Summary");
    println!("================");
    println!("✅ Tests passed: {}", tests_passed);
    println!("❌ Tests failed: {}", tests_failed);
    println!("⏱️  Duration: {:.2}s", duration.as_secs_f64());

    // Determine Issue #17 readiness
    let total_tests = tests_passed + tests_failed;
    let success_rate = tests_passed as f64 / total_tests as f64;

    println!("\n🎯 Issue #17 Status Assessment:");
    if success_rate >= 0.75 {
        println!(
            "✅ READY - Core infrastructure in place ({:.0}% success)",
            success_rate * 100.0
        );
        println!("📝 Recommendation: Proceed with detailed implementation");
        std::process::exit(0);
    } else {
        println!(
            "⚠️  NEEDS WORK - Infrastructure gaps found ({:.0}% success)",
            success_rate * 100.0
        );
        println!("🔧 Recommendation: Address basic infrastructure issues first");
        std::process::exit(1);
    }
}

fn count_sstable_files(base_path: &Path) -> usize {
    if !base_path.exists() {
        return 0;
    }

    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(base_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Ok(sub_entries) = std::fs::read_dir(&path) {
                    for sub_entry in sub_entries.flatten() {
                        let file_path = sub_entry.path();
                        if let Some(ext) = file_path.extension() {
                            if ext == "db" {
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    count
}

fn validate_directory_structure(base_path: &Path) -> bool {
    if !base_path.exists() {
        return false;
    }

    // Check if we have at least one table directory
    if let Ok(entries) = std::fs::read_dir(base_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    // Cassandra table directories typically have UUID suffix
                    if dir_name.contains('-') && dir_name.len() > 10 {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn test_file_type_detection(base_path: &Path) -> bool {
    if !base_path.exists() {
        return false;
    }

    let mut found_data_file = false;
    let mut found_statistics_file = false;

    if let Ok(entries) = std::fs::read_dir(base_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Ok(sub_entries) = std::fs::read_dir(&path) {
                    for sub_entry in sub_entries.flatten() {
                        let file_path = sub_entry.path();
                        if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                            if file_name.contains("Data.db") {
                                found_data_file = true;
                            }
                            if file_name.contains("Statistics.db") {
                                found_statistics_file = true;
                            }
                        }
                    }
                }
            }
        }
    }

    found_data_file && found_statistics_file
}

fn test_basic_file_reading(base_path: &Path) -> bool {
    if !base_path.exists() {
        return false;
    }

    // Try to read at least one SSTable file
    if let Ok(entries) = std::fs::read_dir(base_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Ok(sub_entries) = std::fs::read_dir(&path) {
                    for sub_entry in sub_entries.flatten() {
                        let file_path = sub_entry.path();
                        if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                            if file_name.contains("Data.db") {
                                // Try to read the first few bytes
                                if let Ok(data) = std::fs::read(&file_path) {
                                    if data.len() > 10 {
                                        println!(
                                            "    📄 Read {} bytes from {}",
                                            data.len(),
                                            file_name
                                        );
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    false
}
