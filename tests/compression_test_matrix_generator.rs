//! Test matrix generator for compression algorithms and chunk sizes
//!
//! This module generates test SSTable datasets with different compression
//! algorithms and chunk sizes for comprehensive validation.

use std::fs;
use std::path::Path;

/// Generate test SSTable files with specific compression settings
pub fn generate_compression_test_matrix() -> Result<(), Box<dyn std::error::Error>> {
    let algorithms = vec!["LZ4", "Snappy", "Zstd", "Deflate"];
    let chunk_sizes = ["4KB", "16KB", "64KB"];
    let chunk_bytes = [4096, 16384, 65536];

    // Create test data directory
    let test_data_dir = Path::new("test-data");
    fs::create_dir_all(test_data_dir)?;

    println!("🔧 Generating compression test matrix...");

    for algorithm in &algorithms {
        for (i, chunk_size) in chunk_sizes.iter().enumerate() {
            let dir_name = format!("{}-{}", algorithm.to_lowercase(), chunk_size.to_lowercase());
            let dir_path = test_data_dir.join(&dir_name);
            fs::create_dir_all(&dir_path)?;

            println!("📦 Generating dataset: {algorithm} with {chunk_size} chunks");

            // Generate CQL commands to create test data
            let cql_script = format!(
                r#"
                CREATE KEYSPACE IF NOT EXISTS compression_test_{} 
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
                
                USE compression_test_{};
                
                CREATE TABLE IF NOT EXISTS test_table (
                    id int PRIMARY KEY,
                    data text,
                    value double,
                    timestamp timestamp
                ) WITH compression = {{
                    'class': 'org.apache.cassandra.io.compress.{}Compressor',
                    'chunk_length_in_kb': {}
                }};
                
                -- Insert test data
                INSERT INTO test_table (id, data, value, timestamp) 
                VALUES (1, 'Test data for compression validation', 123.456, toTimestamp(now()));
                
                INSERT INTO test_table (id, data, value, timestamp) 
                VALUES (2, 'Another test row with different data', 789.012, toTimestamp(now()));
                
                INSERT INTO test_table (id, data, value, timestamp) 
                VALUES (3, 'Third row for chunk boundary testing', 345.678, toTimestamp(now()));
                "#,
                algorithm.to_lowercase(),
                algorithm.to_lowercase(),
                algorithm,
                chunk_bytes[i] / 1024
            );

            // Save CQL script
            let script_path = dir_path.join("create_table.cql");
            fs::write(&script_path, cql_script)?;

            // Generate corruption test file
            generate_corruption_test(&dir_path, algorithm, chunk_bytes[i])?;
        }
    }

    println!("✅ Test matrix generation complete!");
    Ok(())
}

/// Generate a corrupted SSTable file for CRC validation testing
fn generate_corruption_test(
    dir_path: &Path,
    algorithm: &str,
    chunk_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create a mock CompressionInfo.db file with intentional CRC mismatch
    let mut compression_info_data = Vec::new();

    // Algorithm name
    let algorithm_name = format!("{algorithm}Compressor");
    compression_info_data.extend_from_slice(&(algorithm_name.len() as u16).to_be_bytes());
    compression_info_data.extend_from_slice(algorithm_name.as_bytes());

    // Padding to 4-byte boundary
    while compression_info_data.len() % 4 != 0 {
        compression_info_data.push(0);
    }

    // Chunk length
    compression_info_data.extend_from_slice(&(chunk_size as u32).to_be_bytes());

    // Data length (4 chunks worth)
    let data_length = (chunk_size * 4) as u64;
    compression_info_data.extend_from_slice(&data_length.to_be_bytes());

    // Number of chunks
    compression_info_data.extend_from_slice(&4u32.to_be_bytes());

    // Chunk offsets
    for i in 0..4 {
        let offset = (i * chunk_size) as u64;
        compression_info_data.extend_from_slice(&offset.to_be_bytes());
    }

    // Per-chunk CRCs (intentionally wrong for testing)
    for i in 0..4u32 {
        let crc: u32 = 0xDEADBEEF + i; // Obviously wrong CRC
        compression_info_data.extend_from_slice(&crc.to_be_bytes());
    }

    // Metadata CRC (also intentionally wrong)
    compression_info_data.extend_from_slice(&0xBADC0FFEu32.to_be_bytes());

    // Save corrupted CompressionInfo.db
    let corruption_path = dir_path.join("corrupted-CompressionInfo.db");
    fs::write(&corruption_path, compression_info_data)?;

    println!(
        "  📝 Created corruption test: {}",
        corruption_path.display()
    );

    Ok(())
}

/// Run validation tests on generated datasets
pub fn run_compression_validation_tests() -> Result<(), Box<dyn std::error::Error>> {
    let test_data_dir = Path::new("test-data");

    if !test_data_dir.exists() {
        eprintln!("❌ Test data directory not found. Run generate_compression_test_matrix first.");
        return Err("Test data not found".into());
    }

    let mut passed = 0;
    let mut failed = 0;

    println!("\n🔍 Running compression validation tests...\n");

    // Test each dataset
    for entry in fs::read_dir(test_data_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            let dir_name = path.file_name().unwrap().to_str().unwrap();
            print!("Testing {dir_name}: ");

            // Check for corrupted file
            let corruption_test = path.join("corrupted-CompressionInfo.db");
            if corruption_test.exists() {
                // This should fail with CRC mismatch
                match validate_corrupted_file(&corruption_test) {
                    Ok(_) => {
                        println!("❌ FAILED - Corruption not detected!");
                        failed += 1;
                    }
                    Err(e) if e.to_string().contains("CRC") => {
                        println!("✅ PASSED - CRC mismatch detected");
                        passed += 1;
                    }
                    Err(e) => {
                        println!("⚠️  FAILED - Wrong error: {e}");
                        failed += 1;
                    }
                }
            }
        }
    }

    println!("\n📊 Test Results:");
    println!("  ✅ Passed: {passed}");
    println!("  ❌ Failed: {failed}");
    println!("  📈 Total:  {}", passed + failed);

    if failed > 0 {
        Err(format!("{failed} tests failed").into())
    } else {
        Ok(())
    }
}

/// Validate a corrupted CompressionInfo file
fn validate_corrupted_file(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use cqlite_core::storage::sstable::compression_info::CompressionInfo;

    let data = fs::read(path)?;
    let _info = CompressionInfo::parse(&data)?;

    // If we got here, validation passed (it shouldn't for corrupted files)
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_matrix() {
        // Generate the test matrix
        generate_compression_test_matrix().expect("Failed to generate test matrix");

        // Verify directories were created
        let test_data_dir = Path::new("test-data");
        assert!(test_data_dir.exists());

        // Check for expected directories
        let expected_dirs = vec![
            "lz4-4kb",
            "lz4-16kb",
            "lz4-64kb",
            "snappy-4kb",
            "snappy-16kb",
            "snappy-64kb",
            "zstd-4kb",
            "zstd-16kb",
            "zstd-64kb",
            "deflate-4kb",
            "deflate-16kb",
            "deflate-64kb",
        ];

        for dir_name in expected_dirs {
            let dir_path = test_data_dir.join(dir_name);
            assert!(dir_path.exists(), "Missing directory: {dir_name}");

            // Check for corruption test file
            let corruption_file = dir_path.join("corrupted-CompressionInfo.db");
            assert!(
                corruption_file.exists(),
                "Missing corruption test in {dir_name}"
            );
        }
    }

    #[test]
    fn test_validation() {
        // Ensure test data exists
        if !Path::new("test-data").exists() {
            generate_compression_test_matrix().expect("Failed to generate test matrix");
        }

        // Run validation tests
        run_compression_validation_tests().expect("Validation tests should pass");
    }
}
