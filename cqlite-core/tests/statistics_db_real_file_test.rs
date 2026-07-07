//! Integration test for real Statistics.db file parsing (Issue #163)

use cqlite_core::parser::enhanced_statistics_parser::parse_enhanced_statistics_file;
use std::fs;
use std::path::Path;

#[test]
fn test_real_statistics_db_parsing() {
    // Initialize env_logger to see debug output
    let _ = env_logger::builder()
        .is_test(true)
        .parse_filters("debug")
        .try_init();

    let datasets_root = std::env::var("CQLITE_DATASETS_ROOT")
        .expect("CQLITE_DATASETS_ROOT environment variable must be set");

    let stats_path = Path::new(&datasets_root).join(
        "sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db",
    );

    if !stats_path.exists() {
        println!(
            "⚠️  Statistics.db test file not found at {:?}, skipping test",
            stats_path
        );
        return;
    }

    println!("Reading Statistics.db from: {:?}", stats_path);
    let file_data = fs::read(&stats_path).expect("Failed to read Statistics.db file");
    println!("File size: {} bytes", file_data.len());

    // Parse the Statistics.db file
    let result = parse_enhanced_statistics_file(&file_data, None);

    match result {
        Ok((remaining, statistics)) => {
            println!("✓ Successfully parsed Statistics.db");
            println!(
                "  Remaining bytes: {} (of {} total)",
                remaining.len(),
                file_data.len()
            );
            println!(
                "  Columns found: {}",
                statistics.serialization_header_columns.len()
            );

            for (idx, col) in statistics.serialization_header_columns.iter().enumerate() {
                println!(
                    "    Column {}: name='{}', type='{}'",
                    idx, col.name, col.column_type
                );
            }

            // Verify we found the expected columns
            assert!(
                !statistics.serialization_header_columns.is_empty(),
                "Should have found columns in SerializationHeader"
            );

            // Check for expected column names from hex analysis
            let column_names: Vec<&str> = statistics
                .serialization_header_columns
                .iter()
                .map(|c| c.name.as_str())
                .collect();

            println!("  Column names: {:?}", column_names);

            // ttl_test_table should have columns: id (UUID), expiring_value, session_info, temporary_data
            // But we don't parse partition keys in this phase, so we should see the regular columns
            assert!(
                column_names.contains(&"expiring_value"),
                "Should find 'expiring_value' column"
            );
        }
        Err(e) => {
            println!("✗ Failed to parse Statistics.db: {:?}", e);
            panic!("Statistics.db parsing failed: {:?}", e);
        }
    }
}
