//! Test for Issue #154 - collections_with_udts header parsing failure
//!
//! This test reproduces the "Verify error" when opening the collections_with_udts table.

use cqlite_core::{Config, Platform};
use std::path::PathBuf;
use std::sync::Arc;

fn get_test_data_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("test-data/datasets"))
}

#[tokio::test]
async fn test_collections_with_udts_can_open() {
    // Initialize logging for debugging
    let _ = env_logger::builder().is_test(true).try_init();

    let test_root = get_test_data_root();
    let data_file_path = test_root.join(
        "sstables/test_collections/collections_with_udts-6bc2bae0a25111f0a3fef1a551383fb9/nb-1-big-Data.db"
    );

    // Skip test if file doesn't exist
    if !data_file_path.exists() {
        println!(
            "⚠️ Skipping: collections_with_udts test file not found at {:?}",
            data_file_path
        );
        return;
    }

    println!("📂 Testing file: {:?}", data_file_path);
    println!(
        "📏 File size: {} bytes",
        std::fs::metadata(&data_file_path).unwrap().len()
    );

    // Initialize platform
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    // Try to open the SSTable - this should NOT fail with "Verify error"
    let result = cqlite_core::storage::sstable::reader::SSTableReader::open(
        &data_file_path,
        &config,
        platform,
    )
    .await;

    match &result {
        Ok(reader) => {
            println!("✅ Successfully opened SSTable");
            println!("   Header: {:?}", reader.header().cassandra_version);
        }
        Err(e) => {
            eprintln!("❌ Failed to open SSTable: {:?}", e);
            eprintln!("   Error details: {}", e);
        }
    }

    // Assert success
    result.expect("collections_with_udts table should open successfully (Issue #154)");
}
