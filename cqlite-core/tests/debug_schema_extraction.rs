//! Debug test to identify why schema extraction isn't working
//! Requires state_machine feature for schema functionality

#![cfg(feature = "state_machine")]

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Platform};
use std::path::Path;
use std::sync::Arc;

#[tokio::test]
async fn debug_schema_extraction() {
    // Initialize logging
    let _ = env_logger::builder()
        .is_test(true)
        .parse_filters("debug")
        .try_init();

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let datasets_root =
        std::env::var("CQLITE_DATASETS_ROOT").expect("CQLITE_DATASETS_ROOT must be set");
    let test_path = Path::new(&datasets_root).join(
        "sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );

    println!("\n=== Opening SSTable ===");
    let reader = SSTableReader::open(&test_path, &config, platform)
        .await
        .expect("Failed to open");

    println!("\n=== Reader State ===");
    println!("Keyspace: {}", reader.header().keyspace);
    println!("Table: {}", reader.header().table_name);
    println!("Header columns count: {}", reader.header().columns.len());

    for (idx, col) in reader.header().columns.iter().enumerate() {
        println!(
            "  Header column {}: {} ({})",
            idx, col.name, col.column_type
        );
    }

    println!("\n=== Schema State ===");
    match reader.schema() {
        Some(schema) => {
            println!("✓ Schema extracted!");
            println!("  Keyspace: {}", schema.keyspace);
            println!("  Table: {}", schema.table);
            println!("  Columns: {}", schema.columns.len());
            println!("  Partition keys: {}", schema.partition_keys.len());
            println!("  Clustering keys: {}", schema.clustering_keys.len());
        }
        None => {
            println!("✗ Schema is None");
            println!("  This is the bug - columns exist in header but schema wasn't created");
        }
    }

    println!("\n=== Cassandra Version ===");
    println!("Version: {:?}", reader.header().cassandra_version);
}
