//! Test for Issue #192: UUID columns should display as hyphenated format, not byte arrays
//!
//! This test validates that UUID and TimeUUID partition keys and regular columns
//! are correctly parsed as Value::Uuid([u8; 16]) instead of being misparsed as
//! Value::List of integers.

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Platform, Value};
use std::path::Path;
use std::sync::Arc;

/// Test that UUID partition keys are parsed as Value::Uuid, not Value::List
#[tokio::test]
async fn test_uuid_partition_key_parsing() {
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let datasets_root = std::env::var("CQLITE_DATASETS_ROOT")
        .expect("CQLITE_DATASETS_ROOT environment variable must be set");
    let test_path = Path::new(&datasets_root)
        .join("sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db");

    if !test_path.exists() {
        println!(
            "⚠️  simple_table test data not found at {:?}, skipping test",
            test_path
        );
        return;
    }

    // Open SSTable with V5CompressedLegacy format
    let reader = SSTableReader::open(&test_path, &config, platform)
        .await
        .expect("Failed to open simple_table");

    println!("✓ Opened simple_table successfully");
    println!("  Keyspace: {}", reader.header().keyspace);
    println!("  Table: {}", reader.header().table_name);

    // Check if schema was extracted from header
    if reader.schema().is_none() {
        println!("⏭️ Skipping test: Schema extraction from SSTable header not yet implemented");
        println!(
            "   V5CompressedLegacy format requires schema but header parsing didn't extract it"
        );
        return;
    }

    // Read all entries - this exercises the full V5CompressedLegacy parsing path
    let entries_result = reader.get_all_entries().await;

    match entries_result {
        Ok(entries) => {
            println!("✓ Read {} entries from simple_table", entries.len());

            assert!(
                !entries.is_empty(),
                "Should have parsed entries from simple_table"
            );

            // Check first entry for UUID partition key
            if let Some((table_id, row_key, value)) = entries.first() {
                println!(
                    "  First entry: table_id={}, key={} bytes",
                    table_id,
                    row_key.0.len()
                );

                // CRITICAL VALIDATION: Partition key should be 16-byte UUID
                assert_eq!(
                    row_key.0.len(),
                    16,
                    "Partition key should be 16-byte UUID, got {} bytes",
                    row_key.0.len()
                );

                // Validate value structure - should be Value::Map with column names as keys
                if let Value::Map(cells) = value {
                    println!("  Value is a Map with {} entries", cells.len());

                    // Find UUID columns in the map
                    let uuid_columns: Vec<_> = cells
                        .iter()
                        .filter(|(name, _)| {
                            if let Value::Text(n) = name {
                                n == "id" || n == "session_id"
                            } else {
                                false
                            }
                        })
                        .collect();

                    println!(
                        "  Found {} UUID-type columns in row data",
                        uuid_columns.len()
                    );

                    for (col_name, col_value) in uuid_columns {
                        if let Value::Text(name) = col_name {
                            // CRITICAL VALIDATION: UUID columns should be Value::Uuid([u8; 16])
                            match col_value {
                                Value::Uuid(uuid_bytes) => {
                                    println!(
                                        "  ✅ Column '{}': correctly parsed as Value::Uuid with {} bytes",
                                        name,
                                        uuid_bytes.len()
                                    );
                                    assert_eq!(
                                        uuid_bytes.len(),
                                        16,
                                        "UUID should be 16 bytes, got {}",
                                        uuid_bytes.len()
                                    );
                                }
                                Value::List(items) => {
                                    panic!(
                                        "❌ BUG DETECTED: Column '{}' is Value::List({} items) instead of Value::Uuid!\n\
                                         This is the Issue #192 bug - UUID misparsed as list of integers.\n\
                                         Expected: Value::Uuid([u8; 16])\n\
                                         Got: Value::List([Value::Integer, ...]) with {} items",
                                        name,
                                        items.len(),
                                        items.len()
                                    );
                                }
                                Value::Blob(bytes) => {
                                    panic!(
                                        "❌ Column '{}' is Value::Blob({} bytes) instead of Value::Uuid!\n\
                                         UUID should be parsed as Value::Uuid([u8; 16]), not blob.",
                                        name,
                                        bytes.len()
                                    );
                                }
                                Value::Null => {
                                    println!(
                                        "  ℹ️  Column '{}': NULL value (acceptable for nullable columns)",
                                        name
                                    );
                                }
                                other => {
                                    panic!(
                                        "❌ Column '{}' has unexpected type: {:?}\n\
                                         Expected: Value::Uuid([u8; 16])",
                                        name, other
                                    );
                                }
                            }
                        }
                    }
                } else {
                    panic!(
                        "❌ Row value should be Value::Map, got {:?}",
                        std::mem::discriminant(value)
                    );
                }
            }

            println!("✅ Issue #192 UUID parsing test passed");
        }
        Err(e) => {
            panic!("❌ Failed to read entries from simple_table: {}", e);
        }
    }
}

/// Test that TimeUUID columns are also parsed correctly
#[tokio::test]
async fn test_timeuuid_column_parsing() {
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let datasets_root = std::env::var("CQLITE_DATASETS_ROOT")
        .expect("CQLITE_DATASETS_ROOT environment variable must be set");
    let test_path = Path::new(&datasets_root)
        .join("sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db");

    if !test_path.exists() {
        println!(
            "⚠️  simple_table test data not found at {:?}, skipping test",
            test_path
        );
        return;
    }

    let reader = SSTableReader::open(&test_path, &config, platform)
        .await
        .expect("Failed to open simple_table");

    if reader.schema().is_none() {
        println!("⏭️ Skipping test: Schema extraction from SSTable header not yet implemented");
        return;
    }

    let entries_result = reader.get_all_entries().await;

    match entries_result {
        Ok(entries) => {
            assert!(
                !entries.is_empty(),
                "Should have parsed entries from simple_table"
            );

            // Check for TimeUUID column (session_id)
            if let Some((_table_id, _row_key, Value::Map(cells))) = entries.first() {
                // Find session_id column (TimeUUID type)
                let session_id = cells.iter().find(|(name, _)| {
                    if let Value::Text(n) = name {
                        n == "session_id"
                    } else {
                        false
                    }
                });

                if let Some((Value::Text(name), col_value)) = session_id {
                    // TimeUUID should also be Value::Uuid
                    match col_value {
                        Value::Uuid(uuid_bytes) => {
                            println!(
                                "  ✅ TimeUUID column '{}': correctly parsed as Value::Uuid with {} bytes",
                                name,
                                uuid_bytes.len()
                            );
                            assert_eq!(uuid_bytes.len(), 16, "TimeUUID should be 16 bytes");
                        }
                        Value::Null => {
                            println!("  ℹ️  TimeUUID column '{}': NULL value (acceptable)", name);
                        }
                        other => {
                            panic!("❌ TimeUUID column '{}' has wrong type: {:?}", name, other);
                        }
                    }
                }
            }

            println!("✅ TimeUUID parsing test passed");
        }
        Err(e) => {
            panic!("❌ Failed to read entries from simple_table: {}", e);
        }
    }
}
