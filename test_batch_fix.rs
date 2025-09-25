#!/usr/bin/env rust-script

//! Test script to verify the batch operations fix
//!
//! ```cargo
//! [dependencies]
//! cqlite-core = { path = "./cqlite-core" }
//! tokio = { version = "1.0", features = ["full"] }
//! tempfile = "3.0"
//! ```

use std::sync::Arc;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing batch operations fix...");

    let temp_dir = TempDir::new()?;
    let config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::platform::Platform::new(&config).await?);

    let mut storage = cqlite_core::storage::StorageEngine::open(temp_dir.path(), &config, platform)
        .await?;

    println!("✅ Storage engine opened successfully");

    // Test batch write operations
    let batch_ops = vec![
        cqlite_core::storage::BatchOperation::Put {
            table_id: cqlite_core::types::TableId::new("test_table"),
            key: cqlite_core::RowKey::from("key1"),
            value: cqlite_core::Value::Text("value1".to_string()),
        },
        cqlite_core::storage::BatchOperation::Put {
            table_id: cqlite_core::types::TableId::new("test_table"),
            key: cqlite_core::RowKey::from("key2"),
            value: cqlite_core::Value::Text("value2".to_string()),
        },
        cqlite_core::storage::BatchOperation::Delete {
            table_id: cqlite_core::types::TableId::new("test_table"),
            key: cqlite_core::RowKey::from("key3"),
        },
    ];

    println!("🔄 Executing batch operations...");

    // This should not hang anymore
    let start_time = std::time::Instant::now();
    storage.batch_write(batch_ops).await?;
    let duration = start_time.elapsed();

    println!("✅ Batch operations completed in {:?}", duration);

    if duration.as_secs() > 5 {
        println!("⚠️  Warning: Batch operation took longer than expected ({}s)", duration.as_secs());
    } else {
        println!("🎉 Batch operation completed quickly - deadlock fix appears to work!");
    }

    storage.shutdown().await?;
    println!("✅ Storage engine shutdown successfully");

    Ok(())
}