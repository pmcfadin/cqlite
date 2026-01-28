//! WriteEngine demonstration
//!
//! This example demonstrates the complete write path:
//! 1. Create WriteEngine
//! 2. Write mutations
//! 3. Automatic flush
//! 4. Manual flush
//! 5. Close cleanly
//!
//! Run with:
//! ```bash
//! cargo run --example write_engine_demo --features write-support
//! ```

#![cfg(feature = "write-support")]

use cqlite_core::error::Result;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

fn create_schema() -> TableSchema {
    TableSchema {
        keyspace: "demo_ks".to_string(),
        table: "users".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "email".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "age".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

fn create_user_mutation(id: i32, name: &str, email: &str, age: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new("demo_ks", "users");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "email".to_string(),
            value: Value::Text(email.to_string()),
        },
        CellOperation::Write {
            column: "age".to_string(),
            value: Value::Integer(age),
        },
    ];

    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init();

    println!("=== WriteEngine Demo ===\n");

    // Create temporary directory for demo
    let temp_dir = TempDir::new().unwrap();
    println!("Demo directory: {}\n", temp_dir.path().display());

    // Create schema
    let schema = create_schema();

    // Configure WriteEngine with low flush threshold for demo
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    )
    .with_flush_threshold(1024); // 1KB threshold for demo

    println!("Configuration:");
    println!("  Data directory: {}", config.data_dir.display());
    println!("  WAL directory: {}", config.wal_dir.display());
    println!("  Flush threshold: {} bytes\n", config.memtable_flush_threshold);

    // Create WriteEngine
    println!("Creating WriteEngine...");
    let mut engine = WriteEngine::new(config)?;
    println!("  Generation: {}", engine.generation());
    println!("  Memtable size: {} bytes", engine.memtable_size());
    println!("  WAL size: {} bytes\n", engine.wal_size());

    // Write mutations
    println!("Writing mutations...");
    let users = vec![
        (1, "Alice Johnson", "alice@example.com", 30),
        (2, "Bob Smith", "bob@example.com", 25),
        (3, "Charlie Brown", "charlie@example.com", 35),
        (4, "Diana Prince", "diana@example.com", 28),
        (5, "Eve Wilson", "eve@example.com", 32),
    ];

    for (i, (id, name, email, age)) in users.iter().enumerate() {
        let timestamp = 1000000 + i as i64 * 1000;
        let mutation = create_user_mutation(*id, name, email, *age, timestamp);
        engine.write_async(mutation).await?;
        println!("  Wrote user {}: {}", id, name);
    }

    println!("\nAfter writes:");
    println!("  Memtable rows: {}", engine.memtable_row_count());
    println!("  Memtable size: {} bytes", engine.memtable_size());
    println!("  WAL size: {} bytes", engine.wal_size());
    println!("  Generation: {}\n", engine.generation());

    // Continue writing to trigger automatic flush
    println!("Writing more mutations to trigger automatic flush...");
    for i in 6..50 {
        let timestamp = 1000000 + i as i64 * 1000;
        let mutation = create_user_mutation(
            i,
            &format!("User{}", i),
            &format!("user{}@example.com", i),
            20 + (i % 40),
            timestamp,
        );
        engine.write_async(mutation).await?;

        if i % 10 == 0 {
            println!("  Wrote {} mutations", i);
        }
    }

    println!("\nAfter automatic flush:");
    println!("  Memtable rows: {}", engine.memtable_row_count());
    println!("  Memtable size: {} bytes", engine.memtable_size());
    println!("  WAL size: {} bytes", engine.wal_size());
    println!("  Generation: {} (incremented after flush)\n", engine.generation());

    // Write a few more mutations
    println!("Writing final mutations...");
    for i in 50..55 {
        let timestamp = 1000000 + i as i64 * 1000;
        let mutation = create_user_mutation(
            i,
            &format!("User{}", i),
            &format!("user{}@example.com", i),
            20 + (i % 40),
            timestamp,
        );
        engine.write_async(mutation).await?;
    }

    println!("  Wrote 5 more mutations\n");

    // Manual flush
    println!("Performing manual flush...");
    let info = engine.flush().await?;

    if let Some(info) = info {
        println!("  Flushed {} partitions", info.partition_count);
        println!("  Data size: {} bytes", info.data_size);
        println!("  Data path: {}", info.data_path.display());
        println!("  Index path: {}", info.index_path.display());
        println!("  Filter path: {}", info.filter_path.display());
        println!("  Summary path: {}", info.summary_path.display());
        println!("  Statistics path: {}", info.stats_path.display());
        println!("  TOC path: {}", info.toc_path.display());
        println!("  Digest path: {}\n", info.digest_path.display());
    }

    println!("After manual flush:");
    println!("  Memtable rows: {}", engine.memtable_row_count());
    println!("  Memtable size: {} bytes", engine.memtable_size());
    println!("  WAL size: {} bytes", engine.wal_size());
    println!("  Generation: {}\n", engine.generation());

    // List generated SSTable files
    println!("Generated SSTable files:");
    let data_dir = temp_dir.path().join("data");
    if data_dir.exists() {
        let mut entries: Vec<_> = std::fs::read_dir(&data_dir)?
            .filter_map(|e| e.ok())
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let filename = entry.file_name();
            let metadata = entry.metadata()?;
            println!("  {} ({} bytes)", filename.to_string_lossy(), metadata.len());
        }
    }

    // Close engine
    println!("\nClosing WriteEngine...");
    engine.close().await?;
    println!("  Engine closed successfully\n");

    println!("=== Demo Complete ===");
    println!("\nDemo files will be cleaned up when temporary directory is dropped.");

    Ok(())
}
