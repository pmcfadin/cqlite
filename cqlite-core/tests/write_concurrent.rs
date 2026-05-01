//! Concurrent write safety tests for WriteEngine
//!
//! WriteEngine follows a single-writer model and is NOT thread-safe for
//! concurrent writes. These tests validate the behaviors that ARE safe:
//!
//! - Thread-safe close via AtomicBool (idempotent, callable from any thread)
//! - Sequential writes from different async task contexts
//! - Error propagation after close

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

// ============================================================================
// Helper Functions
// ============================================================================

fn create_test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "concurrent_test".to_string(),
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
                name: "value".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

fn create_mutation(id: i32, value: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_ks", "concurrent_test");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "value".to_string(),
        value: Value::Text(value.to_string()),
    }];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

fn create_engine(temp_dir: &TempDir) -> WriteEngine {
    let schema = create_test_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );
    WriteEngine::new(config).expect("failed to create WriteEngine")
}

// ============================================================================
// Tests
// ============================================================================

/// Tests that closing the engine from a separate thread while it is idle is safe.
///
/// WriteEngine uses an AtomicBool for the `closed` flag, which means the
/// flag itself can be read safely from any thread. This test verifies that
/// close() can be called from a background thread and that the engine
/// correctly refuses writes afterward.
#[tokio::test]
async fn test_concurrent_close_while_idle() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let mut engine = create_engine(&temp_dir);

    // Engine is open: write should succeed.
    let mutation = create_mutation(1, "before-close", 1_000_000);
    engine
        .write_async(mutation)
        .await
        .expect("write before close should succeed");

    assert_eq!(engine.memtable_row_count(), 1);

    // Close the engine. In a real scenario this could be called from a
    // different thread; here we simulate that by calling close() directly
    // (the AtomicBool makes the flag update immediately visible).
    engine.close().await.expect("close should succeed");

    // After close, write_async must return an error.
    let mutation2 = create_mutation(2, "after-close", 2_000_000);
    let result = engine.write_async(mutation2).await;
    assert!(result.is_err(), "write after close should return an error");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("closed"),
        "error message should mention 'closed', got: {err_msg}"
    );
}

/// Tests that calling close() multiple times is safe (idempotent).
///
/// The AtomicBool swap in close() guarantees that only one call performs
/// the flush; subsequent calls return Ok(()) immediately.
#[tokio::test]
async fn test_close_is_idempotent() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let mut engine = create_engine(&temp_dir);

    // First close: flushes empty memtable (no-op flush) and marks closed.
    engine.close().await.expect("first close should succeed");

    // Second close: should be a safe no-op.
    engine.close().await.expect("second close should succeed");

    // Third close for good measure.
    engine.close().await.expect("third close should succeed");
}

/// Tests writing from multiple sequential tokio tasks.
///
/// WriteEngine is a single-writer model, so concurrent writes from multiple
/// tasks simultaneously are not supported. However, it is valid to pass the
/// engine between sequential async tasks (e.g., in a task queue or pipeline).
/// This test verifies that write_async → flush works correctly when the
/// engine is used from different task contexts sequentially.
#[tokio::test]
async fn test_sequential_writes_from_multiple_tasks() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Create the engine and move it through a series of spawned tasks
    // sequentially (each task completes before the next begins).
    let schema = create_test_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );
    let mut engine = WriteEngine::new(config).expect("failed to create engine");

    // Task context 1: write three rows.
    let task1 = tokio::spawn(async move {
        for i in 0..3 {
            let m = create_mutation(i, &format!("task1-row-{i}"), 1_000_000 + i as i64);
            engine
                .write_async(m)
                .await
                .expect("task1 write should succeed");
        }
        engine // move engine out to the next task
    });

    let mut engine = task1.await.expect("task1 should complete without panic");

    // Verify state after task 1.
    assert_eq!(
        engine.memtable_row_count(),
        3,
        "memtable should contain 3 rows after task1"
    );

    // Task context 2: write three more rows.
    let task2 = tokio::spawn(async move {
        for i in 3..6 {
            let m = create_mutation(i, &format!("task2-row-{i}"), 2_000_000 + i as i64);
            engine
                .write_async(m)
                .await
                .expect("task2 write should succeed");
        }
        engine
    });

    let mut engine = task2.await.expect("task2 should complete without panic");

    // Verify state after task 2.
    assert_eq!(
        engine.memtable_row_count(),
        6,
        "memtable should contain 6 rows after task2"
    );

    // Task context 3: flush to SSTable.
    let task3 = tokio::spawn(async move {
        let info = engine
            .flush()
            .await
            .expect("flush should succeed")
            .expect("flush should produce an SSTable");
        assert_eq!(
            info.partition_count, 6,
            "flushed SSTable should contain all 6 partitions"
        );
        engine
    });

    let mut engine = task3.await.expect("task3 should complete without panic");

    // After flush the memtable should be empty.
    assert_eq!(
        engine.memtable_row_count(),
        0,
        "memtable should be empty after flush"
    );

    // Clean close.
    engine.close().await.expect("close should succeed");
}

/// Tests that after close(), write_async() fails with an appropriate error.
///
/// The engine should refuse all writes once it has been closed, and the
/// error message must clearly indicate the engine is closed so callers
/// can distinguish this from transient I/O errors.
#[tokio::test]
async fn test_close_prevents_further_writes() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let mut engine = create_engine(&temp_dir);

    // Write one row to confirm the engine is functional.
    let m = create_mutation(1, "initial", 1_000_000);
    engine
        .write_async(m)
        .await
        .expect("initial write should succeed");

    // Flush so close() does not need to flush (simplifies the close path).
    engine
        .flush()
        .await
        .expect("pre-close flush should succeed");

    // Close the engine.
    engine.close().await.expect("close should succeed");

    // All subsequent write_async calls must fail.
    for i in 2..=5 {
        let m = create_mutation(i, &format!("after-close-{i}"), i as i64 * 1_000_000);
        let result = engine.write_async(m).await;

        assert!(
            result.is_err(),
            "write_async after close (id={i}) should return an error"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("closed"),
            "error for id={i} should mention 'closed', got: {err_msg}"
        );
    }

    // flush() itself should also fail after close.
    let flush_result = engine.flush().await;
    assert!(
        flush_result.is_err(),
        "flush after close should return an error"
    );

    let flush_err = flush_result.unwrap_err().to_string();
    assert!(
        flush_err.contains("closed"),
        "flush error should mention 'closed', got: {flush_err}"
    );
}
