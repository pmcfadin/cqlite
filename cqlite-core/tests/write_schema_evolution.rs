//! Schema evolution tests for the write engine (#459)
//!
//! Verifies behavior when the schema changes between write sessions.
//! The WriteEngine binds to a single `TableSchema` at construction time.
//! Schema evolution is tested by:
//! 1. Creating engine with schema v1, writing mutations, flushing, dropping engine
//! 2. Creating new engine with schema v2 (same data dir), writing more, flushing
//! 3. Verifying both sessions produce valid SSTables

#![cfg(feature = "write-support")]

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

// ─────────────────────────────── helpers ────────────────────────────────────

/// Build a `TableSchema` from a slice of `(name, data_type)` pairs.
///
/// The column whose name equals `pk_name` becomes the sole partition key.
/// All columns (including the PK column) appear in `columns`.
fn build_schema(
    keyspace: &str,
    table: &str,
    columns: &[(&str, &str)],
    pk_name: &str,
    clustering: Vec<ClusteringColumn>,
) -> TableSchema {
    let mut partition_keys = Vec::new();
    let mut col_defs = Vec::new();

    for (i, (name, dtype)) in columns.iter().enumerate() {
        if *name == pk_name {
            partition_keys.push(KeyColumn {
                name: name.to_string(),
                data_type: dtype.to_string(),
                position: i,
            });
        }
        col_defs.push(Column {
            name: name.to_string(),
            data_type: dtype.to_string(),
            nullable: *name != pk_name,
            default: None,
            is_static: false,
        });
    }

    TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        partition_keys,
        clustering_keys: clustering,
        columns: col_defs,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Build a `Mutation` with the given partition key value and additional column writes.
fn build_mutation(
    keyspace: &str,
    table: &str,
    pk_name: &str,
    pk_value: Value,
    extra_cols: Vec<(&str, Value)>,
    timestamp: i64,
) -> Mutation {
    let table_id = TableId::new(keyspace, table);
    let pk = PartitionKey::single(pk_name, pk_value);
    let ops: Vec<CellOperation> = extra_cols
        .into_iter()
        .map(|(col, val)| CellOperation::Write {
            column: col.to_string(),
            value: val,
        })
        .collect();
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// Build a `Mutation` with a clustering key.
fn build_mutation_with_ck(
    keyspace: &str,
    table: &str,
    pk_name: &str,
    pk_value: Value,
    ck_name: &str,
    ck_value: Value,
    extra_cols: Vec<(&str, Value)>,
    timestamp: i64,
) -> Mutation {
    let table_id = TableId::new(keyspace, table);
    let pk = PartitionKey::single(pk_name, pk_value);
    let ck = Some(ClusteringKey::single(ck_name, ck_value));
    let ops: Vec<CellOperation> = extra_cols
        .into_iter()
        .map(|(col, val)| CellOperation::Write {
            column: col.to_string(),
            value: val,
        })
        .collect();
    Mutation::new(table_id, pk, ck, ops, timestamp, None)
}

// ──────────────────────────────── tests ─────────────────────────────────────

/// Adding a column between sessions: both sessions must flush successfully.
#[tokio::test]
async fn test_add_column_writes_succeed() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");

    // Session 1: schema v1 — (id:int, name:text)
    let schema_v1 = build_schema(
        "evo_ks",
        "add_col",
        &[("id", "int"), ("name", "text")],
        "id",
        vec![],
    );
    {
        let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal1"), schema_v1);
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 0..3_i32 {
            let m = build_mutation(
                "evo_ks",
                "add_col",
                "id",
                Value::Integer(i),
                vec![("name", Value::Text(format!("user_{i}")))],
                1_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        let info = engine.flush().await.unwrap();
        assert!(info.is_some(), "session 1 flush should return Some");
        let info = info.unwrap();
        assert_eq!(
            info.partition_count, 3,
            "session 1 should have 3 partitions"
        );
        assert!(info.data_path.exists());
    }

    // Session 2: schema v2 — (id:int, name:text, email:text)
    let schema_v2 = build_schema(
        "evo_ks",
        "add_col",
        &[("id", "int"), ("name", "text"), ("email", "text")],
        "id",
        vec![],
    );
    {
        let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal2"), schema_v2);
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 10..13_i32 {
            let m = build_mutation(
                "evo_ks",
                "add_col",
                "id",
                Value::Integer(i),
                vec![
                    ("name", Value::Text(format!("user_{i}"))),
                    ("email", Value::Text(format!("user_{i}@example.com"))),
                ],
                2_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        let info = engine.flush().await.unwrap();
        assert!(info.is_some(), "session 2 flush should return Some");
        let info = info.unwrap();
        assert_eq!(
            info.partition_count, 3,
            "session 2 should have 3 partitions"
        );
        assert!(info.data_path.exists());
    }
}

/// Removing a column between sessions: both sessions must flush successfully.
#[tokio::test]
async fn test_remove_column_writes_succeed() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");

    // Session 1: schema v1 — (id:int, name:text, value:int)
    let schema_v1 = build_schema(
        "evo_ks",
        "rm_col",
        &[("id", "int"), ("name", "text"), ("value", "int")],
        "id",
        vec![],
    );
    {
        let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal1"), schema_v1);
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 0..3_i32 {
            let m = build_mutation(
                "evo_ks",
                "rm_col",
                "id",
                Value::Integer(i),
                vec![
                    ("name", Value::Text(format!("item_{i}"))),
                    ("value", Value::Integer(i * 10)),
                ],
                1_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        let info = engine.flush().await.unwrap().unwrap();
        assert_eq!(info.partition_count, 3);
        assert!(info.data_path.exists());
    }

    // Session 2: schema v2 — (id:int, name:text) — value column removed
    let schema_v2 = build_schema(
        "evo_ks",
        "rm_col",
        &[("id", "int"), ("name", "text")],
        "id",
        vec![],
    );
    {
        let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal2"), schema_v2);
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 10..13_i32 {
            let m = build_mutation(
                "evo_ks",
                "rm_col",
                "id",
                Value::Integer(i),
                vec![("name", Value::Text(format!("item_{i}")))],
                2_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        let info = engine.flush().await.unwrap().unwrap();
        assert_eq!(info.partition_count, 3);
        assert!(info.data_path.exists());
    }
}

/// Two sessions with different schemas produce two separate SSTable sets on disk.
#[tokio::test]
async fn test_multiple_flushes_mixed_schemas() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");

    // Session 1: schema v1 — (id:int, col_a:text, col_b:text)
    let schema_v1 = build_schema(
        "evo_ks",
        "multi_flush",
        &[("id", "int"), ("col_a", "text"), ("col_b", "text")],
        "id",
        vec![],
    );
    let info1 = {
        let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal1"), schema_v1);
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 0..5_i32 {
            let m = build_mutation(
                "evo_ks",
                "multi_flush",
                "id",
                Value::Integer(i),
                vec![
                    ("col_a", Value::Text(format!("a_{i}"))),
                    ("col_b", Value::Text(format!("b_{i}"))),
                ],
                1_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        engine.flush().await.unwrap().unwrap()
    };
    assert_eq!(info1.partition_count, 5);

    // Session 2: schema v2 — (id:int, col_a:text, col_b:text, col_c:text)
    let schema_v2 = build_schema(
        "evo_ks",
        "multi_flush",
        &[
            ("id", "int"),
            ("col_a", "text"),
            ("col_b", "text"),
            ("col_c", "text"),
        ],
        "id",
        vec![],
    );
    let info2 = {
        let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal2"), schema_v2);
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 10..15_i32 {
            let m = build_mutation(
                "evo_ks",
                "multi_flush",
                "id",
                Value::Integer(i),
                vec![
                    ("col_a", Value::Text(format!("a_{i}"))),
                    ("col_b", Value::Text(format!("b_{i}"))),
                    ("col_c", Value::Text(format!("c_{i}"))),
                ],
                2_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        engine.flush().await.unwrap().unwrap()
    };
    assert_eq!(info2.partition_count, 5);

    // Both SSTables must exist and be distinct files.
    assert!(info1.data_path.exists());
    assert!(info2.data_path.exists());
    assert_ne!(info1.data_path, info2.data_path);

    // Count Data.db files under the table output directory.
    let table_dir = info1
        .data_path
        .parent()
        .expect("data path should have a parent directory");
    let data_db_count = std::fs::read_dir(table_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        .count();
    assert_eq!(
        data_db_count, 2,
        "expected 2 Data.db files, one per session"
    );
}

/// Adding a clustering column between sessions: both sessions flush correctly.
#[tokio::test]
async fn test_add_clustering_column() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");

    // Session 1: schema v1 — (id:int, name:text), no clustering
    let schema_v1 = build_schema(
        "evo_ks",
        "ck_evo",
        &[("id", "int"), ("name", "text")],
        "id",
        vec![],
    );
    {
        let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal1"), schema_v1);
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 0..3_i32 {
            let m = build_mutation(
                "evo_ks",
                "ck_evo",
                "id",
                Value::Integer(i),
                vec![("name", Value::Text(format!("name_{i}")))],
                1_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        let info = engine.flush().await.unwrap().unwrap();
        assert_eq!(info.partition_count, 3);
        assert!(info.data_path.exists());
    }

    // Session 2: schema v2 — (id:int, ck:int, name:text) with clustering key ck
    let schema_v2 = build_schema(
        "evo_ks",
        "ck_evo",
        &[("id", "int"), ("ck", "int"), ("name", "text")],
        "id",
        vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            order: ClusteringOrder::Asc,
            position: 0,
        }],
    );
    {
        let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal2"), schema_v2);
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 0..3_i32 {
            let m = build_mutation_with_ck(
                "evo_ks",
                "ck_evo",
                "id",
                Value::Integer(i),
                "ck",
                Value::Integer(i * 100),
                vec![("name", Value::Text(format!("name_v2_{i}")))],
                2_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        let info = engine.flush().await.unwrap().unwrap();
        assert_eq!(info.partition_count, 3);
        assert!(info.data_path.exists());
    }
}

/// Sanity check: reopening with the exact same schema works correctly.
#[tokio::test]
async fn test_reopen_same_schema() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");

    let make_schema = || {
        build_schema(
            "evo_ks",
            "reopen",
            &[("id", "int"), ("name", "text")],
            "id",
            vec![],
        )
    };

    // Session 1
    {
        let config =
            WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal1"), make_schema());
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 0..4_i32 {
            let m = build_mutation(
                "evo_ks",
                "reopen",
                "id",
                Value::Integer(i),
                vec![("name", Value::Text(format!("s1_user_{i}")))],
                1_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        let info = engine.flush().await.unwrap().unwrap();
        assert_eq!(info.partition_count, 4);
        assert!(info.data_path.exists());
    }

    // Session 2 — identical schema, different rows
    {
        let config =
            WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal2"), make_schema());
        let mut engine = WriteEngine::new(config).unwrap();
        for i in 10..14_i32 {
            let m = build_mutation(
                "evo_ks",
                "reopen",
                "id",
                Value::Integer(i),
                vec![("name", Value::Text(format!("s2_user_{i}")))],
                2_000_000 + i as i64,
            );
            engine.write_async(m).await.unwrap();
        }
        let info = engine.flush().await.unwrap().unwrap();
        assert_eq!(info.partition_count, 4);
        assert!(info.data_path.exists());
    }
}
