//! Tests for null-element validation in collection writes.
//!
//! CQL semantics:
//!   - LIST and SET elements cannot be null
//!   - MAP keys cannot be null, but MAP values can be null
//!   - Tuple fields can be null

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

fn create_collection_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_nulls".to_string(),
        table: "coll_table".to_string(),
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
                name: "my_list".to_string(),
                data_type: "list<int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "my_set".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "my_map".to_string(),
                data_type: "map<text, int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "my_tuple".to_string(),
                data_type: "tuple<int, text, int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "my_frozen_list".to_string(),
                data_type: "frozen<list<int>>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "my_frozen_set".to_string(),
                data_type: "frozen<set<text>>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "my_frozen_map".to_string(),
                data_type: "frozen<map<text, int>>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

fn make_engine(temp: &TempDir) -> WriteEngine {
    let schema = create_collection_schema();
    let config = WriteEngineConfig::new(temp.path().join("data"), temp.path().join("wal"), schema);
    WriteEngine::new(config).expect("engine creation failed")
}

fn mutation_with_column(id: i32, column: &str, value: Value) -> Mutation {
    let table_id = TableId::new("test_nulls", "coll_table");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: column.to_string(),
        value,
    }];
    Mutation::new(table_id, pk, None, ops, 1_000_000, None)
}

// ---------- Tuple tests (nulls allowed) ----------

#[tokio::test]
async fn test_tuple_with_null_element_roundtrip() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Tuple(vec![
        Value::Integer(1),
        Value::Null,
        Value::Text("x".to_string()),
    ]);
    engine
        .write_async(mutation_with_column(1, "my_tuple", val))
        .await
        .expect("write should succeed");
    engine.flush().await.expect("flush should succeed");
}

#[tokio::test]
async fn test_tuple_all_nulls_roundtrip() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Tuple(vec![Value::Null, Value::Null, Value::Null]);
    engine
        .write_async(mutation_with_column(2, "my_tuple", val))
        .await
        .expect("write should succeed");
    engine.flush().await.expect("flush should succeed");
}

// ---------- LIST null-element rejection ----------

#[tokio::test]
async fn test_list_with_null_element_rejected() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::List(vec![Value::Integer(1), Value::Null, Value::Integer(3)]);
    engine
        .write_async(mutation_with_column(3, "my_list", val))
        .await
        .expect("write_async should succeed (memtable insert)");
    let result = engine.flush().await;
    assert!(
        result.is_err(),
        "flush should fail for list with null element"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.to_lowercase().contains("null"),
        "error message should mention null, got: {err_msg}"
    );
}

// ---------- SET null-element rejection ----------

#[tokio::test]
async fn test_set_with_null_element_rejected() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Set(vec![Value::Text("a".to_string()), Value::Null]);
    engine
        .write_async(mutation_with_column(4, "my_set", val))
        .await
        .expect("write_async should succeed");
    let result = engine.flush().await;
    assert!(
        result.is_err(),
        "flush should fail for set with null element"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.to_lowercase().contains("null"),
        "error message should mention null, got: {err_msg}"
    );
}

// ---------- MAP null-key rejection ----------

#[tokio::test]
async fn test_map_with_null_key_rejected() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Map(vec![(Value::Null, Value::Integer(1))]);
    engine
        .write_async(mutation_with_column(5, "my_map", val))
        .await
        .expect("write_async should succeed");
    let result = engine.flush().await;
    assert!(result.is_err(), "flush should fail for map with null key");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.to_lowercase().contains("null"),
        "error message should mention null, got: {err_msg}"
    );
}

// ---------- MAP null-value allowed ----------

#[tokio::test]
async fn test_map_with_null_value_allowed() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Map(vec![(Value::Text("k".to_string()), Value::Null)]);
    engine
        .write_async(mutation_with_column(6, "my_map", val))
        .await
        .expect("write should succeed");
    engine
        .flush()
        .await
        .expect("flush should succeed for map with null value");
}

// ---------- Frozen collection rejection / allowance ----------

#[tokio::test]
async fn test_frozen_list_with_null_rejected() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Frozen(Box::new(Value::List(vec![Value::Integer(1), Value::Null])));
    engine
        .write_async(mutation_with_column(7, "my_frozen_list", val))
        .await
        .expect("write_async should succeed");
    let result = engine.flush().await;
    assert!(
        result.is_err(),
        "flush should fail for frozen list with null"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.to_lowercase().contains("null"),
        "error message should mention null, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_frozen_set_with_null_rejected() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Frozen(Box::new(Value::Set(vec![Value::Null])));
    engine
        .write_async(mutation_with_column(8, "my_frozen_set", val))
        .await
        .expect("write_async should succeed");
    let result = engine.flush().await;
    assert!(
        result.is_err(),
        "flush should fail for frozen set with null"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.to_lowercase().contains("null"),
        "error message should mention null, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_frozen_map_null_key_rejected() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Frozen(Box::new(Value::Map(vec![(Value::Null, Value::Integer(1))])));
    engine
        .write_async(mutation_with_column(9, "my_frozen_map", val))
        .await
        .expect("write_async should succeed");
    let result = engine.flush().await;
    assert!(
        result.is_err(),
        "flush should fail for frozen map with null key"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.to_lowercase().contains("null"),
        "error message should mention null, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_frozen_map_null_value_allowed() {
    let temp = TempDir::new().unwrap();
    let mut engine = make_engine(&temp);
    let val = Value::Frozen(Box::new(Value::Map(vec![(
        Value::Text("k".to_string()),
        Value::Null,
    )])));
    engine
        .write_async(mutation_with_column(10, "my_frozen_map", val))
        .await
        .expect("write should succeed");
    engine
        .flush()
        .await
        .expect("flush should succeed for frozen map with null value");
}
