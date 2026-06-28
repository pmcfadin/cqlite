//! Shared test fixtures for the `write_engine` test modules.
//!
//! Extracted verbatim from `write_engine/mod.rs` (issue #1120, epic #1116).
//! These helpers are shared by the `mod`, `maintenance`, and `stats` test
//! modules; per-module-only helpers stay local to their owning submodule.

use super::{Mutation, TableSchema, WriteEngine};
use crate::schema::{Column, KeyColumn};
use crate::storage::write_engine::mutation::{CellOperation, PartitionKey, TableId};
use crate::types::Value;
use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) fn create_test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

pub(crate) fn create_test_mutation(id: i32, name: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];

    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// Helper: flush `n` distinct mutations through the engine synchronously.
///
/// Uses a dedicated single-threaded runtime so it can be called from both
/// sync test functions and (via `spawn_blocking`) from async contexts.
pub(crate) fn flush_n_sstables_sync(engine: &mut WriteEngine, n: usize) -> Vec<PathBuf> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut paths = Vec::new();
    for batch in 0..n {
        for row in 0..5 {
            let mutation = create_test_mutation(
                (batch * 100 + row) as i32,
                &format!("User-{}-{}", batch, row),
                1_000_000 + (batch * 100 + row) as i64,
            );
            engine.write(mutation).unwrap();
        }
        let info = rt.block_on(engine.flush()).unwrap().unwrap();
        paths.push(info.data_path);
    }
    paths
}
