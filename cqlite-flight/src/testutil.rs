//! Shared test helpers: build real SSTables in-process via the write engine so
//! tests need no external `test-data` binaries. Compiled only under `cfg(test)`.

use std::collections::HashMap;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

pub const KS: &str = "flight_ks";
pub const TBL: &str = "items";

/// `CREATE TABLE` DDL matching [`simple_schema`].
pub const SIMPLE_DDL: &str =
    "CREATE TABLE flight_ks.items (id int PRIMARY KEY, name text, score int)";

/// PK=id(int), columns name(text), score(int). No clustering key.
pub fn simple_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("name", "text", true),
            col("score", "int", true),
        ],
        comments: HashMap::new(),
    }
}

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

/// A mutation writing the `name` and `score` columns for partition `id`.
pub fn write_row(id: i32, name: &str, score: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![
            CellOperation::Write {
                column: "name".into(),
                value: Value::Text(name.into()),
            },
            CellOperation::Write {
                column: "score".into(),
                value: Value::Integer(score),
            },
        ],
        ts,
        None,
    )
}

/// A row tombstone for partition `id`.
pub fn delete_row(id: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
}

/// Write each batch of mutations as its own SSTable; return the temp dir (keep it
/// alive for the test's lifetime), the data root, and the table directory.
pub fn build_sstables(
    schema: &TableSchema,
    batches: Vec<Vec<Mutation>>,
) -> (TempDir, PathBuf, PathBuf) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    for batch in batches {
        for m in batch {
            engine.write(m).expect("write");
        }
        rt.block_on(engine.flush()).expect("flush").expect("info");
    }
    let table_dir = data_dir.join(KS).join(TBL);
    (temp, data_dir, table_dir)
}

/// Total rows across a slice of record batches.
pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}
