//! Shared test helpers: build real SSTables in-process via the write engine so
//! tests need no external `test-data` binaries. Compiled only under `cfg(test)`.

use std::collections::HashMap;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
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

/// Write only the `name` column for partition `id` (leaves `score` absent → null).
pub fn write_name_only(id: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".into(),
            value: Value::Text(name.into()),
        }],
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

/// Wide-row table: PK=pk(int), clustering ck(text), regular val(int).
pub const WIDE_TBL: &str = "wide";

/// Schema for a clustering-key (wide-row) table.
pub fn clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: WIDE_TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "text".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("pk", "int", false),
            col("ck", "text", false),
            col("val", "int", true),
        ],
        comments: HashMap::new(),
    }
}

/// A mutation writing `val` for clustered row `(pk, ck)`.
pub fn write_clustered(pk: i32, ck: &str, val: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, WIDE_TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Text(ck.into()))),
        vec![CellOperation::Write {
            column: "val".into(),
            value: Value::Integer(val),
        }],
        ts,
        None,
    )
}

/// UUID-keyed table: PK=id(uuid), regular name(text).
pub const UUID_TBL: &str = "uu";

/// Schema for a uuid-keyed table.
pub fn uuid_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: UUID_TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "uuid".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("id", "uuid", false), col("name", "text", true)],
        comments: HashMap::new(),
    }
}

/// A mutation writing `name` for uuid-keyed partition `id`.
pub fn write_uuid_row(id: [u8; 16], name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, UUID_TBL),
        PartitionKey::single("id", Value::Uuid(id)),
        None,
        vec![CellOperation::Write {
            column: "name".into(),
            value: Value::Text(name.into()),
        }],
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
    let table_dir = data_dir.join(&schema.keyspace).join(&schema.table);
    (temp, data_dir, table_dir)
}

/// Total rows across a slice of record batches.
pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Simulate a Sidecar snapshot: hardlink every SSTable component file from
/// `table_dir` into `table_dir/snapshots/<name>/` (Cassandra's snapshot layout).
/// Returns the snapshot directory.
pub fn make_snapshot(table_dir: &std::path::Path, name: &str) -> PathBuf {
    let snap = table_dir.join("snapshots").join(name);
    std::fs::create_dir_all(&snap).unwrap();
    for entry in std::fs::read_dir(table_dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_file() {
            let dest = snap.join(entry.file_name());
            // Hardlink like Sidecar does; fall back to copy across filesystems.
            std::fs::hard_link(&path, &dest).or_else(|_| std::fs::copy(&path, &dest).map(|_| ())).unwrap();
        }
    }
    snap
}
