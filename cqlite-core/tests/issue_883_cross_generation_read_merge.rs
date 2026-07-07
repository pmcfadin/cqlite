//! Issue #883: `SELECT *` over a table directory holding multiple SSTable
//! generations must reproduce the fully-merged authoritative table state —
//! cross-generation last-write-wins plus row/cell tombstone suppression —
//! matching Cassandra read semantics.
//!
//! Before the fix, the read path concatenated each generation's live rows: a row
//! present in several generations was duplicated, and a row/cell deleted in a
//! LATER generation leaked back in because each reader suppresses only its own
//! tombstones. This test writes three generations via the public WriteEngine API
//! (flushing between each, with NO compaction), then scans through
//! `SSTableManager` and asserts the reconciled live-row set.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_883_cross_generation_read_merge

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use cqlite_core::ScanRow;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const KS: &str = "gen_ks";
const TBL: &str = "items";

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
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
                name: "score".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, name: &str, score: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "score".to_string(),
            value: Value::Integer(score),
        },
    ];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

/// Write only the `name` column — used to prove disjoint columns survive across
/// generations (the older generation's `score` must NOT be dropped).
fn write_name_only(id: i32, name: &str, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn delete_row(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    Mutation::new(
        TableId::new(KS, TBL),
        pk,
        None,
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
}

fn delete_score_column(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Delete {
        column: "score".to_string(),
        local_deletion_time: None,
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .count()
}

/// Extract a column value from a scan row (`Value::Map` of `(Text(col), value)`).
fn col<'a>(row: &'a ScanRow, name: &str) -> Option<&'a Value> {
    match row {
        // Issue #1334: rows decode to `ScanRow::Row` keyed by `Arc<str>`.
        ScanRow::Row(cells) => {
            cells
                .iter()
                .find_map(|(k, v)| if k.as_ref() == name { Some(v) } else { None })
        }
        _ => None,
    }
}

#[test]
fn select_star_merges_generations_with_lww_and_tombstone_suppression() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // ── Write three generations, flushing between each (no compaction) ─────────
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // Gen 1 (ts=100): PK 1..=4, each name+score.
    engine.write(write_row(1, "n1-v1", 11, 100)).unwrap();
    engine.write(write_row(2, "n2-v1", 22, 100)).unwrap();
    engine.write(write_row(3, "n3-v1", 33, 100)).unwrap();
    engine.write(write_row(4, "n4-v1", 44, 100)).unwrap();
    rt.block_on(engine.flush()).expect("flush 1").expect("gen1");

    // Gen 2 (ts=200): PK1 name-only update (disjoint), PK3 full overwrite, PK5 new.
    engine.write(write_name_only(1, "n1-v2", 200)).unwrap();
    engine.write(write_row(3, "n3-v2", 333, 200)).unwrap();
    engine.write(write_row(5, "n5-v2", 55, 200)).unwrap();
    rt.block_on(engine.flush()).expect("flush 2").expect("gen2");

    // Gen 3 (ts=300): row-delete PK2, cell-delete score@PK4, PK6 new.
    engine.write(delete_row(2, 300)).unwrap();
    engine.write(delete_score_column(4, 300)).unwrap();
    engine.write(write_row(6, "n6-v3", 66, 300)).unwrap();
    rt.block_on(engine.flush()).expect("flush 3").expect("gen3");

    rt.block_on(engine.close()).expect("close engine");

    // Precondition: three distinct generations are on disk (no compaction ran).
    let sstable_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&sstable_dir),
        3,
        "test must exercise a multi-generation directory"
    );

    // ── Reopen and scan ───────────────────────────────────────────────────────
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager open")
    });

    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("scan must not error");

    // ── Reconciled state: live rows are exactly {1, 3, 4, 5, 6} ───────────────
    // Before the fix this scan returned duplicates (PK1/PK3 from several
    // generations) and resurrected PK2/score@PK4 from gen1.
    let by_pk: HashMap<Vec<u8>, ScanRow> = results
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
        .collect();

    assert_eq!(
        results.len(),
        by_pk.len(),
        "no duplicate partition keys across generations (got {} rows, {} distinct)",
        results.len(),
        by_pk.len()
    );
    assert_eq!(
        results.len(),
        5,
        "expected exactly 5 live rows after cross-generation merge, got {}",
        results.len()
    );

    let pk = |id: i32| -> Vec<u8> { id.to_be_bytes().to_vec() };

    // PK2 row-deleted in gen3 (ts=300) — must NOT resurrect from gen1 (ts=100).
    assert!(
        !by_pk.contains_key(&pk(2)),
        "PK2 was row-deleted in a later generation; it must be suppressed"
    );

    // PK1: disjoint columns merge across generations — name from gen2, score from gen1.
    let row1 = by_pk.get(&pk(1)).expect("PK1 present");
    assert_eq!(
        col(row1, "name"),
        Some(&Value::Text("n1-v2".to_string())),
        "PK1 name must be the gen2 (newer) value"
    );
    assert_eq!(
        col(row1, "score"),
        Some(&Value::Integer(11)),
        "PK1 score (written only in gen1) must survive the name-only gen2 update"
    );

    // PK3: full overwrite — gen2 wins both columns.
    let row3 = by_pk.get(&pk(3)).expect("PK3 present");
    assert_eq!(col(row3, "name"), Some(&Value::Text("n3-v2".to_string())));
    assert_eq!(col(row3, "score"), Some(&Value::Integer(333)));

    // PK4: cell-delete of score in gen3 shadows gen1's score; name survives.
    let row4 = by_pk.get(&pk(4)).expect("PK4 present");
    assert_eq!(col(row4, "name"), Some(&Value::Text("n4-v1".to_string())));
    assert!(
        col(row4, "score").is_none(),
        "PK4 score was cell-deleted in a later generation; it must be absent, got {:?}",
        col(row4, "score")
    );

    // PK5 / PK6: single-generation rows pass through unchanged.
    let row5 = by_pk.get(&pk(5)).expect("PK5 present");
    assert_eq!(col(row5, "score"), Some(&Value::Integer(55)));
    let row6 = by_pk.get(&pk(6)).expect("PK6 present");
    assert_eq!(col(row6, "score"), Some(&Value::Integer(66)));

    drop(temp_dir);
}
