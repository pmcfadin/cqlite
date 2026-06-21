//! Issue #885: the WRITETIME/TTL metadata read path
//! (`SSTableManager::scan_with_cell_metadata`, used when a query projects
//! `WRITETIME(col)` / `TTL(col)`) must reconcile across SSTable generations
//! exactly like the plain `scan` path fixed in #883 — and report the **winning**
//! cell's per-cell write timestamp / TTL after cross-generation last-write-wins.
//!
//! Before the fix, the metadata path concatenated each generation's rows: a row
//! present in several generations was duplicated, and a row/cell deleted in a
//! LATER generation leaked back in (each reader suppresses only its own
//! tombstones). This test writes three generations via the public WriteEngine API
//! (flushing between each, NO compaction), then scans through
//! `SSTableManager::scan_with_cell_metadata` and asserts both the reconciled
//! live-row set AND the per-cell WRITETIME of the winning cell.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_885_cross_generation_metadata_merge

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::{CellWriteMetadata, Value};
use cqlite_core::Config;
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

/// Write only the `name` column with a per-cell TTL.
fn write_name_only_ttl(id: i32, name: &str, ttl_seconds: u32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::WriteWithTtl {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
        ttl_seconds,
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
fn col<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
    match row {
        Value::Map(pairs) => pairs.iter().find_map(|(k, v)| match k {
            Value::Text(c) if c == name => Some(v),
            _ => None,
        }),
        _ => None,
    }
}

#[test]
fn metadata_scan_merges_generations_with_lww_and_tombstone_suppression() {
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

    // Gen 2 (ts=200): PK1 name-only update WITH TTL (disjoint, newer), PK3 full
    // overwrite, PK5 new.
    engine
        .write(write_name_only_ttl(1, "n1-v2", 3600, 200))
        .unwrap();
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

    // ── Reopen and scan with cell metadata ────────────────────────────────────
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
        .block_on(manager.scan_with_cell_metadata(&table_id, None, None, None, Some(&schema)))
        .expect("metadata scan must not error");

    // ── Reconciled state: live rows are exactly {1, 3, 4, 5, 6} ───────────────
    let by_pk: HashMap<Vec<u8>, (Value, HashMap<String, CellWriteMetadata>)> = results
        .iter()
        .map(|(k, v, m)| (k.0.clone(), (v.clone(), m.clone())))
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
        "expected exactly 5 live rows after cross-generation metadata merge, got {}",
        results.len()
    );

    let pk = |id: i32| -> Vec<u8> { id.to_be_bytes().to_vec() };

    // PK2 row-deleted in gen3 — must NOT resurrect from gen1.
    assert!(
        !by_pk.contains_key(&pk(2)),
        "PK2 was row-deleted in a later generation; it must be suppressed"
    );

    // Helper: fetch (value, metadata) for a PK.
    let row = |id: i32| -> &(Value, HashMap<String, CellWriteMetadata>) {
        by_pk.get(&pk(id)).expect("row present")
    };
    let writetime = |meta: &HashMap<String, CellWriteMetadata>, c: &str| -> i64 {
        meta.get(c)
            .unwrap_or_else(|| panic!("metadata for column '{c}' present"))
            .write_timestamp_micros
    };

    // PK1: disjoint columns merge — name from gen2 (ts=200), score from gen1 (ts=100).
    let (v1, m1) = row(1);
    assert_eq!(
        col(v1, "name"),
        Some(&Value::Text("n1-v2".to_string())),
        "PK1 name must be the gen2 (newer) value"
    );
    assert_eq!(
        col(v1, "score"),
        Some(&Value::Integer(11)),
        "PK1 score (written only in gen1) must survive the name-only gen2 update"
    );
    // WRITETIME is per WINNING cell: name won in gen2, score won in gen1.
    assert_eq!(
        writetime(m1, "name"),
        200,
        "PK1 WRITETIME(name) must be the gen2 winner's timestamp"
    );
    assert_eq!(
        writetime(m1, "score"),
        100,
        "PK1 WRITETIME(score) must be the gen1 timestamp (not the newer name update)"
    );
    // TTL: the winning gen2 name cell was written WITH ttl=3600. The reconciled
    // expiration must be that winning cell's TTL — never an arbitrary generation's.
    let exp1 = m1
        .get("name")
        .and_then(|m| m.expiration.as_ref())
        .expect("PK1 name (gen2 winner) carries the TTL it was written with");
    assert_eq!(
        exp1.ttl_seconds, 3600,
        "PK1 TTL(name) must be the winning gen2 cell's TTL"
    );
    // The gen1 score cell had no TTL — must always be None.
    assert!(
        m1.get("score")
            .and_then(|m| m.expiration.as_ref())
            .is_none(),
        "PK1 score had no TTL; expiration must be None"
    );

    // PK3: full overwrite — gen2 (ts=200) wins both columns.
    let (v3, m3) = row(3);
    assert_eq!(col(v3, "name"), Some(&Value::Text("n3-v2".to_string())));
    assert_eq!(col(v3, "score"), Some(&Value::Integer(333)));
    assert_eq!(writetime(m3, "name"), 200);
    assert_eq!(writetime(m3, "score"), 200);

    // PK4: cell-delete of score in gen3 shadows gen1's score; name survives at ts=100.
    let (v4, m4) = row(4);
    assert_eq!(col(v4, "name"), Some(&Value::Text("n4-v1".to_string())));
    assert!(
        col(v4, "score").is_none(),
        "PK4 score was cell-deleted in a later generation; it must be absent, got {:?}",
        col(v4, "score")
    );
    assert_eq!(writetime(m4, "name"), 100);
    assert!(
        !m4.contains_key("score"),
        "PK4 score is deleted; no metadata must be reported for it"
    );

    // PK5 / PK6: single-generation rows pass through with their own timestamps.
    let (v5, m5) = row(5);
    assert_eq!(col(v5, "score"), Some(&Value::Integer(55)));
    assert_eq!(writetime(m5, "name"), 200);
    assert_eq!(writetime(m5, "score"), 200);

    let (v6, m6) = row(6);
    assert_eq!(col(v6, "score"), Some(&Value::Integer(66)));
    assert_eq!(writetime(m6, "name"), 300);
    assert_eq!(writetime(m6, "score"), 300);

    drop(temp_dir);
}
