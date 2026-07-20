//! Issue #2684 — `cqlite.flight.warm_tables` DECREMENTS on a whole-table
//! byte-budget (LRU) eviction.
//!
//! The existing `issue_2684_warm_tables_gauge_test.rs` covers rise-on-first-serve
//! and fall-on-retirement (a rebuild against an empty on-disk generation set).
//! Neither exercises the OTHER whole-table-removal site: `evict_to_budget`, which
//! calls `record_warm_tables` only when an LRU eviction empties a table entry.
//! This file drives enough DISTINCT warm tables past a tight warm byte budget to
//! force that whole-table eviction and proves the gauge is decremented on it.
//!
//! ## Isolation requirement
//!
//! `warm_table_count()` reads a PROCESS-GLOBAL atomic. An exact reading is only
//! meaningful with no sibling warm activity in flight, so this file holds EXACTLY
//! ONE `#[test]` — one file = one binary = one process (matching the
//! `issue_2370_gauge_readback_test.rs` / `issue_2684_warm_tables_gauge_test.rs`
//! precedent). Do not add a second `#[test]` here; add a sibling FILE instead.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --test issue_2684_warm_tables_eviction_test
//! ```

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;

use cqlite_flight::cancel::CancelFlag;
use cqlite_flight::warm::{ddl_hash, warm_table_count, TableKey, WarmTableRegistry};

const KS: &str = "warm_evict_ks";

fn schema(table: &str) -> TableSchema {
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.into(),
        table: table.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("id", "int", false), col("name", "text", true)],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

fn write_row(table: &str, id: i32, name: &str) -> Mutation {
    Mutation::new(
        TableId::new(KS, table),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".into(),
            value: Value::text(name),
        }],
        100,
        None,
    )
}

/// Flush a small single-SSTable fixture for `table` under a shared data root and
/// return its resolved SSTable directory (`<data_dir>/<ks>/<table>`).
fn build_table(
    data_dir: &std::path::Path,
    wal_root: &std::path::Path,
    table: &str,
) -> std::path::PathBuf {
    let config =
        WriteEngineConfig::new(data_dir.to_path_buf(), wal_root.join(table), schema(table));
    let mut engine = WriteEngine::new(config).expect("engine");
    engine.write(write_row(table, 1, "a")).expect("write");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    data_dir.join(KS).join(table)
}

#[test]
fn warm_tables_gauge_decrements_on_whole_table_budget_eviction() {
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_root = temp.path().join("wal");

    // Three distinct single-SSTable tables under one data root.
    let dir_a = build_table(&data_dir, &wal_root, "table_a");
    let dir_b = build_table(&data_dir, &wal_root, "table_b");
    let dir_c = build_table(&data_dir, &wal_root, "table_c");

    // A budget of 1 byte: every table's footprint exceeds it, but a rebuild never
    // evicts the CURRENT request's own (protected) generation — so exactly one
    // table stays warm at a time, and warming the NEXT distinct table always
    // whole-table-evicts the previous one (the `evict_to_budget` decrement site).
    let reg = WarmTableRegistry::with_budget(1);
    let cancel = CancelFlag::new();
    let ddl = ddl_hash("CREATE TABLE warm_evict_ks.x (id int PRIMARY KEY, name text)");

    let warm = |dir: &std::path::Path, table: &str| {
        reg.warm_readers(
            &TableKey::new(KS, table),
            ddl,
            &schema(table),
            None,
            dir,
            None,
            &cancel,
        )
        .expect("warm");
    };

    let baseline = warm_table_count();

    // Warm A: A's generation is protected within its own request, so it is NOT
    // evicted despite exceeding the budget — the gauge rises by one.
    warm(&dir_a, "table_a");
    assert_eq!(
        warm_table_count(),
        baseline + 1,
        "the first warm table raises the gauge to one"
    );
    assert_eq!(
        reg.metrics().snapshot().evicts,
        0,
        "warming the first (protected) table evicts nothing"
    );

    // Warm B: inserts B (transiently two live tables) then the byte budget forces
    // a WHOLE-TABLE LRU eviction of A. If the eviction decrement did NOT fire the
    // gauge would climb to baseline + 2; because it fires, the level holds at
    // baseline + 1 and an eviction is recorded — the decrement is observable.
    warm(&dir_b, "table_b");
    assert_eq!(
        warm_table_count(),
        baseline + 1,
        "the whole-table byte-budget eviction of A decrements the gauge back down \
         (it did NOT climb to baseline + 2)"
    );
    let evicts_after_b = reg.metrics().snapshot().evicts;
    assert!(
        evicts_after_b >= 1,
        "warming B past the budget whole-table-evicted A (got {evicts_after_b} evicts)"
    );

    // Warm C: again evicts the current LRU whole table (B). The gauge stays at
    // capacity and evicts keep accumulating — each whole-table eviction decrements.
    warm(&dir_c, "table_c");
    assert_eq!(
        warm_table_count(),
        baseline + 1,
        "each subsequent distinct warm table whole-table-evicts the prior one; the \
         gauge holds at capacity rather than climbing"
    );
    assert!(
        reg.metrics().snapshot().evicts > evicts_after_b,
        "warming C whole-table-evicted B (a second eviction decrement)"
    );

    drop(temp);
}
