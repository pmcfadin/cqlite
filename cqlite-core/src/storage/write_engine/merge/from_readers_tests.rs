//! Issue #2346 — path-based vs shared-reader (`new_from_readers`) merge parity.
//!
//! Lifted VERBATIM out of `from_readers.rs` (issue #3120): that file had to grow
//! to carry the fail-closed terminator protocol + `catch_unwind`, and at 741
//! lines it was already at the ~800-line source campsite target (epic #1116).
//! Splitting the inline test module out — a `*_tests.rs` sibling, per #1135 — is
//! the sanctioned way to make that room. Nothing here changed: the bodies are
//! byte-identical to their pre-move form, and the module is still a CHILD of
//! `from_readers` (included via `#[path]`), so `super::*` / `super::super::*`
//! resolve exactly as before.

use super::*;
use crate::platform::Platform;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
// `MergeEntry` is no longer among `from_readers`' own imports (issue #3120 moved
// the channel item to `MergeMsg`), so name it explicitly here.
use crate::storage::write_engine::merge::{MergeEntry, MergeStep};
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::storage::write_engine::test_support::{create_test_schema, flush_n_sstables_sync};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
use crate::types::Value;
use crate::Config;
use std::collections::HashMap;
use tempfile::TempDir;

fn config_for(temp_dir: &TempDir) -> WriteEngineConfig {
    WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
}

async fn open_reader(path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(path, &config, platform).await.unwrap()
}

/// Drain a merger into `(partition_key_bytes, fully-reconciled rows)` pairs,
/// sorted by partition key. Unlike a row-COUNT summary, this carries every
/// [`MergeEntry`] field — clustering key, timestamp, `RowData` cells, and all
/// tombstone/deletion markers — so a divergence in HOW the reader-based path
/// decodes or emits a cell (not just how many rows it produces) fails the
/// equality assertion. `MergeEntry: Eq`, so two `Vec<MergeEntry>` compare
/// cell-for-cell (issue #2346, parity-auditor F1).
fn collect_merge_entries(mut merger: KWayMerger) -> Vec<(Vec<u8>, Vec<MergeEntry>)> {
    let mut out = Vec::new();
    while let MergeStep::Partition { key, rows } = merger.step().expect("merge step") {
        out.push((key.key.clone(), rows));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Schema WITH a clustering key (`id int, ck int, name text` — PK `id`,
/// clustering `ck`), so the overlapping-generation fixture below exercises
/// per-clustering-row reconciliation (row/cell tombstones, value overwrite),
/// not just partition-granular merges (issue #2346, parity-auditor F1).
fn clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
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

fn clustering_config_for(temp_dir: &TempDir) -> WriteEngineConfig {
    WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        clustering_schema(),
    )
}

fn ck_mutation(id: i32, ck: i32, ts: i64, op: CellOperation) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(
            crate::storage::write_engine::mutation::ClusteringKey::single("ck", Value::Integer(ck)),
        ),
        vec![op],
        ts,
        None,
    )
}

/// Two OVERLAPPING generations (issue #2346, parity-auditor F1): the same
/// partition keys appear in BOTH SSTables with clustering rows that force
/// real cell-level reconciliation — a newer-generation value overwrite, a
/// cell tombstone, and a row tombstone — so the path-based vs reader-based
/// constructor comparison can catch a cell-level (not merely row-count)
/// divergence. Returns `[gen2_newer, gen1_older]` (newest-first, the order
/// both constructors expect).
fn flush_two_overlapping_generations(temp_dir: &TempDir) -> Vec<std::path::PathBuf> {
    let mut engine = WriteEngine::new(clustering_config_for(temp_dir)).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Generation 1 (older, ts=1000): baseline rows across partitions 1,2,3.
    for (id, ck, name) in [
        (1, 10, "g1-1-10"),
        (1, 20, "g1-1-20"),
        (2, 10, "g1-2-10"),
        (3, 10, "g1-3-10"),
    ] {
        engine
            .write(ck_mutation(
                id,
                ck,
                1000,
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::text(name.to_string()),
                },
            ))
            .unwrap();
    }
    let gen1 = rt.block_on(engine.flush()).unwrap().unwrap().data_path;

    // Generation 2 (newer, ts=2000): same partitions, overlapping rows —
    //  (1,10) value OVERWRITE, (1,20) CELL tombstone, (2,10) ROW tombstone,
    //  plus a fresh (3,20) row.
    engine
        .write(ck_mutation(
            1,
            10,
            2000,
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::text("g2-1-10".to_string()),
            },
        ))
        .unwrap();
    engine
        .write(ck_mutation(
            1,
            20,
            2000,
            CellOperation::Delete {
                column: "name".to_string(),
                local_deletion_time: Some(2),
            },
        ))
        .unwrap();
    // Row tombstone for (2,10): a delete mutation carrying a coexisting row
    // deletion, no surviving cells written afterward.
    engine
        .write(
            ck_mutation(
                2,
                10,
                2000,
                CellOperation::Delete {
                    column: "name".to_string(),
                    local_deletion_time: Some(2),
                },
            )
            .with_row_tombstone(2000, 2),
        )
        .unwrap();
    engine
        .write(ck_mutation(
            3,
            20,
            2000,
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::text("g2-3-20".to_string()),
            },
        ))
        .unwrap();
    let gen2 = rt.block_on(engine.flush()).unwrap().unwrap().data_path;

    vec![gen2, gen1]
}

/// Red-then-green (b): `KWayMerger::new_from_readers` (reader-based) must
/// reconcile BYTE-IDENTICALLY to `KWayMerger::new_cancellable` (path-based)
/// over the SAME SSTables — proving the path-based constructor's behaviour
/// is preserved and the two producer-thread shapes never diverge (issue
/// #2346). Fails to compile on pre-#2346 `main` (`new_from_readers` and
/// `Arc<SSTableReader>`-based construction do not exist there).
///
/// Plain `#[test]` (not `#[tokio::test]`): `flush_n_sstables_sync` drives
/// its OWN runtime to flush (mirrors the `cqlite-flight` test convention —
/// nesting a `#[tokio::test]` runtime here would panic on
/// "Cannot start a runtime from within a runtime"), so the SSTables are
/// built first, then a fresh runtime drives the async reader-open/merge.
#[test]
fn new_from_readers_matches_path_based_reconciliation() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = WriteEngine::new(config_for(&temp_dir)).unwrap();
    // Two distinct-generation SSTables — a real multi-run merge, not a
    // single-input vacuity.
    let paths = flush_n_sstables_sync(&mut engine, 2);
    assert_eq!(paths.len(), 2, "test precondition: two SSTables written");
    let schema = create_test_schema();

    let path_based = KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
        .expect("path-based merger constructs");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader_based = rt.block_on(async {
        let mut readers = Vec::with_capacity(paths.len());
        for path in &paths {
            readers.push(Arc::new(open_reader(path).await));
        }
        KWayMerger::new_from_readers(readers, &schema, ScanCancel::default(), None)
            .expect("reader-based merger constructs")
    });

    assert_eq!(
        collect_merge_entries(path_based),
        collect_merge_entries(reader_based),
        "reader-based full-scan merger must reconcile byte-identically to the \
         path-based one (issue #2346) — only WHO opens the reader differs"
    );
}

/// The STRONG delegation proof (issue #2346, parity-auditor F1): over TWO
/// OVERLAPPING generations (shared partition keys, clustering rows, a
/// value overwrite + a cell tombstone + a row tombstone), the reader-based
/// `new_from_readers` must produce a FULLY CELL-IDENTICAL reconciliation to
/// the path-based `new_cancellable` — every [`MergeEntry`] field, not just
/// the row count. The earlier disjoint-key fixture never reconciles two rows
/// of the SAME key, so it cannot catch a cell-level decode/emit divergence;
/// this one does.
#[test]
fn new_from_readers_matches_path_based_full_cell_equality_overlapping() {
    let temp_dir = TempDir::new().unwrap();
    let paths = flush_two_overlapping_generations(&temp_dir);
    assert_eq!(
        paths.len(),
        2,
        "test precondition: two overlapping generations"
    );
    let schema = clustering_schema();

    let path_based = KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
        .expect("path-based merger constructs");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader_based = rt.block_on(async {
        let mut readers = Vec::with_capacity(paths.len());
        for path in &paths {
            readers.push(Arc::new(open_reader(path).await));
        }
        KWayMerger::new_from_readers(readers, &schema, ScanCancel::default(), None)
            .expect("reader-based merger constructs")
    });

    let path_rows = collect_merge_entries(path_based);
    let reader_rows = collect_merge_entries(reader_based);

    // Non-vacuity: the overlapping fixture MUST yield real reconciled rows
    // across multiple partitions, or a cell-equality assertion over an empty
    // set would pass trivially.
    let total_rows: usize = path_rows.iter().map(|(_, r)| r.len()).sum();
    assert!(
        path_rows.len() >= 3 && total_rows >= 3,
        "fixture must reconcile several overlapping partitions/rows, got \
         {} partitions / {total_rows} rows",
        path_rows.len()
    );

    assert_eq!(
        path_rows, reader_rows,
        "reader-based merger must reconcile CELL-IDENTICALLY to the path-based \
         one over overlapping generations (overwrite + cell/row tombstones) — \
         full MergeEntry equality, not row counts (issue #2346, F1)"
    );
}

/// Red-then-green (b), point-read variant: the reader-based
/// `build_single_partition_merger_from_readers` must match the path-based
/// `build_single_partition_merger` for the SAME target key across the SAME
/// SSTables. Fails to compile on pre-#2346 `main` (the reader-based builder
/// does not exist there).
///
/// Plain `#[test]` — same rationale as the sibling test above.
#[test]
fn build_single_partition_merger_from_readers_matches_path_based() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = WriteEngine::new(config_for(&temp_dir)).unwrap();
    let paths = flush_n_sstables_sync(&mut engine, 2);
    let schema = create_test_schema();

    // `flush_n_sstables_sync` writes ids `batch*100 + row` for row in 0..5;
    // id=0 (batch 0, row 0) is always present.
    let key_bytes = PartitionKey::single("id", Value::Integer(0))
        .to_bytes(&schema)
        .expect("encode target key");

    let path_based = super::super::build_single_partition_merger(
        paths.clone(),
        std::slice::from_ref(&key_bytes),
        &schema,
        ScanCancel::default(),
    )
    .expect("path-based probe succeeds")
    .expect("path-based merger must find the key");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader_based = rt.block_on(async {
        let mut readers = Vec::with_capacity(paths.len());
        for path in &paths {
            readers.push(Arc::new(open_reader(path).await));
        }
        super::super::build_single_partition_merger_from_readers(
            readers,
            &[key_bytes],
            &schema,
            ScanCancel::default(),
        )
        .expect("reader-based probe succeeds")
        .expect("reader-based merger must find the key")
    });

    assert_eq!(
        collect_merge_entries(path_based),
        collect_merge_entries(reader_based),
        "reader-based point-read merger must reconcile byte-identically to the \
         path-based one (issue #2346)"
    );
}
