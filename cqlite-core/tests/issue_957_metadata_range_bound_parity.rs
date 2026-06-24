//! Issue #957 (Epic #951), metadata range-bound follow-up: a BOUNDED
//! multi-generation `scan_with_cell_metadata` (the WRITETIME/TTL projection
//! path) must enforce `start_key`/`end_key` identically to the materializing
//! `scan` and the streaming `scan_stream`.
//!
//! ## The bug this guards
//!
//! In the default (`not(tombstones)`) build with `write-support`,
//! `SSTableManager::scan_with_cell_metadata` routes a multi-generation table
//! through `merge_generations_for_read_with_metadata` (the metadata-aware sibling
//! of the LWW + tombstone-shadowing k-way merge). That helper originally took only
//! `(reader_list, schema, limit)` and DROPPED the caller's key range, so a bounded
//! multi-generation metadata read returned the FULL reconciled table instead of
//! just `[start_key, end_key]` — and applied `limit` BEFORE any range filter. The
//! single-generation / no-schema / no-`write-support` fallback paths bound
//! correctly per reader, so the bug only surfaces with >1 generation AND a schema.
//!
//! This is the same bound-drop that #957 already fixed for the non-metadata
//! `merge_generations_for_read` (commit ede52f1d); this test pins the metadata
//! variant to the same inclusive `[start, end]` semantics, keeping the two helpers
//! definitionally in lockstep.
//!
//! ## What this test asserts
//!
//! Build a 3-generation fixture (one flush per generation, no compaction) whose
//! partitions span a key range, then for a bounded range `[start, end]`:
//!
//!   1. `manager.scan_with_cell_metadata(start, end, ..)` returns ONLY in-range
//!      partitions.
//!   2. That bounded result is a STRICT SUBSET of the unbounded metadata scan
//!      (proving the bound is actually enforced — pre-fix the two were equal).
//!   3. Each in-range partition still carries its per-cell WRITETIME metadata
//!      (the range filter must not strip the metadata it is supposed to project).
//!
//! Bounds are exercised at the `SSTableManager` level directly because `SELECT`
//! does not expose a raw key-range surface. The inclusive `[start, end]` semantics
//! mirror the per-reader scan (`skip key < start`, `skip key > end`, using
//! `RowKey`'s `Ord`).
//!
//! NOTE: excluded under `tombstones` (the metadata merge branch is gated
//! `not(tombstones)`; the feature delegates to a metadata-less path, masking the
//! divergence — same gating lesson as #957/#958).
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_957_metadata_range_bound_parity

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use std::collections::HashMap;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::{Config, RowKey};
use tempfile::TempDir;

const KS: &str = "meta_range_ks";
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

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .count()
}

/// Partition key bytes for an int id, matching the on-disk encoding the merger
/// emits as the `RowKey` (big-endian int). Small positive ids keep byte order ==
/// numeric order, so `[start, end]` is intuitive.
fn pk_bytes(id: i32) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn row_key(id: i32) -> RowKey {
    RowKey(pk_bytes(id))
}

fn id_of(key: &RowKey) -> i32 {
    i32::from_be_bytes(key.0.clone().try_into().expect("4-byte int pk"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_multi_generation_metadata_scan_enforces_range() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // Build a 3-generation fixture on a blocking thread (WriteEngine spins its own
    // runtime via flush). Each generation writes a disjoint, interleaved set of
    // ids so the requested range straddles multiple generations.
    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        let schema = schema.clone();
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
            let mut engine = WriteEngine::new(config).expect("engine creation");

            // Gen 1 (ts=100): ids 1, 4, 7.
            engine.write(write_row(1, "g1-1", 1, 100)).unwrap();
            engine.write(write_row(4, "g1-4", 4, 100)).unwrap();
            engine.write(write_row(7, "g1-7", 7, 100)).unwrap();
            rt.block_on(engine.flush()).expect("flush1").expect("gen1");

            // Gen 2 (ts=200): ids 2, 5, 8.
            engine.write(write_row(2, "g2-2", 2, 200)).unwrap();
            engine.write(write_row(5, "g2-5", 5, 200)).unwrap();
            engine.write(write_row(8, "g2-8", 8, 200)).unwrap();
            rt.block_on(engine.flush()).expect("flush2").expect("gen2");

            // Gen 3 (ts=300): ids 3, 6, 9.
            engine.write(write_row(3, "g3-3", 3, 300)).unwrap();
            engine.write(write_row(6, "g3-6", 6, 300)).unwrap();
            engine.write(write_row(9, "g3-9", 9, 300)).unwrap();
            rt.block_on(engine.flush()).expect("flush3").expect("gen3");

            rt.block_on(engine.close()).expect("close engine");
        })
        .await
        .expect("fixture build task");
    }

    // Precondition: three distinct generations on disk (no compaction ran), so the
    // multi-generation metadata merge branch is taken in scan_with_cell_metadata.
    let sstable_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&sstable_dir),
        3,
        "fixture must exercise a multi-generation directory"
    );

    // Open an SSTableManager directly over the multi-generation directory.
    let cqlite_config = Config::default();
    let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform init"));
    let manager = SSTableManager::new(
        &data_dir,
        &cqlite_config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager open");

    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());

    // ── Unbounded baseline: every live row across all 3 generations ────────────
    let unbounded = manager
        .scan_with_cell_metadata(&table_id, None, None, None, Some(&schema))
        .await
        .expect("unbounded metadata scan must succeed");
    let unbounded_ids: Vec<i32> = {
        let mut ids: Vec<i32> = unbounded.iter().map(|(k, _, _)| id_of(k)).collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        unbounded_ids,
        (1..=9).collect::<Vec<_>>(),
        "unbounded metadata scan must return all 9 partitions across generations"
    );

    // ── Bounded range [3, 6] inclusive — straddles all three generations ───────
    // PRE-FIX: merge_generations_for_read_with_metadata dropped start/end, so this
    // returned all 9 partitions and this assertion failed. POST-FIX: only [3..=6].
    let start = row_key(3);
    let end = row_key(6);
    let expected_in_range: Vec<i32> = vec![3, 4, 5, 6];

    let bounded = manager
        .scan_with_cell_metadata(&table_id, Some(&start), Some(&end), None, Some(&schema))
        .await
        .expect("bounded metadata scan must succeed");
    let bounded_ids: Vec<i32> = {
        let mut ids: Vec<i32> = bounded.iter().map(|(k, _, _)| id_of(k)).collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        bounded_ids, expected_in_range,
        "Issue #957: bounded multi-generation metadata scan must return ONLY the \
         in-range partitions [3..=6] — pre-fix it returned the FULL reconciled table {:?}",
        unbounded_ids
    );

    // The bounded result is a STRICT subset of the unbounded result: same bound,
    // fewer rows, every bounded key present unbounded. Proof the bound does work
    // (pre-fix the two were equal).
    assert!(
        bounded_ids.len() < unbounded_ids.len(),
        "bounded result ({} rows) must be strictly smaller than unbounded ({} rows)",
        bounded_ids.len(),
        unbounded_ids.len()
    );
    for id in &bounded_ids {
        assert!(
            unbounded_ids.contains(id),
            "bounded id {id} must also appear in the unbounded metadata result"
        );
    }

    // The range filter must not strip the per-cell metadata this path projects:
    // every surviving in-range partition still carries WRITETIME for its columns.
    for (key, _value, meta) in &bounded {
        let id = id_of(key);
        assert!(
            meta.contains_key("name") && meta.contains_key("score"),
            "in-range partition id={id} must retain WRITETIME metadata for its cells; got {:?}",
            meta.keys().collect::<Vec<_>>()
        );
    }

    // ── Half-open bounds also honoured ─────────────────────────────────────────
    // start-only [6, ∞): ids 6,7,8,9.
    let start_only = manager
        .scan_with_cell_metadata(&table_id, Some(&row_key(6)), None, None, Some(&schema))
        .await
        .expect("start-only metadata scan");
    let mut start_only_ids: Vec<i32> = start_only.iter().map(|(k, _, _)| id_of(k)).collect();
    start_only_ids.sort_unstable();
    assert_eq!(
        start_only_ids,
        vec![6, 7, 8, 9],
        "start-only bound must keep ids >= start (inclusive)"
    );

    // end-only (∞, 3]: ids 1,2,3.
    let end_only = manager
        .scan_with_cell_metadata(&table_id, None, Some(&row_key(3)), None, Some(&schema))
        .await
        .expect("end-only metadata scan");
    let mut end_only_ids: Vec<i32> = end_only.iter().map(|(k, _, _)| id_of(k)).collect();
    end_only_ids.sort_unstable();
    assert_eq!(
        end_only_ids,
        vec![1, 2, 3],
        "end-only bound must keep ids <= end (inclusive)"
    );

    drop(temp_dir);
}
