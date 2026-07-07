//! Issue #957 (Epic #951), range-bound follow-up: a BOUNDED multi-generation
//! `scan_stream` must enforce `start_key`/`end_key` identically to the
//! materializing `scan`.
//!
//! ## The bug this guards
//!
//! In the default (`not(tombstones)`) build with `write-support`, both
//! `SSTableManager::scan` and `SSTableManager::scan_stream` route a
//! multi-generation table through `merge_generations_for_read` (the LWW +
//! tombstone-shadowing k-way merge). That helper originally took only
//! `(reader_list, schema, limit)` and DROPPED the caller's key range:
//!
//!   - `scan`'s multi-gen branch passed `limit` but not `start_key`/`end_key`.
//!   - `scan_stream`'s multi-gen branch passed `None` for the limit and, like
//!     `scan`, had no way to forward the range at all.
//!
//! So a bounded multi-generation read returned the FULL reconciled table instead
//! of just `[start_key, end_key]`. The single-generation / no-schema /
//! no-`write-support` fallback paths bound correctly per reader, so the bug only
//! surfaces with >1 generation AND a schema.
//!
//! ## What this test asserts
//!
//! Build a 3-generation fixture (one flush per generation, no compaction) whose
//! partitions span a key range, then for a bounded range `[start, end]`:
//!
//!   1. `manager.scan(start, end, ..)` returns ONLY in-range partitions.
//!   2. Draining `manager.scan_stream(start, end, ..)` returns the SAME set.
//!   3. The bounded result is a STRICT SUBSET of the unbounded result (proving the
//!      bound is actually enforced — pre-fix, `scan_stream` returned every row).
//!
//! Bounds are exercised at the `SSTableManager` level directly because `SELECT`
//! does not expose a raw key-range surface. The inclusive `[start, end]` semantics
//! mirror the per-reader scan (`skip key < start`, `skip key > end`, using
//! `RowKey`'s `Ord`).
//!
//! NOTE: excluded under `tombstones`. That feature switches `scan_stream` to
//! delegate to `scan`, masking the divergence (same gating lesson as #957/#958).
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_957_streaming_range_bound_parity

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use std::collections::BTreeMap;
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
use std::collections::HashMap;
use tempfile::TempDir;

const KS: &str = "range_ks";
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
/// emits as the `RowKey` (big-endian int — see issue_883 test). Small positive
/// ids keep byte order == numeric order, so `[start, end]` is intuitive.
fn pk_bytes(id: i32) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn row_key(id: i32) -> RowKey {
    RowKey::new(pk_bytes(id))
}

/// Drain a `scan_stream` receiver into a sorted (by key bytes) Vec.
async fn drain_stream(
    mut rx: tokio::sync::mpsc::Receiver<cqlite_core::Result<(RowKey, cqlite_core::ScanRow)>>,
) -> Vec<(Vec<u8>, cqlite_core::ScanRow)> {
    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        let (k, v) = item.expect("streamed row should be Ok");
        out.push((k.as_bytes().to_vec(), v));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_multi_generation_scan_stream_enforces_range_like_scan() {
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
    // multi-generation merge branch is taken in BOTH scan and scan_stream.
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
    let buffer_size = 4usize;

    // ── Unbounded baseline: every live row across all 3 generations ────────────
    let unbounded_scan = manager
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .expect("unbounded scan must succeed");
    let unbounded_keys: Vec<i32> = {
        let mut ids: Vec<i32> = unbounded_scan
            .iter()
            .map(|(k, _)| i32::from_be_bytes(k.as_bytes().try_into().expect("4-byte int pk")))
            .collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        unbounded_keys,
        (1..=9).collect::<Vec<_>>(),
        "unbounded scan must return all 9 partitions across generations"
    );

    // ── Bounded range [3, 6] inclusive — straddles all three generations ───────
    let start = row_key(3);
    let end = row_key(6);
    let expected_in_range: Vec<i32> = vec![3, 4, 5, 6];

    let bounded_scan = manager
        .scan(&table_id, Some(&start), Some(&end), None, Some(&schema))
        .await
        .expect("bounded scan must succeed");
    let bounded_scan_ids: Vec<i32> = {
        let mut ids: Vec<i32> = bounded_scan
            .iter()
            .map(|(k, _)| i32::from_be_bytes(k.as_bytes().try_into().expect("4-byte int pk")))
            .collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        bounded_scan_ids, expected_in_range,
        "bounded materializing scan must return ONLY the in-range partitions [3..=6]"
    );

    // Stream the SAME bound and compare. PRE-FIX this returned all 9 partitions
    // (the multi-gen branch ignored start/end and forwarded the full reconciled
    // table), so this assertion fails before the fix and passes after.
    let stream = manager
        .scan_stream(
            &table_id,
            Some(&start),
            Some(&end),
            Some(&schema),
            buffer_size,
        )
        .await
        .expect("bounded scan_stream must succeed");
    let bounded_stream = drain_stream(stream).await;
    let bounded_stream_ids: Vec<i32> = bounded_stream
        .iter()
        .map(|(k, _)| i32::from_be_bytes(k.clone().try_into().expect("4-byte int pk")))
        .collect();
    assert_eq!(
        bounded_stream_ids, expected_in_range,
        "Issue #957: bounded scan_stream must enforce [3..=6] like scan — pre-fix it \
         returned the FULL reconciled table {:?}",
        unbounded_keys
    );

    // scan and scan_stream must agree value-for-value, not just on key set.
    let scan_map: BTreeMap<Vec<u8>, cqlite_core::ScanRow> = bounded_scan
        .into_iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v))
        .collect();
    let stream_map: BTreeMap<Vec<u8>, cqlite_core::ScanRow> = bounded_stream.into_iter().collect();
    assert_eq!(
        scan_map, stream_map,
        "Issue #957: bounded scan and scan_stream must agree value-for-value"
    );

    // The bounded result is a STRICT subset of the unbounded result: same bound,
    // fewer rows, every bounded key present unbounded. This is the proof the bound
    // is actually doing work (pre-fix the two were equal).
    assert!(
        bounded_stream_ids.len() < unbounded_keys.len(),
        "bounded result ({} rows) must be strictly smaller than unbounded ({} rows)",
        bounded_stream_ids.len(),
        unbounded_keys.len()
    );
    for id in &bounded_stream_ids {
        assert!(
            unbounded_keys.contains(id),
            "bounded id {id} must also appear in the unbounded result"
        );
    }

    // ── Half-open bounds also honoured: only start, only end ───────────────────
    // start-only [6, ∞): ids 6,7,8,9.
    let start_only = manager
        .scan_stream(
            &table_id,
            Some(&row_key(6)),
            None,
            Some(&schema),
            buffer_size,
        )
        .await
        .expect("start-only scan_stream");
    let start_only_ids: Vec<i32> = drain_stream(start_only)
        .await
        .iter()
        .map(|(k, _)| i32::from_be_bytes(k.clone().try_into().expect("4-byte int pk")))
        .collect();
    assert_eq!(
        start_only_ids,
        vec![6, 7, 8, 9],
        "start-only bound must keep ids >= start (inclusive)"
    );

    // end-only (∞, 3]: ids 1,2,3.
    let end_only = manager
        .scan_stream(
            &table_id,
            None,
            Some(&row_key(3)),
            Some(&schema),
            buffer_size,
        )
        .await
        .expect("end-only scan_stream");
    let end_only_ids: Vec<i32> = drain_stream(end_only)
        .await
        .iter()
        .map(|(k, _)| i32::from_be_bytes(k.clone().try_into().expect("4-byte int pk")))
        .collect();
    assert_eq!(
        end_only_ids,
        vec![1, 2, 3],
        "end-only bound must keep ids <= end (inclusive)"
    );

    drop(temp_dir);
}
