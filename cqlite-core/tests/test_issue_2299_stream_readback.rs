//! End-to-end read-back parity for issue #2299's genuinely-NEW on-disk behavior:
//! the mid-partition scratch flush + buffer clear on a WIDE, tombstone-free
//! partition that crosses ≥1 promoted-index block (>64 KiB
//! `COLUMN_INDEX_SIZE_BYTES`) via the DIRECT row-stream compaction path in
//! `WriteEngine::maintenance_step`.
//!
//! This runs in the DEFAULT gate (feature `write-support`, NO `dhat-heap` gate).
//! It complements the heap-budget-only `test_issue_2299_uncompressed_stream_memory`
//! (which asserts peak heap + `count_data_files == 1` but never reads the rows
//! back) by proving VALUE correctness: after the real N→1 STCS compaction of the
//! wide partition, EVERY clustering row's `ck` + `payload` reads back byte-for-byte
//! equal to what was written.
//!
//! ## Why this exercises the direct-stream mid-partition flush
//!
//! - **Direct-stream path (tombstone-free gate = true):** the fixture writes ONLY
//!   `Write` cell operations — zero deletes / TTLs / range tombstones — so every
//!   input's `Statistics.db` min-LDT stays at Cassandra's `NO_DELETION_TIME`
//!   sentinel and `stream_rows_directly` is set (see
//!   `write_engine::compaction::no_deletions_in_any_input`). That is the path whose
//!   scratch flush is the new behavior.
//! - **Crosses ≥1 promoted-index block + forces a mid-partition flush:** the wide
//!   partition (id = 1) holds `TOTAL_ROWS` rows of `PAYLOAD_BYTES` each, so its
//!   encoded body is `TOTAL_ROWS * PAYLOAD_BYTES` ≈ 800 KiB — an order of magnitude
//!   past the 64 KiB `COLUMN_INDEX_SIZE_BYTES` block size, so the writer completes
//!   ~12 promoted-index blocks mid-partition and flushes the scratch at each.
//! - **Real N→1 STCS compaction via the direct-stream path:** the rows are split
//!   across `SSTABLE_COUNT` input SSTables by DISJOINT clustering-key ranges of the
//!   SAME partition, so a real k-way merge reconciles them into one wide output
//!   partition through `WriteEngine::maintenance_step`.

#![cfg(feature = "write-support")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, STCSPolicy, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use cqlite_core::ScanRow;
use tempfile::TempDir;

const KEYSPACE: &str = "issue2299_readback_ks";
const TABLE: &str = "wide_partition";

// 4 files × 80 rows × 2 KiB payload ≈ 640 KiB of row content in ONE partition
// (id = 1), split across the 4 files by disjoint clustering-key ranges so a real
// k-way merge reconciles them into one wide output partition. At 2 KiB/row the
// partition body is ~10× the 64 KiB `COLUMN_INDEX_SIZE_BYTES` block size, forcing
// ~10 promoted-index block boundaries — hence multiple mid-partition scratch
// flushes on the direct-stream compaction path. Zero tombstones/deletes keeps the
// tombstone-free gate (`stream_rows_directly`) true. Small enough for the normal
// gate budget (no dhat overhead).
const PAYLOAD_BYTES: usize = 2 * 1024;
const ROWS_PER_SSTABLE: i32 = 80;
const SSTABLE_COUNT: i32 = 4;
const TOTAL_ROWS: i32 = ROWS_PER_SSTABLE * SSTABLE_COUNT;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
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
                name: "payload".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// Deterministic per-row payload so read-back can assert full value equality.
fn payload_for(ck: i32) -> String {
    let mut s = format!("row-{ck:08}-");
    s.push_str(&"abcdefghij".repeat((PAYLOAD_BYTES.saturating_sub(s.len())) / 10 + 1));
    s.truncate(PAYLOAD_BYTES);
    s
}

fn write_row(id: i32, ck: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, TABLE);
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ck_key = ClusteringKey::single("ck", Value::Integer(ck));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Text(payload_for(ck)),
    }];
    Mutation::new(table_id, pk, Some(ck_key), ops, timestamp, None)
}

fn make_policy() -> STCSPolicy {
    STCSPolicy::new(SSTABLE_COUNT as usize, 32, 0.5, 1.5, 0).expect("valid STCS parameters")
}

fn count_data_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
                .count()
        })
        .unwrap_or(0)
}

/// Read every surviving clustering row back through `SSTableManager::scan` and
/// return an `(id, ck) -> payload` map. Each wide-partition clustering row surfaces
/// as one `ScanRow::Row` whose cells carry the partition `id`, clustering `ck`, and
/// the `payload` column.
fn read_back_id_ck_payload(data_dir: &Path, schema: &TableSchema) -> BTreeMap<(i32, i32), String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let manager = SSTableManager::new(
            data_dir,
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager opens the compacted output");

        let table_id = CqlTableId::from(format!("{KEYSPACE}.{TABLE}").as_str());
        let results = manager
            .scan(&table_id, None, None, None, Some(schema))
            .await
            .expect("post-compaction scan must not error");

        let mut map = BTreeMap::new();
        for (key, row) in results {
            // The partition key `id` (int) is carried by the RowKey (4-byte
            // big-endian), NOT as a row cell — only clustering + regular columns
            // surface as cells.
            let key_bytes = key.as_bytes();
            let id = if key_bytes.len() == 4 {
                i32::from_be_bytes([key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3]])
            } else {
                continue;
            };
            if let ScanRow::Row(cells) = row {
                let mut ck: Option<i32> = None;
                let mut payload: Option<String> = None;
                for (name, value) in &cells {
                    match name.as_ref() {
                        "ck" => {
                            if let Value::Integer(i) = value {
                                ck = Some(*i);
                            }
                        }
                        "payload" => {
                            if let Value::Text(t) = value {
                                payload = Some(t.clone());
                            }
                        }
                        _ => {}
                    }
                }
                if let (Some(ck), Some(payload)) = (ck, payload) {
                    map.insert((id, ck), payload);
                }
            }
        }
        map
    })
}

/// The genuinely-NEW on-disk behavior — mid-partition scratch flush on a WIDE,
/// tombstone-free partition crossing multiple promoted-index blocks via the
/// direct-stream compaction path — read back and asserted for full VALUE equality
/// (row count + every clustering key + every payload), not just count/heap.
///
/// Uses TWO wide partitions (id = 1, id = 2) so the streaming compaction path also
/// crosses a real partition boundary end-to-end (PartitionDone/reset between them),
/// exercising the partition-boundary handling alongside the mid-partition flush.
#[test]
fn direct_stream_wide_partition_reads_back_byte_identical() {
    const PARTITIONS: [i32; 2] = [1, 2];

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // Phase 1: write SSTABLE_COUNT files, each holding a DISJOINT clustering-key
    // range of BOTH wide partitions (id = 1 and id = 2). Track the exact written
    // values keyed by (id, ck).
    let mut expected: BTreeMap<(i32, i32), String> = BTreeMap::new();
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    for file_idx in 0..SSTABLE_COUNT {
        let base = file_idx * ROWS_PER_SSTABLE;
        for id in PARTITIONS {
            for ck in base..base + ROWS_PER_SSTABLE {
                engine
                    .write(write_row(id, ck, 100 + i64::from(ck)))
                    .expect("write row");
                expected.insert((id, ck), payload_for(ck));
            }
        }
        rt.block_on(engine.flush())
            .expect("flush sstable")
            .expect("non-empty sstable");
    }
    assert_eq!(expected.len(), PARTITIONS.len() * TOTAL_ROWS as usize);

    let sstable_dir = data_dir.join(KEYSPACE).join(TABLE);
    assert_eq!(count_data_files(&sstable_dir), SSTABLE_COUNT as usize);

    engine
        .set_merge_policy(Box::new(make_policy()))
        .expect("set merge policy");

    // Phase 2: run the real N→1 STCS compaction via the direct-stream path.
    let budget = Duration::from_secs(120);
    let mut compaction_completed = false;
    for _ in 0..8 {
        let report = engine
            .maintenance_step(budget)
            .expect("maintenance_step must not error");
        if !report.completed_merges.is_empty() {
            compaction_completed = true;
            break;
        }
        if !report.pending_compaction {
            break;
        }
    }
    assert!(compaction_completed, "compaction must complete");
    assert_eq!(
        count_data_files(&sstable_dir),
        1,
        "N input SSTables must compact to exactly 1 output"
    );

    // The compacted single Data.db body must be well past the 64 KiB
    // `COLUMN_INDEX_SIZE_BYTES` block size — this is the fixture invariant that
    // guarantees the wide partition crossed multiple promoted-index blocks and thus
    // triggered mid-partition scratch flushes on the direct-stream write path. If a
    // future change shrank the payload below the block size, this catches it before
    // the read-back assertion could false-pass on a no-flush partition.
    let data_len = std::fs::read_dir(&sstable_dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .and_then(|e| e.metadata().ok())
        .map(|m| m.len())
        .expect("compacted Data.db present");
    assert!(
        data_len > 4 * 64 * 1024,
        "compacted Data.db ({data_len} bytes) must exceed several 64 KiB promoted-index \
         blocks so the wide partition triggered a mid-partition scratch flush; if this \
         fails the fixture no longer exercises issue #2299's new behavior"
    );

    rt.block_on(engine.close()).expect("close engine");

    // Phase 3: read the compacted output back and assert FULL value equality.
    // `SSTableManager::new` discovers `<keyspace>/<table>` subdirs, so open on the
    // data-dir ROOT (not the table subdir).
    let actual = read_back_id_ck_payload(&data_dir, &schema);
    assert_eq!(
        actual.len(),
        PARTITIONS.len() * TOTAL_ROWS as usize,
        "read-back must return exactly {} rows, got {}",
        PARTITIONS.len() * TOTAL_ROWS as usize,
        actual.len()
    );
    assert_eq!(
        actual, expected,
        "every (partition id, clustering key) + payload must read back byte-identical \
         to what was written through the direct-stream mid-partition-flush compaction path"
    );

    drop(temp_dir);
}
