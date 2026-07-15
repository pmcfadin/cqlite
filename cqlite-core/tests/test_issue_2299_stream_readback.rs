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
use cqlite_core::storage::write_engine::merge;
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
    let mut direct_stream_partitions = 0u64;
    for _ in 0..8 {
        let report = engine
            .maintenance_step(budget)
            .expect("maintenance_step must not error");
        direct_stream_partitions += report.direct_stream_partitions;
        if !report.completed_merges.is_empty() {
            compaction_completed = true;
            break;
        }
        if !report.pending_compaction {
            break;
        }
    }
    assert!(compaction_completed, "compaction must complete");
    // Positively assert the memory-bounding DIRECT-STREAM path actually ran for
    // both wide partitions (roborev job 1723): byte-identity + the Data.db-size
    // block-crossing check below both hold on the buffered path too, so without
    // this a silent regression to buffering (e.g. a future change that stops
    // setting `stream_rows_directly`) would leave these tests green while the new
    // behavior stopped executing. Only the dhat-gated memory test — NOT in the
    // default gate — would otherwise catch it.
    assert_eq!(
        direct_stream_partitions,
        PARTITIONS.len() as u64,
        "both wide tombstone-free partitions must compact via the issue #2299 \
         direct-stream path, not the buffered fall-through"
    );
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

// ---------------------------------------------------------------------------
// Roborev (issue #2299 endgame): the direct-stream static-row prelude
// (`WriteEngine::maintenance_step`, the `if schema_has_static { ... }` block
// that opens a streaming partition session) was UNTESTED — all three existing
// tests in this file use zero-static schemas. The two tests below lock the
// on-disk header -> static -> rows byte order for (a) a partition whose
// direct-stream session opens on its first clustering row (the static prelude
// is fed on that open), and (b) a static-carrier partition with NO clustering
// row at all, which the direct-stream loop never opens a session for and must
// fall through to the buffered path (`maintenance.rs`, the `state.direct_
// session.take()` comment right above the fall-through).
// ---------------------------------------------------------------------------

const TABLE_STATIC: &str = "wide_partition_with_static";
const TABLE_STATIC_ONLY: &str = "static_only_partition";

/// `id int, ck int, region text static, payload text` — same shape as
/// [`make_schema`] plus one static column.
fn schema_with_static(table: &str) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: table.to_string(),
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
                name: "region".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
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

fn write_clustering_row(table: &str, id: i32, ck: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, table);
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ck_key = ClusteringKey::single("ck", Value::Integer(ck));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Text(payload_for(ck)),
    }];
    Mutation::new(table_id, pk, Some(ck_key), ops, timestamp, None)
}

/// A pure static write: `clustering_key: None`, only the `region` column.
fn write_static_region(table: &str, id: i32, region: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, table);
    let pk = PartitionKey::single("id", Value::Integer(id));
    Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "region".to_string(),
            value: Value::Text(region.to_string()),
        }],
        timestamp,
        None,
    )
}

/// One decoded scan row: the partition `id`, its clustering key (`None` for a
/// partition static row), and every named cell (schema-column-name -> value)
/// the reader surfaced for it.
struct DecodedRow2299 {
    id: i32,
    ck: Option<i32>,
    cells: std::collections::HashMap<String, Value>,
}

/// Drive `KWayMerger` directly over the SINGLE compacted output `Data.db` and
/// decode every partition's rows, INCLUDING its partition static row
/// (`ck.is_none()`). This bypasses `SSTableManager::scan`'s query-level
/// reconciliation entirely, so it asserts the PHYSICAL on-disk header ->
/// static -> rows byte order the direct-stream / buffered-fall-through write
/// paths produced — the same convention
/// `issue_1074_static_write_parity.rs`'s `collect_merge_rows` uses.
fn decode_output_rows(data_path: &Path, schema: &TableSchema) -> Vec<DecodedRow2299> {
    let mut merger = merge::KWayMerger::new(vec![data_path.to_path_buf()], schema).expect("merger");
    let mut out = Vec::new();
    loop {
        match merger.step().expect("merge step") {
            merge::MergeStep::Complete => break,
            merge::MergeStep::Partition { key, rows } => {
                let key_bytes = &key.key;
                let id = if key_bytes.len() == 4 {
                    i32::from_be_bytes([key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3]])
                } else {
                    continue;
                };
                for row in rows {
                    let ck = row.clustering_key.as_ref().and_then(|c| {
                        c.columns.first().and_then(|(_, v)| match v {
                            Value::Integer(i) => Some(*i),
                            _ => None,
                        })
                    });
                    if let merge::RowData::Live { cells } = &row.row_data {
                        let mut map = std::collections::HashMap::new();
                        for c in cells {
                            if !matches!(c.value, Value::Tombstone(_)) {
                                map.insert(c.column.clone(), c.value.clone());
                            }
                        }
                        out.push(DecodedRow2299 { id, ck, cells: map });
                    }
                }
            }
        }
    }
    out
}

fn text_cell(row: &DecodedRow2299, column: &str) -> Option<String> {
    match row.cells.get(column) {
        Some(Value::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

/// The single surviving `-big-Data.db` file in `dir` (post-compaction).
fn find_single_data_file(dir: &Path) -> std::path::PathBuf {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .expect("exactly one compacted Data.db")
        .path()
}

/// (a) Direct-stream path WITH a static column: the session opens on the
/// first clustering row per partition, feeding the static prelude at that
/// point (`maintenance.rs`'s `if schema_has_static { ... }` block). Proves
/// the static value AND every clustering row survive the direct-stream
/// compaction byte-for-byte.
#[test]
fn direct_stream_static_column_reads_back_byte_identical() {
    const PARTITIONS: [(i32, &str); 2] = [(1, "region-east"), (2, "region-west")];
    const SSTABLE_COUNT_STATIC: i32 = 3;
    const ROWS_PER_SSTABLE_STATIC: i32 = 5;
    const TOTAL_ROWS_STATIC: i32 = SSTABLE_COUNT_STATIC * ROWS_PER_SSTABLE_STATIC;

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = schema_with_static(TABLE_STATIC);

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // The static write for each partition is folded into FILE 0's flush
    // (alongside its first batch of clustering rows) rather than its own
    // separate flush — a standalone static-only file would be MUCH smaller
    // than the row-bearing files and STCS's size-bucketing (bucket_low/high =
    // 0.5/1.5) would then never group it with the other 3, leaving the merge
    // permanently pending. It is still the ONLY write touching `region`, so
    // no conflict resolution is needed.
    let mut expected_payload: BTreeMap<(i32, i32), String> = BTreeMap::new();
    for file_idx in 0..SSTABLE_COUNT_STATIC {
        let base = file_idx * ROWS_PER_SSTABLE_STATIC;
        if file_idx == 0 {
            for (id, region) in PARTITIONS {
                engine
                    .write(write_static_region(TABLE_STATIC, id, region, 1))
                    .expect("write static region");
            }
        }
        for (id, _region) in PARTITIONS {
            for ck in base..base + ROWS_PER_SSTABLE_STATIC {
                engine
                    .write(write_clustering_row(
                        TABLE_STATIC,
                        id,
                        ck,
                        100 + i64::from(ck),
                    ))
                    .expect("write row");
                expected_payload.insert((id, ck), payload_for(ck));
            }
        }
        rt.block_on(engine.flush())
            .expect("flush sstable")
            .expect("non-empty sstable");
    }

    let sstable_dir = data_dir.join(KEYSPACE).join(TABLE_STATIC);
    assert_eq!(
        count_data_files(&sstable_dir),
        SSTABLE_COUNT_STATIC as usize
    );

    engine
        .set_merge_policy(Box::new(
            STCSPolicy::new(SSTABLE_COUNT_STATIC as usize, 32, 0.5, 1.5, 0)
                .expect("valid STCS parameters"),
        ))
        .expect("set merge policy");

    let budget = Duration::from_secs(60);
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

    rt.block_on(engine.close()).expect("close engine");

    let output_path = find_single_data_file(&sstable_dir);
    let rows = decode_output_rows(&output_path, &schema);

    // Every clustering row must read back with its expected payload.
    let mut actual_payload: BTreeMap<(i32, i32), String> = BTreeMap::new();
    for row in &rows {
        if let Some(ck) = row.ck {
            if let Some(payload) = text_cell(row, "payload") {
                actual_payload.insert((row.id, ck), payload);
            }
        }
    }
    assert_eq!(
        actual_payload.len(),
        PARTITIONS.len() * TOTAL_ROWS_STATIC as usize,
        "read-back must return exactly {} clustering rows, got {}",
        PARTITIONS.len() * TOTAL_ROWS_STATIC as usize,
        actual_payload.len()
    );
    assert_eq!(
        actual_payload, expected_payload,
        "every (partition id, clustering key) + payload must read back byte-identical \
         to what was written through the direct-stream static-column compaction path"
    );

    // Each partition's static `region` value must be readable SOMEWHERE (a
    // dedicated static row with `ck.is_none()`, or folded into every
    // clustering row — either shape proves the writer's header -> static ->
    // rows byte order decoded correctly; a wrong order would corrupt row
    // framing and either error or surface no/garbage region values).
    for (id, expected_region) in PARTITIONS {
        let seen_regions: std::collections::HashSet<String> = rows
            .iter()
            .filter(|r| r.id == id)
            .filter_map(|r| text_cell(r, "region"))
            .collect();
        assert_eq!(
            seen_regions,
            std::collections::HashSet::from([expected_region.to_string()]),
            "partition id={id} must surface its static `region`={expected_region} value \
             (and ONLY that value) after the direct-stream compaction, got {seen_regions:?}"
        );
    }

    drop(temp_dir);
}

/// (b) A partition whose ONLY mutation is a static write (no clustering row
/// at all) alongside partitions that DO have clustering rows in the SAME
/// merge. The direct-stream loop opens its session lazily on the first
/// `Some(ck)` row, so this partition's `direct_session` stays `None` for its
/// whole lifetime — it must fall through to the buffered path
/// (`maintenance.rs`'s `state.direct_session.take()` comment) instead of
/// being silently dropped.
#[test]
fn direct_stream_static_only_partition_falls_through_to_buffered_path() {
    const STATIC_ONLY_ID: i32 = 7;
    const CLUSTERED_ID: i32 = 1;
    const CLUSTERED_ROWS: i32 = 4;
    const STATIC_ONLY_REGION: &str = "static-only-region";

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = schema_with_static(TABLE_STATIC_ONLY);

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // File 1: the static-only partition (no clustering row, ever) plus a
    // clustering partition sharing the SAME merge batch, so the overall
    // `stream_rows_directly` gate (tombstone-free across every input) is
    // still true and both partitions go through the SAME merge.
    engine
        .write(write_static_region(
            TABLE_STATIC_ONLY,
            STATIC_ONLY_ID,
            STATIC_ONLY_REGION,
            1,
        ))
        .expect("write static-only region");
    for ck in 0..CLUSTERED_ROWS {
        engine
            .write(write_clustering_row(
                TABLE_STATIC_ONLY,
                CLUSTERED_ID,
                ck,
                100 + i64::from(ck),
            ))
            .expect("write clustering row (file 1)");
    }
    rt.block_on(engine.flush())
        .expect("flush file 1")
        .expect("non-empty sstable");

    // File 2: more of the clustering partition's rows (disjoint ck range),
    // forcing a real N-input merge.
    for ck in CLUSTERED_ROWS..2 * CLUSTERED_ROWS {
        engine
            .write(write_clustering_row(
                TABLE_STATIC_ONLY,
                CLUSTERED_ID,
                ck,
                100 + i64::from(ck),
            ))
            .expect("write clustering row (file 2)");
    }
    rt.block_on(engine.flush())
        .expect("flush file 2")
        .expect("non-empty sstable");

    let sstable_dir = data_dir.join(KEYSPACE).join(TABLE_STATIC_ONLY);
    assert_eq!(count_data_files(&sstable_dir), 2);

    engine
        .set_merge_policy(Box::new(
            STCSPolicy::new(2, 32, 0.5, 1.5, 0).expect("valid STCS parameters"),
        ))
        .expect("set merge policy");

    let budget = Duration::from_secs(60);
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
    assert_eq!(count_data_files(&sstable_dir), 1);

    rt.block_on(engine.close()).expect("close engine");

    let output_path = find_single_data_file(&sstable_dir);
    let rows = decode_output_rows(&output_path, &schema);

    // The static-only partition must have survived (issue #933/#1072 shape):
    // its `region` value is readable, proving the buffered fall-through fed
    // the static prelude even though no `direct_session` was ever opened for
    // it.
    let static_only_regions: std::collections::HashSet<String> = rows
        .iter()
        .filter(|r| r.id == STATIC_ONLY_ID)
        .filter_map(|r| text_cell(r, "region"))
        .collect();
    assert_eq!(
        static_only_regions,
        std::collections::HashSet::from([STATIC_ONLY_REGION.to_string()]),
        "the static-only partition (id={STATIC_ONLY_ID}) must survive the direct-stream \
         compaction's buffered fall-through with its region value intact, got {static_only_regions:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|r| r.id == STATIC_ONLY_ID && r.ck.is_some()),
        "the static-only partition must never surface a clustering row (it wrote none)"
    );

    // The clustering partition (which DID take the direct-stream path) must
    // still read back completely and correctly in the SAME merge.
    let mut actual_payload: BTreeMap<i32, String> = BTreeMap::new();
    for row in rows.iter().filter(|r| r.id == CLUSTERED_ID) {
        if let Some(ck) = row.ck {
            if let Some(payload) = text_cell(row, "payload") {
                actual_payload.insert(ck, payload);
            }
        }
    }
    let expected_payload: BTreeMap<i32, String> = (0..2 * CLUSTERED_ROWS)
        .map(|ck| (ck, payload_for(ck)))
        .collect();
    assert_eq!(
        actual_payload, expected_payload,
        "the clustering partition sharing this merge batch must read back unaffected \
         by the static-only partition's buffered fall-through"
    );

    drop(temp_dir);
}
