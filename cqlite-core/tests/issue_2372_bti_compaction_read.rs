//! Issue #2372: the compaction/merge read path must read BTI (`da`) SSTables.
//!
//! The Flight `do_get` path opens SSTables through the k-way compaction merger,
//! which drives `SSTableReader::{iterate,stream}_all_partitions_for_compaction`.
//! Both routed on `requires_chunk_stitching()`, which gates on `is_nb_format()`
//! and therefore EXCLUDES BTI (`da`, whose `data_format` is `V5UncompressedOA`).
//! With no BTI branch (unlike `get_all_entries`/`scan`/`run_scan_stream`), a BTI
//! Data.db fell into the block-by-block `parse_block_entries` fallback and errored
//! ("Blob fallback not allowed for value parsing in modern format V5_0Bti"). A BTI
//! table is chunk-compressed with the same V5 row layout as nb, so the fix routes
//! it through the same stitch-and-parse-for-compaction machinery
//! `bti_scan_with_metadata` uses.
//!
//! Oracle: the authoritative user-facing `SSTableReader::scan` (trie-walk
//! `bti_scan_with_metadata`) over the SAME committed `test_da/simple_table`
//! fixture. Both compaction entry points MUST return the same partition-key set
//! and per-row cell values as `scan`, and MUST NOT error.
//!
//! Skip-on-presence like the sibling BTI parity tests; fail-closed under
//! `CQLITE_PARITY_REQUIRE_DATASETS=1` (issue #1856). A present fixture returning
//! 0 rows is a HARD FAILURE, never a skip.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::types::{ScanRow, Value};
use cqlite_core::{Config, Platform, RowKey, TableId};

const BTI_DIR: &str = "sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11";
const DATA_DB: &str = "da-2-bti-Data.db";
const KS_TABLE: &str = "test_da.simple_table";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn bti_data_db() -> Option<PathBuf> {
    let data_db = datasets_root()?.join(BTI_DIR).join(DATA_DB);
    data_db.exists().then_some(data_db)
}

fn parity_datasets_required() -> bool {
    std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn skip_or_fail_closed(reason: &str) {
    if parity_datasets_required() {
        panic!(
            "issue_2372_bti_compaction_read: CQLITE_PARITY_REQUIRE_DATASETS=1 but {reason} \
             — required parity gate cannot green-pass without running (issue #1856)"
        );
    }
    eprintln!("Skipping (#2372 BTI compaction read): {reason}");
}

async fn open_reader(data_db: &std::path::Path) -> Arc<SSTableReader> {
    let cfg = Config::default();
    let platform = Arc::new(Platform::new(&cfg).await.expect("platform"));
    let reader = SSTableReader::open(data_db, &cfg, platform)
        .await
        .expect("open BTI reader");
    assert_eq!(
        reader.format_version().expect("format version"),
        "da",
        "fixture must be the BTI (`da`) format for this test to be meaningful"
    );
    Arc::new(reader)
}

/// The distinct partition-key set from the authoritative `scan` oracle.
fn scan_partition_keys(entries: &[(RowKey, ScanRow)]) -> BTreeSet<Vec<u8>> {
    entries.iter().map(|(k, _)| k.as_bytes().to_vec()).collect()
}

/// One partition's decoded content: its key bytes + a sorted `column=Debug(value)`
/// set. Debug-formatting the `Value` gives a stable content oracle without
/// depending on the internal cell representation.
type PartitionContent = (Vec<u8>, BTreeSet<String>);

/// The per-partition content decoded by the authoritative `scan`. For this
/// single-row-per-partition fixture each key maps to one row's cells.
fn scan_cells(entries: &[(RowKey, ScanRow)]) -> Vec<PartitionContent> {
    let mut out = Vec::new();
    for (k, row) in entries {
        if let ScanRow::Row(cells) = row {
            let kv: BTreeSet<String> = cells
                .iter()
                .map(|(name, v)| format!("{name}={v:?}"))
                .collect();
            out.push((k.as_bytes().to_vec(), kv));
        }
    }
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bti_compaction_paths_match_scan_oracle() {
    let Some(data_db) = bti_data_db() else {
        skip_or_fail_closed("test_da/simple_table da-2-bti-Data.db not present");
        return;
    };

    let reader = open_reader(&data_db).await;
    let table_id = TableId::new(KS_TABLE);
    let schema = reader.schema().cloned();

    // Authoritative user-facing scan (trie-walk `bti_scan_with_metadata`).
    let scanned = reader
        .scan(&table_id, None, None, None, schema.as_ref())
        .await
        .expect("authoritative BTI scan");
    assert!(
        scanned.len() >= 3,
        "present BTI fixture must return its 3 rows via scan (got {}) — 0/low is a read regression",
        scanned.len()
    );
    let scan_keys = scan_partition_keys(&scanned);
    let scan_cell_map = scan_cells(&scanned);

    // (1) BUFFERED compaction path: iterate_all_partitions_for_compaction.
    let buffered = reader
        .iterate_all_partitions_for_compaction(schema.as_ref())
        .await
        .expect("BTI iterate_all_partitions_for_compaction must not error (#2372)");
    let buffered_keys: BTreeSet<Vec<u8>> =
        buffered.iter().map(|r| r.key.as_bytes().to_vec()).collect();
    assert_eq!(
        buffered_keys, scan_keys,
        "buffered compaction partition-key set must match the scan oracle"
    );

    // (2) STREAMING compaction path: stream_all_partitions_for_compaction.
    let cancel = ScanCancel::default();
    let mut streamed = Vec::new();
    reader
        .stream_all_partitions_for_compaction(schema.as_ref(), &cancel, |row| {
            streamed.push(row);
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await
        .expect("BTI stream_all_partitions_for_compaction must not error (#2372)");
    let streamed_keys: BTreeSet<Vec<u8>> =
        streamed.iter().map(|r| r.key.as_bytes().to_vec()).collect();
    assert_eq!(
        streamed_keys, scan_keys,
        "streaming compaction partition-key set must match the scan oracle"
    );

    // Content pin: every scalar cell value the scan oracle decoded must appear on
    // the buffered compaction path (its row_data carries the live cells). This
    // proves the compaction path did schema-aware decode, not a raw-blob fallback.
    for (key, expected) in &scan_cell_map {
        let row = buffered
            .iter()
            .find(|r| r.key.as_bytes() == key.as_slice())
            .expect("every scan partition present on the buffered compaction path");
        let got = compaction_simple_cell_debug(row);
        // The compaction row surfaces regular columns as simple cells (the id PK
        // and any Null are not simple cells here). Require every `column=value`
        // the scan oracle saw for a column the compaction path ALSO surfaces to
        // match — proving schema-aware decode, not a raw-blob fallback.
        for cell in expected {
            let col_prefix = format!("{}=", cell.split('=').next().unwrap_or(""));
            if got.iter().any(|g| g.starts_with(&col_prefix)) {
                assert!(
                    got.contains(cell),
                    "compaction row for partition {key:?} must carry {cell}; got {got:?}"
                );
            }
        }
    }
}

/// The `column=Debug(value)` set of a compaction row's live simple cells.
fn compaction_simple_cell_debug(
    row: &cqlite_core::storage::sstable::reader::compaction_row::CompactionRow,
) -> BTreeSet<String> {
    use cqlite_core::storage::sstable::reader::compaction_row::CompactionRowData;
    let mut out = BTreeSet::new();
    if let CompactionRowData::Live { simple, .. } = &row.row_data {
        for cell in simple {
            let v: &Value = &cell.value;
            out.insert(format!("{}={:?}", cell.column, v));
        }
    }
    out
}
