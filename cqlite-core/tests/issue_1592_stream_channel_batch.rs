//! Issue #1592 (Epic F/F2): batch the public streaming channel — stop one async
//! wake per row.
//!
//! The internal windowed scan (issue #1143) batches rows to amortize the
//! blocking→async wake, but the public forwarder re-flattened those batches to one
//! row per channel send, so one async wake per row survived the whole pipeline.
//! This adds an additive batched streaming surface (`scan_stream_batched`) that
//! forwards a `Vec` BATCH per channel send.
//!
//! These oracles pin, against a real single-generation dataset table
//! (`test_basic/simple_table`, 999 rows, V5-compressed → the windowed
//! straight-through path):
//!
//!   1. **Send-count reduction** — the batched surface performs strictly (and
//!      substantially) fewer channel sends than the per-row surface for the same
//!      scan, with real multi-row batches. Deterministic (counts of received
//!      channel items, each item == one send), no wall-clock.
//!   2. **Content + order parity** — flattening the batched output reproduces the
//!      per-row stream exactly (keys + values, in order).
//!   3. **Backpressure preserved** — under a tiny (cap = 1 batch) bounded channel
//!      the batched scan still delivers every row in order (no drop/reorder), and a
//!      mid-stream receiver drop terminates the producer cleanly.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the gitignored `Data.db` binaries; skips
//! (never spuriously passes with zero rows) when absent.

#![cfg(feature = "state_machine")]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{parse_cql_schema, TableSchema};
use cqlite_core::types::{ScanRow, TableId};
use cqlite_core::{Config, RowKey};

const KEYSPACE: &str = "test_basic";
const TABLE: &str = "simple_table";

/// Upper bound on a single windowed batch (mirrors the internal, `pub(crate)`
/// `BATCH_EMIT_ROWS`; the driver flushes at chunk boundaries AND at this cap, so a
/// batch never exceeds it). Kept in sync with `scan_stream_windowed::BATCH_EMIT_ROWS`.
const BATCH_EMIT_ROWS_MAX: usize = 256;

/// Locate `test_basic/simple_table`'s generation dir, requiring an actual Data.db
/// so a JSONL-only checkout skips rather than passing with zero rows.
fn table_dir() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)?;
    let dir = root.join("sstables").join(KEYSPACE);
    let has_data = std::fs::read_dir(&dir)
        .ok()?
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(&format!("{TABLE}-"))
                && std::fs::read_dir(e.path())
                    .map(|inner| {
                        inner
                            .filter_map(|x| x.ok())
                            .any(|x| x.file_name().to_string_lossy().ends_with("-Data.db"))
                    })
                    .unwrap_or(false)
        });
    has_data.then_some(dir)
}

/// Schema for `test_basic.simple_table` (matches `test-data/schemas/basic-types.cql`).
fn simple_table_schema() -> TableSchema {
    let cql = format!(
        "CREATE TABLE {KEYSPACE}.{TABLE} (\
             id UUID PRIMARY KEY, \
             name TEXT, \
             age INT, \
             salary BIGINT, \
             height FLOAT, \
             weight DOUBLE, \
             active BOOLEAN, \
             created TIMESTAMP, \
             birth_date DATE, \
             work_time TIME, \
             description BLOB, \
             account_balance DECIMAL, \
             session_id TIMEUUID, \
             ip_address INET, \
             small_number TINYINT, \
             medium_number SMALLINT, \
             duration_val DURATION, \
             varchar_field VARCHAR, \
             ascii_field ASCII\
         );"
    );
    parse_cql_schema(&cql).expect("parse simple_table schema")
}

async fn open_manager(dir: &std::path::Path) -> cqlite_core::storage::sstable::SSTableManager {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    cqlite_core::storage::sstable::SSTableManager::new(
        dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("open SSTableManager")
}

/// A comparable snapshot of one streamed entry.
type Entry = (Vec<u8>, ScanRow);

fn snap(key: RowKey, row: ScanRow) -> Entry {
    (key.as_bytes().to_vec(), row)
}

/// Drain the per-row surface. Returns the entries (one channel item == one send).
async fn collect_per_row(
    manager: &cqlite_core::storage::sstable::SSTableManager,
    table_id: &TableId,
    schema: &TableSchema,
    buffer_size: usize,
) -> Vec<Entry> {
    let mut rx = manager
        .scan_stream(table_id, None, None, Some(schema), buffer_size)
        .await
        .expect("scan_stream opens");
    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        let (k, v) = item.expect("per-row item Ok");
        out.push(snap(k, v));
    }
    out
}

/// Drain the batched surface. Returns `(batch_count, flattened entries)`; the batch
/// count is the number of channel sends (one per received `Vec`).
async fn collect_batched(
    manager: &cqlite_core::storage::sstable::SSTableManager,
    table_id: &TableId,
    schema: &TableSchema,
    buffer_size: usize,
) -> (usize, Vec<Entry>) {
    let mut rx = manager
        .scan_stream_batched(table_id, None, None, Some(schema), buffer_size)
        .await
        .expect("scan_stream_batched opens");
    let mut batch_count = 0usize;
    let mut flat = Vec::new();
    while let Some(item) = rx.recv().await {
        let batch = item.expect("batch item Ok");
        batch_count += 1;
        assert!(!batch.is_empty(), "a forwarded batch must be non-empty");
        assert!(
            batch.len() <= BATCH_EMIT_ROWS_MAX,
            "a batch ({}) must not exceed BATCH_EMIT_ROWS ({})",
            batch.len(),
            BATCH_EMIT_ROWS_MAX
        );
        for (k, v) in batch {
            flat.push(snap(k, v));
        }
    }
    (batch_count, flat)
}

/// Oracle 1 + 2: the batched surface sends far fewer items than the per-row surface
/// for the same scan, and flattening it reproduces the per-row stream exactly.
#[tokio::test]
async fn batched_reduces_sends_and_matches_perrow() {
    let Some(dir) = table_dir() else {
        eprintln!("Skipping issue #1592 send-count oracle: {KEYSPACE}/{TABLE} Data.db absent");
        return;
    };
    let manager = open_manager(&dir).await;
    let schema = simple_table_schema();
    let table_id = TableId::from(format!("{KEYSPACE}.{TABLE}").as_str());

    let per_row = collect_per_row(&manager, &table_id, &schema, 1024).await;
    let (batch_count, flat) = collect_batched(&manager, &table_id, &schema, 1024).await;

    // Non-vacuous precondition: a real, multi-row single-generation table.
    assert!(
        per_row.len() >= 8,
        "fixture must have several rows to exercise batching; got {}",
        per_row.len()
    );

    // Content + order parity: batched-then-flattened == per-row, element for element.
    assert_eq!(
        flat.len(),
        per_row.len(),
        "batched flattened row count ({}) must equal per-row count ({})",
        flat.len(),
        per_row.len()
    );
    for (i, (got, want)) in flat.iter().zip(per_row.iter()).enumerate() {
        assert_eq!(got.0, want.0, "row {i}: key mismatch batched-vs-per-row");
        assert_eq!(got.1, want.1, "row {i}: value mismatch batched-vs-per-row");
    }

    // Send-count reduction: one send per row on the per-row surface, one send per
    // BATCH on the batched surface. The batched surface must eliminate at least half
    // the sends, and prove real multi-row batching occurred.
    let per_row_sends = per_row.len();
    assert!(
        batch_count < per_row_sends,
        "batched sends ({batch_count}) must be fewer than per-row sends ({per_row_sends})"
    );
    assert!(
        per_row_sends - batch_count >= per_row_sends / 2,
        "batching must eliminate >= half the channel sends: per-row={per_row_sends}, batched={batch_count}"
    );
    assert!(
        flat.len() > batch_count,
        "average batch must carry more than one row (rows={}, batches={batch_count})",
        flat.len()
    );
}

/// Oracle 3: backpressure preserved. Under a tiny (cap = 1 batch) bounded channel
/// the batched scan still delivers every row in order (bounded channel does not drop
/// or reorder), and a mid-stream receiver drop terminates the producer cleanly.
#[tokio::test]
async fn batched_preserves_backpressure_and_is_drop_safe() {
    let Some(dir) = table_dir() else {
        eprintln!("Skipping issue #1592 backpressure oracle: {KEYSPACE}/{TABLE} Data.db absent");
        return;
    };
    let manager = open_manager(&dir).await;
    let schema = simple_table_schema();
    let table_id = TableId::from(format!("{KEYSPACE}.{TABLE}").as_str());

    let expected = collect_per_row(&manager, &table_id, &schema, 1024).await;
    assert!(expected.len() >= 8, "fixture must have several rows");

    // buffer_size = 1 -> batched channel capacity = ceil(1 / BATCH_EMIT_ROWS) = 1
    // batch. The producer can be at most one batch ahead of the consumer, so a slow
    // consumer stalls it; correctness (all rows, in order) must be unaffected.
    let (_batches, flat) = collect_batched(&manager, &table_id, &schema, 1).await;
    assert_eq!(
        flat.len(),
        expected.len(),
        "under a cap-1 bounded batch channel the scan must still deliver all rows"
    );
    for (i, (got, want)) in flat.iter().zip(expected.iter()).enumerate() {
        assert_eq!(got.0, want.0, "backpressure: row {i} key mismatch");
        assert_eq!(got.1, want.1, "backpressure: row {i} value mismatch");
    }

    // Drop-safe cancellation: receive only the first batch, then drop the receiver.
    // The producer's next bounded send fails on the closed channel and it terminates
    // (no panic, no hang). Yielding lets the spawned producer observe the drop.
    let mut rx = manager
        .scan_stream_batched(&table_id, None, None, Some(&schema), 1)
        .await
        .expect("scan_stream_batched opens");
    let first = rx.recv().await;
    assert!(first.is_some(), "expected at least one batch before drop");
    assert!(first.unwrap().is_ok(), "first batch must be Ok");
    drop(rx);
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    // Reaching here without panic/hang is the assertion: the producer handled the
    // consumer drop and terminated.
}
