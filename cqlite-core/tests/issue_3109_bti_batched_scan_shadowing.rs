//! Issue #3109: the BATCHED streaming surface must apply read shadowing to BTI
//! (`da`) readers, exactly as the per-row surface does.
//!
//! `SSTableReader::run_scan_stream_batched` had no `bti_partitions_db.is_some()`
//! dispatch, unlike `scan` and `run_scan_stream`. A `da` reader therefore fell into
//! the non-stitching block loop, which decodes through
//! `parse_block_entries_at_now` → the `V5UncompressedOA` STATE MACHINE — a decoder
//! that takes neither `read_shadowing` nor a caller-pinned `now_secs`, so both are
//! silently dropped (`parsing/block_entries.rs`, the "KNOWN FAIL-OPEN SEAM"
//! comment). Net effect: a BTI table streamed through the batched surface was read
//! UNSHADOWED — TTL-expired rows (and, for a fixture that had them,
//! partition/range tombstones) were surfaced where the per-row surface hides them.
//! This is the #1577 class: per-surface decode-posture divergence.
//!
//! # Oracle
//!
//! `test_da.ttl_table` is a REAL Cassandra 5.0.2 BTI (`da`) SSTable. Its committed
//! sstabledump golden (`da-2-bti-Data.db.jsonl`) records both rows as written with
//! `"ttl": 86400` and `"expires_at": "2026-06-11T16:17:37Z"` — so Cassandra's own
//! `SELECT` semantics are unambiguous, and the golden is re-read here (never
//! hardcoded blind) so a corpus regeneration fails loudly instead of silently
//! weakening the test:
//!
//!   * at a PINNED `now` BEFORE that instant, both rows are LIVE — the non-vacuous
//!     arm (a strictly positive expected row count, byte-compared row-for-row
//!     against the per-row surface);
//!   * at a PINNED `now` AFTER it, both rows are EXPIRED and a `SELECT` returns
//!     NOTHING. Pre-fix the batched surface returned both rows here while the
//!     per-row surface returned none — the divergence this test pins.
//!
//! `now` is PINNED via the debug-only `CQLITE_TTL_NOW_OVERRIDE_SECS` reader seam
//! (`now_clock.rs`), never sampled from the wall clock (#2642): the fixture's
//! expiry is a fixed instant in the past, so a wall-clock read would only ever
//! exercise the expired arm and could never prove the live arm.
//!
//! Both phases run sequentially inside ONE test so the process-global env seam is
//! never mutated concurrently by a sibling test in this binary.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the gitignored `Data.db` binaries; SKIPs
//! (never passes with zero rows) when absent.

#![cfg(feature = "state_machine")]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{parse_cql_schema, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::types::{ScanRow, TableId};
use cqlite_core::{Config, RowKey};

const KEYSPACE: &str = "test_da";
const TABLE: &str = "ttl_table";
const SSTABLE_PREFIX: &str = "da-2-bti";

/// Debug-only reader seam (`now_clock.rs`): pins the read-time TTL "now" clock.
const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// The fixture's TTL expiry instant, as recorded by Cassandra's own sstabledump in
/// the committed golden. Asserted against the golden below, so a regenerated
/// corpus fails loudly rather than quietly invalidating the two pins.
const GOLDEN_EXPIRES_AT: &str = "2026-06-11T16:17:37Z";
const GOLDEN_EXPIRES_AT_EPOCH: i64 = 1_781_194_657;

/// 2026-06-11T00:00:00Z — strictly BEFORE the expiry, and after every write in the
/// fixture (`tstamp` 2026-06-10T16:17:37Z): every row is live.
const NOW_BEFORE_EXPIRY: i64 = 1_781_136_000;
/// 2026-07-02T00:00:00Z — the pin the sibling query-semantics cases use, well AFTER
/// the expiry: every row is expired.
const NOW_AFTER_EXPIRY: i64 = 1_782_950_400;

/// The `test_da/ttl_table-*` generation dir, requiring a real `Data.db` so a
/// JSONL-only checkout SKIPs rather than passing with zero rows.
fn fixture_dir() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)?;
    let keyspace_dir = root.join("sstables").join(KEYSPACE);
    let gen_dir = std::fs::read_dir(&keyspace_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with(&format!("{TABLE}-")))
                .unwrap_or(false)
        })?;
    gen_dir
        .join(format!("{SSTABLE_PREFIX}-Data.db"))
        .exists()
        .then_some(gen_dir)
}

/// Re-read the committed sstabledump golden and return the number of physical
/// rows, asserting the TTL/expiry facts the two pins are derived from.
///
/// The oracle is Cassandra's own dump of Cassandra-written bytes — never CQLite
/// output (#3042).
fn golden_rows_with_expiry(dir: &std::path::Path) -> usize {
    let golden = dir.join(format!("{SSTABLE_PREFIX}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&golden)
        .unwrap_or_else(|e| panic!("read golden {}: {e}", golden.display()));
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        !lines.is_empty(),
        "golden {} must record at least one partition (no vacuous pass)",
        golden.display()
    );
    for (i, line) in lines.iter().enumerate() {
        assert!(
            line.contains("\"ttl\":") && line.contains(GOLDEN_EXPIRES_AT),
            "golden partition {i} must carry the TTL/expiry this test's pins are \
             derived from (expires_at {GOLDEN_EXPIRES_AT}); the corpus was \
             regenerated — re-derive NOW_BEFORE_EXPIRY / NOW_AFTER_EXPIRY from the \
             new golden. Line: {line}"
        );
    }
    assert!(
        NOW_BEFORE_EXPIRY < GOLDEN_EXPIRES_AT_EPOCH && GOLDEN_EXPIRES_AT_EPOCH < NOW_AFTER_EXPIRY,
        "the two pins must straddle the golden's expiry instant"
    );
    lines.len()
}

/// Schema for `test_da.ttl_table` (matches `test-data/schemas/da-test.cql`).
fn ttl_table_schema() -> TableSchema {
    let cql = format!(
        "CREATE TABLE {KEYSPACE}.{TABLE} (\
             id UUID PRIMARY KEY, \
             data TEXT, \
             expiring_value INT\
         );"
    );
    parse_cql_schema(&cql).expect("parse ttl_table schema")
}

async fn open_manager(keyspace_dir: &std::path::Path) -> SSTableManager {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableManager::new(
        keyspace_dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("open SSTableManager")
}

type Entry = (Vec<u8>, ScanRow);

fn snap(key: RowKey, row: ScanRow) -> Entry {
    (key.as_bytes().to_vec(), row)
}

async fn collect_per_row(
    manager: &SSTableManager,
    table_id: &TableId,
    schema: &TableSchema,
) -> Vec<Entry> {
    let mut rx = manager
        .scan_stream(table_id, None, None, Some(schema), 256)
        .await
        .expect("scan_stream opens");
    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        let (k, v) = item.expect("per-row item Ok");
        out.push(snap(k, v));
    }
    out
}

async fn collect_batched(
    manager: &SSTableManager,
    table_id: &TableId,
    schema: &TableSchema,
) -> Vec<Entry> {
    let mut rx = manager
        .scan_stream_batched(table_id, None, None, Some(schema), 256)
        .await
        .expect("scan_stream_batched opens");
    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        for (k, v) in item.expect("batch item Ok") {
            out.push(snap(k, v));
        }
    }
    out
}

/// Issue #3109: the batched surface applies read shadowing to a BTI (`da`) reader,
/// row-for-row identically to the per-row surface, at BOTH a pinned `now` where the
/// fixture's rows are live and one where they are all TTL-expired.
#[tokio::test]
async fn bti_batched_scan_applies_read_shadowing_like_the_per_row_surface() {
    let Some(gen_dir) = fixture_dir() else {
        eprintln!(
            "SKIP issue #3109: {KEYSPACE}/{TABLE} Data.db absent (set CQLITE_DATASETS_ROOT \
             and fetch the datasets)"
        );
        return;
    };
    let physical_rows = golden_rows_with_expiry(&gen_dir);
    let keyspace_dir = gen_dir.parent().expect("generation dir has a parent");
    let schema = ttl_table_schema();
    let table_id = TableId::from(format!("{KEYSPACE}.{TABLE}").as_str());

    // ---- Phase 1: pinned `now` BEFORE the TTL expiry -> every row is LIVE. -----
    // The non-vacuous arm: a strictly positive expected row count, so neither
    // surface can pass by returning nothing.
    std::env::set_var(TTL_NOW_OVERRIDE_ENV, NOW_BEFORE_EXPIRY.to_string());
    let manager = open_manager(keyspace_dir).await;
    let live_per_row = collect_per_row(&manager, &table_id, &schema).await;
    let live_batched = collect_batched(&manager, &table_id, &schema).await;
    drop(manager);
    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);

    assert_eq!(
        live_per_row.len(),
        physical_rows,
        "at a pinned now BEFORE expiry the per-row surface must return every \
         physical row the golden records ({physical_rows})"
    );
    assert_eq!(
        live_batched.len(),
        live_per_row.len(),
        "at a pinned now BEFORE expiry the batched surface must return the same \
         row count as the per-row surface"
    );
    for (i, (got, want)) in live_batched.iter().zip(live_per_row.iter()).enumerate() {
        assert_eq!(got.0, want.0, "live row {i}: key mismatch batched-vs-per-row");
        assert_eq!(
            got.1, want.1,
            "live row {i}: value mismatch batched-vs-per-row"
        );
    }

    // ---- Phase 2: pinned `now` AFTER the TTL expiry -> every row is EXPIRED. ---
    // Pre-#3109 the batched surface returned all `physical_rows` here (the state
    // machine drops `read_shadowing`), while the per-row surface returned none.
    std::env::set_var(TTL_NOW_OVERRIDE_ENV, NOW_AFTER_EXPIRY.to_string());
    let manager = open_manager(keyspace_dir).await;
    let expired_per_row = collect_per_row(&manager, &table_id, &schema).await;
    let expired_batched = collect_batched(&manager, &table_id, &schema).await;
    drop(manager);
    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);

    assert!(
        expired_per_row.is_empty(),
        "control: at a pinned now AFTER expiry the per-row surface must hide every \
         expired row, got {} (this arm is what the batched surface is compared to)",
        expired_per_row.len()
    );
    assert!(
        expired_batched.is_empty(),
        "issue #3109: at a pinned now AFTER expiry the BATCHED surface must hide \
         every TTL-expired row exactly as the per-row surface does, got {} rows — \
         the BTI reader was decoded UNSHADOWED through the state machine",
        expired_batched.len()
    );
}
