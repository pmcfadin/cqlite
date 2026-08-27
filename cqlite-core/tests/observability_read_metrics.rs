//! Read-path metric honesty: the four headline READ metrics must actually be
//! emitted by the read path (issue #1701, epic #1686 observability honesty).
//!
//! # What this pins, and why it is a HONESTY test
//!
//! `cqlite.read.rows`, `cqlite.read.bytes`, `cqlite.read.partitions` and
//! `cqlite.read.duration` are documented in
//! [`cqlite_core::observability::catalog`], registered as instruments, rendered in
//! the operator metric reference, and showcased in the observability module's own
//! doc example — and before this issue NO production read path ever updated them.
//! An operator running a full `observability` build could not see read throughput
//! at all. A metric that is documented but never written is worse than an absent
//! one: a dashboard shows a flat zero and reads as "no reads happening".
//!
//! So the assertions here are deliberately about EMISSION AT A REAL READ SURFACE
//! (`SSTableReader::scan_stream`, `scan_stream_batched`, `get`) over REAL Cassandra
//! 5.0 SSTables — never about a helper being callable.
//!
//! # Granularity contract (why totals, not per-row assertions)
//!
//! The emission is at BATCH granularity: one counter add and one duration record
//! per read OPERATION (per chunk for `read.bytes`), never per row. The assertions
//! therefore compare the operation TOTAL against the rows the test itself received
//! — an independent tally — and require the duration histogram to hold at least one
//! recording. There is deliberately NO wall-clock threshold assertion anywhere
//! here: "a recording exists" is the property; "it took less than X ms" would be a
//! flake (#2642).
//!
//! # Fixture contract (#3220)
//!
//! Both fixtures are COMMITTED to git (`test_big/wide_partition`, a compressed BIG
//! `nb` SSTable with 3 partitions / 892 on-disk rows, and its Index.db sibling), so
//! they can never be legitimately absent in any checkout: resolution failure is a
//! hard FAILURE, per case, unconditionally — never a suite-wide `assert!(ran > 0)`
//! and never a silent SKIP. A present-but-empty scan (0 rows) fails too.
//!
//! # Why every metric assertion lives in ONE serial test
//!
//! The production metric helpers record through a single process-global `Meter`
//! that binds on first use, so the in-memory capture provider is process-wide and
//! cannot be swapped per test; the exporter uses DELTA temporality, so a
//! concurrently-running sibling test's read flow would land in this test's collect
//! window and break an exact-total assertion. The phases below therefore run in one
//! test, each `reset()`-ing immediately before its own flow — the same rationale
//! (and the same shape) as `observability_correctness.rs`.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core --features observability-testing \
//!   --test observability_read_metrics
//! ```

#![cfg(feature = "observability-testing")]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::observability::catalog;
use cqlite_core::observability::testing::{self, CapturedMetrics};
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::{Config, Platform, RowKey, ScanRow, TableId};

#[path = "support/datasets_root.rs"]
mod datasets_root;

/// A COMMITTED compressed BIG (`nb`) fixture: 3 partitions, 892 on-disk rows, with
/// a `CompressionInfo.db` so the scan really decompresses chunks (which is what
/// `read.bytes` counts).
const KEYSPACE: &str = "test_big";
const TABLE: &str = "wide_partition";

/// Resolve the fixture's `Data.db`. FAIL-CLOSED: the fixture is committed, so an
/// absence is a resolution defect, never a legitimate skip (#3220).
fn committed_data_db() -> PathBuf {
    let root = datasets_root::sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "{KEYSPACE}.{TABLE} is COMMITTED to git and must resolve in every checkout, \
             unconditionally (#3220) — {}",
            datasets_root::describe_search(KEYSPACE, TABLE)
        )
    });
    let ks_dir = root.join(KEYSPACE);
    let prefix = format!("{TABLE}-");
    let gen_dir = std::fs::read_dir(&ks_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", ks_dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
        })
        .unwrap_or_else(|| panic!("no {prefix}* generation dir under {}", ks_dir.display()));
    std::fs::read_dir(&gen_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", gen_dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .unwrap_or_else(|| panic!("no *-Data.db under {}", gen_dir.display()))
}

async fn open_reader() -> Arc<SSTableReader> {
    let data_db = committed_data_db();
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform initialisation"),
    );
    Arc::new(
        SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open the committed fixture"),
    )
}

/// Rows + distinct partition keys the TEST observed, computed independently of the
/// instrumentation: `partitions` is a set of distinct keys, while the production
/// accounting counts partition TRANSITIONS in scan order — two different
/// computations, so the equality below has teeth.
struct Tally {
    rows: u64,
    partitions: u64,
}

fn tally(entries: &[(RowKey, ScanRow)]) -> Tally {
    let distinct: HashSet<&[u8]> = entries.iter().map(|(k, _)| k.as_bytes()).collect();
    Tally {
        rows: entries.len() as u64,
        partitions: distinct.len() as u64,
    }
}

/// Number of recordings aggregated into a histogram metric (0 when absent).
fn histogram_recordings(metrics: &CapturedMetrics, name: &str) -> u64 {
    metrics
        .find(name)
        .map(|m| m.points.iter().filter_map(|p| p.count).sum())
        .unwrap_or(0)
}

fn metric_names(metrics: &CapturedMetrics) -> Vec<String> {
    metrics.entries().iter().map(|m| m.name.clone()).collect()
}

/// Assert the four READ metrics for one completed read operation.
fn assert_read_metrics(
    metrics: &CapturedMetrics,
    phase: &str,
    expected: &Tally,
    expect_bytes: bool,
) {
    assert_eq!(
        metrics.counter_sum(catalog::READ_ROWS),
        expected.rows as f64,
        "[{phase}] cqlite.read.rows must equal the {} rows the read actually \
         delivered; collected metrics: {:?}",
        expected.rows,
        metric_names(metrics)
    );
    assert_eq!(
        metrics.unit(catalog::READ_ROWS),
        Some(catalog::unit::ROWS),
        "[{phase}] cqlite.read.rows unit"
    );

    assert_eq!(
        metrics.counter_sum(catalog::READ_PARTITIONS),
        expected.partitions as f64,
        "[{phase}] cqlite.read.partitions must equal the {} distinct partitions the \
         read touched; collected metrics: {:?}",
        expected.partitions,
        metric_names(metrics)
    );
    assert_eq!(
        metrics.unit(catalog::READ_PARTITIONS),
        Some(catalog::unit::PARTITIONS),
        "[{phase}] cqlite.read.partitions unit"
    );

    // One duration RECORDING per read operation — never a wall-clock threshold
    // (#2642): the property is that the operation was timed at all.
    assert!(
        histogram_recordings(metrics, catalog::READ_DURATION) >= 1,
        "[{phase}] cqlite.read.duration must hold at least one recording for a \
         completed read operation; collected metrics: {:?}",
        metric_names(metrics)
    );
    assert_eq!(
        metrics.unit(catalog::READ_DURATION),
        Some(catalog::unit::SECONDS),
        "[{phase}] cqlite.read.duration unit"
    );

    if expect_bytes {
        assert!(
            metrics.counter_sum(catalog::READ_BYTES) > 0.0,
            "[{phase}] cqlite.read.bytes must count the decompressed Data.db bytes \
             the read materialised; collected metrics: {:?}",
            metric_names(metrics)
        );
        assert_eq!(
            metrics.unit(catalog::READ_BYTES),
            Some(catalog::unit::BYTES),
            "[{phase}] cqlite.read.bytes unit"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn read_path_emits_rows_bytes_partitions_and_duration() {
    let mc = testing::metrics_capture();
    let reader = open_reader().await;
    let tid = TableId::new(format!("{KEYSPACE}.{TABLE}"));

    // ---------------------------------------------------------------------
    // Phase 1 — the per-row streaming scan surface (`scan_stream`).
    // ---------------------------------------------------------------------
    mc.reset();
    let mut entries = Vec::new();
    {
        let mut stream = reader
            .clone()
            .scan_stream(tid.clone(), None, None, None, 64);
        while let Some(item) = stream.recv().await {
            entries.push(item.expect("per-row scan stream item"));
        }
    }
    let scan_metrics = mc.flush_and_collect();
    let scan_tally = tally(&entries);
    assert!(
        scan_tally.rows > 0,
        "the committed fixture is present but the scan delivered 0 rows — a \
         dataset-dependent assertion must never pass on an empty read"
    );
    assert!(
        scan_tally.partitions > 1,
        "the fixture must carry MORE THAN ONE partition for the partitions counter \
         to be distinguishable from the rows counter (saw {})",
        scan_tally.partitions
    );
    assert_read_metrics(&scan_metrics, "scan_stream", &scan_tally, true);

    // ---------------------------------------------------------------------
    // Phase 2 — the BATCHED streaming scan surface (`scan_stream_batched`), the
    // one the streaming SELECT executor consumes.
    // ---------------------------------------------------------------------
    mc.reset();
    let mut batched_entries = Vec::new();
    {
        let mut stream = reader
            .clone()
            .scan_stream_batched(tid.clone(), None, None, None, 64);
        while let Some(item) = stream.recv().await {
            batched_entries.extend(item.expect("batched scan stream item"));
        }
    }
    let batched_metrics = mc.flush_and_collect();
    let batched_tally = tally(&batched_entries);
    assert_eq!(
        batched_tally.rows, scan_tally.rows,
        "the batched surface must deliver the same rows as the per-row one"
    );
    assert_read_metrics(
        &batched_metrics,
        "scan_stream_batched",
        &batched_tally,
        false,
    );
    // ... and the OTHER direction of the read.bytes semantic, pinned positively:
    // this second scan of the same SSTable serves every chunk from the resident
    // decompressed-chunk cache, and a cache hit reads NO Data.db bytes. So a WARM
    // scan must count ZERO bytes while still counting its rows and partitions —
    // the metric is "bytes read from Data.db", not "bytes handed to the decoder".
    assert_eq!(
        batched_metrics.counter_sum(catalog::READ_BYTES),
        0.0,
        "a warm scan (every chunk resident in the decompressed-chunk cache) must \
         count ZERO cqlite.read.bytes — counting a cache hit would overstate the \
         Data.db I/O the read performed"
    );

    // ---------------------------------------------------------------------
    // Phase 3 — the point read (`get`). One partition, one row.
    // ---------------------------------------------------------------------
    let key = entries
        .first()
        .map(|(k, _)| k.clone())
        .expect("a present partition key learned from the scan");
    mc.reset();
    let row = reader.get(&tid, &key).await.expect("point read");
    let point_metrics = mc.flush_and_collect();
    assert!(row.is_some(), "the learned key must resolve a row");
    assert_read_metrics(
        &point_metrics,
        "get",
        &Tally {
            rows: 1,
            partitions: 1,
        },
        // Bytes are counted per decompressed chunk; the point read may serve its
        // covering chunk from the resident chunk cache populated by the scans
        // above, and a cache hit reads no Data.db bytes. So the point phase does
        // not assert bytes — the scan phases do.
        false,
    );
}
