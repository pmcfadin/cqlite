//! Issue #2302: CQLite-written Summary.db/Index.db pairs must fully resolve
//! through the index-random-read path — never a silent fallback to
//! `sequential_scan`.
//!
//! Root cause: `iterate_all_partitions` walked only the SPARSE `Summary.db`
//! samples (≈1-in-128 partitions) and passed `data_size = 0` to the partition
//! parser (Index.db never stores a partition size), so it read zero bytes per
//! entry, resolved zero partitions, and SILENTLY fell back to a full
//! `sequential_scan` on EVERY read — even with complete, valid components. The
//! fix enumerates every partition via the FULL Index.db offset table, bounding
//! each partition by the successor entry's offset (last by the data-section end).
//!
//! Wiring evidence (this file, `work-counters` feature):
//! 1. `index_probes() > 0` (== partition count) after iterating a CQLite-WRITTEN
//!    uncompressed Summary/Index pair — RED before the fix (probes were the 3
//!    sparse samples, all resolving to nothing → 0 rows → silent fallback).
//! 2. The index path returns the SAME row set as an explicit `sequential_scan`
//!    over the same fixture (no data loss, no reordering).

#![cfg(feature = "work-counters")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::read_work_counters;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Platform};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tracing::field::{Field, Visit};
use tracing::subscriber::with_default;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::Registry;

/// One captured `tracing` event (level + rendered message).
#[derive(Clone, Debug)]
struct CapturedEvent {
    level: Level,
    message: String,
}

struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        if let Ok(mut events) = self.events.lock() {
            events.push(CapturedEvent {
                level: *event.metadata().level(),
                message: visitor.message,
            });
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn mutation(id: i32) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(format!("v{id}")),
        }],
        1_000_000 + id as i64,
        None,
    )
}

/// Write `n` single-row partitions to a fresh uncompressed SSTable (flush path),
/// keeping every emitted component (Summary.db/Index.db/Filter.db included).
async fn write_fixture(temp: &TempDir, n: i32) -> std::path::PathBuf {
    let schema = schema();
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &schema).unwrap();
    let mut keyed: Vec<_> = (1..=n)
        .map(|id| {
            let m = mutation(id);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    let info = writer.finish().await.unwrap();
    info.data_path
}

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

/// The pinned regression: iterating a CQLite-WRITTEN Summary/Index pair must
/// probe the Index.db (probes == partition count), not silently full-scan.
///
/// `#[serial]`: `read_work_counters` is a process-global `static`, so a
/// concurrent counter-reading test in the same binary would perturb the probe
/// count. Serialize the counter-observing tests.
#[tokio::test]
#[serial_test::serial]
async fn written_summary_index_pair_resolves_via_index_probes() {
    let temp = TempDir::new().unwrap();
    let n = 300i32; // > min_index_interval (128) so Summary.db is genuinely sparse
    let data_path = write_fixture(&temp, n).await;
    let reader = open_reader(&data_path).await;

    // The reader must actually hold the random-access components for this to be a
    // non-vacuous test.
    assert!(
        reader.has_partition_index(),
        "written fixture must load Summary.db (has_partition_index)"
    );

    read_work_counters::reset();
    let rows = reader.iterate_all_partitions().await.unwrap();
    let probes = read_work_counters::index_probes();

    // RED before the fix: probes were the 3 sparse Summary.db samples (all
    // resolving to nothing), then a silent sequential_scan → this asserted 0.
    assert!(
        probes > 0,
        "index-random-read path must probe Index.db (got {probes} probes) — a silent \
         sequential_scan fallback records zero probes (issue #2302)"
    );
    assert_eq!(
        probes, n as u64,
        "every partition must be resolved through a real Index.db probe"
    );

    // No data loss: all partitions surface.
    assert_eq!(
        rows.len(),
        n as usize,
        "index-random-read path must return every partition, not the sparse samples"
    );
}

/// Correctness parity: the index-random-read enumeration returns exactly the same
/// row set (keys) as an explicit full `sequential_scan` over the same fixture.
///
/// `#[serial]`: this test drives `iterate_all_partitions` (bumping the
/// process-global probe counter), so it must not run concurrently with the
/// probe-counting test in the same binary.
#[tokio::test]
#[serial_test::serial]
async fn index_path_matches_sequential_scan_row_set() {
    let temp = TempDir::new().unwrap();
    let n = 300i32;
    let data_path = write_fixture(&temp, n).await;

    // Index-random-read path.
    let reader = open_reader(&data_path).await;
    let index_rows = reader.iterate_all_partitions().await.unwrap();

    // Force the full-scan oracle by opening a second reader and stripping the
    // random-access components so `iterate_all_partitions` takes the sequential
    // fallback (the pre-fix behaviour / the genuinely index-less case, #2295).
    let temp2 = TempDir::new().unwrap();
    let scan_data_path = write_fixture(&temp2, n).await;
    // The flush path writes components into the same directory as Data.db (a
    // keyspace/table subtree under `temp`), so strip siblings THERE.
    let scan_dir = scan_data_path.parent().unwrap();
    for entry in std::fs::read_dir(scan_dir).unwrap().flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with("-Summary.db")
            || name.ends_with("-Index.db")
            || name.ends_with("-Filter.db")
        {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    let scan_reader = open_reader(&scan_data_path).await;
    assert!(
        !scan_reader.has_partition_index(),
        "stripped fixture must have no partition index (forces sequential_scan)"
    );
    let scan_rows = scan_reader.iterate_all_partitions().await.unwrap();

    let mut index_keys: Vec<Vec<u8>> = index_rows
        .iter()
        .map(|(k, _)| k.as_bytes().to_vec())
        .collect();
    let mut scan_keys: Vec<Vec<u8>> = scan_rows
        .iter()
        .map(|(k, _)| k.as_bytes().to_vec())
        .collect();
    index_keys.sort();
    scan_keys.sort();

    assert_eq!(
        index_keys, scan_keys,
        "index-random-read path must return the SAME partition key set as a full \
         sequential_scan (no data loss / no spurious rows)"
    );
}

/// The fallback must NEVER be silent (issue #2302): when the Index.db is present
/// but structurally unresolvable (here: a corrupted, non-ascending offset), the
/// reader emits a loud WARN naming the fallback and STILL returns every partition
/// via the sequential-scan oracle (correctness preserved).
///
/// Plain `#[test]` (not `#[tokio::test]`) so the `tracing` subscriber installed
/// by `with_default` stays active across the reader's async work, driven on a
/// current-thread runtime inside the subscriber scope.
///
/// `#[serial]`: this test bumps the process-global `read_work_counters` probe
/// counter, so it must not run concurrently with the probe-counting test.
#[test]
#[serial_test::serial]
fn present_but_unresolvable_index_warns_and_falls_back() {
    let events = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let subscriber = Registry::default().with(CaptureLayer {
        events: Arc::clone(&events),
    });

    with_default(subscriber, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let temp = TempDir::new().unwrap();
            let n = 200i32;
            let data_path = write_fixture(&temp, n).await;

            // Corrupt the FIRST Index.db entry's offset so it is LARGER than the
            // second entry's offset (non-ascending) — a structurally inconsistent
            // index the resolver must reject (issue #28: authoritative structure,
            // no size guessing). Entry layout: [key_len u16=0x0004][key 4B]
            // [offset vint][promoted vint=0x00]; byte 6 is the 1-byte offset vint
            // for the first (offset-0) entry. 0x7F (127) > the ~28-byte second
            // entry offset, so `next_offset <= data_offset` trips → helper returns
            // None → WARN + sequential_scan fallback.
            let dir = data_path.parent().unwrap();
            let mut index_path = None;
            for e in std::fs::read_dir(dir).unwrap().flatten() {
                if e.file_name().to_string_lossy().ends_with("-Index.db") {
                    index_path = Some(e.path());
                }
            }
            let index_path = index_path.expect("written fixture must have an Index.db");
            let mut ib = std::fs::read(&index_path).unwrap();
            ib[6] = 0x7F;
            std::fs::write(&index_path, &ib).unwrap();

            let reader = open_reader(&data_path).await;
            let rows = reader.iterate_all_partitions().await.unwrap();

            // Correctness preserved: the sequential-scan fallback still returns
            // every partition despite the unusable index.
            assert_eq!(
                rows.len(),
                n as usize,
                "fallback must recover every partition when the index is unusable"
            );
        });
    });

    // The fallback was NOT silent: a WARN naming issue #2302 was emitted.
    let captured = events.lock().unwrap();
    let warned = captured.iter().any(|e| {
        e.level == Level::WARN
            && e.message.contains("Index.db is present")
            && e.message.contains("#2302")
    });
    assert!(
        warned,
        "a present-but-unresolvable Index.db must emit a loud WARN (issue #2302), \
         never a silent sequential_scan fallback. Captured: {captured:?}"
    );
}
