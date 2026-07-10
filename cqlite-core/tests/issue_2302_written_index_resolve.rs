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

// Requires BOTH `work-counters` (the `read_work_counters` probe gauge asserted
// below) AND `write-support` (the `SSTableWriter` + `write_engine::mutation` API
// that synthesizes the CQLite-written fixtures). The full agent gate runs this via
// the `work-counters-guard` component, whose feature set
// (`write-support,cli-helpers,state_machine,work-counters`) enables both — it is
// this test's ONLY automated executor (issue #2302).
#![cfg(all(feature = "work-counters", feature = "write-support"))]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::read_work_counters;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, PartitionTombstone, TableId,
};
use cqlite_core::types::{ScanRow, Value};
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

/// A mutation whose ONLY effect is a partition-level tombstone (no live cells)
/// on `id` — the entire partition is shadowed and decodes to zero live rows.
fn partition_delete_mutation(id: i32) -> Mutation {
    let deletion_micros = 2_000_000 + id as i64;
    let mut m = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![],
        deletion_micros,
        None,
    );
    m.partition_tombstone = Some(PartitionTombstone {
        deletion_time: deletion_micros,
        local_deletion_time: 2,
    });
    m
}

/// Write `n` live single-row partitions PLUS one PURE partition-delete partition
/// (id `shadow_id`, a partition tombstone with no live cells), keeping every
/// emitted component. Returns `(Data.db path, total partition count)`.
async fn write_fixture_with_shadowed(
    temp: &TempDir,
    n: i32,
    shadow_id: i32,
) -> (std::path::PathBuf, usize) {
    let schema = schema();
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &schema).unwrap();
    let mut keyed: Vec<_> = (1..=n)
        .map(|id| {
            let m = mutation(id);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    let del = partition_delete_mutation(shadow_id);
    let del_key = del.decorated_key(&schema).unwrap();
    keyed.push((del_key, del));
    keyed.sort_by_key(|(k, _)| k.token);
    let total = keyed.len();
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    let info = writer.finish().await.unwrap();
    (info.data_path, total)
}

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

/// Locate the `-Index.db` sibling next to a written `Data.db`.
fn find_index_db(dir: &std::path::Path) -> std::path::PathBuf {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        if e.file_name().to_string_lossy().ends_with("-Index.db") {
            return e.path();
        }
    }
    panic!("written fixture must have an Index.db in {}", dir.display());
}

/// Byte offset where every entry of a CQLite-written BIG `Index.db` begins.
///
/// Entry layout: `[key_len u16 BE][raw key][data_offset uvint][promoted_len uvint]`.
/// The fixtures here write single-row partitions (no wide partitions), so every
/// `promoted_len` is 0 (a single 0x00 byte) — asserted, to keep the truncation
/// boundary math honest. Cassandra unsigned-vint length = 1 + (leading 1-bits of
/// byte 0). Entries start at byte 0 (nb Index.db is headerless).
fn entry_start_offsets(bytes: &[u8]) -> Vec<usize> {
    let mut starts = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        starts.push(pos);
        let key_len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2 + key_len;
        let off_len = 1 + bytes[pos].leading_ones() as usize;
        pos += off_len;
        assert_eq!(
            bytes[pos], 0x00,
            "fixture Index.db entries must carry promoted_len == 0"
        );
        pos += 1; // promoted_len == 0 -> single byte, no payload
    }
    starts
}

/// Run an async body under a `tracing` capture subscriber and return every event
/// it emitted. Mirrors the inline pattern in
/// `present_but_unresolvable_index_warns_and_falls_back`.
fn capture_events<F, Fut>(body: F) -> Vec<CapturedEvent>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let events = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let subscriber = Registry::default().with(CaptureLayer {
        events: Arc::clone(&events),
    });
    with_default(subscriber, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(body());
    });
    let out = events.lock().unwrap().clone();
    out
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

/// FIX B (issue #2302): a partition that decodes SUCCESSFULLY to zero LIVE rows
/// (here a pure partition-delete tombstone) must NOT demote the whole walk to a
/// sequential scan. The index path stays taken (probes == partition count,
/// covering the shadowed partition too) and contributes zero rows for it — never
/// a silent fallback just because one healthy partition happens to be fully
/// shadowed.
///
/// `#[serial]`: reads the process-global `read_work_counters` probe gauge.
#[tokio::test]
#[serial_test::serial]
async fn fully_shadowed_partition_keeps_index_path() {
    let temp = TempDir::new().unwrap();
    let n = 300i32; // > min_index_interval so Summary.db is genuinely sparse
    let shadow_id = 500i32; // deleted (no live cells); OUTSIDE the 1..=n live range
    let (data_path, total) = write_fixture_with_shadowed(&temp, n, shadow_id).await;
    let reader = open_reader(&data_path).await;

    assert!(
        reader.has_partition_index(),
        "written fixture must load Summary.db (has_partition_index)"
    );

    read_work_counters::reset();
    let rows = reader.iterate_all_partitions().await.unwrap();
    let probes = read_work_counters::index_probes();

    // The index path was NOT demoted: every partition (INCLUDING the fully
    // shadowed one) is resolved through a real Index.db probe.
    assert_eq!(
        probes, total as u64,
        "a fully-shadowed partition must NOT demote the walk to sequential_scan — \
         every partition (shadowed included) is probed via Index.db (issue #2302 FIX B)"
    );
    assert!(
        probes > 0,
        "index path must probe Index.db (silent fallback records zero probes)"
    );

    // The shadowed partition contributes zero LIVE rows; the other n survive.
    assert_eq!(
        rows.len(),
        n as usize,
        "the fully-shadowed partition contributes zero live rows; all other \
         partitions survive (no data loss, no spurious rows)"
    );
    let shadow_key = PartitionKey::single("id", Value::Integer(shadow_id))
        .to_decorated_key(&schema())
        .unwrap()
        .key;
    assert!(
        rows.iter()
            .all(|(k, _)| k.as_bytes() != shadow_key.as_slice()),
        "the shadowed partition's key must not surface as a live row"
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

    // Compare the full (key, decoded-value) pairs, not just keys: a future decoder
    // divergence between the index path and sequential_scan (same key set, different
    // cell values) must be caught. `ScanRow` derives `PartialEq`, so the pair vectors
    // compare structurally. Sort by key bytes only (keys are unique — one row per
    // partition — so this is a total, deterministic order without needing `Ord` on
    // `ScanRow`).
    let mut index_pairs: Vec<(Vec<u8>, ScanRow)> = index_rows
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
        .collect();
    let mut scan_pairs: Vec<(Vec<u8>, ScanRow)> = scan_rows
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
        .collect();
    index_pairs.sort_by(|a, b| a.0.cmp(&b.0));
    scan_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        index_pairs, scan_pairs,
        "index-random-read path must return the SAME (key, decoded-value) pairs as a \
         full sequential_scan (no data loss / no spurious rows / no decoder divergence)"
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

/// FINDING 1 (issue #2302, roborev job 1606): `IndexReader::open` accepts a
/// truncated Index.db whose whole trailing entries were dropped at an EXACT entry
/// boundary as a clean parsed PREFIX (no leftover bytes). The full-index walk must
/// NOT treat that prefix as a COMPLETE enumeration — it must detect that the final
/// entry's slice (bounded by the data-section end) spans MORE than one partition
/// (the dropped tail) and bail to the LOUD sequential-scan fallback.
///
/// RED before the fix: the truncated index was silently accepted (no WARN). The
/// last surviving entry's data-section-end backstop happened to still sweep up the
/// dropped tail so the row set stayed complete, but the reader relied on an index
/// it could not prove complete and emitted NO warning — this test's WARN assertion
/// fails on the pre-fix code. GREEN after: the final-partition coverage check
/// refuses the prefix, WARNs, and the sequential fallback returns every partition.
#[test]
#[serial_test::serial]
fn boundary_truncated_index_refused_and_warns() {
    let mut n_rows = 0usize;
    let events = capture_events(|| async {
        let temp = TempDir::new().unwrap();
        let n = 200i32;
        let data_path = write_fixture(&temp, n).await;

        // Drop the LAST whole entry at its exact start offset: the surviving prefix
        // is n-1 complete entries that parse cleanly to EOF (so `is_fully_parsed()`
        // stays true — only the final-partition coverage check can catch this).
        let dir = data_path.parent().unwrap();
        let index_path = find_index_db(dir);
        let bytes = std::fs::read(&index_path).unwrap();
        let starts = entry_start_offsets(&bytes);
        assert_eq!(
            starts.len(),
            n as usize,
            "fixture Index.db must hold one entry per partition"
        );
        let last_start = *starts.last().unwrap();
        std::fs::write(&index_path, &bytes[..last_start]).unwrap();

        let reader = open_reader(&data_path).await;
        let rows = reader.iterate_all_partitions().await.unwrap();
        n_rows = rows.len();

        // Correctness preserved: the sequential fallback still recovers every
        // partition (the Data.db was untouched).
        assert_eq!(
            rows.len(),
            n as usize,
            "the loud fallback must recover every partition from an intact Data.db"
        );
    });

    let warned = events.iter().any(|e| {
        e.level == Level::WARN
            && e.message.contains("Index.db is present")
            && e.message.contains("#2302")
    });
    assert!(
        warned,
        "a boundary-truncated (incomplete) Index.db must be REFUSED with a loud WARN \
         (issue #2302), never silently accepted as a complete enumeration. Rows \
         recovered: {n_rows}. Captured: {events:?}"
    );
}

/// FINDING 1 companion (Signal A): an Index.db cut MID-ENTRY leaves unparsed
/// trailing bytes, so `IndexReader::open` returns a prefix with `is_fully_parsed()
/// == false`. The walk must refuse it (WARN + sequential fallback), never accept a
/// mid-entry-truncated prefix as complete. RED before the fix (no WARN).
#[test]
#[serial_test::serial]
fn mid_entry_truncated_index_refused_and_warns() {
    let mut n_rows = 0usize;
    let events = capture_events(|| async {
        let temp = TempDir::new().unwrap();
        let n = 200i32;
        let data_path = write_fixture(&temp, n).await;

        // Cut the final byte (the last entry's promoted_len marker): the last entry
        // no longer parses, so the parser stops with a NON-EMPTY remainder (the
        // partial last entry) — `is_fully_parsed()` is false.
        let dir = data_path.parent().unwrap();
        let index_path = find_index_db(dir);
        let bytes = std::fs::read(&index_path).unwrap();
        std::fs::write(&index_path, &bytes[..bytes.len() - 1]).unwrap();

        let reader = open_reader(&data_path).await;
        let rows = reader.iterate_all_partitions().await.unwrap();
        n_rows = rows.len();
        assert_eq!(
            rows.len(),
            n as usize,
            "the loud fallback must recover every partition from an intact Data.db"
        );
    });

    let warned = events.iter().any(|e| {
        e.level == Level::WARN
            && e.message.contains("Index.db is present")
            && e.message.contains("#2302")
    });
    assert!(
        warned,
        "a mid-entry-truncated Index.db must be REFUSED with a loud WARN (issue \
         #2302). Rows recovered: {n_rows}. Captured: {events:?}"
    );
}

/// FINDING 2 (issue #2302, roborev job 1606): when Summary.db loads but Index.db
/// is PRESENT-but-unusable (open/parse fails), the reader must WARN loud — the
/// exact silent-degradation this issue kills — distinct from a genuinely ABSENT
/// Index.db (quiet, expected). Here the Index.db is truncated to zero bytes, so
/// `IndexReader::open` fails with a corruption error (file exists, not NotFound).
#[test]
#[serial_test::serial]
fn present_but_unloadable_index_warns_with_summary() {
    let mut n_rows = 0usize;
    let events = capture_events(|| async {
        let temp = TempDir::new().unwrap();
        let n = 200i32;
        let data_path = write_fixture(&temp, n).await;

        // Zero-length the Index.db: it EXISTS on disk but `IndexReader::open`
        // errors (Corruption, not NotFound). Summary.db stays intact.
        let dir = data_path.parent().unwrap();
        let index_path = find_index_db(dir);
        std::fs::write(&index_path, []).unwrap();

        let reader = open_reader(&data_path).await;
        assert!(
            reader.has_partition_index(),
            "Summary.db must still load (present-but-unusable Index.db path)"
        );
        let rows = reader.iterate_all_partitions().await.unwrap();
        n_rows = rows.len();
        assert_eq!(
            rows.len(),
            n as usize,
            "the loud fallback must recover every partition from an intact Data.db"
        );
    });

    let warned = events.iter().any(|e| {
        e.level == Level::WARN
            && e.message.contains("failed to open/parse")
            && e.message.contains("#2302")
    });
    assert!(
        warned,
        "a present-but-unloadable Index.db (Summary.db loaded) must WARN loud \
         (issue #2302), never silently full-scan. Rows: {n_rows}. Captured: {events:?}"
    );
}

/// FINDING 2 negative control: a genuinely ABSENT Index.db (Summary.db present)
/// must NOT emit the present-but-unloadable WARN — absence is quiet & expected.
#[test]
#[serial_test::serial]
fn absent_index_does_not_warn_present_but_unloadable() {
    let events = capture_events(|| async {
        let temp = TempDir::new().unwrap();
        let n = 200i32;
        let data_path = write_fixture(&temp, n).await;

        // Remove the Index.db entirely (genuinely absent); keep Summary.db.
        let dir = data_path.parent().unwrap();
        let index_path = find_index_db(dir);
        std::fs::remove_file(&index_path).unwrap();

        let reader = open_reader(&data_path).await;
        let rows = reader.iterate_all_partitions().await.unwrap();
        assert_eq!(rows.len(), n as usize, "fallback recovers every partition");
    });

    let spurious = events.iter().any(|e| {
        e.level == Level::WARN
            && e.message.contains("failed to open/parse")
            && e.message.contains("#2302")
    });
    assert!(
        !spurious,
        "an absent Index.db must NOT trigger the present-but-unloadable WARN. \
         Captured: {events:?}"
    );
}
