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
//! 1. `index_backed_partitions_resolved() > 0` (== partition count) after
//!    iterating a CQLite-WRITTEN uncompressed Summary/Index pair — RED before the
//!    fix (the pre-#2302 code sparsely walked Summary.db's 3 samples, all
//!    resolving to nothing → 0 rows → silent fallback).
//! 2. The index path returns the SAME row set as an explicit `sequential_scan`
//!    over the same fixture (no data loss, no reordering).
//!
//! NOTE (issue #2430): item 1 was originally pinned on `index_probes()` (a
//! per-partition `lookup_partition_with_index` re-probe). #2430 removed that
//! re-probe as pure redundant work once the loop already holds every entry's
//! `data_offset`, so `index_probes()` reads 0 on this path even when it fully
//! resolves — the oracle moved to `index_backed_partitions_resolved`, which
//! tracks the exact same "index path genuinely taken, not a silent
//! `sequential_scan` fallback" property.

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
            value: Value::text(format!("v{id}")),
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

/// Locate a sibling component file by suffix (e.g. `-CRC.db`) next to Data.db.
fn find_component(dir: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        if e.file_name().to_string_lossy().ends_with(suffix) {
            return e.path();
        }
    }
    panic!(
        "fixture must have a {suffix} component in {}",
        dir.display()
    );
}

/// Every `data_offset` from a CQLite-written BIG `Index.db`, in on-disk (token)
/// order — mirrors [`entry_start_offsets`] but returns the VInt-decoded offset
/// value instead of the entry's byte position, so a test can locate a specific
/// partition's byte range in `Data.db` without re-implementing the reader's own
/// parser. Entry layout: `[key_len u16 BE][raw key][data_offset uvint][promoted_len
/// uvint]`; a Cassandra unsigned-vint's total length is `1 + leading_ones(byte0)`,
/// and its value is the low `8 - leading_ones` bits of byte0 followed by the
/// continuation bytes, big-endian.
fn entry_data_offsets(bytes: &[u8]) -> Vec<u64> {
    let mut offsets = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let key_len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2 + key_len;
        let first = bytes[pos];
        let extra = first.leading_ones() as usize;
        let mut val: u64 = if extra >= 8 {
            0
        } else {
            (first as u64) & ((1u64 << (8 - extra)) - 1)
        };
        for k in 0..extra {
            val = (val << 8) | bytes[pos + 1 + k] as u64;
        }
        offsets.push(val);
        pos += 1 + extra;
        assert_eq!(
            bytes[pos], 0x00,
            "fixture Index.db entries must carry promoted_len == 0"
        );
        pos += 1;
    }
    offsets
}

/// Corrupt ONE partition's ROW BODY (not its header) in an otherwise-healthy
/// written fixture, re-checksumming `CRC.db` so the corruption is NOT caught by
/// CRC verification before the parser ever sees it (issue #2302, roborev job
/// 1609 HIGH: this is the specific "header parses, body doesn't" shape).
///
/// Every fixture partition here has a FIXED 18-byte header (`flags(1) +
/// key_len(1) + key(4, int PK) + nb DeletionTime(12)`), so the first ROW byte
/// (the row's own flags byte) sits at `data_offset(target) + 18`. Overwriting it
/// with `0xFF` sets an implausible combination of row-flag bits (far more than
/// the ~9 remaining body bytes of a one-column single-row partition can satisfy),
/// which reliably breaks structural row framing rather than merely corrupting a
/// cell's content. `CRC.db` covers the whole (single, well under 64 KiB)
/// uncompressed chunk, so the ONE new chunk CRC32 (`crc32fast`, matching
/// `java.util.zip.CRC32`) is written back over the stored value.
fn corrupt_one_partition_row_body(dir: &std::path::Path, target_entry: usize) {
    let index_bytes = std::fs::read(find_component(dir, "-Index.db")).unwrap();
    let offsets = entry_data_offsets(&index_bytes);
    assert!(
        target_entry < offsets.len(),
        "target entry {target_entry} out of range ({} entries)",
        offsets.len()
    );
    let corrupt_at = offsets[target_entry] as usize + 18;

    let data_path = find_component(dir, "-Data.db");
    let mut data_bytes = std::fs::read(&data_path).unwrap();
    assert!(
        corrupt_at < data_bytes.len(),
        "corruption offset {corrupt_at} out of range (Data.db is {} bytes)",
        data_bytes.len()
    );
    data_bytes[corrupt_at] = 0xFF;
    std::fs::write(&data_path, &data_bytes).unwrap();
    rewrite_crc_db_for(dir, &data_bytes);
}

/// Re-checksum `CRC.db`'s single chunk to match `new_data_bytes` (issue #2302):
/// every fixture here is well under the 64 KiB `CRC_CHUNK_SIZE`, so `CRC.db` is
/// exactly `header(4) + one CRC32(4)` = 8 bytes covering the WHOLE `Data.db`.
/// Used after any in-test `Data.db` mutation (corruption or truncation) so the
/// mutated bytes reach the parser instead of being rejected by CRC verification
/// first — the tests need the PARSER's completeness classification exercised,
/// not the (separate, already-covered) CRC-mismatch fail-fast path.
fn rewrite_crc_db_for(dir: &std::path::Path, new_data_bytes: &[u8]) {
    let crc_path = find_component(dir, "-CRC.db");
    let mut crc_bytes = std::fs::read(&crc_path).unwrap();
    assert_eq!(
        crc_bytes.len(),
        8,
        "fixture Data.db must fit in exactly ONE 64 KiB CRC.db chunk (header(4) + one CRC32(4))"
    );
    let new_crc = crc32fast::hash(new_data_bytes);
    crc_bytes[4..8].copy_from_slice(&new_crc.to_be_bytes());
    std::fs::write(&crc_path, &crc_bytes).unwrap();
}

/// The pinned regression: iterating a CQLite-WRITTEN Summary/Index pair must
/// resolve every partition through the index-backed materialising walk
/// (`index_backed_partitions_resolved` == partition count), not silently full-scan.
///
/// Issue #2430: this oracle was originally `index_probes() > 0` (a per-partition
/// `lookup_partition_with_index` re-probe). #2430 removed that re-probe — the
/// walk now resolves each offset directly from the already-loaded `Index.db`
/// entry — so `index_probes` reads 0 on this path even on a full resolve. The
/// discriminating property ("index path genuinely taken, not a silent
/// `sequential_scan` fallback") is unchanged; only the counter that proves it
/// moved to `index_backed_partitions_resolved`, incremented once per partition
/// exactly where the redundant probe used to fire.
///
/// `#[serial]`: `read_work_counters` is a process-global `static`, so a
/// concurrent counter-reading test in the same binary would perturb the count.
/// Serialize the counter-observing tests.
#[tokio::test]
#[serial_test::serial]
async fn written_summary_index_pair_resolves_via_index_backed_partitions() {
    let temp = TempDir::new().unwrap();
    let n = 300i32; // > min_index_interval (128) so Summary.db is genuinely sparse
    let data_path = write_fixture(&temp, n).await;
    let reader = open_reader(&data_path).await;

    // The reader must actually hold the random-access components for this to be a
    // non-vacuous test.
    assert!(
        reader.has_partition_index(),
        "written fixture must expose a random-access index (has_partition_index)"
    );

    read_work_counters::reset();
    let rows = reader.iterate_all_partitions().await.unwrap();
    let resolved = read_work_counters::index_backed_partitions_resolved();

    // RED before the fix (pre-#2302): the walk sparsely sampled Summary.db (3
    // samples, all resolving to nothing), then a silent sequential_scan — this
    // counter would read 0.
    assert!(
        resolved > 0,
        "index-backed materialising path must resolve partitions from the loaded \
         Index.db (got {resolved}) — a silent sequential_scan fallback resolves \
         none (issue #2302)"
    );
    assert_eq!(
        resolved, n as u64,
        "every partition must be resolved through the index-backed walk"
    );

    // Discrimination check (issue #2430): a reader with NO usable Index.db
    // (sibling deleted) falls straight into `sequential_scan`, which does NOT
    // touch this counter at all — proving `index_backed_partitions_resolved`
    // genuinely discriminates the index-backed branch from the fallback, not
    // just "some scan happened".
    let index_path = find_index_db(data_path.parent().unwrap());
    std::fs::remove_file(&index_path).unwrap();
    let fallback_reader = open_reader(&data_path).await;
    assert!(
        !fallback_reader.has_partition_index(),
        "sibling Index.db deleted: the reader must report no usable random-access index"
    );
    read_work_counters::reset();
    let fallback_rows = fallback_reader.iterate_all_partitions().await.unwrap();
    assert_eq!(
        read_work_counters::index_backed_partitions_resolved(),
        0,
        "a sequential_scan fallback (no Index.db) must record ZERO index-backed \
         resolutions — this is the discrimination proof for the counter above"
    );
    assert_eq!(
        fallback_rows.len(),
        n as usize,
        "the fallback must still recover every partition from an intact Data.db"
    );

    // No data loss: all partitions surface via the index-backed walk.
    assert_eq!(
        rows.len(),
        n as usize,
        "index-random-read path must return every partition, not the sparse samples"
    );
}

/// FIX B (issue #2302): a partition that decodes SUCCESSFULLY to zero LIVE rows
/// (here a pure partition-delete tombstone) must NOT demote the whole walk to a
/// sequential scan. The index path stays taken (`index_backed_partitions_resolved`
/// == partition count, covering the shadowed partition too) and contributes zero
/// rows for it — never a silent fallback just because one healthy partition
/// happens to be fully shadowed.
///
/// Issue #2430: migrated off `index_probes` for the same reason as the sibling
/// test above (the per-partition `lookup_partition_with_index` re-probe this
/// counted was removed as redundant work; `index_backed_partitions_resolved` is
/// the surviving discriminator).
///
/// `#[serial]`: reads the process-global `read_work_counters` gauge.
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
        "written fixture must expose a random-access index (has_partition_index)"
    );

    read_work_counters::reset();
    let rows = reader.iterate_all_partitions().await.unwrap();
    let resolved = read_work_counters::index_backed_partitions_resolved();

    // The index path was NOT demoted: every partition (INCLUDING the fully
    // shadowed one) is resolved through the index-backed walk.
    assert_eq!(
        resolved, total as u64,
        "a fully-shadowed partition must NOT demote the walk to sequential_scan — \
         every partition (shadowed included) is resolved via the index-backed walk \
         (issue #2302 FIX B)"
    );
    assert!(
        resolved > 0,
        "index path must resolve partitions from the loaded Index.db (silent \
         fallback resolves none)"
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
        // Index.db is present-but-unloadable (index_reader None), Summary.db loaded.
        // The capability probe (issue #2302 job 1613) reports the HONEST degraded
        // answer — NO usable random-access index — because Summary.db alone never
        // gates the fast path; the reader WARNs and falls back to sequential_scan.
        assert!(
            !reader.has_partition_index(),
            "present-but-unloadable Index.db (Summary loaded) must report NO usable \
             random-access index (issue #2302, roborev job 1613)"
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

/// FINDING (issue #2302, roborev job 1609 HIGH): `parser.parse_block` SWALLOWS a
/// row-parse failure internally (it logs and moves on) and exposes no
/// consumed-vs-available signal, so a partition whose HEADER parses but whose
/// BODY does not was silently accepted as "legitimately zero rows" by the
/// pre-job-1609 header-only recheck — never warning, never falling back. This
/// fixture corrupts ONE partition's row body (not its header) with a CRC.db
/// re-checksummed to match, so the corruption reaches the parser instead of being
/// rejected earlier by CRC verification.
///
/// RED before the fix: the enumeration silently accepted the corrupted partition
/// as empty and returned `n - 1` rows with **no WARN at all** (verified by
/// reverting `full_index_scan.rs` to the job-1606 state and re-running this exact
/// fixture: `iterate_all_partitions` returned 49/50 rows and emitted zero #2302
/// WARN events).
///
/// GREEN: the physical single-partition consumed-vs-available check
/// (`partition_slice_fully_consumed`) proves the corrupted slice was NOT fully
/// structurally consumed, so the walk bails with a loud WARN and falls back to
/// `sequential_scan` — the caller never accepts a guess.
///
/// Consistency note (originally requested by roborev job 1609) — **REWRITTEN BY
/// ISSUE #3721, WHICH CLOSED THE DEFECT THIS NOTE DECLARED OUT OF SCOPE.**
///
/// The note used to read: `sequential_scan` is NOT lossless on this corruption,
/// because its shared row/partition decoder (`parse_block_emit_windowed`) ALSO
/// swallows the same row-parse failure, and its byte-pattern partition-header
/// resync (issue #164/#258) can then drift for several bytes before it happens to
/// resynchronize, silently losing MULTIPLE partitions' rows. That swallow was
/// filed as a pre-existing defect of the shared decoder, out of scope for #2302
/// (which is about index-resolution ROUTING, not the row decoder). **Issue #3721
/// is that defect, and it is fixed**: the shared decoder no longer swallows a
/// row-body decode failure, so neither route serves a lossy row set any more.
///
/// The property this fixture proves therefore got STRICTLY STRONGER, and the
/// assertions below changed shape rather than being relaxed. Before #3721 the
/// claim was "the two routes agree on WHICH ROWS SURVIVE the swallow" — an
/// equality between two lossy sets, which held while both routes lost the same
/// rows for the same reason. Now the claim is "NEITHER route serves rows at all:
/// both REFUSE the corrupted body, with the same named error" — the routing
/// decision still introduces no divergence, and there is no longer a partial
/// result set for it to diverge ABOUT. The `rows.len() < n` assertion is gone
/// with the lossy set it measured; a count strictly below `n` was evidence of
/// tolerated loss, which is exactly what must no longer happen.
///
/// The #2302 WARN assertion is UNCHANGED and still load-bearing: the auto-routed
/// reader must still detect the corruption at the index path, WARN loudly, and
/// fall back — the fallback then refusing is #3721's behaviour, not #2302's.
#[test]
#[serial_test::serial]
fn corrupt_partition_body_refused_not_silently_emptied() {
    let mut rows_len = 0usize;
    let mut oracle_len = 0usize;
    let events = capture_events(|| async {
        let temp = TempDir::new().unwrap();
        let n = 50i32;
        let data_path = write_fixture(&temp, n).await;
        let dir = data_path.parent().unwrap().to_path_buf();

        // Corrupt a MIDDLE entry's row body (not the first or last, to prove this
        // isn't special-cased to the boundary-check sites from jobs 1606).
        corrupt_one_partition_row_body(&dir, 5);

        // Oracle: an INDEPENDENT reader over the SAME corrupted Data.db/CRC.db but
        // with Index.db/Summary.db/Filter.db stripped, forcing `sequential_scan`
        // directly (no auto-routing through the full-index path at all).
        let oracle_dir = temp.path().join("oracle");
        std::fs::create_dir_all(&oracle_dir).unwrap();
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with("-Summary.db")
                || name_str.ends_with("-Index.db")
                || name_str.ends_with("-Filter.db")
            {
                continue;
            }
            std::fs::copy(entry.path(), oracle_dir.join(&name)).unwrap();
        }
        let oracle_data_path = oracle_dir.join(data_path.file_name().unwrap());
        let oracle_reader = open_reader(&oracle_data_path).await;
        assert!(
            !oracle_reader.has_partition_index(),
            "oracle fixture must force sequential_scan directly (no index components)"
        );
        // Both legs are captured as Results and asserted on: this corrupted body
        // must be REFUSED, and an `unwrap()` here would abort the test on the
        // very error the fix exists to produce.
        let oracle_res = oracle_reader.iterate_all_partitions().await;
        oracle_len = oracle_res.as_ref().map(|r| r.len()).unwrap_or(0);

        // Auto-routed reader: full component set, corruption detected mid-walk.
        let reader = open_reader(&data_path).await;
        assert!(
            reader.has_partition_index(),
            "corrupted-but-index-present fixture must still expose the index path"
        );
        let auto_res = reader.iterate_all_partitions().await;
        rows_len = auto_res.as_ref().map(|r| r.len()).unwrap_or(0);

        // Issue #3721: a row body that fails to decode is REFUSED, never served
        // as a partial partition — on BOTH routes. Matched on the variant, never
        // on message text (#28).
        let oracle_err = match oracle_res {
            Err(e) => e,
            Ok(rows) => panic!(
                "issue #3721 REGRESSION: the forced sequential_scan over a \
                 corrupted row body returned Ok with {} of {n} row(s) — the \
                 swallow is back and the corrupted partition was served as a \
                 lossy result set",
                rows.len()
            ),
        };
        let auto_err = match auto_res {
            Err(e) => e,
            Ok(rows) => panic!(
                "issue #3721 REGRESSION: the auto-routed read over a corrupted \
                 row body returned Ok with {} of {n} row(s) — the index path \
                 detected the corruption and fell back, and the fallback then \
                 swallowed the failure",
                rows.len()
            ),
        };
        for (which, e) in [
            ("forced sequential_scan", &oracle_err),
            ("auto-routed", &auto_err),
        ] {
            assert!(
                matches!(e, cqlite_core::Error::ColumnDecode { .. }),
                "the {which} route must refuse the corrupted row body with the \
                 dedicated per-column variant (issue #3721), not some other \
                 error; got {e:?}"
            );
        }

        // The routing decision introduces no divergence: both routes refuse, and
        // they refuse IDENTICALLY. This replaces the pre-#3721 equality between
        // two lossy row sets — see the doc comment. Comparing the rendered
        // messages is sound HERE (unlike a variant check) because the claim is
        // that the two routes report the SAME thing, not that either reports a
        // particular thing.
        assert_eq!(
            oracle_err.to_string(),
            auto_err.to_string(),
            "the auto-routed fallback and an independently-forced \
             sequential_scan over the identical corrupted bytes must refuse with \
             the SAME error — the routing decision introduces no NEW divergence"
        );
    });

    let warned = events.iter().any(|e| {
        e.level == Level::WARN
            && e.message.contains("Index.db is present")
            && e.message.contains("#2302")
    });
    assert!(
        warned,
        "a partition whose header parses but whose body is corrupt must be \
         REFUSED with a loud WARN (issue #2302 job 1609), never silently treated \
         as an empty partition. rows={rows_len} oracle_rows={oracle_len}. \
         Captured: {events:?}"
    );
}

/// FINDING (issue #2302, roborev job 1612): the degraded-state WARN must key on
/// Index.db availability, NOT Summary.db presence. When Summary.db is PRESENT but
/// Index.db is genuinely ABSENT, the reader still cannot use the full-index
/// random-read path (a present Summary.db only samples ~1-in-128 partitions and
/// never gates that path) and drops into `sequential_scan` — the exact silent
/// full-scan perf cliff this issue kills. It must WARN loud, not fall through
/// quietly just because Summary.db happened to load.
///
/// RED before the fix (verified): the old branch chain only warned when
/// `summary_reader.is_none()`, so a present-Summary/absent-Index pair emitted NO
/// #2302 WARN — this test's WARN assertion fails on the pre-fix code.
///
/// GREEN: the WARN branch now fires whenever the BIG reader has no usable
/// `index_reader` (Index.db unusable) regardless of Summary.db, and the sequential
/// fallback still returns every partition.
#[test]
#[serial_test::serial]
fn summary_present_index_absent_warns_and_falls_back() {
    let mut n_rows = 0usize;
    let events = capture_events(|| async {
        let temp = TempDir::new().unwrap();
        let n = 200i32;
        let data_path = write_fixture(&temp, n).await;

        // Remove ONLY the Index.db (genuinely absent, not present-but-unloadable);
        // Summary.db stays intact so `summary_reader` loads.
        let dir = data_path.parent().unwrap();
        let index_path = find_index_db(dir);
        std::fs::remove_file(&index_path).unwrap();

        let reader = open_reader(&data_path).await;
        // Summary.db still loaded, but the full-index random-read path is
        // unavailable (Index.db gone): the degraded case. The capability probe
        // (issue #2302 job 1613) must NOT lie that a fast path exists — a present
        // Summary.db never gates the random-access route, so the probe reports
        // false and the reader will WARN + fall back to sequential_scan below.
        assert!(
            !reader.has_partition_index(),
            "present-Summary / absent-Index BIG reader must report NO usable \
             random-access index — Summary.db alone never gates the fast path \
             (issue #2302, roborev job 1613)"
        );
        let rows = reader.iterate_all_partitions().await.unwrap();
        n_rows = rows.len();

        // Correctness preserved: the sequential fallback recovers every partition
        // from the intact Data.db.
        assert_eq!(
            rows.len(),
            n as usize,
            "the loud fallback must recover every partition from an intact Data.db"
        );
    });

    // The degraded state was NOT silent: the "no usable random-access partition
    // index" WARN naming issue #2302 fired even though Summary.db was present.
    let warned = events.iter().any(|e| {
        e.level == Level::WARN
            && e.message
                .contains("no usable random-access partition index")
            && e.message.contains("#2302")
    });
    assert!(
        warned,
        "a present-Summary / absent-Index BIG SSTable must emit a loud degraded-state \
         WARN (issue #2302, roborev job 1612), never a silent sequential_scan fallback. \
         Rows: {n_rows}. Captured: {events:?}"
    );
}

/// FINDING 2 (issue #2302, roborev job 1610): `parse_one_partition_for_compaction`'s
/// `at_final_chunk = true` leniency collapses "consumed every byte but never saw
/// an explicit END_OF_PARTITION marker" into the SAME `Emitted(consumed)` as a
/// confirmed, marker-terminated partition — so a slice truncated EXACTLY at a
/// parseable row boundary (dropping only the trailing terminator byte, never a
/// row/cell mid-decode) would still satisfy the job-1609 `consumed == raw.len()`
/// check and be silently accepted as complete.
///
/// This fixture truncates the LAST partition's Data.db bytes by exactly ONE byte
/// — precisely the trailing `END_OF_PARTITION` (0x01) marker CQLite's writer
/// always appends (`data_writer/partition.rs`) — with `CRC.db` re-checksummed so
/// the shortened file still passes CRC verification. `data_section_end` is
/// derived from the (now one-byte-shorter) `Data.db` file length, so the LAST
/// index entry's implied span shrinks by exactly one byte too: the row content
/// itself is completely intact, only the terminator is missing.
///
/// RED before the fix (verified): reverted `partition_slice_fully_consumed` to
/// `at_final_chunk = true` and re-ran this exact fixture — `iterate_all_partitions`
/// silently returned 50/50 rows (all row CONTENT happened to survive, since only
/// the harmless trailing marker was cut) with **zero** #2302 WARN events, proving
/// the completeness check was accepting an UNCONFIRMED termination by coincidence,
/// not by proof.
///
/// GREEN: driving with `at_final_chunk = false` makes the ambiguous "ran out of
/// bytes" case report `ParseStep::NeedMore` (never `Emitted`), so the slice is
/// correctly refused (loud WARN, `sequential_scan` fallback) even though this
/// particular truncation happens to cost no rows once the fallback runs (the
/// shared shadowed decoder has its OWN "reached buffer end = partition complete"
/// leniency, so `sequential_scan` also recovers the row) — the fix's point is
/// that the index path's completeness PROOF no longer relies on that same luck.
#[test]
#[serial_test::serial]
fn boundary_truncation_before_terminator_refused_and_warns() {
    let mut rows_len = 0usize;
    let events = capture_events(|| async {
        let temp = TempDir::new().unwrap();
        let n = 50i32;
        let data_path = write_fixture(&temp, n).await;
        let dir = data_path.parent().unwrap();

        let mut data_bytes = std::fs::read(&data_path).unwrap();
        assert_eq!(
            data_bytes[data_bytes.len() - 1],
            0x01,
            "the last written byte must be the END_OF_PARTITION marker (CQLite's \
             writer always appends it)"
        );
        data_bytes.pop(); // drop EXACTLY the trailing terminator byte
        std::fs::write(&data_path, &data_bytes).unwrap();
        rewrite_crc_db_for(dir, &data_bytes);

        let reader = open_reader(&data_path).await;
        let rows = reader.iterate_all_partitions().await.unwrap();
        rows_len = rows.len();
        assert_eq!(
            rows.len(),
            n as usize,
            "the loud fallback must still recover every partition's row content \
             (only the trailing marker byte was dropped, not any row data)"
        );
    });

    let warned = events.iter().any(|e| {
        e.level == Level::WARN
            && e.message.contains("Index.db is present")
            && e.message.contains("#2302")
    });
    assert!(
        warned,
        "a slice truncated exactly at a row boundary with no CONFIRMED \
         end-of-partition terminator must be REFUSED with a loud WARN (issue \
         #2302 job 1610), never silently accepted via a bare 'ran out of bytes' \
         collapse. rows={rows_len}. Captured: {events:?}"
    );
}
