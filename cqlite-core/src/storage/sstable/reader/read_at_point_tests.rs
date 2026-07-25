//! In-crate concurrency scenarios for the `ReadAt` point-read migration
//! (issue #1573, Epic C / C2). These live in-crate (not in `tests/`) because they
//! inject a `pub(crate)` [`ReadAt`](super::read_at::ReadAt) test double into the
//! reader's `point_source` — a surface no external crate can reach.
//!
//! Requires `CQLITE_DATASETS_ROOT` + fetched binaries; every test skips (never
//! fails) when its fixture is absent, and never treats 0 rows as a skip.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::read_at::{SerializingReadAt, SleepingReadAt};
use super::SSTableReader;
use crate::{Config, Platform};

/// Locate a `*-Data.db` under `<datasets>/sstables/<keyspace>/<table>-*/`.
/// Returns `None` (skip) when the datasets root or fixture is absent.
fn find_data_db(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let ks_dir = PathBuf::from(root).join("sstables").join(keyspace);
    let prefix = format!("{table}-");
    for entry in std::fs::read_dir(&ks_dir).ok()?.flatten() {
        if !entry.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        for f in std::fs::read_dir(entry.path()).ok()?.flatten() {
            if f.file_name().to_string_lossy().ends_with("-Data.db") {
                return Some(f.path());
            }
        }
    }
    None
}

async fn open_reader(path: &std::path::Path) -> Option<SSTableReader> {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.ok()?);
    SSTableReader::open(path, &config, platform).await.ok()
}

/// Issue every offset concurrently through `read_value_at_offset` on a shared
/// reader and return the wall time. Each offset is distinct so the shared chunk
/// cache does not collapse the reads into one.
async fn concurrent_point_reads(
    reader: Arc<SSTableReader>,
    offsets: &[u64],
    size: u32,
) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::new();
    for &off in offsets {
        let r = Arc::clone(&reader);
        handles.push(tokio::spawn(async move {
            // Result is intentionally ignored: the scenario measures whether the
            // reads SERIALIZE, not their payload (payload correctness is the
            // parity test's job). Every call still traverses the point source.
            let _ = r.read_value_at_offset(off, size).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    start.elapsed()
}

/// Scenario: 8 concurrent point reads do NOT convoy.
///
/// The reader's scan-side positional source is wrapped in a 12ms-sleeping
/// [`SleepingReadAt`] (no lock) for the treatment, and in a lock-holding
/// [`SerializingReadAt`] for the control. The control proves the harness CAN
/// serialize (the pre-#1573 `Arc<Mutex<BlockSource>>` convoy: ~N×delay); the
/// treatment proves the migrated positional path does NOT (well under 8×delay),
/// because positioned reads on a shared source are independent (`&self`, no
/// cursor mutex).
///
/// `read_value_at_offset`'s UNCOMPRESSED branch (`get_cached_data`) is wrapped
/// here, not `point_source`: it is reached by every index-driven scan
/// (`sequential.rs`) as well as the rare index-less point-lookup fallback
/// (`big_point.rs`), so issue #2876 repointed it at `scan_positional_source` —
/// the shared plane must NOT alias the reader's dedicated `MADV_RANDOM`
/// point-read mapping (issue #2210), which is exactly backwards for a scan's
/// mostly-sequential access pattern.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_concurrent_point_reads_do_not_convoy() {
    let Some(path) = find_data_db("test_basic", "uncompressed_table") else {
        eprintln!("SKIP (#1573 convoy): test_basic/uncompressed_table Data.db absent");
        return;
    };
    let delay = Duration::from_millis(12);
    // 8 distinct offsets (distinct cache keys) so each read reaches the source.
    let offsets: Vec<u64> = (0..8u64).map(|i| i * 96).collect();
    let size = 48u32;

    // --- Control: a lock-holding source serializes (reproduces the convoy). ---
    let control = {
        let mut reader = open_reader(&path).await.expect("open control reader");
        let real = reader.clone_scan_positional_source();
        reader.set_scan_positional_source(Arc::new(SerializingReadAt::new(real, delay)));
        let reader = Arc::new(reader);
        concurrent_point_reads(reader, &offsets, size).await
    };

    // --- Treatment: the migrated positional source runs the 8 reads in parallel. ---
    let calls = Arc::new(AtomicUsize::new(0));
    let treatment = {
        let mut reader = open_reader(&path).await.expect("open treatment reader");
        let real = reader.clone_scan_positional_source();
        reader.set_scan_positional_source(Arc::new(SleepingReadAt::new(
            real,
            delay,
            calls.clone(),
        )));
        let reader = Arc::new(reader);
        concurrent_point_reads(reader, &offsets, size).await
    };

    // Routing proof (deterministic; this is what is RED on `main`, where
    // get_cached_data/verify_uncompressed_range still lock `self.file` and never
    // touch `scan_positional_source`): the read path must reach the injected
    // source.
    assert!(
        calls.load(Ordering::Relaxed) >= offsets.len(),
        "read path must route through `scan_positional_source` (>= {} reads); got {} \
         — a 0 here means the read path no longer uses scan_positional_source",
        offsets.len(),
        calls.load(Ordering::Relaxed)
    );

    // Parallelism proof, expressed RELATIVE to the measured control (no absolute
    // wall-clock threshold, so it does not flake on a slow/loaded CI host): the
    // serialized control does ~8× the sleeping work, so the parallel treatment
    // must be at least ~2× faster. On `main` (mutex convoy) treatment ≈ control.
    assert!(
        treatment.saturating_mul(2) < control,
        "migrated positional point reads must NOT convoy: parallel treatment {treatment:?} \
         should be far faster than the serialized control {control:?} (>= 2×); \
         near-equal means the reads still serialize"
    );
}

/// Scenario: two concurrent interleaved point reads at different offsets each
/// return their own bytes (no cross-contamination), driven end-to-end through the
/// reader's `read_value_at_offset` on the shared positional source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_point_reads_return_correct_bytes() {
    let Some(path) = find_data_db("test_basic", "uncompressed_table") else {
        eprintln!("SKIP (#1573 interleave): test_basic/uncompressed_table Data.db absent");
        return;
    };
    let reader = Arc::new(open_reader(&path).await.expect("open reader"));

    // Two disjoint ranges read concurrently many times; each must be internally
    // consistent across repetitions (a shared cursor would interleave them).
    let (off_a, off_b, size) = (0u64, 256u64, 32u32);
    let first_a = reader
        .read_value_at_offset(off_a, size)
        .await
        .expect("read A");
    let first_b = reader
        .read_value_at_offset(off_b, size)
        .await
        .expect("read B");

    let mut handles = Vec::new();
    for _ in 0..50 {
        let (ra, rb) = (Arc::clone(&reader), Arc::clone(&reader));
        let (ea, eb) = (first_a.clone(), first_b.clone());
        handles.push(tokio::spawn(async move {
            let a = ra.read_value_at_offset(off_a, size).await.expect("A");
            let b = rb.read_value_at_offset(off_b, size).await.expect("B");
            assert_eq!(a, ea, "offset A read drifted under concurrency");
            assert_eq!(b, eb, "offset B read drifted under concurrency");
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
}

/// Build a genuine COMPRESSED BIG ("nb") SSTable with a USABLE `Summary.db`
/// (issue #2876): `N` single-int-PK partitions via the production `WriteEngine`
/// (a valid uncompressed BIG Data.db + Index.db/Summary.db/Statistics.db), then
/// LZ4-compress the flushed Data.db IN PLACE — the same recipe
/// `issue_1293_compressed_big_reverse_seek.rs` established. CQLite's own write
/// surface never emits compression (issue #1406 claim boundary), so this is the
/// only way to get a genuine compressed-nb fixture without a fetched real-
/// Cassandra dataset (whose small tables don't reliably clear the
/// `min_index_interval` sampling threshold either). `N` is comfortably over the
/// default `min_index_interval` (128) so `Summary.db` carries multiple samples —
/// a single-sample summary would make `stream_all_partitions_for_query`'s
/// summary-guided branch untestable (it requires non-empty `Summary.db` entries).
///
/// Sound because the uncompressed-BIG Data.db is HEADERLESS (data starts at byte
/// 0) and `Index.db` offsets are in the uncompressed domain — exactly what the
/// compressed reader assumes (`CompressionInfo` chunk offsets are relative to
/// `Data.db` byte 0), so re-chunking + compressing the bytes in place needs no
/// change to Index.db/Summary.db/Statistics.db.
#[cfg(all(feature = "write-support", feature = "lz4"))]
async fn build_compressed_summary_guided_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    crate::schema::TableSchema,
) {
    use crate::schema::{Column, KeyColumn, TableSchema};
    use crate::storage::sstable::writer::{
        create_compressor, CompressedDataWriter, CompressionAlgorithm, CompressionInfoWriter,
    };
    use crate::storage::write_engine::{
        CellOperation, Durability, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
    };
    use crate::types::Value;

    const KS: &str = "issue_2876_ks";
    const TBL: &str = "items";
    /// Comfortably over the default `min_index_interval` (128, see
    /// `issue_2412_wraparound_scan.rs`'s identical rationale) so `Summary.db`
    /// carries multiple samples spanning distinct `Index.db` positions.
    const N: i32 = 400;
    /// Small chunks so the fixture's LZ4-compressed Data.db spans several
    /// compressed chunks, not just one.
    const CHUNK_SIZE: usize = 4096;

    let schema = TableSchema {
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
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone())
        .with_flush_threshold(1usize << 30)
        .with_durability(Durability::Disabled);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    for id in 0..N {
        let mutation = Mutation::new(
            TableId::new(KS, TBL),
            PartitionKey::single("id", Value::Integer(id)),
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::text(format!("v{id}")),
            }],
            1_000_000 + id as i64,
            None,
        );
        engine.write(mutation).expect("write row");
    }
    engine
        .flush()
        .await
        .expect("flush")
        .expect("flush must produce an SSTable");
    engine.close().await.expect("close engine");

    let (data_path, base) =
        locate_data_db_under(&data_dir).expect("no *-Data.db produced under data_dir");

    // Compress Data.db in place (issue #1293 recipe).
    let uncompressed = std::fs::read(&data_path).expect("read uncompressed Data.db");
    let compressor = create_compressor(CompressionAlgorithm::Lz4).expect("lz4 compressor");
    let mut writer = CompressedDataWriter::with_chunk_size(compressor, CHUNK_SIZE);
    writer.write(&uncompressed).expect("compress Data.db");
    let (compressed, metadata) = writer.finish().expect("finish compression");
    std::fs::write(&data_path, &compressed).expect("overwrite Data.db with compressed bytes");

    let parent = data_path.parent().expect("Data.db parent dir");
    let info_path = parent.join(format!("{base}-CompressionInfo.db"));
    CompressionInfoWriter::new(info_path)
        .write(&metadata)
        .expect("write CompressionInfo.db");

    // The uncompressed-BIG CRC.db describes the old chunking and no longer
    // matches; compressed BIG carries per-chunk CRCs inline in Data.db instead.
    let crc_path = parent.join(format!("{base}-CRC.db"));
    let _ = std::fs::remove_file(&crc_path);

    (temp, data_path, schema)
}

/// Recursively locate the single `*-Data.db` under `dir` and derive its `<base>`
/// (e.g. `nb-1-big`).
#[cfg(all(feature = "write-support", feature = "lz4"))]
fn locate_data_db_under(dir: &std::path::Path) -> Option<(std::path::PathBuf, String)> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if let Some(base) = name.strip_suffix("-Data.db") {
                return Some((p.clone(), base.to_string()));
            }
        }
    }
    None
}

/// Regression test (issue #2876): the Summary-guided COMPRESSED scan walk —
/// `stream_all_partitions_for_query` → `stream_partitions_summary_guided_compaction`
/// → `walk_in_range_partition_slices` (`summary_scan.rs`) →
/// `read_compressed_offset_window` (`compressed_offset.rs`) — must NOT read
/// Data.db through the reader's dedicated `MADV_RANDOM` point-read mapping
/// (`point_source`). That advice exists to suppress kernel readahead for
/// scattered point-lookup faults (issue #2210); on this mostly-sequential walk it
/// was exactly backwards, forcing one 4 KiB page fault per partition instead of
/// the ~128 KiB read-ahead window (the #2210 × #1940 cross-path regression).
///
/// RED before the fix: `compressed_offset.rs` passed `self.point_source` into
/// the shared offset-window reader, so every partition slice the walk
/// decompresses bumps the spy installed on `point_source` below. GREEN after the
/// fix: the walk reads through the new `scan_positional_source` instead, so the
/// spy on `point_source` sees ZERO calls while the walk still emits every row
/// (never a silent 0-rows pass — CLAUDE.md's dataset-dependent-test guardrail).
#[cfg(all(feature = "write-support", feature = "lz4"))]
#[tokio::test]
async fn summary_guided_compressed_scan_walk_avoids_point_source() {
    use std::ops::ControlFlow;

    use super::compaction_row::CompactionRow;
    use crate::storage::scan_cancel::ScanCancel;

    let (_temp, data_path, schema) = build_compressed_summary_guided_fixture().await;
    let mut reader = open_reader(&data_path)
        .await
        .expect("open compressed fixture reader");

    // Fixture preconditions (fail LOUD, never silently skip: this fixture is
    // built in-test, not a fetched dataset that can legitimately be absent) —
    // the walk under test requires compression, a raw-key Index.db, no BTI
    // trie, and a `Summary.db` with at least one sample.
    assert!(
        reader.compression_info.is_some(),
        "fixture must be a genuine compressed nb SSTable"
    );
    assert!(
        reader.index_reader.is_some(),
        "fixture must have a raw-key Index.db"
    );
    assert!(
        reader.bti_partitions_db.is_none(),
        "fixture must be BIG, not BTI"
    );
    let summary_entries = reader
        .summary_reader
        .as_ref()
        .map(|s| s.get_entries().len())
        .unwrap_or(0);
    assert!(
        summary_entries > 0,
        "fixture's Summary.db must carry at least one sample (got {summary_entries}) — \
         the summary-guided walk under test requires a usable Summary.db"
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let real = reader.clone_point_source();
    reader.set_point_source(Arc::new(SleepingReadAt::new(
        real,
        Duration::ZERO,
        calls.clone(),
    )));

    let scan_cancel = ScanCancel::default();
    let mut rows_seen = 0usize;
    reader
        .stream_all_partitions_for_query(
            Some(&schema),
            &scan_cancel,
            None,
            |_row: CompactionRow| {
                rows_seen += 1;
                Ok(ControlFlow::Continue(()))
            },
        )
        .await
        .expect("summary-guided compressed scan walk");

    // Non-vacuity (never let a dataset-dependent assertion pass on zero rows,
    // per CLAUDE.md): the fixture wrote 400 partitions, so the walk must emit
    // them, proving the summary-guided branch — not some fallback — ran.
    assert!(
        rows_seen > 0,
        "the summary-guided walk must have emitted rows; got 0 — the fixture or the \
         summary-guided routing itself is broken, not just the point_source wiring"
    );

    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "the Summary-guided compressed scan walk must not read Data.db through the \
         MADV_RANDOM point_source mapping (issue #2876) — got {} call(s)",
        calls.load(Ordering::Relaxed)
    );
}
