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
/// `point_source` is the wrapped plane: `read_value_at_offset` is the POINT-intent
/// entry point (issue #2876), so a point read must reach the reader's dedicated
/// `MADV_RANDOM` point-read mapping (issue #2210) and this convoy proof belongs on
/// that plane. The genuinely sequential walks reach the never-`MADV_RANDOM` scan plane via the
/// positional helpers, which take their plane from the caller.
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
        let real = reader.clone_point_source();
        reader.set_point_source(Arc::new(SerializingReadAt::new(real, delay)));
        let reader = Arc::new(reader);
        concurrent_point_reads(reader, &offsets, size).await
    };

    // --- Treatment: the migrated positional source runs the 8 reads in parallel. ---
    let calls = Arc::new(AtomicUsize::new(0));
    let treatment = {
        let mut reader = open_reader(&path).await.expect("open treatment reader");
        let real = reader.clone_point_source();
        reader.set_point_source(Arc::new(SleepingReadAt::new(real, delay, calls.clone())));
        let reader = Arc::new(reader);
        concurrent_point_reads(reader, &offsets, size).await
    };

    // Routing proof (deterministic; this is what is RED on `main`, where
    // get_cached_data/verify_uncompressed_range still lock `self.file` and never
    // touch a positional source): the point read path must reach the injected
    // source.
    assert!(
        calls.load(Ordering::Relaxed) >= offsets.len(),
        "point read path must route through `point_source` (>= {} reads); got {} \
         — a 0 here means the point read path no longer uses point_source",
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

/// Number of partitions every Summary-guided fixture below writes. Comfortably
/// over the default `min_index_interval` (128, see
/// `issue_2412_wraparound_scan.rs`'s identical rationale) so `Summary.db` carries
/// multiple samples spanning distinct `Index.db` positions — a single-sample
/// summary would make `stream_all_partitions_for_query`'s summary-guided branch
/// untestable (it requires non-empty `Summary.db` entries). Also the EXACT row
/// count the walk must emit, so a walk that silently emits nothing (or a
/// fallback that emits a different shape) cannot pass.
#[cfg(all(feature = "write-support", feature = "lz4"))]
const SUMMARY_FIXTURE_PARTITIONS: i32 = 400;

/// Build a genuine BIG ("nb") SSTable with a USABLE `Summary.db` (issue #2876):
/// [`SUMMARY_FIXTURE_PARTITIONS`] single-int-PK partitions via the production
/// `WriteEngine` (a valid uncompressed BIG Data.db + Index.db / Summary.db /
/// Statistics.db / CRC.db) and, when `compress` is set, the flushed Data.db
/// LZ4-compressed IN PLACE — the same recipe
/// `issue_1293_compressed_big_reverse_seek.rs` established. CQLite's own write
/// surface never emits compression (issue #1406 claim boundary), so that is the
/// only way to get a genuine compressed-nb fixture without a fetched real-
/// Cassandra dataset (whose small tables don't reliably clear the
/// `min_index_interval` sampling threshold either).
///
/// `compress = false` keeps the writer's UNCOMPRESSED output verbatim —
/// including its `CRC.db`, which is what makes the uncompressed scan's
/// `verify_uncompressed_range` CRC reads observable (issue #2876 Finding 1).
///
/// Compressing in place is sound because the uncompressed-BIG Data.db is
/// HEADERLESS (data starts at byte 0) and `Index.db` offsets are in the
/// uncompressed domain — exactly what the compressed reader assumes
/// (`CompressionInfo` chunk offsets are relative to `Data.db` byte 0), so
/// re-chunking + compressing the bytes in place needs no change to
/// Index.db/Summary.db/Statistics.db.
#[cfg(all(feature = "write-support", feature = "lz4"))]
async fn build_summary_guided_fixture(
    compress: bool,
) -> (
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
    const N: i32 = SUMMARY_FIXTURE_PARTITIONS;
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

    if !compress {
        // Keep the writer's genuine UNCOMPRESSED output (Data.db + CRC.db) as-is:
        // the CRC.db is exactly what makes the uncompressed scan's
        // `verify_uncompressed_range` reads observable (issue #2876, Finding 1).
        return (temp, data_path, schema);
    }

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

/// Drive `stream_all_partitions_for_query` over a Summary-guided fixture with a
/// call-counting spy on BOTH positional planes, and return
/// `(rows_seen, point_reads, scan_reads, partitions_parsed)`.
///
/// Shared by the compressed and uncompressed scan-plane regressions below so the
/// preconditions, the spy installation, and the branch proof are written ONCE.
/// `partitions_parsed` comes from the thread-local
/// [`StreamWalkScope`](crate::storage::sstable::work_counters::stream_walk_scope::StreamWalkScope)
/// (issue #2428): it counts ONLY the per-partition
/// `add_stream_walk_partition_parsed()` increments executed by THIS test's own
/// inline walk, so it is a POSITIVE, pollution-immune proof that
/// `walk_in_range_partition_slices` — the Summary-guided branch, not the
/// full-ring fallback — actually ran.
#[cfg(all(feature = "write-support", feature = "lz4"))]
async fn summary_guided_scan_plane_probe(
    compress: bool,
) -> (usize, usize, usize, u64, tempfile::TempDir) {
    use std::ops::ControlFlow;

    use super::compaction_row::CompactionRow;
    use crate::storage::scan_cancel::ScanCancel;
    use crate::storage::sstable::work_counters::stream_walk_scope::StreamWalkScope;

    let (temp, data_path, schema) = build_summary_guided_fixture(compress).await;
    let mut reader = open_reader(&data_path)
        .await
        .expect("open summary-guided fixture reader");

    // Fixture preconditions (fail LOUD, never silently skip: this fixture is
    // built in-test, not a fetched dataset that can legitimately be absent) —
    // the walk under test requires a raw-key Index.db, no BTI trie, a
    // `Summary.db` with at least one sample, and the requested compression state.
    assert_eq!(
        reader.compression_info.is_some(),
        compress,
        "fixture compression state must match the requested one (compress={compress})"
    );
    // The uncompressed fixture MUST carry the writer's `CRC.db`: the covering-chunk
    // CRC read is the very I/O this scenario proves stays off the point plane
    // (issue #2876, Finding 1). A missing CRC.db would make the whole scenario
    // vacuously green.
    assert_eq!(
        reader.crc_reader.is_some(),
        !compress,
        "uncompressed fixture must carry CRC.db (and the compressed one must not) — \
         the CRC read is the I/O under test"
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

    // Independent spies on the two planes: `point_source` (the dedicated
    // `MADV_RANDOM` point mapping, issue #2210) and `scan_positional_source` (the
    // never-`MADV_RANDOM` scan mapping, issue #2876). They are separate reader fields, so
    // replacing one never redirects the other — even on the Buffered/Direct
    // backends where both start out as clones of the same backing source.
    let point_calls = Arc::new(AtomicUsize::new(0));
    let scan_calls = Arc::new(AtomicUsize::new(0));
    let real_point = reader.clone_point_source();
    reader.set_point_source(Arc::new(SleepingReadAt::new(
        real_point,
        Duration::ZERO,
        point_calls.clone(),
    )));
    let real_scan = reader.clone_scan_positional_source();
    reader.set_scan_positional_source(Arc::new(SleepingReadAt::new(
        real_scan,
        Duration::ZERO,
        scan_calls.clone(),
    )));

    let scan_cancel = ScanCancel::default();
    let mut rows_seen = 0usize;
    // Scope opened on THIS thread around the inline walk (default `#[tokio::test]`
    // drives every `.await` on the test's own thread), so the count is exactly
    // this walk's partition parses.
    let scope = StreamWalkScope::new();
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
        .expect("summary-guided scan walk");
    let partitions_parsed = scope.count();
    drop(scope);

    (
        rows_seen,
        point_calls.load(Ordering::Relaxed),
        scan_calls.load(Ordering::Relaxed),
        partitions_parsed,
        temp,
    )
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
/// Three assertions, none of which a broken walk can satisfy (roborev #2882
/// Finding 3): the walk emits EXACTLY the fixture's partition count, the
/// Summary-guided branch is POSITIVELY proven to have run (that many partition
/// parses recorded on this thread + non-zero reads on the scan plane), and the
/// point plane is untouched.
#[cfg(all(feature = "write-support", feature = "lz4"))]
#[tokio::test]
#[serial_test::serial(work_counters)]
async fn summary_guided_compressed_scan_walk_avoids_point_source() {
    let (rows_seen, point_reads, scan_reads, partitions_parsed, _temp) =
        summary_guided_scan_plane_probe(true).await;

    // Non-vacuity: the fixture wrote exactly this many single-row partitions, so
    // an under-emitting walk (or a fallback with a different emit shape) fails
    // here instead of passing on 0 rows (CLAUDE.md's dataset-test guardrail).
    assert_eq!(
        rows_seen, SUMMARY_FIXTURE_PARTITIONS as usize,
        "the summary-guided walk must emit exactly one row per fixture partition"
    );
    // POSITIVE branch proof: `walk_in_range_partition_slices` bumped its
    // per-partition work probe once per partition. The full-ring fallback bumps a
    // DIFFERENT site (`drain_compaction_window`) once per partition too, so this is
    // paired with the scan-plane read proof below, which the fallback (a cursor
    // walk on `self.file`) cannot satisfy.
    assert_eq!(
        partitions_parsed, SUMMARY_FIXTURE_PARTITIONS as u64,
        "the Summary-guided walk must parse exactly one body per fixture partition"
    );
    assert!(
        scan_reads > 0,
        "the Summary-guided compressed walk must read Data.db through \
         `scan_positional_source`; got 0 reads there — the branch under test did not run"
    );
    assert_eq!(
        point_reads, 0,
        "the Summary-guided compressed scan walk must not read Data.db through the \
         MADV_RANDOM point_source mapping (issue #2876)"
    );
}

/// Regression test (issue #2876, roborev #2882 Finding 1): the Summary-guided
/// UNCOMPRESSED scan walk must ALSO keep every Data.db read off the
/// `MADV_RANDOM` point mapping — including the `CRC.db` covering-chunk reads.
///
/// The uncompressed walk (`walk_in_range_partition_slices` →
/// `read_uncompressed_verified` → `verify_uncompressed_range`) reads each covering
/// `CRC.db` chunk before handing back bytes (issue #1396). Verifying a 64 KiB
/// chunk on the point plane is exactly the readahead-suppressed access the split
/// exists to avoid, so the scan-side verifier must read the scan plane.
///
/// RED before the fix: `verify_uncompressed_range` hardcoded `self.point_source`,
/// so the walk bumps the point spy. GREEN after: the CRC read follows the caller's
/// plane, so the point spy stays at zero while the walk still emits every row.
#[cfg(all(feature = "write-support", feature = "lz4"))]
#[tokio::test]
#[serial_test::serial(work_counters)]
async fn summary_guided_uncompressed_scan_walk_avoids_point_source() {
    let (rows_seen, point_reads, scan_reads, partitions_parsed, _temp) =
        summary_guided_scan_plane_probe(false).await;

    assert_eq!(
        rows_seen, SUMMARY_FIXTURE_PARTITIONS as usize,
        "the summary-guided walk must emit exactly one row per fixture partition"
    );
    assert_eq!(
        partitions_parsed, SUMMARY_FIXTURE_PARTITIONS as u64,
        "the Summary-guided walk must parse exactly one body per fixture partition"
    );
    assert!(
        scan_reads > 0,
        "the Summary-guided uncompressed walk must read Data.db (its CRC.db covering \
         chunks) through `scan_positional_source`; got 0 reads there"
    );
    assert_eq!(
        point_reads, 0,
        "the Summary-guided uncompressed scan walk must not read Data.db through the \
         MADV_RANDOM point_source mapping — including its CRC.db covering-chunk reads \
         (issue #2876, Finding 1)"
    );
}

/// Regression test (issue #2876, roborev #2882 Finding 2): a genuine POINT read
/// must still go through the reader's dedicated `MADV_RANDOM` point mapping.
///
/// The scan-plane split must not sweep the point path along with it: issue #2210
/// gave point lookups an advised mapping precisely because their faults are
/// scattered, and `read_value_at_offset` is the point-intent offset read. So a
/// `read_value_at_offset`
/// must read `point_source` and must NOT touch `scan_positional_source` — on a
/// COMPRESSED reader (whose window comes from `read_compressed_offset_window`) as
/// well as an UNCOMPRESSED one (raw bytes + `CRC.db` verification).
///
/// RED before the fix: the shared helpers hardcoded `scan_positional_source`, so
/// the point read reached the UNADVISED plane — for the compressed reader the
/// point spy stayed at exactly 0.
#[cfg(all(feature = "write-support", feature = "lz4"))]
#[tokio::test]
#[serial_test::serial(work_counters)]
async fn point_offset_read_uses_advised_point_source() {
    // A window comfortably inside the fixture's data section, spanning several
    // small partitions; `read_value_at_offset` returns the raw bytes without
    // parsing, so any in-bounds window exercises the plane routing.
    const OFFSET: u64 = 0;
    const SIZE: u32 = 64;

    for compress in [false, true] {
        let (_temp, data_path, _schema) = build_summary_guided_fixture(compress).await;
        let mut reader = open_reader(&data_path)
            .await
            .expect("open summary-guided fixture reader");
        assert_eq!(
            reader.compression_info.is_some(),
            compress,
            "fixture compression state must match the requested one"
        );

        let point_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let real_point = reader.clone_point_source();
        reader.set_point_source(Arc::new(SleepingReadAt::new(
            real_point,
            Duration::ZERO,
            point_calls.clone(),
        )));
        let real_scan = reader.clone_scan_positional_source();
        reader.set_scan_positional_source(Arc::new(SleepingReadAt::new(
            real_scan,
            Duration::ZERO,
            scan_calls.clone(),
        )));

        let row = reader
            .read_value_at_offset(OFFSET, SIZE)
            .await
            .expect("point offset read")
            .expect("point offset read must return the raw window");
        // Non-vacuity: the read really produced the requested window.
        match &row {
            crate::types::ScanRow::RawRow(bytes) => assert_eq!(
                bytes.len(),
                SIZE as usize,
                "point offset read must return exactly the requested window"
            ),
            other => panic!("expected a RawRow window, got {other:?}"),
        }

        assert!(
            point_calls.load(Ordering::Relaxed) > 0,
            "a point offset read on a {} reader must read Data.db through the advised \
             `point_source` mapping (issue #2210); got 0 reads there",
            if compress {
                "compressed"
            } else {
                "uncompressed"
            }
        );
        assert_eq!(
            scan_calls.load(Ordering::Relaxed),
            0,
            "a point offset read on a {} reader must not read Data.db through the \
             scan-side plane (issue #2876, Finding 2)",
            if compress {
                "compressed"
            } else {
                "uncompressed"
            }
        );
    }
}

/// Build a `V5_0Uncompressed`-CLASSIFIED clustered BIG fixture (issue #3097).
///
/// A `V5_0Uncompressed` reader is what the merge arm's NON-stitching branch
/// (`stream_all_partitions_for_query` → `stream_partitions_summary_guided`)
/// serves — and the ONLY classification where that branch's schema-resolution
/// bug is observable, because a header-derived schema names its clustering
/// column with the placeholder `clustering_key` rather than the caller's real
/// `ck`. A CQLite-written `nb` file normally classifies as `V5_0NewBig`
/// (chunk-stitching, which already honours the caller schema); the reader only
/// takes the `V5_0Uncompressed` path when its headerless Data.db happens to
/// begin with the four bytes of the `V5_0Uncompressed` magic (`00 10 04 5e`)
/// and no `CompressionInfo.db` exists (`reader/header.rs`). We force that by
/// choosing a 16-byte `blob` partition key prefixed `04 5e …` so the first
/// partition's on-disk bytes are `00 10 04 5e` — authoritative on-disk framing,
/// no heuristics (issue #28). Returns the fixture dir, its Data.db path, and
/// the AUTHORITATIVE (caller) schema whose clustering column is named `ck`.
#[cfg(all(feature = "write-support", feature = "lz4"))]
async fn build_uncompressed_classified_clustered_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    crate::schema::TableSchema,
) {
    use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
    use crate::storage::write_engine::{
        CellOperation, ClusteringKey, Durability, Mutation, PartitionKey, TableId, WriteEngine,
        WriteEngineConfig,
    };
    use crate::types::Value;

    const KS: &str = "issue_3097_ks";
    const TBL: &str = "clustered";
    // Comfortably over the default min_index_interval so Summary.db carries
    // multiple samples spanning distinct Index.db positions (see
    // SUMMARY_FIXTURE_PARTITIONS) — the summary-guided branch under test
    // requires a non-empty Summary.db.
    const N: u16 = 400;

    let schema = TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "blob".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            Column {
                name: "pk".into(),
                data_type: "blob".into(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".into(),
                data_type: "int".into(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".into(),
                data_type: "text".into(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    };

    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal"), schema.clone())
        .with_flush_threshold(1usize << 30)
        .with_durability(Durability::Disabled);
    let mut engine = WriteEngine::new(config).expect("engine");
    for i in 0..N {
        // 16-byte key prefixed `04 5e` so the FIRST partition's Data.db bytes
        // are `00 (flags) 10 (key len) 04 5e …` == the V5_0Uncompressed magic.
        let mut key = vec![0x04u8, 0x5e];
        key.extend_from_slice(&[0u8; 12]);
        key.extend_from_slice(&i.to_be_bytes());
        engine
            .write(Mutation::new(
                TableId::new(KS, TBL),
                PartitionKey::single("pk", Value::Blob(key.into())),
                Some(ClusteringKey::single("ck", Value::Integer(i as i32))),
                vec![CellOperation::Write {
                    column: "v".into(),
                    value: Value::text(format!("v{i}")),
                }],
                1_000_000 + i as i64,
                None,
            ))
            .expect("write row");
    }
    engine.flush().await.expect("flush").expect("flush info");
    engine.close().await.expect("close");
    let (data_path, _) =
        locate_data_db_under(&data_dir).expect("no *-Data.db produced under data_dir");
    (temp, data_path, schema)
}

/// Issue #3097: the WARM k-way-merge arm's per-SSTable enumeration
/// (`stream_all_partitions_for_query`, driven by
/// `from_readers::drive_query_stream`) must decode with the CALLER's
/// authoritative schema, so a clustered `V5_0Uncompressed` reader surfaces its
/// clustering column under the caller's real name (`ck`) — not the header
/// schema's placeholder `clustering_key`, which reached the merger as a NULL
/// `ck` for a projected `SELECT`.
///
/// Pins merge-arm-vs-caller-schema equality: over the SAME bytes, the
/// merge-arm surface with `Some(caller_schema)` must yield the clustering
/// column keyed `ck`; with `None` (no caller schema) it falls back to the
/// reader-derived placeholder — proving the parameter, not the reader header,
/// is what governs the name. RED before this change (the branch hard-coded
/// `None`), GREEN after.
#[cfg(all(feature = "write-support", feature = "lz4"))]
#[tokio::test]
async fn merge_arm_query_stream_honours_caller_schema_clustering() {
    use std::collections::BTreeSet;
    use std::ops::ControlFlow;

    use super::compaction_row::{CompactionRow, CompactionRowData};
    use crate::storage::scan_cancel::ScanCancel;

    let (_temp, data_path, schema) = build_uncompressed_classified_clustered_fixture().await;
    let reader = open_reader(&data_path).await.expect("open fixture reader");

    // Precondition: this MUST be the non-stitching V5_0Uncompressed branch —
    // the only place the bug is observable. If a writer change ever makes the
    // fixture classify as V5_0NewBig (chunk-stitching), this pin would pass
    // vacuously via the already-schema-honouring compaction decoder, so fail
    // LOUD instead of skipping.
    assert!(
        !reader.requires_chunk_stitching(),
        "fixture must classify as the non-stitching V5_0Uncompressed branch \
         (the only arm where the #3097 schema-resolution bug is observable)"
    );
    assert!(
        reader.compression_info.is_none() && reader.bti_partitions_db.is_none(),
        "fixture must be an uncompressed BIG reader (no CompressionInfo.db, no BTI)"
    );
    assert!(
        reader
            .summary_reader
            .as_ref()
            .map(|s| !s.get_entries().is_empty())
            .unwrap_or(false),
        "fixture's Summary.db must carry samples — the summary-guided merge arm \
         under test requires it"
    );

    // Drive the SAME merge-arm surface both ways over the SAME bytes.
    async fn columns_seen(
        reader: &SSTableReader,
        schema: Option<&crate::schema::TableSchema>,
    ) -> (usize, BTreeSet<String>) {
        let cancel = ScanCancel::default();
        let mut rows = 0usize;
        let mut cols = BTreeSet::new();
        reader
            .stream_all_partitions_for_query(schema, &cancel, None, |r: CompactionRow| {
                rows += 1;
                if let CompactionRowData::Live { simple, .. } = &r.row_data {
                    for c in simple {
                        cols.insert(c.column.clone());
                    }
                }
                Ok(ControlFlow::Continue(()))
            })
            .await
            .expect("summary-guided merge-arm query stream");
        (rows, cols)
    }

    let (rows_with, cols_with) = columns_seen(&reader, Some(&schema)).await;
    let (rows_none, cols_none) = columns_seen(&reader, None).await;

    assert_eq!(
        rows_with, 400,
        "the merge arm must emit exactly one row per fixture partition — a \
         zero/short result is a failure, not a vacuous pass"
    );
    assert_eq!(
        rows_none, 400,
        "the None-schema walk must emit every row too"
    );

    // The FIX: with the caller schema, the clustering column carries the
    // caller's real name.
    assert!(
        cols_with.contains("ck"),
        "the merge arm must decode the clustering column under the caller's \
         authoritative name `ck` (issue #3097); got {cols_with:?}"
    );
    assert!(
        !cols_with.contains("clustering_key"),
        "the merge arm must NOT surface the header schema's placeholder \
         `clustering_key` when the caller supplied a real schema; got {cols_with:?}"
    );

    // Fallback preserved: with NO caller schema the reader-derived header
    // schema (placeholder-named) still governs — proving the caller-schema
    // PARAMETER is what changed the outcome, not the fixture bytes.
    assert!(
        cols_none.contains("clustering_key") && !cols_none.contains("ck"),
        "with no caller schema the reader-derived placeholder must still apply \
         (fallback unchanged); got {cols_none:?}"
    );
}

/// Issue #3097 (roborev round 2 blocker): the WARM merge arm's FALLBACK —
/// taken when `Summary.db` is absent/unusable, so the summary-guided walk never
/// fires and enumeration delegates to `stream_all_partitions_cancellable`'s
/// full-index/sequential decode — must ALSO honour the caller's authoritative
/// schema. The prior fix only threaded the schema through the summary-guided
/// branch, so this path still resolved the decode schema from the reader alone
/// and surfaced a clustered `V5_0Uncompressed` reader's clustering column as the
/// placeholder `clustering_key` (NULL `ck` to a projected `SELECT`).
///
/// Forces the fallback by DELETING the fixture's `Summary.db` before open, then
/// pins that `stream_all_partitions_for_query(Some(schema), …)` yields the
/// clustering column under the caller's real name `ck` — and with `None` still
/// falls back to the reader-derived placeholder, proving the parameter (not the
/// bytes) governs. RED before this change (fallback hard-coded reader-derived),
/// GREEN after.
#[cfg(all(feature = "write-support", feature = "lz4"))]
#[tokio::test]
async fn merge_arm_query_fallback_no_summary_honours_caller_schema_clustering() {
    use std::collections::BTreeSet;
    use std::ops::ControlFlow;

    use super::compaction_row::{CompactionRow, CompactionRowData};
    use crate::storage::scan_cancel::ScanCancel;

    let (_temp, data_path, schema) = build_uncompressed_classified_clustered_fixture().await;

    // Force the FALLBACK path: remove the `Summary.db` sibling so the reader opens
    // without a usable summary and `stream_all_partitions_for_query` skips the
    // summary-guided walk entirely, delegating to the full-index/sequential
    // fallback (`stream_all_partitions_cancellable`) — the path this test covers.
    let summary_path = data_path.with_file_name(
        data_path
            .file_name()
            .and_then(|f| f.to_str())
            .expect("data file name")
            .replace("-Data.db", "-Summary.db"),
    );
    if summary_path.exists() {
        std::fs::remove_file(&summary_path).expect("remove Summary.db to force fallback");
    }

    let reader = open_reader(&data_path).await.expect("open fixture reader");

    // Preconditions: the non-stitching V5_0Uncompressed branch (the only arm the
    // bug is observable on) AND no usable Summary.db (so the summary-guided walk
    // does NOT run — this exercises the fallback, not the already-fixed branch).
    assert!(
        !reader.requires_chunk_stitching(),
        "fixture must classify as the non-stitching V5_0Uncompressed branch"
    );
    assert!(
        reader.compression_info.is_none() && reader.bti_partitions_db.is_none(),
        "fixture must be an uncompressed BIG reader (no CompressionInfo.db, no BTI)"
    );
    assert!(
        reader
            .summary_reader
            .as_ref()
            .map(|s| s.get_entries().is_empty())
            .unwrap_or(true),
        "Summary.db must be absent/empty so the summary-guided walk is skipped and \
         the query arm takes the full-index/sequential FALLBACK under test"
    );

    async fn columns_seen(
        reader: &SSTableReader,
        schema: Option<&crate::schema::TableSchema>,
    ) -> (usize, BTreeSet<String>) {
        let cancel = ScanCancel::default();
        let mut rows = 0usize;
        let mut cols = BTreeSet::new();
        reader
            .stream_all_partitions_for_query(schema, &cancel, None, |r: CompactionRow| {
                rows += 1;
                if let CompactionRowData::Live { simple, .. } = &r.row_data {
                    for c in simple {
                        cols.insert(c.column.clone());
                    }
                }
                Ok(ControlFlow::Continue(()))
            })
            .await
            .expect("fallback merge-arm query stream");
        (rows, cols)
    }

    let (rows_with, cols_with) = columns_seen(&reader, Some(&schema)).await;
    let (rows_none, cols_none) = columns_seen(&reader, None).await;

    assert_eq!(
        rows_with, 400,
        "the fallback must emit exactly one row per fixture partition — a \
         zero/short result is a failure, not a vacuous pass"
    );
    assert_eq!(
        rows_none, 400,
        "the None-schema fallback must emit every row too"
    );

    // The FIX: the caller schema wins through the fallback too.
    assert!(
        cols_with.contains("ck"),
        "the fallback must decode the clustering column under the caller's \
         authoritative name `ck` (issue #3097 round 2); got {cols_with:?}"
    );
    assert!(
        !cols_with.contains("clustering_key"),
        "the fallback must NOT surface the placeholder `clustering_key` when the \
         caller supplied a real schema; got {cols_with:?}"
    );

    // Fallback-of-the-fallback preserved: with NO caller schema the reader-derived
    // placeholder still governs, proving the PARAMETER changed the outcome.
    assert!(
        cols_none.contains("clustering_key") && !cols_none.contains("ck"),
        "with no caller schema the reader-derived placeholder must still apply; \
         got {cols_none:?}"
    );
}

/// Issue #3097: the WARM merge arm's DEGENERATE fallback — taken when BOTH
/// `Summary.db` AND `Index.db` are absent, so
/// `stream_all_partitions_cancellable` finds `index_reader.is_none()`, skips the
/// full-index route entirely, and delegates to the MATERIALISING `sequential_scan`
/// of Data.db — must ALSO honour the caller's authoritative schema. The sibling
/// `merge_arm_query_fallback_no_summary_honours_caller_schema_clustering` removes
/// only `Summary.db`, so it exercises the FULL-INDEX fallback branch (Index.db
/// still present); this case removes Index.db too, forcing the sequential-scan
/// branch — the `sequential_scan(&table_id, …, caller_schema.or(self.schema), …)`
/// seam the #3097 fix threads the caller schema through.
///
/// Pins that `stream_all_partitions_for_query(Some(schema), …)` surfaces the
/// clustering column under the caller's real name `ck` through the sequential
/// scan, and with `None` still falls back to the reader-derived placeholder —
/// proving the parameter (not the bytes) governs. Would FAIL if the sequential
/// branch resolved the decode schema from the reader alone (pre-#3097).
#[cfg(all(feature = "write-support", feature = "lz4"))]
#[tokio::test]
async fn merge_arm_query_sequential_fallback_no_summary_no_index_honours_caller_schema_clustering()
{
    use std::collections::BTreeSet;
    use std::ops::ControlFlow;

    use super::compaction_row::{CompactionRow, CompactionRowData};
    use crate::storage::scan_cancel::ScanCancel;

    let (_temp, data_path, schema) = build_uncompressed_classified_clustered_fixture().await;

    // Force the SEQUENTIAL-SCAN fallback: remove BOTH the `Summary.db` (so the
    // summary-guided walk never fires) AND the `Index.db` (so
    // `stream_all_partitions_cancellable` finds `index_reader.is_none()` and skips
    // the full-index route, delegating straight to `sequential_scan`).
    for suffix in ["-Summary.db", "-Index.db"] {
        let sibling = data_path.with_file_name(
            data_path
                .file_name()
                .and_then(|f| f.to_str())
                .expect("data file name")
                .replace("-Data.db", suffix),
        );
        if sibling.exists() {
            std::fs::remove_file(&sibling)
                .unwrap_or_else(|e| panic!("remove {suffix} to force sequential fallback: {e}"));
        }
    }

    let reader = open_reader(&data_path)
        .await
        .expect("open fixture reader without Summary.db/Index.db");

    // Preconditions: the non-stitching V5_0Uncompressed branch, no usable
    // Summary.db, AND no Index.db — so the query arm skips both the summary-guided
    // walk and the full-index fallback, genuinely exercising the sequential-scan
    // route this test covers (fail LOUD if any precondition regresses, so the pin
    // can never pass vacuously via a different route).
    assert!(
        !reader.requires_chunk_stitching(),
        "fixture must classify as the non-stitching V5_0Uncompressed branch"
    );
    assert!(
        reader.compression_info.is_none() && reader.bti_partitions_db.is_none(),
        "fixture must be an uncompressed BIG reader (no CompressionInfo.db, no BTI)"
    );
    assert!(
        reader
            .summary_reader
            .as_ref()
            .map(|s| s.get_entries().is_empty())
            .unwrap_or(true),
        "Summary.db must be absent/empty so the summary-guided walk is skipped"
    );
    assert!(
        reader.index_reader.is_none(),
        "Index.db must be absent so `stream_all_partitions_cancellable` skips the \
         full-index route and takes the SEQUENTIAL-SCAN fallback under test"
    );

    async fn columns_seen(
        reader: &SSTableReader,
        schema: Option<&crate::schema::TableSchema>,
    ) -> (usize, BTreeSet<String>) {
        let cancel = ScanCancel::default();
        let mut rows = 0usize;
        let mut cols = BTreeSet::new();
        reader
            .stream_all_partitions_for_query(schema, &cancel, None, |r: CompactionRow| {
                rows += 1;
                if let CompactionRowData::Live { simple, .. } = &r.row_data {
                    for c in simple {
                        cols.insert(c.column.clone());
                    }
                }
                Ok(ControlFlow::Continue(()))
            })
            .await
            .expect("sequential-scan fallback merge-arm query stream");
        (rows, cols)
    }

    let (rows_with, cols_with) = columns_seen(&reader, Some(&schema)).await;
    let (rows_none, cols_none) = columns_seen(&reader, None).await;

    assert_eq!(
        rows_with, 400,
        "the sequential fallback must emit exactly one row per fixture partition — \
         a zero/short result is a failure, not a vacuous pass"
    );
    assert_eq!(
        rows_none, 400,
        "the None-schema sequential fallback must emit every row too"
    );

    // The FIX: the caller schema wins through the sequential scan too.
    assert!(
        cols_with.contains("ck"),
        "the sequential fallback must decode the clustering column under the \
         caller's authoritative name `ck` (issue #3097); got {cols_with:?}"
    );
    assert!(
        !cols_with.contains("clustering_key"),
        "the sequential fallback must NOT surface the placeholder `clustering_key` \
         when the caller supplied a real schema; got {cols_with:?}"
    );

    // Fallback preserved: with NO caller schema the reader-derived placeholder
    // still governs, proving the PARAMETER changed the outcome, not the bytes.
    assert!(
        cols_none.contains("clustering_key") && !cols_none.contains("ck"),
        "with no caller schema the reader-derived placeholder must still apply; \
         got {cols_none:?}"
    );
}
