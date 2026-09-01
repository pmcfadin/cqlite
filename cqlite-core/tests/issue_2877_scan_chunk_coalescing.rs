//! Issue #2877 — TDD pin: the Summary-guided compressed scan walk
//! (`walk_in_range_partition_slices`, `summary_scan/mod.rs`) must decompress each
//! covering `CompressionInfo.db` chunk ONCE across a full-table scan, never once
//! per contained partition.
//!
//! `#![cfg(all(feature = "write-support", feature = "lz4"))]`: this file drives
//! `SSTableWriter` / `WriteEngine` mutations to build fixtures (gated behind
//! `write-support`, mirroring `issue_1495_arrow_accessor_parity`'s `arrow`-gating
//! convention) AND repacks them through `create_compressor(Lz4)`, which returns
//! `Err("feature 'lz4' required")` when `lz4` is off. BOTH gates therefore matter,
//! and they must match the target's `required-features` in `Cargo.toml` exactly —
//! declaring only `write-support` there let `cargo test --no-default-features
//! --features write-support` run this file and panic on that `Err` (roborev round
//! 2, blocker C).
//!
//! ## The bug being pinned
//!
//! Pre-fix, the walk called `read_compressed_offset_window` once PER PARTITION,
//! and that helper maps a byte range onto chunks and decompresses every chunk it
//! touches with no cross-call memoisation. A 16-64 KiB uncompressed chunk holds
//! many narrow partitions, so the SAME chunk was read + LZ4-decompressed once for
//! EVERY partition inside it — the Epic B decompressed-chunk cache is bypassed
//! entirely on this path (a distinct defect from the cache itself; see the issue).
//!
//! The fix (option 2 in the issue, the stated primary) coalesces consecutive
//! in-range partitions into one chunk-aligned windowed read — precedent already
//! in-tree: `SEQUENTIAL_WINDOW_TARGET_BYTES` (`full_index_stream.rs`) does the
//! analogous thing for the uncompressed non-stitching walk.
//!
//! ## A second bug the coalescing window itself introduced (roborev, High)
//!
//! A naive window that always "resumes exactly where the previous window
//! ended" is wrong the moment a partition STRADDLES that boundary (its `start`
//! precedes the new window's start, underflowing the slice arithmetic), or the
//! walk SKIPS a run of out-of-range partitions without reading them (e.g. the
//! gap between a compressed WRAPAROUND range's two disjoint segments) and the
//! next read `start` lands nowhere near the stale window (a false
//! short-window corruption error). [`large_multi_window_scan_serves_every_partition_correctly`]
//! and [`wraparound_compressed_scan_reads_both_disjoint_segments`] pin both
//! shapes; both fail (panic / spurious `Err`) against the naive
//! "always-resume-at-the-old-end" window and pass against the
//! append-on-straddle / realign-on-gap fix.
//!
//! ## Oracle
//!
//! The production write surface emits UNCOMPRESSED SSTables only (issue #1406),
//! so this file hand-builds genuinely LZ4-compressed fixtures via the
//! fixture-synthesis building blocks `CompressedDataWriter`/`CompressionInfoWriter`
//! — the same technique `compaction_cancel_tests.rs`'s `compressed_fixture` uses —
//! but WITHOUT stripping `Summary.db`/`Index.db` (this walk requires both present).
//! `metadata.chunk_count()`, recorded at fixture-build time, is the
//! reader-independent ground truth for "how many chunks exist"; the
//! process-global `SSTableReader::decompress_call_count()` delta across one full
//! scan is asserted to equal EXACTLY that — not the (much larger) partition
//! count, and (for the multi-window fixture) not inflated by a boundary chunk
//! getting decompressed twice. The counter is reliable here because each
//! `tests/` file is its own process and every test below is `#[serial]` (mirrors
//! `decompressed_chunk_cache_tests.rs`).

#![cfg(all(feature = "write-support", feature = "lz4"))]

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::{SSTableReader, ScanTokenBound};
use cqlite_core::storage::sstable::writer::{
    create_compressor, CompressedDataWriter, CompressionAlgorithm, CompressionInfoWriter,
    SSTableWriter,
};
use cqlite_core::storage::write_engine::{CellOperation, Mutation, PartitionKey, TableId};
use cqlite_core::types::Value;
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::{Config, Platform};
use tempfile::TempDir;

const KS: &str = "coalesce_ks";
const TBL: &str = "items";
/// Partition count: modest, but at `REPACK_CHUNK_SIZE` below it comfortably packs
/// many partitions per chunk while still spanning several chunks — both are
/// needed to distinguish "once per chunk" from "once per partition" (#2877).
const N: i32 = 300;
/// Deliberately small uncompressed chunk size so `N` moderate-sized partitions
/// span many chunks with several partitions landing in each (mirrors
/// `compaction_cancel_tests.rs`'s `compressed_fixture` convention, tuned smaller
/// here to keep the multi-partition-per-chunk property comfortably non-vacuous).
const REPACK_CHUNK_SIZE: usize = 2048;
/// Payload padding per partition: long enough that ~N partitions span many
/// `REPACK_CHUNK_SIZE` chunks, short enough to keep the fixture tiny and fast.
const PAYLOAD: &str = "cqlite-2877-chunk-coalescing-payload-0123456789";

/// `COMPRESSED_SCAN_WINDOW_TARGET_BYTES` in
/// `summary_scan/compressed_scan_window.rs` (private to that module, so mirrored
/// here as a test-side constant): the multi-window fixture below deliberately
/// exceeds `2x` this so the walk MUST refill at least twice, exercising the
/// straddle/append path, not just the initial fill — AND (roborev round 2) it is
/// the RAMP's steady-state ceiling, never the size of a fresh fill.
const WINDOW_TARGET_BYTES: usize = 4 * 1024 * 1024;

/// Shared shape of the MULTI-WINDOW fixture (the straddle/gap/ramp tests below):
/// ~20 KiB per partition * 440 partitions ~= 8.8 MiB uncompressed — comfortably
/// over TWO window targets — packed into 64 KiB chunks, so the window boundary
/// falls INSIDE a partition (20 KiB does not divide the 4 MiB target evenly) and
/// the data section spans well over a hundred chunks.
const N_LARGE: i32 = 440;
const LARGE_MIN_PAYLOAD_LEN: usize = 20 * 1024;
const LARGE_REPACK_CHUNK_SIZE: usize = 64 * 1024;
/// Chunks a FLAT (un-ramped) 4 MiB greedy first fill would touch at
/// `LARGE_REPACK_CHUNK_SIZE`: `4 MiB / 64 KiB`. The ramp's whole point is that a
/// fresh fill costs ~ONE chunk instead of this many.
const FLAT_FILL_CHUNKS: u64 = (WINDOW_TARGET_BYTES / LARGE_REPACK_CHUNK_SIZE) as u64;

/// Lower bound on the coalescing window's REFILL count for a full scan of a
/// `data_section_len`-byte compressed data section packed into
/// `repack_chunk_size` chunks — i.e. the wiring evidence these tests need.
///
/// Why a POSITIVE bound is required, not just the ceilings the rest of this file
/// asserts. Every other oracle here is an UPPER bound (`decompress_calls ==
/// chunk_count`, `refills <= 16`, `compactions <= refills`), and upper bounds are
/// all satisfied by a window that does no coalescing WALK at all — one giant fill
/// covering the whole data section still decompresses each chunk exactly once,
/// takes 1 refill, and compacts 0 times, so `compactions <= refills` degenerates
/// to `0 <= 1`. That shape is a real regression (it destroys the ramp: bounded
/// read-ahead, early termination, and token pushdown all collapse) yet it passes
/// every ceiling. `refills >= this` is the assert that pins the STREAMING
/// tiling — the window must refill as the scan advances, which is only possible if
/// the coalescing walk under test actually ran (CQLite's wiring-evidence rule: the
/// public surface must demonstrably exercise the feature).
///
/// Verified by mutation (not assumed): replacing the ramped floor with
/// `data_section_end` (one whole-section fill) leaves
/// `full_scan_decompresses_each_chunk_once_not_once_per_partition` GREEN and is
/// caught only by the refill floors below.
///
/// A total BYPASS — `stream_all_partitions_for_query` routing to
/// `stream_all_partitions_for_compaction` when the Summary/Index pair is unusable
/// — is caught by the existing `decompress_calls == chunk_count` asserts instead,
/// because the stitch path decompresses via the deliberately UNCOUNTED
/// `ChunkSource::decompress_only` (`decompress_calls` observes 0, verified by
/// forcing that branch). The refill floor catches it too, so both a bypass and a
/// degenerate window now fail loudly.
///
/// Derivation (an upper bound on bytes-per-refill inverted): the window tiles
/// `[0, data_section_len)` with no gaps and no overlaps, and one refill reads
/// `max(this partition's span, ramped floor)` rounded UP to a chunk boundary. The
/// floor is capped at `WINDOW_TARGET_BYTES` and every partition in these fixtures
/// is orders of magnitude smaller than that, so no refill can cover more than
/// `WINDOW_TARGET_BYTES + repack_chunk_size` bytes. Hence at least
/// `ceil(data_section_len / (WINDOW_TARGET_BYTES + repack_chunk_size))` refills.
/// Deliberately conservative (the RAMP means real runs need more — the ~8.8 MiB
/// fixture below takes ~8), so a fixture retune cannot make it flaky, only
/// weaker.
fn min_refills_for_full_scan(data_section_len: usize, repack_chunk_size: usize) -> u64 {
    (data_section_len as u64).div_ceil((WINDOW_TARGET_BYTES + repack_chunk_size) as u64)
}

fn schema() -> TableSchema {
    TableSchema {
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
                name: "payload".to_string(),
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

fn key_bytes(id: i32) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

/// A single-row mutation whose `payload` text is at least `min_len` bytes
/// (padded with `x`), so the fixture's per-partition on-disk span is
/// controllable.
fn mutation_sized(id: i32, min_len: usize) -> Mutation {
    let mut value = format!("{PAYLOAD}-{id}-");
    while value.len() < min_len {
        value.push('x');
    }
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::text(value),
        }],
        1_000_000 + id as i64,
        None,
    )
}

/// Write `n` single-row partitions (payload padded to at least `min_len`
/// bytes) to a fresh uncompressed SSTable, keeping EVERY emitted component
/// (`Summary.db`/`Index.db`/`Filter.db` included — the Summary-guided walk
/// under test requires both `Summary.db` and `Index.db` present, unlike
/// `compaction_cancel_tests.rs`'s index-less sibling). Returns the temp dir
/// (keep alive) and the `Data.db` path.
async fn write_fixture(n: i32, min_len: usize) -> (TempDir, PathBuf) {
    let sch = schema();
    let temp = TempDir::new().unwrap();
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &sch).unwrap();

    // The writer enforces ascending token order.
    let mut keyed: Vec<_> = (1..=n)
        .map(|id| {
            let m = mutation_sized(id, min_len);
            let key = m.decorated_key(&sch).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    let info = writer.finish().await.unwrap();
    let data_path = info.data_path.clone();
    (temp, data_path)
}

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

/// Repack `write_fixture`'s uncompressed `Data.db` into a genuinely LZ4-compressed
/// stream via the fixture-synthesis building blocks (issue #1406: the production
/// write surface never does this), keeping `Summary.db`/`Index.db`/`Filter.db`
/// intact — their recorded offsets/samples are all in the UNCOMPRESSED
/// data-section domain, unaffected by how that data section is later packed into
/// chunks. Returns the temp dir, the `Data.db` path, the chunk count, and the
/// uncompressed data-section length recorded at build time (the ground-truth
/// oracle for "how many chunks exist" / "how much data exists").
async fn compressed_fixture_with_index(
    n: i32,
    min_len: usize,
    repack_chunk_size: usize,
) -> (TempDir, PathBuf, usize, usize) {
    let (temp, data_path) = write_fixture(n, min_len).await;

    // Read the raw (uncompressed) data section back exactly as the reader would
    // see it — skip past `calculate_header_size()` bytes (0 for headerless 'nb').
    let header_size = open_reader(&data_path).await.calculate_header_size();
    let raw = std::fs::read(&data_path).unwrap();
    let data_section = &raw[header_size..];
    let data_section_len = data_section.len();

    let compressor = create_compressor(CompressionAlgorithm::Lz4).unwrap();
    let mut writer = CompressedDataWriter::with_chunk_size(compressor, repack_chunk_size);
    writer.write(data_section).unwrap();
    let (compressed, metadata) = writer.finish().unwrap();
    let chunk_count = metadata.chunk_count();

    // Overwrite Data.db with the compressed chunk stream (headerless 'nb' has no
    // prefix to preserve) and write the matching CompressionInfo.db sidecar so
    // `SSTableReader::open` picks up a real `compression_reader`.
    std::fs::write(&data_path, &compressed).unwrap();
    let base = data_path
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .trim_end_matches("-Data");
    let compression_info_path = data_path
        .parent()
        .unwrap()
        .join(format!("{base}-CompressionInfo.db"));
    CompressionInfoWriter::new(compression_info_path.clone())
        .write(&metadata)
        .unwrap();
    assert!(
        compression_info_path.exists(),
        "CompressionInfo.db must be written so the reader takes the compressed path"
    );

    // A stale CRC.db (checksums the ORIGINAL uncompressed bytes) is now
    // meaningless — remove it for hygiene (mirrors compaction_cancel_tests.rs).
    for entry in std::fs::read_dir(temp.path()).unwrap().flatten() {
        if entry.file_name().to_string_lossy().ends_with("-CRC.db") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    (temp, data_path, chunk_count, data_section_len)
}

/// The acceptance criterion (issue #2877): a full-table compressed scan through
/// the PUBLIC Summary-guided query surface (`stream_all_partitions_for_query`,
/// the same call chain the Flight warm `do_get` path drives) decompresses each
/// covering chunk EXACTLY ONCE — not once per contained partition. Pre-fix this
/// fails with `decompress_call_count() == N` (300); post-fix it must equal the
/// fixture's `chunk_count` (a small fraction of `N`).
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn full_scan_decompresses_each_chunk_once_not_once_per_partition() {
    let (_temp, data_path, chunk_count, data_section_len) =
        compressed_fixture_with_index(N, 0, REPACK_CHUNK_SIZE).await;
    assert!(
        chunk_count > 4 && (chunk_count as i32) < N / 2,
        "fixture must pack MANY partitions per chunk (chunk_count {chunk_count} \
         must be > 4 and well under N={N}) for the once-per-chunk-vs-once-per-\
         partition distinction to be meaningful"
    );
    let reader = open_reader(&data_path).await;
    let sch = schema();

    SSTableReader::reset_decompress_calls();
    SSTableReader::reset_scan_window_counters();
    let cancel = ScanCancel::new();
    let mut rows = 0usize;
    reader
        .stream_all_partitions_for_query(Some(&sch), &cancel, None, |_row| {
            rows += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .expect("summary-guided compressed scan must succeed");
    let decompress_calls = SSTableReader::decompress_call_count();
    let refills = SSTableReader::scan_window_refill_count();

    assert_eq!(
        rows, N as usize,
        "the scan must decode every partition exactly once"
    );
    // WIRING EVIDENCE (see `min_refills_for_full_scan`): the chunk-count assert
    // below is an UPPER bound, so it cannot distinguish "the coalescing window
    // served this scan" from "the window was bypassed entirely". This fixture is
    // one window's worth of data, so the floor is the minimum meaningful one: the
    // window must have been entered at least once.
    let min_refills = min_refills_for_full_scan(data_section_len, REPACK_CHUNK_SIZE).max(1);
    assert!(
        refills >= min_refills,
        "the scan must have routed through the coalescing window: expected >= \
         {min_refills} refills for a {data_section_len}-byte data section in \
         {REPACK_CHUNK_SIZE}-byte chunks (ceil(len / (4 MiB + chunk)), floored at \
         1), got {refills} — 0 means the Summary-guided walk never entered the \
         window (e.g. it FELL BACK to stream_all_partitions_for_compaction) and \
         this test's chunk-count assert proved nothing (issue #2877 wiring \
         evidence)"
    );
    assert_eq!(
        decompress_calls, chunk_count as u64,
        "each covering chunk must be decompressed EXACTLY ONCE across the whole \
         scan, not once per contained partition (issue #2877): {N} partitions \
         packed into {chunk_count} chunks would cost {N} decompress calls \
         pre-fix vs {chunk_count} post-fix (got {decompress_calls})"
    );
}

/// Issue #2877 roborev finding (High): a coalescing window that always
/// "resumes exactly where the previous window ended" breaks the moment a
/// partition STRADDLES that boundary — a fixture entirely within one window
/// (like the test above) structurally cannot exercise a refill at all. This
/// fixture is sized to exceed TWO window targets, with per-partition spans
/// chosen so the window boundary is virtually certain to fall INSIDE some
/// partition (not on a partition edge), forcing the straddle/append path.
///
/// Against the naive pre-fix window this panics (`start - self.start`
/// underflow) or returns a spurious short-window `Error::Corruption` — this is
/// the red half of the TDD pin. Against the fix it must decode every
/// partition, in order, with each chunk decompressed exactly once (no
/// boundary chunk double-counted by an append that re-fetched bytes the
/// window already had).
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn large_multi_window_scan_serves_every_partition_correctly() {
    let (_temp, data_path, chunk_count, data_section_len) =
        compressed_fixture_with_index(N_LARGE, LARGE_MIN_PAYLOAD_LEN, LARGE_REPACK_CHUNK_SIZE)
            .await;
    assert!(
        data_section_len > 2 * WINDOW_TARGET_BYTES,
        "fixture must exceed 2x the window target ({} bytes) to force at least \
         one refill, got {data_section_len} bytes",
        2 * WINDOW_TARGET_BYTES
    );
    assert!(
        chunk_count > 10,
        "fixture must span many chunks, got {chunk_count}"
    );

    let reader = open_reader(&data_path).await;
    let sch = schema();

    SSTableReader::reset_decompress_calls();
    SSTableReader::reset_scan_window_counters();
    let cancel = ScanCancel::new();
    let mut ids = Vec::new();
    reader
        .stream_all_partitions_for_query(Some(&sch), &cancel, None, |row| {
            let key_bytes: &[u8] = &row.key.0;
            ids.push(i32::from_be_bytes(
                key_bytes.try_into().expect("4-byte int PK"),
            ));
            Ok(ControlFlow::Continue(()))
        })
        .await
        .expect(
            "a multi-window compressed scan must succeed — a panic or Err here \
             (issue #2877 roborev) means the window refill lost track of a \
             partition straddling a window boundary",
        );
    let decompress_calls = SSTableReader::decompress_call_count();
    let refills = SSTableReader::scan_window_refill_count();

    ids.sort_unstable();
    let expected: Vec<i32> = (1..=N_LARGE).collect();
    assert_eq!(
        ids, expected,
        "every partition must be decoded exactly once, in the correct order — a \
         miscomputed window offset would corrupt or drop entries here"
    );
    assert_eq!(
        decompress_calls, chunk_count as u64,
        "each chunk must be decompressed EXACTLY once even across MULTIPLE window \
         refills (a boundary-straddling partition's append must fetch only the \
         NEW bytes beyond what the window already buffered, never re-fetch the \
         chunk it just appended past): expected {chunk_count}, got {decompress_calls}"
    );
    // WIRING EVIDENCE (see `min_refills_for_full_scan`): every other assert here is
    // an UPPER bound, so all of them are satisfied by a single whole-data-section
    // fill that never STREAMS — i.e. the straddle/append path this test exists to
    // pin would never execute and the test would still pass (verified by mutation).
    // This fixture exceeds 2x the window target, so it must refill at least twice.
    let min_refills = min_refills_for_full_scan(data_section_len, LARGE_REPACK_CHUNK_SIZE).max(2);
    assert!(
        refills >= min_refills,
        "the multi-window scan must have routed through the coalescing window at \
         least {min_refills} times: {data_section_len} bytes / (4 MiB + \
         {LARGE_REPACK_CHUNK_SIZE}-byte chunk) rounded up, floored at 2 (the \
         fixture exceeds 2x the window target), got {refills} — 1 means one giant \
         non-streaming fill and 0 means the window was bypassed; either way the \
         straddle/append path under test never ran (issue #2877 wiring evidence)"
    );
    // Steady-state proof (issue #2877 roborev blocker A): the RAMP must not turn a
    // long scan into a per-chunk read walk. Doubling from one 64 KiB chunk reaches
    // the 4 MiB steady state in 7 refills (64+128+...+4096 KiB ~= 7.9 MiB of the
    // ~8.8 MiB fixture), so the whole scan costs a handful of refills — vs
    // `chunk_count` (>130) if the window never grew.
    const MAX_STEADY_STATE_REFILLS: u64 = 16;
    assert!(
        refills <= MAX_STEADY_STATE_REFILLS,
        "the window must RAMP to its 4 MiB steady state: covering {chunk_count} \
         chunks took {refills} refills, but a ramping window needs <= \
         {MAX_STEADY_STATE_REFILLS} (a per-chunk refill walk would need \
         ~{chunk_count})"
    );
}

/// Issue #2877 roborev round 2 (blocker A): a FRESH fill must not unconditionally
/// read + LZ4-decompress the full 4 MiB steady-state target. A `decode` closure
/// that returns `ControlFlow::Break` after the FIRST row terminates the walk
/// immediately, so the scan's whole I/O + decompression cost must be ~ONE
/// `chunk_length` — the ramp's first step. A flat 4 MiB greedy floor instead reads
/// and decompresses `4 MiB / 64 KiB` = 64 chunks of mostly never-visited partition
/// bodies, undercutting the walk's own early termination.
///
/// RED against the flat-floor code (`decompress_calls == 64`, `refills == 1` with a
/// 4 MiB window); GREEN against the ramp (`decompress_calls == 1`).
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn early_terminating_decode_pays_about_one_chunk_not_the_full_target() {
    let (_temp, data_path, chunk_count, data_section_len) =
        compressed_fixture_with_index(N_LARGE, LARGE_MIN_PAYLOAD_LEN, LARGE_REPACK_CHUNK_SIZE)
            .await;
    assert!(
        data_section_len > 2 * WINDOW_TARGET_BYTES && (chunk_count as u64) > FLAT_FILL_CHUNKS,
        "fixture must hold MANY more chunks ({chunk_count}) than a flat 4 MiB fill \
         would touch ({FLAT_FILL_CHUNKS}), else 'one chunk vs the whole target' is \
         not observable (data_section_len {data_section_len})"
    );

    let reader = open_reader(&data_path).await;
    let sch = schema();

    SSTableReader::reset_decompress_calls();
    SSTableReader::reset_scan_window_counters();
    let cancel = ScanCancel::new();
    let mut rows = 0usize;
    reader
        .stream_all_partitions_for_query(Some(&sch), &cancel, None, |_row| {
            rows += 1;
            // Early termination: the caller is done after one row (a LIMIT 1 / a
            // satisfied downstream token filter).
            Ok(ControlFlow::Break(()))
        })
        .await
        .expect("an early-terminating summary-guided compressed scan must succeed");
    let decompress_calls = SSTableReader::decompress_call_count();
    let refills = SSTableReader::scan_window_refill_count();

    assert_eq!(
        rows, 1,
        "the closure must have broken after exactly one row"
    );
    assert_eq!(
        refills, 1,
        "one partition served means exactly ONE window fill, got {refills}"
    );
    // A ramped first fill covers one chunk_length (rounded up to the chunk
    // boundary): 1 chunk for this fixture, 2 only if the first partition itself
    // straddled a chunk boundary. Anything at/near FLAT_FILL_CHUNKS means the
    // greedy floor is still flat.
    const MAX_EARLY_TERMINATION_CHUNKS: u64 = 2;
    assert!(
        decompress_calls <= MAX_EARLY_TERMINATION_CHUNKS,
        "an early-terminating decode must pay ~ONE chunk, not the whole 4 MiB \
         target: decompressed {decompress_calls} chunks (a flat 4 MiB fresh fill \
         costs {FLAT_FILL_CHUNKS}; the ramp's first step costs \
         <= {MAX_EARLY_TERMINATION_CHUNKS}) — issue #2877 roborev blocker A"
    );
}

/// Issue #2877 roborev round 2 (blocker A, narrow-range half): a token range
/// holding a handful of partitions must not mature the ramp — it reads roughly its
/// own span, not the 4 MiB steady-state target. Same mechanism as the
/// early-termination test, driven through the token-pushdown path the Flight warm
/// split actually uses.
///
/// RED against the flat floor (the single fresh fill alone decompresses
/// `FLAT_FILL_CHUNKS` = 64 chunks for a range spanning ~3); GREEN against the ramp.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn narrow_token_range_does_not_mature_the_ramp() {
    let (_temp, data_path, chunk_count, _data_len) =
        compressed_fixture_with_index(N_LARGE, LARGE_MIN_PAYLOAD_LEN, LARGE_REPACK_CHUNK_SIZE)
            .await;
    assert!(
        (chunk_count as u64) > FLAT_FILL_CHUNKS,
        "fixture must hold more chunks ({chunk_count}) than a flat fresh fill \
         touches ({FLAT_FILL_CHUNKS})"
    );

    // Authoritative tokens (never assumed), ascending — the order Index.db is
    // physically written in. Pick a 3-partition window in the MIDDLE of the ring.
    let mut by_token: Vec<(i32, i64)> = (1..=N_LARGE)
        .map(|id| (id, cassandra_murmur3_token(&key_bytes(id))))
        .collect();
    by_token.sort_by_key(|(_, tok)| *tok);
    const RANGE_PARTITIONS: usize = 3;
    let lo_rank = (N_LARGE as usize) / 2;
    let start_excl = by_token[lo_rank].1;
    let end_incl = by_token[lo_rank + RANGE_PARTITIONS].1;
    assert!(start_excl < end_incl, "must be a non-wraparound range");
    let expected: Vec<i32> = by_token[lo_rank + 1..=lo_rank + RANGE_PARTITIONS]
        .iter()
        .map(|(id, _)| *id)
        .collect();

    let reader = open_reader(&data_path).await;
    let bound = ScanTokenBound {
        start_excl,
        end_incl,
    };
    SSTableReader::reset_decompress_calls();
    SSTableReader::reset_scan_window_counters();
    let cancel = ScanCancel::new();
    let mut ids = Vec::new();
    reader
        .stream_all_partitions_for_query(None, &cancel, Some(bound), |row| {
            let key_bytes: &[u8] = &row.key.0;
            ids.push(i32::from_be_bytes(
                key_bytes.try_into().expect("4-byte int PK"),
            ));
            Ok(ControlFlow::Continue(()))
        })
        .await
        .expect("a narrow-range summary-guided compressed scan must succeed");
    let decompress_calls = SSTableReader::decompress_call_count();

    ids.sort_unstable();
    let mut want = expected.clone();
    want.sort_unstable();
    assert_eq!(
        ids, want,
        "the narrow range must still emit exactly its in-range partitions"
    );
    // 3 partitions * ~20 KiB span ~= 60 KiB, so a ramping walk touches a couple of
    // 64 KiB chunks. A flat 4 MiB floor decompresses FLAT_FILL_CHUNKS on the very
    // first fill regardless of how narrow the range is.
    const MAX_NARROW_RANGE_CHUNKS: u64 = 8;
    assert!(
        decompress_calls <= MAX_NARROW_RANGE_CHUNKS,
        "a {RANGE_PARTITIONS}-partition token range must decompress ~its own span \
         ({MAX_NARROW_RANGE_CHUNKS} chunks max), not the 4 MiB target \
         ({FLAT_FILL_CHUNKS} chunks): got {decompress_calls} — issue #2877 roborev \
         blocker A"
    );
}

/// Issue #2877 roborev round 2 (blocker B): draining the dead prefix for EVERY
/// partition served shifts all remaining window bytes each time — over a 4 MiB
/// window holding thousands of narrow partitions that is quadratic byte copying,
/// negating the coalescing win. Reclamation must instead be O(REFILLS).
///
/// The oracle is the number of prefix-compaction memmoves the window performs
/// across one full scan (`scan_window_prefix_compaction_count`). What this DOES
/// cover: the number of memmove EVENTS, which is the whole difference between the
/// two shapes (per-partition vs per-refill) — with a bounded number of events each
/// bounded by the window size, total copied bytes is linear in the data section.
/// What it does NOT cover: the exact byte volume of each memmove (the harness has
/// no allocator/`memcpy` instrumentation), so this pins the asymptotics, not a
/// constant factor.
///
/// RED against the per-partition drain (`compactions == N_LARGE - 1` = 439);
/// GREEN against per-refill reclamation (a handful).
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn full_scan_prefix_reclamation_is_per_refill_not_per_partition() {
    let (_temp, data_path, chunk_count, data_section_len) =
        compressed_fixture_with_index(N_LARGE, LARGE_MIN_PAYLOAD_LEN, LARGE_REPACK_CHUNK_SIZE)
            .await;
    assert!(
        data_section_len > 2 * WINDOW_TARGET_BYTES,
        "fixture must exceed 2x the window target so MANY partitions are served \
         per window, got {data_section_len} bytes"
    );

    let reader = open_reader(&data_path).await;
    let sch = schema();

    SSTableReader::reset_decompress_calls();
    SSTableReader::reset_scan_window_counters();
    let cancel = ScanCancel::new();
    let mut rows = 0usize;
    reader
        .stream_all_partitions_for_query(Some(&sch), &cancel, None, |_row| {
            rows += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .expect("full compressed scan must succeed");
    let compactions = SSTableReader::scan_window_prefix_compaction_count();
    let refills = SSTableReader::scan_window_refill_count();
    let decompress_calls = SSTableReader::decompress_call_count();

    assert_eq!(
        rows, N_LARGE as usize,
        "every partition must still be decoded exactly once"
    );
    assert_eq!(
        decompress_calls, chunk_count as u64,
        "reclaiming lazily must not re-decompress any chunk: expected \
         {chunk_count}, got {decompress_calls}"
    );
    // WIRING EVIDENCE (see `min_refills_for_full_scan`): `compactions <= refills`
    // is only a meaningful ceiling over a REAL denominator. A window that never
    // streams (one whole-section fill) reports `refills == 1, compactions == 0`,
    // and a bypassed window reports `0 <= 0` — both vacuous, both passing. Pin the
    // refill floor FIRST so the O(refills) claim below is about a walk that
    // genuinely refilled many times while serving hundreds of partitions.
    let min_refills = min_refills_for_full_scan(data_section_len, LARGE_REPACK_CHUNK_SIZE).max(2);
    assert!(
        refills >= min_refills,
        "the full scan must have routed through the coalescing window at least \
         {min_refills} times: {data_section_len} bytes / (4 MiB + \
         {LARGE_REPACK_CHUNK_SIZE}-byte chunk) rounded up, floored at 2 (the \
         fixture exceeds 2x the window target), got {refills} — 0 or 1 would make \
         the O(refills) assert below vacuously true (issue #2877 wiring evidence)"
    );
    assert!(
        compactions <= refills,
        "dead-prefix reclamation must be O(refills) ({refills}), not \
         O(partitions) ({N_LARGE}): {compactions} memmoves — issue #2877 roborev \
         blocker B (quadratic byte copying across a 4 MiB window's partitions)"
    );
}

/// Issue #2877 roborev finding (High): the OTHER refill shape a naive window
/// misses. A compressed WRAPAROUND token range's two segments are disjoint —
/// the walk SKIPS the out-of-range run between them without reading it (so the
/// window never advances for those partitions), then must jump to the second
/// segment's first partition from a position that has nothing to do with the
/// stale window. This mirrors `issue_2412_wraparound_scan.rs`'s bound-selection
/// technique but drives a COMPRESSED fixture through the exact coalescing
/// window under test (that file's fixture is uncompressed, so it never
/// exercised this window at all).
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn wraparound_compressed_scan_reads_both_disjoint_segments() {
    const N_WRAP: i32 = 440;
    // Sized (like the multi-window test) so the file is written in ascending
    // TOKEN order — and therefore ascending PHYSICAL offset too — with the LOW
    // segment (ranks near 0) at the file's physical start and the HIGH segment
    // (ranks near N_WRAP) at its physical end, ~8 MiB apart: comfortably more
    // than one window target, so serving the LOW segment's window cannot
    // possibly still cover the HIGH segment's first partition. Without this
    // sizing the whole file fits in ONE window and the gap-realign path this
    // test targets is never exercised (both segments come "free" from the same
    // initial fill either way — a vacuous pass, not a real pin).
    const MIN_PAYLOAD_LEN: usize = 20 * 1024;
    const WRAP_REPACK_CHUNK_SIZE: usize = 64 * 1024;

    let (_temp, data_path, chunk_count, data_len) =
        compressed_fixture_with_index(N_WRAP, MIN_PAYLOAD_LEN, WRAP_REPACK_CHUNK_SIZE).await;
    assert!(
        data_len > 2 * WINDOW_TARGET_BYTES,
        "fixture must exceed 2x the window target so the LOW segment's window \
         cannot possibly still cover the HIGH segment's start, got {data_len} bytes"
    );

    // Compute every partition's ACTUAL token (never assumed) and sort ascending
    // — the token order Index.db/Summary.db are physically written in.
    let mut by_token: Vec<(i32, i64)> = (1..=N_WRAP)
        .map(|id| (id, cassandra_murmur3_token(&key_bytes(id))))
        .collect();
    by_token.sort_by_key(|(_, tok)| *tok);

    // HIGH segment: ranks 385..N_WRAP — start_excl = rank 384's token.
    // LOW segment: ranks 0..=15 — end_incl = rank 15's token.
    let start_excl = by_token[384].1;
    let end_incl = by_token[15].1;
    assert!(
        start_excl > end_incl,
        "fixture must produce a genuine wraparound pair (start > end); got \
         start_excl={start_excl} end_incl={end_incl}"
    );

    let expected_low: Vec<i32> = by_token[0..=15].iter().map(|(id, _)| *id).collect();
    let expected_high: Vec<i32> = by_token[385..N_WRAP as usize]
        .iter()
        .map(|(id, _)| *id)
        .collect();
    let mut expected: Vec<i32> = expected_low
        .iter()
        .chain(expected_high.iter())
        .copied()
        .collect();
    expected.sort_unstable();
    assert!(
        !expected_low.is_empty() && !expected_high.is_empty(),
        "both wraparound segments must genuinely hold partitions, or this test \
         cannot discriminate a windowing bug from having nothing to find"
    );

    let reader = open_reader(&data_path).await;
    let bound = ScanTokenBound {
        start_excl,
        end_incl,
    };
    SSTableReader::reset_decompress_calls();
    SSTableReader::reset_scan_window_counters();
    let cancel = ScanCancel::new();
    let mut ids = Vec::new();
    reader
        .stream_all_partitions_for_query(None, &cancel, Some(bound), |row| {
            let key_bytes: &[u8] = &row.key.0;
            ids.push(i32::from_be_bytes(
                key_bytes.try_into().expect("4-byte int PK"),
            ));
            Ok(ControlFlow::Continue(()))
        })
        .await
        .expect(
            "a compressed wraparound scan must succeed — a panic or Err here \
             (issue #2877 roborev) means the window tried to 'continue' across \
             the skipped out-of-range gap between the two segments instead of \
             realigning fresh",
        );
    ids.sort_unstable();
    assert_eq!(
        ids,
        expected,
        "a wraparound range must emit every partition in BOTH disjoint segments \
         (low: {} partitions, high: {} partitions) through the COMPRESSED \
         coalescing window",
        expected_low.len(),
        expected_high.len()
    );
    // Token pushdown must survive the ramp (issue #2877 roborev blocker A): the two
    // in-range segments hold only a small fraction of the fixture's N_WRAP
    // partitions, so the walk must decompress roughly THEIR span — NOT the whole
    // data section. Because a gap-realign RESETS the ramp, the second segment
    // re-earns its read-ahead from one chunk instead of inheriting the first
    // segment's matured floor. The 2x slack absorbs chunk-boundary rounding and the
    // ramp's own doubling overshoot on each segment.
    let in_range = expected_low.len() + expected_high.len();
    let in_range_chunks = (in_range * MIN_PAYLOAD_LEN).div_ceil(WRAP_REPACK_CHUNK_SIZE) as u64;
    let ceiling = in_range_chunks * 2;
    let decompress_calls = SSTableReader::decompress_call_count();
    assert!(
        decompress_calls <= ceiling,
        "a wraparound range covering {in_range} partitions (~{in_range_chunks} \
         chunks) must decompress <= {ceiling} chunks, not the whole \
         {chunk_count}-chunk section: got {decompress_calls}"
    );
}
