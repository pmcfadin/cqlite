//! Issue #2877 — TDD pin: the Summary-guided compressed scan walk
//! (`walk_in_range_partition_slices`, `summary_scan.rs`) must decompress each
//! covering `CompressionInfo.db` chunk ONCE across a full-table scan, never once
//! per contained partition.
//!
//! `#![cfg(feature = "write-support")]`: this file drives `SSTableWriter` /
//! `WriteEngine` mutations to build fixtures, both gated behind `write-support`
//! (mirrors `issue_1495_arrow_accessor_parity`'s `arrow`-gating convention) — a
//! `--no-default-features` test build without it must not even try to compile
//! this target.
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

#![cfg(feature = "write-support")]

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

/// `COMPRESSED_SCAN_WINDOW_TARGET_BYTES` in `summary_scan.rs` (private to that
/// module, so mirrored here as a test-side constant): the multi-window fixture
/// below deliberately exceeds `2x` this so the walk MUST refill at least twice,
/// exercising the straddle/append path, not just the initial fill.
const WINDOW_TARGET_BYTES: usize = 4 * 1024 * 1024;

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
    let (_temp, data_path, chunk_count, _data_len) =
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

    assert_eq!(
        rows, N as usize,
        "the scan must decode every partition exactly once"
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
    const N_LARGE: i32 = 440;
    // ~20 KiB/partition * 440 partitions ≈ 8.8 MiB uncompressed data — comfortably
    // over TWO window targets, forcing at least one full refill (not just the
    // initial fill), and with 64 KiB chunks the boundary falls inside a
    // partition almost certainly (20 KiB does not divide the 4 MiB target evenly).
    const MIN_PAYLOAD_LEN: usize = 20 * 1024;
    const LARGE_REPACK_CHUNK_SIZE: usize = 64 * 1024;

    let (_temp, data_path, chunk_count, data_section_len) =
        compressed_fixture_with_index(N_LARGE, MIN_PAYLOAD_LEN, LARGE_REPACK_CHUNK_SIZE).await;
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

    let (_temp, data_path, _chunk_count, data_len) =
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
        wraparound: true,
    };
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
}
