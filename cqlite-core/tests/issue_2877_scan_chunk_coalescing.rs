//! Issue #2877 — TDD pin: the Summary-guided compressed scan walk
//! (`walk_in_range_partition_slices`, `summary_scan.rs`) must decompress each
//! covering `CompressionInfo.db` chunk ONCE across a full-table scan, never once
//! per contained partition.
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
//! ## Oracle
//!
//! The production write surface emits UNCOMPRESSED SSTables only (issue #1406),
//! so this test hand-builds a genuinely LZ4-compressed fixture via the
//! fixture-synthesis building blocks `CompressedDataWriter`/`CompressionInfoWriter`
//! — the same technique `compaction_cancel_tests.rs`'s `compressed_fixture` uses —
//! but WITHOUT stripping `Summary.db`/`Index.db` (this walk requires both present).
//! `metadata.chunk_count()`, recorded at fixture-build time, is the
//! reader-independent ground truth for "how many chunks exist"; the
//! process-global `SSTableReader::decompress_call_count()` delta across one full
//! scan is asserted to equal EXACTLY that — not the (much larger) partition
//! count. The counter is reliable here because each `tests/` file is its own
//! process and this test is `#[serial]` (mirrors `decompressed_chunk_cache_tests.rs`).

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::writer::{
    create_compressor, CompressedDataWriter, CompressionAlgorithm, CompressionInfoWriter,
    SSTableWriter,
};
use cqlite_core::storage::write_engine::{CellOperation, Mutation, PartitionKey, TableId};
use cqlite_core::types::Value;
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

fn mutation(id: i32) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::text(format!("{PAYLOAD}-{id}")),
        }],
        1_000_000 + id as i64,
        None,
    )
}

/// Write `n` single-row partitions to a fresh uncompressed SSTable, keeping
/// EVERY emitted component (`Summary.db`/`Index.db`/`Filter.db` included — the
/// Summary-guided walk under test requires both `Summary.db` and `Index.db`
/// present, unlike `compaction_cancel_tests.rs`'s index-less sibling). Returns
/// the temp dir (keep alive) and the `Data.db` path.
async fn write_fixture(n: i32) -> (TempDir, PathBuf) {
    let sch = schema();
    let temp = TempDir::new().unwrap();
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &sch).unwrap();

    // The writer enforces ascending token order.
    let mut keyed: Vec<_> = (1..=n)
        .map(|id| {
            let m = mutation(id);
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
/// chunks. Returns the temp dir, the `Data.db` path, and the chunk count recorded
/// at build time (the ground-truth oracle for "how many chunks exist").
async fn compressed_fixture_with_index(n: i32) -> (TempDir, PathBuf, usize) {
    let (temp, data_path) = write_fixture(n).await;

    // Read the raw (uncompressed) data section back exactly as the reader would
    // see it — skip past `calculate_header_size()` bytes (0 for headerless 'nb').
    let header_size = open_reader(&data_path).await.calculate_header_size();
    let raw = std::fs::read(&data_path).unwrap();
    let data_section = &raw[header_size..];

    let compressor = create_compressor(CompressionAlgorithm::Lz4).unwrap();
    let mut writer = CompressedDataWriter::with_chunk_size(compressor, REPACK_CHUNK_SIZE);
    writer.write(data_section).unwrap();
    let (compressed, metadata) = writer.finish().unwrap();
    let chunk_count = metadata.chunk_count();
    assert!(
        chunk_count > 4 && (chunk_count as i32) < N / 2,
        "fixture must pack MANY partitions per chunk (chunk_count {chunk_count} \
         must be > 4 and well under N={N}) for the once-per-chunk-vs-once-per-\
         partition distinction to be meaningful"
    );

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

    (temp, data_path, chunk_count)
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
    let (_temp, data_path, chunk_count) = compressed_fixture_with_index(N).await;
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
