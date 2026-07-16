//! Stage-0 measure-first bench for the runtime DecodePolicy work (issue #2211,
//! F6.4). This is the blocking-gate artifact: it exists to answer, honestly and
//! before a single line of `unsafe` is written, whether removing lz4 bounds
//! checks (the whole point of a FastUnsafe policy) is even a *measurable*
//! fraction of a real SSTable scan on the deployment target (Linux).
//!
//! Three throughput signals, all reported in decompressed/scanned bytes/sec so
//! they can be read on one axis:
//!
//!  - `decode_policy/lz4_flex_decompress` — the SAFE (`safe-decode` on, the
//!    library default) lz4 decode *in isolation*: real on-disk compressed chunk
//!    payloads from the fixture, fed straight to `lz4_flex::decompress`. This is
//!    the ONLY step a FastUnsafe policy would change. The delta a future
//!    FastUnsafe backend could buy is bounded ABOVE by this number's inverse
//!    (you cannot save more time than the decode step costs).
//!  - `decode_policy/full_chunk_path` — the whole production chunk-decompress
//!    path over the same fixture (seek + read compressed record + CRC32 verify +
//!    lz4 decode + size validate), fresh `ChunkDecompressor` per iteration so the
//!    chunk cache is cold and every chunk decodes. FastUnsafe removes NONE of the
//!    CRC/read/validate cost, so the decode step is a fraction of even this.
//!  - `decode_policy/full_scan` — end-to-end `SELECT *` over the SAME compressed
//!    fixture through the public query API (requires `cli-helpers`). Throughput
//!    is measured against the fixture's uncompressed `data_length`, so it is
//!    directly comparable to the two decode numbers above. This is the number
//!    field evidence (#2385, #2397) says actually dominates.
//!
//! Fixture: `test_timeseries.sensor_data` — the largest genuinely lz4-*decoded*
//! fixture in the vendored corpus (the larger `test_comp` incompressible fixture
//! stores raw chunks and never calls the lz4 decoder, so it is unusable here).
//! The corpus is small (a fixture artifact, not the production case — real
//! Cassandra SSTables run tens of MiB to GiB), so the decode numbers are a HOT-
//! cache CPU-bound decode rate. That is exactly the right regime for this
//! question: it measures the CPU cost of the bounds checks themselves, the most
//! favorable possible case for FastUnsafe.
//!
//! Run (both decode arms + the end-to-end arm):
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo bench -p cqlite-core --features cli-helpers --bench decode_policy
//!
//! Skip-registers (no group, criterion reports the arm as absent) when the
//! fixture binaries are not fetched. NOT a perf-gate entry — a one-time Stage-0
//! decision artifact.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::path::PathBuf;

#[path = "fixtures/mod.rs"]
mod fixtures;

use cqlite_core::storage::sstable::chunk_decompressor::{
    create_decompressor_from_file, ChunkDecompressor,
};
use cqlite_core::storage::sstable::compression_info::CompressionInfo;

/// The compressed fixture measured by every arm. `sensor_data` is the largest
/// fixture whose LZ4 chunks are actually lz4-decoded (not stored raw).
const FIXTURE: fixtures::ReadFixture = fixtures::ReadFixture::CLUSTERING;

/// Locate the `*-Data.db` and `*-CompressionInfo.db` for the fixture table.
fn fixture_component_paths() -> Option<(PathBuf, PathBuf)> {
    if !fixtures::fixture_present(&FIXTURE) {
        return None;
    }
    let dir = fixtures::table_dir(FIXTURE.keyspace, FIXTURE.table);
    let mut data_db = None;
    let mut ci_db = None;
    for entry in std::fs::read_dir(&dir).ok()?.filter_map(|e| e.ok()) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with("-Data.db") {
            data_db = Some(entry.path());
        } else if name.ends_with("-CompressionInfo.db") {
            ci_db = Some(entry.path());
        }
    }
    Some((data_db?, ci_db?))
}

/// One genuinely-lz4-compressed chunk ready to hand to `lz4_flex::decompress`:
/// the lz4 block bytes (length prefix and trailing CRC already stripped) plus the
/// exact decompressed length Cassandra recorded for it.
struct Lz4Chunk {
    lz4_block: Vec<u8>,
    decompressed_len: usize,
}

/// Extract every lz4-compressed chunk's block bytes from the real Data.db,
/// mirroring `ChunkDecompressor::decompress_lz4_chunk`: for each chunk record,
/// strip the trailing 4-byte inline CRC and the leading 4-byte little-endian
/// uncompressed-length prefix. Chunks Cassandra stored raw (incompressible:
/// `compressed_len >= max_compressed_length`) are skipped — they never reach the
/// lz4 decoder, so they are not part of the decode cost under study.
fn extract_lz4_chunks(data_db: &[u8], info: &CompressionInfo) -> (Vec<Lz4Chunk>, u64) {
    let file_size = data_db.len() as u64;
    let max_compressed = info.max_compressed_length as usize;
    let mut chunks = Vec::new();
    let mut decompressed_total = 0u64;

    for i in 0..info.chunk_offsets.len() {
        let (Some(offset), Some(record_size)) = (
            info.compressed_chunk_offset(i),
            info.compressed_chunk_size(i, file_size),
        ) else {
            continue;
        };
        if record_size < 4 {
            continue;
        }
        // Record = [compressed payload][4-byte BE CRC32]; drop the CRC.
        let compressed_len = (record_size - 4) as usize;
        let start = offset as usize;
        let end = start + compressed_len;
        if end > data_db.len() || compressed_len < 4 {
            continue;
        }
        // Raw (incompressible) chunk — stored uncompressed, no lz4 decode.
        if compressed_len >= max_compressed {
            continue;
        }
        let payload = &data_db[start..end];
        // Cassandra prepends a 4-byte little-endian uncompressed length.
        let decompressed_len =
            u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let lz4_block = payload[4..].to_vec();
        decompressed_total += decompressed_len as u64;
        chunks.push(Lz4Chunk {
            lz4_block,
            decompressed_len,
        });
    }
    (chunks, decompressed_total)
}

/// Arm 1: SAFE lz4 decode in isolation (`lz4_flex::decompress`), the single step
/// a FastUnsafe policy would replace.
fn bench_decompress_only(c: &mut Criterion) {
    let Some((data_path, ci_path)) = fixture_component_paths() else {
        eprintln!("decode_policy/lz4_flex_decompress: fixture absent — skipping (skip-register)");
        return;
    };
    let data_db = std::fs::read(&data_path).expect("read Data.db");
    let ci_bytes = std::fs::read(&ci_path).expect("read CompressionInfo.db");
    let info = CompressionInfo::parse(&ci_bytes).expect("parse CompressionInfo.db");
    assert_eq!(
        info.algorithm, "LZ4Compressor",
        "decode_policy bench requires an LZ4 fixture"
    );
    let (chunks, decompressed_total) = extract_lz4_chunks(&data_db, &info);
    assert!(
        !chunks.is_empty() && decompressed_total > 0,
        "no lz4-compressed chunks extracted from {} — fixture may be entirely raw",
        data_path.display()
    );

    let mut group = c.benchmark_group("decode_policy");
    group.throughput(Throughput::Bytes(decompressed_total));
    group.bench_function("lz4_flex_decompress", |b| {
        b.iter(|| {
            for chunk in &chunks {
                let out = lz4_flex::decompress(
                    black_box(&chunk.lz4_block),
                    black_box(chunk.decompressed_len),
                )
                .expect("lz4_flex decompress");
                black_box(out);
            }
        });
    });
    group.finish();
}

/// Arm 2: the whole production chunk-decompress path over the same fixture, cold
/// cache per iteration (seek + read + CRC32 + lz4 decode + validate).
fn bench_full_chunk_path(c: &mut Criterion) {
    let Some((data_path, ci_path)) = fixture_component_paths() else {
        eprintln!("decode_policy/full_chunk_path: fixture absent — skipping (skip-register)");
        return;
    };
    let ci_bytes = std::fs::read(&ci_path).expect("read CompressionInfo.db");
    let info = CompressionInfo::parse(&ci_bytes).expect("parse CompressionInfo.db");
    let data_length = info.data_length;

    // Sanity: the path actually returns all the uncompressed bytes.
    {
        let mut probe: ChunkDecompressor =
            create_decompressor_from_file(&ci_path).expect("build decompressor");
        let mut f = std::fs::File::open(&data_path).expect("open Data.db");
        let bytes = probe.read_all_data(&mut f).expect("read_all_data");
        assert_eq!(
            bytes.len() as u64,
            data_length,
            "full_chunk_path must return the full uncompressed length"
        );
    }

    let mut group = c.benchmark_group("decode_policy");
    group.throughput(Throughput::Bytes(data_length));
    group.bench_function("full_chunk_path", |b| {
        b.iter(|| {
            // Fresh decompressor → empty chunk cache → every chunk decodes.
            let mut dec = create_decompressor_from_file(&ci_path).expect("build decompressor");
            let mut f = std::fs::File::open(&data_path).expect("open Data.db");
            let bytes = dec.read_all_data(&mut f).expect("read_all_data");
            black_box(bytes);
        });
    });
    group.finish();
}

/// Arm 3: end-to-end `SELECT *` over the same compressed fixture, measured
/// against the uncompressed `data_length` so it shares an axis with the decode
/// arms. Requires `cli-helpers` for the queryable fixture DB.
#[cfg(feature = "cli-helpers")]
fn bench_full_scan(c: &mut Criterion) {
    let Some((_data_path, ci_path)) = fixture_component_paths() else {
        eprintln!("decode_policy/full_scan: fixture absent — skipping (skip-register)");
        return;
    };
    let ci_bytes = std::fs::read(&ci_path).expect("read CompressionInfo.db");
    let info = CompressionInfo::parse(&ci_bytes).expect("parse CompressionInfo.db");
    let data_length = info.data_length;

    let loaded = fixtures::open_read_db(&FIXTURE);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sql = format!("SELECT * FROM {}", FIXTURE.qualified());

    // Honesty guard: never bench a 0-row scan.
    let n = rt
        .block_on(loaded.db.execute(&sql))
        .expect("scan fixture")
        .rows
        .len();
    assert!(n >= 1, "full_scan must return at least one row");

    let mut group = c.benchmark_group("decode_policy");
    group.throughput(Throughput::Bytes(data_length));
    group.bench_function("full_scan", |b| {
        b.iter(|| {
            let res = rt.block_on(loaded.db.execute(&sql)).expect("scan fixture");
            black_box(res.rows.len());
        });
    });
    group.finish();
}

#[cfg(not(feature = "cli-helpers"))]
fn bench_full_scan(_c: &mut Criterion) {
    eprintln!(
        "decode_policy/full_scan: needs --features cli-helpers to open a queryable fixture DB — skipping"
    );
}

criterion_group!(
    benches,
    bench_decompress_only,
    bench_full_chunk_path,
    bench_full_scan
);
criterion_main!(benches);
