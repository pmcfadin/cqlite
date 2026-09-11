//! Issue #4196, roborev round 16 (HIGH): `ChunkReader::read_chunk`'s round-15
//! Medium-finding-2 fix bounded every chunk record to `chunk_length + 4`
//! bytes — a ceiling that is WRONG for real Cassandra-written data whenever
//! `min_compress_ratio` sits at its DEFAULT value of `0`, which makes
//! `CompressionInfo.max_compressed_length` the `i32::MAX` sentinel
//! (`docs/sstable-guide-audit/facts-B5.md:52-53`) rather than a real,
//! smaller bound. At that default, `CompressedSequentialWriter` does NOT
//! fall back to an uncompressed chunk just because the compressor's output
//! exceeds `chunk_length` — it writes the expanded buffer as-is — and both
//! LZ4 and Snappy legitimately expand INCOMPRESSIBLE input past
//! `chunk_length` by their own documented worst-case bounds.
//!
//! This is pinned against a REAL, committed, Cassandra-written fixture
//! rather than a synthetic one (the format-authority rule, issue #3041):
//! `test_basic.simple_table` is Snappy-compressed with `chunk_length=16384`
//! and `max_compressed_length=2147483647` (the `i32::MAX` sentinel), and
//! chunk index 2 is a genuine 16394-byte on-disk record (start=32111,
//! end=48505 — independently re-derived here straight from the binary
//! `CompressionInfo.db`/`Data.db` bytes, matching the roborev finding's own
//! figures exactly) — 6 bytes over the OLD, INCORRECT `chunk_length + 4 =
//! 16388` ceiling. Seven of this fixture's 41 chunks share this shape
//! (indices 2, 6, 17, 23, 24, 33, 39); this test exercises one representative
//! index plus a full round-trip over the whole file to prove none of the
//! other six (or the ordinary-sized chunks) regressed either.
//!
//! Skip-clean when the fixture's binaries are absent (a worktree normally
//! carries only the committed JSONL/TOC sidecars); `CQLITE_REQUIRE_FIXTURES=1`
//! (#1094 doctrine) turns that into a hard failure.

#![cfg(feature = "all-compression")]

use std::fs::File;

use cqlite_core::storage::sstable::chunk_reader::ChunkReader;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::resolve_table_generation_dir;

const KEYSPACE: &str = "test_basic";
const TABLE: &str = "simple_table";

/// The exact chunk this fixture's real bytes are known (independently
/// re-derived from the binary `CompressionInfo.db`, matching the roborev
/// finding) to produce a 16394-byte on-disk record — 6 bytes over the
/// round-15 fix's incorrect `chunk_length + 4 = 16388` ceiling.
const OVERSIZED_CHUNK_INDEX: usize = 2;
const EXPECTED_TOTAL_RECORD_SIZE: u64 = 16394;

fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref() == Ok("1")
}

#[test]
fn snappy_chunk_wider_than_chunk_length_still_reads() {
    let dir = match resolve_table_generation_dir(KEYSPACE, TABLE) {
        Ok(dir) => dir,
        Err(msg) => {
            if require_fixtures() {
                panic!("CQLITE_REQUIRE_FIXTURES=1 but fixture unavailable: {msg}");
            }
            eprintln!(
                "[issue_4196 round16] skipping: {KEYSPACE}.{TABLE} fixture unavailable: {msg}"
            );
            return;
        }
    };

    let mut compression_info_path = None;
    let mut data_db_path = None;
    for entry in std::fs::read_dir(&dir).expect("read fixture dir").flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-CompressionInfo.db") {
            compression_info_path = Some(entry.path());
        } else if name_str.ends_with("-Data.db") && !name_str.ends_with(".jsonl") {
            data_db_path = Some(entry.path());
        }
    }
    let (Some(compression_info_path), Some(data_db_path)) = (compression_info_path, data_db_path)
    else {
        if require_fixtures() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {} is missing a *-CompressionInfo.db / *-Data.db \
                 pair (worktree carries only committed sidecars — fetch real binaries)",
                dir.display()
            );
        }
        eprintln!(
            "[issue_4196 round16] skipping: {} has no *-CompressionInfo.db / *-Data.db pair \
             (fetch real binaries with test-data/scripts/fetch-datasets.sh)",
            dir.display()
        );
        return;
    };

    let raw_compression_info =
        std::fs::read(&compression_info_path).expect("read CompressionInfo.db");
    let compression_info =
        CompressionInfo::parse(&raw_compression_info).expect("parse CompressionInfo.db");

    // Sanity-check the fixture still has the exact shape this test's
    // reasoning depends on — if a future dataset regen changes this
    // fixture's compression parameters, this assertion fails LOUDLY
    // (naming what changed) instead of the test silently exercising a
    // different, no-longer-oversized chunk.
    assert_eq!(
        compression_info.algorithm, "SnappyCompressor",
        "fixture's compression algorithm changed; re-derive OVERSIZED_CHUNK_INDEX"
    );
    assert_eq!(
        compression_info.chunk_length, 16384,
        "fixture's chunk_length changed; re-derive OVERSIZED_CHUNK_INDEX"
    );
    assert_eq!(
        compression_info.max_compressed_length,
        i32::MAX as u32,
        "fixture's max_compressed_length is no longer the i32::MAX sentinel — this fixture no \
         longer exercises the default-min_compress_ratio case this test targets"
    );

    let data_db_file = File::open(&data_db_path).expect("open Data.db");
    let total_file_size = data_db_file.metadata().expect("stat Data.db").len();

    let declared_total_size = compression_info
        .compressed_chunk_size(OVERSIZED_CHUNK_INDEX, total_file_size)
        .expect("chunk index in range");
    assert_eq!(
        declared_total_size, EXPECTED_TOTAL_RECORD_SIZE,
        "fixture's chunk {OVERSIZED_CHUNK_INDEX} record size drifted from the figure this test \
         (and the roborev round-16 finding) independently re-derived from the binary bytes"
    );
    assert!(
        declared_total_size > compression_info.chunk_length as u64 + 4,
        "test setup bug: chunk {OVERSIZED_CHUNK_INDEX}'s {declared_total_size}-byte record does \
         not actually exceed chunk_length+4 ({}) — this test would pass vacuously",
        compression_info.chunk_length + 4
    );

    let chunk_count = compression_info.chunk_offsets.len();
    let mut reader = ChunkReader::new(data_db_file, compression_info, total_file_size);

    // The specific chunk the finding names: must be READABLE, not rejected
    // as an implausible/oversized record.
    let chunk = reader
        .read_chunk(OVERSIZED_CHUNK_INDEX)
        .unwrap_or_else(|e| {
            panic!(
                "chunk {OVERSIZED_CHUNK_INDEX} (a real, legitimately-oversized Snappy chunk) \
                 must read successfully post-round-16-fix; got: {e}"
            )
        });
    assert!(
        !chunk.is_empty(),
        "chunk {OVERSIZED_CHUNK_INDEX} read as empty — expected real compressed payload bytes"
    );

    // Full round-trip over every chunk: proves none of the other six
    // similarly-oversized chunks (6, 17, 23, 24, 33, 39) regressed either,
    // and that ordinary-sized chunks are unaffected by the widened ceiling.
    let mut oversized_chunks_seen = 0usize;
    for i in 0..chunk_count {
        let chunk = reader
            .read_chunk(i)
            .unwrap_or_else(|e| panic!("chunk {i} failed to read post-round-16-fix: {e}"));
        assert!(!chunk.is_empty(), "chunk {i} read as empty");
        if reader
            .chunk_length()
            .checked_add(4)
            .map(|ceiling| chunk.len() as u64 + 4 > ceiling as u64)
            .unwrap_or(false)
        {
            oversized_chunks_seen += 1;
        }
    }
    assert_eq!(
        oversized_chunks_seen, 7,
        "expected exactly the 7 chunks the roborev finding named (indices 2, 6, 17, 23, 24, 33, \
         39) to exceed the old chunk_length+4 ceiling; got {oversized_chunks_seen} — fixture may \
         have changed"
    );

    eprintln!(
        "[issue_4196 round16] {KEYSPACE}.{TABLE}: all {chunk_count} chunks read successfully \
         post-fix, including {oversized_chunks_seen} legitimately-oversized Snappy chunks."
    );
}
