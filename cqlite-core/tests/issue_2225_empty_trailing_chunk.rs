//! Regression test for issue #2225: degenerate empty trailing chunk in a
//! compressed (Deflate) SSTable must not abort a scan.
//!
//! Cassandra compaction can emit a compressed SSTable whose `CompressionInfo.db`
//! carries one extra "chunk" past the real data: its offset == end of Data.db and
//! its payload is 0 bytes (`chunkCount = realchunks + 1`). Cassandra's own reader
//! never touches it — every logical position `< data_length` maps to an earlier
//! chunk. cqlite's stitch/scan path used to eagerly decompress EVERY
//! `CompressionInfo` offset, so Deflate rejected the 0-byte chunk with
//! "Invalid Deflate data: empty chunk" and the whole scan failed. (LZ4/Snappy only
//! survived by accident.) The fix bounds the chunk walk to `data_length` at the
//! single chunk-yield source, so every decompress-per-chunk consumer — the
//! `scan` stitch path AND the windowed `scan_stream` path — skips the empty chunk.
//!
//! The committed fixture under `tests/fixtures/issue_2225/` is the SMALLEST real
//! repro from the v3.5 corpus: a Deflate `multi_partition_table` whose
//! `CompressionInfo.db` has 2 chunks (data_length 5681, chunk_length 16384) where
//! chunk 1 starts at logical 16384 >= 5681 — the degenerate empty trailing chunk.
//! Its Data.db / CompressionInfo.db / Statistics.db are gitignored binaries, so
//! they are force-added (`git add -f`); this test is self-contained and does NOT
//! depend on `CQLITE_DATASETS_ROOT`.

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::{Config, Platform, TableId};

/// Absolute path to the committed repro Data.db.
fn repro_data_db() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/issue_2225/multi_partition_table/nb-1-big-Data.db")
}

/// The fixture partitions/rows: the reference JSONL for this SSTable has 50
/// partitions, each with exactly one row.
const EXPECTED_ROWS: usize = 50;

/// The sequential/stitch scan path (`SSTableReader::scan`) opens a Deflate
/// SSTable with a degenerate empty trailing chunk and yields all rows — it must
/// NOT fail with "Invalid Deflate data: empty chunk" (issue #2225).
#[tokio::test]
async fn scan_opens_deflate_sstable_with_empty_trailing_chunk() {
    let data_db = repro_data_db();
    assert!(
        data_db.is_file(),
        "committed repro fixture missing at {} — it must be force-added \
         (git add -f), Data.db is gitignored",
        data_db.display()
    );

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open Deflate SSTable with empty trailing chunk");

    let table_id = TableId::from("test_basic.multi_partition_table");
    let rows = reader
        .scan(&table_id, None, None, None, None)
        .await
        .expect("scan (stitch path) must not fail on the empty trailing chunk");

    assert_eq!(
        rows.len(),
        EXPECTED_ROWS,
        "stitch scan must yield every row past the degenerate trailing chunk"
    );
}

/// The windowed streaming scan path (`SSTableReader::scan_stream`) decompresses
/// each chunk fed from the same chunk-yield source, so it must also skip the
/// degenerate empty trailing chunk and stream all rows (issue #2225).
#[tokio::test]
async fn scan_stream_opens_deflate_sstable_with_empty_trailing_chunk() {
    let data_db = repro_data_db();
    assert!(data_db.is_file(), "committed repro fixture missing");

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    let reader = Arc::new(
        SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open Deflate SSTable with empty trailing chunk"),
    );

    let table_id = TableId::from("test_basic.multi_partition_table");
    let mut rx = reader.scan_stream(table_id, None, None, None, 64);
    let mut count = 0usize;
    while let Some(item) = rx.recv().await {
        item.expect("windowed scan item must not fail on the empty trailing chunk");
        count += 1;
    }

    assert_eq!(
        count, EXPECTED_ROWS,
        "windowed scan_stream must yield every row past the degenerate trailing chunk"
    );
}
