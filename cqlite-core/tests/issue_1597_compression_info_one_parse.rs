//! Issue #1597 (Epic G / G1): opening a compressed SSTable parses its
//! `CompressionInfo.db` file EXACTLY ONCE.
//!
//! Before G1 the reader open path parsed the same `CompressionInfo.db` twice —
//! once via the legacy `compression::CompressionInfo::parse_binary` (inside
//! `detect_and_initialize_compression`, only to learn the algorithm) and once via
//! the modern `compression_info::CompressionInfo::parse` (for the chunk metadata).
//! G1 consolidates to the single modern parser and derives the `CompressionReader`
//! algorithm from that one result. This test pins the win via the
//! `COMPRESSION_INFO_PARSES` read-work counter: a cold open records exactly 1
//! (RED = 2 on the pre-consolidation tree).
//!
//! Compiled only with `--features work-counters` (the getter/`reset` live behind
//! it; see `read_work_counters`). Requires `CQLITE_DATASETS_ROOT` + fetched
//! binaries; skips (never fails) when the compressed fixture is absent.
//!
//! The counter is a shared process-global, so this test serializes on the
//! `serial_test` mutex (the existing counter-test convention) — a stale value
//! from a parallel test can never satisfy an assertion after a `reset`.

#![cfg(feature = "work-counters")]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::Config;
use serial_test::serial;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Locate a `*-Data.db` in `<datasets>/sstables/<keyspace>/<table>-*/` that has a
/// sibling `*-CompressionInfo.db` (i.e. the fixture is compressed). Returns the
/// Data.db path. Skip keys off fixture presence, so a present fixture that fails
/// to open stays a hard failure rather than a silent skip.
fn find_compressed_data_file(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let entries = std::fs::read_dir(root.join("sstables").join(keyspace)).ok()?;
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        let dir = e.path();
        let files = std::fs::read_dir(&dir).ok()?;
        let mut data_file: Option<PathBuf> = None;
        let mut has_compression_info = false;
        for f in files.flatten() {
            let name = f.file_name().to_string_lossy().into_owned();
            if name.ends_with("-Data.db") {
                data_file = Some(f.path());
            } else if name.ends_with("-CompressionInfo.db") {
                has_compression_info = true;
            }
        }
        if has_compression_info {
            if let Some(df) = data_file {
                return Some(df);
            }
        }
    }
    None
}

/// Scenario: opening a compressed SSTable parses `CompressionInfo.db` exactly once.
///
/// Reset `COMPRESSION_INFO_PARSES`, open the reader on a compressed fixture, and
/// assert the counter delta is exactly 1. On the pre-G1 path this was 2 (legacy
/// `parse_binary` + modern `parse`), so `== 1` is a tight RED→GREEN discriminator.
#[tokio::test]
#[serial]
async fn open_parses_compression_info_exactly_once() {
    let Some(data_file) = find_compressed_data_file("test_basic", "simple_table") else {
        eprintln!(
            "Skipping (G1 one-parse-per-open): compressed test_basic/simple_table fixture absent"
        );
        return;
    };

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform must initialize"),
    );

    // Reset BEFORE the open so the entire open path is measured from zero.
    rwc::reset();
    assert_eq!(
        rwc::compression_info_parses(),
        0,
        "reset must zero COMPRESSION_INFO_PARSES"
    );

    let reader = SSTableReader::open(&data_file, &config, platform)
        .await
        .expect("compressed fixture must open");
    // Keep the reader alive across the assertion so the open work is attributed.
    let _ = &reader;

    let parses = rwc::compression_info_parses();
    assert_eq!(
        parses, 1,
        "G1: opening a compressed SSTable must parse CompressionInfo.db exactly ONCE \
         (got {parses}); pre-G1 this was 2 — a legacy parse_binary plus the modern parse"
    );
}

/// Guard: the counter increments per parse, so a bare `CompressionInfo::parse`
/// call records exactly one. Keeps the wiring honest even without a fixture.
#[tokio::test]
#[serial]
async fn direct_parse_records_one() {
    use cqlite_core::storage::sstable::compression_info::CompressionInfo;

    // Minimal valid Cassandra CompressionInfo.db blob: LZ4Compressor, no options,
    // chunk_length=16384, max_compressed_length=i32::MAX, data_length=32768,
    // one chunk offset at 0.
    let mut blob = Vec::new();
    let name = b"LZ4Compressor";
    blob.extend_from_slice(&(name.len() as u16).to_be_bytes());
    blob.extend_from_slice(name);
    blob.extend_from_slice(&0u32.to_be_bytes()); // option_count
    blob.extend_from_slice(&16384u32.to_be_bytes()); // chunk_length
    blob.extend_from_slice(&(i32::MAX as u32).to_be_bytes()); // max_compressed_length
    blob.extend_from_slice(&32768u64.to_be_bytes()); // data_length
    blob.extend_from_slice(&1u32.to_be_bytes()); // chunk_count
    blob.extend_from_slice(&0u64.to_be_bytes()); // chunk_offsets[0]

    rwc::reset();
    let _info = CompressionInfo::parse(&blob).expect("blob parses");
    assert_eq!(
        rwc::compression_info_parses(),
        1,
        "a single CompressionInfo::parse must record exactly one parse"
    );
}
