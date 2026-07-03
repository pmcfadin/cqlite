//! Issue #1562 (Epic A read-perf-gate): the `read/get_partition_big` bench
//! fixture must have more than one compression chunk.
//!
//! The perf gate's real point-read bench (`read/get_partition_big`) is only a
//! meaningful signal if the BIG fixture (`test_basic.simple_table`, nb format)
//! actually spans multiple compression chunks — a single-chunk fixture would let
//! a whole-file decode masquerade as a targeted single-chunk seek, so a prefetch
//! or chunk-decode regression on the point path could hide. This test pins that
//! invariant by parsing the fixture's `CompressionInfo.db` and asserting it holds
//! more than one chunk.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present, matching the repo's other dataset-backed
//! integration tests.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::storage::sstable::compression_info::CompressionInfo;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Resolve `<sstables>/test_basic/simple_table-<hash>/` by globbing the prefix.
fn simple_table_dir() -> Option<PathBuf> {
    let parent = datasets_root()?.join("sstables").join("test_basic");
    let entry = std::fs::read_dir(&parent)
        .ok()?
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with("simple_table-"))?;
    Some(entry.path())
}

/// Find the `*CompressionInfo.db` component inside a table directory.
fn compression_info_path(table_dir: &std::path::Path) -> Option<PathBuf> {
    std::fs::read_dir(table_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().ends_with("CompressionInfo.db"))
                .unwrap_or(false)
        })
}

#[test]
fn simple_table_fixture_spans_multiple_compression_chunks() {
    let Some(table_dir) = simple_table_dir() else {
        eprintln!(
            "Skipping: test_basic/simple_table not found under CQLITE_DATASETS_ROOT \
             (fixtures not fetched?)"
        );
        return;
    };
    let Some(ci_path) = compression_info_path(&table_dir) else {
        eprintln!(
            "Skipping: no CompressionInfo.db in {} (uncompressed fixture?)",
            table_dir.display()
        );
        return;
    };

    let bytes =
        std::fs::read(&ci_path).unwrap_or_else(|e| panic!("read {}: {e}", ci_path.display()));
    let parsed = CompressionInfo::parse(&bytes)
        .unwrap_or_else(|e| panic!("parse {}: {e}", ci_path.display()));

    assert!(
        parsed.chunk_offsets.len() > 1,
        "Issue #1562: perf-gate BIG fixture {} must span >1 compression chunk so the \
         read/get_partition_big bench exercises a real single-chunk seek (not a whole-file \
         decode); got {} chunk(s)",
        ci_path.display(),
        parsed.chunk_offsets.len(),
    );

    println!(
        "Issue #1562: {} has {} compression chunks (chunk_length={} bytes)",
        ci_path.display(),
        parsed.chunk_offsets.len(),
        parsed.chunk_length,
    );
}
