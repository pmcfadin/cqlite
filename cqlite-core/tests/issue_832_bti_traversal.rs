//! Fixture-backed integration tests for Issue #832: BTI full trie traversal.
//!
//! These exercise the new headerless, footer-based traversal entry points
//! against the real `test_da` BTI fixtures:
//!
//!   - `iterate_partitions_in_bti_file` — full Partitions.db DFS.
//!   - `iterate_rows_in_bti_file`       — full Rows.db DFS.
//!
//! The reconstructed keys are byte-comparable token prefixes (not original
//! partition keys), so assertions target the definitive payload OFFSETS, which
//! match the `position` fields in the sstabledump JSONL goldens.
//!
//! All tests are guarded by `CQLITE_DATASETS_ROOT` and skip (return early) when
//! the binary fixtures are absent, so they never block CI that runs without
//! test data.

use cqlite_core::storage::sstable::bti::{
    iterate_partitions_in_bti_file, iterate_rows_in_bti_file, BtiPartitionLocation,
};
use std::io::Cursor;
use std::path::{Path, PathBuf};

/// Resolve the datasets root, skipping the test if it is not configured.
fn datasets_root() -> Option<PathBuf> {
    match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(v) => Some(PathBuf::from(v)),
        Err(_) => {
            eprintln!("SKIP: CQLITE_DATASETS_ROOT not set; needs real BTI fixtures");
            None
        }
    }
}

/// Load a BTI component file into a Cursor, skipping the test if absent.
fn load_component(root: &Path, rel: &str) -> Option<Cursor<Vec<u8>>> {
    let path = root.join(rel);
    if !path.exists() {
        eprintln!("SKIP: BTI fixture not found at {:?}", path);
        return None;
    }
    match std::fs::read(&path) {
        Ok(bytes) => Some(Cursor::new(bytes)),
        Err(e) => {
            eprintln!("SKIP: cannot read {:?}: {}", path, e);
            None
        }
    }
}

/// (6) PartitionIterator over real `test_da/simple_table` Partitions.db
/// (79 bytes, root offset 17) yields exactly 3 entries with DataOffset
/// [0, 63, 125] in byte-comparable order — matching the JSONL goldens via the
/// transition chain root(17)→Sparse8→{0x90,0xBC,0xF9}→{0,63,125}.
#[test]
fn partition_iterator_real_simple_table_three_entries() {
    let Some(root) = datasets_root() else {
        return;
    };
    let Some(mut cursor) = load_component(
        &root,
        "sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Partitions.db",
    ) else {
        return;
    };

    let entries = iterate_partitions_in_bti_file(&mut cursor)
        .expect("full Partitions.db traversal must succeed");

    let offsets: Vec<u64> = entries
        .iter()
        .map(|(_, loc)| match loc {
            BtiPartitionLocation::DataOffset(o) => *o,
            BtiPartitionLocation::RowsOffset(o) => *o,
        })
        .collect();

    assert_eq!(
        entries.len(),
        3,
        "simple_table has exactly 3 partitions; got {}: {:?}",
        entries.len(),
        entries
    );
    assert_eq!(
        offsets,
        vec![0, 63, 125],
        "DFS must yield DataOffsets in byte-comparable order [0, 63, 125]"
    );
    for (_, loc) in &entries {
        assert!(
            matches!(loc, BtiPartitionLocation::DataOffset(_)),
            "narrow partitions must resolve to DataOffset, got {:?}",
            loc
        );
    }
}

/// (7) PartitionIterator over real `test_da/collection_table` Partitions.db
/// (74 bytes, root 12) yields exactly 2 entries with DataOffset [0, 107].
#[test]
fn partition_iterator_real_collection_table_two_entries() {
    let Some(root) = datasets_root() else {
        return;
    };
    let Some(mut cursor) = load_component(
        &root,
        "sstables/test_da/collection_table-de2c155064e711f19ad401a8c8227b11/da-2-bti-Partitions.db",
    ) else {
        return;
    };

    let entries = iterate_partitions_in_bti_file(&mut cursor)
        .expect("full Partitions.db traversal must succeed");

    let offsets: Vec<u64> = entries
        .iter()
        .map(|(_, loc)| match loc {
            BtiPartitionLocation::DataOffset(o) => *o,
            BtiPartitionLocation::RowsOffset(o) => *o,
        })
        .collect();

    assert_eq!(
        entries.len(),
        2,
        "collection_table has exactly 2 partitions; got {}: {:?}",
        entries.len(),
        entries
    );
    assert_eq!(
        offsets,
        vec![0, 107],
        "DFS must yield DataOffsets [0, 107] in byte-comparable order"
    );
}

/// (8) RowIterator over the empty (0-byte) simple_table Rows.db yields nothing
/// without panicking.
#[test]
fn row_iterator_real_empty_rows_db_yields_nothing() {
    let Some(root) = datasets_root() else {
        return;
    };
    let Some(mut cursor) = load_component(
        &root,
        "sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Rows.db",
    ) else {
        return;
    };

    let entries =
        iterate_rows_in_bti_file(&mut cursor).expect("empty Rows.db must yield Ok(empty), not Err");
    assert!(
        entries.is_empty(),
        "0-byte Rows.db must yield no entries; got {:?}",
        entries
    );
}
