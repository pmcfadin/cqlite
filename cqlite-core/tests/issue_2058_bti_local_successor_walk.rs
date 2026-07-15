//! Issue #2058 (audit C4 part 2): the reader resolves a BTI partition's exclusive
//! END bound (its successor partition's `Data.db` start) with an O(depth) LOCAL
//! next-greater trie walk, replacing the pre-#2058 whole-trie DFS that enumerated +
//! sorted EVERY partition offset into a `OnceLock` array on the first seek.
//!
//! CORRECTNESS ORACLE (a wrong end-bound silently TRUNCATES a partition read): for
//! EVERY partition in a real `da` `Partitions.db`, the offset resolved by the new
//! O(depth) local walk MUST be byte-identical to what the old whole-trie DFS + sort
//! produced. This test computes both and asserts they match exactly:
//!
//!   - REFERENCE (old path): `iterate_partitions_in_bti_file` DFS-enumerates every
//!     `(reconstructed_token_key, BtiPartitionLocation)` in byte-comparable order;
//!     the resolved `Data.db` offsets, sorted ascending, are the exact array the old
//!     `successor_partition_offset` binary-searched. The successor of a partition at
//!     offset `o` is `sorted.partition_point(|x| x <= o)` (the smallest offset `>
//!     o`), or `None` for the last.
//!   - NEW path: `partition_successor_in_bti_slice_for_test`, keyed on each
//!     partition's OWN reconstructed trie key, resolves the successor with a single
//!     strict-ceiling descent — O(depth), never the whole trie.
//!
//! The two MUST agree for every partition and at the trie boundaries (first, last,
//! single-child, dense-node fixtures are covered by the crate-internal
//! `partition_successor_walk` unit tests; this pins it on REAL `da` binaries across
//! all four `test_da` tables, incl. the wide `wide_table` whose partitions are
//! `RowsOffset`s resolved through `Rows.db`).
//!
//! Requires `CQLITE_DATASETS_ROOT` + fetched binaries; skips (never fails) when
//! absent — but when the fixture IS present it asserts a non-empty partition set, so
//! a truncation regression cannot pass on empty data. Excluded under `tombstones`
//! (the seek path — and the successor walk — are compiled out there).

#![cfg(not(feature = "tombstones"))]

use std::path::PathBuf;

use cqlite_core::storage::sstable::bti::parser::{
    iterate_partition_locations_in_bti_file, partition_successor_in_bti_slice_for_test,
    BtiPartitionLocation,
};

/// Every `test_da` table's `da-2-bti-Partitions.db`, relative to
/// `$CQLITE_DATASETS_ROOT`. `simple_table`/`ttl_table`/`collection_table` are narrow
/// (DataOffset leaves); `wide_table` is wide (RowsOffset leaves, resolved via Rows.db
/// — its Data.db-layout order is what the reader's real successor path resolves).
const PARTITIONS_DBS: &[&str] = &[
    "sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Partitions.db",
    "sstables/test_da/ttl_table-de3b579064e711f19ad401a8c8227b11/da-2-bti-Partitions.db",
    "sstables/test_da/collection_table-de2c155064e711f19ad401a8c8227b11/da-2-bti-Partitions.db",
    "sstables/test_da/wide_table-9099a7c06c1811f19864870fb8444786/da-2-bti-Partitions.db",
];

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn loc_offset(loc: &BtiPartitionLocation) -> u64 {
    // For the ORACLE the RowsOffset vs DataOffset distinction is irrelevant: the two
    // resolution paths (DFS reference and local walk) both return the SAME
    // `BtiPartitionLocation` variant for the same partition, so comparing the raw
    // offset value proves they resolved the identical trie node. (The reader's own
    // successor path then resolves a RowsOffset through Rows.db to a Data.db offset;
    // that resolution is a pure function of the RowsOffset, so identical RowsOffsets
    // yield identical Data.db offsets — see the reader-level end-to-end read tests.)
    match loc {
        BtiPartitionLocation::DataOffset(o) => *o,
        BtiPartitionLocation::RowsOffset(o) => *o,
    }
}

/// For every partition in a real `da` Partitions.db, the O(depth) local successor
/// walk resolves the identical successor the whole-trie DFS + sort produced.
#[test]
fn local_walk_end_bound_matches_dfs_for_all_partitions() {
    let Some(root) = datasets_root() else {
        eprintln!("SKIP (#2058): CQLITE_DATASETS_ROOT not set");
        return;
    };

    let mut tables_checked = 0usize;
    for rel in PARTITIONS_DBS {
        let path = root.join(rel);
        if !path.exists() {
            eprintln!("SKIP (#2058): fixture not found at {path:?}");
            continue;
        }
        let file_bytes =
            std::fs::read(&path).unwrap_or_else(|e| panic!("#2058: cannot read {path:?}: {e}"));

        // REFERENCE: the old whole-trie DFS enumeration in byte-comparable order.
        // `iterate_partition_locations_in_bti_file` is the exact offset-only
        // enumeration the pre-#2058 `bti_partition_offsets` cache was built from.
        let mut cursor = std::io::Cursor::new(file_bytes.clone());
        let locations = iterate_partition_locations_in_bti_file(&mut cursor)
            .unwrap_or_else(|e| panic!("#2058: DFS enumeration failed for {rel}: {e}"));

        // Fixture present => must have partitions (never let a truncation regression
        // pass on empty data).
        assert!(
            !locations.is_empty(),
            "#2058: {rel} yielded zero partitions (fixture present but empty?)"
        );

        // The pre-#2058 sorted-offset array the reader binary-searched.
        let mut sorted: Vec<u64> = locations.iter().map(loc_offset).collect();
        sorted.sort_unstable();

        // We also need each partition's byte-comparable trie KEY to drive the local
        // walk. Enumerate the full `(reconstructed_key, location)` DFS for that.
        let mut cursor2 = std::io::Cursor::new(file_bytes.clone());
        let entries = cqlite_core::storage::sstable::bti::parser::iterate_partitions_in_bti_file(
            &mut cursor2,
        )
        .unwrap_or_else(|e| panic!("#2058: keyed DFS enumeration failed for {rel}: {e}"));
        assert_eq!(
            entries.len(),
            locations.len(),
            "#2058: keyed and offset-only DFS must enumerate the same partition count"
        );

        for (key, loc) in &entries {
            let target_off = loc_offset(loc);

            // REFERENCE successor: smallest sorted offset strictly greater than the
            // target's, or None for the last partition (== the old binary search).
            let idx = sorted.partition_point(|&o| o <= target_off);
            let expected: Option<u64> = sorted.get(idx).copied();

            // NEW path: O(depth) local strict-ceiling walk keyed on the SAME trie key.
            let got = partition_successor_in_bti_slice_for_test(&file_bytes, key)
                .unwrap_or_else(|e| {
                    panic!("#2058: local successor walk errored for {rel} key {key:02x?}: {e}")
                })
                .map(|l| loc_offset(&l));

            assert_eq!(
                got, expected,
                "#2058 TRUNCATION GUARD: local walk end-bound must equal the whole-trie DFS \
                 successor for {rel} partition key {key:02x?} (target offset {target_off}); \
                 a divergence here would silently truncate this partition's read",
            );
        }

        eprintln!(
            "#2058: {rel} — {} partitions, local walk == DFS successor for ALL",
            entries.len()
        );
        tables_checked += 1;
    }

    if tables_checked == 0 {
        eprintln!("SKIP (#2058): no test_da BTI fixtures present");
    }
}
