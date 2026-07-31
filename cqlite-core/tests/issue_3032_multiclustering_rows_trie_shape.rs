//! Issue #3032: the `test_da/multiclustering_table` fixture's `Rows.db` row-index
//! tries have a root whose LAST CHILD is WIDER than 2 bytes.
//!
//! ## Why this fixture exists
//!
//! The pre-#3002 root base was `RowsOffset + key_length` — 2 bytes low, because it
//! omitted the `u16` prefix `writeWithShortLength` emits
//! (`BtiTableWriter.IndexWriter.append` captures `basePosition` AFTER the write;
//! `BtiTableReader.retrieveEntryIfAcceptable` reads it back as `in.getFilePointer()`
//! after `readWithShortLength`). On the ONLY wide fixture in the corpus at the time
//! — `test_da/wide_table`, `PRIMARY KEY (pk, ck)` with a single `int` clustering
//! column — every row-index separator is `40 80 00 00 <byte>`, so the trie is a
//! chain of single-transition nodes and the root's only child is a 2-byte
//! `SINGLE_NOPAYLOAD_4`. `root - 2` therefore landed exactly on that child's FIRST
//! byte and parsed as a perfectly well-formed node. A wrong offset that parses
//! cleanly is the worst kind: it narrows to a bogus clustering window instead of
//! erroring.
//!
//! This fixture removes that coincidence. `PRIMARY KEY (pk, bucket, seq)` is a
//! COMPOUND clustering key of two components of DIFFERING types (`text`, `int`)
//! whose `bucket` values have distinct first bytes, so the separators branch
//! immediately below the root and the root's last child is a MULTI-TRANSITION node.
//! `root - 2` then lands strictly INSIDE that child rather than on its header — the
//! discriminating property asserted below.
//!
//! ## What is authority here
//!
//! Every expectation is a structural property DECODED from the Cassandra 5.0-written
//! bytes (via the production `resolve_rows_db_entry` + `parse_bti_node_for_test`),
//! read against `TrieNode.java`'s 16 ordinals from cassandra-5.0.8. Nothing is
//! asserted from CQLite's previous behaviour, and no node type is inferred from a
//! byte pattern (no-heuristics, issue #28) — the ordinal IS the header byte's high
//! nibble by format definition.
//!
//! Excluded under `tombstones`: that build serves reads by a full-scan filter, so the
//! clustering-window path is compiled out there (mirrors
//! `issue_3002_bti_rows_root_base.rs`).
#![cfg(not(feature = "tombstones"))]

use cqlite_core::storage::sstable::bti::{
    lookup_raw_key_in_bti_partitions_db, parse_bti_node_for_test, resolve_rows_db_entry,
    rows_node_serialized_extent_end_for_test, BtiNodeData, BtiPartitionLocation,
};
use std::io::Cursor;
use std::path::{Path, PathBuf};

/// Relative path of the compound-clustering BTI fixture directory.
const MC_DIR: &str = "sstables/test_da/multiclustering_table-fd74ad508d2311f1a29b6d2c15dcffdf";
/// Component prefix of the fixture's single compacted generation.
const GEN: &str = "da-2-bti";

/// The partition keys the fixture was generated with (`gen-multiclustering-bti.sh`,
/// `SHAPES=1:3:60,2:5:32,3:8:16`). Deliberately NON-uniform shapes, so the three
/// tries differ from each other.
const PKS: [i32; 3] = [1, 2, 3];

/// `TrieNode` ordinals (cassandra-5.0.8 `TrieNode.java`) that are strictly WIDER than
/// the 2-byte `SINGLE_NOPAYLOAD_4`, and so make `root - 2` land mid-node:
/// `SINGLE_16` (4), the `SPARSE_*` family (5..=9) and the `DENSE_*` family (10..=15).
fn ordinal_is_wide_enough(ordinal: u8) -> bool {
    ordinal == 4 || (5..=15).contains(&ordinal)
}

/// Human name of a `TrieNode` ordinal, for assertion messages.
fn ordinal_name(ordinal: u8) -> &'static str {
    match ordinal {
        0 => "PAYLOAD_ONLY",
        1 => "SINGLE_NOPAYLOAD_4",
        2 => "SINGLE_8",
        3 => "SINGLE_NOPAYLOAD_12",
        4 => "SINGLE_16",
        5 => "SPARSE_8",
        6 => "SPARSE_12",
        7 => "SPARSE_16",
        8 => "SPARSE_24",
        9 => "SPARSE_40",
        10 => "DENSE_12",
        11 => "DENSE_16",
        12 => "DENSE_24",
        13 => "DENSE_32",
        14 => "DENSE_40",
        _ => "LONG_DENSE",
    }
}

/// Fail-closed switch: when set, an absent fixture is a hard FAILURE instead of a
/// clean skip, so this lane can never green-pass without running.
fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Datasets root: `CQLITE_DATASETS_ROOT` when it holds the fixture, else the in-repo
/// committed corpus.
fn datasets_root() -> Option<PathBuf> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent repo dir")
        .join("test-data")
        .join("datasets");
    let candidates = [
        std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(PathBuf::from),
        Some(repo),
    ];
    let found = candidates
        .into_iter()
        .flatten()
        .find(|root| root.join(MC_DIR).join(format!("{GEN}-Rows.db")).exists());
    if found.is_none() {
        let msg = format!(
            "{MC_DIR}/{GEN}-Rows.db not found under CQLITE_DATASETS_ROOT nor the in-repo corpus"
        );
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but {msg} — fail-closed"
        );
        eprintln!("SKIP: {msg}");
    }
    found
}

/// Read one component of the fixture, SKIPping when absent.
fn read_component(rel: &str) -> Option<Vec<u8>> {
    let root = datasets_root()?;
    let path = root.join(MC_DIR).join(rel);
    match std::fs::read(&path) {
        Ok(b) if !b.is_empty() => Some(b),
        Ok(_) => panic!(
            "fixture {} is present but EMPTY — never pass on it",
            path.display()
        ),
        Err(e) => {
            let msg = format!("cannot read {}: {e}", path.display());
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but {msg} — fail-closed"
            );
            eprintln!("SKIP: {msg}");
            None
        }
    }
}

/// The `(rows_db, partitions_db)` pair, or `None` to SKIP.
fn mc_components() -> Option<(Vec<u8>, Vec<u8>)> {
    let rows = read_component(&format!("{GEN}-Rows.db"))?;
    let partitions = read_component(&format!("{GEN}-Partitions.db"))?;
    Some((rows, partitions))
}

/// Absolute offsets of every child of a decoded node, in transition order.
fn child_offsets(data: &BtiNodeData) -> Vec<u64> {
    match data {
        BtiNodeData::PayloadOnly { .. } => Vec::new(),
        BtiNodeData::Single { transition } => vec![transition.child.distance],
        BtiNodeData::Sparse { transitions } => {
            transitions.iter().map(|t| t.child.distance).collect()
        }
        BtiNodeData::Dense { children, .. } => children
            .iter()
            .filter_map(|c| c.as_ref().map(|p| p.distance))
            .collect(),
    }
}

/// THE discriminating property (issue #3032): for every indexed partition, the node
/// immediately preceding the row-index trie root — the root's LAST child, since
/// Cassandra's incremental trie writer serializes children before parents — is WIDER
/// than 2 bytes, so the pre-#3002 base (`root - 2`) lands strictly INSIDE it rather
/// than on a node header.
#[test]
fn rows_trie_root_last_child_is_wider_than_two_bytes() {
    let Some((rdb, pdb)) = mc_components() else {
        return;
    };

    // `(block_count, last-child byte length)` per partition, collected so the
    // heterogeneity requirement below is asserted from DECODED shape, not assumed.
    let mut shapes: Vec<(u32, usize)> = Vec::with_capacity(PKS.len());

    for pk in PKS {
        let mut cur = Cursor::new(pdb.clone());
        let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &pk.to_be_bytes())
            .expect("Partitions.db lookup must succeed")
            .expect("pk must be present");
        let BtiPartitionLocation::RowsOffset(rows_offset) = loc else {
            panic!("pk={pk} must be a WIDE partition with a Rows.db entry, got {loc:?}");
        };
        let rows_offset = rows_offset as usize;

        let header = resolve_rows_db_entry(&rdb, rows_offset).expect("entry must deserialize");
        let root = header
            .require_trie_root()
            .unwrap_or_else(|e| panic!("pk={pk}: Cassandra-written root must validate: {e}"));

        // A single-block partition has no usable row index: this fixture must have
        // multi-block tries for the window-narrowing paths to be exercised at all.
        assert!(
            header.block_count > 1,
            "pk={pk}: block_count must exceed 1, got {}",
            header.block_count
        );

        let root_node =
            parse_bti_node_for_test(&rdb[root..], root as u64).expect("root node must parse");
        let children = child_offsets(&root_node.data);
        assert!(
            !children.is_empty(),
            "pk={pk}: a row-index root indexing {} blocks must have children",
            header.block_count
        );

        // Children are written before their parent, in transition order, so the LAST
        // transition's child is the node closest to (and immediately before) the root.
        let last_child = *children
            .last()
            .expect("children is non-empty (asserted above)");
        let max_child = children.iter().copied().max().unwrap_or(last_child);
        assert_eq!(
            last_child, max_child,
            "pk={pk}: the last transition's child must be the highest-offset child \
             (children are serialized before parents, in transition order)"
        );
        let last_child = last_child as usize;

        // Adjacency: the last child's serialized bytes end exactly where the root
        // begins. This is what makes `root - 2` a probe of THAT node.
        let child_end = rows_node_serialized_extent_end_for_test(&rdb, last_child)
            .unwrap_or_else(|| panic!("pk={pk}: last child at {last_child} must have an extent"));
        assert_eq!(
            child_end, root,
            "pk={pk}: the root's last child must end exactly at the root offset {root}"
        );

        let child_header = rdb[last_child];
        let child_ordinal = (child_header >> 4) & 0x0F;
        let child_len = root - last_child;

        eprintln!(
            "pk={pk} RowsOffset={rows_offset} root={root} blocks={} \
             root_ordinal={} ({}) root_children={} last_child@{last_child} \
             len={child_len} ordinal={child_ordinal} ({}) type={:?}",
            header.block_count,
            (rdb[root] >> 4) & 0x0F,
            ordinal_name((rdb[root] >> 4) & 0x0F),
            children.len(),
            ordinal_name(child_ordinal),
            parse_bti_node_for_test(&rdb[last_child..], last_child as u64)
                .expect("last child must parse")
                .node_type,
        );

        assert!(
            ordinal_is_wide_enough(child_ordinal),
            "pk={pk}: the root's last child at {last_child} is ordinal {child_ordinal} \
             ({}) — this fixture exists precisely to make it SINGLE_16 / SPARSE_* / \
             DENSE_*, so that `root - 2` cannot land on a node header",
            ordinal_name(child_ordinal)
        );

        // THE property: the pre-#3002 base lands strictly inside the last child.
        assert!(
            child_len > 2,
            "pk={pk}: the root's last child is {child_len} bytes — `root - 2` would land \
             on its header byte (the wide_table coincidence this fixture removes)"
        );
        assert!(
            root - 2 > last_child && root - 2 < child_end,
            "pk={pk}: root-2 ({}) must lie strictly INSIDE the last child \
             [{last_child}, {child_end})",
            root - 2
        );
        shapes.push((header.block_count, child_len));
    }

    // Issue #3032 scope (c): the three partitions must NOT be structurally identical.
    // `wide_table`'s three tries are byte-identical in shape, which is part of why it
    // could not discriminate anything that depends on per-partition trie structure.
    assert_eq!(shapes.len(), PKS.len(), "every pk must have been decoded");
    let mut distinct = shapes.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        shapes.len(),
        "the partitions' (block_count, root-last-child length) shapes must all DIFFER, \
         got {shapes:?}"
    );
}
