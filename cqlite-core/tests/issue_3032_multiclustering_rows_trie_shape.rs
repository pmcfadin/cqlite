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
//! bytes, read against the pinned `cassandra-5.0.8` sources:
//!
//! * `io/tries/TrieNode.java` — the 16-entry `Types.values` array fixes the
//!   ordinal↔type mapping (`values[10] == DENSE_12`), and its static initializer
//!   asserts `values[i].ordinal == i`.
//! * `io/sstable/format/bti/BtiTableWriter.IndexWriter.append` /
//!   `BtiTableReader.retrieveEntryIfAcceptable` — the root-delta base is
//!   `RowsOffset + 2 + key_length` (the `u16` `writeWithShortLength` prefix).
//! * `io/sstable/format/bti/RowIndexWriter.add`/`complete` — one separator per
//!   block (the first being `ByteComparable.EMPTY`) plus a trailing separator, so a
//!   faithful traversal yields `blockCount + 1` entries.
//!
//! Nothing is asserted from CQLite's previous behaviour, and no node type is
//! inferred from a byte pattern (no-heuristics, issue #28) — the ordinal IS the
//! header byte's high nibble by format definition. The block offsets are
//! independently cross-checked against the committed `sstabledump` golden.
//!
//! Excluded under `tombstones`: that build serves reads by a full-scan filter, so the
//! clustering-window path is compiled out there (mirrors
//! `issue_3002_bti_rows_root_base.rs`).
#![cfg(not(feature = "tombstones"))]

use cqlite_core::storage::sstable::bti::{
    iterate_rows_in_bti_trie, lookup_raw_key_in_bti_partitions_db, parse_bti_node_for_test,
    resolve_rows_db_entry, rows_floor_block_for_test, rows_node_serialized_extent_end_for_test,
    rows_strict_ceiling_block_for_test, BtiNodeData, BtiNodeType, BtiPartitionLocation,
    RowsTrieRootRejectReason,
};
use std::collections::BTreeSet;
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

/// `TrieNode` ordinal for `DENSE_12` — `Types.values[10]` in the pinned
/// cassandra-5.0.8 `io/tries/TrieNode.java` (whose static initializer asserts
/// `values[i].ordinal == i`). This is the type EVERY partition's root child decodes
/// to in this fixture, and it is the discriminating shape the fixture exists to
/// provide: a `DENSE_12` header is followed by a dense transition range plus 12-bit
/// packed pointers, so it is always WIDER than the 2-byte `SINGLE_NOPAYLOAD_4` that
/// made `root - 2` land benignly on `wide_table`.
const ORDINAL_DENSE_12: u8 = 10;

/// `TrieNode` ordinal for `SINGLE_8` (`Types.values[2]`): a 1-byte transition + a
/// 1-byte backward delta, and — unlike the `SINGLE_NOPAYLOAD_*` types — able to
/// carry a payload. Every one of this fixture's row-index roots is this type.
const ORDINAL_SINGLE_8: u8 = 2;

/// Width in bytes of the `u16` length prefix `writeWithShortLength` emits before a
/// `TrieIndexEntry`'s partition key — the ENTIRE difference between the correct
/// root-delta base (`RowsOffset + 2 + key_length`) and the pre-#3002 one
/// (`RowsOffset + key_length`).
const SHORT_LENGTH_PREFIX_LEN: usize = 2;

/// Within-partition byte offset of the FIRST row body in every partition of this
/// fixture: the `u16` key length (2) + the 4-byte `int` partition key + the 1-byte
/// LIVE `DeletionTime` sentinel. Independently cross-checked below against the
/// committed sstabledump golden (`row.position - partition.position`).
const BLOCK_0_OFFSET: u64 = 7;

/// Human name of a `TrieNode` ordinal (pinned `TrieNode.java` `Types.values`), for
/// assertion messages.
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

/// Read one component of the fixture, SKIPping when absent. A PRESENT-but-EMPTY
/// component is a hard failure — never a pass (issue #3032 AC8).
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

/// `RowsOffset` of `pk`, resolved through `Partitions.db` (never hardcoded).
fn rows_offset_of(pdb: &[u8], pk: i32) -> usize {
    let mut cur = Cursor::new(pdb.to_vec());
    let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &pk.to_be_bytes())
        .expect("Partitions.db lookup must succeed")
        .expect("pk must be present");
    match loc {
        BtiPartitionLocation::RowsOffset(o) => o as usize,
        other => panic!("pk={pk} must be a WIDE partition with a Rows.db entry, got {other:?}"),
    }
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

// ─────────────────────────────────────────────────────────────────────────────
// An INDEPENDENT `TrieIndexEntry` decoder.
//
// AC3 asserts the root offset `resolve_rows_db_entry` produces equals the
// CASSANDRA-DERIVED value. Deriving that value with production's own entry parser
// would be circular, so the entry is re-decoded here by hand from the layout
// `BtiTableWriter.IndexWriter.append` writes:
//
//   [u16 key_length][key][dataPosition unsigned vint][rootΔ zigzag vint]
//   [blockCount unsigned vint][DeletionTime]
//
// with `basePosition` captured AFTER `writeWithShortLength`, i.e. at
// `RowsOffset + 2 + key_length`.
// ─────────────────────────────────────────────────────────────────────────────

/// Read a Cassandra unsigned VInt independently of production code.
fn read_unsigned_vint(bytes: &[u8], pos: &mut usize) -> u64 {
    let first = bytes[*pos];
    *pos += 1;
    let extra = first.leading_ones() as usize;
    let mask: u64 = if extra >= 7 {
        0
    } else {
        (1u64 << (7 - extra)) - 1
    };
    let mut value = (first as u64) & mask;
    for _ in 0..extra {
        value = (value << 8) | bytes[*pos] as u64;
        *pos += 1;
    }
    value
}

/// The hand-decoded `TrieIndexEntry` fields this file needs.
struct EntryFields {
    /// `RowsOffset + 2 + key_length` — the Cassandra-true root-delta base.
    base: usize,
    /// `RowsOffset + key_length` — the pre-#3002 base, 2 bytes low.
    pre_3002_base: usize,
    /// The absolute file position of the partition's data.
    data_position: u64,
    /// The SIGNED (ZigZag) root delta, relative to `base`.
    root_delta: i64,
    /// Byte position of the root-delta vint (so a test can rewrite it).
    root_delta_at: usize,
    /// Width of the root-delta vint, in bytes.
    root_delta_width: usize,
    /// `TrieIndexEntry.blockCount`.
    block_count: u64,
}

/// Hand-decode the `TrieIndexEntry` at `rows_offset`.
fn decode_entry_independently(rdb: &[u8], rows_offset: usize) -> EntryFields {
    let key_length = u16::from_be_bytes([rdb[rows_offset], rdb[rows_offset + 1]]) as usize;
    let base = rows_offset + SHORT_LENGTH_PREFIX_LEN + key_length;
    let mut pos = base;
    let data_position = read_unsigned_vint(rdb, &mut pos);
    let root_delta_at = pos;
    let zig = read_unsigned_vint(rdb, &mut pos);
    let root_delta_width = pos - root_delta_at;
    let root_delta = ((zig >> 1) as i64) ^ -((zig & 1) as i64);
    let block_count = read_unsigned_vint(rdb, &mut pos);
    EntryFields {
        base,
        pre_3002_base: rows_offset + key_length,
        data_position,
        root_delta,
        root_delta_at,
        root_delta_width,
        block_count,
    }
}

/// `(partition_absolute_position, sorted within-partition row offsets)` per pk, read
/// off the committed `sstabledump` golden — an oracle INDEPENDENT of every CQLite
/// decoder. `None` SKIPs (hard FAIL under `CQLITE_REQUIRE_FIXTURES=1`).
fn golden_row_offsets() -> Option<Vec<(i32, u64, BTreeSet<u64>)>> {
    let text = String::from_utf8(read_component(&format!("{GEN}-Data.db.jsonl"))?).ok()?;
    let mut out = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("golden line must be JSON");
        let part = v.get("partition").expect("golden line has a partition");
        let pk: i32 = part
            .get("key")
            .and_then(|k| k.as_array())
            .and_then(|a| a.first())
            .and_then(|k| k.as_str())
            .and_then(|s| s.parse().ok())
            .expect("golden partition key is a single int");
        let part_pos = part
            .get("position")
            .and_then(|p| p.as_u64())
            .expect("golden partition position");
        let rows = v
            .get("rows")
            .and_then(|r| r.as_array())
            .expect("golden partition has rows");
        let offsets: BTreeSet<u64> = rows
            .iter()
            .filter(|r| r.get("type").and_then(|t| t.as_str()) == Some("row"))
            .map(|r| {
                r.get("position")
                    .and_then(|p| p.as_u64())
                    .expect("golden row position")
                    - part_pos
            })
            .collect();
        assert!(
            !offsets.is_empty(),
            "golden partition pk={pk} has no rows — a present-but-empty golden is a FAILURE"
        );
        out.push((pk, part_pos, offsets));
    }
    assert_eq!(
        out.len(),
        PKS.len(),
        "the committed golden must cover all {} partitions",
        PKS.len()
    );
    Some(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// AC2 — the root's last child is a DENSE_12, by name.
// ─────────────────────────────────────────────────────────────────────────────

/// THE discriminating property (issue #3032): for every indexed partition, the node
/// immediately preceding the row-index trie root — the root's LAST child, since
/// Cassandra's incremental trie writer serializes children before parents — is a
/// `DENSE_12` (pinned `TrieNode.java` `Types.values[10]`), a MULTI-transition type
/// that is always wider than 2 bytes. So the pre-#3002 base (`root - 2`) lands
/// strictly INSIDE it rather than on a node header.
///
/// The type is pinned BY NAME, not merely as "some node longer than 2 bytes": a
/// regeneration that changed the bucket-name spread could quietly collapse the child
/// back to a `SINGLE_*` chain and re-introduce exactly the `wide_table` coincidence
/// this fixture exists to escape. If this assertion fails after a deliberate
/// regeneration, RE-DERIVE the new shape from the bytes (and re-confirm `root - 2`
/// still lands mid-node) before re-pinning it.
#[test]
fn rows_trie_root_last_child_is_a_dense_12_node() {
    let Some((rdb, pdb)) = mc_components() else {
        return;
    };

    // `(block_count, last-child byte length)` per partition, collected so the
    // heterogeneity requirement below is asserted from DECODED shape, not assumed.
    let mut shapes: Vec<(u32, usize)> = Vec::with_capacity(PKS.len());

    for pk in PKS {
        let rows_offset = rows_offset_of(&pdb, pk);
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
        let child_node = parse_bti_node_for_test(&rdb[last_child..], last_child as u64)
            .expect("last child must parse");

        eprintln!(
            "pk={pk} RowsOffset={rows_offset} root={root} blocks={} \
             root_ordinal={} ({}) root_children={} last_child@{last_child} \
             len={child_len} ordinal={child_ordinal} ({}) type={:?}",
            header.block_count,
            (rdb[root] >> 4) & 0x0F,
            ordinal_name((rdb[root] >> 4) & 0x0F),
            children.len(),
            ordinal_name(child_ordinal),
            child_node.node_type,
        );

        // THE named shape: DENSE_12, asserted on BOTH the on-disk ordinal nibble and
        // the decoded node type — the two must agree, and both must name a DENSE node.
        assert_eq!(
            child_ordinal,
            ORDINAL_DENSE_12,
            "pk={pk}: the root's last child at {last_child} is ordinal {child_ordinal} \
             ({}), but this fixture exists precisely to make it DENSE_12 (ordinal \
             {ORDINAL_DENSE_12}, pinned TrieNode.java Types.values[10]) — a narrower \
             type re-introduces the wide_table coincidence where `root - 2` lands on a \
             node HEADER and parses benignly",
            ordinal_name(child_ordinal)
        );
        assert_eq!(
            child_node.node_type,
            BtiNodeType::Dense,
            "pk={pk}: the decoded node type must agree with the DENSE_12 ordinal"
        );
        match &child_node.data {
            BtiNodeData::Dense { children, .. } => {
                let present = children.iter().filter(|c| c.is_some()).count();
                assert!(
                    present > 1,
                    "pk={pk}: a DENSE_12 root child must carry MORE THAN ONE transition \
                     (multi-transition branching is what widens it past 2 bytes); got \
                     {present}"
                );
            }
            other => panic!("pk={pk}: DENSE_12 must decode as BtiNodeData::Dense, got {other:?}"),
        }

        // THE consequence: the pre-#3002 base lands strictly inside the last child.
        assert!(
            child_len > SHORT_LENGTH_PREFIX_LEN,
            "pk={pk}: the root's last child is {child_len} bytes — `root - 2` would land \
             on its header byte (the wide_table coincidence this fixture removes)"
        );
        assert!(
            root - SHORT_LENGTH_PREFIX_LEN > last_child
                && root - SHORT_LENGTH_PREFIX_LEN < child_end,
            "pk={pk}: root-2 ({}) must lie strictly INSIDE the last child \
             [{last_child}, {child_end})",
            root - SHORT_LENGTH_PREFIX_LEN
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

// ─────────────────────────────────────────────────────────────────────────────
// AC3 — the resolved root equals the Cassandra-derived value.
// ─────────────────────────────────────────────────────────────────────────────

/// `resolve_rows_db_entry`'s `trie_root` equals `RowsOffset + 2 + key_length +
/// root_delta` for EVERY indexed partition, where the entry fields are re-decoded
/// here by hand (never via the production parser under test), and the resolved offset
/// is cross-checked three independent ways:
///
/// 1. the node AT that offset is a payload-bearing `SINGLE_8` (`payloadFlags == 1`,
///    the 1-byte `SizedInts` block offset of block 0's `ByteComparable.EMPTY`
///    separator);
/// 2. a faithful traversal from it yields `blockCount + 1` separators
///    (`RowIndexWriter.add` writes one per block, `complete()` appends a trailing
///    one), strictly ascending, the first being the EMPTY separator at the partition
///    body start;
/// 3. every block offset it yields is a REAL row position in the committed
///    `sstabledump` golden — an oracle produced by Cassandra's own tooling.
#[test]
fn rows_trie_root_matches_the_cassandra_derived_base() {
    let Some((rdb, pdb)) = mc_components() else {
        return;
    };
    let Some(golden) = golden_row_offsets() else {
        return;
    };

    for pk in PKS {
        let rows_offset = rows_offset_of(&pdb, pk);
        let e = decode_entry_independently(&rdb, rows_offset);

        // The Cassandra-derived root: base (captured AFTER writeWithShortLength) plus
        // the signed delta. Computed from the hand-decoded entry, then required to
        // equal what production resolves.
        let derived_root = e.base as i64 + e.root_delta;
        assert!(
            derived_root > 0 && (derived_root as usize) < rows_offset,
            "pk={pk}: the trie body precedes its entry; derived root {derived_root} is \
             not below RowsOffset {rows_offset}"
        );
        let derived_root = derived_root as usize;

        let header = resolve_rows_db_entry(&rdb, rows_offset).expect("entry must deserialize");
        let trie_root = header.require_trie_root().unwrap_or_else(|err| {
            panic!("pk={pk}: the Cassandra-written root must validate structurally: {err}")
        });
        assert_eq!(
            trie_root, derived_root,
            "pk={pk}: resolve_rows_db_entry must resolve the root to \
             RowsOffset({rows_offset}) + {SHORT_LENGTH_PREFIX_LEN} + key_length + \
             delta({}) = {derived_root}",
            e.root_delta
        );
        assert_eq!(
            header.block_count as u64, e.block_count,
            "pk={pk}: block_count must match the hand-decoded entry"
        );
        assert_eq!(
            header.data_position, e.data_position,
            "pk={pk}: data_position must match the hand-decoded entry"
        );

        // (1) The node AT the resolved root: SINGLE_8 carrying a payload.
        let header_byte = rdb[trie_root];
        assert_eq!(
            (header_byte >> 4) & 0x0F,
            ORDINAL_SINGLE_8,
            "pk={pk}: root node byte 0x{header_byte:02x} must be ordinal \
             {ORDINAL_SINGLE_8} (SINGLE_8), got {}",
            ordinal_name((header_byte >> 4) & 0x0F)
        );
        assert_eq!(
            header_byte & 0x0F,
            1,
            "pk={pk}: root payloadFlags must be 1 — a 1-byte SizedInts block offset, \
             i.e. block 0's ByteComparable.EMPTY separator payload lives ON the root"
        );
        let root_node = parse_bti_node_for_test(&rdb[trie_root..], trie_root as u64)
            .expect("root node must parse");
        assert_eq!(
            root_node.node_type,
            BtiNodeType::Single,
            "pk={pk}: SINGLE_8 must decode as a Single node"
        );

        // The root's serialized bytes end exactly at the entry — the root is the LAST
        // node written before its `TrieIndexEntry`.
        assert_eq!(
            rows_node_serialized_extent_end_for_test(&rdb, trie_root),
            Some(rows_offset),
            "pk={pk}: the root node must end exactly at its entry ({rows_offset})"
        );

        // (2) A faithful traversal: blockCount + 1 strictly ascending separators, the
        // first being ByteComparable.EMPTY at the partition body start.
        let entries =
            iterate_rows_in_bti_trie(&rdb, trie_root).expect("traversal from the root must work");
        assert_eq!(
            entries.len(),
            header.block_count as usize + 1,
            "pk={pk}: traversal must yield blockCount + 1 = {} separators (one per \
             block, first = ByteComparable.EMPTY, plus complete()'s trailing separator)",
            header.block_count as usize + 1
        );
        assert!(
            entries[0].0.is_empty(),
            "pk={pk}: the FIRST separator must be the empty key (ByteComparable.EMPTY), \
             got {:02x?}",
            entries[0].0
        );
        assert_eq!(
            entries[0].1.data_offset, BLOCK_0_OFFSET,
            "pk={pk}: the empty separator indexes block 0 at the partition body start"
        );
        for w in entries.windows(2) {
            assert!(
                w[0].0 < w[1].0,
                "pk={pk}: separators must be strictly ascending: {:02x?} then {:02x?}",
                w[0].0,
                w[1].0
            );
        }
        // Compound-clustering shape: every non-empty separator carries the OSS50
        // `NEXT_COMPONENT` byte for the FIRST component AND a second one introducing
        // the `seq` component — this is a two-component clustering key, which is the
        // whole point of the fixture.
        for (sep, _) in entries.iter().skip(1) {
            assert_eq!(
                sep.first().copied(),
                Some(0x40u8),
                "pk={pk}: every separator starts with the OSS50 NEXT_COMPONENT byte; \
                 got {sep:02x?}"
            );
            assert!(
                sep[1..].contains(&0x40u8),
                "pk={pk}: a two-component clustering separator carries a SECOND \
                 NEXT_COMPONENT byte before `seq`; got {sep:02x?}"
            );
        }

        // (3) Every block offset is a real row position in the Cassandra-produced
        // golden, and the entry's `data_position` is the partition's file position.
        let (_, part_pos, row_offsets) = golden
            .iter()
            .find(|(gpk, _, _)| *gpk == pk)
            .expect("the golden must cover every pk");
        assert_eq!(
            e.data_position, *part_pos,
            "pk={pk}: the entry's data_position must be the golden's partition position"
        );
        assert!(
            row_offsets.contains(&BLOCK_0_OFFSET),
            "pk={pk}: the golden's first row must sit at the partition body start \
             ({BLOCK_0_OFFSET}); offsets start at {:?}",
            row_offsets.iter().next()
        );
        // Every BLOCK separator (all but the last) points at a real row start...
        let (block_seps, trailing) = entries
            .split_at_checked(entries.len() - 1)
            .expect("entries is non-empty");
        for (sep, entry) in block_seps {
            assert!(
                row_offsets.contains(&entry.data_offset),
                "pk={pk}: block offset {} (separator {sep:02x?}) is not a row position \
                 in the committed sstabledump golden — the resolved root indexes bytes \
                 that are not row starts",
                entry.data_offset
            );
        }
        assert_eq!(
            block_seps.len(),
            header.block_count as usize,
            "pk={pk}: exactly blockCount separators index a block"
        );
        // ...while the LAST is `RowIndexWriter.complete()`'s trailing separator, which
        // bounds the partition ABOVE its final row rather than naming a block start.
        let last_row = *row_offsets
            .iter()
            .next_back()
            .expect("the golden partition has rows");
        let trailing_offset = trailing[0].1.data_offset;
        assert!(
            trailing_offset > last_row && !row_offsets.contains(&trailing_offset),
            "pk={pk}: complete()'s trailing separator must bound the partition ABOVE \
             its last row ({last_row}); got {trailing_offset}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AC4 — the pre-#3002 base is DISCRIMINATED by this fixture.
// ─────────────────────────────────────────────────────────────────────────────

/// Rewrite every partition's root delta so a CORRECT reader resolves the offset the
/// PRE-#3002 reader would have computed (`RowsOffset + key_length + delta`, 2 bytes
/// low). The trie bytes are untouched and the re-encoded ZigZag vint is asserted to
/// keep its original width, so every other offset in the file stays valid: the ONLY
/// difference is the base the delta is measured against. Synthesized here — no new
/// binary fixture is shipped.
///
/// This is byte-exactly the input the old reader fed to its own traversal: the same
/// `Rows.db`, the same `RowsOffset`, and the resolved root the old base arithmetic
/// produced. It lets the PRODUCTION resolver/validator/walks be pointed at that
/// offset without reintroducing the removed code path.
fn rows_db_resolving_to_the_pre_3002_reader_base(rdb: &[u8], offsets: &[usize]) -> Vec<u8> {
    let mut out = rdb.to_vec();
    for &rows_offset in offsets {
        let e = decode_entry_independently(rdb, rows_offset);
        // Same trie, base 2 bytes LOWER ⇒ a correct reader lands 2 bytes LOWER.
        let shifted = e.root_delta - SHORT_LENGTH_PREFIX_LEN as i64;
        let zig = ((shifted << 1) ^ (shifted >> 63)) as u64;
        assert!(
            zig < 128 && e.root_delta_width == 1,
            "this fixture's deltas are 1-byte vints ({} → {shifted}); a width change \
             would move every following byte",
            e.root_delta
        );
        out[e.root_delta_at] = zig as u8;
    }
    out
}

/// AC4 — on THIS fixture the pre-#3002 root base is DISCRIMINATED: it lands strictly
/// mid-node and cannot resolve at all.
///
/// ## Why this test cannot pass vacuously
///
/// The failure mode being guarded is a future regeneration whose trie shape makes the
/// old and new bases coincide or both parse — exactly the `test_da/wide_table`
/// situation this issue exists to escape (there, `root - 2` was the FIRST byte of a
/// well-formed 2-byte `SINGLE_NOPAYLOAD_4`, so the wrong base parsed cleanly and
/// narrowed to a bogus window instead of erroring). Three explicit guards make that
/// regeneration FAIL here rather than pass quietly:
///
/// * `pre_3002_base != base` is asserted DIRECTLY, and pinned to a difference of
///   exactly `SHORT_LENGTH_PREFIX_LEN`;
/// * `old_root != correct_root` is asserted DIRECTLY, so a fixture where the two
///   coincide can never satisfy this test;
/// * the old root must land STRICTLY INSIDE the root's last child (not on its header)
///   AND the production resolver must REFUSE it with a named structural reason —
///   `Ok(_)` at the old base is a hard failure, not a tolerated outcome.
#[test]
fn pre_3002_reader_base_lands_mid_node_and_cannot_resolve() {
    let Some((rdb, pdb)) = mc_components() else {
        return;
    };

    let offsets: Vec<usize> = PKS.iter().map(|&pk| rows_offset_of(&pdb, pk)).collect();
    let patched = rows_db_resolving_to_the_pre_3002_reader_base(&rdb, &offsets);
    assert_ne!(
        patched, rdb,
        "the pre-#3002-base simulation must change the entry bytes"
    );

    for (pk, rows_offset) in PKS.iter().copied().zip(offsets.iter().copied()) {
        let e = decode_entry_independently(&rdb, rows_offset);

        // ---- Guard 1: the two bases genuinely DIFFER -------------------------
        assert_ne!(
            e.pre_3002_base, e.base,
            "pk={pk}: the pre-#3002 base and the Cassandra-true base must differ — a \
             fixture where they coincide discriminates NOTHING"
        );
        assert_eq!(
            e.base - e.pre_3002_base,
            SHORT_LENGTH_PREFIX_LEN,
            "pk={pk}: the two bases differ by exactly the u16 writeWithShortLength \
             prefix"
        );

        let good = resolve_rows_db_entry(&rdb, rows_offset).expect("baseline entry");
        let correct_root = good
            .require_trie_root()
            .unwrap_or_else(|err| panic!("pk={pk}: the real root must validate: {err}"));
        let old_root = (e.pre_3002_base as i64 + e.root_delta) as usize;

        // ---- Guard 2: the two ROOTS genuinely DIFFER -------------------------
        assert_ne!(
            old_root, correct_root,
            "pk={pk}: the pre-#3002 base must resolve to a DIFFERENT offset than the \
             correct base — otherwise this fixture cannot discriminate the bug"
        );
        assert_eq!(
            correct_root - old_root,
            SHORT_LENGTH_PREFIX_LEN,
            "pk={pk}: the old root is exactly {SHORT_LENGTH_PREFIX_LEN} bytes below the \
             correct one"
        );

        // ---- Guard 3a: the old root is STRICTLY MID-NODE ---------------------
        // (Not a node header: it is interior to the root's last child, whose extent
        // ends exactly at the root.)
        let root_node = parse_bti_node_for_test(&rdb[correct_root..], correct_root as u64)
            .expect("root must parse");
        let last_child = *child_offsets(&root_node.data)
            .last()
            .unwrap_or_else(|| panic!("pk={pk}: the root must have children"))
            as usize;
        let child_end = rows_node_serialized_extent_end_for_test(&rdb, last_child)
            .unwrap_or_else(|| panic!("pk={pk}: the last child must have an extent"));
        assert_eq!(child_end, correct_root, "pk={pk}: adjacency");
        assert!(
            old_root > last_child && old_root < child_end,
            "pk={pk}: the pre-#3002 root {old_root} must lie STRICTLY INSIDE the root's \
             last child [{last_child}, {child_end}) — landing on the child's HEADER is \
             the wide_table coincidence that made the wrong base parse benignly"
        );

        // ---- Guard 3b: the production resolver REFUSES the old root ----------
        let bad = resolve_rows_db_entry(&patched, rows_offset)
            .expect("the ENTRY must still deserialize — only the ROOT is unusable");
        assert_eq!(
            (bad.data_position, bad.block_count),
            (good.data_position, good.block_count),
            "pk={pk}: only the root is affected; the other entry fields are untouched"
        );
        assert_eq!(
            bad.trie_root_offset(),
            None,
            "pk={pk}: the pre-#3002 root must NOT be exposed as usable"
        );
        let rejection = match bad.trie_root {
            Ok(root) => panic!(
                "pk={pk}: the pre-#3002 root {root:?} was ACCEPTED — this fixture exists \
                 precisely so that base cannot resolve"
            ),
            Err(rejection) => rejection,
        };
        assert_eq!(rejection.resolved_offset, old_root as i64);
        assert_eq!(
            rejection.reason,
            RowsTrieRootRejectReason::ChildlessRootWithoutPayload {
                header_byte: rdb[old_root]
            },
            "pk={pk}: 2 bytes into the DENSE_12 child the byte is 0x{:02x}, a childless \
             PayloadOnly shape TrieNode.typeFor never emits",
            rdb[old_root]
        );

        // ---- Guard 3c: every walk from the old root ERRORS -------------------
        // The correct root drives a real window; the old root cannot even be parsed,
        // so there is no window at all — a demonstrably different outcome, not a
        // subtly-wrong one. (On `wide_table` the same probe returns a well-formed
        // 38-entry traversal, which is why that fixture could not discriminate.)
        assert!(
            parse_bti_node_for_test(&rdb[old_root..], old_root as u64).is_err(),
            "pk={pk}: the node at the pre-#3002 root must NOT parse"
        );
        assert!(
            iterate_rows_in_bti_trie(&rdb, old_root).is_err(),
            "pk={pk}: a traversal from the pre-#3002 root must error"
        );
        assert!(
            rows_floor_block_for_test(&rdb, old_root, b"").is_err(),
            "pk={pk}: the floor walk from the pre-#3002 root must error"
        );
        assert!(
            rows_strict_ceiling_block_for_test(&rdb, old_root, &[0xFFu8; 8]).is_err(),
            "pk={pk}: the ceiling walk from the pre-#3002 root must error"
        );

        // ...while the SAME walks from the correct root all succeed and cover the
        // partition — so the discrimination is between "works" and "cannot work",
        // proven on both sides in the same test.
        let entries =
            iterate_rows_in_bti_trie(&rdb, correct_root).expect("the correct root traverses");
        assert_eq!(entries.len(), good.block_count as usize + 1);
        assert_eq!(
            rows_floor_block_for_test(&rdb, correct_root, b"")
                .expect("the correct floor walk succeeds")
                .map(|b| b.data_offset),
            Some(BLOCK_0_OFFSET),
            "pk={pk}: the correct root floors an open lower bound to block 0"
        );
    }
}
