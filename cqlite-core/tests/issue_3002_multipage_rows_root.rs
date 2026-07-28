//! Issue #3002: the `Rows.db` root-placement invariant OUTSIDE the single-page
//! regime.
//!
//! `validate_rows_trie_root` enforces the writer-ordering equality — the root
//! node's serialized extent ends EXACTLY at its `TrieIndexEntry`'s `RowsOffset`
//! — as a HARD error in `iterate_rows_for_partition` and `verify`, not merely as
//! the benign "cannot narrow" read-path fallback. The real `test_da/wide_table`
//! fixture only exercises it against ~200-byte tries (39 separators, three
//! identically-shaped partitions), i.e. a row index that fits in one 4 KiB page.
//!
//! Cassandra emits the row-index trie via `IncrementalTrieWriterPageAware`, which
//! pads to the next 4096-byte page boundary whenever a node or branch would
//! otherwise straddle a page (`PageAware.java`'s page constant; see
//! docs/sstables-definitive-guide/chapters/17-bti-formats.md §"Root placement
//! invariant"). Two properties of that regime are invisible in the fixture:
//!
//!   1. **Wide backward pointers.** Once a trie spans pages, a parent's backward
//!      child deltas exceed 255/65535, so the writer picks `SPARSE_16`/`_24`/`_40`
//!      (or the `Dense*` equivalents) and the reader's extent computation must
//!      size the pointer area by ORDINAL, not by a small-trie coincidence.
//!   2. **Padding immediately BEFORE the root.** A page-aware writer may promote
//!      the root onto a fresh page, so the bytes just below the root are `0x00`
//!      pad — while the bytes just ABOVE it are still the entry.
//!
//! A false rejection in either case is not the documented benign fallback: it is a
//! `BtiTrieCorrupt` finding from `verify` and a hard `Error::Parse` from
//! `iterate_rows_for_partition` on a VALID Cassandra file. This lane pins both.
//!
//! ## What this lane does and does NOT cover
//!
//! * `multi_page_rows_trie_from_the_production_writer_validates_and_walks` builds
//!   a GENUINELY multi-page trie (> 3 pages of nodes) through CQLite's production
//!   `RowsTrieWriter`, so property (1) is exercised end-to-end by real writer
//!   output — including the wide-pointer root ordinal the reader must size.
//! * `page_padded_root_on_a_fresh_page_validates_and_walks` covers property (2).
//!   CQLite's `RowsTrieWriter` is deliberately NOT page-aware (it writes the trie
//!   contiguously), so the padded LAYOUT is constructed byte-wise here — the root
//!   placed at a 4096 boundary with a `0x00` pad run below it and its children
//!   more than a page away.
//! * NOT covered: byte-for-byte agreement with a REAL Cassandra-written multi-page
//!   `Rows.db`. The corpus contains no such file (`test_da/wide_table`'s is the
//!   only non-empty `Rows.db`, and it is single-page), so
//!   `IncrementalTrieWriterPageAware`'s own placement DECISIONS — which nodes get
//!   promoted to a fresh page, how a branch is laid out within one — are not
//!   pinned here. What is pinned is that the reader's root validation and its
//!   floor/ceiling walks hold under the two layout properties that regime
//!   introduces.
//!
//! Excluded under `tombstones` (that build compiles the clustering-window path,
//! and hence the floor/ceiling walks, out entirely) — mirroring
//! `issue_3002_bti_rows_root_base.rs`.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::storage::sstable::bti::{
    iterate_rows_in_bti_trie, parse_bti_node_for_test, resolve_rows_db_entry,
    rows_floor_block_for_test, rows_node_serialized_extent_end_for_test,
    rows_strict_ceiling_block_for_test, BtiNodeData, BtiRowIndexEntry,
};
use cqlite_core::storage::sstable::writer::partitions_writer::{RowIndexBlock, RowsTrieWriter};

/// `PageAware.PAGE_SIZE` (cassandra-5.0.8 `PageAware.java`).
const PAGE_SIZE: usize = 4096;

/// Blocks in the production-writer tries. Each block contributes a
/// `PayloadOnly` leaf plus its share of the internal nodes, so this is comfortably
/// more than three pages of trie body (asserted, never assumed).
const MANY_BLOCKS: usize = 4000;

/// The OSS50 byte-comparable separator for `int` clustering value `ck`: the
/// `NEXT_COMPONENT` byte then the sign-flipped big-endian int — the exact shape the
/// real fixture's separators carry (`40 80 00 00 08`). Written literally so this
/// lane does not depend on the encoder under test elsewhere in #3002.
fn separator(ck: i32) -> Vec<u8> {
    let mut key = vec![0x40u8];
    key.extend_from_slice(&(ck ^ i32::MIN).to_be_bytes());
    key
}

/// A separator whose FIRST byte varies with `i`: `[1 + i / 16, (i % 16) * 16]`.
///
/// Ascending in `i` (equal-length keys compare lexicographically), and — unlike
/// [`separator`] — it gives the trie ROOT one child per distinct leading byte
/// instead of a single shared `NEXT_COMPONENT` transition, which is how a root ends
/// up with pointers reaching across pages. The leading byte starts at 1 so
/// `[0x00, 0x00]` is a key genuinely BELOW every separator.
fn fanned_separator(i: usize) -> Vec<u8> {
    vec![(1 + i / 16) as u8, ((i % 16) * 16) as u8]
}

/// Serialize a single-partition `Rows.db` through the PRODUCTION `RowsTrieWriter`
/// for `separators` (ascending), returning `(bytes, RowsOffset, blocks)`.
fn write_single_partition_rows_db(
    separators: Vec<Vec<u8>>,
) -> (Vec<u8>, usize, Vec<RowIndexBlock>) {
    let blocks: Vec<RowIndexBlock> = separators
        .into_iter()
        .enumerate()
        .map(|(i, separator_key)| RowIndexBlock {
            separator_key,
            // Ascending, spread out enough to be realistic (and to force multi-byte
            // SizedInts payloads on the later leaves).
            block_offset: 7 + i as u64 * 16_384,
            open_marker: None,
        })
        .collect();
    let partition_key = 7i32.to_be_bytes();
    let mut writer = RowsTrieWriter::new();
    writer.add_partition_row_index(&partition_key, 4096, blocks.clone(), None);
    let (rows_db, rows_offsets) = writer.finish().expect("Rows.db must serialize");
    (rows_db, rows_offsets[0] as usize, blocks)
}

/// Shared multi-page assertions: the trie body spans > 3 pages, the entry's root
/// VALIDATES, its extent ends exactly at the entry, and a DFS from it recovers every
/// block in order. Returns `(root, entries)` for the caller's own probes.
fn assert_multi_page_root_validates(
    rows_db: &[u8],
    rows_offset: usize,
    blocks: &[RowIndexBlock],
) -> (usize, Vec<(Vec<u8>, BtiRowIndexEntry)>) {
    assert!(
        rows_offset > 3 * PAGE_SIZE,
        "the trie body must span more than three 4 KiB pages to exercise the \
         multi-page regime; got {rows_offset} bytes"
    );

    let header = resolve_rows_db_entry(rows_db, rows_offset).expect("entry must deserialize");
    let root = header.require_trie_root().unwrap_or_else(|e| {
        panic!(
            "a multi-page row-index root written by the production writer must VALIDATE \
             (a rejection here is a corruption report on a valid file): {e}"
        )
    });
    assert_eq!(
        header.block_count as usize,
        blocks.len(),
        "the entry must index every block"
    );
    // The invariant itself, stated directly.
    assert_eq!(
        rows_node_serialized_extent_end_for_test(rows_db, root),
        Some(rows_offset),
        "the root's serialized extent must end EXACTLY at the entry"
    );

    let entries = iterate_rows_in_bti_trie(rows_db, root).expect("DFS from the root must succeed");
    assert_eq!(
        entries.len(),
        blocks.len(),
        "every one of the {} blocks must be reachable from the root",
        blocks.len()
    );
    for (i, (sep, entry)) in entries.iter().enumerate() {
        assert_eq!(
            (sep.as_slice(), entry.data_offset),
            (blocks[i].separator_key.as_slice(), blocks[i].block_offset),
            "entry {i} must round-trip its separator and block offset"
        );
    }
    (root, entries)
}

/// Largest separator `<= key` in an ascending `(separator, entry)` list — the
/// `RowIndexReader.separatorFloor` oracle, by enumeration.
fn floor_oracle(entries: &[(Vec<u8>, BtiRowIndexEntry)], key: &[u8]) -> Option<u64> {
    entries
        .iter()
        .rfind(|(sep, _)| sep.as_slice() <= key)
        .map(|(_, e)| e.data_offset)
}

/// Smallest separator strictly `> key` — the `strict_ceiling` oracle, by
/// enumeration.
fn ceiling_oracle(entries: &[(Vec<u8>, BtiRowIndexEntry)], key: &[u8]) -> Option<u64> {
    entries
        .iter()
        .find(|(sep, _)| sep.as_slice() > key)
        .map(|(_, e)| e.data_offset)
}

/// The maximum backward distance from `root` to one of its direct children — how
/// far the root's pointers must reach, and therefore which pointer WIDTH the writer
/// had to pick.
fn max_child_distance(rows_db: &[u8], root: usize) -> usize {
    let node = parse_bti_node_for_test(&rows_db[root..], root as u64).expect("root node parses");
    let child_offsets: Vec<usize> = match node.data {
        BtiNodeData::Single { transition } => vec![transition.child.distance as usize],
        BtiNodeData::Sparse { transitions } => transitions
            .iter()
            .map(|t| t.child.distance as usize)
            .collect(),
        BtiNodeData::Dense { children, .. } => children
            .iter()
            .flatten()
            .map(|p| p.distance as usize)
            .collect(),
        BtiNodeData::PayloadOnly { .. } => {
            panic!("the root of a {MANY_BLOCKS}-block row index cannot be childless")
        }
    };
    root - child_offsets
        .iter()
        .copied()
        .min()
        .expect("an internal root has at least one child")
}

/// Cross-check the floor/ceiling walks from `root` against enumeration oracles at
/// keys spread across the whole trie — including below the first separator, at an
/// exact separator, between separators, and above the last.
fn assert_walks_agree_with_oracles(
    rows_db: &[u8],
    root: usize,
    entries: &[(Vec<u8>, BtiRowIndexEntry)],
    probes: &[Vec<u8>],
) {
    for key in probes {
        assert_eq!(
            rows_floor_block_for_test(rows_db, root, key)
                .expect("floor walk must succeed")
                .map(|e| e.data_offset),
            floor_oracle(entries, key),
            "floor walk must agree with the enumeration oracle for key {key:02x?}"
        );
        assert_eq!(
            rows_strict_ceiling_block_for_test(rows_db, root, key)
                .expect("ceiling walk must succeed")
                .map(|e| e.data_offset),
            ceiling_oracle(entries, key),
            "ceiling walk must agree with the enumeration oracle for key {key:02x?}"
        );
    }
}

/// A genuinely MULTI-PAGE row-index trie in the REAL fixture's separator shape
/// (`0x40` + sign-flipped `int`), emitted by the production `RowsTrieWriter`: 4000
/// blocks instead of the fixture's 38, a trie body spanning several 4096-byte
/// pages. The root validates, its extent ends exactly at the entry, and a DFS plus
/// the floor/ceiling walks agree with enumeration oracles across the whole body.
///
/// It also pins a property that is easy to assume away: because the trie is
/// serialized children-before-parents, the root's ONLY child (the shared
/// `NEXT_COMPONENT` transition) is written immediately below it, so the root of a
/// multi-page int-clustering trie stays a NARROW-pointer node. The wide-pointer
/// root regime therefore needs a different separator shape — see
/// [`multi_page_rows_trie_with_a_wide_pointer_root_validates_and_walks`].
#[test]
fn multi_page_rows_trie_from_the_production_writer_validates_and_walks() {
    let (rows_db, rows_offset, blocks) =
        write_single_partition_rows_db((0..MANY_BLOCKS).map(|i| separator(i as i32 * 8)).collect());
    let (root, entries) = assert_multi_page_root_validates(&rows_db, rows_offset, &blocks);

    // Children before parents: the root's single `0x40` child sits just below it,
    // even though the subtree beneath that child spans pages.
    let reach = max_child_distance(&rows_db, root);
    assert!(
        reach < PAGE_SIZE,
        "with one shared leading byte the root's only child is written immediately \
         below it; a reach of {reach} means the writer's node ordering changed and this \
         test no longer covers the narrow-root case"
    );

    // Floor/ceiling walks across the whole multi-page trie.
    let probes = vec![
        separator(-1),                               // below every separator
        separator(0),                                // the first separator, exactly
        separator(4),                                // between separators 0 and 8
        separator(8 * (MANY_BLOCKS as i32 / 2)),     // an exact separator mid-trie
        separator(8 * (MANY_BLOCKS as i32 / 2) + 3), // between two mid-trie separators
        separator(8 * (MANY_BLOCKS as i32 - 1)),     // the last separator, exactly
        separator(8 * MANY_BLOCKS as i32),           // above every separator
    ];
    assert_walks_agree_with_oracles(&rows_db, root, &entries, &probes);
    // Spot-check that the oracles themselves are not degenerate here.
    assert_eq!(
        floor_oracle(&entries, &separator(-1)),
        None,
        "a key below the first separator must have no floor"
    );
    assert_eq!(
        ceiling_oracle(&entries, &separator(8 * MANY_BLOCKS as i32)),
        None,
        "a key above the last separator must have no strict ceiling"
    );
}

/// Property (1) — a multi-page trie whose ROOT carries pointers reaching back
/// across pages.
///
/// Separators fan out on their FIRST byte, so the root has one child per distinct
/// leading byte and the earliest of those subtrees sits pages behind it. The writer
/// must then pick a multi-byte pointer ordinal, and the reader's extent computation
/// must size that pointer area BY ORDINAL — a 1-byte-pointer assumption (the only
/// width the ~200-byte fixture root exercises) would put the extent end in the wrong
/// place and reject a valid root.
#[test]
fn multi_page_rows_trie_with_a_wide_pointer_root_validates_and_walks() {
    let (rows_db, rows_offset, blocks) =
        write_single_partition_rows_db((0..MANY_BLOCKS).map(fanned_separator).collect());
    let (root, entries) = assert_multi_page_root_validates(&rows_db, rows_offset, &blocks);

    let reach = max_child_distance(&rows_db, root);
    assert!(
        reach > PAGE_SIZE,
        "the root's furthest child must be more than one page away for this to be a \
         wide-pointer root; got {reach} bytes"
    );
    let header_byte = rows_db[root];
    let root_ordinal = header_byte >> 4;
    assert!(
        matches!(root_ordinal, 7 | 8 | 9 | 11 | 12 | 13 | 14 | 15),
        "a root reaching {reach} bytes back must use a multi-byte pointer ordinal \
         (SPARSE_16/24/40 or a Dense family member); got ordinal {root_ordinal} \
         (header byte 0x{header_byte:02x})"
    );

    let probes = vec![
        vec![0x00, 0x00],                         // below every separator
        fanned_separator(0),                      // the first separator, exactly
        vec![0x01, 0x08],                         // between separators 0 and 1
        fanned_separator(MANY_BLOCKS / 2),        // an exact separator mid-trie
        vec![(1 + MANY_BLOCKS / 32) as u8, 0x08], // between two mid-trie separators
        fanned_separator(MANY_BLOCKS - 1),        // the last separator, exactly
        vec![0xFF, 0xFF],                         // above every separator
    ];
    assert_walks_agree_with_oracles(&rows_db, root, &entries, &probes);
    assert_eq!(
        floor_oracle(&entries, &[0x00, 0x00]),
        None,
        "a key below the first separator must have no floor"
    );
    assert_eq!(
        ceiling_oracle(&entries, &[0xFF, 0xFF]),
        None,
        "a key above the last separator must have no strict ceiling"
    );
}

/// Property (2) — the root written AFTER page padding, on a fresh page.
///
/// The layout below is what `IncrementalTrieWriterPageAware` produces when the root
/// does not fit in the tail of the page its children occupy: leaves on earlier
/// pages, a `0x00` pad run, the root at a 4096 boundary, then the entry
/// IMMEDIATELY after the root (`BtiTableWriter.IndexWriter.append` writes the entry
/// at the file position `complete()` left).
///
/// CQLite's own `RowsTrieWriter` writes contiguously, so this layout is built
/// byte-wise here rather than by a page-aware writer — see the module docs for
/// exactly what that leaves uncovered. Everything the assertions touch is a
/// structural property of the bytes: the pad run below the root, the root's
/// cross-page pointers, and the extent equality at the entry.
#[test]
fn page_padded_root_on_a_fresh_page_validates_and_walks() {
    let leaf_count = 32usize;
    let mut rows_db = Vec::new();

    // Leaves: `PayloadOnly` (ordinal 0) with payloadBits = 2, i.e. a 2-byte
    // SizedInts block offset — the shape `write_row_leaf` emits for offsets that
    // need two bytes.
    let mut leaves: Vec<(u8, usize, u64)> = Vec::with_capacity(leaf_count);
    for i in 0..leaf_count {
        let offset = rows_db.len();
        let block_offset = 256 + i as u64 * 512;
        rows_db.push(0x02);
        rows_db.extend_from_slice(&(block_offset as u16).to_be_bytes());
        // Single-byte separators keep the hand-built root a flat Sparse node.
        leaves.push((0x10 + i as u8, offset, block_offset));
    }
    let leaves_end = rows_db.len();

    // Page padding: pad with `0x00` until the next page boundary that leaves every
    // child more than one page behind the root.
    let root = rows_db.len().next_multiple_of(PAGE_SIZE) + PAGE_SIZE;
    rows_db.resize(root, 0x00);
    assert_eq!(
        root % PAGE_SIZE,
        0,
        "the root must start on a page boundary"
    );
    assert!(
        root - leaves_end > PAGE_SIZE,
        "the pad run must be long enough that the root's children sit more than a \
         page behind it"
    );
    assert_eq!(
        rows_db[root - 1],
        0x00,
        "the byte immediately BELOW the root must be page padding — the layout a \
         non-page-aware reader would not expect"
    );

    // The root: `SPARSE_16` (ordinal 7, 2-byte backward pointers), payloadBits = 0.
    // A payload-less internal root is legal and is exactly what CQLite's own writer
    // emits (`write_sparse`); `validate_rows_trie_root` accepts it.
    rows_db.push(0x70);
    rows_db.push(leaf_count as u8);
    for (transition, _, _) in &leaves {
        rows_db.push(*transition);
    }
    for (_, leaf_offset, _) in &leaves {
        let delta = root - leaf_offset;
        assert!(
            delta > PAGE_SIZE && delta <= u16::MAX as usize,
            "each child delta must cross a page yet fit the SPARSE_16 pointer width; \
             got {delta}"
        );
        rows_db.extend_from_slice(&(delta as u16).to_be_bytes());
    }

    // The `TrieIndexEntry`, immediately after the root:
    // `[u16 keylen][key][dataPos vint][rootΔ zigzag vint][blockCount vint][0x80 LIVE]`.
    let rows_offset = rows_db.len();
    let key = 9i32.to_be_bytes();
    rows_db.extend_from_slice(&(key.len() as u16).to_be_bytes());
    rows_db.extend_from_slice(&key);
    rows_db.push(0x00); // data position 0 (1-byte unsigned vint)
    let base = rows_offset + 2 + key.len();
    let delta = root as i64 - base as i64;
    write_signed_vint(&mut rows_db, delta);
    rows_db.push(leaf_count as u8); // block count (1-byte unsigned vint)
    rows_db.push(0x80); // LIVE partition deletion

    // The entry resolves to the padded-page root, and the root VALIDATES: the
    // extent equality holds across the pad run.
    let header = resolve_rows_db_entry(&rows_db, rows_offset).expect("entry must deserialize");
    let validated = header.require_trie_root().unwrap_or_else(|e| {
        panic!(
            "a root written after page padding must VALIDATE — rejecting it would report \
             a valid page-aware file as corrupt: {e}"
        )
    });
    assert_eq!(validated, root, "the entry must resolve to the padded root");
    assert_eq!(
        rows_node_serialized_extent_end_for_test(&rows_db, root),
        Some(rows_offset),
        "the padded root's serialized extent must still end EXACTLY at the entry"
    );
    let reach = max_child_distance(&rows_db, root);
    assert!(
        reach > PAGE_SIZE,
        "the padded root's children must live a page or more back; got {reach}"
    );

    // And it walks: DFS recovers every leaf, and the floor/ceiling walks agree with
    // enumeration oracles either side of the padding.
    let entries =
        iterate_rows_in_bti_trie(&rows_db, validated).expect("DFS from the padded root succeeds");
    assert_eq!(
        entries.len(),
        leaf_count,
        "every leaf must be reachable across the page padding"
    );
    for (i, (sep, entry)) in entries.iter().enumerate() {
        assert_eq!(
            (sep.as_slice(), entry.data_offset),
            (&[leaves[i].0][..], leaves[i].2),
            "leaf {i} must round-trip its separator and block offset"
        );
    }
    let probes: Vec<Vec<u8>> = vec![
        vec![0x00],
        vec![0x0F],
        vec![0x10],
        vec![0x18],
        vec![0x10 + leaf_count as u8 - 1],
        vec![0xFF],
    ];
    assert_walks_agree_with_oracles(&rows_db, validated, &entries, &probes);
}

/// Cassandra's signed (ZigZag) VInt (`DataOutputPlus.writeVInt`), hand-rolled so
/// this lane builds its fixture without the writer whose output it checks.
fn write_signed_vint(buf: &mut Vec<u8>, value: i64) {
    let zig = ((value << 1) ^ (value >> 63)) as u64;
    let extra = if zig == 0 {
        0
    } else {
        let bits = 64 - zig.leading_zeros() as usize;
        let mut n = 0usize;
        while n < 8 && (7 - n) + 8 * n < bits {
            n += 1;
        }
        n
    };
    if extra == 0 {
        buf.push(zig as u8);
        return;
    }
    let mut first = (0xFFu16 << (8 - extra)) as u8;
    if extra < 7 {
        first |= (zig >> (8 * extra)) as u8;
    }
    buf.push(first);
    for i in (0..extra).rev() {
        buf.push((zig >> (8 * i)) as u8);
    }
}
