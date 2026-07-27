//! Issue #3002: the `Rows.db` row-index root base and the leading OSS50
//! `NEXT_COMPONENT` byte, pinned against the REAL Cassandra 5.0 `da` fixture.
//!
//! Two production defects used to CANCEL each other, so fixing either alone
//! regresses BTI clustering reads:
//!
//!   A. `resolve_rows_db_entry` computed the SIGNED root-delta base as
//!      `RowsOffset + key_length` — 2 bytes low. Cassandra 5.0.8 captures
//!      `basePosition` AFTER `writeWithShortLength`
//!      (`BtiTableWriter.IndexWriter.append:184-187`) and reads it back as
//!      `in.getFilePointer()` after `readWithShortLength`
//!      (`BtiTableReader.retrieveEntryIfAcceptable:191`), i.e.
//!      `RowsOffset + 2 + key_length`. The 2-low root pointed straight at the
//!      root's only CHILD (a coincidence of this fixture's 2-byte child node) and
//!      so lost the root's own payload: the `ByteComparable.EMPTY` separator that
//!      indexes block 0 (`RowIndexWriter.add`'s first `sep`).
//!   B. The OSS50 clustering-bound encoders emitted `0x40 NEXT_COMPONENT` only
//!      BETWEEN components. `ClusteringComparator.ByteComparableClustering`
//!      (5.0.8, `ClusteringComparator.java:260-275`) emits it before EACH
//!      component INCLUDING the first, so the real on-disk separator for a single
//!      `int` ck=8 is `40 80 00 00 08`. Un-prefixed bounds happened to be keyed
//!      for exactly the subtree the 2-low root pointed at.
//!
//! Every assertion here is derived from Cassandra's writer/reader source and the
//! fixture's own bytes — never from CQLite's previous behaviour. The traversal
//! counts (39 entries, first = empty separator at block offset 7) FAIL on the
//! pre-fix tree, which produced 38 entries starting at `80 00 00 08`.
//!
//! Fixture: `test_da/wide_table` (`PRIMARY KEY (pk, ck)`, 3 partitions pk=1/2/3 ×
//! 300 rows, LZ4) — the only non-empty `Rows.db` in the corpus. Its binaries are
//! COMMITTED (28 KiB Data.db), so these tests really run; an absent fixture SKIPs
//! loudly and a present-but-empty result is a hard failure, never a pass.
//!
//! Excluded under `tombstones`: that build serves reads by a full-scan filter, so
//! the whole clustering-window path (and the `rows_floor_block` walk this pins) is
//! compiled out there — mirroring `issue_1647_rows_floor_walk.rs`.
#![cfg(not(feature = "tombstones"))]

use cqlite_core::storage::sstable::bti::encode_clustering_bound_oss50;
use cqlite_core::storage::sstable::bti::{
    iterate_rows_in_bti_trie, lookup_raw_key_in_bti_partitions_db, parse_bti_node_for_test,
    resolve_rows_db_entry, rows_floor_block_for_test, rows_strict_ceiling_block_for_test,
    BtiNodeData, BtiNodeType, BtiPartitionLocation, BtiRowIndexEntry,
};
use cqlite_core::types::Value;
use std::io::Cursor;
use std::path::{Path, PathBuf};

/// Relative path of the wide-partition BTI fixture directory.
const WIDE_DIR: &str = "sstables/test_da/wide_table-9099a7c06c1811f19864870fb8444786";

/// The fixture's three partitions as `(pk, RowsOffset, trie_root)`: the
/// `RowsOffset` each `pk` resolves to in Partitions.db (re-verified below, not
/// trusted), and the Cassandra-true trie root that entry resolves to —
/// `RowsOffset + 2 + key_length + root_delta`, with `key_length = 4` and
/// `root_delta = -10` for all three. The pre-fix formula produced 236/488/742
/// (each exactly 2 lower).
const PARTITIONS: [(i32, usize, usize); 3] = [(1, 242, 238), (2, 494, 490), (3, 748, 744)];

/// `TrieNode` ordinal for `SINGLE_8` (cassandra-5.0.8 `TrieNode.java`): a 1-byte
/// transition + 1-byte backward delta, and — unlike `SINGLE_NOPAYLOAD_4`
/// (ordinal 1) — able to carry a payload.
const ORDINAL_SINGLE_8: u8 = 2;
/// `TrieNode` ordinal for `SINGLE_NOPAYLOAD_4`: the delta lives in the low nibble,
/// so the node type structurally CANNOT carry a payload.
const ORDINAL_SINGLE_NOPAYLOAD_4: u8 = 1;

/// Each partition's row index covers 38 blocks (`TrieIndexEntry.blockCount`).
const BLOCK_COUNT: u32 = 38;
/// Cassandra stores one separator per block (the first being
/// `ByteComparable.EMPTY`) PLUS the trailing separator `RowIndexWriter.complete()`
/// appends after the last block — so a faithful traversal yields `38 + 1` entries.
const EXPECTED_TRIE_ENTRIES: usize = BLOCK_COUNT as usize + 1;
/// Block 0's within-partition offset: the partition body start, i.e. the first
/// row's `position` in the sstabledump golden (2-byte key length + 4 key bytes +
/// the 1-byte LIVE `DeletionTime` sentinel).
const BLOCK_0_OFFSET: u64 = 7;

/// Fail-closed switch: when set, an absent fixture/schema is a hard FAILURE
/// instead of a clean skip, so this lane can never green-pass without running.
/// Mirrors `query_semantics_oracle_parity.rs` / `point_vs_full_differential.rs`
/// (the agent-gate components set it).
fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Datasets root: `CQLITE_DATASETS_ROOT` when it holds the fixture, else the
/// in-repo committed corpus (these binaries are committed, not gitignored). An
/// absent fixture prints an explicit SKIP — and FAILS under
/// `CQLITE_REQUIRE_FIXTURES=1`.
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
        .find(|root| root.join(WIDE_DIR).join("da-2-bti-Rows.db").exists());
    if found.is_none() {
        let msg = format!(
            "{WIDE_DIR}/da-2-bti-Rows.db not found under CQLITE_DATASETS_ROOT nor the \
             in-repo committed corpus"
        );
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but {msg} — fail-closed (the #3002 fixture is \
             COMMITTED, so an absent one is a broken checkout, never a pass)"
        );
        eprintln!("SKIP: {msg}");
    }
    found
}

/// Read a `Rows.db`/`Partitions.db` component, SKIPping when absent (hard FAIL
/// under `CQLITE_REQUIRE_FIXTURES=1`).
fn read_component(rel: &str) -> Option<Vec<u8>> {
    let root = datasets_root()?;
    let path = root.join(WIDE_DIR).join(rel);
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
fn wide_components() -> Option<(Vec<u8>, Vec<u8>)> {
    let rows = read_component("da-2-bti-Rows.db")?;
    let partitions = read_component("da-2-bti-Partitions.db")?;
    Some((rows, partitions))
}

/// OSS50 byte-comparable image of a single `int` clustering bound, via the
/// PRODUCTION encoder (so this test is wired to the encoder under test).
fn enc_ck(ck: i32) -> Vec<u8> {
    encode_clustering_bound_oss50(&[Value::Integer(ck)]).expect("int ck encodes")
}

/// Largest separator `<= key` in an ascending `(separator, block)` list — the
/// `RowIndexReader.separatorFloor` oracle, computed by enumeration.
fn floor_oracle(entries: &[(Vec<u8>, BtiRowIndexEntry)], key: &[u8]) -> Option<u64> {
    entries
        .iter()
        .rfind(|(sep, _)| sep.as_slice() <= key)
        .map(|(_, e)| e.data_offset)
}

/// AC 4 — the root base is `RowsOffset + 2 + key_length` for all three
/// partitions: the resolved root is a `SINGLE_8` node WITH a payload
/// (`payloadFlags == 1`), and a faithful traversal from it yields
/// `blockCount + 1` separators whose FIRST is the empty-key separator for block 0
/// (`data_offset == 7`), followed by `40 80 00 00 08`.
///
/// Pre-fix (base 2 low) this produced 38 entries whose first key was
/// `80 00 00 08` and which contained NO block-0 entry.
#[test]
fn rows_db_root_base_includes_short_length_prefix() {
    let Some((rdb, pdb)) = wide_components() else {
        return;
    };

    for (pk, rows_offset, expected_root) in PARTITIONS {
        // The RowsOffset is authoritative from Partitions.db, not hardcoded trust.
        let mut cur = Cursor::new(pdb.clone());
        let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &pk.to_be_bytes())
            .expect("Partitions.db lookup must succeed")
            .expect("pk must be present");
        assert_eq!(
            loc,
            BtiPartitionLocation::RowsOffset(rows_offset as u64),
            "pk={pk} must be a WIDE partition at RowsOffset({rows_offset})"
        );

        let header = resolve_rows_db_entry(&rdb, rows_offset).expect("entry must deserialize");
        assert_eq!(
            header.trie_root,
            expected_root,
            "pk={pk}: trie root must be RowsOffset + 2 + key_length + delta = \
             {expected_root} (the pre-#3002 base was 2 bytes low → {})",
            expected_root - 2
        );
        assert_eq!(header.block_count, BLOCK_COUNT, "pk={pk} block count");

        // The node AT the correct root is SINGLE_8 with payloadFlags == 1 (it
        // carries the block-0 IndexInfo); the node 2 bytes earlier — the pre-fix
        // "root" — is SINGLE_NOPAYLOAD_4, a type that cannot carry a payload.
        let header_byte = rdb[expected_root];
        assert_eq!(
            header_byte >> 4,
            ORDINAL_SINGLE_8,
            "pk={pk}: root node byte 0x{header_byte:02x} must be ordinal {ORDINAL_SINGLE_8} \
             (SINGLE_8)"
        );
        assert_eq!(
            header_byte & 0x0F,
            1,
            "pk={pk}: root payloadFlags must be 1 (a 1-byte SizedInts block offset)"
        );
        let wrong_byte = rdb[expected_root - 2];
        assert_eq!(
            wrong_byte >> 4,
            ORDINAL_SINGLE_NOPAYLOAD_4,
            "pk={pk}: the pre-fix root byte 0x{wrong_byte:02x} is SINGLE_NOPAYLOAD_4 — \
             structurally payload-less, which is how the block-0 entry went missing"
        );

        // Structurally: the root's single transition is the 0x40 NEXT_COMPONENT
        // byte, and its child is exactly the pre-fix root.
        let node = parse_bti_node_for_test(&rdb[expected_root..], expected_root as u64)
            .expect("root node must parse");
        assert_eq!(
            node.node_type,
            BtiNodeType::Single,
            "pk={pk}: root is Single"
        );
        match node.data {
            BtiNodeData::Single { transition } => {
                assert_eq!(
                    transition.byte, 0x40,
                    "pk={pk}: the root transition is the OSS50 NEXT_COMPONENT byte"
                );
                assert_eq!(
                    transition.child.distance as usize,
                    expected_root - 2,
                    "pk={pk}: the root's only child IS the pre-fix root (the coincidence \
                     that masked this bug)"
                );
            }
            other => panic!("pk={pk}: unexpected root node data {other:?}"),
        }

        // A faithful traversal from the correct root.
        let entries = iterate_rows_in_bti_trie(&rdb, header.trie_root)
            .expect("traversal from the resolved root must succeed");
        assert_eq!(
            entries.len(),
            EXPECTED_TRIE_ENTRIES,
            "pk={pk}: traversal must yield blockCount + 1 = {EXPECTED_TRIE_ENTRIES} separators \
             (one per block, first = ByteComparable.EMPTY, plus complete()'s trailing \
             separator); pre-#3002 it yielded {BLOCK_COUNT}"
        );
        assert_eq!(
            entries[0],
            (
                Vec::<u8>::new(),
                BtiRowIndexEntry {
                    data_offset: BLOCK_0_OFFSET,
                    open_marker: None,
                }
            ),
            "pk={pk}: the FIRST separator must be the empty key (ByteComparable.EMPTY) \
             indexing block 0 at the partition body start (offset {BLOCK_0_OFFSET})"
        );
        // LITERAL on-disk bytes, deliberately NOT `enc_ck(8)`: comparing against the
        // encoder under test would let a coordinated encoder + reconstruction change
        // stay green. `40` = ClusteringComparator's NEXT_COMPONENT, `80 00 00 08` =
        // the sign-flipped `int` 8.
        assert_eq!(
            entries[1].0,
            vec![0x40u8, 0x80, 0x00, 0x00, 0x08],
            "pk={pk}: the second separator's on-disk bytes must be 40 80 00 00 08, i.e. \
             the ck=8 image WITH the leading NEXT_COMPONENT byte"
        );
        assert_eq!(
            enc_ck(8),
            vec![0x40u8, 0x80, 0x00, 0x00, 0x08],
            "the production encoder must independently produce those same literal bytes"
        );
        if pk == 1 {
            assert_eq!(
                entries[1].1.data_offset, 16_512,
                "pk=1: block 1 starts at within-partition offset 16512"
            );
        }
        assert_eq!(
            entries.last().map(|(k, _)| k.clone()),
            Some(enc_ck(300)),
            "pk={pk}: the trailing complete() separator is the nudged ck=300 image"
        );
        for w in entries.windows(2) {
            assert!(
                w[0].0 < w[1].0,
                "pk={pk}: separators must be strictly ascending: {:02x?} then {:02x?}",
                w[0].0,
                w[1].0
            );
        }
    }
}

/// The `[body_start_rel, body_end_rel)` row-body decode window
/// `bti_clustering_row_window` derives from `root` for the physical bounds
/// `[start, end]`, transcribed here for a table with NO static columns
/// (`test_da.wide_table` has none): `floor(start)` narrows the start — `None` is
/// the #1968 implicit-first signal, i.e. rel 0 — and `strict_ceiling(end)` is the
/// EXCLUSIVE end (`None` ⇒ the partition end, modelled as `usize::MAX` exactly as
/// production does before the caller clamps it).
fn window(rdb: &[u8], root: usize, start: &[u8], end: &[u8]) -> (usize, usize) {
    let floor = rows_floor_block_for_test(rdb, root, start).expect("floor walk must succeed");
    let ceil =
        rows_strict_ceiling_block_for_test(rdb, root, end).expect("ceiling walk must succeed");
    (
        floor.map(|b| b.data_offset as usize).unwrap_or(0),
        ceil.map(|b| b.data_offset as usize).unwrap_or(usize::MAX),
    )
}

/// AC 3 — the compensation was LOAD-BEARING: each fix ALONE regresses, and BOTH
/// directions are proved EXECUTABLY against the fixture's own bytes by pinning the
/// resolved `[body_start_rel, body_end_rel)` window for the canonical slice
/// `ck >= 100 AND ck < 110` (the query `issue_954`/`issue_1647` assert returns
/// exactly ck=100..=109).
///
/// The ground truth for "where those rows live" comes from the CORRECT trie: no row
/// with `ck >= 100` starts before its own block's start (`floor(ck=100)`), and every
/// row with `ck <= 109` ends before the start of the block that FOLLOWS ck=109's
/// (`strict_ceiling(ck=109)`) — clustering order is byte order within a partition.
/// So `[rows_lo, rows_hi)` brackets the slice, and a window that does not intersect
/// it CANNOT return the slice.
///
/// Half A alone (correct root, un-prefixed bounds): a bound missing the leading
/// `0x40` sorts ABOVE the root's only transition, so the floor walk falls into
/// `goMax` and returns the LAST block while the ceiling walk finds no greater
/// branch at all — the window collapses to the partition TAIL, entirely ABOVE the
/// slice.
///
/// Half B alone (0x40-prefixed bounds, pre-fix root): the prefixed bound sorts
/// BELOW every key in the pre-fix subtree, so the floor is the (benign, #1968)
/// `None` ⇒ rel 0 — but the END bound collapses to the FIRST stored separator, so
/// the window is block 0 only (ck=0..7), entirely BELOW the slice. That is the
/// wrong-ANSWER half: the `SELECT` returns 0 rows.
#[test]
fn each_fix_alone_regresses_the_read_path() {
    let Some((rdb, _pdb)) = wide_components() else {
        return;
    };
    let root = resolve_rows_db_entry(&rdb, PARTITIONS[0].1)
        .expect("resolve pk=1")
        .trie_root;
    // The pre-#3002 base was exactly 2 bytes low (it omitted the u16 short-length
    // prefix), so the pre-fix root is `root - 2`. Reconstructed here rather than
    // left in production code behind a flag.
    let pre_fix_root = root - 2;

    // The physical bounds production derives for `ck >= 100 AND ck < 110`
    // (`physical_byte_bounds_for_slice`, ASC column: no swap).
    let correct = iterate_rows_in_bti_trie(&rdb, root).expect("traverse correct root");
    let bound_ck100 = enc_ck(100);
    let bound_ck110 = enc_ck(110);

    // Ground truth: the byte range the slice's rows occupy, from the CORRECT trie.
    let rows_lo = rows_floor_block_for_test(&rdb, root, &bound_ck100)
        .expect("floor walk must succeed")
        .expect("ck=100 has a stored floor block")
        .data_offset as usize;
    let rows_hi = rows_strict_ceiling_block_for_test(&rdb, root, &enc_ck(109))
        .expect("ceiling walk must succeed")
        .expect("ck=109 has a stored successor block")
        .data_offset as usize;
    assert_eq!(
        rows_lo,
        floor_oracle(&correct, &bound_ck100).expect("ck=100 has a floor separator") as usize,
        "the floor walk must agree with the enumerate-and-filter oracle for ck=100"
    );
    assert!(
        (BLOCK_0_OFFSET as usize) < rows_lo && rows_lo < rows_hi,
        "the ck=100..=109 rows must live in a non-empty byte range ABOVE block 0; \
         got [{rows_lo}, {rows_hi}) with block 0 at {BLOCK_0_OFFSET}"
    );
    println!("#3002 slice ck=100..=109 occupies [{rows_lo}, {rows_hi})");

    // Both fixes together: the window COVERS the slice's rows.
    let good = window(&rdb, root, &bound_ck100, &bound_ck110);
    assert!(
        good.0 <= rows_lo && good.1 >= rows_hi,
        "with BOTH fixes the window {good:?} must cover the slice's byte range \
         [{rows_lo}, {rows_hi})"
    );

    // ---- Half A alone: correct root + the pre-fix (un-prefixed) encoding ----
    let unprefixed_100 = unprefixed_of(&bound_ck100);
    assert_eq!(
        unprefixed_100,
        vec![0x80, 0x00, 0x00, 0x64],
        "the pre-fix encoder produced the bare sign-flipped int"
    );
    let half_a = window(&rdb, root, &unprefixed_100, &unprefixed_of(&bound_ck110));
    let last_block = correct
        .last()
        .map(|(_, e)| e.data_offset as usize)
        .expect("non-empty trie");
    assert_eq!(
        half_a,
        (last_block, usize::MAX),
        "half A alone: an un-prefixed bound sorts ABOVE the root's only (0x40) \
         transition, so floor goMax'es to the LAST block ({last_block}) and the ceiling \
         finds no greater branch — the window collapses to the partition TAIL"
    );
    assert!(
        half_a.0 >= rows_hi,
        "half A alone must regress: the window {half_a:?} starts at/after {rows_hi}, so it \
         is DISJOINT from the slice's byte range [{rows_lo}, {rows_hi}) and the \
         `ck >= 100 AND ck < 110` SELECT can only return rows it never decoded"
    );

    // ---- Half B alone: 0x40-prefixed bounds + the pre-fix root ----
    let pre_fix_entries =
        iterate_rows_in_bti_trie(&rdb, pre_fix_root).expect("traverse pre-fix root");
    assert_eq!(
        pre_fix_entries.len(),
        BLOCK_COUNT as usize,
        "the pre-fix root yields {BLOCK_COUNT} entries — one short, missing block 0"
    );
    assert!(
        !pre_fix_entries
            .iter()
            .any(|(_, e)| e.data_offset == BLOCK_0_OFFSET),
        "the pre-fix root's subtree contains NO block-0 (offset {BLOCK_0_OFFSET}) entry — \
         the payload it lost lives on the root node itself"
    );
    assert_eq!(
        pre_fix_entries[0].0,
        unprefixed_of(&enc_ck(8)),
        "keys reconstructed from the pre-fix root lack the leading 0x40"
    );

    let half_b = window(&rdb, pre_fix_root, &bound_ck100, &bound_ck110);
    let first_stored = pre_fix_entries[0].1.data_offset as usize;
    assert_eq!(
        half_b,
        (0, first_stored),
        "half B alone: a correctly-0x40-prefixed bound sorts BELOW every key in the \
         pre-fix subtree, so the START is the (benign, #1968) implicit-first rel 0 while \
         the END bound collapses to the FIRST stored separator ({first_stored}) — the \
         window is block 0 (ck=0..=7) ONLY"
    );
    assert!(
        half_b.1 <= rows_lo,
        "half B alone must regress: the window {half_b:?} ends at/before {rows_lo}, so it \
         is DISJOINT from the slice's byte range [{rows_lo}, {rows_hi}) and the \
         `ck >= 100 AND ck < 110` SELECT returns ZERO rows — a WRONG ANSWER, not merely \
         a lost narrowing (the floor's `None` alone is the documented-benign #1968 \
         signal; the END bound is what breaks)"
    );
}

/// Strip the leading `NEXT_COMPONENT` byte — the pre-#3002 encoder's output.
fn unprefixed_of(encoded: &[u8]) -> Vec<u8> {
    assert_eq!(
        encoded[0], 0x40,
        "encoded bound must start with NEXT_COMPONENT"
    );
    encoded[1..].to_vec()
}

/// AC 6 — block 0 is now reachable as a REAL stored floor (the root's own
/// `ByteComparable.EMPTY` payload), not via the #1968 implicit-first `None`
/// fallback; and #1968's `None` is preserved for a bound genuinely below a
/// NON-empty first separator.
#[test]
fn block_zero_is_a_stored_floor_and_implicit_first_is_preserved() {
    let Some((rdb, _pdb)) = wide_components() else {
        return;
    };
    let root = resolve_rows_db_entry(&rdb, PARTITIONS[0].1)
        .expect("resolve pk=1")
        .trie_root;

    // An OPEN lower bound (the empty physical-low sentinel) floors to the stored
    // block-0 entry — previously `None` (implicit-first).
    let open_lower = rows_floor_block_for_test(&rdb, root, b"")
        .expect("floor walk must succeed")
        .expect("an open lower bound must now find the STORED empty-key separator");
    assert_eq!(
        open_lower.data_offset, BLOCK_0_OFFSET,
        "the empty separator indexes block 0 at the partition body start"
    );
    assert!(
        open_lower.open_marker.is_none(),
        "block 0 carries no open range-tombstone marker in this fixture"
    );

    // ck=0 sorts below the first NON-empty separator (ck=8) but at/above the empty
    // one, so it too floors to the stored block 0 rather than to `None`.
    for ck in [0, 1, 7] {
        assert_eq!(
            rows_floor_block_for_test(&rdb, root, &enc_ck(ck))
                .expect("floor walk must succeed")
                .map(|e| e.data_offset),
            Some(BLOCK_0_OFFSET),
            "ck={ck} must floor to the STORED block-0 separator, not the implicit-first \
             fallback"
        );
    }

    // #1968 preserved: a trie whose FIRST separator is NOT empty (e.g. one written
    // by CQLite's own row-index writer) still reports `None` for a bound below it,
    // so the caller keeps decoding from the partition body start.
    let (synthetic, syn_root) = sparse8_trie_over_single_byte_separators(&[0x10, 0x20]);
    assert_eq!(
        rows_floor_block_for_test(&synthetic, syn_root, &[0x05])
            .expect("synthetic floor walk must succeed")
            .map(|e| e.data_offset),
        None,
        "#1968: a bound below a NON-empty first separator must still yield None \
         (implicit first block)"
    );
    assert_eq!(
        rows_floor_block_for_test(&synthetic, syn_root, &[0x25])
            .expect("synthetic floor walk must succeed")
            .map(|e| e.data_offset),
        Some(2),
        "the synthetic trie's own floors still resolve normally"
    );
}

/// Build a `Rows.db`-shaped trie: `PayloadOnly` leaves (payloadBits = 1, a 1-byte
/// SizedInts offset `i + 1`) under a Sparse8 root keyed by `seps` — the canonical
/// single-byte-separator shape, with NO empty-key root payload.
fn sparse8_trie_over_single_byte_separators(seps: &[u8]) -> (Vec<u8>, usize) {
    let mut trie = Vec::new();
    let mut leaf_offsets = Vec::new();
    for i in 0..seps.len() {
        leaf_offsets.push(trie.len() as u64);
        trie.extend_from_slice(&[0x01, (i + 1) as u8]);
    }
    let root = trie.len() as u64;
    trie.push(0x50); // Sparse8, payloadFlags = 0
    trie.push(seps.len() as u8);
    trie.extend_from_slice(seps);
    for off in &leaf_offsets {
        trie.push((root - off) as u8);
    }
    (trie, root as usize)
}

/// WIRING EVIDENCE — the fix is exercised through the public `SELECT` path
/// (`Database::execute` → `scan_single_partition_clustering` →
/// `resolve_clustering_seek_window` → `bti_clustering_row_window` → the corrected
/// root + the 0x40-prefixed bounds), not helper-only unit tests.
#[cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]
mod wiring {
    use super::{datasets_root, require_fixtures};
    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::Database;

    const TABLE: &str = "test_da.wide_table";

    async fn open_db() -> Option<Database> {
        let root = datasets_root()?;
        let schema = root
            .parent()
            .expect("datasets has a parent")
            .join("schemas")
            .join("wide-table-bti.cql");
        if !schema.exists() {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the committed schema {} is absent — \
                 fail-closed",
                schema.display()
            );
            eprintln!("SKIP: {} not found", schema.display());
            return None;
        }
        let cfg = IngestionConfig {
            schema_paths: vec![schema],
            data_dir: root.join("sstables"),
            version_hint: None,
            core_config: cqlite_core::Config::default(),
            table_directory_filter: Some("/test_da/".to_string()),
        };
        let result = ingest(cfg).await.expect("ingestion must succeed");
        assert!(
            result.schema_load_result.schemas_loaded > 0,
            "the wide_table schema must load"
        );
        Some(result.database)
    }

    /// Every clustering-slice class over the BTI wide partition returns EXACTLY the
    /// matching rows through the public query path. The slices deliberately include
    /// the block-0 range (`ck < 8`, whose floor is the empty separator this fix
    /// restored) and a point read mid-partition.
    #[tokio::test]
    async fn bti_clustering_slices_return_exact_rows_through_select() {
        let Some(db) = open_db().await else {
            return;
        };

        // Anti-empty-pass: the fixture must really decode.
        let full = db
            .execute(&format!("SELECT pk, ck FROM {TABLE} WHERE pk = 1"))
            .await
            .expect("full partition read must succeed");
        assert_eq!(
            full.rows.len(),
            300,
            "fixture invariant: pk=1 holds 300 clustering rows (0 rows ⇒ fixture not \
             decoded, which is a FAILURE, never a pass)"
        );

        // (slice predicate, expected ck values)
        let cases: [(&str, Vec<i32>); 5] = [
            ("ck >= 100 AND ck < 110", (100..110).collect()),
            ("ck = 150", vec![150]),
            ("ck < 8", (0..8).collect()),
            ("ck >= 296", (296..300).collect()),
            ("ck > 0 AND ck <= 3", (1..=3).collect()),
        ];
        for (predicate, expected) in cases {
            let res = db
                .execute(&format!(
                    "SELECT pk, ck FROM {TABLE} WHERE pk = 1 AND {predicate}"
                ))
                .await
                .unwrap_or_else(|e| panic!("`{predicate}` must succeed: {e}"));
            let mut got: Vec<i32> = res
                .rows
                .iter()
                .map(|r| match r.values.get("ck") {
                    Some(cqlite_core::types::Value::Integer(v)) => *v,
                    other => panic!("`{predicate}`: ck decoded as {other:?}"),
                })
                .collect();
            // Order is part of the contract: a single-partition clustering read emits
            // rows in ASCENDING clustering order (the column is ASC), so assert
            // monotonicity BEFORE sorting — sorting first would hide an out-of-order
            // window stitch (e.g. a second block decoded ahead of the first).
            assert!(
                got.windows(2).all(|w| w[0] < w[1]),
                "`{predicate}` must return rows in strictly ascending ck order; got {got:?}"
            );
            got.sort_unstable();
            assert_eq!(got, expected, "`{predicate}` must return exactly its slice");
        }
    }
}
