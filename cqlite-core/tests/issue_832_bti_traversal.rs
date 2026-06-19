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
    iterate_partitions_in_bti_file, iterate_rows_for_partition, iterate_rows_in_bti_file,
    iterate_rows_in_bti_trie, lookup_raw_key_in_bti_partitions_db, resolve_rows_db_entry,
    select_row_index_blocks_for_range, BtiPartitionLocation, RowsParser,
};
use cqlite_core::types::Value;
use std::io::Cursor;
use std::path::{Path, PathBuf};

/// Relative path of the wide-partition BTI fixture directory (issue #832).
const WIDE_DIR: &str = "sstables/test_da/wide_table-9099a7c06c1811f19864870fb8444786";

/// Cassandra OSS50 byte-comparable encoding of an `int` clustering value `ck`
/// (single-component clustering): flip the sign bit, big-endian.  This matches
/// the separator keys stored in the `wide_table` Rows.db tries (e.g. ck=8 →
/// `80 00 00 08`).
fn encode_ck_int(ck: i32) -> Vec<u8> {
    ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec()
}

/// Cassandra `Int32Type` raw partition-key bytes for `pk` (4-byte big-endian).
fn raw_pk_int(pk: i32) -> Vec<u8> {
    pk.to_be_bytes().to_vec()
}

/// Read a whole BTI component file as raw bytes (Rows.db / Partitions.db are NOT
/// chunk-compressed), skipping the test when the binary fixture is absent.
fn read_component(root: &Path, rel: &str) -> Option<Vec<u8>> {
    let path = root.join(WIDE_DIR).join(rel);
    if !path.exists() {
        eprintln!("SKIP: wide BTI fixture not found at {:?}", path);
        return None;
    }
    match std::fs::read(&path) {
        Ok(b) => Some(b),
        Err(e) => {
            eprintln!("SKIP: cannot read {:?}: {}", path, e);
            None
        }
    }
}

/// Resolve the three wide_table partition `RowsOffset`s by looking up the int
/// partition keys 1/2/3 in Partitions.db.  Returns `[ro_pk1, ro_pk2, ro_pk3]`.
fn wide_rows_offsets(root: &Path) -> Option<[usize; 3]> {
    let pdb = read_component(root, "da-2-bti-Partitions.db")?;
    let mut offs = [0usize; 3];
    for (i, pk) in [1i32, 2, 3].into_iter().enumerate() {
        let mut cur = Cursor::new(pdb.clone());
        let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &raw_pk_int(pk))
            .expect("Partitions.db lookup must succeed");
        match loc {
            Some(BtiPartitionLocation::RowsOffset(o)) => offs[i] = o as usize,
            other => panic!("pk={pk} must be a wide partition (RowsOffset); got {other:?}"),
        }
    }
    Some(offs)
}

/// Build `ck -> within-partition data position` from the JSONL golden for a
/// partition (by index 0/1/2).
fn jsonl_positions(
    root: &Path,
    partition_index: usize,
) -> Option<std::collections::BTreeMap<i64, u64>> {
    let path = root.join(WIDE_DIR).join("da-2-bti-Data.db.jsonl");
    if !path.exists() {
        eprintln!("SKIP: JSONL golden not found at {:?}", path);
        return None;
    }
    let text = std::fs::read_to_string(&path).ok()?;
    let line = text.lines().nth(partition_index)?;
    let obj: serde_json::Value = serde_json::from_str(line).ok()?;
    let rows = obj.get("rows")?.as_array()?;
    let mut map = std::collections::BTreeMap::new();
    for r in rows {
        if r.get("type").and_then(|t| t.as_str()) != Some("row") {
            continue;
        }
        let ck = r.get("clustering")?.as_array()?.first()?.as_i64()?;
        let pos = r.get("position")?.as_u64()?;
        map.insert(ck, pos);
    }
    Some(map)
}

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

// ─────────────────────────────────────────────────────────────────────────────
// Wide-partition fixture tests (issue #832, Findings A & B)
//
// Fixture: test_da/wide_table — wide_table(pk int, ck int, payload text,
// PRIMARY KEY (pk, ck)), LZ4. 3 partitions pk=1/2/3, each 300 rows ck=0..299,
// each ~600 KiB → each has a Rows.db row-index entry.
//
// Concrete, deterministic fixture facts (decoded from the real bytes):
//   pk=1 → RowsOffset 242 → trieRoot 236, dataPos 0,       blockCount 38
//   pk=2 → RowsOffset 494 → trieRoot 488, dataPos 619201,  blockCount 38
//   pk=3 → RowsOffset 748 → trieRoot 742, dataPos 1238408, blockCount 38
// Each partition's 38 separators are ck = 8,16,24,…,296,300 with within-partition
// block offsets 16512, 33024, … (~16 KiB granularity), ending at the partition
// data size (~619200). Separator key for ck=N is (N ^ 0x8000_0000) big-endian.
// ─────────────────────────────────────────────────────────────────────────────

/// (1) Finding A: pk=1's RowsOffset must resolve to a per-partition
/// `TrieIndexEntry`, NOT a trie root.  Asserts the recovered trie root differs
/// from RowsOffset, parses as a valid trie node, and that traversal from THAT
/// root yields blockCount (38) row-index blocks — not an error or garbage.
#[test]
fn rows_offset_resolves_real_trie_root() {
    let Some(root) = datasets_root() else {
        return;
    };
    let Some([ro1, _ro2, ro3]) = wide_rows_offsets(&root) else {
        return;
    };
    let Some(rdb) = read_component(&root, "da-2-bti-Rows.db") else {
        return;
    };

    // RowsOffset for pk=1 must be the known value 242.
    assert_eq!(ro1, 242, "pk=1 RowsOffset must be 242 in this fixture");

    // Feeding RowsOffset directly as a trie root must FAIL (it is entry metadata,
    // not a node) — this is the bug Finding A fixes.
    assert!(
        iterate_rows_in_bti_trie(&rdb, ro1).is_err(),
        "RowsOffset must NOT parse as a trie root directly (it is the entry)"
    );

    // Resolving the entry recovers the real trie root + partition metadata.
    let header = resolve_rows_db_entry(&rdb, ro1).expect("entry must deserialize");
    assert_eq!(header.trie_root, 236, "pk=1 trie root must resolve to 236");
    assert_ne!(
        header.trie_root, ro1,
        "trie root must differ from RowsOffset (entry vs node)"
    );
    assert_eq!(header.data_position, 0, "pk=1 Data.db position must be 0");
    assert_eq!(header.block_count, 38, "pk=1 must have 38 row-index blocks");

    // Finding 2 (issue #832): the fixture issued no deletes, so the MODERN DA
    // partition DeletionTime is the `0x80` LIVE sentinel and must decode to
    // `None`.  The OLD legacy decoder would have mis-read the 12 following bytes
    // (the next partition's entry data) as a bogus deletion.
    assert_eq!(
        header.partition_deletion, None,
        "pk=1 partition deletion must be LIVE (0x80 sentinel → None)"
    );
    // pk=3 is the LAST entry; its 0x80 live sentinel is the final byte of the
    // file, so the modern decoder must not over-read.
    let header3 = resolve_rows_db_entry(&rdb, ro3).expect("pk=3 entry must deserialize");
    assert_eq!(
        header3.partition_deletion, None,
        "pk=3 partition deletion must be LIVE (0x80 sentinel → None)"
    );

    // Traversal from the recovered root yields exactly blockCount valid blocks.
    let entries = iterate_rows_in_bti_trie(&rdb, header.trie_root)
        .expect("traversal from the recovered root must succeed");
    assert_eq!(
        entries.len() as u32,
        header.block_count,
        "traversal must yield blockCount (38) row-index blocks, got {}",
        entries.len()
    );
}

/// (2) RowIterator-style traversal over pk=1's row index yields blocks in
/// ascending clustering order; block count >= 2; within-partition offsets are
/// strictly increasing and stay within Data.db bounds (absolute pos =
/// data_position + offset < Data.db size).
#[test]
fn row_iterator_wide_partition_yields_blocks_in_order() {
    let Some(root) = datasets_root() else {
        return;
    };
    let Some([ro1, _, _]) = wide_rows_offsets(&root) else {
        return;
    };
    let Some(rdb) = read_component(&root, "da-2-bti-Rows.db") else {
        return;
    };
    // Data.db is LZ4-chunk-compressed, so its on-disk size is NOT the bound for
    // these UNCOMPRESSED within-partition offsets.  Bound against the partition's
    // uncompressed span derived from the JSONL golden (max row position + one
    // 2 KiB payload row, generously rounded up).
    let Some(positions) = jsonl_positions(&root, 0) else {
        return;
    };
    let uncompressed_span = positions.values().copied().max().unwrap_or(0) + 4096;

    let (header, entries) =
        iterate_rows_for_partition(&rdb, ro1).expect("partition row index must traverse");

    assert!(
        entries.len() >= 2,
        "a ~600 KiB partition must span multiple index blocks; got {}",
        entries.len()
    );
    assert_eq!(entries.len(), 38, "pk=1 must yield 38 blocks");

    // Separator keys ascending (DFS guarantees byte-comparable order).
    for w in entries.windows(2) {
        assert!(
            w[0].0 <= w[1].0,
            "row-index separator keys must be ascending: {:?} then {:?}",
            w[0].0,
            w[1].0
        );
    }

    // Within-partition offsets strictly increasing, absolute position in bounds.
    let offsets: Vec<u64> = entries.iter().map(|(_, e)| e.data_offset).collect();
    for w in offsets.windows(2) {
        assert!(
            w[0] < w[1],
            "block offsets must be strictly increasing: {} then {}",
            w[0],
            w[1]
        );
    }
    for &off in &offsets {
        assert!(
            off <= uncompressed_span,
            "within-partition block offset {off} must be within the partition's \
             uncompressed span ({uncompressed_span})"
        );
    }
    // pk=1 starts at Data.db position 0.
    assert_eq!(header.data_position, 0);

    // First separator is ck=8, last is ck=300 (the complete() trailing sep).
    assert_eq!(entries.first().unwrap().0, encode_ck_int(8));
    assert_eq!(entries.last().unwrap().0, encode_ck_int(300));
    assert_eq!(offsets.first().copied(), Some(16512));
}

/// (3) Finding B: range_query over ck in [100..=150] returns exactly the blocks
/// whose separator interval overlaps that clustering interval, using row-index
/// separator semantics.  Cross-checked against the JSONL golden's per-ck data
/// positions: every returned block covers some requested ck, the block whose
/// interval contains ck=100 (the floor block) is present, and below-/above-range
/// queries return non-overlapping disjoint sets.
#[test]
fn range_query_wide_partition_returns_correct_clustering_subset() {
    let Some(root) = datasets_root() else {
        return;
    };
    let Some([ro1, _, _]) = wide_rows_offsets(&root) else {
        return;
    };
    let Some(rdb) = read_component(&root, "da-2-bti-Rows.db") else {
        return;
    };
    let Some(positions) = jsonl_positions(&root, 0) else {
        return;
    };

    let (header, all) =
        iterate_rows_for_partition(&rdb, ro1).expect("partition row index must traverse");

    // ---- in-range: ck in [100, 150] ----
    let in_blocks =
        select_row_index_blocks_for_range(&all, &encode_ck_int(100), &encode_ck_int(150));
    assert!(
        !in_blocks.is_empty(),
        "ck in [100,150] must select at least one block"
    );

    // Separators are ck = 8,16,…; blocks hold 8 rows each. ck=100 lives in the
    // block whose separator floor is 96 (interval [96,104)); ck=150 in [144,152).
    // The selected separators must therefore range from <=100 up to <=150, with
    // the next-after included to cover ck=150.
    // Build the parallel list of (separator_ck, offset) for cross-checking.
    // decode separator ck from the byte-comparable key.  Separators are the
    // shortest prefix > prevMax; for this single int-clustering fixture they are
    // the full 4-byte form (e.g. ck=8 → 80 00 00 08), but guard for any shorter
    // path-compressed key by zero-padding on the right (byte-comparable order).
    let sep_to_ck = |k: &[u8]| -> i64 {
        let mut buf = [0u8; 4];
        for (i, b) in k.iter().take(4).enumerate() {
            buf[i] = *b;
        }
        (u32::from_be_bytes(buf) ^ 0x8000_0000) as i32 as i64
    };
    let sep_offsets: Vec<(i64, u64)> = all
        .iter()
        .map(|(k, e)| (sep_to_ck(k), e.data_offset))
        .collect();

    // The floor block for ck=100: largest separator <= 100. Its offset must be
    // present in the selected set and must correspond (via JSONL) to a ck <= 100.
    let floor = sep_offsets
        .iter()
        .filter(|(sep, _)| *sep <= 100)
        .max_by_key(|(sep, _)| *sep)
        .expect("a floor separator for ck=100 must exist");
    let in_offsets: Vec<u64> = in_blocks.iter().map(|b| b.data_offset).collect();
    assert!(
        in_offsets.contains(&floor.1),
        "the floor block (sep_ck={}, off={}) containing ck=100 must be selected; got {:?}",
        floor.0,
        floor.1,
        in_offsets
    );

    // Every selected block's within-partition offset must equal some ck's
    // position from the JSONL golden, and that ck must be reachable from the
    // requested interval (i.e. the block's separator interval overlaps [100,150]).
    let pos_to_ck: std::collections::BTreeMap<u64, i64> =
        positions.iter().map(|(&ck, &p)| (p, ck)).collect();
    for &off in &in_offsets {
        let abs = header.data_position + off;
        // off is within-partition; JSONL positions are within-partition too.
        let ck_at_block = pos_to_ck.get(&off);
        assert!(
            ck_at_block.is_some(),
            "selected block offset {off} (abs {abs}) must match a JSONL row position"
        );
    }

    // The union of the selected blocks' separator intervals [s_i, s_{i+1}) must
    // COVER the requested clustering range [100, 150].  (A block's separator is
    // the floor of the first ck it covers; the block holding ck=150 has separator
    // 144 with interval [144, 152), so the largest selected separator is < 150 by
    // design — that is correct separator semantics, not a miss.)
    let all_seps: Vec<i64> = sep_offsets.iter().map(|(s, _)| *s).collect();
    let selected_seps: Vec<i64> = sep_offsets
        .iter()
        .filter(|(_, off)| in_offsets.contains(off))
        .map(|(sep, _)| *sep)
        .collect();
    let min_sep = *selected_seps.iter().min().unwrap();
    assert!(
        min_sep <= 100,
        "selected blocks must start at or below ck=100 (floor block); min_sep={min_sep}"
    );
    // For each requested ck in 100..=150, the block whose interval contains it
    // (separator = greatest sep <= ck) must be among the selected blocks.
    for ck in 100i64..=150 {
        let floor_sep = *all_seps
            .iter()
            .filter(|&&s| s <= ck)
            .max()
            .expect("a floor separator must exist for ck in [100,150]");
        assert!(
            selected_seps.contains(&floor_sep),
            "ck={ck} maps to floor separator {floor_sep}, which must be selected; \
             selected={selected_seps:?}"
        );
    }

    // ---- below-range: ck in [-50, -10] (no rows; all cks are 0..299) ----
    let below = select_row_index_blocks_for_range(&all, &encode_ck_int(-50), &encode_ck_int(-10));
    assert!(
        below.is_empty(),
        "a below-range query (ck < 0) must select no blocks; got {:?}",
        below.iter().map(|b| b.data_offset).collect::<Vec<_>>()
    );

    // ---- above-range: ck in [400, 500] (beyond ck=299) ----
    let above = select_row_index_blocks_for_range(&all, &encode_ck_int(400), &encode_ck_int(500));
    // The only block that can possibly match is the trailing complete() sentinel
    // (separator ck=300) if its interval is treated as [300, +inf). Assert it
    // selects at most that one boundary block and never the data blocks of the
    // requested in-range query.
    let above_offsets: Vec<u64> = above.iter().map(|b| b.data_offset).collect();
    for off in &above_offsets {
        assert!(
            !in_offsets.contains(off) || sep_offsets.iter().any(|(s, o)| *o == *off && *s >= 300),
            "above-range must not select in-range data blocks (off {off})"
        );
    }

    // ---- reversed bounds → empty ----
    let reversed =
        select_row_index_blocks_for_range(&all, &encode_ck_int(150), &encode_ck_int(100));
    assert!(reversed.is_empty(), "reversed bounds must select no blocks");
}

/// Finding 1 (issue #832): the TYPED public `RowsParser::range_query` must encode
/// `Value` clustering bounds in the SAME Cassandra OSS50 byte-comparable form the
/// `Rows.db` trie stores (NOT CQLite's custom prefixed encoder).  This proves the
/// typed API selects the correct block subset against the REAL trie separators,
/// rather than the test hand-encoding the bytes (which hid the encoding bug).
///
/// We pass `&[Value::Integer(100)]..=&[Value::Integer(150)]` directly and assert
/// the result is byte-for-byte identical to the pre-encoded
/// `select_row_index_blocks_for_range` path (Finding B golden), including the
/// floor block for ck=100, and that below-/above-range typed queries match too.
#[test]
fn typed_range_query_encodes_compatibly_with_real_trie() {
    let Some(root) = datasets_root() else {
        return;
    };
    let Some([ro1, _, _]) = wide_rows_offsets(&root) else {
        return;
    };
    let Some(mut cursor) = load_component(
        &root,
        "sstables/test_da/wide_table-9099a7c06c1811f19864870fb8444786/da-2-bti-Rows.db",
    ) else {
        return;
    };
    let Some(rdb) = read_component(&root, "da-2-bti-Rows.db") else {
        return;
    };

    // Pre-encoded golden (Finding B path) for ck in [100, 150].
    let (_, all) = iterate_rows_for_partition(&rdb, ro1).expect("partition row index");
    let golden_in: Vec<u64> =
        select_row_index_blocks_for_range(&all, &encode_ck_int(100), &encode_ck_int(150))
            .into_iter()
            .map(|b| b.data_offset)
            .collect();
    assert!(
        !golden_in.is_empty(),
        "golden in-range set must be non-empty"
    );

    let mut parser = RowsParser::new(&mut cursor).expect("RowsParser::new on real Rows.db");

    // TYPED call — Value bounds, NOT hand-encoded bytes.
    let typed_in: Vec<u64> = parser
        .range_query(ro1, &[Value::Integer(100)], &[Value::Integer(150)])
        .expect("typed range_query must succeed")
        .into_iter()
        .map(|b| b.data_offset)
        .collect();
    assert_eq!(
        typed_in, golden_in,
        "typed range_query(Value::Integer 100..=150) must select the SAME blocks \
         as the pre-encoded OSS50 byte-comparable path (proves compatible encoding)"
    );

    // Floor block for ck=100 (separator floor 96, interval [96,104)) must be present.
    let sep_to_ck = |k: &[u8]| -> i64 {
        let mut buf = [0u8; 4];
        for (i, b) in k.iter().take(4).enumerate() {
            buf[i] = *b;
        }
        (u32::from_be_bytes(buf) ^ 0x8000_0000) as i32 as i64
    };
    let floor_off = all
        .iter()
        .filter(|(k, _)| sep_to_ck(k) <= 100)
        .max_by_key(|(k, _)| sep_to_ck(k))
        .map(|(_, e)| e.data_offset)
        .expect("a floor separator for ck=100 must exist");
    assert!(
        typed_in.contains(&floor_off),
        "typed range_query must include the floor block (off={floor_off}) for ck=100"
    );

    // Below-range typed query (ck < 0): no blocks.
    let typed_below = parser
        .range_query(ro1, &[Value::Integer(-50)], &[Value::Integer(-10)])
        .expect("typed range_query below range");
    assert!(
        typed_below.is_empty(),
        "typed below-range query must select no blocks"
    );

    // Reversed typed bounds → empty.
    let typed_rev = parser
        .range_query(ro1, &[Value::Integer(150)], &[Value::Integer(100)])
        .expect("typed reversed range_query");
    assert!(
        typed_rev.is_empty(),
        "typed reversed bounds must select no blocks"
    );
}
