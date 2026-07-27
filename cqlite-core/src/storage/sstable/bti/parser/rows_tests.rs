//! Unit tests for [`super`] — the `Rows.db` in-trie payload / `TrieIndexEntry`
//! decoders. Split out of `rows.rs` to respect the campsite file-size rule
//! (epic #1135); included via `#[path]` so `use super::*` still reaches the
//! parent module's private items.

use super::*;

/// Dense16 (ordinal 11): [0xB0|pf] [start] [len-1] [range * 2-byte deltas]
fn dense16_node(payload_flags: u8, start: u8, deltas: &[u16]) -> Vec<u8> {
    let len = deltas.len() as u8;
    let mut v = vec![0xB0 | (payload_flags & 0x0F), start, len - 1];
    for &d in deltas {
        v.extend_from_slice(&d.to_be_bytes());
    }
    v
}

/// A `Rows.db` PayloadOnly leaf with no open marker (payloadBits=1): a
/// single-byte unsigned-vint Data.db position (value 0..=127).
fn row_leaf_no_marker(pos: u8) -> Vec<u8> {
    assert!(pos <= 127, "use a 1-byte unsigned vint position");
    vec![0x01, pos] // ordinal=0, payloadBits=1 (no FLAG_OPEN_MARKER)
}

/// Build a 3-leaf Rows.db-style trie via a Sparse8 root.  Returns
/// `(trie_bytes, root_offset)`.
fn make_rows_trie_three(
    (k1, p1): (u8, u8),
    (k2, p2): (u8, u8),
    (k3, p3): (u8, u8),
) -> (Vec<u8>, usize) {
    let mut trie = Vec::new();
    let o1 = trie.len() as u64; // 0
    trie.extend_from_slice(&row_leaf_no_marker(p1));
    let o2 = trie.len() as u64; // 2
    trie.extend_from_slice(&row_leaf_no_marker(p2));
    let o3 = trie.len() as u64; // 4
    trie.extend_from_slice(&row_leaf_no_marker(p3));
    let root = trie.len() as u64; // 6
    trie.push(0x50); // Sparse8
    trie.push(0x03); // count=3
    trie.push(k1);
    trie.push(k2);
    trie.push(k3);
    trie.push((root - o1) as u8);
    trie.push((root - o2) as u8);
    trie.push((root - o3) as u8);
    (trie, root as usize)
}

/// (5) DFS row collector yields clustering keys in byte order with correct
/// Rows.db-decoded payloads.
#[test]
fn dfs_rows_yields_byte_order_with_row_payloads() {
    let (trie, root) = make_rows_trie_three((0x10, 5), (0x20, 17), (0x30, 99));
    let entries = dfs_collect_row_entries(&trie, root).unwrap();
    assert_eq!(
        entries,
        vec![
            (
                vec![0x10],
                BtiRowIndexEntry {
                    data_offset: 5,
                    open_marker: None
                }
            ),
            (
                vec![0x20],
                BtiRowIndexEntry {
                    data_offset: 17,
                    open_marker: None
                }
            ),
            (
                vec![0x30],
                BtiRowIndexEntry {
                    data_offset: 99,
                    open_marker: None
                }
            ),
        ],
        "Rows.db DFS must yield byte-ordered keys with decoded Data.db positions"
    );
}

/// Finding 1 (issue #832): a Dense node whose FIRST real child is at
/// absolute trie offset 0 AND that has a "no transition" gap elsewhere.
#[test]
fn dfs_dense_emits_offset_zero_child_and_skips_gap() {
    let mut trie = Vec::new();
    trie.extend_from_slice(&row_leaf_no_marker(5)); // offset 0
    trie.extend_from_slice(&row_leaf_no_marker(9)); // offset 2
    let root = trie.len() as u64; // 4

    // Dense16 root: delta 0 is the "no transition" sentinel.
    let deltas = [root as u16, 0x0000, (root - 2) as u16];
    trie.extend(dense16_node(0, 0x10, &deltas));

    let entries = dfs_collect_row_entries(&trie, root as usize).unwrap();
    assert_eq!(
        entries,
        vec![
            (
                vec![0x10],
                BtiRowIndexEntry {
                    data_offset: 5,
                    open_marker: None
                }
            ),
            (
                vec![0x12],
                BtiRowIndexEntry {
                    data_offset: 9,
                    open_marker: None
                }
            ),
        ],
        "DFS must emit the real child at absolute offset 0 and skip the \
         no-transition gap (0x11)"
    );
}

/// Rows.db payload with FLAG_OPEN_MARKER decodes a trailing MODERN DA
/// DeletionTime (issue #832 Finding 2): markedForDeleteAt FIRST.
#[test]
fn decode_row_payload_open_marker_modern() {
    // payloadBits = 0x9 → FLAG_OPEN_MARKER (0x8) set.
    let mut data = vec![0x07u8]; // pos = 7
    data.extend_from_slice(&567890i64.to_be_bytes());
    data.extend_from_slice(&1234u32.to_be_bytes());
    let entry = decode_bti_row_payload(&data, 0, 0x9).unwrap();
    assert_eq!(
        entry,
        BtiRowIndexEntry {
            data_offset: 7,
            // open_marker tuple is (local_deletion_time, marked_for_delete_at).
            open_marker: Some((1234, 567890)),
        }
    );
}

/// Rows.db payload with FLAG_OPEN_MARKER but a `0x80` LIVE sentinel decodes
/// to NO open deletion (issue #832 Finding 2).
#[test]
fn decode_row_payload_open_marker_live_sentinel() {
    let data = vec![0x07u8, 0x80u8];
    let entry = decode_bti_row_payload(&data, 0, 0x9).unwrap();
    assert_eq!(
        entry,
        BtiRowIndexEntry {
            data_offset: 7,
            open_marker: None,
        }
    );
}

/// Direct coverage of the modern DA `DeletionTime` decoder (issue #832
/// Finding 2).
#[test]
fn da_deletion_time_decoder() {
    // Live sentinel.
    assert_eq!(decode_da_deletion_time(&[0x80], 0).unwrap(), (None, 1));
    // Live sentinel mid-buffer (trailing bytes ignored, consumes 1).
    assert_eq!(
        decode_da_deletion_time(&[0x00, 0x80, 0xFF], 1).unwrap(),
        (None, 1)
    );

    // Non-live: markedForDeleteAt FIRST (i64), then localDeletionTime (u32).
    let mut buf = Vec::new();
    buf.extend_from_slice(&987_654_321_000i64.to_be_bytes()); // mfda
    buf.extend_from_slice(&1_700_000_000u32.to_be_bytes()); // ldt
    let (del, n) = decode_da_deletion_time(&buf, 0).unwrap();
    assert_eq!(n, 12);
    assert_eq!(del, Some((1_700_000_000i32, 987_654_321_000i64)));

    // The leading byte of mfda here is 0x00 (not 0x80).
    assert_ne!(buf[0], 0x80);

    // Truncated non-live value errors.
    assert!(decode_da_deletion_time(&[0x00, 0x01, 0x02], 0).is_err());
    // Out-of-bounds start errors.
    assert!(decode_da_deletion_time(&[0x00], 5).is_err());
}

/// Multi-byte unsigned vint decode (count-leading-ones, NOT zigzag).
#[test]
fn read_unsigned_vint_multibyte() {
    // 300 = 0x12C → two-byte vint: 0b1000_0001 0b0010_1100 = [0x81, 0x2C]
    let (v, n) = read_unsigned_vint_from_slice(&[0x81, 0x2C]).unwrap();
    assert_eq!((v, n), (300, 2));
    // 127 fits in one byte: [0x7F]
    let (v, n) = read_unsigned_vint_from_slice(&[0x7F]).unwrap();
    assert_eq!((v, n), (127, 1));
}

/// (4) range filter over a synthetic 3-leaf rows-style trie.
#[test]
fn range_filter_subset_and_empty_and_reversed() {
    let (trie, root) = make_rows_trie_three((0x10, 5), (0x20, 17), (0x30, 99));
    let all = dfs_collect_row_entries(&trie, root).unwrap();

    // Inclusive filter helper mirroring range_query's filter.
    let filter = |lo: &[u8], hi: &[u8]| -> Vec<u64> {
        if lo > hi {
            return Vec::new();
        }
        all.iter()
            .filter(|(k, _)| k.as_slice() >= lo && k.as_slice() <= hi)
            .map(|(_, e)| e.data_offset)
            .collect()
    };

    assert_eq!(filter(&[0x10], &[0x20]), vec![5, 17]); // k1..=k2 excludes k3
    assert_eq!(filter(&[0x20], &[0x30]), vec![17, 99]); // k2..=k3 excludes k1
    assert_eq!(filter(&[0x00], &[0x0F]), Vec::<u64>::new()); // below range
    assert_eq!(filter(&[0x31], &[0xFF]), Vec::<u64>::new()); // above range
    assert_eq!(filter(&[0x30], &[0x10]), Vec::<u64>::new()); // reversed bounds
    assert_eq!(filter(&[0x10], &[0x30]), vec![5, 17, 99]); // full inclusive
}

/// Finding B (issue #832): `select_row_index_blocks_for_range` applies
/// row-index SEPARATOR semantics.
#[test]
fn select_blocks_separator_semantics() {
    let entries = vec![
        (
            vec![0x10u8],
            BtiRowIndexEntry {
                data_offset: 5,
                open_marker: None,
            },
        ),
        (
            vec![0x20u8],
            BtiRowIndexEntry {
                data_offset: 17,
                open_marker: None,
            },
        ),
        (
            vec![0x30u8],
            BtiRowIndexEntry {
                data_offset: 99,
                open_marker: None,
            },
        ),
    ];
    let offs = |start: &[u8], end: &[u8]| -> Vec<u64> {
        select_row_index_blocks_for_range(&entries, start, end)
            .into_iter()
            .map(|b| b.data_offset)
            .collect()
    };

    // Floor block must be selected even though its separator <= start.
    assert_eq!(
        offs(&[0x18], &[0x18]),
        vec![5],
        "floor block must be selected"
    );

    // A range spanning block 1 into block 2 selects both.
    assert_eq!(offs(&[0x18], &[0x28]), vec![5, 17]);

    // A range starting exactly at a separator includes that block.
    assert_eq!(offs(&[0x20], &[0x2F]), vec![17]);

    // A range above the last separator selects only the open-ended last block.
    assert_eq!(offs(&[0x40], &[0x50]), vec![99]);

    // Below the first separator: nothing selected.
    assert_eq!(offs(&[0x00], &[0x0F]), Vec::<u64>::new());

    // Full range selects all blocks.
    assert_eq!(offs(&[0x00], &[0xFF]), vec![5, 17, 99]);

    // Reversed bounds → empty.
    assert_eq!(offs(&[0x30], &[0x10]), Vec::<u64>::new());

    // Empty entries → empty.
    assert!(select_row_index_blocks_for_range(&[], &[0x00], &[0xFF]).is_empty());
}

/// Finding A (issue #832): `resolve_rows_db_entry` deserializes a synthetic
/// per-partition `TrieIndexEntry` and recovers the trie root.
#[test]
fn resolve_rows_db_entry_recovers_root_and_metadata() {
    // Place a 1-byte pad so the trie "root" lives at a non-zero offset.
    let mut buf = vec![0xEEu8; 4]; // bytes 0..4: pretend trie nodes
    let rows_offset = buf.len(); // entry starts here, e.g. 4

    // key: length 4, value 0x00000007
    buf.extend_from_slice(&4u16.to_be_bytes());
    buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]);
    // RowsOffset + 2 (short length) + key_length — issue #3002.
    let base = rows_offset + 2 + 4;

    // dataPos = 123 (unsigned vint, 1 byte since < 128)
    buf.push(123);
    // rootΔ such that trie_root = 2 (a node inside the pad region): Δ = 2 - base
    let root_delta: i64 = 2 - base as i64;
    // zigzag-encode then unsigned-vint (1 byte for small magnitudes)
    let zig = ((root_delta << 1) ^ (root_delta >> 63)) as u64;
    assert!(zig < 128, "test setup expects a 1-byte vint");
    buf.push(zig as u8);
    // blockCount = 38 (unsigned vint)
    buf.push(38);
    // partition DeletionTime (MODERN DA non-live): [i64 mfda=17][u32 ldt=9].
    buf.extend_from_slice(&17i64.to_be_bytes());
    buf.extend_from_slice(&9u32.to_be_bytes());

    let header = resolve_rows_db_entry(&buf, rows_offset).unwrap();
    assert_eq!(header.data_position, 123);
    assert_eq!(
        header.trie_root, 2,
        "trie root = rootΔ + (RowsOffset + 2 + keylen)"
    );
    assert_eq!(header.block_count, 38);
    // (local_deletion_time, marked_for_delete_at) = (9, 17).
    assert_eq!(header.partition_deletion, Some((9, 17)));

    // Out-of-bounds RowsOffset → clean error, no panic.
    assert!(resolve_rows_db_entry(&buf, buf.len() + 10).is_err());
}

/// Finding 2 (issue #832): a `TrieIndexEntry` whose partition DeletionTime is
/// the MODERN `0x80` LIVE sentinel decodes to `partition_deletion == None`.
#[test]
fn resolve_rows_db_entry_live_partition_deletion() {
    let mut buf = vec![0xEEu8; 4];
    let rows_offset = buf.len();
    buf.extend_from_slice(&4u16.to_be_bytes());
    buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]);
    let base = rows_offset + 2 + 4; // issue #3002
    buf.push(123); // dataPos
    let root_delta: i64 = 2 - base as i64;
    let zig = ((root_delta << 1) ^ (root_delta >> 63)) as u64;
    buf.push(zig as u8); // rootΔ
    buf.push(38); // blockCount
    buf.push(0x80); // MODERN DA live sentinel → no deletion

    let header = resolve_rows_db_entry(&buf, rows_offset).unwrap();
    assert_eq!(header.block_count, 38);
    assert_eq!(
        header.partition_deletion, None,
        "0x80 live sentinel must decode to no partition deletion"
    );
}

/// Signed vint (zig-zag) decode round-trips small +/- values.
#[test]
fn read_signed_vint_zigzag() {
    // -10 → zigzag 19 → 1-byte vint 0x13 (the real wide_table rootΔ).
    let (v, n) = read_signed_vint_from_slice(&[0x13]).unwrap();
    assert_eq!((v, n), (-10, 1));
    // 0 → 0x00
    assert_eq!(read_signed_vint_from_slice(&[0x00]).unwrap(), (0, 1));
    // 63 → zigzag 126 → 0x7E
    assert_eq!(read_signed_vint_from_slice(&[0x7E]).unwrap(), (63, 1));
}

/// The Rows.db payload offset is a SizedInts value (NOT an unsigned vint).
#[test]
fn decode_row_payload_sizedints_two_bytes() {
    // payloadBits = 2 → 2 SizedInts bytes, no open marker.
    let data = vec![0x40u8, 0x80];
    let entry = decode_bti_row_payload(&data, 0, 0x2).unwrap();
    assert_eq!(
        entry,
        BtiRowIndexEntry {
            data_offset: 16512,
            open_marker: None,
        }
    );
}

/// Finding 2 (issue #832): the rooted Rows.db API must handle empty /
/// out-of-bounds cases cleanly (no panic).
#[test]
fn iterate_rows_in_bti_trie_empty_and_oob_root() {
    // Empty trie_data with root 0 → out-of-bounds → clean error, no panic.
    let err = iterate_rows_in_bti_trie(&[], 0);
    assert!(err.is_err(), "empty Rows.db trie must error, not panic");

    // Non-empty trie but root beyond bounds → clean error.
    let (trie, _root) = make_rows_trie_three((0x10, 5), (0x20, 17), (0x30, 99));
    let err = iterate_rows_in_bti_trie(&trie, trie.len() + 100);
    assert!(err.is_err(), "out-of-bounds root must error, not panic");

    // A valid per-partition root traverses correctly.
    let (trie, root) = make_rows_trie_three((0x10, 5), (0x20, 17), (0x30, 99));
    let entries = iterate_rows_in_bti_trie(&trie, root).unwrap();
    assert_eq!(entries.len(), 3);
}

/// Finding 3 (issue #832): range_query soundness over reconstructed keys
/// when an internal-node payload's key is a STRICT PREFIX of a deeper leaf
/// key.
#[test]
fn range_filter_prefix_relationship_is_sound() {
    let mut trie = Vec::new();
    // offset 0: leaf for K2 = [0x10,0x20], pos=2
    trie.extend_from_slice(&row_leaf_no_marker(2));
    // offset 2: Single8 with payload — the K = [0x10] node.
    let k_off = trie.len() as u64; // 2
    trie.push(0x21); // Single8 + payloadBits=1
    trie.push(0x20); // transition byte
    trie.push(k_off as u8); // delta=2 → child at offset 0
    trie.push(0x01); // payload: unsigned vint pos=1

    // offset 6: Single8 root, no payload, transition=0x10 → child = k_off (2).
    let root = trie.len() as u64; // 6
    trie.push(0x20); // Single8, payloadBits=0 (no payload)
    trie.push(0x10); // transition byte
    trie.push((root - k_off) as u8); // delta=4 → child at k_off=2

    let all = iterate_rows_in_bti_trie(&trie, root as usize).unwrap();
    // DFS emits K's own payload before descending to K2.
    assert_eq!(
        all,
        vec![
            (
                vec![0x10],
                BtiRowIndexEntry {
                    data_offset: 1,
                    open_marker: None
                }
            ),
            (
                vec![0x10, 0x20],
                BtiRowIndexEntry {
                    data_offset: 2,
                    open_marker: None
                }
            ),
        ],
        "K (internal payload) must sort before its descendant K2"
    );

    // Inclusive byte-comparable filter mirroring range_query's filter.
    let filter = |lo: &[u8], hi: &[u8]| -> Vec<u64> {
        if lo > hi {
            return Vec::new();
        }
        all.iter()
            .filter(|(k, _)| k.as_slice() >= lo && k.as_slice() <= hi)
            .map(|(_, e)| e.data_offset)
            .collect()
    };

    let k = [0x10u8];
    let k2 = [0x10u8, 0x20u8];

    assert_eq!(
        filter(&k, &k),
        vec![1],
        "[K..=K] must include K and exclude the longer K2"
    );
    assert_eq!(
        filter(&k, &k2),
        vec![1, 2],
        "[K..=K2] must include both K and K2"
    );
    assert_eq!(
        filter(&[0x10, 0x00], &[0x10, 0x10]),
        Vec::<u64>::new(),
        "a range strictly between K and K2 must exclude both"
    );
}
