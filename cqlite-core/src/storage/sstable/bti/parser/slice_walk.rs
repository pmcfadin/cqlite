//! Zero-copy, zero-alloc BTI `Partitions.db` trie descent (issue #1574, audit C3).
//!
//! Two hot-path wins over the `Read + Seek` entry point in [`super::partitions`]:
//!
//! 1. **Zero-copy lookup** — [`lookup_partition_in_bti_slice`] parses the 8-byte
//!    big-endian root-offset footer and walks a borrowed `&[u8]` view of the
//!    already-resident `Partitions.db` buffer in place, instead of `read_exact`ing
//!    the whole trie into a fresh `Vec<u8>` on every lookup.
//! 2. **Zero-alloc child descent** — [`find_child_offset`] decodes ONLY the child
//!    pointer for the searched byte directly from the node's byte slice, for every
//!    `TrieNode` ordinal, instead of materializing the node's full child table
//!    (`Vec<Transition>` / `Vec<Option<SizedPointer>>`) just to follow one byte.
//!
//! Both are byte-faithful to the existing decoders: the resolved child offsets and
//! partition locations are identical to [`super::node_decode::parse_bti_node`] +
//! `BtiNode::find_child` and to [`super::partitions::lookup_partition_in_bti_file`].
//! Node-type decoding follows `TrieNode.java` exactly; a structurally invalid or
//! truncated node is an error, never a silent miss (no-heuristics).

use crate::{error::Error, storage::sstable::bti::node::BtiResult};

use super::node_decode::{classify_node_nibble, read_12bit_packed, read_be_unsigned};
use super::partitions::{encode_partition_key_for_bti_trie, walk_bti_trie, BtiPartitionLocation};

/// Resolve the absolute trie offset of the child reachable from the node at
/// `node_offset` via `search_byte`, decoding ONLY that one child pointer in place.
///
/// Returns:
/// - `Ok(Some(child_offset))` — a transition for `search_byte` exists.
/// - `Ok(None)` — no such transition (key not in this subtree).
/// - `Err(_)` — the node is out of bounds or structurally truncated (identical to
///   the failure [`super::node_decode::parse_bti_node`] would report).
///
/// This is the allocation-free analogue of `parse_bti_node(...).find_child(byte)`
/// and produces bit-identical child offsets (same `saturating_sub` arithmetic and
/// same Dense delta-0 "no transition" sentinel). It is the descent primitive behind
/// [`super::partitions::walk_bti_trie`].
pub(crate) fn find_child_offset(
    trie_data: &[u8],
    node_offset: usize,
    search_byte: u8,
) -> BtiResult<Option<usize>> {
    if node_offset >= trie_data.len() {
        return Err(Error::Parse(format!(
            "BTI node offset {node_offset} out of bounds (trie_data.len={})",
            trie_data.len()
        )));
    }
    let data = &trie_data[node_offset..];
    let header_byte = data[0];
    let ordinal = (header_byte >> 4) & 0x0F;
    // Validate the ordinal is a known node type (mirrors parse_bti_node), so a
    // corrupt nibble errors here rather than silently missing.
    let _ = classify_node_nibble(ordinal)?;
    let off = node_offset as u64;

    match ordinal {
        // PayloadOnly — leaf, no children.
        0 => Ok(None),

        // SingleNoPayload4: [ordinal|delta4][transition]; delta in low nibble.
        1 => {
            require_len(data, 2, "SingleNoPayload4")?;
            if data[1] != search_byte {
                return Ok(None);
            }
            let delta = (header_byte & 0x0F) as u64;
            Ok(Some(off.saturating_sub(delta) as usize))
        }
        // Single8: [ordinal|pf][transition][1-byte delta].
        2 => single_child(data, off, 1, search_byte, "Single8"),
        // SingleNoPayload12: [ordinal|delta_hi4][delta_lo8][transition].
        3 => {
            require_len(data, 3, "SingleNoPayload12")?;
            if data[2] != search_byte {
                return Ok(None);
            }
            let delta = (((header_byte & 0x0F) as u64) << 8) | (data[1] as u64);
            Ok(Some(off.saturating_sub(delta) as usize))
        }
        // Single16: [ordinal|pf][transition][2-byte delta].
        4 => single_child(data, off, 2, search_byte, "Single16"),

        // Sparse: [ordinal|pf][count][count transition bytes][count deltas].
        5 => sparse_child(data, off, ptr_width(1), search_byte, "Sparse8"),
        6 => sparse_child(data, off, PtrWidth::Packed12, search_byte, "Sparse12"),
        7 => sparse_child(data, off, ptr_width(2), search_byte, "Sparse16"),
        8 => sparse_child(data, off, ptr_width(3), search_byte, "Sparse24"),
        9 => sparse_child(data, off, ptr_width(5), search_byte, "Sparse40"),

        // Dense: [ordinal|pf][start][len-1][range deltas]; delta 0 == no transition.
        10 => dense_child(data, off, PtrWidth::Packed12, search_byte, "Dense12"),
        11 => dense_child(data, off, ptr_width(2), search_byte, "Dense16"),
        12 => dense_child(data, off, ptr_width(3), search_byte, "Dense24"),
        13 => dense_child(data, off, ptr_width(4), search_byte, "Dense32"),
        14 => dense_child(data, off, ptr_width(5), search_byte, "Dense40"),
        15 => dense_child(data, off, ptr_width(8), search_byte, "LongDense"),

        // classify_node_nibble already rejected everything else.
        _ => Ok(None),
    }
}

/// Pointer encoding width for a node's child deltas.
#[derive(Clone, Copy)]
enum PtrWidth {
    /// Fixed big-endian delta of N bytes.
    Fixed(usize),
    /// 12-bit packed delta (Sparse12 / Dense12).
    Packed12,
}

fn ptr_width(n: usize) -> PtrWidth {
    PtrWidth::Fixed(n)
}

fn require_len(data: &[u8], needed: usize, what: &str) -> BtiResult<()> {
    if data.len() < needed {
        return Err(Error::Parse(format!(
            "{what} BTI node too short: need {needed} bytes, have {}",
            data.len()
        )));
    }
    Ok(())
}

/// Single8 / Single16: `[header][transition][ptr_bytes delta]`.
fn single_child(
    data: &[u8],
    off: u64,
    ptr_bytes: usize,
    search_byte: u8,
    what: &str,
) -> BtiResult<Option<usize>> {
    require_len(data, 2 + ptr_bytes, what)?;
    if data[1] != search_byte {
        return Ok(None);
    }
    let delta = read_be_unsigned(&data[2..2 + ptr_bytes]);
    Ok(Some(off.saturating_sub(delta) as usize))
}

/// Sparse node: read `count`, scan the transition bytes for `search_byte`, and
/// decode only that index's delta. Cassandra stores the transition bytes sorted
/// and unique, so a linear scan for the first match is equivalent to the binary
/// search `BtiNode::find_child` performs.
fn sparse_child(
    data: &[u8],
    off: u64,
    width: PtrWidth,
    search_byte: u8,
    what: &str,
) -> BtiResult<Option<usize>> {
    require_len(data, 2, what)?;
    let count = data[1] as usize;
    if count == 0 {
        return Err(Error::Parse(format!(
            "{what} BTI node must have at least one transition"
        )));
    }
    let bytes_start = 2;
    let pointers_start = bytes_start + count;
    // Validate the full pointer area up front so a truncated node errors even when
    // the searched byte is not present (matches parse_bti_node's length check).
    let ptr_area = match width {
        PtrWidth::Fixed(n) => count * n,
        PtrWidth::Packed12 => (count * 3).div_ceil(2),
    };
    require_len(data, pointers_start + ptr_area, what)?;

    let Some(idx) = data[bytes_start..pointers_start]
        .iter()
        .position(|&b| b == search_byte)
    else {
        return Ok(None);
    };
    let delta = match width {
        PtrWidth::Fixed(n) => {
            let p = pointers_start + idx * n;
            read_be_unsigned(&data[p..p + n])
        }
        PtrWidth::Packed12 => read_12bit_packed(&data[pointers_start..], idx),
    };
    Ok(Some(off.saturating_sub(delta) as usize))
}

/// Dense node: `[header][start][len-1][range deltas]`; the slot for `search_byte`
/// is `search_byte - start`. A delta of `0` is the "no transition" sentinel.
fn dense_child(
    data: &[u8],
    off: u64,
    width: PtrWidth,
    search_byte: u8,
    what: &str,
) -> BtiResult<Option<usize>> {
    require_len(data, 3, what)?;
    let start_byte = data[1];
    let range_len = data[2] as usize + 1;
    let ptr_area = match width {
        PtrWidth::Fixed(n) => range_len * n,
        PtrWidth::Packed12 => (range_len * 3).div_ceil(2),
    };
    require_len(data, 3 + ptr_area, what)?;

    if search_byte < start_byte {
        return Ok(None);
    }
    let idx = (search_byte - start_byte) as usize;
    if idx >= range_len {
        return Ok(None);
    }
    let delta = match width {
        PtrWidth::Fixed(n) => {
            let p = 3 + idx * n;
            read_be_unsigned(&data[p..p + n])
        }
        PtrWidth::Packed12 => read_12bit_packed(&data[3..], idx),
    };
    // delta 0 is the sentinel for "no child at this byte" (a real child may live at
    // absolute offset 0 only via a non-zero delta equal to `off`).
    if delta == 0 {
        Ok(None)
    } else {
        Ok(Some(off.saturating_sub(delta) as usize))
    }
}

/// Look up a partition by its byte-comparable encoded key directly in a resident
/// `Partitions.db` byte buffer, WITHOUT copying the trie.
///
/// `file_bytes` is the full `Partitions.db` file (trie bytes + 8-byte big-endian
/// root-offset footer). This is the zero-copy analogue of
/// [`super::partitions::lookup_partition_in_bti_file`] and returns the identical
/// [`BtiPartitionLocation`].
pub fn lookup_partition_in_bti_slice(
    file_bytes: &[u8],
    encoded_key: &[u8],
) -> BtiResult<Option<BtiPartitionLocation>> {
    let file_size = file_bytes.len();
    if file_size < 8 {
        return Err(Error::Parse(format!(
            "BTI Partitions.db is too small ({file_size} bytes; need at least 8 for footer)"
        )));
    }
    let trie_size = file_size - 8;
    let root_offset = u64::from_be_bytes([
        file_bytes[trie_size],
        file_bytes[trie_size + 1],
        file_bytes[trie_size + 2],
        file_bytes[trie_size + 3],
        file_bytes[trie_size + 4],
        file_bytes[trie_size + 5],
        file_bytes[trie_size + 6],
        file_bytes[trie_size + 7],
    ]);
    if root_offset as usize >= trie_size {
        return Err(Error::Parse(format!(
            "BTI Partitions.db: root_offset {root_offset} >= trie_size {trie_size}"
        )));
    }
    walk_bti_trie(&file_bytes[..trie_size], root_offset as usize, encoded_key)
}

/// Look up a raw partition key in a resident `Partitions.db` buffer using the
/// `Murmur3Partitioner` byte-comparable encoding, WITHOUT copying the trie.
///
/// Zero-copy analogue of [`super::partitions::lookup_raw_key_in_bti_partitions_db`].
pub fn lookup_raw_key_in_bti_partitions_slice(
    file_bytes: &[u8],
    raw_key_bytes: &[u8],
) -> BtiResult<Option<BtiPartitionLocation>> {
    let encoded = encode_partition_key_for_bti_trie(raw_key_bytes);
    lookup_partition_in_bti_slice(file_bytes, &encoded)
}

#[cfg(test)]
mod tests {
    use super::super::node_decode::parse_bti_node;
    use super::super::partitions::lookup_partition_in_bti_file;
    use super::*;
    use crate::storage::sstable::bti::node::BtiNodeData;
    use std::io::Cursor;

    /// Build a complete synthetic Partitions.db (12-byte trie + 8-byte footer):
    /// Sparse8 root at offset 6 over two PayloadOnly leaves (offsets 0 and 3).
    fn synthetic_partitions_db() -> Vec<u8> {
        let mut f = vec![0u8; 12 + 8];
        // Leaf A @0: DataOffset(0)
        f[0] = 0x08;
        f[1] = 0x11;
        f[2] = 0xFF; // -1 as i8 → ~(-1) = 0
                     // Leaf B @3: DataOffset(64)
        f[3] = 0x08;
        f[4] = 0x22;
        f[5] = 0xBF; // -65 as i8 → 64
                     // Sparse8 root @6: count=2, bytes {0xAA,0xBB}, deltas {6,3}
        f[6] = 0x50;
        f[7] = 0x02;
        f[8] = 0xAA;
        f[9] = 0xBB;
        f[10] = 0x06;
        f[11] = 0x03;
        f[12..20].copy_from_slice(&6u64.to_be_bytes());
        f
    }

    #[test]
    fn slice_lookup_matches_stream_lookup_synthetic() {
        let file = synthetic_partitions_db();
        for (key, expect) in [
            (vec![0xAAu8], Some(BtiPartitionLocation::DataOffset(0))),
            (vec![0xBB], Some(BtiPartitionLocation::DataOffset(64))),
            (vec![0xCC], None),
        ] {
            let via_slice = lookup_partition_in_bti_slice(&file, &key).unwrap();
            let via_stream =
                lookup_partition_in_bti_file(&mut Cursor::new(file.clone()), &key).unwrap();
            assert_eq!(via_slice, expect, "slice lookup for {key:?}");
            assert_eq!(via_slice, via_stream, "slice must equal stream for {key:?}");
        }
    }

    #[test]
    fn slice_lookup_too_small_is_error() {
        assert!(lookup_partition_in_bti_slice(&[0u8; 4], &[0x00]).is_err());
    }

    /// `find_child_offset` must agree with `parse_bti_node(...).find_child(...)`
    /// for every crafted node: same `Some(child)` / `None` for present/absent bytes.
    fn assert_find_child_agrees(node_bytes: &[u8], node_offset: usize, probe_bytes: &[u8]) {
        // Build a trie buffer with the node placed at `node_offset`.
        let mut trie = vec![0u8; node_offset];
        trie.extend_from_slice(node_bytes);
        let parsed = parse_bti_node(&trie[node_offset..], node_offset as u64).unwrap();
        for &b in probe_bytes {
            let in_place = find_child_offset(&trie, node_offset, b).unwrap();
            let via_parse = parsed.find_child(b).map(|p| p.distance as usize);
            assert_eq!(
                in_place, via_parse,
                "find_child_offset disagrees with parse for byte {b:#04x} at node_offset {node_offset}"
            );
        }
    }

    #[test]
    fn find_child_offset_single8() {
        // Single8 @offset 10: transition 0x41, delta 4 → child @6.
        assert_find_child_agrees(&[0x20, 0x41, 0x04], 10, &[0x40, 0x41, 0x42]);
    }

    #[test]
    fn find_child_offset_single_nopayload4() {
        // SingleNoPayload4 @8: delta 3 in low nibble, transition 0x55 → child @5.
        assert_find_child_agrees(&[0x13, 0x55], 8, &[0x54, 0x55, 0x56]);
    }

    #[test]
    fn find_child_offset_single_nopayload12() {
        // SingleNoPayload12 @300: delta 0x101, transition 0x7A.
        let node = [0x30 | 0x01u8, 0x01, 0x7A];
        assert_find_child_agrees(&node, 300, &[0x79, 0x7A, 0x7B]);
    }

    #[test]
    fn find_child_offset_single16() {
        // Single16 @600: transition 0x33, delta 0x0102.
        assert_find_child_agrees(&[0x40, 0x33, 0x01, 0x02], 600, &[0x32, 0x33, 0x34]);
    }

    #[test]
    fn find_child_offset_sparse8() {
        // Sparse8 @20: bytes {0x10,0x20,0x30}, deltas {5,10,15}.
        let node = [0x50u8, 0x03, 0x10, 0x20, 0x30, 0x05, 0x0A, 0x0F];
        assert_find_child_agrees(&node, 20, &[0x0F, 0x10, 0x20, 0x30, 0x31]);
    }

    #[test]
    fn find_child_offset_sparse16() {
        // Sparse16 @400: bytes {0x10,0x20}, deltas {0x0101,0x0102}.
        let node = [0x70u8, 0x02, 0x10, 0x20, 0x01, 0x01, 0x01, 0x02];
        assert_find_child_agrees(&node, 400, &[0x0F, 0x10, 0x20, 0x21]);
    }

    #[test]
    fn find_child_offset_sparse24_and_40() {
        // Sparse24 @1000: byte 0x10 delta 0x010203.
        let s24 = [0x80u8, 0x01, 0x10, 0x01, 0x02, 0x03];
        assert_find_child_agrees(&s24, 1000, &[0x10, 0x11]);
        // Sparse40 @2000: byte 0x10 delta 0x0102030405.
        let s40 = [0x90u8, 0x01, 0x10, 0x01, 0x02, 0x03, 0x04, 0x05];
        assert_find_child_agrees(&s40, 2000, &[0x10, 0x11]);
    }

    #[test]
    fn find_child_offset_sparse12() {
        // Sparse12 @50: bytes {0x10,0x20}, packed deltas {0x012, 0x034}.
        // pack pairs: p0=0x012, p1=0x034 → [0x01][0x20|0x00][0x34] = [0x01,0x20,0x34]
        let node = [0x60u8, 0x02, 0x10, 0x20, 0x01, 0x20, 0x34];
        assert_find_child_agrees(&node, 50, &[0x0F, 0x10, 0x20, 0x21]);
    }

    #[test]
    fn find_child_offset_dense16_with_gap_and_zero_offset_child() {
        // Dense16 @16: start 0x10, len 3, deltas {16, 0, 14}.
        // delta 16 → child @0 (a real child at absolute offset 0); delta 0 → no
        // transition (0x11); delta 14 → child @2.
        let node = [0xB0u8, 0x10, 0x02, 0x00, 0x10, 0x00, 0x00, 0x00, 0x0E];
        assert_find_child_agrees(&node, 16, &[0x0F, 0x10, 0x11, 0x12, 0x13]);
    }

    #[test]
    fn find_child_offset_dense12() {
        // Dense12 @40: start 0x10, len 2, packed deltas {0x010, 0x000}.
        // pack: p0=0x010,p1=0x000 → [0x01][0x00|0x00][0x00] = [0x01,0x00,0x00]
        let node = [0xA0u8, 0x10, 0x01, 0x01, 0x00, 0x00];
        assert_find_child_agrees(&node, 40, &[0x0F, 0x10, 0x11, 0x12]);
    }

    #[test]
    fn find_child_offset_dense24_32_40_longdense() {
        // Dense24 @5000: start 0x10 len 1 delta 0x010203.
        assert_find_child_agrees(&[0xC0u8, 0x10, 0x00, 0x01, 0x02, 0x03], 5000, &[0x10, 0x11]);
        // Dense32 @70000: start 0x10 len 1 delta 0x01020304.
        assert_find_child_agrees(
            &[0xD0u8, 0x10, 0x00, 0x01, 0x02, 0x03, 0x04],
            70000,
            &[0x10, 0x11],
        );
        // Dense40 @90000: start 0x10 len 1 delta 0x0102030405.
        assert_find_child_agrees(
            &[0xE0u8, 0x10, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05],
            90000,
            &[0x10, 0x11],
        );
        // LongDense @200000: start 0x10 len 1 delta 0x0000000000030000.
        assert_find_child_agrees(
            &[
                0xF0u8, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
            ],
            200000,
            &[0x10, 0x11],
        );
    }

    #[test]
    fn find_child_offset_payload_only_has_no_children() {
        let trie = [0x08u8, 0x11, 0xFF];
        assert_eq!(find_child_offset(&trie, 0, 0x00).unwrap(), None);
    }

    #[test]
    fn find_child_offset_truncated_node_is_error() {
        // Single8 header claims a 1-byte delta but the node is cut short.
        let trie = [0x20u8, 0x41]; // missing delta byte
        assert!(find_child_offset(&trie, 0, 0x41).is_err());
    }

    #[test]
    fn find_child_offset_out_of_bounds_is_error() {
        let trie = [0x08u8];
        assert!(find_child_offset(&trie, 5, 0x00).is_err());
    }

    #[test]
    fn find_child_offset_sparse_kind_is_sparse() {
        // Guard that the crafted Sparse node parses to a Sparse variant (so the
        // agreement test above is exercising the intended path).
        let node = [0x50u8, 0x02, 0x10, 0x20, 0x05, 0x0A];
        let parsed = parse_bti_node(&node, 0).unwrap();
        assert!(matches!(parsed.data, BtiNodeData::Sparse { .. }));
    }
}
