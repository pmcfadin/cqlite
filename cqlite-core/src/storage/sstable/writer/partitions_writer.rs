//! BTI `Partitions.db` trie writer (issue #766, epic #762 writer fidelity D4).
//!
//! Phase 1 of BTI write support: emit a valid `Partitions.db` partition trie
//! that our own BTI reader
//! ([`crate::storage::sstable::bti::lookup_raw_key_in_bti_partitions_db`])
//! reads back to resolve `Data.db` byte offsets for each partition.
//!
//! ## What this produces
//!
//! A compact, header-less trie identical in layout to Cassandra 5.0's
//! `Partitions.db` (read by `parser.rs`):
//!
//! - Nodes are written **bottom-up**: every child appears at a *lower* file
//!   offset than its parent, so child pointers are stored as **backward
//!   deltas** (`child_pos = parent_pos − delta`). This matches
//!   `parser::parse_bti_node`'s sign convention.
//! - The root node's absolute byte offset is stored as the **last 8 bytes** of
//!   the file (big-endian `u64`), which is exactly what
//!   `lookup_partition_in_bti_file` reads from the footer.
//! - Every partition leaf is a `PayloadOnly` node (ordinal 0) whose payload is
//!   `[hash_byte] ++ SizedInts(position)` with
//!   `payloadBits = FLAG_HAS_HASH_BYTE + (position_bytes − 1)` and
//!   `position = !data_offset` (negative ⇒ direct `Data.db` offset, per
//!   `PartitionIndex.java` sign convention mirrored in
//!   [`crate::storage::sstable::bti::decode_bti_partition_payload`]).
//!
//! ## Trie key
//!
//! The byte-comparable lookup key for `Murmur3Partitioner` is produced by
//! [`crate::storage::sstable::bti::encode_partition_key_for_bti_trie`]:
//!
//! ```text
//! key = [0x40] ++ be8(murmur3_token(raw_key) ^ 0x8000_0000_0000_0000)
//! ```
//!
//! The writer builds the trie from these 9-byte keys. Because the leading
//! `0x40` byte is shared by every partition, the trie root has a single child
//! and the discriminating bytes are the 8 token bytes.
//!
//! ## Node types emitted
//!
//! The writer uses three of the reader's node categories:
//!
//! - `PayloadOnly` (ordinal 0) for leaves.
//! - `Sparse` (ordinals 5/7/8/9, sized by the largest backward delta among the
//!   node's children) for internal nodes with 1..=255 children, so it subsumes
//!   the single-child case too.
//! - `Dense` for an internal node with full 256-byte fan-out (which `Sparse`
//!   cannot encode — its child count is a single byte). The reader's
//!   `parse_bti_node` + `find_child` handle exactly these encodings.
//!
//! ## Out of scope (recorded for the epic)
//!
//! - `Rows.db` (within-partition clustering trie) is **not** written here. For
//!   phase 1 every partition payload encodes a direct `Data.db` offset
//!   (negative `position`), never a `RowsOffset`. Wide-partition row tries are a
//!   follow-up (see issue #766 / epic #762).
//! - `Single*` and 12-bit packed node variants are not emitted. They are valid
//!   and the reader parses them, but `PayloadOnly`/`Sparse`/`Dense` cover every
//!   trie phase 1 produces.

use crate::error::{Error, Result};
use crate::storage::sstable::bti::encode_partition_key_for_bti_trie;
use crate::storage::sstable::bti::parser::FLAG_HAS_HASH_BYTE;
use crate::util::cassandra_murmur3::cassandra_murmur3_token;
use std::collections::BTreeMap;

/// One partition's entry in the partition trie.
#[derive(Debug, Clone)]
pub struct PartitionTrieEntry {
    /// The 9-byte byte-comparable trie key
    /// (`[0x40] ++ be8(token ^ 0x8000_0000_0000_0000)`).
    key: [u8; 9],
    /// Filter hash byte (lowest 8 bits of the partition-key filter hash).
    hash_byte: u8,
    /// `Data.db` byte offset where this partition begins.
    data_offset: u64,
}

/// Builder that accumulates partition entries and serializes the BTI
/// `Partitions.db` trie.
#[derive(Debug, Default)]
pub struct PartitionsTrieWriter {
    entries: Vec<PartitionTrieEntry>,
}

impl PartitionsTrieWriter {
    /// Create an empty writer.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Number of partitions accumulated so far.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether any partitions have been accumulated.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Record a partition: its raw on-disk key bytes and its `Data.db` offset.
    ///
    /// The byte-comparable trie key and the filter hash byte are derived here
    /// from the raw partition-key bytes via the same `Murmur3Partitioner`
    /// encoding the reader expects.
    pub fn add_partition(&mut self, raw_key_bytes: &[u8], data_offset: u64) {
        let key = encode_partition_key_for_bti_trie(raw_key_bytes);
        let hash_byte = filter_hash_byte(raw_key_bytes);
        self.entries.push(PartitionTrieEntry {
            key,
            hash_byte,
            data_offset,
        });
    }

    /// Serialize the accumulated entries into the on-disk `Partitions.db`
    /// trie bytes (including the 8-byte big-endian root-offset footer).
    ///
    /// Returns an empty `Vec` if no partitions were recorded (an empty
    /// `Partitions.db`, mirroring an empty SSTable).
    pub fn finish(self) -> Result<Vec<u8>> {
        if self.entries.is_empty() {
            return Ok(Vec::new());
        }

        // Sort by byte-comparable trie key. The SSTable writer already enforces
        // ascending token order, and the trie key is monotonic in the token, so
        // this is typically a no-op — but sorting defensively guarantees a valid
        // trie regardless of caller ordering and rejects duplicate keys.
        let mut entries = self.entries;
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        for w in entries.windows(2) {
            if w[0].key == w[1].key {
                return Err(Error::InvalidInput(
                    "duplicate partition trie key (token collision) in Partitions.db".to_string(),
                ));
            }
        }

        let root = build_trie(&entries);
        serialize_trie(&root)
    }
}

/// Compute the filter hash byte stored at the front of each partition leaf
/// payload.
///
/// Cassandra stores the high byte of the partition key's Murmur3 hash here for
/// fast mismatch rejection. Our BTI reader does **not** verify this byte during
/// lookup (it skips it), so round-trip correctness does not depend on its exact
/// value; we nonetheless derive it from the same Murmur3 token the trie key
/// uses, keeping the payload self-consistent.
fn filter_hash_byte(raw_key_bytes: &[u8]) -> u8 {
    let token = cassandra_murmur3_token(raw_key_bytes);
    let bc = (token as u64) ^ 0x8000_0000_0000_0000u64;
    // High byte of the byte-comparable token (matches the first discriminating
    // trie byte, which is the most significant token byte).
    (bc >> 56) as u8
}

// ---------------------------------------------------------------------------
// Trie construction (in-memory)
// ---------------------------------------------------------------------------

/// An in-memory trie node prior to serialization.
enum TrieBuildNode {
    /// Leaf: a single partition's payload.
    Leaf { hash_byte: u8, data_offset: u64 },
    /// Internal node keyed by the next byte of each child's key.
    Internal {
        children: BTreeMap<u8, TrieBuildNode>,
    },
}

/// Build a radix-1 (byte-per-edge) trie from the sorted, de-duplicated entries.
///
/// Each entry's 9-byte key is inserted byte-by-byte; the terminal node is a
/// `Leaf`. Because all keys share length and are unique, no key is a prefix of
/// another, so every leaf sits at depth 9.
fn build_trie(entries: &[PartitionTrieEntry]) -> TrieBuildNode {
    let mut root = TrieBuildNode::Internal {
        children: BTreeMap::new(),
    };
    for entry in entries {
        insert(&mut root, &entry.key, entry.hash_byte, entry.data_offset);
    }
    root
}

fn insert(node: &mut TrieBuildNode, key: &[u8], hash_byte: u8, data_offset: u64) {
    match node {
        TrieBuildNode::Internal { children } => {
            if key.is_empty() {
                // Replace this internal node with a leaf in place is impossible
                // here because keys are fixed-length and unique; an empty key at
                // an internal node would mean a key was a prefix of another. This
                // branch is unreachable for valid 9-byte keys, but handle it
                // defensively by inserting a sentinel leaf under byte 0.
                children.entry(0).or_insert(TrieBuildNode::Leaf {
                    hash_byte,
                    data_offset,
                });
                return;
            }
            let first = key[0];
            let rest = &key[1..];
            if rest.is_empty() {
                children.insert(
                    first,
                    TrieBuildNode::Leaf {
                        hash_byte,
                        data_offset,
                    },
                );
            } else {
                let child = children
                    .entry(first)
                    .or_insert_with(|| TrieBuildNode::Internal {
                        children: BTreeMap::new(),
                    });
                insert(child, rest, hash_byte, data_offset);
            }
        }
        TrieBuildNode::Leaf { .. } => {
            // Unreachable for unique fixed-length keys.
        }
    }
}

// ---------------------------------------------------------------------------
// Trie serialization (bottom-up, backward-delta pointers)
// ---------------------------------------------------------------------------

/// Serialize the trie into `Partitions.db` bytes.
///
/// Performs a post-order traversal so each child is fully written (and its
/// absolute offset known) before its parent. Child pointers are encoded as
/// backward deltas `parent_pos − child_pos`, matching the reader. The final 8
/// bytes are the big-endian absolute offset of the root node.
fn serialize_trie(root: &TrieBuildNode) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let root_offset = write_node(root, &mut buf)?;
    buf.extend_from_slice(&(root_offset as u64).to_be_bytes());
    Ok(buf)
}

/// Write one node (and, recursively, its subtree) to `buf`, returning the
/// absolute offset at which this node's header byte was written.
fn write_node(node: &TrieBuildNode, buf: &mut Vec<u8>) -> Result<usize> {
    match node {
        TrieBuildNode::Leaf {
            hash_byte,
            data_offset,
        } => write_leaf(*hash_byte, *data_offset, buf),
        TrieBuildNode::Internal { children } => {
            // Post-order: write every child first so we know its offset.
            let mut child_offsets: Vec<(u8, usize)> = Vec::with_capacity(children.len());
            for (&byte, child) in children.iter() {
                let off = write_node(child, buf)?;
                child_offsets.push((byte, off));
            }
            // A radix-1 trie node can have up to 256 children. `Sparse` encodes
            // its child count in a single `u8` and therefore tops out at 255
            // transitions. When fan-out reaches the full 256-byte alphabet we
            // must emit a `Dense` node instead (the reader parses both). We keep
            // `Sparse` for every smaller node so existing tries are byte-for-byte
            // unchanged.
            if child_offsets.len() == 256 {
                write_dense(&child_offsets, buf)
            } else {
                write_sparse(&child_offsets, buf)
            }
        }
    }
}

/// Write a `PayloadOnly` (ordinal 0) leaf node.
///
/// Layout: `[header=(0<<4)|payloadBits] ++ [hash_byte] ++ SizedInts(position)`
/// where `position = !data_offset` and
/// `payloadBits = FLAG_HAS_HASH_BYTE + (position_bytes − 1)`.
fn write_leaf(hash_byte: u8, data_offset: u64, buf: &mut Vec<u8>) -> Result<usize> {
    // Negative `position` ⇒ direct Data.db offset (PartitionIndex sign convention).
    // `~data_offset` as i64. Guard against offsets that would overflow i64.
    if data_offset > i64::MAX as u64 {
        return Err(Error::InvalidInput(format!(
            "Data.db offset {data_offset} too large to encode as a signed BTI position"
        )));
    }
    let position: i64 = !(data_offset as i64);
    let position_bytes = sized_ints_non_zero_size(position);
    debug_assert!((1..=8).contains(&position_bytes));

    // payloadBits encodes (hash byte present) + (position byte count − 1).
    let payload_bits = FLAG_HAS_HASH_BYTE + (position_bytes as u8 - 1);
    debug_assert!(payload_bits <= 16);

    let offset = buf.len();
    // PayloadOnly is ordinal 0, so the high nibble is 0; the low nibble carries
    // payloadBits (≤ 8 + 7 = 15, fits in the nibble).
    let header = payload_bits & 0x0F;
    buf.push(header);
    buf.push(hash_byte);
    write_sized_int_be(buf, position, position_bytes);
    Ok(offset)
}

/// Write an internal node as a `Sparse` node, choosing the smallest pointer
/// width (1/2/3/5 bytes ⇒ ordinals 5/7/8/9) that fits the largest backward
/// delta among the children.
///
/// Layout (full-byte-pointer Sparse, matching the reader):
/// ```text
/// [header=(ordinal<<4)|0]   // internal nodes carry no payload
/// [count: u8]               // 1..=255 transitions
/// [transition bytes...]     // ascending
/// [count × ptr_bytes: backward deltas, big-endian]
/// ```
fn write_sparse(child_offsets: &[(u8, usize)], buf: &mut Vec<u8>) -> Result<usize> {
    let count = child_offsets.len();
    if count == 0 {
        return Err(Error::InvalidInput(
            "internal BTI trie node has no children".to_string(),
        ));
    }
    if count > 255 {
        // A radix-1 trie node has at most 256 children; 256 would need a Dense
        // node. With a single shared 0x40 prefix and 8 token bytes the fan-out
        // per node never reaches 256 in practice, but guard explicitly.
        return Err(Error::InvalidInput(format!(
            "BTI Sparse node fan-out {count} exceeds 255; Dense node required"
        )));
    }

    // The node's header offset is the current buffer length. Backward deltas are
    // measured from this header position to each child's header position.
    let node_offset = buf.len();

    // Determine the maximum backward delta to size the pointers.
    let max_delta = child_offsets
        .iter()
        .map(|(_, child_off)| node_offset - child_off)
        .max()
        .unwrap_or(0);
    let (ordinal, ptr_bytes) = sparse_ordinal_for_delta(max_delta as u64)?;

    // Internal nodes carry no payload, so the low nibble (payloadBits) is 0.
    let header = ordinal << 4;
    buf.push(header);
    buf.push(count as u8);
    // Transition bytes (child_offsets are already in ascending byte order
    // because they come from a BTreeMap iterator).
    for (byte, _) in child_offsets {
        buf.push(*byte);
    }
    // Backward-delta pointers, big-endian, fixed width.
    for (_, child_off) in child_offsets {
        let delta = (node_offset - child_off) as u64;
        write_be_unsigned(buf, delta, ptr_bytes);
    }
    Ok(node_offset)
}

/// Write an internal node as a `Dense` node, used when fan-out covers the full
/// 256-byte alphabet (`Sparse` cannot, as its count field is a single `u8`).
///
/// Layout (full-byte-pointer Dense, matching `parser.rs` ordinals 11-14):
/// ```text
/// [header=(ordinal<<4)|0]   // internal nodes carry no payload
/// [start_byte: u8]          // first transition character
/// [range_len - 1: u8]       // so range_len = byte + 1 (256 ⇒ stored as 255)
/// [range_len × ptr_bytes: backward deltas, big-endian]
/// ```
/// Each child at transition byte `b` lives at index `b - start_byte`; a delta of
/// `0` is the reader's "no transition" sentinel. Because we only emit Dense for a
/// full 256-child node, every slot is present and every delta is non-zero (each
/// child is written strictly before its parent).
fn write_dense(child_offsets: &[(u8, usize)], buf: &mut Vec<u8>) -> Result<usize> {
    debug_assert_eq!(child_offsets.len(), 256, "write_dense expects full fan-out");
    if child_offsets.len() != 256 {
        return Err(Error::InvalidInput(format!(
            "write_dense requires a full 256-child node, got {}",
            child_offsets.len()
        )));
    }

    // child_offsets come from a BTreeMap iterator, so they are already sorted by
    // transition byte. A full 256-child node spans bytes 0..=255 contiguously.
    let start_byte = child_offsets[0].0; // 0 for a full alphabet
    let node_offset = buf.len();

    let max_delta = child_offsets
        .iter()
        .map(|(_, child_off)| node_offset - child_off)
        .max()
        .unwrap_or(0);
    let (ordinal, ptr_bytes) = dense_ordinal_for_delta(max_delta as u64)?;

    // Internal nodes carry no payload, so the low nibble (payloadBits) is 0.
    let header = ordinal << 4;
    buf.push(header);
    buf.push(start_byte);
    // range_len - 1: 256 children ⇒ 255. The byte is `u8`, so 256 fits exactly.
    buf.push((child_offsets.len() - 1) as u8);
    // Backward-delta pointers in transition-byte order. Every slot is occupied,
    // so there are no "no transition" (delta 0) sentinels to emit.
    for (_, child_off) in child_offsets {
        let delta = (node_offset - child_off) as u64;
        write_be_unsigned(buf, delta, ptr_bytes);
    }
    Ok(node_offset)
}

/// Map a maximum backward delta to a full-byte-pointer Dense ordinal and its
/// pointer width (in bytes). Mirrors `parser::pointer_bytes_for_ordinal` for the
/// Dense ordinals (11=2B Dense16, 12=3B Dense24, 13=4B Dense32, 14=5B Dense40,
/// 15=8B LongDense).
fn dense_ordinal_for_delta(max_delta: u64) -> Result<(u8, usize)> {
    if max_delta <= 0xFFFF {
        Ok((11, 2))
    } else if max_delta <= 0xFF_FFFF {
        Ok((12, 3))
    } else if max_delta <= 0xFFFF_FFFF {
        Ok((13, 4))
    } else if max_delta <= 0xFF_FFFF_FFFF {
        Ok((14, 5))
    } else {
        Ok((15, 8))
    }
}

/// Map a maximum backward delta to a full-byte-pointer Sparse ordinal and its
/// pointer width (in bytes). Mirrors `parser::pointer_bytes_for_ordinal`.
fn sparse_ordinal_for_delta(max_delta: u64) -> Result<(u8, usize)> {
    // Sparse8=5 (1B), Sparse16=7 (2B), Sparse24=8 (3B), Sparse40=9 (5B).
    if max_delta <= 0xFF {
        Ok((5, 1))
    } else if max_delta <= 0xFFFF {
        Ok((7, 2))
    } else if max_delta <= 0xFF_FFFF {
        Ok((8, 3))
    } else if max_delta <= 0xFF_FFFF_FFFF {
        Ok((9, 5))
    } else {
        Err(Error::InvalidInput(format!(
            "BTI Partitions.db trie too large: backward delta {max_delta} exceeds 40 bits"
        )))
    }
}

// ---------------------------------------------------------------------------
// SizedInts write helpers (mirror sized_ints.rs read/non_zero_size)
// ---------------------------------------------------------------------------

/// Number of bytes needed to store a signed value, matching
/// `SizedInts.nonZeroSize` (see `sized_ints::non_zero_size`).
fn sized_ints_non_zero_size(value: i64) -> usize {
    let abs_value = if value < 0 { !value } else { value } as u64;
    if abs_value == 0 {
        return 1;
    }
    let significant_bits = 64 - abs_value.leading_zeros() as usize;
    (significant_bits + 1).div_ceil(8).clamp(1, 8)
}

/// Write a signed value as `bytes` big-endian bytes (`SizedInts.write`).
///
/// Stores the low `bytes` bytes of the two's-complement representation in
/// big-endian order, which `sized_ints_read_from_slice` sign-extends back.
fn write_sized_int_be(buf: &mut Vec<u8>, value: i64, bytes: usize) {
    let raw = value as u64;
    write_be_unsigned(buf, raw, bytes);
}

/// Write the low `bytes` bytes of `value` in big-endian order.
fn write_be_unsigned(buf: &mut Vec<u8>, value: u64, bytes: usize) {
    let all = value.to_be_bytes();
    // Take the last `bytes` bytes (the least-significant ones in big-endian).
    buf.extend_from_slice(&all[8 - bytes..]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::bti::sized_ints;
    use crate::storage::sstable::bti::{lookup_raw_key_in_bti_partitions_db, BtiPartitionLocation};
    use std::io::Cursor;

    /// `sized_ints_non_zero_size` must agree with the reader's `non_zero_size`.
    #[test]
    fn sized_int_size_matches_reader() {
        let values = [
            0i64,
            1,
            -1,
            127,
            -128,
            128,
            -129,
            255,
            -256,
            32767,
            -32768,
            32768,
            -32769,
            i64::MAX,
            i64::MIN,
            !0i64,
            !63i64,
            !125i64,
            !1000i64,
            !1_000_000i64,
            !300_000_000_000i64,
        ];
        for v in values {
            assert_eq!(
                sized_ints_non_zero_size(v),
                sized_ints::non_zero_size(v),
                "size mismatch for {v}"
            );
        }
    }

    /// A written SizedInt round-trips through the reader's `read`.
    #[test]
    fn sized_int_write_read_roundtrip() {
        for v in [0i64, !0i64, !63i64, !125i64, !1_000_000i64, i64::MIN] {
            let n = sized_ints_non_zero_size(v);
            let mut buf = Vec::new();
            write_sized_int_be(&mut buf, v, n);
            assert_eq!(buf.len(), n);
            let mut cur = Cursor::new(buf);
            let got = sized_ints::read(&mut cur, n).unwrap();
            assert_eq!(got, v, "SizedInt roundtrip failed for {v}");
        }
    }

    /// Build a trie from raw keys, then look every key back up through the
    /// reader and assert the resolved Data.db offset matches.
    fn assert_roundtrip(keys_and_offsets: &[(Vec<u8>, u64)]) {
        let mut w = PartitionsTrieWriter::new();
        for (k, off) in keys_and_offsets {
            w.add_partition(k, *off);
        }
        let bytes = w.finish().expect("finish trie");
        assert!(bytes.len() >= 8, "trie must include 8-byte footer");

        for (k, expected) in keys_and_offsets {
            let mut cur = Cursor::new(bytes.clone());
            let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, k)
                .expect("lookup")
                .unwrap_or_else(|| panic!("key {k:?} not found in written trie"));
            match loc {
                BtiPartitionLocation::DataOffset(got) => assert_eq!(
                    got, *expected,
                    "key {k:?}: expected DataOffset({expected}) got DataOffset({got})"
                ),
                BtiPartitionLocation::RowsOffset(r) => {
                    panic!("key {k:?}: phase-1 writer must emit DataOffset, got RowsOffset({r})")
                }
            }
        }
    }

    #[test]
    fn empty_trie_is_empty_bytes() {
        let w = PartitionsTrieWriter::new();
        assert!(w.finish().unwrap().is_empty());
    }

    #[test]
    fn single_partition_roundtrip() {
        assert_roundtrip(&[(vec![0x11u8; 16], 0)]);
    }

    #[test]
    fn three_uuid_partitions_roundtrip() {
        assert_roundtrip(&[
            (vec![0x11u8; 16], 63),
            (vec![0x22u8; 16], 0),
            (vec![0x33u8; 16], 125),
        ]);
    }

    #[test]
    fn large_offsets_roundtrip() {
        assert_roundtrip(&[
            (vec![0xA1u8; 16], 1_000_000),
            (vec![0xB2u8; 16], 300_000_000_000),
            (vec![0xC3u8; 16], 5),
        ]);
    }

    #[test]
    fn many_partitions_roundtrip() {
        // Exercise multi-level fan-out and varied token bytes.
        let mut data = Vec::new();
        for i in 0u64..200 {
            let mut key = vec![0u8; 16];
            key[0..8].copy_from_slice(&i.to_be_bytes());
            key[8..16].copy_from_slice(&(i.wrapping_mul(2654435761)).to_be_bytes());
            data.push((key, i * 37));
        }
        assert_roundtrip(&data);
    }

    #[test]
    fn duplicate_key_is_rejected() {
        let mut w = PartitionsTrieWriter::new();
        // Identical raw key bytes ⇒ identical token ⇒ identical trie key.
        w.add_partition(&[0x55u8; 16], 0);
        w.add_partition(&[0x55u8; 16], 100);
        assert!(w.finish().is_err());
    }

    /// Finding 1 (issue #766 review): an internal node whose fan-out covers all
    /// 256 possible transition bytes must serialize (as a Dense node) and round-
    /// trip through the reader. Previously the serializer rejected count == 256.
    ///
    /// We construct the trie directly so we can guarantee a full 256-byte fan-out
    /// at one node, independent of Murmur3 token distribution.
    #[test]
    fn full_256_fanout_internal_node_serializes_and_roundtrips() {
        use std::collections::BTreeMap;

        // Build a two-level trie: root has one child byte 0xFF leading to an
        // internal node with all 256 transition bytes, each pointing at a leaf.
        let mut inner_children: BTreeMap<u8, TrieBuildNode> = BTreeMap::new();
        for b in 0u16..=255 {
            inner_children.insert(
                b as u8,
                TrieBuildNode::Leaf {
                    hash_byte: b as u8,
                    data_offset: (b as u64) * 17,
                },
            );
        }
        let inner = TrieBuildNode::Internal {
            children: inner_children,
        };
        let mut root_children: BTreeMap<u8, TrieBuildNode> = BTreeMap::new();
        root_children.insert(0xFF, inner);
        let root = TrieBuildNode::Internal {
            children: root_children,
        };

        let bytes = serialize_trie(&root).expect("256-fan-out node must serialize");

        // Walk the trie via the reader's node parser for each terminal byte,
        // following root[0xFF] then inner[b], and confirm the resolved leaf
        // payload offset matches what we wrote.
        for b in 0u16..=255 {
            let key = [0xFFu8, b as u8];
            let loc = lookup_key_in_trie(&bytes, &key)
                .unwrap_or_else(|| panic!("byte {b} not found in 256-fan-out trie"));
            assert_eq!(
                loc,
                (b as u64) * 17,
                "byte {b}: wrong Data.db offset resolved"
            );
        }
    }

    /// Resolve a raw trie key (the byte-comparable bytes traversed from the
    /// root) to its leaf's decoded Data.db offset, using the production BTI
    /// parser/lookup path. `key` is the already-encoded trie key (no Murmur3
    /// transform applied), so `lookup_partition_in_bti_file` walks it directly.
    fn lookup_key_in_trie(bytes: &[u8], key: &[u8]) -> Option<u64> {
        use crate::storage::sstable::bti::lookup_partition_in_bti_file;
        let mut cur = Cursor::new(bytes.to_vec());
        match lookup_partition_in_bti_file(&mut cur, key).ok()?? {
            BtiPartitionLocation::DataOffset(o) => Some(o),
            BtiPartitionLocation::RowsOffset(_) => None,
        }
    }
}
