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
//! ## Rows.db (within-partition row-index trie) — issue #910
//!
//! [`RowsTrieWriter`] (this module) serializes `Rows.db`: one independent
//! per-partition row-index trie + `TrieIndexEntry` for each WIDE partition
//! (`>= 2` column-index blocks). A wide partition's `Partitions.db` leaf then
//! stores a **positive** `RowsOffset` ([`PartitionPayload::RowsOffset`])
//! pointing at its `TrieIndexEntry`; narrow partitions keep the **negative**
//! direct `Data.db` offset ([`PartitionPayload::DataOffset`]). The leaf payloads
//! and entry layout match the reader exactly
//! ([`crate::storage::sstable::bti::parser::decode_bti_row_payload`] /
//! [`resolve_rows_db_entry`](crate::storage::sstable::bti::resolve_rows_db_entry)).
//!
//! ## Out of scope (recorded for the epic)
//!
//! - `Single*` and 12-bit packed node variants are not emitted. They are valid
//!   and the reader parses them, but `PayloadOnly`/`Sparse`/`Dense` cover every
//!   trie this writer produces.
//! - Per-block **open range-tombstone markers** are supported on the wire
//!   ([`RowIndexBlock::open_marker`]) but the `SSTableWriter` BTI path does not
//!   yet derive them from Data.db range-tombstone state; it passes `None`
//!   (the common case). Partition-level deletions ARE wired through.

use crate::error::{Error, Result};
use crate::storage::sstable::bti::encode_partition_key_for_bti_trie;
use crate::storage::sstable::bti::parser::FLAG_HAS_HASH_BYTE;
use crate::util::cassandra_murmur3::cassandra_partition_filter_hash_lower_bits;
use std::collections::BTreeMap;

/// Where a partition leaf payload points (issue #910).
///
/// Mirrors `BtiPartitionLocation` on the read side and Cassandra's
/// `PartitionIndex` position-sign convention: a **direct** `Data.db` offset is
/// stored as a *negative* `position = ~data_offset`; a **row-index** offset is
/// stored as a *non-negative* `position = rows_offset` that points at the
/// partition's `TrieIndexEntry` in `Rows.db`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionPayload {
    /// Direct `Data.db` byte offset (narrow partition; `< 2` column-index
    /// blocks). Encoded as `position = ~data_offset` (negative).
    DataOffset(u64),
    /// Offset of this partition's `TrieIndexEntry` in `Rows.db` (wide partition;
    /// `>= 2` column-index blocks). Encoded as `position = rows_offset`
    /// (non-negative).
    RowsOffset(u64),
}

/// One partition's entry in the partition trie.
#[derive(Debug, Clone)]
pub struct PartitionTrieEntry {
    /// The 9-byte byte-comparable trie key
    /// (`[0x40] ++ be8(token ^ 0x8000_0000_0000_0000)`).
    key: [u8; 9],
    /// Filter hash byte (lowest 8 bits of the partition-key filter hash).
    hash_byte: u8,
    /// Where this partition's leaf payload points (`Data.db` or `Rows.db`).
    payload: PartitionPayload,
}

/// Builder that accumulates partition entries and serializes the BTI
/// `Partitions.db` trie.
///
/// Only the two boundary raw partition keys (lowest- and highest-token, i.e.
/// min/max trie key) are retained for the Cassandra `PartitionIndex`
/// first/last-key region. Interior raw keys are never read, so they are not
/// stored (issue #1678): retaining a full raw-key copy per partition wasted
/// ~1.6 MB for 100k 16-byte keys and grew linearly with partition count.
#[derive(Debug, Default)]
pub struct PartitionsTrieWriter {
    entries: Vec<PartitionTrieEntry>,
    /// `(min_trie_key, raw_key)` for the lowest-token partition seen so far.
    /// Mirrors `entries.first()` after `finish` sorts by trie key.
    first_raw: Option<([u8; 9], Vec<u8>)>,
    /// `(max_trie_key, raw_key)` for the highest-token partition seen so far.
    /// Mirrors `entries.last()` after `finish` sorts by trie key.
    last_raw: Option<([u8; 9], Vec<u8>)>,
}

impl PartitionsTrieWriter {
    /// Create an empty writer.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            first_raw: None,
            last_raw: None,
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

    /// Total retained raw-key bytes for the boundary (first/last) keys only.
    ///
    /// Interior partition keys are not retained (issue #1678), so this is bounded
    /// by two partition-key sizes regardless of partition count. Used by the
    /// retention-budget test to prove no interior copies accumulate.
    #[cfg(test)]
    pub(crate) fn retained_raw_key_bytes(&self) -> usize {
        self.first_raw.as_ref().map_or(0, |(_, k)| k.len())
            + self.last_raw.as_ref().map_or(0, |(_, k)| k.len())
    }

    /// Record a partition: its raw on-disk key bytes and its `Data.db` offset.
    ///
    /// Convenience wrapper for a **narrow** partition whose leaf points directly
    /// into `Data.db` (the common, `< 2` column-index-block case).
    ///
    /// The byte-comparable trie key and the filter hash byte are derived here
    /// from the raw partition-key bytes via the same `Murmur3Partitioner`
    /// encoding the reader expects.
    pub fn add_partition(&mut self, raw_key_bytes: &[u8], data_offset: u64) {
        self.add_partition_with_payload(raw_key_bytes, PartitionPayload::DataOffset(data_offset));
    }

    /// Record a partition with an explicit payload target (issue #910).
    ///
    /// Use [`PartitionPayload::RowsOffset`] for a **wide** partition whose leaf
    /// must point at its `TrieIndexEntry` in `Rows.db` (positive position), or
    /// [`PartitionPayload::DataOffset`] for a narrow partition (negative
    /// position, direct `Data.db` offset).
    pub fn add_partition_with_payload(&mut self, raw_key_bytes: &[u8], payload: PartitionPayload) {
        let key = encode_partition_key_for_bti_trie(raw_key_bytes);
        let hash_byte = filter_hash_byte(raw_key_bytes);

        // Track only the boundary raw keys (min/max trie key). `finish` sorts by
        // trie key before writing the first/last-key region, so the boundaries
        // must correspond to the min/max trie key, NOT insertion order. Clone the
        // raw bytes only when a new min or max is seen (issue #1678).
        match &self.first_raw {
            Some((min_key, _)) if key >= *min_key => {}
            _ => self.first_raw = Some((key, raw_key_bytes.to_vec())),
        }
        match &self.last_raw {
            Some((max_key, _)) if key <= *max_key => {}
            _ => self.last_raw = Some((key, raw_key_bytes.to_vec())),
        }

        self.entries.push(PartitionTrieEntry {
            key,
            hash_byte,
            payload,
        });
    }

    /// Serialize the accumulated entries into the on-disk `Partitions.db` bytes
    /// in Cassandra's canonical `PartitionIndex` layout (issue #911):
    ///
    /// ```text
    /// [ trie nodes ............ ]   root node at absolute offset `root`
    /// [ firstKey  (u16 len + bytes) ]   at absolute offset `firstPos`
    /// [ lastKey   (u16 len + bytes) ]
    /// [ footer: i64 firstPos | i64 keyCount | i64 root ]   last 24 bytes
    /// ```
    ///
    /// `PartitionIndex.load` (cassandra-5.0.0) reads the 24-byte footer from
    /// `dataLength() - 24`, seeks to `firstPos`, then reads the first and last
    /// decorated keys via `ByteBufferUtil.readWithShortLength`. The trie node
    /// encoding is unchanged (already Cassandra-`TrieNode`-compatible and verified
    /// against the real `da` fixtures); only this footer + first/last-key region
    /// is added so `sstabledump`/`sstablemetadata` and a live node can open the
    /// index. The final 8 bytes remain `root`, so CQLite's own footer-based reader
    /// (which reads `root` from the last 8 bytes) is unaffected.
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

        // First/last decorated key (lowest/highest token after the trie-key sort).
        // These boundary raw keys were tracked incrementally by min/max trie key
        // in `add_partition_with_payload`, so they are byte-identical to
        // `entries.first()/last()` after the sort — without retaining an interior
        // raw-key copy per partition (issue #1678). Bounded copies (partition-key
        // sized); `entries` is non-empty here, so both boundaries are `Some`.
        let first_key = self.first_raw.map(|(_, k)| k).unwrap_or_default();
        let last_key = self.last_raw.map(|(_, k)| k).unwrap_or_default();
        let key_count = entries.len() as i64;

        // 1. Serialize the trie nodes; `root` is the absolute offset of the root
        //    node within the buffer (post-order write, backward-delta pointers).
        let root = build_trie(&entries);
        let mut buf = Vec::new();
        let root_offset = write_node(&root, &mut buf)?;

        // 2. firstPos marks the start of the first/last-key region.
        let first_pos = buf.len() as i64;
        write_with_short_length(&mut buf, &first_key)?;
        write_with_short_length(&mut buf, &last_key)?;

        // 3. 24-byte footer: firstPos, keyCount, root (each i64 BE). The last 8
        //    bytes stay `root` so CQLite's existing footer reader is unaffected.
        buf.extend_from_slice(&first_pos.to_be_bytes());
        buf.extend_from_slice(&key_count.to_be_bytes());
        buf.extend_from_slice(&(root_offset as i64).to_be_bytes());
        Ok(buf)
    }
}

/// Write a byte slice with a 2-byte big-endian unsigned-short length prefix
/// (Cassandra `ByteBufferUtil.writeWithShortLength`). Keys longer than
/// `u16::MAX` are rejected — a partition key cannot exceed 64 KiB in Cassandra.
fn write_with_short_length(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if bytes.len() > u16::MAX as usize {
        return Err(Error::InvalidInput(format!(
            "partition key too long for Partitions.db short-length prefix: {} bytes",
            bytes.len()
        )));
    }
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Compute the canonical filter hash byte stored at the front of each partition
/// leaf payload.
///
/// Cassandra's BTI partition index writes `(byte) decoratedKey.filterHashLowerBits()`
/// as each leaf's hash byte for fast mismatch rejection (see
/// `org.apache.cassandra.io.sstable.format.bti.PartitionIndex` and
/// `org.apache.cassandra.db.DecoratedKey.filterHashLowerBits()`, Cassandra 5.0.8).
/// `filterHashLowerBits()` returns `hash[1]` — the **second** 64-bit word (`h2`) of
/// the partition key's 128-bit Murmur3 hash — which is a *distinct* quantity from the
/// token (`hash[0]` / `h1`) used to build the byte-comparable trie key. The stored
/// byte is the low 8 bits of that `h2` value.
///
/// This is the canonical, authoritative derivation (no heuristics, issue #28), verified
/// byte-for-byte against the real `da-2-bti-Partitions.db` fixture — see the
/// `canonical_filter_hash_byte_matches_real_bti_fixture` vector test below. It replaces
/// the earlier phase-1 placeholder that derived the byte from the trie-key token (`h1`),
/// which produced values a Cassandra-canonical reader would reject.
fn filter_hash_byte(raw_key_bytes: &[u8]) -> u8 {
    cassandra_partition_filter_hash_lower_bits(raw_key_bytes) as u8
}

// ---------------------------------------------------------------------------
// Trie construction (in-memory)
// ---------------------------------------------------------------------------

/// An in-memory trie node prior to serialization.
enum TrieBuildNode {
    /// Leaf: a single partition's payload.
    Leaf {
        hash_byte: u8,
        payload: PartitionPayload,
    },
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
        insert(&mut root, &entry.key, entry.hash_byte, entry.payload);
    }
    root
}

fn insert(node: &mut TrieBuildNode, key: &[u8], hash_byte: u8, payload: PartitionPayload) {
    match node {
        TrieBuildNode::Internal { children } => {
            if key.is_empty() {
                // Replace this internal node with a leaf in place is impossible
                // here because keys are fixed-length and unique; an empty key at
                // an internal node would mean a key was a prefix of another. This
                // branch is unreachable for valid 9-byte keys, but handle it
                // defensively by inserting a sentinel leaf under byte 0.
                children
                    .entry(0)
                    .or_insert(TrieBuildNode::Leaf { hash_byte, payload });
                return;
            }
            let first = key[0];
            let rest = &key[1..];
            if rest.is_empty() {
                children.insert(first, TrieBuildNode::Leaf { hash_byte, payload });
            } else {
                let child = children
                    .entry(first)
                    .or_insert_with(|| TrieBuildNode::Internal {
                        children: BTreeMap::new(),
                    });
                insert(child, rest, hash_byte, payload);
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

/// Serialize the trie into bytes with a legacy 8-byte big-endian root-offset
/// footer (NOT the canonical Cassandra `PartitionIndex` `[firstPos|keyCount|root]`
/// footer that [`PartitionsTrieWriter::finish`] now emits).
///
/// Performs a post-order traversal so each child is fully written (and its
/// absolute offset known) before its parent. Child pointers are encoded as
/// backward deltas `parent_pos − child_pos`, matching the reader. Retained as a
/// trie-node-encoding helper for the unit tests that walk a node tree via the
/// footer-based reader; production `Partitions.db` is written by `finish`.
#[cfg(test)]
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
        TrieBuildNode::Leaf { hash_byte, payload } => write_leaf(*hash_byte, *payload, buf),
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
/// where `payloadBits = FLAG_HAS_HASH_BYTE + (position_bytes − 1)` and the
/// signed `position` encodes the payload target per the `PartitionIndex` sign
/// convention:
/// - [`PartitionPayload::DataOffset`] ⇒ `position = ~data_offset` (negative).
/// - [`PartitionPayload::RowsOffset`] ⇒ `position = rows_offset` (non-negative;
///   a `Rows.db` `TrieIndexEntry` offset, issue #910).
fn write_leaf(hash_byte: u8, payload: PartitionPayload, buf: &mut Vec<u8>) -> Result<usize> {
    let position: i64 = match payload {
        // Negative `position` ⇒ direct Data.db offset.
        PartitionPayload::DataOffset(data_offset) => {
            if data_offset > i64::MAX as u64 {
                return Err(Error::InvalidInput(format!(
                    "Data.db offset {data_offset} too large to encode as a signed BTI position"
                )));
            }
            !(data_offset as i64)
        }
        // Non-negative `position` ⇒ Rows.db TrieIndexEntry offset.
        PartitionPayload::RowsOffset(rows_offset) => {
            if rows_offset > i64::MAX as u64 {
                return Err(Error::InvalidInput(format!(
                    "Rows.db offset {rows_offset} too large to encode as a signed BTI position"
                )));
            }
            rows_offset as i64
        }
    };
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

// ===========================================================================
// Rows.db within-partition row-index trie writer (issue #910)
// ===========================================================================
//
// `Rows.db` is a concatenation of independent per-partition structures.  For
// each WIDE partition (one that spans >= 2 column-index blocks, mirroring
// `RowIndexEntry.create()` / `IndexWriter::add_partition_with_promoted`) we
// write, in order:
//
//   1. The row-index trie body (children before parents, backward deltas),
//      whose leaves are `PayloadOnly` nodes carrying a `RowIndexReader.IndexInfo`
//      payload: `[SizedInts(block_offset)] [optional DeletionTime]`.  The low
//      nibble (payloadBits) is `offset_bytes | FLAG_OPEN_MARKER`.  This is the
//      EXACT format `parser::decode_bti_row_payload` consumes.
//   2. The partition's `TrieIndexEntry` at offset `RowsOffset`:
//        [u16 key_length][key bytes]
//        [data position : unsigned vint]
//        [trieRoot - base : SIGNED vint]   (base = RowsOffset + key_length)
//        [block count : unsigned vint]
//        [partition DeletionTime]          (0x80 LIVE sentinel, else 12 bytes)
//      consumed by `parser::resolve_rows_db_entry`.
//
// The returned `RowsOffset` for each partition is the offset of its
// `TrieIndexEntry` (step 2), which is stored as the POSITIVE position in that
// partition's `Partitions.db` leaf payload.
//
// References: cassandra-5.0.0 `RowIndexWriter.java`, `TrieIndexEntry.java`,
// `RowIndexReader.java`; docs/sstables-definitive-guide chapter 17.

/// One row-index block separator for a wide partition's `Rows.db` trie.
///
/// Mirrors the read-side `BtiRowIndexEntry` plus its byte-comparable separator
/// key.  `separator_key` is the OSS50 byte-comparable clustering prefix the
/// reader reconstructs during DFS (e.g. `ck=8 → 80 00 00 08` for an `int`
/// clustering); `block_offset` is the block's offset RELATIVE to the partition
/// start in `Data.db`.
#[derive(Debug, Clone)]
pub struct RowIndexBlock {
    /// OSS50 byte-comparable separator key for this block (ascending order).
    pub separator_key: Vec<u8>,
    /// Block offset relative to the partition's `Data.db` start.
    pub block_offset: u64,
    /// Optional open-deletion `(local_deletion_time, marked_for_delete_at)` for
    /// a range-tombstone that spans this block boundary; `None` for the common
    /// no-open-marker case.
    pub open_marker: Option<(i32, i64)>,
}

/// One wide partition's row index, queued for serialization into `Rows.db`.
#[derive(Debug, Clone)]
struct PartitionRowIndex {
    /// Raw partition-key bytes (length-prefixed into the `TrieIndexEntry`).
    partition_key: Vec<u8>,
    /// Absolute `Data.db` byte position of the partition start.
    data_position: u64,
    /// Per-block separators (ascending, de-duplicated).
    blocks: Vec<RowIndexBlock>,
    /// Partition-level deletion `(local_deletion_time, marked_for_delete_at)`;
    /// `None` ⇒ the LIVE `0x80` sentinel is written.
    partition_deletion: Option<(i32, i64)>,
}

/// Builder that accumulates wide-partition row indexes and serializes `Rows.db`.
///
/// Narrow partitions are NOT added here; they keep a direct
/// [`PartitionPayload::DataOffset`] in `Partitions.db`.  Call
/// [`RowsTrieWriter::add_partition_row_index`] for each wide partition (in the
/// same ascending token order partitions are written), then
/// [`RowsTrieWriter::finish`] to obtain the `Rows.db` bytes plus each
/// partition's `RowsOffset` (the `TrieIndexEntry` position to store in the
/// partition leaf).
#[derive(Debug, Default)]
pub struct RowsTrieWriter {
    partitions: Vec<PartitionRowIndex>,
}

impl RowsTrieWriter {
    /// Create an empty `Rows.db` writer.
    pub fn new() -> Self {
        Self {
            partitions: Vec::new(),
        }
    }

    /// Number of wide-partition row indexes accumulated.
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    /// Whether any wide-partition row indexes have been accumulated.
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Queue a wide partition's row index.
    ///
    /// `partition_key` is the raw on-disk partition-key bytes; `data_position`
    /// is the partition's absolute `Data.db` start offset; `blocks` are the
    /// per-block separators (ascending); `partition_deletion` is the
    /// partition-level deletion (or `None` for LIVE).
    ///
    /// The caller must supply at least one block (a wide partition always has
    /// >= 2 column-index blocks); zero blocks is rejected at [`Self::finish`].
    pub fn add_partition_row_index(
        &mut self,
        partition_key: &[u8],
        data_position: u64,
        blocks: Vec<RowIndexBlock>,
        partition_deletion: Option<(i32, i64)>,
    ) {
        self.partitions.push(PartitionRowIndex {
            partition_key: partition_key.to_vec(),
            data_position,
            blocks,
            partition_deletion,
        });
    }

    /// Serialize all accumulated row indexes into the `Rows.db` bytes.
    ///
    /// Returns `(bytes, rows_offsets)` where `rows_offsets[i]` is the
    /// `TrieIndexEntry` offset for the i-th partition added (the POSITIVE
    /// position to store in that partition's `Partitions.db` leaf).
    ///
    /// An empty writer returns `(empty bytes, empty offsets)` — Cassandra emits
    /// a 0-byte `Rows.db` component when no partition is wide (verified against
    /// the real `simple_table`/`collection_table`/`ttl_table` `da-2-bti-Rows.db`
    /// fixtures), and the reader's `iterate_rows_in_bti_file` accepts it.
    pub fn finish(self) -> Result<(Vec<u8>, Vec<u64>)> {
        let mut buf = Vec::new();
        let mut rows_offsets = Vec::with_capacity(self.partitions.len());

        for part in &self.partitions {
            if part.blocks.is_empty() {
                return Err(Error::InvalidInput(
                    "Rows.db: wide partition must have at least one row-index block".to_string(),
                ));
            }
            // Validate ascending, unique separator keys (a trie cannot encode a
            // key that is a prefix of another, and DFS expects strict order).
            for w in part.blocks.windows(2) {
                if w[0].separator_key >= w[1].separator_key {
                    return Err(Error::InvalidInput(
                        "Rows.db: row-index separators must be strictly ascending".to_string(),
                    ));
                }
            }

            // 1. Serialize the row-index trie body. Its root offset is recorded
            //    for the SIGNED root delta in the TrieIndexEntry.
            let root = build_row_trie(&part.blocks);
            let trie_root = write_row_node(&root, &mut buf)?;

            // 2. Serialize this partition's TrieIndexEntry. `RowsOffset` is the
            //    offset of the key-length prefix (the entry start).
            let rows_offset = buf.len();
            write_trie_index_entry(
                &mut buf,
                &part.partition_key,
                part.data_position,
                trie_root,
                part.blocks.len() as u64,
                part.partition_deletion,
            )?;

            rows_offsets.push(rows_offset as u64);
        }

        Ok((buf, rows_offsets))
    }
}

/// In-memory row-index trie node prior to serialization.
enum RowTrieBuildNode {
    /// Leaf: a single block's `IndexInfo` payload.
    Leaf {
        block_offset: u64,
        open_marker: Option<(i32, i64)>,
    },
    /// Internal node keyed by the next byte of each child's separator.
    Internal {
        children: BTreeMap<u8, RowTrieBuildNode>,
    },
}

/// Build the radix-1 row-index trie from ascending block separators.
fn build_row_trie(blocks: &[RowIndexBlock]) -> RowTrieBuildNode {
    let mut root = RowTrieBuildNode::Internal {
        children: BTreeMap::new(),
    };
    for b in blocks {
        insert_row(&mut root, &b.separator_key, b.block_offset, b.open_marker);
    }
    root
}

fn insert_row(
    node: &mut RowTrieBuildNode,
    key: &[u8],
    block_offset: u64,
    open_marker: Option<(i32, i64)>,
) {
    match node {
        RowTrieBuildNode::Internal { children } => {
            if key.is_empty() {
                // A zero-length separator collides with the trie root; the
                // shortest real separator is at least one byte. Defensive: place
                // the leaf under byte 0 (unreachable for valid OSS50 keys, which
                // are weakly prefix-free and non-empty).
                children.entry(0).or_insert(RowTrieBuildNode::Leaf {
                    block_offset,
                    open_marker,
                });
                return;
            }
            let first = key[0];
            let rest = &key[1..];
            if rest.is_empty() {
                children.insert(
                    first,
                    RowTrieBuildNode::Leaf {
                        block_offset,
                        open_marker,
                    },
                );
            } else {
                let child = children
                    .entry(first)
                    .or_insert_with(|| RowTrieBuildNode::Internal {
                        children: BTreeMap::new(),
                    });
                insert_row(child, rest, block_offset, open_marker);
            }
        }
        RowTrieBuildNode::Leaf { .. } => {
            // Unreachable for unique separators (validated in `finish`).
        }
    }
}

/// Write one row-index trie node (and its subtree), returning the absolute
/// offset of its header byte. Internal nodes reuse the shared
/// `write_sparse`/`write_dense` serializers (they are leaf-type agnostic).
fn write_row_node(node: &RowTrieBuildNode, buf: &mut Vec<u8>) -> Result<usize> {
    match node {
        RowTrieBuildNode::Leaf {
            block_offset,
            open_marker,
        } => write_row_leaf(*block_offset, *open_marker, buf),
        RowTrieBuildNode::Internal { children } => {
            let mut child_offsets: Vec<(u8, usize)> = Vec::with_capacity(children.len());
            for (&byte, child) in children.iter() {
                let off = write_row_node(child, buf)?;
                child_offsets.push((byte, off));
            }
            if child_offsets.len() == 256 {
                write_dense(&child_offsets, buf)
            } else {
                write_sparse(&child_offsets, buf)
            }
        }
    }
}

/// Write a `PayloadOnly` (ordinal 0) row-index leaf node carrying a
/// `RowIndexReader.IndexInfo` payload.
///
/// Layout: `[header=(0<<4)|payloadBits] ++ SizedInts(block_offset) ++ [DeletionTime?]`
/// where `payloadBits = SizedInts.nonZeroSize(block_offset) | (FLAG_OPEN_MARKER
/// if open_marker)`.  This is exactly what `decode_bti_row_payload` reads:
/// `offset_bytes = payloadBits & !FLAG_OPEN_MARKER` (must be 1..=7), and an open
/// `DeletionTime` follows when `FLAG_OPEN_MARKER` is set.
fn write_row_leaf(
    block_offset: u64,
    open_marker: Option<(i32, i64)>,
    buf: &mut Vec<u8>,
) -> Result<usize> {
    if block_offset > i64::MAX as u64 {
        return Err(Error::InvalidInput(format!(
            "Rows.db block offset {block_offset} too large to encode as SizedInts"
        )));
    }
    // Block offsets are non-negative; size them as an unsigned magnitude so the
    // high bit (which SizedInts.read sign-extends) is never set for a value that
    // fits in (bytes*8 - 1) bits. `sized_ints_non_zero_size` already reserves the
    // sign bit, so a non-negative value never round-trips to a negative.
    let offset_bytes = sized_ints_non_zero_size(block_offset as i64);
    // The reader rejects offset_bytes == 0 or > 7 (RowIndexWriter asserts < 8).
    if !(1..=7).contains(&offset_bytes) {
        return Err(Error::InvalidInput(format!(
            "Rows.db block offset {block_offset} needs {offset_bytes} SizedInts bytes; \
             expected 1..=7"
        )));
    }

    let mut payload_bits = offset_bytes as u8;
    if open_marker.is_some() {
        payload_bits |= crate::storage::sstable::bti::parser::FLAG_OPEN_MARKER;
    }

    let offset = buf.len();
    // PayloadOnly ordinal 0: high nibble 0, low nibble = payloadBits.
    buf.push(payload_bits & 0x0F);
    write_sized_int_be(buf, block_offset as i64, offset_bytes);
    if let Some((ldt, mfda)) = open_marker {
        write_da_deletion_time(buf, Some((ldt, mfda)));
    }
    Ok(offset)
}

/// Serialize a per-partition `TrieIndexEntry` (Cassandra `TrieIndexEntry.serialize`).
///
/// Layout (consumed by `parser::resolve_rows_db_entry`):
/// ```text
/// [u16 key_length][partition key bytes]
/// [data position : unsigned vint]
/// [trieRoot - base : SIGNED vint]      (base = entry_start + key_length)
/// [block count : unsigned vint]
/// [partition DeletionTime]
/// ```
/// `entry_start` is the current `buf.len()` (the `RowsOffset` of this entry).
fn write_trie_index_entry(
    buf: &mut Vec<u8>,
    partition_key: &[u8],
    data_position: u64,
    trie_root: usize,
    block_count: u64,
    partition_deletion: Option<(i32, i64)>,
) -> Result<()> {
    let entry_start = buf.len();
    let key_length = partition_key.len();
    let key_length_u16 = u16::try_from(key_length).map_err(|_| {
        Error::InvalidInput(format!(
            "Rows.db TrieIndexEntry: partition key length {key_length} exceeds u16"
        ))
    })?;

    // [u16 key_length][key bytes]
    buf.extend_from_slice(&key_length_u16.to_be_bytes());
    buf.extend_from_slice(partition_key);

    // [data position : unsigned vint]
    write_unsigned_vint(buf, data_position);

    // [trieRoot - base : SIGNED vint]. base = RowsOffset + key_length = the
    // position immediately after the length-prefixed key (entry_start + 2 +
    // key_length) MINUS 2; `resolve_rows_db_entry` computes
    // base = rows_offset + key_length, so root_delta = trie_root - base.
    let base = entry_start + key_length;
    let root_delta = trie_root as i64 - base as i64;
    write_signed_vint(buf, root_delta);

    // [block count : unsigned vint]
    write_unsigned_vint(buf, block_count);

    // [partition DeletionTime]
    write_da_deletion_time(buf, partition_deletion);

    Ok(())
}

/// Write an unsigned VInt in Cassandra's count-leading-ones encoding
/// (`DataOutputPlus.writeUnsignedVInt`); the inverse of
/// `parser::read_unsigned_vint_from_slice`.
fn write_unsigned_vint(buf: &mut Vec<u8>, value: u64) {
    // extra_bytes = number of bytes after the first; determined by magnitude.
    // The first byte holds `extra_bytes` leading 1-bits, then a 0 separator,
    // then the top data bits; remaining bytes are big-endian.
    let extra_bytes = if value == 0 {
        0
    } else {
        let significant_bits = 64 - value.leading_zeros() as usize;
        // Each extra byte carries 8 data bits; the first byte carries
        // (7 - extra_bytes) data bits. Find the smallest extra_bytes such that
        // (7 - extra_bytes) + 8*extra_bytes >= significant_bits.
        let mut n = 0usize;
        while n < 8 && (7 - n) + 8 * n < significant_bits {
            n += 1;
        }
        n
    };

    if extra_bytes == 0 {
        buf.push(value as u8);
        return;
    }
    if extra_bytes >= 8 {
        // 8 extra bytes: first byte is all ones, value spans the full 8 bytes.
        buf.push(0xFF);
        buf.extend_from_slice(&value.to_be_bytes());
        return;
    }

    let data_bits_first = 7 - extra_bytes;
    // Leading 1-bits mask (extra_bytes ones) in the high bits of the first byte.
    let leading_ones: u8 = (!0u8) << (8 - extra_bytes);
    let total_bytes = extra_bytes + 1;
    let mut bytes = value.to_be_bytes().to_vec();
    // Keep only the low `total_bytes` of the big-endian representation.
    let tail = bytes.split_off(8 - total_bytes);
    // tail[0] holds the most-significant data byte; its low `data_bits_first`
    // bits go into the first output byte, OR'd with the leading-ones prefix.
    let first = leading_ones | (tail[0] & ((1u8 << data_bits_first) - 1));
    buf.push(first);
    buf.extend_from_slice(&tail[1..]);
}

/// Write a signed VInt (Cassandra ZigZag) — the inverse of
/// `parser::read_signed_vint_from_slice`.
fn write_signed_vint(buf: &mut Vec<u8>, value: i64) {
    // ZigZag encode: (n << 1) ^ (n >> 63)
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    write_unsigned_vint(buf, zigzag);
}

/// Write a modern (DA/BTI) `DeletionTime` — the inverse of
/// `parser::decode_da_deletion_time`.
///
/// `None` ⇒ the single `0x80` LIVE sentinel. `Some((ldt, mfda))` ⇒ the 12-byte
/// body `[markedForDeleteAt : i64 BE][localDeletionTime : u32 BE]` (note the
/// modern field order/width).
fn write_da_deletion_time(buf: &mut Vec<u8>, deletion: Option<(i32, i64)>) {
    match deletion {
        None => buf.push(0x80),
        Some((local_deletion_time, marked_for_delete_at)) => {
            buf.extend_from_slice(&marked_for_delete_at.to_be_bytes());
            buf.extend_from_slice(&(local_deletion_time as u32).to_be_bytes());
        }
    }
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

    /// Finding 1 (roborev #908): the partition leaf hash byte must be the
    /// **canonical** Cassandra value — `(byte) DecoratedKey.filterHashLowerBits()`,
    /// i.e. the low 8 bits of `h2` (the second 64-bit Murmur3 word) — NOT the
    /// byte-comparable token's high byte that the phase-1 placeholder emitted.
    ///
    /// These expected bytes are read directly from the real, Cassandra-produced
    /// `da-2-bti-Partitions.db` fixture at
    /// `test-data/datasets/sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11`.
    /// Its three PayloadOnly leaves store:
    ///   UUID 2222… → hash byte 0x24 (Data.db offset 0)
    ///   UUID 1111… → hash byte 0x22 (Data.db offset 63)
    ///   UUID 3333… → hash byte 0xf4 (Data.db offset 125)
    /// (hexdump: `08 24 ff | 08 22 c0 | 08 f4 82 …`).
    #[test]
    fn canonical_filter_hash_byte_matches_real_bti_fixture() {
        // (raw partition key bytes, expected canonical hash byte from the fixture)
        let vectors: [(Vec<u8>, u8); 3] = [
            (vec![0x22u8; 16], 0x24),
            (vec![0x11u8; 16], 0x22),
            (vec![0x33u8; 16], 0xf4),
        ];
        for (raw_key, expected) in vectors {
            let got = filter_hash_byte(&raw_key);
            assert_eq!(
                got, expected,
                "canonical hash byte mismatch for key {raw_key:02x?}: \
                 expected 0x{expected:02x} (from real da-2-bti-Partitions.db), got 0x{got:02x}"
            );
        }

        // And confirm the placeholder it replaced (token/h1 high byte) would have
        // produced *different*, non-canonical values — guarding against a regression
        // that reintroduces the token-derived byte.
        for (raw_key, expected) in [(vec![0x22u8; 16], 0x90u8), (vec![0x11u8; 16], 0xbc)] {
            let token = crate::util::cassandra_murmur3::cassandra_murmur3_token(&raw_key);
            let placeholder = (((token as u64) ^ 0x8000_0000_0000_0000u64) >> 56) as u8;
            assert_eq!(placeholder, expected, "placeholder reference value drifted");
            assert_ne!(
                placeholder,
                filter_hash_byte(&raw_key),
                "canonical hash byte must differ from the old token-derived placeholder"
            );
        }
    }

    /// The canonical hash byte the writer emits round-trips: building a trie that
    /// includes these partitions yields leaf payloads whose first byte equals the
    /// canonical hash byte (decoded straight from the serialized bytes).
    #[test]
    fn written_leaf_hash_byte_is_canonical() {
        let raw_key = vec![0x22u8; 16];
        let mut w = PartitionsTrieWriter::new();
        w.add_partition(&raw_key, 0);
        let bytes = w.finish().expect("finish trie");
        // The first written node is the only leaf (PayloadOnly). Its layout is
        // [header=0x08][hash_byte][SizedInts position…]; header 0x08 = payloadBits 8.
        assert_eq!(
            bytes[0], 0x08,
            "expected PayloadOnly leaf with payloadBits=8"
        );
        assert_eq!(
            bytes[1],
            filter_hash_byte(&raw_key),
            "serialized leaf hash byte must be the canonical value"
        );
        assert_eq!(bytes[1], 0x24, "canonical hash byte for UUID 2222… is 0x24");
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

    /// Issue #1678: interior partition raw keys must NOT be retained. Only the two
    /// boundary raw keys (min/max trie key) are kept for the first/last-key region.
    /// On `main` (per-entry `raw_key`) the accessor reported `N × keylen`; here it
    /// is bounded by first+last only.
    #[test]
    fn interior_raw_keys_are_not_retained() {
        let mut w = PartitionsTrieWriter::new();
        for i in 0u64..1000 {
            let mut k = vec![0u8; 16];
            k[0..8].copy_from_slice(&i.to_be_bytes());
            w.add_partition(&k, i);
        }
        // Only the first and last (16 bytes each) may be retained.
        assert!(
            w.retained_raw_key_bytes() <= 32,
            "retained {} raw-key bytes; expected <= 32 (first+last only)",
            w.retained_raw_key_bytes()
        );
    }

    /// Issue #1678: the boundary raw keys tracked incrementally by min/max trie
    /// key must equal what `entries.first()/last()` would yield after the sort, so
    /// `finish` emits an unchanged first/last-key region. Partitions are added in a
    /// deliberately scrambled order (not token order) to prove first/last follow
    /// the trie-key sort, not insertion order.
    #[test]
    fn boundary_raw_keys_match_sorted_first_last() {
        let raws: Vec<Vec<u8>> = [0x33u8, 0x11, 0x88, 0x22, 0x55, 0x44]
            .iter()
            .map(|b| vec![*b; 16])
            .collect();
        let mut w = PartitionsTrieWriter::new();
        for (i, r) in raws.iter().enumerate() {
            w.add_partition(r, i as u64);
        }

        // Oracle: sort the same keys by trie key and take first/last raw bytes.
        let mut sorted: Vec<(_, Vec<u8>)> = raws
            .iter()
            .map(|r| (encode_partition_key_for_bti_trie(r), r.clone()))
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let expect_first = sorted.first().map(|(_, r)| r.clone()).unwrap();
        let expect_last = sorted.last().map(|(_, r)| r.clone()).unwrap();

        assert_eq!(
            w.first_raw.as_ref().map(|(_, k)| k.clone()),
            Some(expect_first),
            "first boundary raw key must be the min-trie-key partition"
        );
        assert_eq!(
            w.last_raw.as_ref().map(|(_, k)| k.clone()),
            Some(expect_last),
            "last boundary raw key must be the max-trie-key partition"
        );
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
                    payload: PartitionPayload::DataOffset((b as u64) * 17),
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

    // ── Rows.db writer (#910) ────────────────────────────────────────────

    /// The unsigned-VInt writer must be the exact inverse of the reader's
    /// `read_unsigned_vint_from_slice` for a wide spread of values, including
    /// the 1-/2-/.../9-byte boundaries.
    #[test]
    fn unsigned_vint_roundtrips_through_reader() {
        use crate::storage::sstable::bti::parser::read_unsigned_vint_from_slice_for_test as read_u;
        let values: [u64; 20] = [
            0,
            1,
            63,
            64,
            127,
            128,
            255,
            256,
            16_383,
            16_384,
            65_535,
            65_536,
            1_000_000,
            300_000_000_000,
            (1u64 << 35) - 1,
            1u64 << 35,
            (1u64 << 49) - 1,
            1u64 << 49,
            u64::MAX - 1,
            u64::MAX,
        ];
        for v in values {
            let mut buf = Vec::new();
            write_unsigned_vint(&mut buf, v);
            let (got, n) = read_u(&buf).expect("read");
            assert_eq!(got, v, "unsigned vint roundtrip failed for {v}: {buf:02x?}");
            assert_eq!(n, buf.len(), "consumed all bytes for {v}");
        }
    }

    /// The signed-VInt (ZigZag) writer must be the exact inverse of the reader's
    /// `read_signed_vint_from_slice` for positive, negative and zero deltas.
    #[test]
    fn signed_vint_roundtrips_through_reader() {
        use crate::storage::sstable::bti::parser::read_signed_vint_from_slice_for_test as read_s;
        for v in [
            0i64,
            1,
            -1,
            10,
            -10,
            127,
            -128,
            1000,
            -1000,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ] {
            let mut buf = Vec::new();
            write_signed_vint(&mut buf, v);
            let (got, _n) = read_s(&buf).expect("read");
            assert_eq!(got, v, "signed vint roundtrip failed for {v}: {buf:02x?}");
        }
    }

    /// A wide partition's Rows.db trie must round-trip through the production
    /// reader: `resolve_rows_db_entry` recovers the trie root + metadata, and
    /// `iterate_rows_in_bti_trie` yields the exact separators + block offsets we
    /// wrote, in ascending order.
    #[test]
    fn rows_db_single_wide_partition_roundtrips() {
        use crate::storage::sstable::bti::{iterate_rows_in_bti_trie, resolve_rows_db_entry};

        // int clustering separators ck = 8,16,24 → OSS50 sign-flipped 4 bytes.
        let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
        let blocks = vec![
            RowIndexBlock {
                separator_key: sep(8),
                block_offset: 16_512,
                open_marker: None,
            },
            RowIndexBlock {
                separator_key: sep(16),
                block_offset: 33_024,
                open_marker: None,
            },
            RowIndexBlock {
                separator_key: sep(24),
                block_offset: 49_536,
                open_marker: None,
            },
        ];

        let raw_pk = 1i32.to_be_bytes().to_vec();
        let mut w = RowsTrieWriter::new();
        w.add_partition_row_index(&raw_pk, 0, blocks.clone(), None);
        let (rows_db, offsets) = w.finish().expect("finish Rows.db");
        assert_eq!(offsets.len(), 1);
        let rows_offset = offsets[0] as usize;

        // Feeding RowsOffset directly as a trie root must FAIL (it is the entry).
        assert!(
            iterate_rows_in_bti_trie(&rows_db, rows_offset).is_err(),
            "RowsOffset is a TrieIndexEntry, not a trie root"
        );

        // resolve_rows_db_entry recovers the header.
        let header = resolve_rows_db_entry(&rows_db, rows_offset).expect("resolve entry");
        assert_eq!(header.data_position, 0);
        assert_eq!(header.block_count, blocks.len() as u32);
        assert_eq!(header.partition_deletion, None, "LIVE sentinel → None");

        // Traversal from the recovered root yields our separators + offsets.
        let entries =
            iterate_rows_in_bti_trie(&rows_db, header.trie_root).expect("traverse from root");
        assert_eq!(entries.len(), blocks.len());
        for (got, expected) in entries.iter().zip(blocks.iter()) {
            assert_eq!(got.0, expected.separator_key, "separator key");
            assert_eq!(got.1.data_offset, expected.block_offset, "block offset");
            assert_eq!(got.1.open_marker, None);
        }
    }

    /// Multiple wide partitions concatenate in Rows.db; each RowsOffset resolves
    /// to its OWN trie root and metadata (no cross-talk between partitions).
    #[test]
    fn rows_db_multiple_wide_partitions_roundtrip() {
        use crate::storage::sstable::bti::{iterate_rows_in_bti_trie, resolve_rows_db_entry};

        let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
        let mk = |base: u64| {
            vec![
                RowIndexBlock {
                    separator_key: sep(8),
                    block_offset: base + 16_512,
                    open_marker: None,
                },
                RowIndexBlock {
                    separator_key: sep(16),
                    block_offset: base + 33_024,
                    open_marker: None,
                },
            ]
        };

        let mut w = RowsTrieWriter::new();
        w.add_partition_row_index(&1i32.to_be_bytes(), 0, mk(0), None);
        w.add_partition_row_index(&2i32.to_be_bytes(), 700_000, mk(0), None);
        w.add_partition_row_index(&3i32.to_be_bytes(), 1_400_000, mk(0), None);
        let (rows_db, offsets) = w.finish().expect("finish");
        assert_eq!(offsets.len(), 3);

        let data_positions = [0u64, 700_000, 1_400_000];
        for (i, &ro) in offsets.iter().enumerate() {
            let header = resolve_rows_db_entry(&rows_db, ro as usize).expect("resolve");
            assert_eq!(
                header.data_position, data_positions[i],
                "partition {i} data position"
            );
            assert_eq!(header.block_count, 2);
            let entries = iterate_rows_in_bti_trie(&rows_db, header.trie_root).expect("traverse");
            assert_eq!(entries.len(), 2, "partition {i} block count");
            assert_eq!(entries[0].0, sep(8));
            assert_eq!(entries[1].0, sep(16));
        }
    }

    /// An empty RowsTrieWriter yields 0-byte Rows.db with no offsets — exactly
    /// what Cassandra emits for a narrow-only BTI SSTable (verified against the
    /// real `simple_table`/`collection_table`/`ttl_table` 0-byte `da-2-bti-Rows.db`
    /// fixtures), and the reader accepts it.
    #[test]
    fn rows_db_empty_writer_is_zero_bytes() {
        let w = RowsTrieWriter::new();
        let (bytes, offsets) = w.finish().expect("finish empty");
        assert!(bytes.is_empty(), "empty Rows.db must be 0 bytes");
        assert!(offsets.is_empty());
    }

    /// A wide partition with an open-marker block round-trips the DeletionTime.
    #[test]
    fn rows_db_open_marker_roundtrips() {
        use crate::storage::sstable::bti::{iterate_rows_in_bti_trie, resolve_rows_db_entry};
        let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
        let blocks = vec![
            RowIndexBlock {
                separator_key: sep(8),
                block_offset: 16_512,
                open_marker: Some((1_700_000_000, 1_700_000_000_000_000)),
            },
            RowIndexBlock {
                separator_key: sep(16),
                block_offset: 33_024,
                open_marker: None,
            },
        ];
        let mut w = RowsTrieWriter::new();
        w.add_partition_row_index(&7i32.to_be_bytes(), 0, blocks.clone(), None);
        let (rows_db, offsets) = w.finish().expect("finish");
        let header = resolve_rows_db_entry(&rows_db, offsets[0] as usize).expect("resolve");
        let entries = iterate_rows_in_bti_trie(&rows_db, header.trie_root).expect("traverse");
        assert_eq!(
            entries[0].1.open_marker,
            Some((1_700_000_000, 1_700_000_000_000_000))
        );
        assert_eq!(entries[1].1.open_marker, None);
    }

    /// Partition-level deletion (non-LIVE) round-trips through the TrieIndexEntry.
    #[test]
    fn rows_db_partition_deletion_roundtrips() {
        use crate::storage::sstable::bti::resolve_rows_db_entry;
        let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
        let blocks = vec![
            RowIndexBlock {
                separator_key: sep(8),
                block_offset: 16_512,
                open_marker: None,
            },
            RowIndexBlock {
                separator_key: sep(16),
                block_offset: 33_024,
                open_marker: None,
            },
        ];
        let mut w = RowsTrieWriter::new();
        w.add_partition_row_index(&9i32.to_be_bytes(), 0, blocks, Some((1234, 5678)));
        let (rows_db, offsets) = w.finish().expect("finish");
        let header = resolve_rows_db_entry(&rows_db, offsets[0] as usize).expect("resolve");
        assert_eq!(header.partition_deletion, Some((1234, 5678)));
    }

    /// A non-ascending separator set is rejected (the trie cannot encode it and
    /// DFS expects strict order).
    #[test]
    fn rows_db_rejects_non_ascending_separators() {
        let sep = |ck: i32| ((ck as u32) ^ 0x8000_0000).to_be_bytes().to_vec();
        let blocks = vec![
            RowIndexBlock {
                separator_key: sep(16),
                block_offset: 16_512,
                open_marker: None,
            },
            RowIndexBlock {
                separator_key: sep(8),
                block_offset: 33_024,
                open_marker: None,
            },
        ];
        let mut w = RowsTrieWriter::new();
        w.add_partition_row_index(&1i32.to_be_bytes(), 0, blocks, None);
        assert!(w.finish().is_err());
    }

    /// A partition leaf with a positive RowsOffset payload decodes back to the
    /// SAME RowsOffset via the reader (`BtiPartitionLocation::RowsOffset`).
    #[test]
    fn partition_leaf_rows_offset_roundtrips() {
        let raw_key = vec![0x11u8; 16];
        let mut w = PartitionsTrieWriter::new();
        w.add_partition_with_payload(&raw_key, PartitionPayload::RowsOffset(242));
        let bytes = w.finish().expect("finish");

        let mut cur = Cursor::new(bytes);
        let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &raw_key)
            .expect("lookup")
            .expect("found");
        match loc {
            BtiPartitionLocation::RowsOffset(o) => assert_eq!(o, 242),
            BtiPartitionLocation::DataOffset(o) => {
                panic!("expected RowsOffset(242), got DataOffset({o})")
            }
        }
    }
}
