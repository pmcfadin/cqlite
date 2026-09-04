//! Cassandra 5.0 `Partitions.db` trie-payload decoding, traversal, and lookup.
//!
//! Cassandra 5.0 `Partitions.db` has NO header.  The file is a compact trie
//! whose nodes are written bottom-up (children appear at lower offsets than
//! parents), with the root offset stored as the **last 8 bytes** of the file as
//! a big-endian u64.
//!
//! Every leaf node (ordinal 0 = PayloadOnly) carries a *payload* described by
//! the `payloadBits` value encoded in the **low nibble** of the node's header
//! byte.  In Cassandra 5.0 the hash byte is **always present**
//! (`FLAG_HAS_HASH_BYTE = 8`), so `payloadBits >= 8` for every real partition
//! leaf.  The payload layout is:
//!
//!   byte 0            : partition-key filter hash (lowest 8 bits of Murmur3)
//!   bytes 1 .. N      : SizedInts-encoded signed `position`, where
//!                        N = payloadBits − 8 + 1  (= payloadBits − 7)
//!
//! **Sign convention** (mirrors `PartitionIndex.java`):
//!   - If `position < 0` → `data_offset = ~position` (bitwise NOT; integer NOT)
//!   - If `position >= 0` → `position` points into `Rows.db` (wide partition)
//!
//! Reference: `PartitionIndex.java:131–135`, `BtiFormat.md:946–963`,
//!            `SizedInts.java` (local mirror: `sized_ints.rs`).

use crate::{
    error::Error,
    storage::sstable::bti::node::{BtiNode, BtiNodeData, BtiNodeType, BtiResult},
};
use std::io::{Read, Seek, SeekFrom};

use super::node_decode::{classify_node_nibble, parse_bti_node, pointer_bytes_for_ordinal};

/// The `FLAG_HAS_HASH_BYTE` bit in `payloadBits` (low nibble of a BTI leaf
/// node's header byte).  When set, the first payload byte is the partition-key
/// filter hash; the remaining bytes encode the `position` via `SizedInts`.
///
/// In Cassandra 5.0 this bit is **always** set for partition index leaves.
/// Mirrors `PartitionIndex.java:131`.
pub const FLAG_HAS_HASH_BYTE: u8 = 8;

/// Result of decoding a BTI partition leaf payload.
///
/// Mirrors `PartitionIndex.java` payload interpretation: either a direct
/// `Data.db` offset (for narrow partitions) or a `Rows.db` offset (for wide
/// partitions that need row-level indexing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BtiPartitionLocation {
    /// Direct `Data.db` byte offset.  The partition starts here.
    ///
    /// Occurs when the trie leaf's encoded position is **negative**:
    /// `data_offset = ~position` (bitwise NOT).
    DataOffset(u64),

    /// Offset into `Rows.db` for this partition's row-level trie index.
    ///
    /// Occurs when the trie leaf's encoded position is **non-negative**.
    RowsOffset(u64),
}

/// Decode the BTI partition leaf payload at `payload_start` inside `trie_data`.
///
/// # Arguments
/// * `trie_data`      – raw bytes of the trie (i.e. the file **excluding** the
///   8-byte root-offset footer).
/// * `payload_start`  – offset within `trie_data` where the payload begins
///   (immediately after the 1-byte node header).
/// * `payload_bits`   – the `payloadBits` value from the node header's low nibble.
///
/// # Returns
/// `Ok(BtiPartitionLocation)` on success; a parse error if `payload_bits` is
/// out of the expected range or the data is too short.
///
/// # Reference
/// Mirrors `PartitionIndex.java` `readPayload` and Cassandra's `SizedInts.read`.
pub fn decode_bti_partition_payload(
    trie_data: &[u8],
    payload_start: usize,
    payload_bits: u8,
) -> BtiResult<BtiPartitionLocation> {
    // Every real Cassandra 5.0 partition leaf must have the hash byte flag set.
    if payload_bits < FLAG_HAS_HASH_BYTE {
        return Err(Error::Parse(format!(
            "BTI payload_bits {payload_bits} < FLAG_HAS_HASH_BYTE (8); \
             hash-byte-less payloads are not supported in Cassandra 5.0 BTI format"
        )));
    }
    if payload_bits > 16 {
        // SizedInts stores at most 8 bytes; plus 1 hash byte = 9 bytes max → payloadBits ≤ 16
        return Err(Error::Parse(format!(
            "BTI payload_bits {payload_bits} > 16; invalid BTI partition leaf"
        )));
    }

    // N = payloadBits − FLAG_HAS_HASH_BYTE + 1
    // For payloadBits = 8 (most common: 1-byte position): N = 1
    // For payloadBits = 9: N = 2, etc.
    let position_bytes = (payload_bits - FLAG_HAS_HASH_BYTE + 1) as usize;

    // Minimum payload = 1 hash byte + position_bytes
    let needed = 1 + position_bytes;
    if payload_start + needed > trie_data.len() {
        return Err(Error::Parse(format!(
            "BTI partition payload at {payload_start} is too short: \
             need {needed} bytes, have {}",
            trie_data.len().saturating_sub(payload_start)
        )));
    }

    // Skip hash byte (payload_start + 0), read position starting at payload_start + 1
    let pos_data = &trie_data[payload_start + 1..payload_start + 1 + position_bytes];

    // Sign-extend the big-endian bytes into a signed i64 (SizedInts semantics)
    let position: i64 = sized_ints_read_from_slice(pos_data)?;

    // Sign convention: negative → ~position is the Data.db byte offset
    if position < 0 {
        let data_offset = !position as u64; // bitwise NOT of the negative value
        Ok(BtiPartitionLocation::DataOffset(data_offset))
    } else {
        Ok(BtiPartitionLocation::RowsOffset(position as u64))
    }
}

/// Read a signed big-endian SizedInts value from a byte slice.
///
/// Mirrors `SizedInts.read(DataInputPlus, int)` from Cassandra — sign-extends
/// the most-significant byte.  Accepts 1–8 bytes.
///
/// This is the slice-based analogue of `crate::storage::sstable::bti::sized_ints::read`
/// (which operates on a `std::io::Read` stream).
pub(crate) fn sized_ints_read_from_slice(data: &[u8]) -> BtiResult<i64> {
    match data.len() {
        0 => Ok(0),
        1 => Ok(data[0] as i8 as i64),
        2 => Ok(i16::from_be_bytes([data[0], data[1]]) as i64),
        3 => {
            let high = data[0] as i8 as i64;
            let low = u16::from_be_bytes([data[1], data[2]]) as i64;
            Ok((high << 16) | low)
        }
        4 => Ok(i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as i64),
        5 => {
            let high = data[0] as i8 as i64;
            let low = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as i64;
            Ok((high << 32) | low)
        }
        6 => {
            let high = i16::from_be_bytes([data[0], data[1]]) as i64;
            let low = u32::from_be_bytes([data[2], data[3], data[4], data[5]]) as i64;
            Ok((high << 32) | low)
        }
        7 => {
            let high1 = data[0] as i8 as i64;
            let high2 = u16::from_be_bytes([data[1], data[2]]) as i64;
            let low = u32::from_be_bytes([data[3], data[4], data[5], data[6]]) as i64;
            Ok((high1 << 48) | (high2 << 32) | low)
        }
        8 => Ok(i64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ])),
        n => Err(Error::Parse(format!(
            "SizedInts: invalid byte count {n} (expected 1–8)"
        ))),
    }
}

/// Parse a BTI trie node from `trie_data` at `node_offset` for **traversal**
/// purposes (i.e. to follow child pointers).  Unlike [`parse_bti_node`], this
/// function does NOT require any particular payload format: for PayloadOnly and
/// for nodes with a non-zero `payloadBits` low nibble the payload bytes are
/// simply skipped — the caller is responsible for reading the payload via
/// [`read_node_payload`].
///
/// This is important because real Cassandra 5.0 `Partitions.db` uses a
/// compact SizedInts-based payload (2–10 bytes) that is incompatible with the
/// legacy 12-byte `PayloadRef` format expected by `parse_payload_ref`.
pub(crate) fn parse_bti_node_for_traversal(
    trie_data: &[u8],
    node_offset: usize,
) -> BtiResult<BtiNode> {
    if node_offset >= trie_data.len() {
        return Err(Error::Parse(format!(
            "BTI traversal: node_offset {node_offset} >= trie_data.len {}",
            trie_data.len()
        )));
    }

    let data = &trie_data[node_offset..];
    let header_byte = data[0];
    let ordinal = (header_byte >> 4) & 0x0F;
    let payload_flags = header_byte & 0x0F;
    let node_type = classify_node_nibble(ordinal)?;

    match node_type {
        BtiNodeType::PayloadOnly => {
            // PayloadOnly leaf: no children.  Skip payload (we decode it separately).
            if payload_flags == 0 {
                return Err(Error::Parse(
                    "PayloadOnly node has no payload flags set".to_string(),
                ));
            }
            // Use a stub PayloadRef — the caller uses read_node_payload for the real data
            let stub = crate::storage::sstable::bti::node::PayloadRef::new(0, 0);
            Ok(BtiNode {
                node_type,
                level: 0,
                key_prefix: Vec::new(),
                data: BtiNodeData::PayloadOnly { payload: stub },
            })
        }
        BtiNodeType::Single => {
            // Reuse the existing full parser — it handles pointers correctly.
            // Payload (if any) comes after the pointer bytes and is not read here.
            parse_bti_node(data, node_offset as u64)
        }
        BtiNodeType::Sparse => parse_bti_node(data, node_offset as u64),
        BtiNodeType::Dense => parse_bti_node(data, node_offset as u64),
    }
}

/// Walk one BTI trie node at `node_offset` and return the absolute offset of
/// the child reachable via `search_byte`, or `None` if no such transition
/// exists.
///
/// Returns `Ok(Some(child_offset))` when the byte is found and the child is an
/// internal node.  Returns `Ok(None)` when there is no matching transition
/// (key not in trie).
///
/// Separately, if the *current* node (at `node_offset`) has an embedded
/// payload **and** `key_exhausted` is true, the caller should read that
/// payload; this function does not return it — see [`read_node_payload`].
fn find_next_child_offset(
    trie_data: &[u8],
    node_offset: usize,
    search_byte: u8,
) -> BtiResult<Option<usize>> {
    // Issue #1574 (C3): resolve ONLY the child pointer for `search_byte` in place,
    // instead of materializing the node's full child table via
    // `parse_bti_node_for_traversal` just to follow one byte. The in-place decode
    // produces bit-identical child offsets (same `saturating_sub` arithmetic and
    // Dense delta-0 sentinel) and the same structural-error semantics.
    super::slice_walk::find_child_offset(trie_data, node_offset, search_byte)
}

/// Read the `BtiPartitionLocation` from the payload attached to the BTI node at
/// `node_offset` (`None` if it has no payload).  Pass `parsed = Some(&node)` when the
/// caller already parsed the node (the DFS single parse, issue #1650 / L3) so a
/// payload-bearing internal node is not re-parsed; `None` parses it lazily here.
pub(crate) fn read_node_payload(
    trie_data: &[u8],
    node_offset: usize,
    parsed: Option<&BtiNode>,
) -> BtiResult<Option<BtiPartitionLocation>> {
    let node = parsed;
    if node_offset >= trie_data.len() {
        return Err(Error::Parse(format!(
            "BTI payload read: node_offset {node_offset} out of bounds"
        )));
    }
    let header_byte = trie_data[node_offset];
    let ordinal = (header_byte >> 4) & 0x0F;
    let payload_flags = header_byte & 0x0F; // aka payloadBits

    // Ordinals 1 (SingleNoPayload4) and 3 (SingleNoPayload12) encode their
    // backward delta in the low nibble / low 12 bits — that nibble is NOT a
    // payload flag, and these node types can NEVER carry a payload.  Treating
    // their low nibble as `payloadBits` was a latent bug exposed by full DFS
    // (issue #832): e.g. a `0x18` SingleNoPayload4 was misread as having a
    // payload, producing a spurious entry.
    if ordinal == 1 || ordinal == 3 {
        return Ok(None);
    }

    if ordinal == 0 {
        // PayloadOnly node: the entire node is just [header][payload…]
        if payload_flags == 0 {
            return Err(Error::Parse(
                "PayloadOnly node has zero payload_flags".to_string(),
            ));
        }
        let payload_start = node_offset + 1; // immediately after the 1-byte header
        Ok(Some(decode_bti_partition_payload(
            trie_data,
            payload_start,
            payload_flags,
        )?))
    } else if payload_flags != 0 {
        // Non-leaf node with an embedded payload: skip its pointer/transition bytes
        // to reach the payload.  Use the caller's pre-parsed node (DFS single parse,
        // issue #1650) when supplied, else parse here.
        let owned;
        let node = match node {
            Some(n) => n,
            None => {
                owned = parse_bti_node(&trie_data[node_offset..], node_offset as u64)?;
                &owned
            }
        };
        let payload_start = payload_start_in_node(node, trie_data, node_offset)?;
        Ok(Some(decode_bti_partition_payload(
            trie_data,
            payload_start,
            payload_flags,
        )?))
    } else {
        Ok(None)
    }
}

/// Compute the byte offset of the payload within `trie_data` for a non-leaf
/// node that carries an embedded payload (`payloadBits != 0`).
///
/// This is the byte position immediately after all the node's transition bytes
/// and pointer bytes, but before any subsequent node data.
pub(crate) fn payload_start_in_node(
    node: &BtiNode,
    trie_data: &[u8],
    node_offset: usize,
) -> BtiResult<usize> {
    use BtiNodeData::*;
    let header_byte = trie_data[node_offset];
    let ordinal = (header_byte >> 4) & 0x0F;

    let payload_offset = match &node.data {
        PayloadOnly { .. } => {
            // Header(1) + payload: caller handles this case separately
            node_offset + 1
        }
        Single { .. } => {
            // Singles with payload: header(1) + transition(1) + ptr_bytes
            let ptr_bytes = pointer_bytes_for_ordinal(ordinal) as usize;
            // Ordinals 1 and 3 are "NoPayload" variants; they cannot have payloads
            // (the payload_flags nibble is always 0 for those ordinals, so this
            // branch is unreachable for them).
            node_offset + 1 + 1 + ptr_bytes
        }
        Sparse { transitions } => {
            let count = transitions.len();
            let ptr_bytes = pointer_bytes_for_ordinal(ordinal) as usize;
            // Ordinal 6 (Sparse12): packed 12-bit pointers, ceil(count*3/2) bytes
            let ptr_area = if ordinal == 6 {
                (count * 3).div_ceil(2)
            } else {
                count * ptr_bytes
            };
            node_offset + 1 + 1 + count + ptr_area // header + count + transitions + deltas
        }
        Dense { children, .. } => {
            let range_len = children.len();
            let ptr_bytes = pointer_bytes_for_ordinal(ordinal) as usize;
            let ptr_area = if ordinal == 10 {
                // Dense12
                (range_len * 3).div_ceil(2)
            } else {
                range_len * ptr_bytes
            };
            node_offset + 1 + 1 + 1 + ptr_area // header + start_byte + len-1 + deltas
        }
    };
    Ok(payload_offset)
}

/// Walk the BTI partition trie loaded in `trie_data`, looking for a partition
/// whose **byte-comparable** encoded key *prefix* routes to a leaf.
///
/// Cassandra BTI uses a **path-compressed (Patricia) trie**: once a path leads to
/// a `PayloadOnly` leaf, the remaining key bytes are stored implicitly (as the
/// compressed suffix).  Therefore a match is declared as soon as a `PayloadOnly`
/// leaf is reached — regardless of whether the encoded key has been fully consumed.
/// The caller is responsible for verifying the actual partition key against the
/// Data.db bytes at the resolved offset (using the partition's hash byte for a
/// fast pre-filter and the raw key bytes for definitive confirmation).
///
/// Returns:
/// - `Ok(Some(BtiPartitionLocation))` when a leaf is reached.
/// - `Ok(None)` when no transition exists for a key byte (key not in trie).
/// - `Err(_)` on structural parse errors.
///
/// This is the inner engine for [`lookup_partition_in_bti_file`].
pub(crate) fn walk_bti_trie(
    trie_data: &[u8],
    root_offset: usize,
    encoded_key: &[u8],
) -> BtiResult<Option<BtiPartitionLocation>> {
    let mut current_offset = root_offset;
    let mut key_pos = 0;

    loop {
        if current_offset >= trie_data.len() {
            return Err(Error::Parse(format!(
                "BTI trie walk: offset {current_offset} out of bounds (trie size {})",
                trie_data.len()
            )));
        }

        let header_byte = trie_data[current_offset];
        let ordinal = (header_byte >> 4) & 0x0F;
        let payload_flags = header_byte & 0x0F;
        let is_leaf = ordinal == 0; // PayloadOnly

        if is_leaf {
            // Path-compressed (Patricia) trie: a PayloadOnly leaf represents the
            // unique key whose path through the trie led here.  The remaining key
            // bytes are stored implicitly (compressed suffix) and need not be
            // compared here — the caller verifies the actual partition key in Data.db.
            //
            // Note: the original implementation required the key to be fully consumed
            // at the leaf (return Ok(None) otherwise), which is incorrect for
            // path-compressed tries.  Issue #755 corrects this.
            return read_node_payload(trie_data, current_offset, None);
        }

        // Non-leaf node: if key exhausted, check for an embedded payload
        if key_pos >= encoded_key.len() {
            if payload_flags != 0 {
                return read_node_payload(trie_data, current_offset, None);
            }
            return Ok(None);
        }

        // Advance: look for the next key byte's transition
        let next_byte = encoded_key[key_pos];
        match find_next_child_offset(trie_data, current_offset, next_byte)? {
            Some(child_offset) => {
                current_offset = child_offset;
                key_pos += 1;
            }
            None => {
                // No transition for this byte → key not in trie
                return Ok(None);
            }
        }
    }
}

/// Look up a partition by its **byte-comparable** encoded key in a real
/// Cassandra 5.0 `Partitions.db` BTI trie file.
///
/// # Format
/// The file is a compact trie with **no header**:
/// - Nodes are written bottom-up (children before parents).
/// - The root node's absolute byte offset is stored as the **last 8 bytes** of
///   the file (big-endian u64).
/// - Every partition leaf carries a payload consisting of a 1-byte filter hash
///   plus a SizedInts-encoded signed `position` (see [`decode_bti_partition_payload`]).
/// - Negative `position` → `data_offset = ~position` (direct `Data.db` pointer).
/// - Non-negative `position` → `rows_offset` into `Rows.db` (wide partition).
///
/// # Arguments
/// * `reader`      – positioned at any point; this function seeks as needed.
/// * `encoded_key` – the byte-comparable encoding of the partition key (e.g.
///   as produced by [`ByteComparableEncoder`](crate::storage::sstable::bti::encoder::ByteComparableEncoder)).
///
/// # Returns
/// * `Ok(Some(BtiPartitionLocation::DataOffset(off)))` – the partition starts
///   at byte `off` in `Data.db`.  **This is the headline result of issue #755.**
/// * `Ok(Some(BtiPartitionLocation::RowsOffset(off)))` – the partition has a
///   row index; consult `Rows.db` at `off` for the row-level trie.
/// * `Ok(None)` – the key was not found.
/// * `Err(_)` – structural parse error.
///
/// # Reference
/// Mirrors `PartitionIndex.java` (Cassandra 5.0.8), specifically the
/// `openBlocking` + `exactCandidate` read path; `SizedInts.read`; and the
/// payload sign convention at `PartitionIndex.java:131–135`.
pub fn lookup_partition_in_bti_file<R: Read + Seek>(
    reader: &mut R,
    encoded_key: &[u8],
) -> BtiResult<Option<BtiPartitionLocation>> {
    // Step 1: determine the file size
    let file_size = reader.seek(SeekFrom::End(0))?;

    if file_size < 8 {
        return Err(Error::Parse(format!(
            "BTI Partitions.db is too small ({file_size} bytes; need at least 8 for footer)"
        )));
    }

    // Step 2: read the 8-byte root offset from the end of the file
    reader.seek(SeekFrom::End(-8))?;
    let mut footer_buf = [0u8; 8];
    reader.read_exact(&mut footer_buf)?;
    let root_offset = u64::from_be_bytes(footer_buf);

    // The trie data is everything before the 8-byte footer
    let trie_size = file_size - 8;

    if root_offset >= trie_size {
        return Err(Error::Parse(format!(
            "BTI Partitions.db: root_offset {root_offset} >= trie_size {trie_size}"
        )));
    }

    // Step 3: load the entire trie into memory
    // For typical Partitions.db files this is small (tens of KB for millions of
    // partitions because the trie shares prefixes aggressively).
    reader.seek(SeekFrom::Start(0))?;
    let mut trie_data = vec![0u8; trie_size as usize];
    reader.read_exact(&mut trie_data)?;

    // Step 4: walk the trie
    walk_bti_trie(&trie_data, root_offset as usize, encoded_key)
}

// ─────────────────────────────────────────────────────────────────────────────
// BTI partition key encoding for Murmur3Partitioner (issue #755)
// ─────────────────────────────────────────────────────────────────────────────
//
// Cassandra 5.0 BTI `Partitions.db` trie keys are derived from `DecoratedKey`
// using `ByteComparable.Version.OSS50`.  For `Murmur3Partitioner` the encoding
// is a path-compressed representation of:
//
//   [type_prefix=0x40] ++ [8 bytes: murmur3_token_bc]
//
// where `murmur3_token_bc = (murmur3_token(raw_key_bytes) as u64) XOR 0x8000_0000_0000_0000`
// (flips the sign bit so signed i64 tokens sort correctly as unsigned big-endian bytes).
//
// Because the trie uses path compression (Patricia trie), looking up the first
// few bytes of this 9-byte key is sufficient to reach the unique leaf for a
// given partition.  The caller must verify the actual partition key in Data.db
// to confirm the match (the hash byte in the leaf payload provides a fast pre-filter).
//
// This encoding was verified against the real `da-2-bti-Partitions.db` fixture:
//   UUID 22222222-... → token bc first byte 0x90 → DataOffset(0)   ✓
//   UUID 11111111-... → token bc first byte 0xBC → DataOffset(63)  ✓
//   UUID 33333333-... → token bc first byte 0xF9 → DataOffset(125) ✓
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a raw partition key (any CQL type) into the BTI trie lookup key for
/// `Murmur3Partitioner`.
///
/// The encoding is:
///   `[0x40] ++ [8 bytes big-endian: (murmur3_token(raw_key) as u64) XOR 0x8000_0000_0000_0000]`
///
/// This 9-byte prefix uniquely identifies the partition in the trie for typical
/// Cassandra datasets.  In a path-compressed (Patricia) trie, using only the
/// first few bytes is correct because each leaf represents exactly one partition:
/// once the traversal reaches a leaf, no further byte matching is required.
///
/// # Arguments
/// * `raw_key_bytes` – the raw serialized partition key bytes as stored on disk
///   (e.g. 16 bytes for a UUID, big-endian bytes for int/bigint, UTF-8 for text).
///
/// # Verified against
/// Real `da-2-bti-Partitions.db` fixture — see issue #755 for derivation.
pub fn encode_partition_key_for_bti_trie(raw_key_bytes: &[u8]) -> [u8; 9] {
    // C4 work-counter (KEY_HASH_CALLS, issue #1575): one per Murmur3 hash + encoding
    // of a query key. C4 hoists this call out of the candidate-prune loop so a
    // multi-generation point read records exactly 1 (not one per candidate). No-op in
    // release builds (read_work_counters design.md Decision 1).
    crate::storage::sstable::read_work_counters::record_key_hash();
    encode_partition_key_for_bti_trie_uncounted(raw_key_bytes)
}

/// Byte-comparable BTI trie key encoding WITHOUT the C4 `KEY_HASH_CALLS` counter
/// (issue #2058). Identical output to [`encode_partition_key_for_bti_trie`]; used by
/// the next-partition successor walk, which encodes the SAME key a point read already
/// hashed once (via the candidate prune) so it must not inflate `KEY_HASH_CALLS`
/// beyond the one-hash-per-read C4 invariant.
pub(crate) fn encode_partition_key_for_bti_trie_uncounted(raw_key_bytes: &[u8]) -> [u8; 9] {
    use crate::util::cassandra_murmur3::cassandra_murmur3_token;

    let token: i64 = cassandra_murmur3_token(raw_key_bytes);
    encode_bti_trie_key_from_token(token)
}

/// Encode the byte-comparable BTI trie key from a *precomputed* Murmur3
/// partition token — the single authoritative definition of the trie-key byte
/// layout (`[0x40] ++ be8(token ^ 0x8000_0000_0000_0000)`).
///
/// This lets a caller that already holds the 128-bit hash words derive the trie
/// key without re-hashing the raw key (issue #1681).
///
/// **`token` is the key's TOKEN, which is not always `normalize(h1)`.** For a
/// NON-EMPTY key, pass `cassandra_murmur3_normalize_token(h1)` and the output is
/// byte-identical to [`encode_partition_key_for_bti_trie`] over the same raw
/// bytes. For an EMPTY key, `normalize(h1) == 0` but the token is
/// `CASSANDRA_MINIMUM_TOKEN`, so a caller passing `normalize(h1)` there encodes
/// at token 0 and its leaf becomes unreachable by every reader probe, which is
/// exactly the defect fixed in issue #3633. Callers on this single-pass route
/// MUST carry their own `is_empty()` guard; the safe route is
/// `cassandra_murmur3_token`, which already does.
pub fn encode_bti_trie_key_from_token(token: i64) -> [u8; 9] {
    let bc: u64 = (token as u64) ^ 0x8000_0000_0000_0000u64;
    let bc_bytes = bc.to_be_bytes();

    let mut key = [0u8; 9];
    key[0] = 0x40; // fixed type-discriminator prefix (observed in Partitions.db trie)
    key[1..9].copy_from_slice(&bc_bytes);
    key
}

/// Look up a raw partition key in a Cassandra 5.0 BTI `Partitions.db` trie file
/// using the `Murmur3Partitioner` byte-comparable encoding.
///
/// This is a convenience wrapper over [`lookup_partition_in_bti_file`] that
/// handles key encoding:
///
/// 1. Encodes `raw_key_bytes` → 9-byte BTI trie key via
///    [`encode_partition_key_for_bti_trie`].
/// 2. Looks up the key in the trie.
/// 3. Returns the `BtiPartitionLocation` (Data.db or Rows.db offset).
///
/// Because the BTI trie uses path compression the lookup may return a candidate
/// for a *different* key that shares the same trie prefix.  The caller must
/// verify the actual partition key at the returned Data.db offset.
///
/// # Arguments
/// * `partitions_db_reader` – seekable reader for the `Partitions.db` file.
/// * `raw_key_bytes`        – raw on-disk partition key bytes (e.g. 16 bytes for UUID).
///
/// # Returns
/// * `Ok(Some(BtiPartitionLocation::DataOffset(off)))` – candidate Data.db offset.
/// * `Ok(None)` – definitely not in this SSTable (no trie path for this key prefix).
/// * `Err(_)` – structural parse error.
pub fn lookup_raw_key_in_bti_partitions_db<R: Read + Seek>(
    partitions_db_reader: &mut R,
    raw_key_bytes: &[u8],
) -> BtiResult<Option<BtiPartitionLocation>> {
    let encoded = encode_partition_key_for_bti_trie(raw_key_bytes);
    lookup_partition_in_bti_file(partitions_db_reader, &encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // decode_bti_partition_payload unit tests
    // -----------------------------------------------------------------------

    /// Construct a minimal trie_data slice and verify decode_bti_partition_payload
    /// returns the correct DataOffset.  Mirrors the simple_table leaf at offset 0
    /// (hash=0x24, position=−1 → data_offset=0).
    #[test]
    fn decode_bti_partition_payload_data_offset_zero() {
        // PayloadOnly header byte: 0x08 (ordinal=0, payloadBits=8)
        // payload[0] = 0x24 (hash byte)
        // payload[1] = 0xFF (position = i8(-1) = -1 → ~(-1) = 0)
        let header_byte: u8 = 0x08;
        let hash_byte: u8 = 0x24;
        let position_byte: u8 = 0xFF; // -1 as i8
        let trie_data = vec![header_byte, hash_byte, position_byte, 0x00, 0x00];
        let payload_start = 1; // immediately after header
        let payload_bits = header_byte & 0x0F; // = 8
        let result = decode_bti_partition_payload(&trie_data, payload_start, payload_bits)
            .expect("should decode successfully");
        assert_eq!(
            result,
            BtiPartitionLocation::DataOffset(0),
            "position=-1 (0xFF as i8) must map to data_offset=0 via ~(-1)=0"
        );
    }

    #[test]
    fn decode_bti_partition_payload_data_offset_63() {
        // simple_table: leaf at offset 3 (hash=0x22, position=−64 → data_offset=63)
        // payloadBits = 8 → 1 position byte
        // position = 0xC0 as i8 = -64; ~(-64) = 63
        let trie_data = vec![
            0x08u8, // header: ordinal=0, payloadBits=8
            0x22,   // hash byte
            0xC0,   // position = -64 as i8 → data_offset = ~(-64) = 63
        ];
        let payload_bits = 8u8;
        let result = decode_bti_partition_payload(&trie_data, 1, payload_bits).unwrap();
        assert_eq!(result, BtiPartitionLocation::DataOffset(63));
    }

    #[test]
    fn decode_bti_partition_payload_data_offset_125() {
        // simple_table: leaf at offset 6 (hash=0xF4, position=−126 → data_offset=125)
        // 0x82 as i8 = -126; ~(-126) = 125
        let trie_data = vec![
            0x08u8, // header
            0xF4,   // hash
            0x82,   // position = -126 as i8 → data_offset = 125
        ];
        let result = decode_bti_partition_payload(&trie_data, 1, 8).unwrap();
        assert_eq!(result, BtiPartitionLocation::DataOffset(125));
    }

    #[test]
    fn decode_bti_partition_payload_rows_offset() {
        // Positive position → RowsOffset (wide partition).
        // payloadBits=9 → 2 position bytes; position = 0x0100 = 256 (positive)
        let trie_data = vec![
            0x09u8, // header: ordinal=0, payloadBits=9
            0xAB,   // hash
            0x01,   // position[0] (MSB)
            0x00,   // position[1] (LSB) → i16 = 0x0100 = 256
        ];
        let result = decode_bti_partition_payload(&trie_data, 1, 9).unwrap();
        assert_eq!(result, BtiPartitionLocation::RowsOffset(256));
    }

    #[test]
    fn decode_bti_partition_payload_no_hash_byte_returns_error() {
        // payloadBits < FLAG_HAS_HASH_BYTE (8) → not supported in C5.0 BTI
        let trie_data = vec![0x07u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let err = decode_bti_partition_payload(&trie_data, 1, 7);
        assert!(err.is_err(), "payloadBits < 8 must be an error");
    }

    #[test]
    fn decode_bti_partition_payload_2byte_position() {
        // payloadBits=9 → 2 position bytes.
        // position = 0x00C0 as i16 = 192 (positive) → RowsOffset
        let trie_data = vec![0x09u8, 0xAB, 0x00, 0xC0];
        let result = decode_bti_partition_payload(&trie_data, 1, 9).unwrap();
        assert_eq!(result, BtiPartitionLocation::RowsOffset(192));
    }

    // -----------------------------------------------------------------------
    // sized_ints_read_from_slice unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn sized_ints_slice_1_byte_positive() {
        assert_eq!(sized_ints_read_from_slice(&[0x7F]).unwrap(), 127);
    }

    #[test]
    fn sized_ints_slice_1_byte_negative() {
        // 0xFF as i8 = -1
        assert_eq!(sized_ints_read_from_slice(&[0xFF]).unwrap(), -1);
        // 0xC0 as i8 = -64
        assert_eq!(sized_ints_read_from_slice(&[0xC0]).unwrap(), -64);
        // 0x82 as i8 = -126
        assert_eq!(sized_ints_read_from_slice(&[0x82]).unwrap(), -126);
    }

    #[test]
    fn sized_ints_slice_2_bytes() {
        // 0x00FF = 255 (positive)
        assert_eq!(sized_ints_read_from_slice(&[0x00, 0xFF]).unwrap(), 255);
        // 0xFF00 as i16 = -256
        assert_eq!(sized_ints_read_from_slice(&[0xFF, 0x00]).unwrap(), -256);
    }

    // -----------------------------------------------------------------------
    // walk_bti_trie unit tests — hand-crafted trie bytes
    // -----------------------------------------------------------------------

    /// Build a minimal 1-partition trie with a single PayloadOnly root node.
    #[test]
    fn walk_bti_trie_payload_only_root_with_empty_key() {
        // PayloadOnly (ordinal 0, payloadBits=8) at offset 0
        // payload: hash=0x00, position=0xFF (i8 = -1 → data_offset=0)
        let trie_data: Vec<u8> = vec![
            0x08, // header: ordinal=0, payloadBits=8
            0x00, // hash
            0xFF, // position = -1 → data_offset = ~(-1) = 0
        ];
        let result = walk_bti_trie(&trie_data, 0, &[]).unwrap();
        assert_eq!(result, Some(BtiPartitionLocation::DataOffset(0)));
    }

    /// Simplified: 2-partition trie, key "A" maps to offset 0, key "B" maps to offset 64.
    #[test]
    fn walk_bti_trie_two_partitions_via_sparse8_root() {
        let mut trie_data = vec![0u8; 12];
        // Leaf at offset 0: PayloadOnly, pf=8, hash=0x11, position=-1 → data_offset=0
        trie_data[0] = 0x08; // ordinal=0, payloadBits=8
        trie_data[1] = 0x11; // hash
        trie_data[2] = 0xFF; // position byte = -1 as i8

        // Leaf at offset 3: PayloadOnly, pf=8, hash=0x22, position=-65 → data_offset=64
        trie_data[3] = 0x08;
        trie_data[4] = 0x22; // hash
        trie_data[5] = 0xBF; // -65 as i8 → ~(-65) = 64

        // Sparse8 at offset 6: [0x50][count=2][0xAA][0xBB][delta_AA=6][delta_BB=3]
        trie_data[6] = 0x50; // ordinal=5, payloadBits=0
        trie_data[7] = 0x02; // count=2
        trie_data[8] = 0xAA; // transition[0]
        trie_data[9] = 0xBB; // transition[1]
        trie_data[10] = 0x06; // delta[0] → child = 6 - 6 = 0
        trie_data[11] = 0x03; // delta[1] → child = 6 - 3 = 3

        let result_a = walk_bti_trie(&trie_data, 6, &[0xAA]).unwrap();
        assert_eq!(
            result_a,
            Some(BtiPartitionLocation::DataOffset(0)),
            "key 0xAA should resolve to data_offset=0"
        );

        let result_b = walk_bti_trie(&trie_data, 6, &[0xBB]).unwrap();
        assert_eq!(
            result_b,
            Some(BtiPartitionLocation::DataOffset(64)),
            "key 0xBB should resolve to data_offset=64"
        );

        let result_miss = walk_bti_trie(&trie_data, 6, &[0xCC]).unwrap();
        assert_eq!(result_miss, None, "key 0xCC should not be found");
    }

    // -----------------------------------------------------------------------
    // lookup_partition_in_bti_file — hand-crafted in-memory Partitions.db
    // -----------------------------------------------------------------------

    /// Build a complete in-memory Partitions.db (trie + 8-byte root footer)
    /// and assert that `lookup_partition_in_bti_file` returns the correct
    /// Data.db offsets.
    #[test]
    fn lookup_partition_in_bti_file_synthetic_two_partitions() {
        use std::io::Cursor;

        let mut trie_file = vec![0u8; 12 + 8];

        // Leaf A (offset 0)
        trie_file[0] = 0x08;
        trie_file[1] = 0x11;
        trie_file[2] = 0xFF; // -1 as i8 → data_offset=0

        // Leaf B (offset 3)
        trie_file[3] = 0x08;
        trie_file[4] = 0x22;
        trie_file[5] = 0xBF; // -65 as i8 → data_offset=64

        // Sparse8 root (offset 6)
        trie_file[6] = 0x50;
        trie_file[7] = 0x02;
        trie_file[8] = 0xAA;
        trie_file[9] = 0xBB;
        trie_file[10] = 0x06;
        trie_file[11] = 0x03;

        // Footer: root_offset = 6 as big-endian u64
        trie_file[12..20].copy_from_slice(&6u64.to_be_bytes());

        {
            let mut cursor = Cursor::new(trie_file.clone());
            let result =
                lookup_partition_in_bti_file(&mut cursor, &[0xAA]).expect("lookup must not error");
            assert_eq!(
                result,
                Some(BtiPartitionLocation::DataOffset(0)),
                "Trie lookup for key 0xAA must return DataOffset(0), NOT a sequential scan"
            );
        }

        {
            let mut cursor = Cursor::new(trie_file.clone());
            let result =
                lookup_partition_in_bti_file(&mut cursor, &[0xBB]).expect("lookup must not error");
            assert_eq!(
                result,
                Some(BtiPartitionLocation::DataOffset(64)),
                "Trie lookup for key 0xBB must return DataOffset(64)"
            );
        }

        {
            let mut cursor = Cursor::new(trie_file.clone());
            let result =
                lookup_partition_in_bti_file(&mut cursor, &[0xCC]).expect("lookup must not error");
            assert_eq!(result, None, "Key 0xCC must not be found");
        }
    }

    /// Prove that `lookup_partition_in_bti_file` resolves Data.db offsets via
    /// the trie (NOT a sequential scan) against the real BTI fixture.
    #[test]
    fn lookup_partition_in_bti_file_real_simple_table_fixture() {
        use std::fs::File;

        // Require the real test-data fixtures
        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(v) => std::path::PathBuf::from(v),
            Err(_) => {
                eprintln!(
                    "SKIP: CQLITE_DATASETS_ROOT not set; \
                     test requires real BTI fixture files"
                );
                return;
            }
        };

        let partitions_db = datasets_root.join(
            "sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Partitions.db",
        );
        if !partitions_db.exists() {
            eprintln!("SKIP: BTI fixture not found at {:?}", partitions_db);
            return;
        }

        let mut file = File::open(&partitions_db)
            .unwrap_or_else(|e| panic!("Cannot open {:?}: {}", partitions_db, e));

        // Read the trie data (file minus 8-byte footer)
        let file_size = {
            use std::io::Seek;
            file.seek(SeekFrom::End(0)).unwrap()
        };
        assert_eq!(file_size, 79, "simple_table Partitions.db must be 79 bytes");

        // Read root offset from footer
        {
            use std::io::Seek;
            file.seek(SeekFrom::End(-8)).unwrap();
        }
        let mut footer = [0u8; 8];
        file.read_exact(&mut footer).unwrap();
        let root_offset = u64::from_be_bytes(footer);
        assert_eq!(
            root_offset, 17,
            "simple_table Partitions.db root must be at offset 17"
        );

        // Load the full trie
        {
            use std::io::Seek;
            file.seek(SeekFrom::Start(0)).unwrap();
        }
        let mut trie_data = vec![0u8; 71]; // 79 - 8 footer
        file.read_exact(&mut trie_data).unwrap();

        // Verify all three leaves decode to known Data.db offsets.
        let loc0 =
            decode_bti_partition_payload(&trie_data, 1, 8).expect("leaf at offset 0 must decode");
        assert_eq!(
            loc0,
            BtiPartitionLocation::DataOffset(0),
            "leaf at trie offset 0 must map to Data.db position 0 (UUID 22222222...)"
        );

        let loc63 =
            decode_bti_partition_payload(&trie_data, 4, 8).expect("leaf at offset 3 must decode");
        assert_eq!(
            loc63,
            BtiPartitionLocation::DataOffset(63),
            "leaf at trie offset 3 must map to Data.db position 63 (UUID 11111111...)"
        );

        let loc125 =
            decode_bti_partition_payload(&trie_data, 7, 8).expect("leaf at offset 6 must decode");
        assert_eq!(
            loc125,
            BtiPartitionLocation::DataOffset(125),
            "leaf at trie offset 6 must map to Data.db position 125 (UUID 33333333...)"
        );

        // Prove the trie walk itself works end-to-end for the encoded key prefixes.
        let result_0 = walk_bti_trie(&trie_data, 17, &[0x40, 0x90]).unwrap();
        assert_eq!(
            result_0,
            Some(BtiPartitionLocation::DataOffset(0)),
            "[0x40,0x90] must resolve to DataOffset(0)"
        );

        let result_63 = walk_bti_trie(&trie_data, 17, &[0x40, 0xBC]).unwrap();
        assert_eq!(
            result_63,
            Some(BtiPartitionLocation::DataOffset(63)),
            "[0x40,0xBC] must resolve to DataOffset(63)"
        );

        let result_125 = walk_bti_trie(&trie_data, 17, &[0x40, 0xF9]).unwrap();
        assert_eq!(
            result_125,
            Some(BtiPartitionLocation::DataOffset(125)),
            "[0x40,0xF9] must resolve to DataOffset(125)"
        );

        // Now use the public API (lookup_partition_in_bti_file) with a fresh Cursor:
        use std::io::Cursor;
        let raw = std::fs::read(&partitions_db).unwrap();

        let mut cursor = Cursor::new(raw.clone());
        let r = lookup_partition_in_bti_file(&mut cursor, &[0x40, 0x90]).unwrap();
        assert_eq!(r, Some(BtiPartitionLocation::DataOffset(0)));

        let mut cursor = Cursor::new(raw.clone());
        let r = lookup_partition_in_bti_file(&mut cursor, &[0x40, 0xBC]).unwrap();
        assert_eq!(r, Some(BtiPartitionLocation::DataOffset(63)));

        let mut cursor = Cursor::new(raw.clone());
        let r = lookup_partition_in_bti_file(&mut cursor, &[0x40, 0xF9]).unwrap();
        assert_eq!(r, Some(BtiPartitionLocation::DataOffset(125)));

        let mut cursor = Cursor::new(raw.clone());
        let r = lookup_partition_in_bti_file(&mut cursor, &[0x40, 0x00]).unwrap();
        assert_eq!(r, None);

        println!(
            "VERIFIED: lookup_partition_in_bti_file resolved all 3 BTI partition \
             offsets (0, 63, 125) via trie walk, NOT sequential scan"
        );
    }
}
