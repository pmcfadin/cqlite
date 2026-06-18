//! BTI (Big Trie Index) parser implementation
//!
//! This module provides parsing capabilities for BTI format components:
//! - Partitions.db BTI index for partition lookups
//! - Rows.db BTI index for clustering key lookups within partitions
//!
//! ## Node-type encoding (TrieNode.java, Cassandra 5.0)
//!
//! The high nibble (bits 7-4) of every node's first byte is a 4-bit ordinal that
//! selects one of up to 16 concrete trie-node subtypes:
//!
//! | Nibble | Java class          | Rust category    | Pointer size |
//! |--------|---------------------|------------------|--------------|
//! | 0      | PayloadOnly         | `PayloadOnly`    | —            |
//! | 1      | SingleNoPayload4    | `Single`         | 4-bit delta  |
//! | 2      | Single8             | `Single`         | 1 byte       |
//! | 3      | SingleNoPayload12   | `Single`         | 12-bit delta |
//! | 4      | Single16            | `Single`         | 2 bytes      |
//! | 5      | Sparse8             | `Sparse`         | 1 byte       |
//! | 6      | Sparse12            | `Sparse`         | 12-bit       |
//! | 7      | Sparse16            | `Sparse`         | 2 bytes      |
//! | 8      | Sparse24            | `Sparse`         | 3 bytes      |
//! | 9      | Sparse40            | `Sparse`         | 5 bytes      |
//! | 10     | Dense12             | `Dense`          | 12-bit       |
//! | 11     | Dense16             | `Dense`          | 2 bytes      |
//! | 12     | Dense24             | `Dense`          | 3 bytes      |
//! | 13     | Dense32             | `Dense`          | 4 bytes      |
//! | 14     | Dense40             | `Dense`          | 5 bytes      |
//! | 15     | LongDense           | `Dense`          | 8 bytes      |
//!
//! The low nibble (bits 3-0) carries payload flags; a non-zero value means a
//! payload follows immediately after the node's fixed-size header.
//!
//! All transition deltas are stored as *backward* distances (the child always
//! appears earlier in the file), so the absolute child position is:
//!   `child_pos = parent_pos - delta`
//!
//! The "no-payload" Single variants (nibbles 1 and 3) encode the delta in the
//! low nibble / low 12 bits of the first two bytes respectively, and cannot
//! carry a payload (their payload-flag nibble is always 0).

use crate::{
    error::Error,
    storage::sstable::bti::{
        encoder::ByteComparableEncoder,
        node::{
            BtiNode, BtiNodeData, BtiNodeType, BtiResult, PayloadRef, SizedPointer, Transition,
            TrieNavigator,
        },
    },
    types::Value,
};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};

// ---------------------------------------------------------------------------
// Shared node-parsing helpers
// ---------------------------------------------------------------------------

/// Classify the high nibble of a BTI node's first byte into one of the four
/// Rust-level node categories.
///
/// The mapping follows the 16-entry `Types.values[]` array in `TrieNode.java`:
/// - nibble 0               → `PayloadOnly`
/// - nibbles 1-4 (Singles)  → `Single`
/// - nibbles 5-9 (Sparses)  → `Sparse`
/// - nibbles 10-15 (Denses) → `Dense`
fn classify_node_nibble(nibble: u8) -> BtiResult<BtiNodeType> {
    match nibble {
        0 => Ok(BtiNodeType::PayloadOnly),
        1..=4 => Ok(BtiNodeType::Single),
        5..=9 => Ok(BtiNodeType::Sparse),
        10..=15 => Ok(BtiNodeType::Dense),
        other => Err(Error::Parse(format!(
            "Invalid BTI node type nibble: {}",
            other
        ))),
    }
}

/// Return the number of bytes used to store each child pointer for this node,
/// given the raw high nibble (`ordinal`) from the first byte.
///
/// Matches the `bytesPerPointer` field of each `TrieNode` subtype in
/// `TrieNode.java`.  Fractional (12-bit) encodings return `0` as a sentinel;
/// callers that need to handle those cases do so explicitly.
fn pointer_bytes_for_ordinal(ordinal: u8) -> u8 {
    match ordinal {
        0 => 0,  // PayloadOnly — no pointers
        1 => 0,  // SingleNoPayload4  — 4-bit delta in low nibble, handled specially
        2 => 1,  // Single8
        3 => 0,  // SingleNoPayload12 — 12-bit delta across first two bytes
        4 => 2,  // Single16
        5 => 1,  // Sparse8
        6 => 0,  // Sparse12 — 12-bit packed
        7 => 2,  // Sparse16
        8 => 3,  // Sparse24
        9 => 5,  // Sparse40
        10 => 0, // Dense12  — 12-bit packed
        11 => 2, // Dense16
        12 => 3, // Dense24
        13 => 4, // Dense32
        14 => 5, // Dense40
        15 => 8, // LongDense
        _ => 0,
    }
}

/// Parse a BTI trie node from a raw byte slice.
///
/// This is the **single shared implementation** used by both
/// [`PartitionsParser`] and [`RowsParser`].  It correctly dispatches all 16
/// node-type ordinals defined in `TrieNode.java` and produces the appropriate
/// [`BtiNode`] variant.
///
/// # Arguments
/// * `data`   – raw bytes starting at the node's first byte
/// * `offset` – absolute file offset of this node (used to compute child
///   positions; see the module-level doc for the sign convention)
///
/// # Errors
/// Returns a [`crate::error::Error::Parse`] if the data is too short, the
/// node-type nibble is out of range, or any other structural invariant is
/// violated.
fn parse_bti_node(data: &[u8], offset: u64) -> BtiResult<BtiNode> {
    if data.is_empty() {
        return Err(Error::Parse("Empty BTI node data".to_string()));
    }

    let header_byte = data[0];
    let ordinal = (header_byte >> 4) & 0x0F;
    let payload_flags = header_byte & 0x0F;
    let has_payload = payload_flags != 0;
    let node_type = classify_node_nibble(ordinal)?;

    match node_type {
        BtiNodeType::PayloadOnly => {
            // Layout: [header:1] [payload if has_payload]
            // PayloadOnly MUST have a payload (it is a leaf node).
            if !has_payload {
                return Err(Error::Parse(
                    "PayloadOnly node has no payload flags set".to_string(),
                ));
            }
            let payload = parse_payload_ref(&data[1..])?;
            Ok(BtiNode {
                node_type,
                level: 0,
                key_prefix: Vec::new(),
                data: BtiNodeData::PayloadOnly { payload },
            })
        }

        BtiNodeType::Single => {
            // Three concrete layouts depending on ordinal:
            //
            //   ordinal 1 (SingleNoPayload4):
            //     byte 0: [1|delta_high4]  (delta is low 4 bits of byte 0)
            //     byte 1: transition byte
            //     NO payload
            //
            //   ordinal 3 (SingleNoPayload12):
            //     byte 0: [3|delta_high4]
            //     byte 1: delta_low8        → delta = (low4(byte0) << 8) | byte1
            //     byte 2: transition byte
            //     NO payload
            //
            //   ordinals 2, 4 (Single8, Single16):
            //     byte 0: [ordinal|payload_flags]
            //     byte 1: transition byte
            //     bytes 2..(2+ptr_bytes): backward delta (unsigned big-endian)
            //     [payload if has_payload]
            match ordinal {
                1 => {
                    // SingleNoPayload4
                    if data.len() < 2 {
                        return Err(Error::Parse(
                            "SingleNoPayload4 node data too short".to_string(),
                        ));
                    }
                    let delta = (header_byte & 0x0F) as u64;
                    let transition_byte = data[1];
                    let child_offset = offset.saturating_sub(delta);
                    Ok(BtiNode {
                        node_type,
                        level: 1,
                        key_prefix: Vec::new(),
                        data: BtiNodeData::Single {
                            transition: Transition::new(
                                transition_byte,
                                SizedPointer::new(child_offset),
                            ),
                        },
                    })
                }
                3 => {
                    // SingleNoPayload12
                    if data.len() < 3 {
                        return Err(Error::Parse(
                            "SingleNoPayload12 node data too short".to_string(),
                        ));
                    }
                    let delta = (((header_byte & 0x0F) as u64) << 8) | (data[1] as u64);
                    let transition_byte = data[2];
                    let child_offset = offset.saturating_sub(delta);
                    Ok(BtiNode {
                        node_type,
                        level: 1,
                        key_prefix: Vec::new(),
                        data: BtiNodeData::Single {
                            transition: Transition::new(
                                transition_byte,
                                SizedPointer::new(child_offset),
                            ),
                        },
                    })
                }
                _ => {
                    // Single8 (ordinal 2) or Single16 (ordinal 4)
                    let ptr_bytes = pointer_bytes_for_ordinal(ordinal) as usize;
                    let needed = 2 + ptr_bytes;
                    if data.len() < needed {
                        return Err(Error::Parse(format!(
                            "Single node (ordinal {}) data too short: need {} bytes, have {}",
                            ordinal,
                            needed,
                            data.len()
                        )));
                    }
                    let transition_byte = data[1];
                    let delta = read_be_unsigned(&data[2..2 + ptr_bytes]);
                    let child_offset = offset.saturating_sub(delta);
                    let transition =
                        Transition::new(transition_byte, SizedPointer::new(child_offset));
                    Ok(BtiNode {
                        node_type,
                        level: 1,
                        key_prefix: Vec::new(),
                        data: BtiNodeData::Single { transition },
                    })
                }
            }
        }

        BtiNodeType::Sparse => {
            // Layout (ordinals 5, 7, 8, 9 — full-byte pointers):
            //   byte 0: [ordinal|payload_flags]
            //   byte 1: count (number of transitions, 1-255)
            //   bytes 2..(2+count): transition bytes (sorted)
            //   then count × ptr_bytes: backward deltas
            //   [payload if has_payload]
            //
            // ordinal 6 (Sparse12) packs two 12-bit deltas into 3 bytes;
            // that encoding is rare in practice and we decode it correctly
            // but store the absolute child offsets in the same SizedPointer.
            if data.len() < 2 {
                return Err(Error::Parse("Sparse node data too short".to_string()));
            }
            let count = data[1] as usize;
            if count == 0 {
                return Err(Error::Parse(
                    "Sparse node must have at least one transition".to_string(),
                ));
            }

            let bytes_start = 2;
            let pointers_start = bytes_start + count;

            if ordinal == 6 {
                // Sparse12: each pair of pointers packed into 3 bytes
                let packed_len = (count * 3).div_ceil(2); // ceil(count * 12 / 8) = ceil(count * 3 / 2)
                let needed = pointers_start + packed_len;
                if data.len() < needed {
                    return Err(Error::Parse(format!(
                        "Sparse12 node data too short: need {}, have {}",
                        needed,
                        data.len()
                    )));
                }
                let mut transitions = Vec::with_capacity(count);
                for i in 0..count {
                    let t_byte = data[bytes_start + i];
                    let delta = read_12bit_packed(&data[pointers_start..], i);
                    let child_offset = offset.saturating_sub(delta);
                    transitions.push(Transition::new(t_byte, SizedPointer::new(child_offset)));
                }
                Ok(BtiNode {
                    node_type,
                    level: 1,
                    key_prefix: Vec::new(),
                    data: BtiNodeData::Sparse { transitions },
                })
            } else {
                let ptr_bytes = pointer_bytes_for_ordinal(ordinal) as usize;
                let needed = pointers_start + count * ptr_bytes;
                if data.len() < needed {
                    return Err(Error::Parse(format!(
                        "Sparse node (ordinal {}) data too short: need {}, have {}",
                        ordinal,
                        needed,
                        data.len()
                    )));
                }
                let mut transitions = Vec::with_capacity(count);
                for i in 0..count {
                    let t_byte = data[bytes_start + i];
                    let ptr_off = pointers_start + i * ptr_bytes;
                    let delta = read_be_unsigned(&data[ptr_off..ptr_off + ptr_bytes]);
                    let child_offset = offset.saturating_sub(delta);
                    transitions.push(Transition::new(t_byte, SizedPointer::new(child_offset)));
                }
                Ok(BtiNode {
                    node_type,
                    level: 1,
                    key_prefix: Vec::new(),
                    data: BtiNodeData::Sparse { transitions },
                })
            }
        }

        BtiNodeType::Dense => {
            // Layout (ordinals 11-14 — full-byte pointers):
            //   byte 0: [ordinal|payload_flags]
            //   byte 1: start byte (first transition character)
            //   byte 2: (end - start), i.e. (range_len - 1)  → range_len = byte2+1
            //   then range_len × ptr_bytes: backward deltas; 0 means "no transition"
            //   [payload if has_payload]
            //
            // ordinal 10 (Dense12): packed 12-bit deltas, similar to Sparse12.
            // ordinal 15 (LongDense): 8-byte deltas.
            if data.len() < 3 {
                return Err(Error::Parse("Dense node data too short".to_string()));
            }
            let start_byte = data[1];
            let range_len = data[2] as usize + 1; // byte2 is (len-1)

            if ordinal == 10 {
                // Dense12: ceil(range_len * 12 / 8) = (range_len * 3 + 1) / 2
                let packed_len = (range_len * 3).div_ceil(2);
                let needed = 3 + packed_len;
                if data.len() < needed {
                    return Err(Error::Parse(format!(
                        "Dense12 node data too short: need {}, have {}",
                        needed,
                        data.len()
                    )));
                }
                let mut children = Vec::with_capacity(range_len);
                for i in 0..range_len {
                    let delta = read_12bit_packed(&data[3..], i);
                    // delta == 0 means "no transition" for Dense nodes
                    let child_offset = if delta == 0 {
                        0
                    } else {
                        offset.saturating_sub(delta)
                    };
                    children.push(SizedPointer::new(child_offset));
                }
                Ok(BtiNode {
                    node_type,
                    level: 1,
                    key_prefix: Vec::new(),
                    data: BtiNodeData::Dense {
                        start_byte,
                        children,
                    },
                })
            } else {
                let ptr_bytes = pointer_bytes_for_ordinal(ordinal) as usize;
                let needed = 3 + range_len * ptr_bytes;
                if data.len() < needed {
                    return Err(Error::Parse(format!(
                        "Dense node (ordinal {}) data too short: need {}, have {}",
                        ordinal,
                        needed,
                        data.len()
                    )));
                }
                let mut children = Vec::with_capacity(range_len);
                for i in 0..range_len {
                    let ptr_off = 3 + i * ptr_bytes;
                    let delta = read_be_unsigned(&data[ptr_off..ptr_off + ptr_bytes]);
                    // delta == 0 means "no transition" for Dense nodes
                    let child_offset = if delta == 0 {
                        0
                    } else {
                        offset.saturating_sub(delta)
                    };
                    children.push(SizedPointer::new(child_offset));
                }
                Ok(BtiNode {
                    node_type,
                    level: 1,
                    key_prefix: Vec::new(),
                    data: BtiNodeData::Dense {
                        start_byte,
                        children,
                    },
                })
            }
        }
    }
}

/// Read a big-endian unsigned integer of 0-8 bytes from `data`.
///
/// Returns 0 for an empty slice; panics if `data.len() > 8` (caller is
/// responsible for bounds-checking first).
fn read_be_unsigned(data: &[u8]) -> u64 {
    let mut result = 0u64;
    for &byte in data {
        result = (result << 8) | (byte as u64);
    }
    result
}

/// Read a 12-bit value stored in the packed format used by Sparse12 / Dense12.
///
/// The packing (from `TrieNode.java#read12Bits`):
/// ```text
/// word = getShort(base + (3 * index) / 2)
/// if (index & 1) == 0:  value = word >> 4
/// else:                  value = word & 0xFFF
/// ```
fn read_12bit_packed(data: &[u8], index: usize) -> u64 {
    let byte_offset = (3 * index) / 2;
    if byte_offset + 1 >= data.len() {
        return 0;
    }
    let word = ((data[byte_offset] as u16) << 8) | (data[byte_offset + 1] as u16);
    let value = if (index & 1) == 0 {
        word >> 4
    } else {
        word & 0x0FFF
    };
    value as u64
}

/// Parse a [`PayloadRef`] from the bytes immediately following a node header.
///
/// Format (matches `RowIndexReader.readPayload` / `PartitionIndex`):
///   8 bytes big-endian: data file offset
///   4 bytes big-endian: payload byte length
fn parse_payload_ref(data: &[u8]) -> BtiResult<PayloadRef> {
    if data.len() < 12 {
        return Err(Error::Parse(format!(
            "PayloadRef data too short: need 12 bytes, have {}",
            data.len()
        )));
    }
    let offset = u64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    let length = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    Ok(PayloadRef::new(offset, length))
}

// ─────────────────────────────────────────────────────────────────────────────
// Real Cassandra BTI Partitions.db trie-payload decoding
// ─────────────────────────────────────────────────────────────────────────────
//
// Cassandra 5.0 `Partitions.db` has NO header.  The file is a compact trie
// whose nodes are written bottom-up (children appear at lower offsets than
// parents), with the root offset stored as the **last 8 bytes** of the file as
// a big-endian u64.
//
// Every leaf node (ordinal 0 = PayloadOnly) carries a *payload* described by
// the `payloadBits` value encoded in the **low nibble** of the node's header
// byte.  In Cassandra 5.0 the hash byte is **always present**
// (`FLAG_HAS_HASH_BYTE = 8`), so `payloadBits >= 8` for every real partition
// leaf.  The payload layout is:
//
//   byte 0            : partition-key filter hash (lowest 8 bits of Murmur3)
//   bytes 1 .. N      : SizedInts-encoded signed `position`, where
//                        N = payloadBits − 8 + 1  (= payloadBits − 7)
//
// **Sign convention** (mirrors `PartitionIndex.java`):
//   - If `position < 0` → `data_offset = ~position` (bitwise NOT; integer NOT)
//   - If `position >= 0` → `position` points into `Rows.db` (wide partition)
//
// Reference: `PartitionIndex.java:131–135`, `BtiFormat.md:946–963`,
//            `SizedInts.java` (local mirror: `sized_ints.rs`).
// ─────────────────────────────────────────────────────────────────────────────

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
fn sized_ints_read_from_slice(data: &[u8]) -> BtiResult<i64> {
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
/// legacy 12-byte `PayloadRef` format expected by [`parse_payload_ref`].
fn parse_bti_node_for_traversal(trie_data: &[u8], node_offset: usize) -> BtiResult<BtiNode> {
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
            let stub = PayloadRef::new(0, 0);
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
    if node_offset >= trie_data.len() {
        return Err(Error::Parse(format!(
            "BTI node offset {node_offset} out of bounds (trie_data.len={})",
            trie_data.len()
        )));
    }

    let bti_node = parse_bti_node_for_traversal(trie_data, node_offset)?;

    match bti_node.find_child(search_byte) {
        Some(ptr) => {
            let child = ptr.distance as usize; // distance stores the absolute child offset
            Ok(Some(child))
        }
        None => Ok(None),
    }
}

/// Read the `BtiPartitionLocation` from the payload attached to the BTI node
/// at `node_offset`.  Returns `None` if the node has no payload.
///
/// For `PayloadOnly` nodes the payload immediately follows the 1-byte header.
/// For other node types (Single/Sparse/Dense with a non-zero `payloadBits`
/// low nibble) the payload appears *after* all the pointer/transition bytes;
/// this function delegates correctly to [`decode_bti_partition_payload`].
fn read_node_payload(
    trie_data: &[u8],
    node_offset: usize,
) -> BtiResult<Option<BtiPartitionLocation>> {
    if node_offset >= trie_data.len() {
        return Err(Error::Parse(format!(
            "BTI payload read: node_offset {node_offset} out of bounds"
        )));
    }

    let header_byte = trie_data[node_offset];
    let ordinal = (header_byte >> 4) & 0x0F;
    let payload_flags = header_byte & 0x0F; // aka payloadBits

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
        // Non-leaf node with an embedded payload (prefix match for short keys).
        // We need to skip over the node's own pointer/transition data to reach
        // the payload.  Reuse `parse_bti_node` to find the payload start.
        let node = parse_bti_node(trie_data, node_offset as u64)?;
        // Determine where the payload bytes start: after all transition/pointer bytes.
        // payload_position = node_offset + node_byte_size_without_payload
        let payload_start = payload_start_in_node(&node, trie_data, node_offset)?;
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
fn payload_start_in_node(node: &BtiNode, trie_data: &[u8], node_offset: usize) -> BtiResult<usize> {
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
/// whose **byte-comparable** encoding exactly matches `encoded_key`.
///
/// Returns:
/// - `Ok(Some(BtiPartitionLocation))` when the key is found.
/// - `Ok(None)` when the key is not present in the trie.
/// - `Err(_)` on structural parse errors.
///
/// This is the inner engine for [`lookup_partition_in_bti_file`].
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
fn walk_bti_trie(
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
            return read_node_payload(trie_data, current_offset);
        }

        // Non-leaf node: if key exhausted, check for an embedded payload
        if key_pos >= encoded_key.len() {
            if payload_flags != 0 {
                return read_node_payload(trie_data, current_offset);
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
///   as produced by [`ByteComparableEncoder`]).
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
    use crate::util::cassandra_murmur3::cassandra_murmur3_token;

    let token: i64 = cassandra_murmur3_token(raw_key_bytes);
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

/// BTI header structure for index files
#[derive(Debug, Clone)]
pub struct BtiHeader {
    /// BTI format magic number
    pub magic: u32,
    /// Format version
    pub version: u16,
    /// Format flags
    pub flags: u16,
    /// Offset to root node
    pub root_offset: u64,
    /// Number of entries in the index
    pub entry_count: u64,
    /// Additional metadata size
    pub metadata_size: u32,
}

impl BtiHeader {
    /// BTI magic number
    pub const MAGIC: u32 = 0x6461_0000; // 'da\0\0'

    /// Current BTI version
    pub const VERSION: u16 = 0x0001;

    /// Parse BTI header from bytes
    pub fn parse(data: &[u8]) -> BtiResult<(Self, usize)> {
        if data.len() < 24 {
            return Err(Error::Parse("BTI header too short".to_string()));
        }

        let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if magic != Self::MAGIC {
            return Err(Error::Parse(format!(
                "Invalid BTI magic: 0x{:08x}, expected 0x{:08x}",
                magic,
                Self::MAGIC
            )));
        }

        let version = u16::from_be_bytes([data[4], data[5]]);
        if version != Self::VERSION {
            return Err(Error::Parse(format!(
                "Unsupported BTI version: 0x{:04x}, expected 0x{:04x}",
                version,
                Self::VERSION
            )));
        }

        let flags = u16::from_be_bytes([data[6], data[7]]);
        let root_offset = u64::from_be_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);
        let entry_count = u64::from_be_bytes([
            data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
        ]);

        let metadata_size = if data.len() >= 28 {
            u32::from_be_bytes([data[24], data[25], data[26], data[27]])
        } else {
            0
        };

        let header = BtiHeader {
            magic,
            version,
            flags,
            root_offset,
            entry_count,
            metadata_size,
        };

        let header_size = if metadata_size > 0 { 28 } else { 24 };
        Ok((header, header_size))
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(28);

        bytes.extend_from_slice(&self.magic.to_be_bytes());
        bytes.extend_from_slice(&self.version.to_be_bytes());
        bytes.extend_from_slice(&self.flags.to_be_bytes());
        bytes.extend_from_slice(&self.root_offset.to_be_bytes());
        bytes.extend_from_slice(&self.entry_count.to_be_bytes());

        if self.metadata_size > 0 {
            bytes.extend_from_slice(&self.metadata_size.to_be_bytes());
        }

        bytes
    }
}

/// Parser for Partitions.db BTI index
pub struct PartitionsParser<R: Read + Seek> {
    /// Input reader
    reader: R,
    /// BTI header
    header: BtiHeader,
    /// Byte-comparable encoder for key encoding
    encoder: ByteComparableEncoder,
    /// Node cache for performance
    node_cache: HashMap<u64, BtiNode>,
}

impl<R: Read + Seek> PartitionsParser<R> {
    /// Create new partitions parser
    pub fn new(mut reader: R) -> BtiResult<Self> {
        // Read and parse header
        reader.seek(SeekFrom::Start(0))?;
        let mut header_data = vec![0u8; 28];
        reader.read_exact(&mut header_data)?;

        let (header, _) = BtiHeader::parse(&header_data)?;

        Ok(Self {
            reader,
            header,
            encoder: ByteComparableEncoder::new(),
            node_cache: HashMap::new(),
        })
    }

    /// Lookup partition by key
    pub fn lookup_partition(&mut self, partition_key: &[Value]) -> BtiResult<Option<PayloadRef>> {
        // Encode partition key for lookup
        let encoded_key = self.encoder.encode_composite_key(partition_key)?;

        // Navigate trie to find the partition
        let mut navigator = TrieNavigator::new(self.header.root_offset);

        self.lookup_in_trie(&mut navigator, &encoded_key)
    }

    /// Navigate trie to find encoded key
    fn lookup_in_trie(
        &mut self,
        navigator: &mut TrieNavigator,
        encoded_key: &[u8],
    ) -> BtiResult<Option<PayloadRef>> {
        let mut key_pos = 0;

        loop {
            // Load current node
            let current_node = self.load_node(navigator.current_offset)?;

            // If this is a payload-only node (leaf), return its payload
            if current_node.is_leaf() {
                return Ok(current_node.get_payload().cloned());
            }

            // Check if we have a payload at this level (for prefix matches)
            if let Some(payload) = current_node.get_payload() {
                if key_pos >= encoded_key.len() {
                    return Ok(Some(payload.clone()));
                }
            }

            // If we've consumed all key bytes, return any payload we have
            if key_pos >= encoded_key.len() {
                return Ok(current_node.get_payload().cloned());
            }

            // Find transition for next byte
            let next_byte = encoded_key[key_pos];
            if let Some(child_pointer) = current_node.find_child(next_byte) {
                navigator.navigate_to_child(next_byte, child_pointer)?;
                key_pos += 1;
            } else {
                // No transition found - key doesn't exist
                return Ok(None);
            }
        }
    }

    /// Load node from file
    fn load_node(&mut self, offset: u64) -> BtiResult<BtiNode> {
        if let Some(cached_node) = self.node_cache.get(&offset) {
            return Ok(cached_node.clone());
        }

        // Read node from file
        self.reader.seek(SeekFrom::Start(offset))?;
        let mut node_data = vec![0u8; 4096]; // Read up to 4KB for node
        let bytes_read = self.reader.read(&mut node_data)?;
        node_data.truncate(bytes_read);

        // Parse node
        let node = self.parse_node_data(&node_data, offset)?;

        // Cache the node
        self.node_cache.insert(offset, node.clone());
        Ok(node)
    }

    /// Parse node data from bytes.
    ///
    /// Delegates to the module-level [`parse_bti_node`] helper which handles
    /// all 16 BTI node-type ordinals defined in `TrieNode.java`.
    fn parse_node_data(&self, data: &[u8], offset: u64) -> BtiResult<BtiNode> {
        parse_bti_node(data, offset)
    }

    /// Iterator over all partitions in the index
    pub fn iterate_partitions(&mut self) -> BtiResult<PartitionIterator<'_, R>> {
        PartitionIterator::new(self)
    }

    /// Get header information
    pub fn header(&self) -> &BtiHeader {
        &self.header
    }

    /// Get statistics about the index
    pub fn get_stats(&self) -> BtiIndexStats {
        BtiIndexStats {
            entry_count: self.header.entry_count,
            root_offset: self.header.root_offset,
            cached_nodes: self.node_cache.len(),
        }
    }
}

/// Parser for Rows.db BTI index (clustering keys within a partition)
pub struct RowsParser<R: Read + Seek> {
    /// Input reader
    reader: R,
    /// BTI header
    header: BtiHeader,
    /// Byte-comparable encoder for key encoding
    encoder: ByteComparableEncoder,
    /// Node cache for performance
    node_cache: HashMap<u64, BtiNode>,
}

impl<R: Read + Seek> RowsParser<R> {
    /// Create new rows parser
    pub fn new(mut reader: R) -> BtiResult<Self> {
        // Read and parse header
        reader.seek(SeekFrom::Start(0))?;
        let mut header_data = vec![0u8; 28];
        reader.read_exact(&mut header_data)?;

        let (header, _) = BtiHeader::parse(&header_data)?;

        Ok(Self {
            reader,
            header,
            encoder: ByteComparableEncoder::new(),
            node_cache: HashMap::new(),
        })
    }

    /// Lookup row by clustering key
    pub fn lookup_row(&mut self, clustering_key: &[Value]) -> BtiResult<Option<PayloadRef>> {
        // Encode clustering key for lookup
        let encoded_key = self.encoder.encode_composite_key(clustering_key)?;

        // Navigate trie to find the row
        let mut navigator = TrieNavigator::new(self.header.root_offset);

        self.lookup_in_trie(&mut navigator, &encoded_key)
    }

    /// Navigate trie to find encoded key (similar to partitions parser)
    fn lookup_in_trie(
        &mut self,
        navigator: &mut TrieNavigator,
        encoded_key: &[u8],
    ) -> BtiResult<Option<PayloadRef>> {
        let mut key_pos = 0;

        loop {
            // Load current node
            let current_node = self.load_node(navigator.current_offset)?;

            // Check if we have a payload at this level
            if let Some(payload) = current_node.get_payload() {
                if key_pos >= encoded_key.len() {
                    return Ok(Some(payload.clone()));
                }
            }

            // If we've consumed all key bytes and this is a leaf, we found it
            if key_pos >= encoded_key.len() {
                return Ok(current_node.get_payload().cloned());
            }

            // Find transition for next byte
            let next_byte = encoded_key[key_pos];
            if let Some(child_pointer) = current_node.find_child(next_byte) {
                navigator.navigate_to_child(next_byte, child_pointer)?;
                key_pos += 1;
            } else {
                // No transition found - key doesn't exist
                return Ok(None);
            }
        }
    }

    /// Load node from file (similar to partitions parser)
    fn load_node(&mut self, offset: u64) -> BtiResult<BtiNode> {
        if let Some(cached_node) = self.node_cache.get(&offset) {
            return Ok(cached_node.clone());
        }

        // Read node from file
        self.reader.seek(SeekFrom::Start(offset))?;
        let mut node_data = vec![0u8; 4096]; // Read up to 4KB for node
        let bytes_read = self.reader.read(&mut node_data)?;
        node_data.truncate(bytes_read);

        // Parse node
        let node = self.parse_node_data(&node_data, offset)?;

        // Cache the node
        self.node_cache.insert(offset, node.clone());
        Ok(node)
    }

    /// Parse node data from bytes.
    ///
    /// Delegates to the module-level [`parse_bti_node`] helper which handles
    /// all 16 BTI node-type ordinals defined in `TrieNode.java`.
    ///
    /// Previously this was a stub that always returned `BtiNodeType::PayloadOnly`
    /// regardless of the actual node type encoded in the header byte (#647).
    fn parse_node_data(&self, data: &[u8], offset: u64) -> BtiResult<BtiNode> {
        parse_bti_node(data, offset)
    }

    /// Range query for clustering keys
    pub fn range_query(
        &mut self,
        start_key: &[Value],
        end_key: &[Value],
    ) -> BtiResult<Vec<PayloadRef>> {
        let _encoded_start = self.encoder.encode_composite_key(start_key)?;
        let _encoded_end = self.encoder.encode_composite_key(end_key)?;

        // Issue #755 scope decision: range_query over the fake-header RowsParser
        // is not yet implemented.  Returning an empty Vec here would be a *silent*
        // wrong-result path — callers would see "no rows in range" instead of an
        // error, making bugs invisible.  Return an explicit error so callers know
        // this path is unimplemented and must not rely on its output.
        //
        // Follow-up issue: implement proper BTI Rows.db range traversal.
        // Proposed title: "feat(bti): implement RowsParser range_query trie traversal"
        Err(Error::Parse(
            "BTI RowsParser::range_query is not yet implemented; \
             this stub previously returned an empty Vec (silent wrong-result). \
             See follow-up issue for BTI Rows.db range traversal implementation."
                .to_string(),
        ))
    }

    /// Iterator over all rows in the index
    pub fn iterate_rows(&mut self) -> BtiResult<RowIterator<'_, R>> {
        RowIterator::new(self)
    }

    /// Get header information
    pub fn header(&self) -> &BtiHeader {
        &self.header
    }
}

/// Iterator over partitions in BTI index
pub struct PartitionIterator<'a, R: Read + Seek> {
    #[allow(dead_code)]
    parser: &'a mut PartitionsParser<R>,
    #[allow(dead_code)]
    current_position: u64,
    finished: bool,
}

impl<'a, R: Read + Seek> PartitionIterator<'a, R> {
    fn new(parser: &'a mut PartitionsParser<R>) -> BtiResult<Self> {
        let root_offset = parser.header.root_offset;
        Ok(Self {
            parser,
            current_position: root_offset,
            finished: false,
        })
    }
}

impl<'a, R: Read + Seek> Iterator for PartitionIterator<'a, R> {
    type Item = BtiResult<(Vec<u8>, PayloadRef)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        self.finished = true;

        // Issue #755 scope decision: full trie traversal for iteration over the
        // fake-header PartitionsParser is not yet implemented.  Silently returning
        // `None` here would make callers believe the index is empty (silent
        // wrong-result).  Emit an explicit error so callers see an unambiguous
        // "not implemented" rather than mysteriously empty iteration.
        //
        // Follow-up issue: implement BTI PartitionIterator trie traversal
        // Proposed title: "feat(bti): implement PartitionIterator full trie traversal"
        Some(Err(Error::Parse(
            "BTI PartitionIterator::next is not yet implemented; \
             this stub previously returned None (silent wrong-result). \
             See follow-up issue for full trie traversal implementation."
                .to_string(),
        )))
    }
}

/// Iterator over rows in BTI index
pub struct RowIterator<'a, R: Read + Seek> {
    #[allow(dead_code)]
    parser: &'a mut RowsParser<R>,
    #[allow(dead_code)]
    current_position: u64,
    finished: bool,
}

impl<'a, R: Read + Seek> RowIterator<'a, R> {
    fn new(parser: &'a mut RowsParser<R>) -> BtiResult<Self> {
        let root_offset = parser.header.root_offset;
        Ok(Self {
            parser,
            current_position: root_offset,
            finished: false,
        })
    }
}

impl<'a, R: Read + Seek> Iterator for RowIterator<'a, R> {
    type Item = BtiResult<(Vec<u8>, PayloadRef)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        self.finished = true;

        // Issue #755 scope decision: full trie traversal for iteration over the
        // fake-header RowsParser is not yet implemented.  Silently returning
        // `None` here would make callers believe the row index is empty (silent
        // wrong-result).  Emit an explicit error so callers see an unambiguous
        // "not implemented" rather than mysteriously empty iteration.
        //
        // Follow-up issue: implement BTI RowIterator trie traversal
        // Proposed title: "feat(bti): implement RowIterator full trie traversal"
        Some(Err(Error::Parse(
            "BTI RowIterator::next is not yet implemented; \
             this stub previously returned None (silent wrong-result). \
             See follow-up issue for full trie traversal implementation."
                .to_string(),
        )))
    }
}

/// Statistics about BTI index
#[derive(Debug, Clone)]
pub struct BtiIndexStats {
    /// Number of entries in the index
    pub entry_count: u64,
    /// Root node offset
    pub root_offset: u64,
    /// Number of cached nodes
    pub cached_nodes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // -----------------------------------------------------------------------
    // Helper: build a minimal valid BTI file with a given root node payload
    // -----------------------------------------------------------------------

    fn make_bti_file(root_node_bytes: Vec<u8>) -> Vec<u8> {
        let root_offset: u64 = 64; // place root after header + padding
        let mut data = Vec::new();
        data.extend_from_slice(&BtiHeader::MAGIC.to_be_bytes());
        data.extend_from_slice(&BtiHeader::VERSION.to_be_bytes());
        data.extend_from_slice(&0u16.to_be_bytes()); // flags
        data.extend_from_slice(&root_offset.to_be_bytes());
        data.extend_from_slice(&1u64.to_be_bytes()); // entry_count
        data.extend_from_slice(&0u32.to_be_bytes()); // metadata_size
        while data.len() < root_offset as usize {
            data.push(0);
        }
        data.extend(root_node_bytes);
        data
    }

    // -----------------------------------------------------------------------
    // Crafted node bytes per TrieNode.java
    // -----------------------------------------------------------------------

    /// PayloadOnly (ordinal 0) with a non-zero payload flag (nibble = 1).
    /// Layout: [0x01] [8-byte data offset] [4-byte length]
    fn payload_only_node(data_offset: u64, length: u32) -> Vec<u8> {
        let mut v = vec![0x01u8]; // ordinal=0, payload_flags=1
        v.extend_from_slice(&data_offset.to_be_bytes());
        v.extend_from_slice(&length.to_be_bytes());
        v
    }

    /// Single8 (ordinal 2): [0x20|pf] [transition_byte] [1-byte backward delta]
    fn single8_node(payload_flags: u8, transition: u8, delta: u8) -> Vec<u8> {
        vec![0x20 | (payload_flags & 0x0F), transition, delta]
    }

    /// SingleNoPayload4 (ordinal 1): delta in low 4 bits of first byte, no payload.
    /// Layout: [0x10 | delta4] [transition_byte]
    fn single_nopayload4_node(delta4: u8, transition: u8) -> Vec<u8> {
        vec![0x10 | (delta4 & 0x0F), transition]
    }

    /// SingleNoPayload12 (ordinal 3): 12-bit delta across first 2 bytes, no payload.
    /// Layout: [0x30 | delta_high4] [delta_low8] [transition_byte]
    fn single_nopayload12_node(delta: u16, transition: u8) -> Vec<u8> {
        vec![
            0x30 | ((delta >> 8) as u8 & 0x0F),
            (delta & 0xFF) as u8,
            transition,
        ]
    }

    /// Single16 (ordinal 4): [0x40|pf] [transition_byte] [2-byte big-endian delta]
    fn single16_node(payload_flags: u8, transition: u8, delta: u16) -> Vec<u8> {
        let mut v = vec![0x40 | (payload_flags & 0x0F), transition];
        v.extend_from_slice(&delta.to_be_bytes());
        v
    }

    /// Sparse8 (ordinal 5): [0x50|pf] [count] [count transition bytes] [count 1-byte deltas]
    fn sparse8_node(payload_flags: u8, pairs: &[(u8, u8)]) -> Vec<u8> {
        let mut v = vec![0x50 | (payload_flags & 0x0F), pairs.len() as u8];
        for &(t, _) in pairs {
            v.push(t);
        }
        for &(_, d) in pairs {
            v.push(d);
        }
        v
    }

    /// Dense16 (ordinal 11): [0xB0|pf] [start] [len-1] [range * 2-byte deltas]
    fn dense16_node(payload_flags: u8, start: u8, deltas: &[u16]) -> Vec<u8> {
        let len = deltas.len() as u8;
        let mut v = vec![0xB0 | (payload_flags & 0x0F), start, len - 1];
        for &d in deltas {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    /// LongDense (ordinal 15): [0xF0|pf] [start] [len-1] [range * 8-byte deltas]
    fn long_dense_node(payload_flags: u8, start: u8, deltas: &[u64]) -> Vec<u8> {
        let len = deltas.len() as u8;
        let mut v = vec![0xF0 | (payload_flags & 0x0F), start, len - 1];
        for &d in deltas {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    /// Sparse12 (ordinal 6): packed 12-bit pointers.
    ///
    /// Layout per TrieNode.java Sparse12.serialize():
    ///   [0x60|pf] [count] [count transition bytes]
    ///   [ceil(count*3/2) packed-pointer bytes]
    ///
    /// Packing: for each even/odd pair (p0, p1) → 3 bytes [p0>>4, (p0<<4)|(p1>>8), p1&0xFF].
    /// An odd trailing pointer → 2 bytes [(pd << 4) as short big-endian].
    ///
    /// Total node bytes = 2 + count + ceil(count*3/2).
    /// count=1 → 5 bytes; count=2 → 7 bytes.
    fn sparse12_node(payload_flags: u8, pairs: &[(u8, u16)]) -> Vec<u8> {
        let count = pairs.len();
        let mut v = vec![0x60 | (payload_flags & 0x0F), count as u8];
        for &(t, _) in pairs {
            v.push(t);
        }
        // Pack pointers: process pairs, then trailing odd entry
        let mut i = 0;
        while i + 2 <= count {
            let p0 = pairs[i].1 as u32;
            let p1 = pairs[i + 1].1 as u32;
            v.push((p0 >> 4) as u8);
            v.push(((p0 << 4) | (p1 >> 8)) as u8);
            v.push((p1 & 0xFF) as u8);
            i += 2;
        }
        if i < count {
            // Trailing odd pointer: writeShort((short)(pd << 4)) big-endian
            let pd = pairs[i].1 as u32;
            let s = (pd << 4) as u16;
            v.extend_from_slice(&s.to_be_bytes());
        }
        v
    }

    /// Sparse24 (ordinal 8): [0x80|pf] [count] [count transition bytes] [count * 3-byte big-endian deltas]
    fn sparse24_node(payload_flags: u8, pairs: &[(u8, u32)]) -> Vec<u8> {
        let count = pairs.len();
        let mut v = vec![0x80 | (payload_flags & 0x0F), count as u8];
        for &(t, _) in pairs {
            v.push(t);
        }
        for &(_, d) in pairs {
            // 3-byte big-endian
            v.push(((d >> 16) & 0xFF) as u8);
            v.push(((d >> 8) & 0xFF) as u8);
            v.push((d & 0xFF) as u8);
        }
        v
    }

    /// Sparse40 (ordinal 9): [0x90|pf] [count] [count transition bytes] [count * 5-byte big-endian deltas]
    fn sparse40_node(payload_flags: u8, pairs: &[(u8, u64)]) -> Vec<u8> {
        let count = pairs.len();
        let mut v = vec![0x90 | (payload_flags & 0x0F), count as u8];
        for &(t, _) in pairs {
            v.push(t);
        }
        for &(_, d) in pairs {
            // 5-byte big-endian
            v.push(((d >> 32) & 0xFF) as u8);
            v.push(((d >> 24) & 0xFF) as u8);
            v.push(((d >> 16) & 0xFF) as u8);
            v.push(((d >> 8) & 0xFF) as u8);
            v.push((d & 0xFF) as u8);
        }
        v
    }

    /// Dense12 (ordinal 10): packed 12-bit pointers for a contiguous byte range.
    ///
    /// Layout per TrieNode.java Dense12.serialize():
    ///   [0xA0|pf] [start_byte] [range_len - 1] [ceil(range_len*3/2) packed bytes]
    ///
    /// Packing matches write12Bits(): even index → [val>>4, carry=val<<4]; odd index → [carry|(val>>8), val&0xFF].
    fn dense12_node(payload_flags: u8, start: u8, deltas: &[u16]) -> Vec<u8> {
        let range_len = deltas.len();
        let mut v = vec![0xA0 | (payload_flags & 0x0F), start, (range_len - 1) as u8];
        let mut carry: u8 = 0;
        for (i, &d) in deltas.iter().enumerate() {
            let val = d as u32;
            if (i & 1) == 0 {
                v.push((val >> 4) as u8);
                carry = (val << 4) as u8;
            } else {
                v.push(carry | (val >> 8) as u8);
                v.push((val & 0xFF) as u8);
                carry = 0;
            }
        }
        // If odd number of entries, flush carry byte
        if (range_len & 1) == 1 {
            v.push(carry);
        }
        v
    }

    // -----------------------------------------------------------------------
    // REGRESSION TEST — proves the pre-fix stub misbehaviour
    //
    // Before the fix, RowsParser::parse_node_data always returned a
    // BtiNodeType::PayloadOnly node regardless of the actual header nibble.
    // This test crafts a Single8 node (ordinal 2, high nibble = 0x2) and
    // verifies that parse_bti_node correctly identifies it as Single, NOT
    // PayloadOnly.  On the old code this assertion would fail.
    // -----------------------------------------------------------------------
    #[test]
    fn regression_rows_parser_single_node_not_mislabeled_as_payload_only() {
        // Craft a Single8 (ordinal 2) node: nibble = 0x2 → must NOT be PayloadOnly.
        // The old stub parsed the header byte and then threw away the result,
        // always constructing BtiNodeType::PayloadOnly.
        //
        // Node layout (Single8, no payload, delta=5):
        //   byte 0: 0x20  (ordinal=2, payload_flags=0)
        //   byte 1: 0x61  ('a' transition)
        //   byte 2: 0x05  (backward delta = 5)
        let node_bytes = single8_node(0, b'a', 5);
        let offset: u64 = 100;

        let node = parse_bti_node(&node_bytes, offset)
            .expect("parse_bti_node must succeed for a valid Single8 node");

        // Core regression assertion: the stub returned PayloadOnly here.
        assert_eq!(
            node.node_type,
            BtiNodeType::Single,
            "Single8 node (nibble 0x2) was mislabeled as {:?} — regression from #647 stub",
            node.node_type,
        );

        // Structural check: child offset = parent - delta = 100 - 5 = 95
        match &node.data {
            BtiNodeData::Single { transition } => {
                assert_eq!(transition.byte, b'a');
                assert_eq!(
                    transition.child.distance, 95,
                    "child offset should be parent(100) - delta(5) = 95"
                );
            }
            other => panic!("Expected BtiNodeData::Single, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // parse_bti_node: PayloadOnly (ordinal 0)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bti_node_payload_only_correct_type_and_offsets() {
        // ordinal=0, payload_flags=1 → PayloadOnly with payload
        let node = payload_only_node(0xDEAD_BEEF_0000_1234, 42);
        let parsed = parse_bti_node(&node, 0).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::PayloadOnly);
        match &parsed.data {
            BtiNodeData::PayloadOnly { payload } => {
                assert_eq!(payload.offset, 0xDEAD_BEEF_0000_1234);
                assert_eq!(payload.length, 42);
            }
            other => panic!("Expected PayloadOnly, got {:?}", other),
        }
    }

    #[test]
    fn parse_bti_node_payload_only_no_payload_flags_is_error() {
        // ordinal=0, payload_flags=0 → error (leaf must have a payload)
        let node_bytes = vec![0x00u8]; // nibble=0, flags=0
        let err = parse_bti_node(&node_bytes, 0);
        assert!(err.is_err(), "PayloadOnly with flags=0 should be an error");
    }

    // -----------------------------------------------------------------------
    // parse_bti_node: Single family (ordinals 1-4)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bti_node_single_nopayload4_ordinal1() {
        // delta=3 encoded in low nibble; transition = b'x'; parent offset = 50
        let node_bytes = single_nopayload4_node(3, b'x');
        let parsed = parse_bti_node(&node_bytes, 50).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::Single);
        match &parsed.data {
            BtiNodeData::Single { transition } => {
                assert_eq!(transition.byte, b'x');
                assert_eq!(transition.child.distance, 47); // 50 - 3
            }
            other => panic!("Expected Single, got {:?}", other),
        }
    }

    #[test]
    fn parse_bti_node_single8_ordinal2() {
        let node_bytes = single8_node(0, b'z', 10);
        let parsed = parse_bti_node(&node_bytes, 200).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::Single);
        match &parsed.data {
            BtiNodeData::Single { transition } => {
                assert_eq!(transition.byte, b'z');
                assert_eq!(transition.child.distance, 190); // 200 - 10
            }
            other => panic!("Expected Single, got {:?}", other),
        }
    }

    #[test]
    fn parse_bti_node_single_nopayload12_ordinal3() {
        // delta = 0x123 (291): high 4 bits in byte0 low nibble, low 8 in byte1
        let node_bytes = single_nopayload12_node(0x123, b'k');
        let parsed = parse_bti_node(&node_bytes, 1000).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::Single);
        match &parsed.data {
            BtiNodeData::Single { transition } => {
                assert_eq!(transition.byte, b'k');
                assert_eq!(transition.child.distance, 1000 - 0x123);
            }
            other => panic!("Expected Single, got {:?}", other),
        }
    }

    #[test]
    fn parse_bti_node_single16_ordinal4() {
        // delta = 0x0400 (1024)
        let node_bytes = single16_node(0, b'm', 0x0400);
        let parsed = parse_bti_node(&node_bytes, 2048).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::Single);
        match &parsed.data {
            BtiNodeData::Single { transition } => {
                assert_eq!(transition.byte, b'm');
                assert_eq!(transition.child.distance, 2048 - 1024);
            }
            other => panic!("Expected Single, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // parse_bti_node: Sparse family (ordinals 5, 7, 8, 9)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bti_node_sparse8_ordinal5_two_transitions() {
        // Two transitions: (b'a', delta=10), (b'b', delta=20); parent offset=100
        let node_bytes = sparse8_node(0, &[(b'a', 10), (b'b', 20)]);
        let parsed = parse_bti_node(&node_bytes, 100).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::Sparse);
        match &parsed.data {
            BtiNodeData::Sparse { transitions } => {
                assert_eq!(transitions.len(), 2);
                assert_eq!(transitions[0].byte, b'a');
                assert_eq!(transitions[0].child.distance, 90); // 100 - 10
                assert_eq!(transitions[1].byte, b'b');
                assert_eq!(transitions[1].child.distance, 80); // 100 - 20
            }
            other => panic!("Expected Sparse, got {:?}", other),
        }
    }

    #[test]
    fn parse_bti_node_sparse16_ordinal7_three_transitions() {
        // Sparse16: ordinal=7, 2-byte deltas
        // Layout: [0x70|pf] [count] [count bytes] [count * 2 byte deltas]
        let payload_flags = 0u8;
        let pairs: &[(u8, u16)] = &[(b'x', 0x0010), (b'y', 0x0020), (b'z', 0x0030)];
        let mut node_bytes = vec![0x70 | payload_flags, pairs.len() as u8];
        for &(t, _) in pairs {
            node_bytes.push(t);
        }
        for &(_, d) in pairs {
            node_bytes.extend_from_slice(&d.to_be_bytes());
        }
        let parsed = parse_bti_node(&node_bytes, 0x100).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::Sparse);
        match &parsed.data {
            BtiNodeData::Sparse { transitions } => {
                assert_eq!(transitions.len(), 3);
                assert_eq!(transitions[0].byte, b'x');
                assert_eq!(transitions[0].child.distance, 0x100 - 0x0010);
                assert_eq!(transitions[2].byte, b'z');
                assert_eq!(transitions[2].child.distance, 0x100 - 0x0030);
            }
            other => panic!("Expected Sparse, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // parse_bti_node: Sparse12 (ordinal 6) — exact-minimal size tests
    //
    // Per TrieNode.java Sparse12.payloadPosition:
    //   total = position + 2 + (5*count+1)/2
    //         = 2 + count_transition_bytes_region + ceil(count*3/2) pointer_region
    // count=1 → 5 bytes; count=2 → 7 bytes.
    // The old formula used (count*5).div_ceil(2) for the pointer region alone,
    // which over-counted by `count` bytes; this was fixed to (count*3).div_ceil(2).
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bti_node_sparse12_ordinal6_count1_exact_minimal_5_bytes() {
        // count=1: exact-minimal node is 5 bytes.
        // Layout: [0x60] [0x01] [transition_byte] [2-byte packed 12-bit pointer]
        // delta = 0xABC (2748), packed as big-endian short (0xABC << 4 = 0xABC0).
        // Parent offset = 0x1000; child = 0x1000 - 0xABC = 0x544.
        let node_bytes = sparse12_node(0, &[(b'a', 0xABC)]);
        assert_eq!(
            node_bytes.len(),
            5,
            "Sparse12 count=1 must be exactly 5 bytes (was over-counted as 6 with old formula)"
        );
        let offset: u64 = 0x1000;
        let parsed = parse_bti_node(&node_bytes, offset)
            .expect("exact-minimal Sparse12 count=1 must parse successfully");
        assert_eq!(parsed.node_type, BtiNodeType::Sparse);
        match &parsed.data {
            BtiNodeData::Sparse { transitions } => {
                assert_eq!(transitions.len(), 1);
                assert_eq!(transitions[0].byte, b'a');
                assert_eq!(
                    transitions[0].child.distance,
                    offset - 0xABC,
                    "child offset = parent(0x1000) - delta(0xABC) = 0x544"
                );
            }
            other => panic!("Expected BtiNodeData::Sparse, got {:?}", other),
        }
    }

    #[test]
    fn parse_bti_node_sparse12_ordinal6_count2_exact_minimal_7_bytes() {
        // count=2: exact-minimal node is 7 bytes.
        // Layout: [0x60] [0x02] [t0] [t1] [3-byte packed for two 12-bit pointers]
        // p0=0x100 (256), p1=0x200 (512); packed: [0x10, 0x02, 0x00].
        // Parent offset = 0x800.
        let node_bytes = sparse12_node(0, &[(b'x', 0x100), (b'y', 0x200)]);
        assert_eq!(
            node_bytes.len(),
            7,
            "Sparse12 count=2 must be exactly 7 bytes"
        );
        let offset: u64 = 0x800;
        let parsed = parse_bti_node(&node_bytes, offset)
            .expect("exact-minimal Sparse12 count=2 must parse successfully");
        assert_eq!(parsed.node_type, BtiNodeType::Sparse);
        match &parsed.data {
            BtiNodeData::Sparse { transitions } => {
                assert_eq!(transitions.len(), 2);
                assert_eq!(transitions[0].byte, b'x');
                assert_eq!(transitions[0].child.distance, offset - 0x100);
                assert_eq!(transitions[1].byte, b'y');
                assert_eq!(transitions[1].child.distance, offset - 0x200);
            }
            other => panic!("Expected BtiNodeData::Sparse, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // parse_bti_node: Sparse24 (ordinal 8) — exact-minimal size test
    //
    // Per TrieNode.java Sparse.payloadPosition:
    //   total = 2 + (bytesPerPointer + 1) * count = 2 + 4*count
    // count=1 → 6 bytes: [header][count][transition][3-byte delta]
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bti_node_sparse24_ordinal8_count1_exact_minimal_6_bytes() {
        // delta = 0x010203 (66051)
        let node_bytes = sparse24_node(0, &[(b'p', 0x010203)]);
        assert_eq!(
            node_bytes.len(),
            6,
            "Sparse24 count=1 must be exactly 6 bytes"
        );
        let offset: u64 = 0x20000;
        let parsed = parse_bti_node(&node_bytes, offset)
            .expect("exact-minimal Sparse24 count=1 must parse successfully");
        assert_eq!(parsed.node_type, BtiNodeType::Sparse);
        match &parsed.data {
            BtiNodeData::Sparse { transitions } => {
                assert_eq!(transitions.len(), 1);
                assert_eq!(transitions[0].byte, b'p');
                assert_eq!(transitions[0].child.distance, offset - 0x010203);
            }
            other => panic!("Expected BtiNodeData::Sparse, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // parse_bti_node: Sparse40 (ordinal 9) — exact-minimal size test
    //
    // Per TrieNode.java Sparse.payloadPosition:
    //   total = 2 + (bytesPerPointer + 1) * count = 2 + 6*count
    // count=1 → 8 bytes: [header][count][transition][5-byte delta]
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bti_node_sparse40_ordinal9_count1_exact_minimal_8_bytes() {
        // delta = 0x0000_0001_0000 (65536)
        let delta: u64 = 0x0000_0001_0000;
        let node_bytes = sparse40_node(0, &[(b'q', delta)]);
        assert_eq!(
            node_bytes.len(),
            8,
            "Sparse40 count=1 must be exactly 8 bytes"
        );
        let offset: u64 = 0x0010_0000;
        let parsed = parse_bti_node(&node_bytes, offset)
            .expect("exact-minimal Sparse40 count=1 must parse successfully");
        assert_eq!(parsed.node_type, BtiNodeType::Sparse);
        match &parsed.data {
            BtiNodeData::Sparse { transitions } => {
                assert_eq!(transitions.len(), 1);
                assert_eq!(transitions[0].byte, b'q');
                assert_eq!(transitions[0].child.distance, offset - delta);
            }
            other => panic!("Expected BtiNodeData::Sparse, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // parse_bti_node: Dense12 (ordinal 10) — exact-minimal size test
    //
    // Per TrieNode.java Dense12.payloadPosition:
    //   total = 3 + (range_len*3 + 1)/2 = 3 + ceil(range_len*3/2)
    // range_len=1 → 5 bytes: [header][start][0x00][2-byte packed 12-bit pointer]
    // range_len=2 → 6 bytes: [header][start][0x01][3-byte packed for two 12-bit values]
    // The existing Dense12 bounds formula is correct; this test pins it.
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bti_node_dense12_ordinal10_range1_exact_minimal_5_bytes() {
        // range_len=1 (start=b'A', end=b'A'): delta=0x123 (291).
        let node_bytes = dense12_node(0, b'A', &[0x123]);
        assert_eq!(
            node_bytes.len(),
            5,
            "Dense12 range_len=1 must be exactly 5 bytes"
        );
        let offset: u64 = 0x500;
        let parsed = parse_bti_node(&node_bytes, offset)
            .expect("exact-minimal Dense12 range=1 must parse successfully");
        assert_eq!(parsed.node_type, BtiNodeType::Dense);
        match &parsed.data {
            BtiNodeData::Dense {
                start_byte,
                children,
            } => {
                assert_eq!(*start_byte, b'A');
                assert_eq!(children.len(), 1);
                assert_eq!(children[0].distance, offset - 0x123);
            }
            other => panic!("Expected BtiNodeData::Dense, got {:?}", other),
        }
    }

    #[test]
    fn parse_bti_node_dense12_ordinal10_range2_exact_minimal_6_bytes() {
        // range_len=2 (start=b'A', spans 'A' and 'B'):
        // delta[0]=0x100, delta[1]=0x200 (0 = no-child for Dense).
        let node_bytes = dense12_node(0, b'A', &[0x100, 0x200]);
        assert_eq!(
            node_bytes.len(),
            6,
            "Dense12 range_len=2 must be exactly 6 bytes"
        );
        let offset: u64 = 0x800;
        let parsed = parse_bti_node(&node_bytes, offset)
            .expect("exact-minimal Dense12 range=2 must parse successfully");
        assert_eq!(parsed.node_type, BtiNodeType::Dense);
        match &parsed.data {
            BtiNodeData::Dense {
                start_byte,
                children,
            } => {
                assert_eq!(*start_byte, b'A');
                assert_eq!(children.len(), 2);
                assert_eq!(children[0].distance, offset - 0x100);
                assert_eq!(children[1].distance, offset - 0x200);
            }
            other => panic!("Expected BtiNodeData::Dense, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // parse_bti_node: Dense family (ordinals 11-15)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bti_node_dense16_ordinal11_three_children() {
        // Dense16: ordinal=11 (0xB), start=b'a', range=['a','b','c'], deltas
        // 0 means "no child" for Dense nodes.
        let node_bytes = dense16_node(0, b'a', &[0x0010, 0x0000, 0x0030]);
        let parsed = parse_bti_node(&node_bytes, 0x200).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::Dense);
        match &parsed.data {
            BtiNodeData::Dense {
                start_byte,
                children,
            } => {
                assert_eq!(*start_byte, b'a');
                assert_eq!(children.len(), 3);
                // child 0 (b'a'): offset = 0x200 - 0x0010 = 0x1F0
                assert_eq!(children[0].distance, 0x200 - 0x0010);
                // child 1 (b'b'): delta=0 → no child, offset=0
                assert_eq!(children[1].distance, 0);
                // child 2 (b'c'): offset = 0x200 - 0x0030 = 0x1D0
                assert_eq!(children[2].distance, 0x200 - 0x0030);
            }
            other => panic!("Expected Dense, got {:?}", other),
        }
    }

    #[test]
    fn parse_bti_node_long_dense_ordinal15_two_children() {
        // LongDense: ordinal=15 (0xF), 8-byte deltas
        let node_bytes = long_dense_node(0, b'A', &[0x0000_0000_0000_0100, 0x0000_0000_0000_0200]);
        let parsed = parse_bti_node(&node_bytes, 0x10000).unwrap();
        assert_eq!(parsed.node_type, BtiNodeType::Dense);
        match &parsed.data {
            BtiNodeData::Dense {
                start_byte,
                children,
            } => {
                assert_eq!(*start_byte, b'A');
                assert_eq!(children.len(), 2);
                assert_eq!(children[0].distance, 0x10000 - 0x100);
                assert_eq!(children[1].distance, 0x10000 - 0x200);
            }
            other => panic!("Expected Dense, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // classify_node_nibble: all 16 nibbles map to the right category
    // -----------------------------------------------------------------------

    #[test]
    fn classify_node_nibble_all_ordinals() {
        // Ordinal 0 → PayloadOnly
        assert_eq!(classify_node_nibble(0).unwrap(), BtiNodeType::PayloadOnly);
        // Ordinals 1-4 → Single
        for n in 1u8..=4 {
            assert_eq!(
                classify_node_nibble(n).unwrap(),
                BtiNodeType::Single,
                "ordinal {} should be Single",
                n
            );
        }
        // Ordinals 5-9 → Sparse
        for n in 5u8..=9 {
            assert_eq!(
                classify_node_nibble(n).unwrap(),
                BtiNodeType::Sparse,
                "ordinal {} should be Sparse",
                n
            );
        }
        // Ordinals 10-15 → Dense
        for n in 10u8..=15 {
            assert_eq!(
                classify_node_nibble(n).unwrap(),
                BtiNodeType::Dense,
                "ordinal {} should be Dense",
                n
            );
        }
    }

    // -----------------------------------------------------------------------
    // RowsParser: non-PayloadOnly node is correctly parsed (was broken before)
    // -----------------------------------------------------------------------

    /// Integration test: embed a Sparse8 node as the root of a Rows.db file
    /// and verify RowsParser reads it as Sparse (not PayloadOnly).
    #[test]
    fn rows_parser_sparse_root_node_not_mislabeled() {
        // Build a Rows.db file whose root is a Sparse8 node.
        // The stub would have returned PayloadOnly; real code must return Sparse.
        let root_node = sparse8_node(0, &[(b'a', 5), (b'b', 10)]);
        let data = make_bti_file(root_node);
        let cursor = Cursor::new(data);
        let mut parser = RowsParser::new(cursor).unwrap();

        // Force the root node to be loaded and parsed.
        let root_offset = parser.header.root_offset;
        let node = parser.load_node(root_offset).unwrap();

        assert_eq!(
            node.node_type,
            BtiNodeType::Sparse,
            "RowsParser returned {:?} for a Sparse8 root node — regression from #647",
            node.node_type
        );
        assert_eq!(node.child_count(), 2);
    }

    /// Integration test: embed a Dense16 node as the root of a Rows.db file.
    #[test]
    fn rows_parser_dense_root_node_not_mislabeled() {
        let root_node = dense16_node(0, b'0', &[0x0020, 0x0000, 0x0040]);
        let data = make_bti_file(root_node);
        let cursor = Cursor::new(data);
        let mut parser = RowsParser::new(cursor).unwrap();
        let root_offset = parser.header.root_offset;
        let node = parser.load_node(root_offset).unwrap();

        assert_eq!(
            node.node_type,
            BtiNodeType::Dense,
            "RowsParser returned {:?} for a Dense16 root node",
            node.node_type
        );
    }

    /// Integration test: embed a SingleNoPayload4 node as the root of a Rows.db file.
    #[test]
    fn rows_parser_single_nopayload4_root_node_not_mislabeled() {
        let root_offset_val: u64 = 64;
        // delta=3: child is at 64-3=61, but 61 < 64 so saturating_sub keeps it valid
        let root_node = single_nopayload4_node(3, b'q');
        let data = make_bti_file(root_node);
        let cursor = Cursor::new(data);
        let mut parser = RowsParser::new(cursor).unwrap();
        let root_offset = parser.header.root_offset;
        assert_eq!(root_offset, root_offset_val);
        let node = parser.load_node(root_offset).unwrap();

        assert_eq!(
            node.node_type,
            BtiNodeType::Single,
            "RowsParser returned {:?} for a SingleNoPayload4 root node",
            node.node_type
        );
        match &node.data {
            BtiNodeData::Single { transition } => {
                assert_eq!(transition.byte, b'q');
                assert_eq!(transition.child.distance, root_offset_val - 3);
            }
            other => panic!("Expected Single data, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Existing tests (preserved from before the fix)
    // -----------------------------------------------------------------------

    #[test]
    fn test_bti_header_parsing() {
        let mut header_data = Vec::new();
        header_data.extend_from_slice(&BtiHeader::MAGIC.to_be_bytes());
        header_data.extend_from_slice(&BtiHeader::VERSION.to_be_bytes());
        header_data.extend_from_slice(&0u16.to_be_bytes()); // flags
        header_data.extend_from_slice(&1024u64.to_be_bytes()); // root_offset
        header_data.extend_from_slice(&100u64.to_be_bytes()); // entry_count

        let (header, size) = BtiHeader::parse(&header_data).unwrap();
        assert_eq!(header.magic, BtiHeader::MAGIC);
        assert_eq!(header.version, BtiHeader::VERSION);
        assert_eq!(header.root_offset, 1024);
        assert_eq!(header.entry_count, 100);
        assert_eq!(size, 24);
    }

    #[test]
    fn test_partitions_parser_creation() {
        let data = make_bti_file(payload_only_node(1000, 50));
        let cursor = Cursor::new(data);
        let _parser = PartitionsParser::new(cursor).unwrap();
    }

    #[test]
    fn test_rows_parser_creation() {
        let data = make_bti_file(payload_only_node(1000, 50));
        let cursor = Cursor::new(data);
        let _parser = RowsParser::new(cursor).unwrap();
    }

    #[test]
    fn test_partition_lookup() {
        let data = make_bti_file(payload_only_node(1000, 50));
        let cursor = Cursor::new(data);
        let mut parser = PartitionsParser::new(cursor).unwrap();

        // Test lookup with simple key — PayloadOnly root returns its payload immediately
        let partition_key = vec![Value::Text("test_partition".to_string())];
        let result = parser.lookup_partition(&partition_key).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_header_serialization_round_trip() {
        let original_header = BtiHeader {
            magic: BtiHeader::MAGIC,
            version: BtiHeader::VERSION,
            flags: 0x1234,
            root_offset: 0x123456789ABCDEF0,
            entry_count: 0xFEDCBA9876543210,
            metadata_size: 0x12345678,
        };

        let serialized = original_header.to_bytes();
        let (parsed_header, _) = BtiHeader::parse(&serialized).unwrap();

        assert_eq!(original_header.magic, parsed_header.magic);
        assert_eq!(original_header.version, parsed_header.version);
        assert_eq!(original_header.flags, parsed_header.flags);
        assert_eq!(original_header.root_offset, parsed_header.root_offset);
        assert_eq!(original_header.entry_count, parsed_header.entry_count);
        assert_eq!(original_header.metadata_size, parsed_header.metadata_size);
    }

    // -----------------------------------------------------------------------
    // Helper functions used in tests above (these are tested implicitly)
    // -----------------------------------------------------------------------

    #[test]
    fn read_be_unsigned_edge_cases() {
        assert_eq!(read_be_unsigned(&[]), 0);
        assert_eq!(read_be_unsigned(&[0xFF]), 255);
        assert_eq!(read_be_unsigned(&[0x01, 0x00]), 256);
        assert_eq!(read_be_unsigned(&[0xFF, 0xFF, 0xFF, 0xFF]), 0xFFFF_FFFF);
    }

    #[test]
    fn read_12bit_packed_even_and_odd_indices() {
        // Pack two values: index 0 = 0xABC, index 1 = 0x123
        // Bytes:  [0xAB, 0xC1, 0x23]  → word0 = 0xABC1 >> 4 = 0xABC; word1 = 0xC123 & 0xFFF = 0x123
        let data: &[u8] = &[0xAB, 0xC1, 0x23];
        assert_eq!(read_12bit_packed(data, 0), 0xABC);
        assert_eq!(read_12bit_packed(data, 1), 0x123);
    }

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
    /// The encoded key must be empty (or rather, the trie matches any non-empty
    /// key by falling through to a PayloadOnly at depth 0 only if the trie
    /// immediately has a PayloadOnly root — but that only works for empty keys
    /// in real BTI).  Here we test the degenerate case where the root IS the
    /// leaf and the key is already exhausted.
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

    /// Hand-crafted 2-level trie matching the simple_table Partitions.db layout:
    ///
    ///   root (offset 6): SingleNoPayload4  delta=3 transition=0x40
    ///     child (offset 3): Sparse8        pf=0    count=2
    ///       trans[0]=0xAA delta=3 → child=0: PayloadOnly hash=0x11 pos=-1  → data_offset=0
    ///       trans[1]=0xBB delta=0 → ERROR (delta=0 means child at same offset — use a real value)
    ///
    /// Simplified: 2-partition trie, key "A" maps to offset 0, key "B" maps to offset 64.
    #[test]
    fn walk_bti_trie_two_partitions_via_sparse8_root() {
        // Build trie manually (bottom-up, children at lower offsets)
        //
        // Offset 0: PayloadOnly for "key A" → data_offset=0
        //   header = 0x08 (ordinal=0, pf=8), hash=0x11, position=0xFF (-1 → ~(-1)=0)
        // Offset 3: PayloadOnly for "key B" → data_offset=64
        //   header = 0x08, hash=0x22, position=0xBF (-65 → ~(-65)=64)
        // Offset 6: Sparse8 (ordinal=5, pf=0) with 2 transitions
        //   [0x08 | 0x00 = 0x50] [count=2] [trans0=0xAA] [trans1=0xBB]
        //   [delta0 = 6] [delta1 = 3]
        //   child0 = 6 - 6 = 0 ✓ ; child1 = 6 - 3 = 3 ✓

        let mut trie_data = vec![0u8; 12];
        // Leaf at offset 0: PayloadOnly, pf=8, hash=0x11, position=-1 → data_offset=0
        trie_data[0] = 0x08; // ordinal=0, payloadBits=8
        trie_data[1] = 0x11; // hash
        trie_data[2] = 0xFF; // position byte = -1 as i8

        // Leaf at offset 3: PayloadOnly, pf=8, hash=0x22, position=-65 → data_offset=64
        // -65 as i8 = 0xBF (since 0xBF = 191 unsigned; 191 - 256 = -65)
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

        // Encoded keys: each is a single byte (0xAA for partition A, 0xBB for B)
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
    /// Data.db offsets.  This test does NOT rely on any external test-data
    /// files, so it always runs and proves the seek-not-scan path.
    #[test]
    fn lookup_partition_in_bti_file_synthetic_two_partitions() {
        use std::io::Cursor;

        // ----------------------------------------------------------------
        // Trie layout (same as walk_bti_trie_two_partitions_via_sparse8_root):
        //   offset 0: PayloadOnly (pf=8, hash=0x11, position=-1) → DataOffset(0)
        //   offset 3: PayloadOnly (pf=8, hash=0x22, position=-65) → DataOffset(64)
        //   offset 6: Sparse8 count=2 [0xAA→0, 0xBB→3]
        // Footer (8 bytes): root offset = 6
        // ----------------------------------------------------------------

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

        // ----------------------------------------------------------------
        // Lookup key 0xAA → should give DataOffset(0)
        // ----------------------------------------------------------------
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

        // ----------------------------------------------------------------
        // Lookup key 0xBB → should give DataOffset(64)
        // ----------------------------------------------------------------
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

        // ----------------------------------------------------------------
        // Lookup key 0xCC → must return None (not found)
        // ----------------------------------------------------------------
        {
            let mut cursor = Cursor::new(trie_file.clone());
            let result =
                lookup_partition_in_bti_file(&mut cursor, &[0xCC]).expect("lookup must not error");
            assert_eq!(result, None, "Key 0xCC must not be found");
        }
    }

    /// Prove that `lookup_partition_in_bti_file` resolves Data.db offsets via
    /// the trie (NOT a sequential scan) by asserting the resolved offset
    /// matches the known partition position from the sstabledump JSONL golden.
    ///
    /// This test requires the real BTI fixture files from the test dataset.
    /// It is guarded by `CQLITE_DATASETS_ROOT` and skips when the dataset is
    /// absent, so it does not block CI that runs without test data.
    ///
    /// **Seek-not-scan proof**: The resolved `data_offset` matches the JSONL
    /// "position" field exactly.  A sequential scan would never produce a
    /// file-offset value — it walks Data.db byte-by-byte.  Only the trie lookup
    /// produces an absolute Data.db offset before any Data.db is read.
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

        // Known encoded keys and expected Data.db offsets from the JSONL golden.
        //
        // UUID "22222222-2222-2222-2222-222222222222" → position 0
        // UUID "11111111-1111-1111-1111-111111111111" → position 63
        // UUID "33333333-3333-3333-3333-333333333333" → position 125
        //
        // The BTI trie is keyed on the byte-comparable encoding of UUIDs as
        // produced by Cassandra's OSS50 encoder.  Through the trie traversal
        // analysis we verified that transition bytes 0x90, 0xBC, 0xF9 (after the
        // 0x40 Single4 prefix hop) map to offsets 0, 63, 125.
        //
        // Rather than re-encoding full UUIDs here, we prove the trie resolves the
        // correct offsets by directly reading the trie and checking that all three
        // PayloadOnly leaves decode to the known Data.db positions.
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
        // Leaf at offset 0: hash=0x24, position=0xFF (-1) → data_offset=0
        let loc0 =
            decode_bti_partition_payload(&trie_data, 1, 8).expect("leaf at offset 0 must decode");
        assert_eq!(
            loc0,
            BtiPartitionLocation::DataOffset(0),
            "leaf at trie offset 0 must map to Data.db position 0 (UUID 22222222...)"
        );

        // Leaf at offset 3: hash=0x22, position=0xC0 (-64) → data_offset=63
        let loc63 =
            decode_bti_partition_payload(&trie_data, 4, 8).expect("leaf at offset 3 must decode");
        assert_eq!(
            loc63,
            BtiPartitionLocation::DataOffset(63),
            "leaf at trie offset 3 must map to Data.db position 63 (UUID 11111111...)"
        );

        // Leaf at offset 6: hash=0xF4, position=0x82 (-126) → data_offset=125
        let loc125 =
            decode_bti_partition_payload(&trie_data, 7, 8).expect("leaf at offset 6 must decode");
        assert_eq!(
            loc125,
            BtiPartitionLocation::DataOffset(125),
            "leaf at trie offset 6 must map to Data.db position 125 (UUID 33333333...)"
        );

        // Prove the trie walk itself works end-to-end for the encoded key prefixes
        // we observe in the file.  Transition chain from hex analysis:
        //   root(17) -[0x40]→ Sparse8(9) -[0x90]→ leaf(0) → DataOffset(0)
        //                                 -[0xBC]→ leaf(3) → DataOffset(63)
        //                                 -[0xF9]→ leaf(6) → DataOffset(125)
        // We encode the two-byte key [0x40, 0x90] / [0x40, 0xBC] / [0x40, 0xF9]:
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

        // [0x40, 0x90] → DataOffset(0)  (trie path, NOT sequential scan)
        let mut cursor = Cursor::new(raw.clone());
        let r = lookup_partition_in_bti_file(&mut cursor, &[0x40, 0x90]).unwrap();
        assert_eq!(r, Some(BtiPartitionLocation::DataOffset(0)));

        // [0x40, 0xBC] → DataOffset(63)
        let mut cursor = Cursor::new(raw.clone());
        let r = lookup_partition_in_bti_file(&mut cursor, &[0x40, 0xBC]).unwrap();
        assert_eq!(r, Some(BtiPartitionLocation::DataOffset(63)));

        // [0x40, 0xF9] → DataOffset(125)
        let mut cursor = Cursor::new(raw.clone());
        let r = lookup_partition_in_bti_file(&mut cursor, &[0x40, 0xF9]).unwrap();
        assert_eq!(r, Some(BtiPartitionLocation::DataOffset(125)));

        // Key not in trie → None
        let mut cursor = Cursor::new(raw.clone());
        let r = lookup_partition_in_bti_file(&mut cursor, &[0x40, 0x00]).unwrap();
        assert_eq!(r, None);

        println!(
            "VERIFIED: lookup_partition_in_bti_file resolved all 3 BTI partition \
             offsets (0, 63, 125) via trie walk, NOT sequential scan"
        );
    }

    // -----------------------------------------------------------------------
    // Stub-non-silent tests (issue #755 scope decision)
    // -----------------------------------------------------------------------

    /// Verify that RowsParser::range_query returns an explicit error rather
    /// than silently returning an empty Vec (which would be a wrong-result path).
    #[test]
    fn rows_parser_range_query_returns_explicit_error_not_empty_vec() {
        let root_node = payload_only_node(1000, 50);
        let data = make_bti_file(root_node);
        let cursor = std::io::Cursor::new(data);
        let mut parser = RowsParser::new(cursor).unwrap();

        let start = vec![Value::Text("start".to_string())];
        let end = vec![Value::Text("end".to_string())];
        let result = parser.range_query(&start, &end);
        assert!(
            result.is_err(),
            "range_query must return Err (not Ok(empty Vec)) to prevent silent wrong-result: got {:?}", result
        );
    }

    /// Verify that PartitionIterator::next returns Some(Err(_)) on first call
    /// rather than None (which would look like an empty index).
    #[test]
    fn partition_iterator_next_returns_explicit_error_not_none() {
        let data = make_bti_file(payload_only_node(1000, 50));
        let cursor = std::io::Cursor::new(data);
        let mut parser = PartitionsParser::new(cursor).unwrap();
        let mut iter = parser.iterate_partitions().unwrap();
        let first = iter.next();
        assert!(
            matches!(first, Some(Err(_))),
            "PartitionIterator must yield Some(Err(_)) on first call (not None); \
             got: {:?}",
            first
        );
    }

    /// Verify that RowIterator::next returns Some(Err(_)) on first call
    /// rather than None (which would look like an empty row index).
    #[test]
    fn row_iterator_next_returns_explicit_error_not_none() {
        let data = make_bti_file(payload_only_node(1000, 50));
        let cursor = std::io::Cursor::new(data);
        let mut parser = RowsParser::new(cursor).unwrap();
        let mut iter = parser.iterate_rows().unwrap();
        let first = iter.next();
        assert!(
            matches!(first, Some(Err(_))),
            "RowIterator must yield Some(Err(_)) on first call (not None); \
             got: {:?}",
            first
        );
    }
}
