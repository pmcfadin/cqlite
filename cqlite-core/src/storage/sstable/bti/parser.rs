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
                    // delta == 0 is the "no transition" sentinel → None.  Any
                    // other delta is a real child at `offset - delta` (which may
                    // be absolute offset 0 — a legitimate child, distinct from
                    // the sentinel).
                    let child = if delta == 0 {
                        None
                    } else {
                        Some(SizedPointer::new(offset.saturating_sub(delta)))
                    };
                    children.push(child);
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
                    // delta == 0 is the "no transition" sentinel → None.  Any
                    // other delta is a real child at `offset - delta` (which may
                    // be absolute offset 0 — a legitimate child, distinct from
                    // the sentinel).
                    let child = if delta == 0 {
                        None
                    } else {
                        Some(SizedPointer::new(offset.saturating_sub(delta)))
                    };
                    children.push(child);
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
        // Non-leaf node with an embedded payload (prefix match for short keys).
        // We need to skip over the node's own pointer/transition data to reach
        // the payload.  Reuse `parse_bti_node` to find the payload start.
        //
        // NOTE: `parse_bti_node` reads its header from `data[0]`, so it must be
        // passed the slice STARTING at the node (`&trie_data[node_offset..]`),
        // while the absolute `node_offset` drives child-position arithmetic.
        // Passing the whole `trie_data` here was a latent bug (it parsed the
        // node at offset 0 instead) — only exposed once a non-leaf node carries
        // an embedded payload at a non-zero offset (issue #832).
        let node = parse_bti_node(&trie_data[node_offset..], node_offset as u64)?;
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

// ─────────────────────────────────────────────────────────────────────────────
// Full trie traversal (DFS) primitives — issue #832
// ─────────────────────────────────────────────────────────────────────────────
//
// These free functions operate purely on in-memory trie bytes plus a root
// offset (mirroring `walk_bti_trie`).  They enumerate *every* leaf/payload in
// **byte-comparable order** by performing an explicit, depth-capped, stack-based
// depth-first search.
//
// In-order semantics (matches Cassandra `Walker`/`ReverseValueIterator`):
//   - A node that carries its own payload sorts BEFORE all of its children
//     (the key that terminates here is a prefix of every continuation).
//   - Children are visited in ascending transition-byte order.
//
// The reconstructed key for each emitted entry is the concatenation of the
// transition bytes from the root down to (and including) the node that carries
// the payload.  This is a *byte-comparable / token* encoding, NOT the original
// partition/clustering key — callers that need the original key must resolve it
// from Data.db.  The payload OFFSETS, however, are definitive.

/// Maximum DFS stack depth, mirroring [`crate::storage::sstable::bti::MAX_TRIE_DEPTH`].
const DFS_MAX_DEPTH: usize = 128;

/// Read the root offset from the 8-byte big-endian footer of a BTI file and
/// load the entire trie (everything before the footer) into memory.
///
/// Real Cassandra 5.0 BTI files (`Partitions.db` / `Rows.db`) have **no
/// header** — the root node's absolute offset is the last 8 bytes of the file.
/// This is the footer-based loader used by the traversal iterators; it does NOT
/// rely on the fictional [`BtiHeader`] (whose `parse` would misread byte 0 of a
/// real BTI file).
///
/// Returns `(trie_data, root_offset)`.
fn load_bti_trie_via_footer<R: Read + Seek>(reader: &mut R) -> BtiResult<(Vec<u8>, usize)> {
    let file_size = reader.seek(SeekFrom::End(0))?;
    if file_size < 8 {
        return Err(Error::Parse(format!(
            "BTI file too small ({file_size} bytes; need at least 8 for footer)"
        )));
    }

    reader.seek(SeekFrom::End(-8))?;
    let mut footer = [0u8; 8];
    reader.read_exact(&mut footer)?;
    let root_offset = u64::from_be_bytes(footer);

    let trie_size = file_size - 8;
    if root_offset >= trie_size {
        return Err(Error::Parse(format!(
            "BTI file: root_offset {root_offset} >= trie_size {trie_size}"
        )));
    }

    reader.seek(SeekFrom::Start(0))?;
    let mut trie_data = vec![0u8; trie_size as usize];
    reader.read_exact(&mut trie_data)?;
    Ok((trie_data, root_offset as usize))
}

/// Collect the ascending-transition-byte child list `(transition_byte, child_offset)`
/// for a parsed BTI node, ready for in-order DFS.
///
/// Crucially this iterates `Dense` children directly by index (transition byte =
/// `start_byte + i`) and **skips** the `None` slots, which are the Dense "no
/// transition" sentinels (raw delta `0`).  A `Some(ptr)` slot is a real child
/// and is always emitted — *including* one whose absolute offset is `0` (the
/// first-written leaf in BTI's bottom-up layout legitimately lives at offset 0).
/// ([`BtiNode::get_transitions`] returns an empty Vec for Dense nodes and must
/// NOT be used for traversal.)
///
/// `Sparse` transitions are returned in their stored order; the parser preserves
/// the on-disk ascending order, and we sort defensively.
fn ordered_children(node: &BtiNode) -> Vec<(u8, usize)> {
    match &node.data {
        BtiNodeData::PayloadOnly { .. } => Vec::new(),
        BtiNodeData::Single { transition } => {
            vec![(transition.byte, transition.child.distance as usize)]
        }
        BtiNodeData::Sparse { transitions } => {
            let mut out: Vec<(u8, usize)> = transitions
                .iter()
                .map(|t| (t.byte, t.child.distance as usize))
                .collect();
            // Defensive: BTI Sparse transitions are stored ascending; enforce it.
            out.sort_by_key(|&(b, _)| b);
            out
        }
        BtiNodeData::Dense {
            start_byte,
            children,
        } => {
            let mut out = Vec::new();
            for (i, child) in children.iter().enumerate() {
                // `None` is the Dense "no transition" sentinel (raw delta 0):
                // skip it.  `Some(ptr)` is a real child and is always emitted,
                // even when `ptr.distance == 0` — a real child can live at
                // absolute trie offset 0 (the first-written leaf).
                if let Some(ptr) = child {
                    let transition_byte = start_byte.wrapping_add(i as u8);
                    out.push((transition_byte, ptr.distance as usize));
                }
            }
            out
        }
    }
}

/// Generic in-order DFS over a BTI trie, decoding each node payload with
/// `decode_payload` and pushing `(reconstructed_key, decoded_payload)` onto the
/// result Vec in byte-comparable order.
///
/// The traversal is iterative (explicit stack), depth-capped at
/// [`DFS_MAX_DEPTH`], and bounds-checks every offset — corrupt or cyclic tries
/// produce an error rather than an infinite loop or panic.
fn dfs_collect_in_order<T, F>(
    trie_data: &[u8],
    root_offset: usize,
    mut decode_payload: F,
) -> BtiResult<Vec<(Vec<u8>, T)>>
where
    F: FnMut(&[u8], usize) -> BtiResult<Option<T>>,
{
    let mut results: Vec<(Vec<u8>, T)> = Vec::new();

    // Stack frame: (node_offset, accumulated_key_bytes).
    // We use an explicit stack and push children in *reverse* order so the
    // smallest transition byte is processed first (LIFO).
    let mut stack: Vec<(usize, Vec<u8>)> = vec![(root_offset, Vec::new())];

    while let Some((node_offset, key_bytes)) = stack.pop() {
        if key_bytes.len() > DFS_MAX_DEPTH {
            return Err(Error::Parse(format!(
                "BTI DFS exceeded max depth {DFS_MAX_DEPTH} (corrupt or cyclic trie)"
            )));
        }
        if node_offset >= trie_data.len() {
            return Err(Error::Parse(format!(
                "BTI DFS: node_offset {node_offset} out of bounds (trie size {})",
                trie_data.len()
            )));
        }

        // 1) Emit this node's own payload (if any) BEFORE descending — the key
        //    terminating here sorts before any continuation.
        if let Some(payload) = decode_payload(trie_data, node_offset)? {
            results.push((key_bytes.clone(), payload));
        }

        // 2) Descend children in ASCENDING transition-byte order.  Because the
        //    stack is LIFO, push them in DESCENDING order.
        let node = parse_bti_node_for_traversal(trie_data, node_offset)?;
        let children = ordered_children(&node);
        for &(transition_byte, child_offset) in children.iter().rev() {
            let mut child_key = key_bytes.clone();
            child_key.push(transition_byte);
            stack.push((child_offset, child_key));
        }
    }

    Ok(results)
}

/// Enumerate every partition entry in a `Partitions.db` trie in byte-comparable
/// order: `(reconstructed_token_key, BtiPartitionLocation)`.
fn dfs_collect_partition_entries(
    trie_data: &[u8],
    root_offset: usize,
) -> BtiResult<Vec<(Vec<u8>, BtiPartitionLocation)>> {
    dfs_collect_in_order(trie_data, root_offset, |data, off| {
        read_node_payload(data, off)
    })
}

/// Enumerate **all** partitions in a real Cassandra 5.0 `Partitions.db` BTI file
/// (issue #832), in byte-comparable order.
///
/// This is the headerless public entry point: the trie is loaded via the 8-byte
/// footer (NOT the fictional [`BtiHeader`]).  Each returned tuple is
/// `(reconstructed_token_key, BtiPartitionLocation)`; the offset is definitive,
/// the key is a byte-comparable token prefix (see the DFS module note).
///
/// Returns an empty Vec for a `< 8`-byte (e.g. empty) file.
pub fn iterate_partitions_in_bti_file<R: Read + Seek>(
    reader: &mut R,
) -> BtiResult<Vec<(Vec<u8>, BtiPartitionLocation)>> {
    let file_size = reader.seek(SeekFrom::End(0))?;
    if file_size < 8 {
        return Ok(Vec::new());
    }
    let (trie_data, root_offset) = load_bti_trie_via_footer(reader)?;
    dfs_collect_partition_entries(&trie_data, root_offset)
}

// ─────────────────────────────────────────────────────────────────────────────
// Rows.db in-trie payload decoding (RowIndexReader / TrieIndexEntry)
// ─────────────────────────────────────────────────────────────────────────────
//
// IMPORTANT: `Rows.db` in-trie payloads are NOT the `Partitions.db` payload
// format (which is a hash byte + SizedInts *signed* position).  A `Rows.db`
// trie leaf carries a `RowIndexReader.IndexInfo` whose byte layout is defined
// authoritatively by `RowIndexReader.readPayload`
// (cassandra-5.0.0 `RowIndexReader.java:111-125`):
//
//   static IndexInfo readPayload(ByteBuffer buf, int ppos, int bits, Version v) {
//       if (bits == 0) return null;
//       int bytes = bits & ~FLAG_OPEN_MARKER;            // FLAG_OPEN_MARKER = 8
//       long offset = SizedInts.read(buf, ppos, bytes);  // SizedInts, NOT a vint
//       ppos += bytes;
//       DeletionTime del = (bits & FLAG_OPEN_MARKER) != 0
//                          ? DeletionTime.deserialize(buf, ppos) : null;
//       return new IndexInfo(offset, del);
//   }
//
// So the low nibble of the node header byte (`payloadBits`) splits as:
//   - low 3 bits  → the number of `SizedInts` bytes encoding the block offset
//   - bit 0x8     → FLAG_OPEN_MARKER: an open-deletion `DeletionTime` follows
//
// The `offset` field is the block's offset **relative to the partition start**
// in `Data.db` (the IndexInfo doc: "where in the data file to start looking for
// a given key"), so absolute Data.db position = `entry.data_position + offset`.
//
// The writer side (`RowIndexWriter.getSerializer.write`,
// `RowIndexWriter.java:160-180`) confirms this exactly:
//   bytes = SizedInts.nonZeroSize(payload.offset);
//   type.serialize(dest, node, bytes | hasOpenMarker, nodePosition);
//   SizedInts.write(dest, payload.offset, bytes);
//   if (hasOpenMarker) DeletionTime.serialize(payload.openDeletion, dest);
//
// Earlier this decoder read the offset as an *unsigned vint* and treated the
// whole low nibble as a flag — that misparsed every real wide-partition block
// (issue #832, Finding A's sibling bug: payloads came out as 64/0/1/… garbage).
//
// Reference: docs/sstables-definitive-guide chapter 17 (Rows.db footer);
//            cassandra-5.0.0 `RowIndexReader.java`, `RowIndexWriter.java`,
//            `TrieIndexEntry.java`, `SizedInts.java`.
// ─────────────────────────────────────────────────────────────────────────────

/// The `FLAG_OPEN_MARKER` bit in a `Rows.db` trie node's `payloadBits`
/// (low nibble of the header byte).  When set, an open-deletion `DeletionTime`
/// follows the `SizedInts` block offset.  Mirrors
/// `RowIndexReader.FLAG_OPEN_MARKER`.
pub const FLAG_OPEN_MARKER: u8 = 0x8;

/// A decoded `Rows.db` in-trie row-index block entry (`RowIndexReader.IndexInfo`).
///
/// The headline field is [`data_offset`](Self::data_offset): the block's offset
/// **relative to the partition start** in `Data.db`.  To obtain the absolute
/// `Data.db` byte position, add the partition's data position (see
/// [`BtiRowIndexHeader::data_position`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtiRowIndexEntry {
    /// Block offset **relative to the partition start**, decoded via
    /// `SizedInts.read(buf, ppos, payloadBits & ~FLAG_OPEN_MARKER)`.
    pub data_offset: u64,
    /// Open-deletion time `(local_deletion_time, marked_for_delete_at)`,
    /// present only when the `FLAG_OPEN_MARKER` payload bit is set.
    pub open_marker: Option<(i32, i64)>,
}

/// Read an unsigned VInt (Cassandra count-leading-ones encoding, **not** ZigZag)
/// from `data`, returning `(value, bytes_consumed)`.
///
/// This is the encoding Cassandra uses for Data.db positions in the row index
/// (`DataOutputPlus.writeUnsignedVInt`).  The number of extra bytes equals the
/// number of leading 1-bits in the first byte; the value is big-endian across
/// the remaining bits.
fn read_unsigned_vint_from_slice(data: &[u8]) -> BtiResult<(u64, usize)> {
    if data.is_empty() {
        return Err(Error::Parse(
            "Rows.db payload: unexpected end of data reading unsigned vint".to_string(),
        ));
    }
    let first = data[0];
    let extra_bytes = first.leading_ones() as usize;
    if extra_bytes > 8 {
        return Err(Error::Parse(format!(
            "Rows.db payload: invalid unsigned vint first byte 0x{first:02x}"
        )));
    }
    let total = extra_bytes + 1;
    if data.len() < total {
        return Err(Error::Parse(format!(
            "Rows.db payload: unsigned vint needs {total} bytes, have {}",
            data.len()
        )));
    }

    // Data bits in the first byte: 8 - extra_bytes - 1 (the separator 0 bit),
    // except when extra_bytes == 8 (first byte is all ones, no data bits).
    let mut value: u64 = if extra_bytes >= 8 {
        0
    } else {
        let data_bits = 8 - extra_bytes - 1;
        let mask = if data_bits == 0 {
            0
        } else {
            (1u16 << data_bits) - 1
        };
        (first as u16 & mask) as u64
    };
    for &b in &data[1..total] {
        value = (value << 8) | (b as u64);
    }
    Ok((value, total))
}

/// The modern (DA/BTI) `DeletionTime` "live" sentinel byte.
///
/// In the `da`-family on-disk serializer, a `DeletionTime` written by the BTI
/// row-index / trie-index path is encoded as a single `0x80` byte when it is
/// `DeletionTime.LIVE` (no deletion), and otherwise as the full value (see
/// [`decode_da_deletion_time`]).  Mirrors the live/no-deletion fast-path of
/// `org.apache.cassandra.db.DeletionTime.Serializer` in the trie-index
/// (`cassandra-5.0.0`) format, where the live sentinel avoids writing the
/// 12-byte body.
const DA_DELETION_TIME_LIVE_SENTINEL: u8 = 0x80;

/// Width of a non-live modern (DA) `DeletionTime` body: `i64 markedForDeleteAt`
/// followed by `u32 localDeletionTime`.
const DA_DELETION_TIME_BODY_LEN: usize = 12;

/// Decode a modern (DA/BTI) `DeletionTime` at `data[start..]`, returning
/// `(deletion, bytes_consumed)` where `deletion` is `None` for the LIVE
/// sentinel (issue #832 Finding 2).
///
/// Layout (mirrors `org.apache.cassandra.db.DeletionTime.Serializer` in the
/// `da`/trie-index format, cassandra-5.0.0):
///
///   - a single `0x80` byte → `DeletionTime.LIVE` (no deletion); consumes 1 byte.
///   - otherwise the body is `[markedForDeleteAt : i64 BE][localDeletionTime :
///     u32 BE]` — `markedForDeleteAt` FIRST, then `localDeletionTime`; consumes
///     12 bytes.  This differs from the LEGACY layout
///     (`[localDeletionTime i32][markedForDeleteAt i64]`) in BOTH field order
///     and the width/signedness of `localDeletionTime` (modern: `u32`).
///
/// Returns the deletion as `(local_deletion_time, marked_for_delete_at)` to
/// match [`BtiRowIndexEntry::open_marker`]'s existing tuple ordering, even
/// though the modern wire order is the reverse.
///
/// # Errors
/// Returns a parse error if `start` is out of bounds or a non-live value is
/// truncated.
fn decode_da_deletion_time(data: &[u8], start: usize) -> BtiResult<(Option<(i32, i64)>, usize)> {
    if start >= data.len() {
        return Err(Error::Parse(format!(
            "DA DeletionTime: start {start} beyond buffer size {}",
            data.len()
        )));
    }
    if data[start] == DA_DELETION_TIME_LIVE_SENTINEL {
        return Ok((None, 1));
    }
    if start + DA_DELETION_TIME_BODY_LEN > data.len() {
        return Err(Error::Parse(format!(
            "DA DeletionTime: non-live value needs {DA_DELETION_TIME_BODY_LEN} bytes, have {}",
            data.len().saturating_sub(start)
        )));
    }
    let b = &data[start..start + DA_DELETION_TIME_BODY_LEN];
    // markedForDeleteAt FIRST (i64), then localDeletionTime (u32).
    let marked_for_delete_at = i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
    let local_deletion_time = u32::from_be_bytes([b[8], b[9], b[10], b[11]]) as i32;
    Ok((
        Some((local_deletion_time, marked_for_delete_at)),
        DA_DELETION_TIME_BODY_LEN,
    ))
}

/// Decode a `Rows.db` in-trie payload (`RowIndexReader.IndexInfo`) at
/// `payload_start` inside `trie_data`, given the node's `payload_bits` (low
/// nibble of the header byte).
///
/// Layout (mirrors `RowIndexReader.readPayload`, cassandra-5.0.0
/// `RowIndexReader.java:111-125`):
///   - `bytes = payload_bits & !FLAG_OPEN_MARKER` → block offset is a
///     `SizedInts` value of `bytes` bytes (the offset is relative to the
///     partition's data position).
///   - if `payload_bits & FLAG_OPEN_MARKER`, an open-deletion `DeletionTime`
///     follows in the MODERN DA form ([`decode_da_deletion_time`]): a `0x80`
///     LIVE sentinel, else `[i64 markedForDeleteAt][u32 localDeletionTime]`.
///
/// A `payload_bits` of `0` is not a valid leaf payload here (the caller filters
/// such nodes out) and yields an error.
fn decode_bti_row_payload(
    trie_data: &[u8],
    payload_start: usize,
    payload_bits: u8,
) -> BtiResult<BtiRowIndexEntry> {
    if payload_start > trie_data.len() {
        return Err(Error::Parse(format!(
            "Rows.db payload start {payload_start} beyond trie size {}",
            trie_data.len()
        )));
    }

    // Low 3 bits = number of SizedInts bytes; bit 0x8 = open-marker flag.
    let offset_bytes = (payload_bits & !FLAG_OPEN_MARKER) as usize;
    if offset_bytes == 0 || offset_bytes > 7 {
        // RowIndexWriter asserts `bytes < 8` ("rows larger than 32 PiB"); a
        // 0-byte offset would mean an empty payload, which the trie does not
        // emit for a real row-index block.
        return Err(Error::Parse(format!(
            "Rows.db payload: invalid SizedInts byte count {offset_bytes} \
             (payload_bits=0x{payload_bits:02x}); expected 1..=7"
        )));
    }
    if payload_start + offset_bytes > trie_data.len() {
        return Err(Error::Parse(format!(
            "Rows.db payload: SizedInts offset needs {offset_bytes} bytes, have {}",
            trie_data.len().saturating_sub(payload_start)
        )));
    }

    // SizedInts is a signed sign-extended big-endian read (SizedInts.read).
    // Block offsets are non-negative in practice, but we decode faithfully.
    let raw = sized_ints_read_from_slice(&trie_data[payload_start..payload_start + offset_bytes])?;
    let data_offset = raw as u64;

    let open_marker = if payload_bits & FLAG_OPEN_MARKER != 0 {
        // DA/BTI modern DeletionTime (issue #832 Finding 2): a 0x80 sentinel
        // means LIVE (no open deletion); otherwise [i64 markedForDeleteAt][u32
        // localDeletionTime].  This is NOT the legacy [i32 ldt][i64 mfda] form.
        let dt_start = payload_start + offset_bytes;
        let (deletion, _consumed) = decode_da_deletion_time(trie_data, dt_start)?;
        deletion
    } else {
        None
    };

    Ok(BtiRowIndexEntry {
        data_offset,
        open_marker,
    })
}

/// Read the `BtiRowIndexEntry` from the payload attached to a `Rows.db` node at
/// `node_offset`, or `None` if the node carries no payload.
///
/// Structurally parallels [`read_node_payload`] but decodes the Rows.db payload
/// format via [`decode_bti_row_payload`].
fn read_row_node_payload(
    trie_data: &[u8],
    node_offset: usize,
) -> BtiResult<Option<BtiRowIndexEntry>> {
    if node_offset >= trie_data.len() {
        return Err(Error::Parse(format!(
            "Rows.db payload read: node_offset {node_offset} out of bounds"
        )));
    }

    let header_byte = trie_data[node_offset];
    let ordinal = (header_byte >> 4) & 0x0F;
    let payload_flags = header_byte & 0x0F;

    // SingleNoPayload variants (ordinals 1, 3) carry their delta in the low
    // nibble and never have a payload.  See `read_node_payload`.
    if ordinal == 1 || ordinal == 3 {
        return Ok(None);
    }

    if ordinal == 0 {
        if payload_flags == 0 {
            return Err(Error::Parse(
                "Rows.db PayloadOnly node has zero payload_flags".to_string(),
            ));
        }
        let payload_start = node_offset + 1;
        Ok(Some(decode_bti_row_payload(
            trie_data,
            payload_start,
            payload_flags,
        )?))
    } else if payload_flags != 0 {
        // Slice must start at the node (see note in `read_node_payload`).
        let node = parse_bti_node(&trie_data[node_offset..], node_offset as u64)?;
        let payload_start = payload_start_in_node(&node, trie_data, node_offset)?;
        Ok(Some(decode_bti_row_payload(
            trie_data,
            payload_start,
            payload_flags,
        )?))
    } else {
        Ok(None)
    }
}

/// Enumerate every row-index entry in a `Rows.db` trie (rooted at `root_offset`)
/// in byte-comparable order: `(reconstructed_clustering_key, BtiRowIndexEntry)`.
fn dfs_collect_row_entries(
    trie_data: &[u8],
    root_offset: usize,
) -> BtiResult<Vec<(Vec<u8>, BtiRowIndexEntry)>> {
    dfs_collect_in_order(trie_data, root_offset, |data, off| {
        read_row_node_payload(data, off)
    })
}

/// Enumerate every row-index entry in a `Rows.db` row-index trie **rooted at an
/// explicit `root_offset`**, in byte-comparable order
/// (`(reconstructed_clustering_key, BtiRowIndexEntry)`).
///
/// ## Why the root must be supplied by the caller
///
/// A real Cassandra 5.0 `Rows.db` is NOT a single whole-file trie: it holds
/// **many independent per-partition row-index tries** concatenated together.
/// There is one row-index trie per (wide) partition, and the root of a given
/// partition's trie is the `RowsOffset` returned from the corresponding
/// `Partitions.db` lookup ([`BtiPartitionLocation::RowsOffset`]) — it is NOT the
/// 8-byte file footer, which spans the whole file and would misparse any
/// multi-partition `Rows.db`.
///
/// This is therefore the correct general entry point: pass the full `Rows.db`
/// bytes as `trie_data` and the partition's `RowsOffset` as `root_offset`.
///
/// An out-of-bounds `root_offset` (e.g. on empty `trie_data`) yields a clean
/// parse error rather than a panic.
pub fn iterate_rows_in_bti_trie(
    trie_data: &[u8],
    root_offset: usize,
) -> BtiResult<Vec<(Vec<u8>, BtiRowIndexEntry)>> {
    dfs_collect_row_entries(trie_data, root_offset)
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-partition Rows.db entry resolution — TrieIndexEntry (issue #832 Finding A)
// ─────────────────────────────────────────────────────────────────────────────
//
// The positive `position` stored in a `Partitions.db` leaf payload
// (`BtiPartitionLocation::RowsOffset`) does NOT point at a row-index trie root.
// It points at this partition's **row-index entry** in `Rows.db`, which must be
// deserialized to recover the actual trie root (plus the partition's Data.db
// position, block count and partition-level deletion).  Feeding `RowsOffset`
// straight to `iterate_rows_in_bti_trie` parses entry metadata as a trie node
// and fails (or, worse, returns garbage) on real wide partitions.
//
// On-disk layout at `RowsOffset` (validated against the `wide_table` fixture —
// see `tests/issue_832_bti_traversal.rs`; cassandra-5.0.0 `TrieIndexEntry.java`
// `serialize`/`deserialize`, lines 89-120, and `RowIndexWriter.complete`):
//
//   [u16 key_length][partition key bytes]      ← short-length-prefixed key
//   [data file position : unsigned vint]       ← partition start in Data.db
//   [trie_root - base    : SIGNED vint]         ← base = RowsOffset + key_length
//   [row index block count : unsigned vint32]
//   [partition DeletionTime]                    ← delta/compact form; best-effort
//
// `TrieIndexEntry.deserialize` computes `indexTrieRoot = readVInt() + base`.
// Cassandra passes `base` as the file position from which the entry is read;
// for this fixture that base is `RowsOffset + key_length` (the partition key
// is length-prefixed and precedes the vint fields).  Concretely, the three
// `wide_table` partitions decode as:
//
//   RowsOffset 242 → key=pk1, dataPos=0,       rootΔ=-10 → trieRoot=236, blocks=38
//   RowsOffset 494 → key=pk2, dataPos=619201,  rootΔ=-10 → trieRoot=488, blocks=38
//   RowsOffset 748 → key=pk3, dataPos=1238408, rootΔ=-10 → trieRoot=742, blocks=38
//
// ─────────────────────────────────────────────────────────────────────────────

/// A single `Rows.db` row-index entry paired with its reconstructed
/// byte-comparable clustering separator key, as yielded by the in-order DFS.
pub type BtiRowIndexEntryWithKey = (Vec<u8>, BtiRowIndexEntry);

/// A deserialized per-partition `Rows.db` row-index entry (Cassandra
/// `TrieIndexEntry`).  Produced by [`resolve_rows_db_entry`] from the
/// `RowsOffset` returned by a `Partitions.db` lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtiRowIndexHeader {
    /// Absolute byte position of the partition's start in `Data.db`.  Block
    /// offsets in [`BtiRowIndexEntry::data_offset`] are relative to this.
    pub data_position: u64,
    /// Byte offset, within the `Rows.db` file, of this partition's row-index
    /// trie root node.  Feed this to [`iterate_rows_in_bti_trie`] /
    /// [`RowsParser::range_query`] / [`RowsParser::iterate_rows`].
    pub trie_root: usize,
    /// Number of row-index blocks indexed by this partition's trie.
    pub block_count: u32,
    /// Partition-level deletion `(local_deletion_time, marked_for_delete_at)`,
    /// decoded via the MODERN DA `DeletionTime` form
    /// ([`decode_da_deletion_time`], issue #832 Finding 2); `None` for the `0x80`
    /// LIVE sentinel or when too few trailing bytes remain.
    pub partition_deletion: Option<(i32, i64)>,
}

/// Read a signed VInt (Cassandra zig-zag, `DataInputPlus.readVInt`) from `data`,
/// returning `(value, bytes_consumed)`.
fn read_signed_vint_from_slice(data: &[u8]) -> BtiResult<(i64, usize)> {
    let (u, n) = read_unsigned_vint_from_slice(data)?;
    // ZigZag decode: (u >>> 1) ^ -(u & 1)
    let value = ((u >> 1) as i64) ^ -((u & 1) as i64);
    Ok((value, n))
}

/// Test-only re-export of [`read_unsigned_vint_from_slice`] so the BTI `Rows.db`
/// writer (`writer::partitions_writer`) can assert its unsigned-VInt encoder is
/// the exact inverse of this reader decoder.
#[doc(hidden)]
pub fn read_unsigned_vint_from_slice_for_test(data: &[u8]) -> BtiResult<(u64, usize)> {
    read_unsigned_vint_from_slice(data)
}

/// Test-only re-export of [`read_signed_vint_from_slice`] (see
/// [`read_unsigned_vint_from_slice_for_test`]).
#[doc(hidden)]
pub fn read_signed_vint_from_slice_for_test(data: &[u8]) -> BtiResult<(i64, usize)> {
    read_signed_vint_from_slice(data)
}

/// Resolve a partition's row-index entry in `Rows.db`, given the `RowsOffset`
/// from a `Partitions.db` lookup ([`BtiPartitionLocation::RowsOffset`]).
///
/// This is the fix for issue #832 Finding A: `RowsOffset` is the offset of the
/// per-partition `TrieIndexEntry`, NOT a trie root.  This deserializes that
/// entry — recovering the partition's Data.db position, the actual row-index
/// trie root, the block count and the partition deletion — so traversal can be
/// rooted correctly.
///
/// `rows_db` is the full `Rows.db` file contents; `rows_offset` is the
/// `RowsOffset` value.  All reads are bounds-checked.
///
/// # Errors
/// Returns a parse error if `rows_offset` is out of bounds, the key length is
/// implausible, the vint fields are truncated, or the recovered trie root falls
/// outside `rows_db`.
pub fn resolve_rows_db_entry(rows_db: &[u8], rows_offset: usize) -> BtiResult<BtiRowIndexHeader> {
    if rows_offset + 2 > rows_db.len() {
        return Err(Error::Parse(format!(
            "Rows.db entry: rows_offset {rows_offset} + 2 (key length) exceeds file size {}",
            rows_db.len()
        )));
    }

    // [u16 key_length][key bytes]
    let key_length = u16::from_be_bytes([rows_db[rows_offset], rows_db[rows_offset + 1]]) as usize;
    let entry_start = rows_offset + 2 + key_length;
    if entry_start > rows_db.len() {
        return Err(Error::Parse(format!(
            "Rows.db entry: key length {key_length} at offset {rows_offset} overruns file size {}",
            rows_db.len()
        )));
    }

    // base for the SIGNED root delta = RowsOffset + key_length (see module note).
    let base = rows_offset + key_length;

    let mut cur = entry_start;
    let (data_position, n) = read_unsigned_vint_from_slice(&rows_db[cur..])?;
    cur += n;

    let (root_delta, n) = read_signed_vint_from_slice(&rows_db[cur..])?;
    cur += n;

    // indexTrieRoot = readVInt() + base   (TrieIndexEntry.deserialize)
    let trie_root_signed = root_delta + base as i64;
    if trie_root_signed < 0 || (trie_root_signed as usize) >= rows_db.len() {
        return Err(Error::Parse(format!(
            "Rows.db entry: recovered trie root {trie_root_signed} out of bounds \
             (base={base}, delta={root_delta}, file size={})",
            rows_db.len()
        )));
    }
    let trie_root = trie_root_signed as usize;

    let (block_count_u64, n) = read_unsigned_vint_from_slice(&rows_db[cur..])?;
    cur += n;
    let block_count = u32::try_from(block_count_u64).map_err(|_| {
        Error::Parse(format!(
            "Rows.db entry: implausible block count {block_count_u64}"
        ))
    })?;

    // Partition DeletionTime: decode the MODERN DA/BTI form (issue #832
    // Finding 2) — a 0x80 byte means LIVE (no deletion, the common case when no
    // deletes were issued); otherwise [i64 markedForDeleteAt][u32
    // localDeletionTime].  Decoded best-effort: if too few trailing bytes
    // remain, leave it `None` rather than failing (it is not required for
    // traversal correctness).
    let partition_deletion = match decode_da_deletion_time(rows_db, cur) {
        Ok((deletion, _consumed)) => deletion,
        Err(_) => None,
    };

    Ok(BtiRowIndexHeader {
        data_position,
        trie_root,
        block_count,
        partition_deletion,
    })
}

/// Cassandra component separator used between clustering components in the
/// OSS50 byte-comparable form.  Mirrors `ByteSource.NEXT_COMPONENT` (value
/// `0x40`) emitted by `ClusteringComparator.asByteComparable` /
/// `ByteSource.withTerminator`/`ByteSource.of(...)` in
/// `org.apache.cassandra.utils.bytecomparable.ByteSource` (cassandra-5.0.0).
const OSS50_NEXT_COMPONENT: u8 = 0x40;

/// Encode a single clustering-component [`Value`] in Cassandra OSS50
/// **byte-comparable** form (the SAME encoding the `Rows.db` row-index trie
/// stores its separators in), appending to `out`.
///
/// This is NOT CQLite's custom [`ByteComparableEncoder`] (which prepends a
/// 1-byte type discriminator and so is byte-incompatible with the on-disk
/// trie).  It reproduces the per-type `AbstractType.asComparableBytes(...)`
/// production used by Cassandra to build the byte-comparable keys:
///
/// - `Int32Type` → `(v as u32 ^ 0x8000_0000)` big-endian (sign-flip, 4B), per
///   `Int32Type.asComparableBytes` / `ByteSource.optionalSignedFixedLengthNumber`.
/// - `LongType` → `(v as u64 ^ 0x8000_0000_0000_0000)` big-endian (8B), per
///   `LongType.asComparableBytes`.
/// - `ShortType`/`ByteType` (`smallint`/`tinyint`) → sign-flip big-endian (2B /
///   1B), per `ShortType`/`ByteType.asComparableBytes`.
/// - `BooleanType` → single byte `0x00`/`0x01`, per `BooleanType.asComparableBytes`.
/// - `TimestampType` (`timestamp`) → `LongType`-style sign-flip 8B (`TimestampType`
///   extends `LongType`'s comparable form).
/// - `UUIDType`/`TimeUUIDType` → the 16 raw bytes (network order is already
///   byte-comparable for the on-disk separator form here).
/// - `UTF8Type`/`AsciiType` (`text`/`ascii`) and `BytesType` (`blob`) → the raw
///   value bytes, terminated by the OSS50 variable-length component terminator
///   (`0x00`) so a shorter value sorts before a longer one sharing its prefix
///   (`ByteSource.of(ByteBuffer)` + the terminator convention in
///   `ByteComparable.Version.OSS50`).
///
/// Any clustering type not enumerated here returns an explicit parse error
/// (NO silent wrong results — issue #28 no-heuristics mandate).
/// OSS50 variable-length byte-comparable encoding for `BytesType`/`UTF8Type`
/// (Cassandra `ByteSource`): every literal `0x00` byte is escaped as
/// `0x00 0xFE` (`ESCAPE` + `ESCAPED_0_CONT`) and the component is terminated
/// with `0x00 0xFF` (`ESCAPE` + `ESCAPED_0_DONE`).  This makes the encoding
/// weakly prefix-free, so a shorter value sorts before a longer value that
/// shares its prefix — matching the separators stored in a real `Rows.db`
/// trie.  (The project's `ByteComparableEncoder` uses a different,
/// non-Cassandra escape scheme and must NOT be used for trie-compatible bounds.)
fn encode_varlen_oss50(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == 0x00 {
            out.push(0xFE);
        }
    }
    out.push(0x00);
    out.push(0xFF);
}

fn encode_clustering_component_oss50(value: &Value, out: &mut Vec<u8>) -> BtiResult<()> {
    match value {
        // int — Int32Type: sign-flip, big-endian (matches `wide_table` separators,
        // e.g. ck=8 → 80 00 00 08).
        Value::Integer(v) => {
            out.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes());
            Ok(())
        }
        // bigint — LongType: sign-flip, big-endian, 8 bytes.
        Value::BigInt(v) | Value::Counter(v) => {
            out.extend_from_slice(&((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
            Ok(())
        }
        // smallint — ShortType: sign-flip, big-endian, 2 bytes.
        Value::SmallInt(v) => {
            out.extend_from_slice(&((*v as u16) ^ 0x8000).to_be_bytes());
            Ok(())
        }
        // tinyint — ByteType: sign-flip, 1 byte.
        Value::TinyInt(v) => {
            out.push((*v as u8) ^ 0x80);
            Ok(())
        }
        // boolean — BooleanType: single 0x00/0x01 byte.
        Value::Boolean(b) => {
            out.push(if *b { 0x01 } else { 0x00 });
            Ok(())
        }
        // timestamp — TimestampType shares LongType's comparable form (8-byte
        // sign-flip big-endian of the millisecond value).
        Value::Timestamp(v) => {
            out.extend_from_slice(&((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
            Ok(())
        }
        // uuid / timeuuid — raw 16 bytes (already byte-comparable in the on-disk
        // separator form for this fixed-length type).
        Value::Uuid(bytes) => {
            out.extend_from_slice(bytes);
            Ok(())
        }
        // text / ascii — OSS50 variable-length byte-comparable encoding.
        Value::Text(s) => {
            encode_varlen_oss50(s.as_bytes(), out);
            Ok(())
        }
        // blob / inet — OSS50 variable-length byte-comparable encoding.
        Value::Blob(b) | Value::Inet(b) => {
            encode_varlen_oss50(b, out);
            Ok(())
        }
        other => Err(Error::Parse(format!(
            "BTI range_query: byte-comparable encoding not implemented for {:?}",
            other.data_type()
        ))),
    }
}

/// Encode a multi-component clustering bound (`&[Value]`) in Cassandra OSS50
/// byte-comparable form — the SAME encoding the `Rows.db` trie stores.
///
/// A single-component clustering encodes to the bare component bytes (matching
/// the `wide_table` fixture separators, e.g. ck=8 → `80 00 00 08`, NO framing).
/// Multi-component clusterings concatenate per-component byte-comparable
/// encodings separated by [`OSS50_NEXT_COMPONENT`] (`ByteSource.NEXT_COMPONENT`,
/// per `ClusteringComparator.asByteComparable`), with no leading/trailing frame
/// so a prefix bound sorts before any longer key sharing it.
pub fn encode_clustering_bound_oss50(values: &[Value]) -> BtiResult<Vec<u8>> {
    // All-ascending convenience: every component uses its base byte-comparable
    // form. Equivalent to `encode_clustering_bound_oss50_with_order` with all
    // `is_reversed = false`.
    let mut out = Vec::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(OSS50_NEXT_COMPONENT);
        }
        encode_clustering_component_oss50(v, &mut out)?;
    }
    Ok(out)
}

/// Encode a multi-component clustering bound applying each column's clustering
/// **ORDER** (ASC/DESC), producing Cassandra OSS50 byte-comparable separators
/// that sort in the SAME order the rows are physically written.
///
/// ## Why DESC must invert the component bytes
///
/// Cassandra wraps a `CLUSTERING ORDER BY (c DESC)` column's type in
/// `ReversedType`. `ReversedType.asComparableBytes` delegates to the base type's
/// byte-comparable production and then inverts the resulting [`ByteSource`] via
/// `ByteSource.invert(...)`, which complements every emitted data byte
/// (`b -> 0xFF ^ b`). Complementing every byte of a (weakly prefix-free)
/// byte-comparable encoding reverses its lexicographic order, so a DESCENDING
/// value order maps to an ASCENDING byte order. This is exactly what a `Rows.db`
/// row-index trie needs: separators are always stored in ascending *byte* order,
/// while the underlying clustering values run descending for a DESC column.
/// (cassandra-5.0.0 `org.apache.cassandra.db.marshal.ReversedType.asComparableBytes`
/// + `org.apache.cassandra.utils.bytecomparable.ByteSource.invert`.)
///
/// The inter-component framing byte ([`OSS50_NEXT_COMPONENT`], `0x40`) is emitted
/// by the *comparator* (`ClusteringComparator.asByteComparable`), NOT by a
/// component's type, so it is **not** inverted even when neighbouring components
/// are DESC — only the per-component byte-comparable bytes are complemented. This
/// matches Cassandra, where each `subtype(i)` (possibly a `ReversedType`) emits
/// its own (already inverted) byte source and the comparator separates them with
/// the un-inverted `NEXT_COMPONENT` byte.
///
/// `is_reversed[i]` MUST correspond positionally to `values[i]` (i.e. the schema
/// clustering-key order). A short `is_reversed` slice treats missing entries as
/// ascending (`false`).
pub fn encode_clustering_bound_oss50_with_order(
    values: &[Value],
    is_reversed: &[bool],
) -> BtiResult<Vec<u8>> {
    let mut out = Vec::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(OSS50_NEXT_COMPONENT);
        }
        if is_reversed.get(i).copied().unwrap_or(false) {
            // Encode the base component into a scratch buffer, then complement
            // every byte (ReversedType / ByteSource.invert).
            let mut scratch = Vec::new();
            encode_clustering_component_oss50(v, &mut scratch)?;
            for b in &scratch {
                out.push(0xFF ^ *b);
            }
        } else {
            encode_clustering_component_oss50(v, &mut out)?;
        }
    }
    Ok(out)
}

/// Select the row-index blocks that may contain clustering keys in the
/// inclusive byte-comparable range `[start, end]`, applying row-index
/// **separator** semantics (issue #832 Finding B).
///
/// ## Why naive `[start, end]` filtering is wrong
///
/// A `Rows.db` row-index trie stores **separators**, not block start keys.  For
/// consecutive blocks the writer (`RowIndexWriter.add`) stores the shortest
/// `sep` with `prevMax < sep <= nextBlockFirstKey`, and `complete()` appends a
/// trailing separator after the last block.  Consequently the separator `s_i`
/// labels the boundary at the START of block `i`'s key range, and block `i`
/// covers the half-open key interval `[s_i, s_{i+1})` (the final block runs to
/// the trailing separator).  A reader locates the block for a key `K` via the
/// trie *floor* of `K` (`RowIndexReader.separatorFloor`), i.e. the block whose
/// separator is the greatest `<= K`.
///
/// Therefore a block `i` overlaps the requested clustering range `[start, end]`
/// iff its key interval `[s_i, s_{i+1})` intersects `[start, end]`:
///
///   `s_i <= end`  AND  `s_{i+1} > start`
///
/// (For the last block, `s_{i+1}` is treated as +∞.)  Filtering separators by
/// `start <= s_i <= end` — as the original implementation did — drops the
/// *floor* block whose separator is below `start` but whose interval still
/// contains `start`, and mishandles the block straddling `end`.
///
/// `entries` MUST be the full ascending-order `(separator, block)` list for one
/// partition (as produced by [`iterate_rows_in_bti_trie`]).  `start`/`end` are
/// byte-comparable clustering bounds in the **same encoding as the trie keys**
/// (Cassandra OSS50 clustering byte-comparable order).  Reversed bounds
/// (`start > end`) yield an empty result.
pub fn select_row_index_blocks_for_range(
    entries: &[(Vec<u8>, BtiRowIndexEntry)],
    start: &[u8],
    end: &[u8],
) -> Vec<BtiRowIndexEntry> {
    if start > end || entries.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    for (i, (sep_i, block)) in entries.iter().enumerate() {
        // s_{i+1}: the next separator, or +∞ for the last block.
        let next_is_greater_than_start = match entries.get(i + 1) {
            Some((sep_next, _)) => sep_next.as_slice() > start,
            None => true, // +∞ > start
        };
        let overlaps = sep_i.as_slice() <= end && next_is_greater_than_start;
        if overlaps {
            out.push(block.clone());
        }
    }
    out
}

/// Enumerate every row-index block entry for the partition whose `Rows.db`
/// row-index entry is at `rows_offset` (the `RowsOffset` from `Partitions.db`),
/// in ascending byte-comparable (clustering) order.
///
/// This is the convenience entry point that combines [`resolve_rows_db_entry`]
/// (Finding A) with [`iterate_rows_in_bti_trie`]: it resolves the real trie root
/// from the per-partition entry and then traverses from that root.  Each
/// returned [`BtiRowIndexEntry::data_offset`] is **relative to the partition
/// start**; add `header.data_position` for an absolute `Data.db` position.
///
/// Returns `(header, entries)`.
pub fn iterate_rows_for_partition(
    rows_db: &[u8],
    rows_offset: usize,
) -> BtiResult<(BtiRowIndexHeader, Vec<BtiRowIndexEntryWithKey>)> {
    let header = resolve_rows_db_entry(rows_db, rows_offset)?;
    let entries = iterate_rows_in_bti_trie(rows_db, header.trie_root)?;
    Ok((header, entries))
}

/// Enumerate row-index entries in a `Rows.db` file that is a **single-partition**
/// trie rooted at its 8-byte footer, in byte-comparable order.
///
/// ## Precondition
///
/// This treats the WHOLE file as one trie whose root is named by the trailing
/// 8-byte footer.  That is only correct when the `Rows.db` contains exactly one
/// partition's row-index trie (or is empty).  For a real multi-partition
/// `Rows.db` you MUST instead use [`iterate_rows_in_bti_trie`] with the
/// per-partition `RowsOffset` obtained from `Partitions.db`; the file footer
/// does not describe a single whole-file root in that case.
///
/// A `< 8`-byte (e.g. 0-byte) `Rows.db` — common for SSTables where no partition
/// has a row index — yields an empty Vec without erroring.
pub fn iterate_rows_in_bti_file<R: Read + Seek>(
    reader: &mut R,
) -> BtiResult<Vec<(Vec<u8>, BtiRowIndexEntry)>> {
    let file_size = reader.seek(SeekFrom::End(0))?;
    if file_size < 8 {
        return Ok(Vec::new());
    }
    let (trie_data, root_offset) = load_bti_trie_via_footer(reader)?;
    iterate_rows_in_bti_trie(&trie_data, root_offset)
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

    /// A benign placeholder header for files that carry no fictional
    /// [`BtiHeader`] (i.e. every real Cassandra `Rows.db`/`Partitions.db`, which
    /// are footer-rooted).  Used by [`RowsParser::new`] so the trie-based,
    /// whole-file entry points work on real files that legitimately lack the
    /// header; the fields are never consulted by those entry points.
    pub fn placeholder() -> Self {
        BtiHeader {
            magic: Self::MAGIC,
            version: Self::VERSION,
            flags: 0,
            root_offset: 0,
            entry_count: 0,
            metadata_size: 0,
        }
    }

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
    /// Create new rows parser.
    ///
    /// A real Cassandra 5.0 `Rows.db` has **no** whole-file [`BtiHeader`] (that
    /// fictional 28-byte header is only emitted by CQLite's own test/index
    /// writers); its leading bytes are trie node data and its layout is described
    /// per-partition via the `RowsOffset` from `Partitions.db`.  The trie-based
    /// entry points used here ([`range_query`](Self::range_query),
    /// [`range_query_encoded`](Self::range_query_encoded),
    /// [`iterate_rows`](Self::iterate_rows)) read the whole file and root from a
    /// resolved per-partition entry, so they do not consult `self.header`.
    ///
    /// We therefore *attempt* to parse the fictional header (for files that do
    /// carry it) but fall back to a benign default when it is absent — rather
    /// than rejecting every real `Rows.db` with an "invalid BTI magic" error.
    pub fn new(mut reader: R) -> BtiResult<Self> {
        // Attempt to read the (optional, CQLite-only) fictional header.  Real
        // Rows.db files have no such header; a parse failure there is expected
        // and must not block construction.
        reader.seek(SeekFrom::Start(0))?;
        let mut header_data = vec![0u8; 28];
        let header = match reader.read_exact(&mut header_data) {
            Ok(()) => BtiHeader::parse(&header_data)
                .map(|(h, _)| h)
                .unwrap_or_else(|_| BtiHeader::placeholder()),
            // File shorter than 28 bytes (e.g. tiny/empty Rows.db): also fine.
            Err(_) => BtiHeader::placeholder(),
        };
        // Leave the reader positioned at the start for whole-file reads.
        reader.seek(SeekFrom::Start(0))?;

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

    /// Clustering-key range/slice traversal of a single partition's `Rows.db`
    /// row-index trie (issue #832), taking **pre-encoded byte-comparable
    /// clustering bounds** and applying row-index **separator** semantics.
    ///
    /// ## Rooting (Finding A)
    ///
    /// `rows_offset` is the partition's `RowsOffset` from the `Partitions.db`
    /// lookup ([`BtiPartitionLocation::RowsOffset`]).  It points at the
    /// per-partition `TrieIndexEntry`, NOT the trie root, so this resolves the
    /// real trie root via [`resolve_rows_db_entry`] before traversing.
    ///
    /// ## Separator semantics (Finding B)
    ///
    /// `encoded_start`/`encoded_end` are inclusive byte-comparable clustering
    /// bounds in the **same encoding as the trie keys** (Cassandra OSS50
    /// clustering byte-comparable form — e.g. an `int` clustering `ck` encodes
    /// as `(ck as u32 ^ 0x8000_0000).to_be_bytes()`).  Because the trie stores
    /// separators (block boundaries), block selection uses
    /// [`select_row_index_blocks_for_range`]: a block is returned iff its
    /// half-open key interval `[s_i, s_{i+1})` intersects `[start, end]`.  This
    /// correctly includes the *floor* block that contains `start` even when its
    /// separator is below `start`, and excludes blocks that only abut the range.
    ///
    /// Reversed bounds yield an empty result.
    ///
    /// Returns the resolved [`BtiRowIndexHeader`] (for the partition's Data.db
    /// position and block count) together with the selected blocks; each block's
    /// `data_offset` is relative to `header.data_position`.
    pub fn range_query_encoded(
        &mut self,
        rows_offset: usize,
        encoded_start: &[u8],
        encoded_end: &[u8],
    ) -> BtiResult<(BtiRowIndexHeader, Vec<BtiRowIndexEntry>)> {
        let trie_data = self.read_full_rows_db()?;
        let header = resolve_rows_db_entry(&trie_data, rows_offset)?;
        let all = iterate_rows_in_bti_trie(&trie_data, header.trie_root)?;
        let blocks = select_row_index_blocks_for_range(&all, encoded_start, encoded_end);
        Ok((header, blocks))
    }

    /// Typed clustering-key range/slice traversal (issue #832 Finding 1).
    ///
    /// Encodes the `Value` clustering bounds in Cassandra **OSS50
    /// byte-comparable** form — the SAME encoding the `Rows.db` row-index trie
    /// stores its separators in — via [`encode_clustering_bound_oss50`], then
    /// delegates to the separator-aware [`range_query_encoded`].  This is the
    /// fix for the prior bug where bounds were encoded with CQLite's custom
    /// [`ByteComparableEncoder`] (which prepends a type discriminator and so is
    /// byte-incompatible with the on-disk trie), causing the typed API to
    /// compare incompatible byte formats and return empty/wrong block sets for
    /// real callers.
    ///
    /// Supported clustering types: `int`, `bigint`/`counter`, `smallint`,
    /// `tinyint`, `boolean`, `timestamp`, `uuid`/`timeuuid`, `text`/`ascii`,
    /// `blob`/`inet`.  Any other clustering type returns an explicit
    /// `Error::Parse("BTI range_query: byte-comparable encoding not implemented
    /// for <type>")` (no silent wrong results — issue #28).
    ///
    /// `rows_offset` is the partition's `RowsOffset` (resolved to the real trie
    /// root via [`resolve_rows_db_entry`], Finding A).  Separator semantics
    /// (Finding B) are applied via [`select_row_index_blocks_for_range`].
    /// Reversed bounds yield an empty result.
    pub fn range_query(
        &mut self,
        rows_offset: usize,
        start_key: &[Value],
        end_key: &[Value],
    ) -> BtiResult<Vec<BtiRowIndexEntry>> {
        let encoded_start = encode_clustering_bound_oss50(start_key)?;
        let encoded_end = encode_clustering_bound_oss50(end_key)?;
        if encoded_start > encoded_end {
            return Ok(Vec::new());
        }
        let (_, blocks) = self.range_query_encoded(rows_offset, &encoded_start, &encoded_end)?;
        Ok(blocks)
    }

    /// Order-aware typed clustering range query: the read-side counterpart to the
    /// writer's [`encode_clustering_bound_oss50_with_order`]. The trie stores
    /// DESC columns' separators in REVERSED byte-comparable form (complemented
    /// bytes), so a lookup MUST encode its bounds with the SAME per-column order
    /// and then compare in the trie's ascending BYTE order.
    ///
    /// `is_reversed[i]` matches `start_key[i]`/`end_key[i]` positionally (schema
    /// clustering order). For a DESC leading column the value bound `lo` encodes
    /// to LARGER bytes than `hi`, so after encoding we take the min/max of the
    /// two encoded byte sequences as the ascending byte range handed to
    /// [`select_row_index_blocks_for_range`]. This keeps writer↔reader
    /// consistent: both sides agree the on-disk separators are reversed bytes for
    /// DESC, and block selection always operates in ascending byte space.
    pub fn range_query_with_order(
        &mut self,
        rows_offset: usize,
        start_key: &[Value],
        end_key: &[Value],
        is_reversed: &[bool],
    ) -> BtiResult<Vec<BtiRowIndexEntry>> {
        let a = encode_clustering_bound_oss50_with_order(start_key, is_reversed)?;
        let b = encode_clustering_bound_oss50_with_order(end_key, is_reversed)?;
        // Map the value range to the ascending BYTE range the trie is keyed on.
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        let (_, blocks) = self.range_query_encoded(rows_offset, &lo, &hi)?;
        Ok(blocks)
    }

    /// Read the entire `Rows.db` file into a buffer for in-trie traversal.
    ///
    /// Unlike `Partitions.db`, a `Rows.db` has no whole-file footer describing a
    /// single root (see [`iterate_rows_in_bti_trie`]); callers supply the
    /// per-partition `RowsOffset` separately.
    fn read_full_rows_db(&mut self) -> BtiResult<Vec<u8>> {
        let file_size = self.reader.seek(SeekFrom::End(0))?;
        self.reader.seek(SeekFrom::Start(0))?;
        let mut buf = vec![0u8; file_size as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Iterator over all row-index blocks of a single partition, given the
    /// partition's `RowsOffset` from `Partitions.db`
    /// ([`BtiPartitionLocation::RowsOffset`]).
    ///
    /// `rows_offset` points at the per-partition `TrieIndexEntry` (NOT the trie
    /// root); this resolves the real root via [`resolve_rows_db_entry`]
    /// (Finding A) before traversing.  Blocks are yielded in ascending
    /// byte-comparable clustering order; each [`BtiRowIndexEntry::data_offset`]
    /// is relative to the partition's Data.db position.
    pub fn iterate_rows(&mut self, rows_offset: usize) -> BtiResult<RowIterator<'_, R>> {
        RowIterator::new(self, rows_offset)
    }

    /// Get header information
    pub fn header(&self) -> &BtiHeader {
        &self.header
    }
}

/// Iterator over **all** partitions in a `Partitions.db` BTI index, in
/// byte-comparable order (issue #832).
///
/// The full trie is loaded via the footer-based loader (NOT the fictional
/// [`BtiHeader`]) and traversed in-order during [`PartitionIterator::new`]; the
/// materialized entries are then yielded one at a time.  BTI partition indexes
/// are tiny (tens of KB even for millions of partitions, thanks to prefix
/// sharing), so eager materialization is acceptable.
///
/// The yielded `Vec<u8>` key is the reconstructed *byte-comparable token* key
/// (concatenated transition bytes), NOT the original partition key.  The
/// [`BtiPartitionLocation`] offset is definitive.
pub struct PartitionIterator<'a, R: Read + Seek> {
    #[allow(dead_code)]
    parser: &'a mut PartitionsParser<R>,
    /// Materialized entries in byte-comparable order.
    entries: std::vec::IntoIter<(Vec<u8>, BtiPartitionLocation)>,
    /// A deferred error to surface on the first `next()` call.
    pending_error: Option<Error>,
}

impl<'a, R: Read + Seek> PartitionIterator<'a, R> {
    fn new(parser: &'a mut PartitionsParser<R>) -> BtiResult<Self> {
        // Load and traverse via the footer-based loader; surface any error on
        // the first `next()` call rather than failing construction (so callers
        // that ignore errors still see a non-silent failure).
        let (entries, pending_error) = match load_bti_trie_via_footer(&mut parser.reader)
            .and_then(|(trie, root)| dfs_collect_partition_entries(&trie, root))
        {
            Ok(v) => (v, None),
            Err(e) => (Vec::new(), Some(e)),
        };
        Ok(Self {
            parser,
            entries: entries.into_iter(),
            pending_error,
        })
    }
}

impl<'a, R: Read + Seek> Iterator for PartitionIterator<'a, R> {
    type Item = BtiResult<(Vec<u8>, BtiPartitionLocation)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.pending_error.take() {
            return Some(Err(err));
        }
        self.entries.next().map(Ok)
    }
}

/// Iterator over the row-index block entries of **one partition's** `Rows.db`
/// row-index trie (issue #832), in ascending byte-comparable clustering order.
///
/// A real `Rows.db` concatenates one per-partition `TrieIndexEntry` + row-index
/// trie per wide partition.  The iterator is created from the partition's
/// `RowsOffset` (from `Partitions.db`); that offset points at the per-partition
/// entry, so the real trie root is resolved via [`resolve_rows_db_entry`]
/// (Finding A) before traversal — it is NOT a whole-file footer root.  The full
/// file is read into memory and traversed in-order during construction.  Each
/// yielded `Vec<u8>` is the reconstructed byte-comparable clustering separator
/// key; the [`BtiRowIndexEntry`] carries the block offset (relative to the
/// partition's Data.db position) and an optional open-deletion `DeletionTime`.
///
/// An empty (e.g. 0-byte) `Rows.db` yields nothing without panicking.
pub struct RowIterator<'a, R: Read + Seek> {
    #[allow(dead_code)]
    parser: &'a mut RowsParser<R>,
    entries: std::vec::IntoIter<(Vec<u8>, BtiRowIndexEntry)>,
    pending_error: Option<Error>,
}

impl<'a, R: Read + Seek> RowIterator<'a, R> {
    fn new(parser: &'a mut RowsParser<R>, rows_offset: usize) -> BtiResult<Self> {
        // An empty Rows.db (e.g. a 0-byte file for SSTables with no row index)
        // yields nothing rather than erroring.
        let file_size = parser.reader.seek(SeekFrom::End(0))?;
        if file_size == 0 {
            return Ok(Self {
                parser,
                entries: Vec::new().into_iter(),
                pending_error: None,
            });
        }

        // Finding A: resolve the per-partition TrieIndexEntry at `rows_offset`
        // to recover the actual trie root, then traverse from THAT root.
        let (entries, pending_error) = match parser.read_full_rows_db().and_then(|trie| {
            let header = resolve_rows_db_entry(&trie, rows_offset)?;
            iterate_rows_in_bti_trie(&trie, header.trie_root)
        }) {
            Ok(v) => (v, None),
            Err(e) => (Vec::new(), Some(e)),
        };
        Ok(Self {
            parser,
            entries: entries.into_iter(),
            pending_error,
        })
    }
}

impl<'a, R: Read + Seek> Iterator for RowIterator<'a, R> {
    type Item = BtiResult<(Vec<u8>, BtiRowIndexEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.pending_error.take() {
            return Some(Err(err));
        }
        self.entries.next().map(Ok)
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
                assert_eq!(children[0].as_ref().unwrap().distance, offset - 0x123);
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
                assert_eq!(children[0].as_ref().unwrap().distance, offset - 0x100);
                assert_eq!(children[1].as_ref().unwrap().distance, offset - 0x200);
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
                assert_eq!(children[0].as_ref().unwrap().distance, 0x200 - 0x0010);
                // child 1 (b'b'): delta=0 → no transition → None
                assert!(children[1].is_none());
                // child 2 (b'c'): offset = 0x200 - 0x0030 = 0x1D0
                assert_eq!(children[2].as_ref().unwrap().distance, 0x200 - 0x0030);
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
                assert_eq!(children[0].as_ref().unwrap().distance, 0x10000 - 0x100);
                assert_eq!(children[1].as_ref().unwrap().distance, 0x10000 - 0x200);
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
    // issue #832 — full trie traversal (DFS) unit tests
    // -----------------------------------------------------------------------
    //
    // These hand-craft in-memory tries (no external files) and exercise the new
    // free-function DFS/range primitives directly.  They always run in CI.

    /// Build a complete in-memory Partitions.db-style file (trie + 8-byte
    /// big-endian root-offset footer).
    fn make_partitions_db(trie_bytes: Vec<u8>, root_offset: u64) -> Vec<u8> {
        let mut v = trie_bytes;
        v.extend_from_slice(&root_offset.to_be_bytes());
        v
    }

    /// A `Partitions.db` PayloadOnly leaf with payloadBits=8 (hash + 1-byte
    /// SizedInts position).  `position` is the *signed* byte; negative →
    /// DataOffset(~position).
    fn partition_leaf(hash: u8, position: i8) -> Vec<u8> {
        vec![0x08, hash, position as u8]
    }

    /// (1) partition DFS over a synthetic Sparse trie yields entries in
    /// ascending transition-byte order with correct payloads/offsets, and the
    /// reconstructed keys are the transition bytes.
    #[test]
    fn dfs_partition_sparse_ascending_order_with_offsets() {
        // Leaves (bottom-up):
        //   offset 0: leaf hash=0x11 pos=-1  → DataOffset(0)
        //   offset 3: leaf hash=0x22 pos=-65 → DataOffset(64)
        //   offset 6: Sparse8 count=2  trans 0xAA→0 (delta6), 0xBB→3 (delta3)
        let mut trie = vec![0u8; 12];
        trie[0..3].copy_from_slice(&partition_leaf(0x11, -1));
        trie[3..6].copy_from_slice(&partition_leaf(0x22, -65));
        trie[6] = 0x50; // Sparse8
        trie[7] = 0x02; // count
        trie[8] = 0xAA;
        trie[9] = 0xBB;
        trie[10] = 0x06; // child = 6-6 = 0
        trie[11] = 0x03; // child = 6-3 = 3

        let entries = dfs_collect_partition_entries(&trie, 6).unwrap();
        assert_eq!(
            entries,
            vec![
                (vec![0xAA], BtiPartitionLocation::DataOffset(0)),
                (vec![0xBB], BtiPartitionLocation::DataOffset(64)),
            ],
            "Sparse DFS must emit ascending transition bytes with correct offsets"
        );
    }

    /// (2) partition DFS over a Dense node skips distance==0 gaps and emits in
    /// start_byte+i order.
    #[test]
    fn dfs_partition_dense_skips_gaps() {
        // A leading pad byte ensures the real children never resolve to absolute
        // offset 0 (which would collide with the Dense "no transition" sentinel).
        //   offset 0: pad
        //   offset 1: leaf → DataOffset(0)
        //   offset 4: leaf → DataOffset(64)
        //   Dense16 root: start_byte=0x10, range_len=3
        //     index 0 (byte 0x10): → child at offset 1
        //     index 1 (byte 0x11): delta=0 → NO TRANSITION (skip)
        //     index 2 (byte 0x12): → child at offset 4
        let mut trie = vec![0x00u8]; // pad at offset 0
        let l1 = trie.len() as u64; // 1
        trie.extend_from_slice(&partition_leaf(0x11, -1)); // DataOffset(0)
        let l2 = trie.len() as u64; // 4
        trie.extend_from_slice(&partition_leaf(0x22, -65)); // DataOffset(64)
        let dense_off = trie.len() as u64; // 7
        trie.push(0xB0); // Dense16 (ordinal 11)
        trie.push(0x10); // start_byte
        trie.push(0x02); // range_len - 1 = 2 → 3 entries
        trie.extend_from_slice(&((dense_off - l1) as u16).to_be_bytes()); // 0x10 → leaf 1
        trie.extend_from_slice(&0u16.to_be_bytes()); // 0x11 → gap (sentinel)
        trie.extend_from_slice(&((dense_off - l2) as u16).to_be_bytes()); // 0x12 → leaf 4

        let entries = dfs_collect_partition_entries(&trie, dense_off as usize).unwrap();
        assert_eq!(
            entries,
            vec![
                (vec![0x10], BtiPartitionLocation::DataOffset(0)),
                (vec![0x12], BtiPartitionLocation::DataOffset(64)),
            ],
            "Dense DFS must skip distance==0 gaps and emit start_byte+i order"
        );
    }

    /// (3) partition DFS emits an internal node's own payload BEFORE its children.
    #[test]
    fn dfs_partition_internal_payload_before_children() {
        // Layout (bottom-up):
        //   offset 0: child leaf → DataOffset(0)
        //   offset 3: Single8 WITH its own payload (payloadBits=8):
        //             [0x28][transition=0xCC][delta=3][hash][pos]
        //             pos=-65 → the node's own payload = DataOffset(64)
        //             child via 0xCC = offset 3-3 = 0 → DataOffset(0)
        let mut trie = Vec::new();
        trie.extend_from_slice(&partition_leaf(0x11, -1)); // offset 0 → DataOffset(0)
        let node_off = trie.len() as u64; // 3
        trie.push(0x28); // Single8 (ordinal 2), payloadBits=8
        trie.push(0xCC); // transition byte
        trie.push(node_off as u8); // delta=3 → child at offset 0
        trie.push(0x99); // payload hash
        trie.push((-65i8) as u8); // payload position → DataOffset(64)

        let entries = dfs_collect_partition_entries(&trie, node_off as usize).unwrap();
        assert_eq!(
            entries,
            vec![
                // The node's OWN payload (empty key prefix) sorts first.
                (vec![], BtiPartitionLocation::DataOffset(64)),
                // Then its child via transition 0xCC.
                (vec![0xCC], BtiPartitionLocation::DataOffset(0)),
            ],
            "An internal node's payload must be emitted before its children"
        );
    }

    // ----- Rows.db-style synthetic tries (issue #832) -----

    /// A `Rows.db` PayloadOnly leaf with no open marker (payloadBits=1): a
    /// single-byte unsigned-vint Data.db position (value 0..=127).
    fn row_leaf_no_marker(pos: u8) -> Vec<u8> {
        assert!(pos <= 127, "use a 1-byte unsigned vint position");
        vec![0x01, pos] // ordinal=0, payloadBits=1 (no FLAG_OPEN_MARKER)
    }

    /// Build a 3-leaf Rows.db-style trie via a Sparse8 root.  Returns
    /// `(trie_bytes, root_offset)`.  Transition bytes k1<k2<k3 map to Data.db
    /// positions p1,p2,p3.
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

    /// (5) RowIterator over a synthetic Rows.db-style trie yields clustering
    /// keys in byte order with correct Rows.db-decoded payloads.  We exercise
    /// the underlying DFS row collector directly.
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
    /// absolute trie offset 0 (the first-written leaf in BTI's bottom-up layout)
    /// AND that has a "no transition" gap elsewhere.  DFS must:
    ///   - emit the offset-0 child (NOT silently drop it), and
    ///   - skip the gap byte.
    ///
    /// Layout (bottom-up; children at lower offsets):
    ///   offset 0: Rows leaf  pos=5   (the offset-0 child)
    ///   offset 2: Rows leaf  pos=9
    ///   offset 4: Dense16 root, start=0x10, range 3:
    ///       0x10 → delta 4 → child offset 0  (real, absolute offset 0)
    ///       0x11 → delta 0 → no transition   (sentinel)
    ///       0x12 → delta 2 → child offset 2  (real)
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

        // And find_child on the parsed Dense root must agree.
        let node = parse_bti_node_for_traversal(&trie, root as usize).unwrap();
        let c10 = node.find_child(0x10).expect("0x10 child must be found");
        assert_eq!(c10.distance, 0, "0x10 must route to absolute offset 0");
        assert!(
            node.find_child(0x11).is_none(),
            "0x11 is the no-transition gap"
        );
        assert!(node.find_child(0x12).is_some());
    }

    /// Rows.db payload with FLAG_OPEN_MARKER decodes a trailing MODERN DA
    /// DeletionTime (issue #832 Finding 2): `[i64 markedForDeleteAt][u32
    /// localDeletionTime]` — markedForDeleteAt FIRST.
    #[test]
    fn decode_row_payload_open_marker_modern() {
        // payloadBits = 0x9 → FLAG_OPEN_MARKER (0x8) set.
        // payload: [pos vint = 7][i64 markedForDeleteAt=567890][u32 localDeletionTime=1234]
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
    /// to NO open deletion (issue #832 Finding 2): the live fast-path consumes
    /// only 1 byte and yields `open_marker == None`.
    #[test]
    fn decode_row_payload_open_marker_live_sentinel() {
        // payloadBits = 0x9 → FLAG_OPEN_MARKER set; payload: [pos=7][0x80 live].
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
    /// Finding 2): the `0x80` sentinel → live (None, 1 byte); a non-live value
    /// → `[i64 markedForDeleteAt][u32 localDeletionTime]` in the CORRECT order
    /// and widths (12 bytes); a truncated non-live value errors; an
    /// out-of-bounds start errors.
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

        // The leading byte of mfda here is 0x00 (not 0x80), so this is correctly
        // treated as non-live rather than the live sentinel.
        assert_ne!(buf[0], 0x80);

        // Truncated non-live value (leading byte != 0x80 but < 12 bytes) errors.
        assert!(decode_da_deletion_time(&[0x00, 0x01, 0x02], 0).is_err());
        // Out-of-bounds start errors.
        assert!(decode_da_deletion_time(&[0x00], 5).is_err());
    }

    /// The OSS50 byte-comparable clustering encoder (issue #832 Finding 1)
    /// reproduces the on-disk trie separator bytes: `int` is sign-flip BE
    /// (ck=8 → 80 00 00 08), and a multi-component clustering joins components
    /// with the `0x40` NEXT_COMPONENT separator.  Unsupported clustering types
    /// error out explicitly (no silent wrong results).
    #[test]
    fn oss50_clustering_encoder() {
        // Single int component — bare, sign-flip big-endian (matches fixture).
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(8)]).unwrap(),
            vec![0x80, 0x00, 0x00, 0x08]
        );
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(-1)]).unwrap(),
            vec![0x7F, 0xFF, 0xFF, 0xFF]
        );
        // Ordering is preserved: -1 < 0 < 100 byte-comparably.
        let neg = encode_clustering_bound_oss50(&[Value::Integer(-1)]).unwrap();
        let zero = encode_clustering_bound_oss50(&[Value::Integer(0)]).unwrap();
        let pos = encode_clustering_bound_oss50(&[Value::Integer(100)]).unwrap();
        assert!(neg < zero && zero < pos);

        // bigint — 8-byte sign-flip BE.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::BigInt(1)]).unwrap(),
            vec![0x80, 0, 0, 0, 0, 0, 0, 0x01]
        );

        // Multi-component: int(1) + text("ab") joined with 0x40 NEXT_COMPONENT,
        // text terminated by the OSS50 end marker 0x00 0xFF.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(1), Value::Text("ab".to_string())])
                .unwrap(),
            vec![0x80, 0x00, 0x00, 0x01, 0x40, b'a', b'b', 0x00, 0xFF]
        );

        // Variable-length OSS50: text terminates with 0x00 0xFF; a literal 0x00
        // byte is escaped as 0x00 0xFE so it never collides with the terminator.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Text("a".to_string())]).unwrap(),
            vec![b'a', 0x00, 0xFF]
        );
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Blob(vec![0x01, 0x00, 0x02])]).unwrap(),
            vec![0x01, 0x00, 0xFE, 0x02, 0x00, 0xFF]
        );
        // Prefix-free ordering: "a" sorts before "ab" (00 0xFE/0xFF < any byte
        // after the shared prefix means the shorter value compares first).
        let a = encode_clustering_bound_oss50(&[Value::Text("a".to_string())]).unwrap();
        let ab = encode_clustering_bound_oss50(&[Value::Text("ab".to_string())]).unwrap();
        assert!(a < ab);

        // Unsupported clustering type errors out explicitly.
        assert!(encode_clustering_bound_oss50(&[Value::Float(1.0)]).is_err());
    }

    /// Order-aware OSS50 encoder (DESC clustering): a `CLUSTERING ORDER BY DESC`
    /// column wraps in Cassandra `ReversedType`, whose `asComparableBytes`
    /// complements every byte of the base byte-comparable form
    /// (`ByteSource.invert`). The result: DESCENDING value order maps to
    /// ASCENDING byte order, so a `Rows.db` row-index trie can store the
    /// separators (which must be strictly ascending bytes).
    #[test]
    fn oss50_clustering_encoder_reversed_order() {
        // DESC int(8): base 80 00 00 08, complemented -> 7F FF FF F7.
        assert_eq!(
            encode_clustering_bound_oss50_with_order(&[Value::Integer(8)], &[true]).unwrap(),
            vec![0x7F, 0xFF, 0xFF, 0xF7]
        );
        // ASC path is identical to the plain encoder.
        assert_eq!(
            encode_clustering_bound_oss50_with_order(&[Value::Integer(8)], &[false]).unwrap(),
            encode_clustering_bound_oss50(&[Value::Integer(8)]).unwrap()
        );

        // The decisive property: for DESC, larger VALUE => smaller BYTES, i.e.
        // writing rows in descending value order yields ascending separator
        // bytes. Take values 0,1,..,5 (the physical DESC write order is 5..0).
        let enc = |v: i32| {
            encode_clustering_bound_oss50_with_order(&[Value::Integer(v)], &[true]).unwrap()
        };
        // Physical write order for DESC: 5,4,3,2,1,0. Their separator bytes must
        // be strictly ASCENDING in that same sequence.
        let write_order = [5, 4, 3, 2, 1, 0];
        for w in write_order.windows(2) {
            assert!(
                enc(w[0]) < enc(w[1]),
                "DESC: value {} written before {} must yield smaller separator bytes",
                w[0],
                w[1]
            );
        }

        // Mixed ASC/DESC: first column ASC int, second column DESC int. The
        // NEXT_COMPONENT (0x40) framing byte is NOT inverted; only the DESC
        // component's bytes are complemented.
        let mixed = encode_clustering_bound_oss50_with_order(
            &[Value::Integer(1), Value::Integer(8)],
            &[false, true],
        )
        .unwrap();
        assert_eq!(
            mixed,
            vec![0x80, 0x00, 0x00, 0x01, 0x40, 0x7F, 0xFF, 0xFF, 0xF7],
            "ASC component bare, 0x40 framing un-inverted, DESC component complemented"
        );

        // Mixed-order monotonicity: for equal leading ASC component, the trailing
        // DESC component must reverse the byte order (descending value second
        // component => ascending bytes).
        let m = |a: i32, b: i32| {
            encode_clustering_bound_oss50_with_order(
                &[Value::Integer(a), Value::Integer(b)],
                &[false, true],
            )
            .unwrap()
        };
        // Same first key (1), second key descending 9,8,7 -> ascending bytes.
        assert!(m(1, 9) < m(1, 8) && m(1, 8) < m(1, 7));
        // Different first key dominates regardless of second (ASC leading).
        assert!(m(1, 0) < m(2, 9));
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

    /// (4) range filter over a synthetic 3-leaf rows-style trie returns the
    /// correct subset (k1..=k2 excludes k3), empty for below/above range, and
    /// empty for reversed bounds.  This exercises the byte-level filter that
    /// `range_query` applies on top of `dfs_collect_row_entries`.
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
    /// row-index SEPARATOR semantics — a block whose half-open key interval
    /// `[s_i, s_{i+1})` overlaps `[start, end]` is included, including the FLOOR
    /// block whose separator is below `start`.  This is the property naive
    /// `[start, end]` membership filtering gets wrong.
    #[test]
    fn select_blocks_separator_semantics() {
        // Three separators (block boundaries) at byte-comparable keys 0x10, 0x20,
        // 0x30 with offsets 5, 17, 99.  Block intervals:
        //   block@5  : [0x10, 0x20)
        //   block@17 : [0x20, 0x30)
        //   block@99 : [0x30, +inf)
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

        // A key strictly inside the FIRST block's interval ([0x10,0x20)): the
        // floor block @5 must be selected even though its separator (0x10) is
        // <= start (0x18) — naive `start<=sep<=end` would WRONGLY drop it.
        assert_eq!(
            offs(&[0x18], &[0x18]),
            vec![5],
            "floor block must be selected"
        );

        // A range spanning block 1 into block 2 selects both.
        assert_eq!(offs(&[0x18], &[0x28]), vec![5, 17]);

        // A range starting exactly at a separator includes that block (and the
        // straddling next is excluded because its s_{i} > end).
        assert_eq!(offs(&[0x20], &[0x2F]), vec![17]);

        // A range above the last separator selects only the open-ended last block.
        assert_eq!(offs(&[0x40], &[0x50]), vec![99]);

        // Below the first separator: block@5's interval [0x10,0x20) does not
        // overlap [0x00,0x0F], so nothing is selected.
        assert_eq!(offs(&[0x00], &[0x0F]), Vec::<u64>::new());

        // Full range selects all blocks.
        assert_eq!(offs(&[0x00], &[0xFF]), vec![5, 17, 99]);

        // Reversed bounds → empty.
        assert_eq!(offs(&[0x30], &[0x10]), Vec::<u64>::new());

        // Empty entries → empty.
        assert!(select_row_index_blocks_for_range(&[], &[0x00], &[0xFF]).is_empty());
    }

    /// Finding A (issue #832): `resolve_rows_db_entry` deserializes a synthetic
    /// per-partition `TrieIndexEntry` and recovers the trie root via
    /// `indexTrieRoot = readVInt() + (RowsOffset + key_length)`.  Mirrors the
    /// real `wide_table` layout: `[u16 keylen][key][dataPos uvint][rootΔ svint]
    /// [blockCount uvint][modern DA deletion]`.  The deletion here is a non-live
    /// MODERN value `[i64 markedForDeleteAt][u32 localDeletionTime]` (Finding 2).
    #[test]
    fn resolve_rows_db_entry_recovers_root_and_metadata() {
        // Place a 1-byte pad so the trie "root" lives at a non-zero offset.
        let mut buf = vec![0xEEu8; 4]; // bytes 0..4: pretend trie nodes
        let rows_offset = buf.len(); // entry starts here, e.g. 4

        // key: length 4, value 0x00000007
        buf.extend_from_slice(&4u16.to_be_bytes());
        buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]);
        let base = rows_offset + 4; // RowsOffset + key_length

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
        // (Leading byte 0x00 != 0x80 live sentinel.)
        buf.extend_from_slice(&17i64.to_be_bytes());
        buf.extend_from_slice(&9u32.to_be_bytes());

        let header = resolve_rows_db_entry(&buf, rows_offset).unwrap();
        assert_eq!(header.data_position, 123);
        assert_eq!(
            header.trie_root, 2,
            "trie root = rootΔ + (RowsOffset + keylen)"
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
        let base = rows_offset + 4;
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

    /// The Rows.db payload offset is a SizedInts value (NOT an unsigned vint):
    /// `bytes = payloadBits & ~FLAG_OPEN_MARKER`.  A 2-byte SizedInts offset of
    /// 0x4080 (=16512, the real first block offset) must decode correctly.
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

    /// PartitionIterator footer-based loading: build a complete in-memory
    /// Partitions.db (trie + footer) and prove the footer loader + DFS produce
    /// the correct entries (re-platformed off BtiHeader).
    #[test]
    fn partition_iterator_full_traversal_synthetic() {
        let mut trie = vec![0u8; 12];
        trie[0..3].copy_from_slice(&partition_leaf(0x11, -1));
        trie[3..6].copy_from_slice(&partition_leaf(0x22, -65));
        trie[6] = 0x50;
        trie[7] = 0x02;
        trie[8] = 0xAA;
        trie[9] = 0xBB;
        trie[10] = 0x06;
        trie[11] = 0x03;
        let file = make_partitions_db(trie, 6);

        let (trie_data, root) = load_bti_trie_via_footer(&mut Cursor::new(file)).unwrap();
        assert_eq!(root, 6);
        let entries = dfs_collect_partition_entries(&trie_data, root).unwrap();
        assert_eq!(
            entries,
            vec![
                (vec![0xAA], BtiPartitionLocation::DataOffset(0)),
                (vec![0xBB], BtiPartitionLocation::DataOffset(64)),
            ]
        );
    }

    /// Finding 2 (issue #832): the rooted Rows.db API must handle the empty /
    /// out-of-bounds cases cleanly (no panic).  Empty `trie_data` or a root
    /// beyond the buffer yields a clean parse error; a real partition root is
    /// what callers pass in production (from `Partitions.db` `RowsOffset`).
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
    /// key.  This exercises the byte-comparable inclusive filter applied on top
    /// of the DFS — the same filter `RowsParser::range_query` uses — and proves
    /// that prefixes are neither spuriously dropped nor included.
    ///
    /// Trie layout (bottom-up):
    ///   K  = [0x10]        (internal node, carries a payload: pos=1)
    ///   K2 = [0x10, 0x20]  (deeper leaf: pos=2)
    ///
    ///   offset 0: Rows leaf pos=2                         → K2 payload
    ///   offset 2: Single8 trans=0x20 delta=2, payload pos=1 (the K node)
    ///   offset 5: Single8 root trans=0x10 delta=3, no payload
    ///
    /// So root --0x10--> K(internal, payload) --0x20--> K2(leaf).
    #[test]
    fn range_filter_prefix_relationship_is_sound() {
        let mut trie = Vec::new();
        // offset 0: leaf for K2 = [0x10,0x20], pos=2
        trie.extend_from_slice(&row_leaf_no_marker(2));
        // offset 2: Single8 with payload — the K = [0x10] node.
        //   header 0x21 (ordinal=2 Single8, payloadBits=1), transition=0x20,
        //   delta=2 (child = 2 - 2 = 0 → the K2 leaf), then payload pos=1.
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

        // [K..=K] returns K but NOT K2 (K2 = [0x10,0x20] > [0x10]).
        assert_eq!(
            filter(&k, &k),
            vec![1],
            "[K..=K] must include K and exclude the longer K2"
        );
        // [K..=K2] returns both.
        assert_eq!(
            filter(&k, &k2),
            vec![1, 2],
            "[K..=K2] must include both K and K2"
        );
        // A bound strictly between K and K2 excludes both:
        //   [0x10,0x00] ..= [0x10,0x10]; K=[0x10] < [0x10,0x00], K2=[0x10,0x20] > [0x10,0x10].
        assert_eq!(
            filter(&[0x10, 0x00], &[0x10, 0x10]),
            Vec::<u64>::new(),
            "a range strictly between K and K2 must exclude both"
        );
    }
}
