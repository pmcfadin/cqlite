//! Shared BTI trie-node decoding helpers.
//!
//! This submodule holds the low-level node parsers shared by every BTI read
//! path: nibble classification, pointer-width lookup, the 16-ordinal
//! [`parse_bti_node`] dispatcher, and the small big-endian / 12-bit packed
//! readers it relies on.  See the [parent module](super) doc for the
//! node-type encoding table.

use crate::{
    error::Error,
    storage::sstable::bti::node::{
        BtiNode, BtiNodeData, BtiNodeType, BtiResult, PayloadRef, SizedPointer, Transition,
    },
};

/// Classify the high nibble of a BTI node's first byte into one of the four
/// Rust-level node categories.
///
/// The mapping follows the 16-entry `Types.values[]` array in `TrieNode.java`:
/// - nibble 0               → `PayloadOnly`
/// - nibbles 1-4 (Singles)  → `Single`
/// - nibbles 5-9 (Sparses)  → `Sparse`
/// - nibbles 10-15 (Denses) → `Dense`
pub(crate) fn classify_node_nibble(nibble: u8) -> BtiResult<BtiNodeType> {
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
pub(crate) fn pointer_bytes_for_ordinal(ordinal: u8) -> u8 {
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

/// Parse a `PayloadOnly` (ordinal 0) node.  PayloadOnly MUST carry a payload
/// (it is a leaf node), decoded via the legacy 12-byte [`parse_payload_ref`].
fn parse_payload_only_node(data: &[u8], has_payload: bool) -> BtiResult<BtiNode> {
    if !has_payload {
        return Err(Error::Parse(
            "PayloadOnly node has no payload flags set".to_string(),
        ));
    }
    let payload = parse_payload_ref(&data[1..])?;
    Ok(BtiNode {
        node_type: BtiNodeType::PayloadOnly,
        level: 0,
        key_prefix: Vec::new(),
        data: BtiNodeData::PayloadOnly { payload },
    })
}

/// Parse a `Single` (ordinals 1-4) node.
///
/// Three concrete layouts depending on ordinal:
///
///   ordinal 1 (SingleNoPayload4):
///     byte 0: [1|delta_high4]  (delta is low 4 bits of byte 0)
///     byte 1: transition byte; NO payload
///   ordinal 3 (SingleNoPayload12):
///     byte 0: [3|delta_high4]
///     byte 1: delta_low8 → delta = (low4(byte0) << 8) | byte1
///     byte 2: transition byte; NO payload
///   ordinals 2, 4 (Single8, Single16):
///     byte 0: [ordinal|payload_flags]
///     byte 1: transition byte
///     bytes 2..(2+ptr_bytes): backward delta (unsigned big-endian)
///     [payload if has_payload]
fn parse_single_node(data: &[u8], offset: u64, ordinal: u8, header_byte: u8) -> BtiResult<BtiNode> {
    crate::storage::sstable::read_work_counters::record_bti_pointer_decode(); // H5 (#1650): 1 child
    let transition = match ordinal {
        1 => {
            // SingleNoPayload4
            if data.len() < 2 {
                return Err(Error::Parse(
                    "SingleNoPayload4 node data too short".to_string(),
                ));
            }
            let delta = (header_byte & 0x0F) as u64;
            let child_offset = offset.saturating_sub(delta);
            Transition::new(data[1], SizedPointer::new(child_offset))
        }
        3 => {
            // SingleNoPayload12
            if data.len() < 3 {
                return Err(Error::Parse(
                    "SingleNoPayload12 node data too short".to_string(),
                ));
            }
            let delta = (((header_byte & 0x0F) as u64) << 8) | (data[1] as u64);
            let child_offset = offset.saturating_sub(delta);
            Transition::new(data[2], SizedPointer::new(child_offset))
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
            let delta = read_be_unsigned(&data[2..2 + ptr_bytes]);
            let child_offset = offset.saturating_sub(delta);
            Transition::new(data[1], SizedPointer::new(child_offset))
        }
    };
    Ok(BtiNode {
        node_type: BtiNodeType::Single,
        level: 1,
        key_prefix: Vec::new(),
        data: BtiNodeData::Single { transition },
    })
}

/// Parse a `Sparse` (ordinals 5-9) node.
///
/// Layout (ordinals 5, 7, 8, 9 — full-byte pointers):
///   byte 0: [ordinal|payload_flags]
///   byte 1: count (number of transitions, 1-255)
///   bytes 2..(2+count): transition bytes (sorted)
///   then count × ptr_bytes: backward deltas
///   [payload if has_payload]
///
/// ordinal 6 (Sparse12) packs two 12-bit deltas into 3 bytes.
fn parse_sparse_node(data: &[u8], offset: u64, ordinal: u8) -> BtiResult<BtiNode> {
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

    let mut transitions = Vec::with_capacity(count);
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
        for i in 0..count {
            crate::storage::sstable::read_work_counters::record_bti_pointer_decode(); // H5 (#1650)
            let t_byte = data[bytes_start + i];
            let delta = read_12bit_packed(&data[pointers_start..], i);
            let child_offset = offset.saturating_sub(delta);
            transitions.push(Transition::new(t_byte, SizedPointer::new(child_offset)));
        }
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
        for i in 0..count {
            crate::storage::sstable::read_work_counters::record_bti_pointer_decode(); // H5 (#1650)
            let t_byte = data[bytes_start + i];
            let ptr_off = pointers_start + i * ptr_bytes;
            let delta = read_be_unsigned(&data[ptr_off..ptr_off + ptr_bytes]);
            let child_offset = offset.saturating_sub(delta);
            transitions.push(Transition::new(t_byte, SizedPointer::new(child_offset)));
        }
    }
    Ok(BtiNode {
        node_type: BtiNodeType::Sparse,
        level: 1,
        key_prefix: Vec::new(),
        data: BtiNodeData::Sparse { transitions },
    })
}

/// Parse a `Dense` (ordinals 10-15) node.
///
/// Layout (ordinals 11-14 — full-byte pointers): byte 0 `[ordinal|payload_flags]`,
/// byte 1 start byte, byte 2 `(range_len - 1)`, then `range_len × ptr_bytes` backward
/// deltas (`0` means "no transition"), then `[payload if has_payload]`.
/// ordinal 10 (Dense12): packed 12-bit deltas; ordinal 15 (LongDense): 8-byte.
fn parse_dense_node(data: &[u8], offset: u64, ordinal: u8) -> BtiResult<BtiNode> {
    if data.len() < 3 {
        return Err(Error::Parse("Dense node data too short".to_string()));
    }
    let start_byte = data[1];
    let range_len = data[2] as usize + 1; // byte2 is (len-1)

    let mut children = Vec::with_capacity(range_len);
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
        for i in 0..range_len {
            crate::storage::sstable::read_work_counters::record_bti_pointer_decode(); // H5 (#1650)
            let delta = read_12bit_packed(&data[3..], i);
            children.push(dense_child(offset, delta));
        }
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
        for i in 0..range_len {
            crate::storage::sstable::read_work_counters::record_bti_pointer_decode(); // H5 (#1650)
            let ptr_off = 3 + i * ptr_bytes;
            let delta = read_be_unsigned(&data[ptr_off..ptr_off + ptr_bytes]);
            children.push(dense_child(offset, delta));
        }
    }
    Ok(BtiNode {
        node_type: BtiNodeType::Dense,
        level: 1,
        key_prefix: Vec::new(),
        data: BtiNodeData::Dense {
            start_byte,
            children,
        },
    })
}

/// Resolve one Dense child slot from its raw backward delta.
///
/// `delta == 0` is the "no transition" sentinel → `None`.  Any other delta is
/// a real child at `offset - delta` (which may be absolute offset 0 — a
/// legitimate child, distinct from the sentinel).
fn dense_child(offset: u64, delta: u64) -> Option<SizedPointer> {
    if delta == 0 {
        None
    } else {
        Some(SizedPointer::new(offset.saturating_sub(delta)))
    }
}

/// Parse a BTI trie node from a raw byte slice.
///
/// This is the **single shared implementation** used by both the partition and
/// row parsers.  It correctly dispatches all 16 node-type ordinals defined in
/// `TrieNode.java` and produces the appropriate [`BtiNode`] variant.
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
pub(crate) fn parse_bti_node(data: &[u8], offset: u64) -> BtiResult<BtiNode> {
    if data.is_empty() {
        return Err(Error::Parse("Empty BTI node data".to_string()));
    }

    let header_byte = data[0];
    let ordinal = (header_byte >> 4) & 0x0F;
    let payload_flags = header_byte & 0x0F;
    let has_payload = payload_flags != 0;
    let node_type = classify_node_nibble(ordinal)?;

    match node_type {
        BtiNodeType::PayloadOnly => parse_payload_only_node(data, has_payload),
        BtiNodeType::Single => parse_single_node(data, offset, ordinal, header_byte),
        BtiNodeType::Sparse => parse_sparse_node(data, offset, ordinal),
        BtiNodeType::Dense => parse_dense_node(data, offset, ordinal),
    }
}

/// Read a big-endian unsigned integer of 0-8 bytes from `data`.
///
/// Returns 0 for an empty slice; panics if `data.len() > 8` (caller is
/// responsible for bounds-checking first).
pub(crate) fn read_be_unsigned(data: &[u8]) -> u64 {
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
pub(crate) fn read_12bit_packed(data: &[u8], index: usize) -> u64 {
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
pub(crate) fn parse_payload_ref(data: &[u8]) -> BtiResult<PayloadRef> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
    // REGRESSION TEST — proves the pre-fix stub misbehaviour (#647)
    // -----------------------------------------------------------------------
    #[test]
    fn regression_rows_parser_single_node_not_mislabeled_as_payload_only() {
        // Craft a Single8 (ordinal 2) node: nibble = 0x2 → must NOT be PayloadOnly.
        let node_bytes = single8_node(0, b'a', 5);
        let offset: u64 = 100;

        let node = parse_bti_node(&node_bytes, offset)
            .expect("parse_bti_node must succeed for a valid Single8 node");

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

    #[test]
    fn parse_bti_node_sparse12_ordinal6_count1_exact_minimal_5_bytes() {
        // count=1: exact-minimal node is 5 bytes.
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
    // parse_bti_node: Dense12 (ordinal 10) — exact-minimal size tests
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
}
