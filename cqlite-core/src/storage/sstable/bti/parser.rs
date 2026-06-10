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
                let packed_len = (count * 5).div_ceil(2); // ceil(count * 12 / 8)
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

        let results = Vec::new();

        // Navigate to start position
        let _navigator = TrieNavigator::new(self.header.root_offset);

        // For now, just return empty results - full implementation would traverse range
        // TODO: Implement proper range traversal

        Ok(results)
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

        // TODO: Implement proper trie traversal for iteration
        // For now, just mark as finished
        self.finished = true;
        None
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

        // TODO: Implement proper trie traversal for iteration
        // For now, just mark as finished
        self.finished = true;
        None
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
}
