//! BTI (Big Trie Index) parser implementation
//!
//! This module provides parsing capabilities for BTI format components:
//! - Partitions.db BTI index for partition lookups
//! - Rows.db BTI index for clustering key lookups within partitions

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

    /// Parse node data from bytes
    fn parse_node_data(&self, data: &[u8], offset: u64) -> BtiResult<BtiNode> {
        if data.is_empty() {
            return Err(Error::Parse("Empty node data".to_string()));
        }

        let header_byte = data[0];
        let node_type = self.parse_node_type(header_byte)?;
        let has_payload = (header_byte & 0x01) != 0;
        let mut pos = 1;

        match node_type {
            BtiNodeType::PayloadOnly => {
                let payload = if has_payload {
                    let payload_ref = self.parse_payload_ref(&data[pos..])?;
                    let _ = pos + 16; // PayloadRef is typically 16 bytes
                    payload_ref
                } else {
                    return Err(Error::Parse(
                        "PayloadOnly node must have payload".to_string(),
                    ));
                };

                Ok(BtiNode {
                    node_type,
                    level: 0,
                    key_prefix: Vec::new(),
                    data: BtiNodeData::PayloadOnly { payload },
                })
            }

            BtiNodeType::Single => {
                if pos >= data.len() {
                    return Err(Error::Parse("Single node data too short".to_string()));
                }

                let byte = data[pos];
                pos += 1;

                let child_pointer = self.parse_sized_pointer(&data[pos..], offset)?;
                let _ = pos + 8; // Assuming 8-byte pointers for simplicity

                let transition = Transition::new(byte, child_pointer);

                Ok(BtiNode {
                    node_type,
                    level: 1,
                    key_prefix: Vec::new(),
                    data: BtiNodeData::Single { transition },
                })
            }

            BtiNodeType::Sparse => {
                if pos >= data.len() {
                    return Err(Error::Parse("Sparse node data too short".to_string()));
                }

                let transition_count = data[pos] as usize;
                pos += 1;

                let mut transitions = Vec::with_capacity(transition_count);

                // Read transition bytes
                let mut bytes = Vec::with_capacity(transition_count);
                for _ in 0..transition_count {
                    if pos >= data.len() {
                        return Err(Error::Parse(
                            "Sparse node transitions data too short".to_string(),
                        ));
                    }
                    bytes.push(data[pos]);
                    pos += 1;
                }

                // Read transition pointers
                for byte in bytes {
                    let child_pointer = self.parse_sized_pointer(&data[pos..], offset)?;
                    pos += 8;
                    transitions.push(Transition::new(byte, child_pointer));
                }

                Ok(BtiNode {
                    node_type,
                    level: 1,
                    key_prefix: Vec::new(),
                    data: BtiNodeData::Sparse { transitions },
                })
            }

            BtiNodeType::Dense => {
                if pos + 1 >= data.len() {
                    return Err(Error::Parse("Dense node data too short".to_string()));
                }

                let start_byte = data[pos];
                let end_byte = data[pos + 1];
                pos += 2;

                let range_size = (end_byte - start_byte + 1) as usize;
                let mut children = Vec::with_capacity(range_size);

                for _ in 0..range_size {
                    let child_pointer = self.parse_sized_pointer(&data[pos..], offset)?;
                    pos += 8;
                    children.push(child_pointer);
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

    /// Parse node type from header byte
    fn parse_node_type(&self, header_byte: u8) -> BtiResult<BtiNodeType> {
        match (header_byte >> 4) & 0x0F {
            0 => Ok(BtiNodeType::PayloadOnly),
            1 => Ok(BtiNodeType::Single),
            2 => Ok(BtiNodeType::Sparse),
            3 => Ok(BtiNodeType::Dense),
            other => Err(Error::Parse(format!("Invalid node type: {}", other))),
        }
    }

    /// Parse payload reference
    fn parse_payload_ref(&self, data: &[u8]) -> BtiResult<PayloadRef> {
        if data.len() < 12 {
            return Err(Error::Parse("PayloadRef data too short".to_string()));
        }

        let offset = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);

        let length = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);

        Ok(PayloadRef::new(offset, length))
    }

    /// Parse sized pointer
    fn parse_sized_pointer(&self, data: &[u8], _base_offset: u64) -> BtiResult<SizedPointer> {
        if data.len() < 8 {
            return Err(Error::Parse("SizedPointer data too short".to_string()));
        }

        let distance = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);

        Ok(SizedPointer::new(distance))
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

    /// Parse node data from bytes (reuse partitions parser logic)
    fn parse_node_data(&self, data: &[u8], offset: u64) -> BtiResult<BtiNode> {
        // Implementation is the same as PartitionsParser::parse_node_data
        // TODO: Extract to common utility function
        if data.is_empty() {
            return Err(Error::Parse("Empty node data".to_string()));
        }

        let header_byte = data[0];
        let _node_type = self.parse_node_type(header_byte)?;

        // For now, return a simple payload node
        let payload = PayloadRef::new(offset + 1, 0);

        Ok(BtiNode {
            node_type: BtiNodeType::PayloadOnly,
            level: 0,
            key_prefix: Vec::new(),
            data: BtiNodeData::PayloadOnly { payload },
        })
    }

    /// Parse node type from header byte
    fn parse_node_type(&self, header_byte: u8) -> BtiResult<BtiNodeType> {
        match (header_byte >> 4) & 0x0F {
            0 => Ok(BtiNodeType::PayloadOnly),
            1 => Ok(BtiNodeType::Single),
            2 => Ok(BtiNodeType::Sparse),
            3 => Ok(BtiNodeType::Dense),
            other => Err(Error::Parse(format!("Invalid node type: {}", other))),
        }
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
        let mut data = Vec::new();
        data.extend_from_slice(&BtiHeader::MAGIC.to_be_bytes());
        data.extend_from_slice(&BtiHeader::VERSION.to_be_bytes());
        data.extend_from_slice(&0u16.to_be_bytes()); // flags
        data.extend_from_slice(&64u64.to_be_bytes()); // root_offset
        data.extend_from_slice(&10u64.to_be_bytes()); // entry_count
        data.extend_from_slice(&0u32.to_be_bytes()); // metadata_size

        // Pad to root offset
        while data.len() < 64 {
            data.push(0);
        }

        // Simple root node
        data.push(0x01); // PayloadOnly with payload
        data.extend_from_slice(&12u16.to_be_bytes()); // payload size
        data.extend_from_slice(&1000u64.to_be_bytes()); // payload offset
        data.extend_from_slice(&50u32.to_be_bytes()); // payload length

        let cursor = Cursor::new(data);
        let _parser = PartitionsParser::new(cursor).unwrap();
    }

    #[test]
    fn test_rows_parser_creation() {
        let mut data = Vec::new();
        data.extend_from_slice(&BtiHeader::MAGIC.to_be_bytes());
        data.extend_from_slice(&BtiHeader::VERSION.to_be_bytes());
        data.extend_from_slice(&0u16.to_be_bytes()); // flags
        data.extend_from_slice(&64u64.to_be_bytes()); // root_offset
        data.extend_from_slice(&10u64.to_be_bytes()); // entry_count
        data.extend_from_slice(&0u32.to_be_bytes()); // metadata_size

        // Pad to root offset
        while data.len() < 64 {
            data.push(0);
        }

        // Simple root node
        data.push(0x01); // PayloadOnly with payload
        data.extend_from_slice(&12u16.to_be_bytes()); // payload size
        data.extend_from_slice(&1000u64.to_be_bytes()); // payload offset
        data.extend_from_slice(&50u32.to_be_bytes()); // payload length

        let cursor = Cursor::new(data);
        let _parser = RowsParser::new(cursor).unwrap();
    }

    #[test]
    fn test_partition_lookup() {
        let mut data = Vec::new();
        data.extend_from_slice(&BtiHeader::MAGIC.to_be_bytes());
        data.extend_from_slice(&BtiHeader::VERSION.to_be_bytes());
        data.extend_from_slice(&0u16.to_be_bytes()); // flags
        data.extend_from_slice(&64u64.to_be_bytes()); // root_offset
        data.extend_from_slice(&1u64.to_be_bytes()); // entry_count
        data.extend_from_slice(&0u32.to_be_bytes()); // metadata_size

        // Pad to root offset
        while data.len() < 64 {
            data.push(0);
        }

        // Simple root node (PayloadOnly)
        data.push(0x01); // PayloadOnly with payload
        data.extend_from_slice(&12u16.to_be_bytes()); // payload size
        data.extend_from_slice(&1000u64.to_be_bytes()); // payload offset
        data.extend_from_slice(&50u32.to_be_bytes()); // payload length

        let cursor = Cursor::new(data);
        let mut parser = PartitionsParser::new(cursor).unwrap();

        // Test lookup with simple key
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
}
