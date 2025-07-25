//! BTI file parsing for Partitions.db and Rows.db
//!
//! Implements parsing of BTI trie-indexed files

use crate::error::Result;
use super::{BtiError, BtiLookupResult, BtiMetadata, MAX_TRIE_DEPTH};
use super::nodes::{TrieNode, NodeParser, NodeRef};
use super::encoder::{ByteComparableEncoder, ByteComparableDecoder};
use crate::types::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// BTI Partitions.db parser
pub struct PartitionsParser {
    /// File handle
    file: File,
    /// Root trie node offset
    root_offset: u64,
    /// Node parser
    node_parser: NodeParser,
    /// Cache for parsed nodes
    node_cache: HashMap<u64, TrieNode>,
    /// Byte-comparable encoder for lookups
    encoder: ByteComparableEncoder,
}

impl PartitionsParser {
    /// Create new partitions parser
    pub fn new(mut file: File) -> Result<Self> {
        // Read BTI header to get root offset
        let root_offset = Self::parse_bti_header(&mut file)?;
        
        Ok(Self {
            file,
            root_offset,
            node_parser: NodeParser::new(),
            node_cache: HashMap::new(),
            encoder: ByteComparableEncoder::new(),
        })
    }

    /// Parse BTI file header to get root trie offset
    fn parse_bti_header(file: &mut File) -> Result<u64> {
        let mut header = [0u8; 16];
        file.read_exact(&mut header)?;
        
        // BTI header format:
        // - Magic number (4 bytes): 0x6461_0000
        // - Version (2 bytes)
        // - Flags (2 bytes)
        // - Root offset (8 bytes)
        
        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        if magic != 0x6461_0000 {
            return Err(BtiError::CorruptedTrie(
                format!("Invalid BTI magic number: 0x{:08x}", magic)
            ).into());
        }
        
        let version = u16::from_be_bytes([header[4], header[5]]);
        if version != 0x0001 {
            return Err(BtiError::CorruptedTrie(
                format!("Unsupported BTI version: {}", version)
            ).into());
        }
        
        let root_offset = u64::from_be_bytes([
            header[8], header[9], header[10], header[11],
            header[12], header[13], header[14], header[15]
        ]);
        
        Ok(root_offset)
    }

    /// Lookup partition by key
    pub fn lookup_partition(&mut self, partition_key: &[Value]) -> Result<Option<BtiLookupResult>> {
        // Encode partition key to byte-comparable format
        let encoded_key = self.encoder.encode_composite_key(partition_key)?;
        
        // Traverse trie from root
        self.lookup_in_trie(&encoded_key, self.root_offset, 0)
    }

    /// Lookup in trie starting from given node
    fn lookup_in_trie(&mut self, key: &[u8], node_offset: u64, depth: usize) -> Result<Option<BtiLookupResult>> {
        if depth > MAX_TRIE_DEPTH {
            return Err(BtiError::MaxDepthExceeded(depth).into());
        }

        // Load node from cache or parse from file
        let node = self.load_node(node_offset)?;
        
        // Check if we've consumed the entire key
        if depth >= key.len() {
            // If node has payload, we found our result
            if let Some(payload) = node.payload() {
                return Ok(Some(self.parse_lookup_result(payload)?));
            } else {
                return Ok(None);
            }
        }
        
        // Get next character in key
        let ch = key[depth];
        
        // Find transition for this character
        if let Some(target_ref) = node.find_transition(ch) {
            if target_ref.is_null() {
                return Ok(None);
            }
            
            // Recursively search in target node
            self.lookup_in_trie(key, target_ref.absolute_position, depth + 1)
        } else {
            // No transition found
            Ok(None)
        }
    }

    /// Load node from cache or file
    fn load_node(&mut self, offset: u64) -> Result<TrieNode> {
        if let Some(cached_node) = self.node_cache.get(&offset) {
            return Ok(cached_node.clone());
        }

        // Seek to node position
        self.file.seek(SeekFrom::Start(offset))?;
        
        // Read node data (assuming max node size of 4KB for now)
        let mut buffer = vec![0u8; 4096];
        let bytes_read = self.file.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        
        // Parse node
        let (_, node) = self.node_parser.parse_node(&buffer, offset)
            .map_err(|e| BtiError::CorruptedTrie(format!("Failed to parse node at offset {}: {:?}", offset, e)))?;
        
        // Cache node
        self.node_cache.insert(offset, node.clone());
        
        Ok(node)
    }

    /// Parse lookup result from payload bytes
    fn parse_lookup_result(&self, payload: &[u8]) -> Result<BtiLookupResult> {
        if payload.len() < 8 {
            return Err(BtiError::CorruptedTrie("Payload too short".to_string()).into());
        }
        
        // Payload format:
        // - Data offset (8 bytes)
        // - Data size (4 bytes, optional)
        // - Row index offset (8 bytes, optional)
        
        let data_offset = u64::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
            payload[4], payload[5], payload[6], payload[7]
        ]);
        
        let data_size = if payload.len() >= 12 {
            Some(u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]))
        } else {
            None
        };
        
        let row_index_offset = if payload.len() >= 20 {
            let offset = u64::from_be_bytes([
                payload[12], payload[13], payload[14], payload[15],
                payload[16], payload[17], payload[18], payload[19]
            ]);
            if offset != 0 { Some(offset) } else { None }
        } else {
            None
        };
        
        Ok(BtiLookupResult {
            data_offset,
            data_size,
            row_index_offset,
        })
    }

    /// Get iterator over all partitions
    pub fn iter_partitions(&mut self) -> Result<PartitionIterator> {
        PartitionIterator::new(self, self.root_offset)
    }
}

/// BTI Rows.db parser (similar to Partitions.db but for row indexes)
pub struct RowsParser {
    /// File handle
    file: File,
    /// Root trie node offset
    root_offset: u64,
    /// Node parser
    node_parser: NodeParser,
    /// Node cache
    node_cache: HashMap<u64, TrieNode>,
    /// Encoder for row keys
    encoder: ByteComparableEncoder,
}

impl RowsParser {
    /// Create new rows parser
    pub fn new(mut file: File) -> Result<Self> {
        let root_offset = Self::parse_bti_header(&mut file)?;
        
        Ok(Self {
            file,
            root_offset,
            node_parser: NodeParser::new(),
            node_cache: HashMap::new(),
            encoder: ByteComparableEncoder::new(),
        })
    }

    /// Parse BTI header (same format as Partitions.db)
    fn parse_bti_header(file: &mut File) -> Result<u64> {
        PartitionsParser::parse_bti_header(file)
    }

    /// Lookup row by clustering key
    pub fn lookup_row(&mut self, clustering_key: &[Value]) -> Result<Option<BtiLookupResult>> {
        let encoded_key = self.encoder.encode_composite_key(clustering_key)?;
        self.lookup_in_trie(&encoded_key, self.root_offset, 0)
    }

    /// Lookup in trie (same implementation as PartitionsParser)
    fn lookup_in_trie(&mut self, key: &[u8], node_offset: u64, depth: usize) -> Result<Option<BtiLookupResult>> {
        if depth > MAX_TRIE_DEPTH {
            return Err(BtiError::MaxDepthExceeded(depth).into());
        }

        let node = self.load_node(node_offset)?;
        
        if depth >= key.len() {
            if let Some(payload) = node.payload() {
                return Ok(Some(self.parse_lookup_result(payload)?));
            } else {
                return Ok(None);
            }
        }
        
        let ch = key[depth];
        
        if let Some(target_ref) = node.find_transition(ch) {
            if target_ref.is_null() {
                return Ok(None);
            }
            
            self.lookup_in_trie(key, target_ref.absolute_position, depth + 1)
        } else {
            Ok(None)
        }
    }

    /// Load node (same implementation as PartitionsParser)
    fn load_node(&mut self, offset: u64) -> Result<TrieNode> {
        if let Some(cached_node) = self.node_cache.get(&offset) {
            return Ok(cached_node.clone());
        }

        self.file.seek(SeekFrom::Start(offset))?;
        
        let mut buffer = vec![0u8; 4096];
        let bytes_read = self.file.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        
        let (_, node) = self.node_parser.parse_node(&buffer, offset)
            .map_err(|e| BtiError::CorruptedTrie(format!("Failed to parse node at offset {}: {:?}", offset, e)))?;
        
        self.node_cache.insert(offset, node.clone());
        
        Ok(node)
    }

    /// Parse lookup result (same format as partitions)
    fn parse_lookup_result(&self, payload: &[u8]) -> Result<BtiLookupResult> {
        // For now, return a dummy result - TODO: implement proper parsing
        Ok(BtiLookupResult {
            data_offset: 0,
            data_size: Some(payload.len() as u32),
            row_index_offset: None,
        })
    }
}

/// Iterator over all partitions in BTI format
pub struct PartitionIterator<'a> {
    /// Reference to parser
    parser: &'a mut PartitionsParser,
    /// Stack of (node_offset, depth, key_prefix) for DFS traversal
    stack: Vec<(u64, usize, Vec<u8>)>,
    /// Current key being built
    current_key: Vec<u8>,
}

impl<'a> PartitionIterator<'a> {
    /// Create new partition iterator
    fn new(parser: &'a mut PartitionsParser, root_offset: u64) -> Result<Self> {
        Ok(Self {
            parser,
            stack: vec![(root_offset, 0, Vec::new())],
            current_key: Vec::new(),
        })
    }
}

impl<'a> Iterator for PartitionIterator<'a> {
    type Item = Result<(Vec<u8>, BtiLookupResult)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node_offset, depth, key_prefix)) = self.stack.pop() {
            // Load node
            let node = match self.parser.load_node(node_offset) {
                Ok(node) => node,
                Err(e) => return Some(Err(e)),
            };

            // Check if node has payload
            if let Some(payload) = node.payload() {
                match self.parser.parse_lookup_result(payload) {
                    Ok(result) => {
                        let key = key_prefix.clone();
                        
                        // Add child nodes to stack for further traversal
                        for (ch, target_ref) in node.get_transitions() {
                            if !target_ref.is_null() {
                                let mut child_key = key_prefix.clone();
                                child_key.push(ch);
                                self.stack.push((target_ref.absolute_position, depth + 1, child_key));
                            }
                        }
                        
                        return Some(Ok((key, result)));
                    }
                    Err(e) => return Some(Err(e)),
                }
            } else {
                // No payload, add child nodes to stack
                for (ch, target_ref) in node.get_transitions() {
                    if !target_ref.is_null() {
                        let mut child_key = key_prefix.clone();
                        child_key.push(ch);
                        self.stack.push((target_ref.absolute_position, depth + 1, child_key));
                    }
                }
            }
        }
        
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_bti_header_parsing() {
        let mut header = Vec::new();
        header.extend_from_slice(&0x6461_0000u32.to_be_bytes()); // Magic
        header.extend_from_slice(&0x0001u16.to_be_bytes());       // Version
        header.extend_from_slice(&0x0000u16.to_be_bytes());       // Flags
        header.extend_from_slice(&0x1000u64.to_be_bytes());       // Root offset
        
        let mut cursor = Cursor::new(header);
        let root_offset = PartitionsParser::parse_bti_header(&mut cursor).unwrap();
        assert_eq!(root_offset, 0x1000);
    }

    #[test]
    fn test_invalid_bti_magic() {
        let mut header = Vec::new();
        header.extend_from_slice(&0xDEADBEEFu32.to_be_bytes()); // Invalid magic
        header.extend_from_slice(&0x0001u16.to_be_bytes());
        header.extend_from_slice(&0x0000u16.to_be_bytes());
        header.extend_from_slice(&0x1000u64.to_be_bytes());
        
        let mut cursor = Cursor::new(header);
        let result = PartitionsParser::parse_bti_header(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_lookup_result_parsing() {
        let parser = PartitionsParser {
            file: File::open("/dev/null").unwrap(),
            root_offset: 0,
            node_parser: NodeParser::new(),
            node_cache: HashMap::new(),
            encoder: ByteComparableEncoder::new(),
        };

        // Minimal payload: just data offset
        let payload = 0x123456789ABCDEFu64.to_be_bytes();
        let result = parser.parse_lookup_result(&payload).unwrap();
        assert_eq!(result.data_offset, 0x123456789ABCDEF);
        assert_eq!(result.data_size, None);
        assert_eq!(result.row_index_offset, None);
        
        // Full payload with data size and row index
        let mut full_payload = Vec::new();
        full_payload.extend_from_slice(&0x123456789ABCDEFu64.to_be_bytes()); // Data offset
        full_payload.extend_from_slice(&0x12345678u32.to_be_bytes());        // Data size
        full_payload.extend_from_slice(&0xFEDCBA9876543210u64.to_be_bytes()); // Row index offset
        
        let result = parser.parse_lookup_result(&full_payload).unwrap();
        assert_eq!(result.data_offset, 0x123456789ABCDEF);
        assert_eq!(result.data_size, Some(0x12345678));
        assert_eq!(result.row_index_offset, Some(0xFEDCBA9876543210));
    }
}