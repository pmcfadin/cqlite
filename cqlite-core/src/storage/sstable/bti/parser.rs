//! BTI file parsing for Partitions.db and Rows.db
//!
//! Implements parsing of BTI trie-indexed files

use super::encoder::{ByteComparableDecoder, ByteComparableEncoder};
use super::nodes::{NodeParser, TrieNode};
use super::{BTI_PAGE_SIZE, BtiError, BtiLookupResult, MAX_TRIE_DEPTH};
use crate::error::Result;
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
    /// Cache for parsed nodes with LRU eviction
    node_cache: LruCache<u64, TrieNode>,
    /// Byte-comparable encoder for lookups
    encoder: ByteComparableEncoder,
    /// Maximum cache size
    max_cache_size: usize,
    /// File size for bounds checking
    file_size: u64,
    /// Statistics for performance monitoring
    stats: ParserStats,
}

impl PartitionsParser {
    /// Create new partitions parser
    pub fn new(mut file: File) -> Result<Self> {
        // Get file size for bounds checking
        let file_size = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        // Read BTI header to get root offset
        let root_offset = Self::parse_bti_header(&mut file)?;

        // Validate root offset
        if root_offset >= file_size {
            return Err(BtiError::CorruptedTrie(format!(
                "Root offset {} exceeds file size {}",
                root_offset, file_size
            ))
            .into());
        }

        Ok(Self {
            file,
            root_offset,
            node_parser: NodeParser::new(),
            node_cache: LruCache::new(1024),
            encoder: ByteComparableEncoder::new(),
            max_cache_size: 1024,
            file_size,
            stats: ParserStats::default(),
        })
    }

    /// Create new partitions parser with custom cache size
    pub fn with_cache_size(mut file: File, cache_size: usize) -> Result<Self> {
        let file_size = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        let root_offset = Self::parse_bti_header(&mut file)?;

        if root_offset >= file_size {
            return Err(BtiError::CorruptedTrie(format!(
                "Root offset {} exceeds file size {}",
                root_offset, file_size
            ))
            .into());
        }

        Ok(Self {
            file,
            root_offset,
            node_parser: NodeParser::new(),
            node_cache: LruCache::new(cache_size),
            encoder: ByteComparableEncoder::new(),
            max_cache_size: cache_size,
            file_size,
            stats: ParserStats::default(),
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
            return Err(BtiError::CorruptedTrie(format!(
                "Invalid BTI magic number: 0x{:08x}",
                magic
            ))
            .into());
        }

        let version = u16::from_be_bytes([header[4], header[5]]);
        if version != 0x0001 {
            return Err(
                BtiError::CorruptedTrie(format!("Unsupported BTI version: {}", version)).into(),
            );
        }

        let root_offset = u64::from_be_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14],
            header[15],
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
    fn lookup_in_trie(
        &mut self,
        key: &[u8],
        node_offset: u64,
        depth: usize,
    ) -> Result<Option<BtiLookupResult>> {
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
        let (_, node) = self.node_parser.parse_node(&buffer, offset).map_err(|e| {
            BtiError::CorruptedTrie(format!(
                "Failed to parse node at offset {}: {:?}",
                offset, e
            ))
        })?;

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
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ]);

        let data_size = if payload.len() >= 12 {
            Some(u32::from_be_bytes([
                payload[8],
                payload[9],
                payload[10],
                payload[11],
            ]))
        } else {
            None
        };

        let row_index_offset = if payload.len() >= 20 {
            let offset = u64::from_be_bytes([
                payload[12],
                payload[13],
                payload[14],
                payload[15],
                payload[16],
                payload[17],
                payload[18],
                payload[19],
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

    /// Range lookup: find all keys between start and end (inclusive)
    pub fn range_lookup(
        &mut self,
        start_key: Option<&[Value]>,
        end_key: Option<&[Value]>,
    ) -> Result<Vec<(Vec<u8>, BtiLookupResult)>> {
        let mut results = Vec::new();

        let encoded_start = if let Some(key) = start_key {
            Some(self.encoder.encode_composite_key(key)?)
        } else {
            None
        };

        let encoded_end = if let Some(key) = end_key {
            Some(self.encoder.encode_composite_key(key)?)
        } else {
            None
        };

        self.range_lookup_recursive(
            self.root_offset,
            0,
            Vec::new(),
            &encoded_start,
            &encoded_end,
            &mut results,
        )?;

        Ok(results)
    }

    /// Recursive range lookup implementation
    fn range_lookup_recursive(
        &mut self,
        node_offset: u64,
        depth: usize,
        key_prefix: Vec<u8>,
        start_key: &Option<Vec<u8>>,
        end_key: &Option<Vec<u8>>,
        results: &mut Vec<(Vec<u8>, BtiLookupResult)>,
    ) -> Result<()> {
        if depth > MAX_TRIE_DEPTH {
            return Err(BtiError::MaxDepthExceeded(depth).into());
        }

        let node = self.load_node(node_offset)?;

        // Check if current key is within range and has payload
        if let Some(payload) = node.payload() {
            let current_key = key_prefix.clone();

            let within_range = match (start_key, end_key) {
                (Some(start), Some(end)) => current_key >= *start && current_key <= *end,
                (Some(start), None) => current_key >= *start,
                (None, Some(end)) => current_key <= *end,
                (None, None) => true,
            };

            if within_range {
                let result = self.parse_lookup_result(payload)?;
                results.push((current_key, result));
            }
        }

        // Recursively explore child nodes
        for (ch, target_ref) in node.get_transitions() {
            if target_ref.is_null() {
                continue;
            }

            let mut child_key = key_prefix.clone();
            child_key.push(ch);

            // Prune if child key is definitely outside range
            let should_explore = match (start_key, end_key) {
                (Some(start), Some(end)) => {
                    // Child key prefix could lead to keys in range
                    child_key <= *end && (child_key.len() <= start.len() || child_key >= *start)
                }
                (Some(start), None) => child_key.len() <= start.len() || child_key >= *start,
                (None, Some(end)) => child_key <= *end,
                (None, None) => true,
            };

            if should_explore {
                self.range_lookup_recursive(
                    target_ref.absolute_position,
                    depth + 1,
                    child_key,
                    start_key,
                    end_key,
                    results,
                )?;
            }
        }

        Ok(())
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, f64) {
        let hit_rate = if self.stats.cache_hits + self.stats.cache_misses > 0 {
            self.stats.cache_hits as f64 / (self.stats.cache_hits + self.stats.cache_misses) as f64
        } else {
            0.0
        };
        (self.node_cache.len(), hit_rate)
    }

    /// Get parser statistics
    pub fn stats(&self) -> &ParserStats {
        &self.stats
    }

    /// Clear cache to free memory
    pub fn clear_cache(&mut self) {
        self.node_cache.clear();
    }

    /// Validate trie structure starting from root
    pub fn validate_trie(&mut self) -> Result<TrieValidationReport> {
        let mut report = TrieValidationReport::default();
        let mut visited = std::collections::HashSet::new();

        self.validate_node_recursive(self.root_offset, 0, &mut visited, &mut report)?;

        Ok(report)
    }

    /// Recursive trie validation
    fn validate_node_recursive(
        &mut self,
        node_offset: u64,
        depth: usize,
        visited: &mut std::collections::HashSet<u64>,
        report: &mut TrieValidationReport,
    ) -> Result<()> {
        if depth > MAX_TRIE_DEPTH {
            report
                .errors
                .push(format!("Max depth exceeded at offset {}", node_offset));
            return Ok(());
        }

        if visited.contains(&node_offset) {
            report
                .errors
                .push(format!("Cycle detected at offset {}", node_offset));
            return Ok(());
        }

        visited.insert(node_offset);
        report.nodes_visited += 1;
        report.max_depth = report.max_depth.max(depth);

        let node = match self.load_node(node_offset) {
            Ok(node) => node,
            Err(e) => {
                report
                    .errors
                    .push(format!("Failed to load node at {}: {}", node_offset, e));
                return Ok(());
            }
        };

        if node.payload().is_some() {
            report.payload_nodes += 1;
        }

        // Validate transitions
        let transitions = node.get_transitions();
        for (ch, target_ref) in &transitions {
            if !target_ref.is_null() {
                if target_ref.absolute_position >= self.file_size {
                    report.errors.push(format!(
                        "Invalid target reference {} from node {} (char: {})",
                        target_ref.absolute_position, node_offset, ch
                    ));
                } else {
                    self.validate_node_recursive(
                        target_ref.absolute_position,
                        depth + 1,
                        visited,
                        report,
                    )?;
                }
            }
        }

        visited.remove(&node_offset);
        Ok(())
    }

    /// Get iterator over all partitions
    pub fn iter_partitions(&mut self) -> Result<PartitionIterator> {
        PartitionIterator::new(self, self.root_offset)
    }

    /// Get iterator with prefix filter
    pub fn iter_partitions_with_prefix(&mut self, prefix: &[Value]) -> Result<PrefixIterator> {
        let encoded_prefix = self.encoder.encode_composite_key(prefix)?;
        PrefixIterator::new(self, self.root_offset, encoded_prefix)
    }
}

/// BTI Rows.db parser (enhanced implementation)
pub struct RowsParser {
    /// File handle
    file: File,
    /// Root trie node offset
    root_offset: u64,
    /// Node parser
    node_parser: NodeParser,
    /// Node cache with LRU eviction
    node_cache: LruCache<u64, TrieNode>,
    /// Encoder for row keys
    encoder: ByteComparableEncoder,
    /// File size for bounds checking
    file_size: u64,
    /// Parser statistics
    stats: ParserStats,
    /// Row index metadata cache
    row_index_cache: HashMap<u64, RowIndexMetadata>,
}

impl RowsParser {
    /// Create new rows parser
    pub fn new(mut file: File) -> Result<Self> {
        let file_size = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        let root_offset = Self::parse_bti_header(&mut file)?;

        if root_offset >= file_size {
            return Err(BtiError::CorruptedTrie(format!(
                "Root offset {} exceeds file size {}",
                root_offset, file_size
            ))
            .into());
        }

        Ok(Self {
            file,
            root_offset,
            node_parser: NodeParser::new(),
            node_cache: LruCache::new(1024),
            encoder: ByteComparableEncoder::new(),
            file_size,
            stats: ParserStats::default(),
            row_index_cache: HashMap::new(),
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
    fn lookup_in_trie(
        &mut self,
        key: &[u8],
        node_offset: u64,
        depth: usize,
    ) -> Result<Option<BtiLookupResult>> {
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

    /// Load node with enhanced caching and error handling
    fn load_node(&mut self, offset: u64) -> Result<TrieNode> {
        if let Some(cached_node) = self.node_cache.get(&offset) {
            self.stats.cache_hits += 1;
            return Ok(cached_node);
        }

        self.stats.cache_misses += 1;

        if offset >= self.file_size {
            return Err(BtiError::CorruptedTrie(format!(
                "Node offset {} exceeds file size {}",
                offset, self.file_size
            ))
            .into());
        }

        self.file.seek(SeekFrom::Start(offset))?;

        let remaining_bytes = (self.file_size - offset) as usize;
        let read_size = remaining_bytes.min(BTI_PAGE_SIZE);

        if read_size == 0 {
            return Err(
                BtiError::CorruptedTrie(format!("No data available at offset {}", offset)).into(),
            );
        }

        let mut buffer = vec![0u8; read_size];
        let bytes_read = self.file.read(&mut buffer)?;

        if bytes_read == 0 {
            return Err(
                BtiError::CorruptedTrie(format!("No bytes read at offset {}", offset)).into(),
            );
        }

        buffer.truncate(bytes_read);
        self.stats.bytes_read += bytes_read as u64;

        let (remaining, node) = self.node_parser.parse_node(&buffer, offset).map_err(|e| {
            BtiError::CorruptedTrie(format!(
                "Failed to parse node at offset {} (read {} bytes): {:?}\nBuffer: {}",
                offset,
                bytes_read,
                e,
                ByteComparableDecoder::decode_key_debug(&buffer[..bytes_read.min(32)])
            ))
        })?;

        if remaining.len() > BTI_PAGE_SIZE / 2 {
            return Err(BtiError::CorruptedTrie(format!(
                "Node parsing left {} bytes unparsed at offset {}",
                remaining.len(),
                offset
            ))
            .into());
        }

        self.stats.nodes_parsed += 1;
        self.node_cache.insert(offset, node.clone());

        Ok(node)
    }

    /// Parse lookup result with enhanced row-specific handling
    fn parse_lookup_result(&self, payload: &[u8]) -> Result<BtiLookupResult> {
        if payload.len() < 8 {
            return Err(BtiError::CorruptedTrie("Row payload too short".to_string()).into());
        }

        // Row payload format:
        // - Data offset (8 bytes)
        // - Data size (4 bytes)
        // - Row flags (2 bytes) - indicates if row has deletions, tombstones, etc.
        // - Row count (4 bytes, optional) - for multi-row payloads
        // - Additional metadata (variable length)

        let data_offset = u64::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ]);

        let data_size = if payload.len() >= 12 {
            Some(u32::from_be_bytes([
                payload[8],
                payload[9],
                payload[10],
                payload[11],
            ]))
        } else {
            None
        };

        // Check for row flags and extended metadata
        let (_row_flags, _row_count) = if payload.len() >= 18 {
            let flags = u16::from_be_bytes([payload[12], payload[13]]);
            let count = u32::from_be_bytes([payload[14], payload[15], payload[16], payload[17]]);
            (Some(flags), Some(count))
        } else {
            (None, None)
        };

        // For large partitions, there might be a row index offset
        let row_index_offset = if payload.len() >= 26 {
            let offset = u64::from_be_bytes([
                payload[18],
                payload[19],
                payload[20],
                payload[21],
                payload[22],
                payload[23],
                payload[24],
                payload[25],
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

    /// Range lookup for clustering keys
    pub fn range_lookup_rows(
        &mut self,
        start_key: Option<&[Value]>,
        end_key: Option<&[Value]>,
    ) -> Result<Vec<(Vec<u8>, BtiLookupResult)>> {
        let mut results = Vec::new();

        let encoded_start = if let Some(key) = start_key {
            Some(self.encoder.encode_composite_key(key)?)
        } else {
            None
        };

        let encoded_end = if let Some(key) = end_key {
            Some(self.encoder.encode_composite_key(key)?)
        } else {
            None
        };

        self.range_lookup_recursive(
            self.root_offset,
            0,
            Vec::new(),
            &encoded_start,
            &encoded_end,
            &mut results,
        )?;

        Ok(results)
    }

    /// Recursive range lookup for rows
    fn range_lookup_recursive(
        &mut self,
        node_offset: u64,
        depth: usize,
        key_prefix: Vec<u8>,
        start_key: &Option<Vec<u8>>,
        end_key: &Option<Vec<u8>>,
        results: &mut Vec<(Vec<u8>, BtiLookupResult)>,
    ) -> Result<()> {
        if depth > MAX_TRIE_DEPTH {
            return Err(BtiError::MaxDepthExceeded(depth).into());
        }

        let node = self.load_node(node_offset)?;

        if let Some(payload) = node.payload() {
            let current_key = key_prefix.clone();

            let within_range = match (start_key, end_key) {
                (Some(start), Some(end)) => current_key >= *start && current_key <= *end,
                (Some(start), None) => current_key >= *start,
                (None, Some(end)) => current_key <= *end,
                (None, None) => true,
            };

            if within_range {
                let result = self.parse_lookup_result(payload)?;
                results.push((current_key, result));
            }
        }

        for (ch, target_ref) in node.get_transitions() {
            if target_ref.is_null() {
                continue;
            }

            let mut child_key = key_prefix.clone();
            child_key.push(ch);

            let should_explore = match (start_key, end_key) {
                (Some(start), Some(end)) => {
                    child_key <= *end && (child_key.len() <= start.len() || child_key >= *start)
                }
                (Some(start), None) => child_key.len() <= start.len() || child_key >= *start,
                (None, Some(end)) => child_key <= *end,
                (None, None) => true,
            };

            if should_explore {
                self.range_lookup_recursive(
                    target_ref.absolute_position,
                    depth + 1,
                    child_key,
                    start_key,
                    end_key,
                    results,
                )?;
            }
        }

        Ok(())
    }

    /// Parse row index metadata for large partitions
    pub fn parse_row_index(&mut self, index_offset: u64) -> Result<RowIndexMetadata> {
        if let Some(cached) = self.row_index_cache.get(&index_offset) {
            return Ok(cached.clone());
        }

        if index_offset >= self.file_size {
            return Err(BtiError::CorruptedTrie(format!(
                "Row index offset {} exceeds file size {}",
                index_offset, self.file_size
            ))
            .into());
        }

        self.file.seek(SeekFrom::Start(index_offset))?;

        // Read row index header
        let mut header_buf = [0u8; 16];
        self.file.read_exact(&mut header_buf)?;

        let row_count =
            u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let first_key_len =
            u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);
        let last_key_len =
            u32::from_be_bytes([header_buf[8], header_buf[9], header_buf[10], header_buf[11]]);
        let block_count = u32::from_be_bytes([
            header_buf[12],
            header_buf[13],
            header_buf[14],
            header_buf[15],
        ]);

        // Read first and last clustering keys
        let mut first_key = vec![0u8; first_key_len as usize];
        let mut last_key = vec![0u8; last_key_len as usize];
        self.file.read_exact(&mut first_key)?;
        self.file.read_exact(&mut last_key)?;

        // Read index blocks
        let mut index_blocks = Vec::with_capacity(block_count as usize);
        for _ in 0..block_count {
            let mut block_header = [0u8; 16];
            self.file.read_exact(&mut block_header)?;

            let key_len = u32::from_be_bytes([
                block_header[0],
                block_header[1],
                block_header[2],
                block_header[3],
            ]);
            let data_offset = u64::from_be_bytes([
                block_header[4],
                block_header[5],
                block_header[6],
                block_header[7],
                block_header[8],
                block_header[9],
                block_header[10],
                block_header[11],
            ]);
            let data_size = u32::from_be_bytes([
                block_header[12],
                block_header[13],
                block_header[14],
                block_header[15],
            ]);

            let mut clustering_key = vec![0u8; key_len as usize];
            self.file.read_exact(&mut clustering_key)?;

            index_blocks.push(RowIndexBlock {
                clustering_key,
                data_offset,
                data_size,
            });
        }

        let metadata = RowIndexMetadata {
            row_count,
            first_clustering_key: first_key,
            last_clustering_key: last_key,
            index_blocks,
        };

        self.row_index_cache.insert(index_offset, metadata.clone());
        Ok(metadata)
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, f64) {
        let hit_rate = if self.stats.cache_hits + self.stats.cache_misses > 0 {
            self.stats.cache_hits as f64 / (self.stats.cache_hits + self.stats.cache_misses) as f64
        } else {
            0.0
        };
        (self.node_cache.len(), hit_rate)
    }

    /// Get parser statistics
    pub fn stats(&self) -> &ParserStats {
        &self.stats
    }

    /// Clear caches to free memory
    pub fn clear_cache(&mut self) {
        self.node_cache.clear();
        self.row_index_cache.clear();
    }

    /// Get iterator over all rows
    pub fn iter_rows(&mut self) -> Result<RowIterator> {
        RowIterator::new(self, self.root_offset)
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
                                self.stack.push((
                                    target_ref.absolute_position,
                                    depth + 1,
                                    child_key,
                                ));
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
                        self.stack
                            .push((target_ref.absolute_position, depth + 1, child_key));
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
        header.extend_from_slice(&0x0001u16.to_be_bytes()); // Version
        header.extend_from_slice(&0x0000u16.to_be_bytes()); // Flags
        header.extend_from_slice(&0x1000u64.to_be_bytes()); // Root offset

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
        full_payload.extend_from_slice(&0x12345678u32.to_be_bytes()); // Data size
        full_payload.extend_from_slice(&0xFEDCBA9876543210u64.to_be_bytes()); // Row index offset

        let result = parser.parse_lookup_result(&full_payload).unwrap();
        assert_eq!(result.data_offset, 0x123456789ABCDEF);
        assert_eq!(result.data_size, Some(0x12345678));
        assert_eq!(result.row_index_offset, Some(0xFEDCBA9876543210));
    }
}

/// Trie validation report
#[derive(Debug, Default)]
pub struct TrieValidationReport {
    pub nodes_visited: usize,
    pub payload_nodes: usize,
    pub max_depth: usize,
    pub errors: Vec<String>,
}

/// LRU Cache for BTI nodes
pub struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<K, (V, usize)>,
    access_counter: usize,
}

impl<K: Clone + std::hash::Hash + Eq, V: Clone> LruCache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            access_counter: 0,
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        if let Some((value, access_time)) = self.map.get_mut(key) {
            self.access_counter += 1;
            *access_time = self.access_counter;
            Some(value.clone())
        } else {
            None
        }
    }

    fn insert(&mut self, key: K, value: V) {
        if self.map.len() >= self.capacity {
            // Find least recently used item
            let lru_key = self
                .map
                .iter()
                .min_by_key(|(_, (_, access_time))| *access_time)
                .map(|(k, _)| k.clone());

            if let Some(lru_key) = lru_key {
                self.map.remove(&lru_key);
            }
        }

        self.access_counter += 1;
        self.map.insert(key, (value, self.access_counter));
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn clear(&mut self) {
        self.map.clear();
        self.access_counter = 0;
    }
}

/// Parser performance statistics
#[derive(Debug, Clone, Default)]
pub struct ParserStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub nodes_parsed: u64,
    pub bytes_read: u64,
    pub max_depth_reached: usize,
}

/// Row index metadata for large partitions
#[derive(Debug, Clone)]
pub struct RowIndexMetadata {
    pub row_count: u32,
    pub first_clustering_key: Vec<u8>,
    pub last_clustering_key: Vec<u8>,
    pub index_blocks: Vec<RowIndexBlock>,
}

/// Individual row index block
#[derive(Debug, Clone)]
pub struct RowIndexBlock {
    pub clustering_key: Vec<u8>,
    pub data_offset: u64,
    pub data_size: u32,
}

/// Iterator over partitions with prefix filtering
pub struct PrefixIterator<'a> {
    parser: &'a mut PartitionsParser,
    stack: Vec<(u64, usize, Vec<u8>)>,
    prefix: Vec<u8>,
}

impl<'a> PrefixIterator<'a> {
    fn new(parser: &'a mut PartitionsParser, root_offset: u64, prefix: Vec<u8>) -> Result<Self> {
        Ok(Self {
            parser,
            stack: vec![(root_offset, 0, Vec::new())],
            prefix,
        })
    }
}

impl<'a> Iterator for PrefixIterator<'a> {
    type Item = Result<(Vec<u8>, BtiLookupResult)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node_offset, depth, key_prefix)) = self.stack.pop() {
            if depth > MAX_TRIE_DEPTH {
                return Some(Err(BtiError::MaxDepthExceeded(depth).into()));
            }

            // Check if we've moved beyond the prefix
            if depth <= self.prefix.len() && !self.prefix.starts_with(&key_prefix) {
                continue;
            }

            let node = match self.parser.load_node(node_offset) {
                Ok(node) => node,
                Err(e) => return Some(Err(e)),
            };

            if let Some(payload) = node.payload() {
                if key_prefix.starts_with(&self.prefix) {
                    match self.parser.parse_lookup_result(payload) {
                        Ok(result) => {
                            let key = key_prefix.clone();

                            // Add child nodes
                            let mut transitions = node.get_transitions();
                            transitions.reverse();

                            for (ch, target_ref) in transitions {
                                if !target_ref.is_null() {
                                    let mut child_key = key_prefix.clone();
                                    child_key.push(ch);

                                    // Only continue if child could have matching prefix
                                    if child_key.len() <= self.prefix.len()
                                        || child_key.starts_with(&self.prefix)
                                        || self.prefix.starts_with(&child_key)
                                    {
                                        self.stack.push((
                                            target_ref.absolute_position,
                                            depth + 1,
                                            child_key,
                                        ));
                                    }
                                }
                            }

                            return Some(Ok((key, result)));
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
            }

            // Add child nodes even if no payload
            let mut transitions = node.get_transitions();
            transitions.reverse();

            for (ch, target_ref) in transitions {
                if !target_ref.is_null() {
                    let mut child_key = key_prefix.clone();
                    child_key.push(ch);

                    if child_key.len() <= self.prefix.len()
                        || child_key.starts_with(&self.prefix)
                        || self.prefix.starts_with(&child_key)
                    {
                        self.stack
                            .push((target_ref.absolute_position, depth + 1, child_key));
                    }
                }
            }
        }

        None
    }
}

/// Iterator over rows in BTI format
pub struct RowIterator<'a> {
    parser: &'a mut RowsParser,
    stack: Vec<(u64, usize, Vec<u8>)>,
}

impl<'a> RowIterator<'a> {
    fn new(parser: &'a mut RowsParser, root_offset: u64) -> Result<Self> {
        Ok(Self {
            parser,
            stack: vec![(root_offset, 0, Vec::new())],
        })
    }
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = Result<(Vec<u8>, BtiLookupResult)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node_offset, depth, key_prefix)) = self.stack.pop() {
            if depth > MAX_TRIE_DEPTH {
                return Some(Err(BtiError::MaxDepthExceeded(depth).into()));
            }

            let node = match self.parser.load_node(node_offset) {
                Ok(node) => node,
                Err(e) => return Some(Err(e)),
            };

            if let Some(payload) = node.payload() {
                match self.parser.parse_lookup_result(payload) {
                    Ok(result) => {
                        let key = key_prefix.clone();

                        let mut transitions = node.get_transitions();
                        transitions.reverse();

                        for (ch, target_ref) in transitions {
                            if !target_ref.is_null() {
                                if target_ref.absolute_position >= self.parser.file_size {
                                    return Some(Err(BtiError::CorruptedTrie(format!(
                                        "Invalid target reference {}",
                                        target_ref.absolute_position
                                    ))
                                    .into()));
                                }

                                let mut child_key = key_prefix.clone();
                                child_key.push(ch);
                                self.stack.push((
                                    target_ref.absolute_position,
                                    depth + 1,
                                    child_key,
                                ));
                            }
                        }

                        return Some(Ok((key, result)));
                    }
                    Err(e) => return Some(Err(e)),
                }
            } else {
                let mut transitions = node.get_transitions();
                transitions.reverse();

                for (ch, target_ref) in transitions {
                    if !target_ref.is_null() {
                        if target_ref.absolute_position >= self.parser.file_size {
                            return Some(Err(BtiError::CorruptedTrie(format!(
                                "Invalid target reference {}",
                                target_ref.absolute_position
                            ))
                            .into()));
                        }

                        let mut child_key = key_prefix.clone();
                        child_key.push(ch);
                        self.stack
                            .push((target_ref.absolute_position, depth + 1, child_key));
                    }
                }
            }
        }

        None
    }
}
