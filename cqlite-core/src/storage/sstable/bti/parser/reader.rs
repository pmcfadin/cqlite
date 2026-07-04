//! Stateful BTI index readers: the (CQLite-only) [`BtiHeader`], the
//! [`PartitionsParser`] / [`RowsParser`] node-cached navigators, their
//! whole-trie iterators, and the [`BtiIndexStats`] summary.

use crate::{
    error::Error,
    storage::sstable::bti::{
        encoder::ByteComparableEncoder,
        node::{BtiNode, BtiResult, PayloadRef, TrieNavigator},
    },
    types::Value,
};
use rustc_hash::FxHashMap;
use std::io::{Read, Seek, SeekFrom};

use super::encoding::{encode_clustering_bound_oss50, encode_clustering_bound_oss50_with_order};
use super::node_decode::parse_bti_node;
use super::partitions::BtiPartitionLocation;
use super::rows::{
    iterate_rows_in_bti_trie, resolve_rows_db_entry, select_row_index_blocks_for_range,
    BtiRowIndexEntry, BtiRowIndexHeader,
};
use super::traversal::{dfs_collect_partition_entries, load_bti_trie_via_footer};

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
    node_cache: FxHashMap<u64, BtiNode>,
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
            node_cache: FxHashMap::default(),
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
    node_cache: FxHashMap<u64, BtiNode>,
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
            node_cache: FxHashMap::default(),
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
    /// clustering byte-comparable form).  Because the trie stores separators
    /// (block boundaries), block selection uses
    /// [`select_row_index_blocks_for_range`].
    ///
    /// Reversed bounds yield an empty result.
    ///
    /// Returns the resolved [`BtiRowIndexHeader`] together with the selected
    /// blocks; each block's `data_offset` is relative to `header.data_position`.
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
    /// delegates to the separator-aware [`range_query_encoded`](Self::range_query_encoded).
    ///
    /// Supported clustering types: `int`, `bigint`/`counter`, `smallint`,
    /// `tinyint`, `boolean`, `timestamp`, `uuid`/`timeuuid`, `text`/`ascii`,
    /// `blob`/`inet`.  Any other clustering type returns an explicit
    /// `Error::Parse` (no silent wrong results — issue #28).
    ///
    /// `rows_offset` is the partition's `RowsOffset` (resolved to the real trie
    /// root via [`resolve_rows_db_entry`], Finding A).  Reversed bounds yield an
    /// empty result.
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
    /// clustering order). Both bounds are encoded WITH the per-column order.
    ///
    /// ## Reversed-bounds contract (matches [`range_query`](Self::range_query))
    ///
    /// Like [`range_query`](Self::range_query), a reversed range yields an empty
    /// result rather than silently re-ordering the bounds. The bounds are NOT
    /// swapped.
    pub fn range_query_with_order(
        &mut self,
        rows_offset: usize,
        start_key: &[Value],
        end_key: &[Value],
        is_reversed: &[bool],
    ) -> BtiResult<Vec<BtiRowIndexEntry>> {
        let encoded_start = encode_clustering_bound_oss50_with_order(start_key, is_reversed)?;
        let encoded_end = encode_clustering_bound_oss50_with_order(end_key, is_reversed)?;
        // Reversed bounds in the trie's ascending byte space yield empty, exactly
        // like `range_query`. Do NOT swap — pass the encoded bounds through as-is.
        if encoded_start > encoded_end {
            return Ok(Vec::new());
        }
        let (_, blocks) = self.range_query_encoded(rows_offset, &encoded_start, &encoded_end)?;
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
    /// (Finding A) before traversing.
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
/// materialized entries are then yielded one at a time.
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
/// (Finding A) before traversal.
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
    use crate::storage::sstable::bti::node::{BtiNodeData, BtiNodeType};
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

    /// PayloadOnly (ordinal 0) with a non-zero payload flag (nibble = 1).
    fn payload_only_node(data_offset: u64, length: u32) -> Vec<u8> {
        let mut v = vec![0x01u8]; // ordinal=0, payload_flags=1
        v.extend_from_slice(&data_offset.to_be_bytes());
        v.extend_from_slice(&length.to_be_bytes());
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

    /// SingleNoPayload4 (ordinal 1): delta in low 4 bits of first byte, no payload.
    fn single_nopayload4_node(delta4: u8, transition: u8) -> Vec<u8> {
        vec![0x10 | (delta4 & 0x0F), transition]
    }

    // -----------------------------------------------------------------------
    // RowsParser: non-PayloadOnly node is correctly parsed (was broken before)
    // -----------------------------------------------------------------------

    /// Integration test: embed a Sparse8 node as the root of a Rows.db file
    /// and verify RowsParser reads it as Sparse (not PayloadOnly).
    #[test]
    fn rows_parser_sparse_root_node_not_mislabeled() {
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
    // BtiHeader + parser construction + lookup
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
    // range_query_with_order: reversed-bounds contract parity with range_query
    // -----------------------------------------------------------------------

    /// Build a 3-leaf Rows.db-style trie via a Sparse8 root (single-byte keys).
    fn make_rows_trie_three(
        (k1, p1): (u8, u8),
        (k2, p2): (u8, u8),
        (k3, p3): (u8, u8),
    ) -> (Vec<u8>, usize) {
        let mut trie = Vec::new();
        let o1 = trie.len() as u64; // 0
        trie.extend_from_slice(&[0x01, p1]); // row leaf no marker
        let o2 = trie.len() as u64; // 2
        trie.extend_from_slice(&[0x01, p2]);
        let o3 = trie.len() as u64; // 4
        trie.extend_from_slice(&[0x01, p3]);
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

    /// Wrap a single-byte-keyed 3-leaf Rows.db trie behind a synthetic
    /// per-partition `TrieIndexEntry`, returning `(buffer, rows_offset)`.
    fn make_rows_db_with_three(
        (k1, p1): (u8, u8),
        (k2, p2): (u8, u8),
        (k3, p3): (u8, u8),
    ) -> (Vec<u8>, usize) {
        let (trie, root) = make_rows_trie_three((k1, p1), (k2, p2), (k3, p3));
        let mut buf = trie; // bytes [0, root..] are the trie nodes
        let rows_offset = buf.len(); // TrieIndexEntry starts right after the trie

        // key: length 4, value 0x00000007 (content is irrelevant to range tests).
        buf.extend_from_slice(&4u16.to_be_bytes());
        buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]);
        let base = rows_offset + 4; // RowsOffset + key_length

        // dataPos = 0 (unsigned vint); block offsets are relative to it.
        buf.push(0);
        // rootΔ such that trie_root = `root`: Δ = root - base (zigzag + uvint).
        let root_delta: i64 = root as i64 - base as i64;
        let zig = ((root_delta << 1) ^ (root_delta >> 63)) as u64;
        write_uvint(&mut buf, zig);
        // blockCount = 3 (unsigned vint).
        buf.push(3);
        // partition DeletionTime: MODERN DA live sentinel (no deletion).
        buf.push(0x80);

        (buf, rows_offset)
    }

    /// Minimal unsigned-vint writer (count-leading-ones form) for test fixtures.
    fn write_uvint(out: &mut Vec<u8>, v: u64) {
        if v < 0x80 {
            // 1-byte form: high bit 0, 7 data bits.
            out.push(v as u8);
            return;
        }
        // 2-byte form: leading byte `10xxxxxx` + 1 trailing byte = 14 data bits.
        assert!(v < 0x4000, "test vint fixture expects <= 14-bit values");
        out.push(0x80 | (v >> 8) as u8);
        out.push((v & 0xFF) as u8);
    }

    /// `range_query_with_order` with all columns ASC must behave byte-for-byte
    /// like `range_query`.
    #[test]
    fn range_query_with_order_all_asc_matches_range_query() {
        // tinyint clustering values 3,5,9 → OSS50 single-byte separators
        // 0x83, 0x85, 0x89 (v ^ 0x80). Block offsets 5,17,99.
        let v = |t: i8| (t as u8) ^ 0x80;
        let (buf, rows_offset) = make_rows_db_with_three((v(3), 5), (v(5), 17), (v(9), 99));

        let start = [Value::TinyInt(3)];
        let end = [Value::TinyInt(9)];

        // Forward all-ASC range: both APIs return identical blocks.
        let mut p1 = RowsParser::new(Cursor::new(buf.clone())).unwrap();
        let plain = p1.range_query(rows_offset, &start, &end).unwrap();
        let mut p2 = RowsParser::new(Cursor::new(buf.clone())).unwrap();
        let ordered = p2
            .range_query_with_order(rows_offset, &start, &end, &[false])
            .unwrap();
        assert_eq!(
            ordered, plain,
            "all-ASC range_query_with_order must equal range_query (forward)"
        );
        let offs: Vec<u64> = ordered.iter().map(|b| b.data_offset).collect();
        assert_eq!(
            offs,
            vec![5, 17, 99],
            "forward range returns all three blocks"
        );

        // Reversed bounds (start=9, end=3): range_query returns empty; the
        // order-aware variant MUST do the same (NO swap).
        let mut p3 = RowsParser::new(Cursor::new(buf.clone())).unwrap();
        let plain_rev = p3.range_query(rows_offset, &end, &start).unwrap();
        let mut p4 = RowsParser::new(Cursor::new(buf)).unwrap();
        let ordered_rev = p4
            .range_query_with_order(rows_offset, &end, &start, &[false])
            .unwrap();
        assert!(
            plain_rev.is_empty(),
            "sanity: range_query is empty for reversed bounds"
        );
        assert_eq!(
            ordered_rev, plain_rev,
            "all-ASC reversed range must yield empty, matching range_query (no swap)"
        );
    }

    /// DESC clustering: the trie stores reversed (complemented) byte-comparable
    /// separators.
    #[test]
    fn range_query_with_order_desc_reversed_yields_empty() {
        // DESC tinyint: separator byte = 0xFF ^ (v ^ 0x80).
        let dv = |t: i8| 0xFFu8 ^ ((t as u8) ^ 0x80);
        // dv(9) < dv(5) < dv(3) (descending value => ascending bytes).
        assert!(dv(9) < dv(5) && dv(5) < dv(3));
        let (buf, rows_offset) = make_rows_db_with_three((dv(9), 5), (dv(5), 17), (dv(3), 99));

        // Forward DESC range in VALUE space: from 9 down to 3.
        let mut pf = RowsParser::new(Cursor::new(buf.clone())).unwrap();
        let fwd = pf
            .range_query_with_order(
                rows_offset,
                &[Value::TinyInt(9)],
                &[Value::TinyInt(3)],
                &[true],
            )
            .unwrap();
        let offs: Vec<u64> = fwd.iter().map(|b| b.data_offset).collect();
        assert_eq!(
            offs,
            vec![5, 17, 99],
            "forward DESC range (9..3 in DESC value order) returns all blocks"
        );

        // Reversed DESC range: from 3 up to 9 → empty, NOT swapped.
        let mut pr = RowsParser::new(Cursor::new(buf)).unwrap();
        let rev = pr
            .range_query_with_order(
                rows_offset,
                &[Value::TinyInt(3)],
                &[Value::TinyInt(9)],
                &[true],
            )
            .unwrap();
        assert!(
            rev.is_empty(),
            "reversed DESC range must yield empty (encoded_start > encoded_end), no swap"
        );
    }
}
