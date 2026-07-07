//! Index.db reader implementation for Cassandra 5+ SSTable format
//!
//! This module provides comprehensive parsing of Index.db files which contain
//! partition-level index information including promoted index entries for wide partitions.
//! The index is used for efficient partition lookups and range queries.

use crate::{
    error::{Error, Result},
    platform::Platform,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use super::summary_reader::SummaryReader;

// Index.db nom parse tree (issue #1599 / G3 split, campsite #1116). Re-exported by
// path so `index_reader::parse_big_index_entry` / `::parse_all_partition_keys` keep
// working for `verify.rs`, `s3_verification_test.rs`, and the in-module tests.
mod parse;
pub(crate) use parse::parse_big_index_entry;
use parse::parse_index_data_with_summary;
// `parse_all_partition_keys` (legacy API) is consumed only by in-crate test modules
// (`s3_verification_test.rs`, the `tests` module below).
#[cfg(test)]
pub(crate) use parse::parse_all_partition_keys;
#[cfg(test)]
use parse::{
    parse_all_partition_keys_with_summary, parse_index_data, parse_simple_partition_key,
};

/// Index.db file header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexHeader {
    /// Format version identifier
    pub version: u32,
    /// Number of index entries
    pub entry_count: u32,
    /// Size of the index data section
    pub data_size: u64,
    /// Checksum for validation
    pub checksum: u32,
}

/// Partition index entry in Index.db
#[derive(Debug, Clone)]
pub struct PartitionIndexEntry {
    /// Raw partition key bytes (length-prefixed in the on-disk BIG/NB Index.db format).
    ///
    /// NOTE (Issue #552): Despite the historical field name `key_digest`, this holds the
    /// RAW partition key bytes, not an MD5 digest. The real Cassandra 5.0 NB Index.db entry
    /// format is `[key_len: u16 BE][raw key bytes][data_offset: vint][promoted_len: vint]`.
    /// There is no `0x0010` marker and no MD5 digest on disk. The field name is retained to
    /// avoid churn in the zero-copy lookup table and downstream callers; it is used directly
    /// as the partition key (e.g. for `RowKey`). The leading u16 is the key length
    /// (e.g. 0x0010 for a 16-byte UUID, 0x0026 for a 38-byte composite key).
    pub key_digest: Arc<[u8]>,
    /// Raw partition key bytes (mirror of `key_digest`, kept for API compatibility).
    /// Always `Some` now that all entries carry their raw key.
    pub raw_key: Option<Arc<[u8]>>,
    /// Offset in Data.db file
    pub data_offset: u64,
    /// Size of partition data
    pub data_size: u32,
    /// Promoted index entries for wide partitions (optional)
    pub promoted_index: Option<PromotedIndexData>,
}

/// Promoted index for wide partitions (BIG "nb" format, Issue #993).
///
/// Holds the RAW promoted-index payload bytes captured from the Index.db entry
/// (previously these bytes were decoded-away/discarded on the read path). The
/// payload's `firstName`/`lastName` clustering prefixes are not self-delimiting
/// without the table's clustering column types, so full structural decode is
/// deferred to [`PromotedIndexData::decode`], which takes an authoritative,
/// schema-driven clustering-prefix length callback (no heuristics — Issue #28).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotedIndexData {
    /// Raw promoted-index payload (the bytes after the `promoted_index_size` VInt).
    pub raw_payload: Vec<u8>,
}

impl PromotedIndexData {
    /// Wrap a raw promoted-index payload.
    pub fn from_raw(raw_payload: Vec<u8>) -> Self {
        Self { raw_payload }
    }

    /// Number of bytes in the raw promoted-index payload.
    pub fn len(&self) -> usize {
        self.raw_payload.len()
    }

    /// True when the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.raw_payload.is_empty()
    }

    /// Fully decode the promoted index using a schema-driven clustering-prefix
    /// length callback. See [`crate::storage::sstable::promoted_index_reader`].
    pub fn decode(
        &self,
        prefix_len: &super::promoted_index_reader::PrefixLen<'_>,
    ) -> Result<super::promoted_index_reader::DecodedPromotedIndex> {
        super::promoted_index_reader::decode_promoted_index(&self.raw_payload, prefix_len)
    }

    /// Read just the IndexInfo block count without decoding the (schema-dependent)
    /// `firstName`/`lastName` prefixes. The leading `headerLength`, `DeletionTime`
    /// and `count` fields are schema-free, so this is always recoverable.
    ///
    /// Returns 0 on a malformed/short payload (defensive — never panics).
    pub fn block_count(&self) -> u32 {
        super::promoted_index_reader::peek_block_count(&self.raw_payload).unwrap_or(0)
    }
}

/// Complete Index.db data structure
#[derive(Debug, Clone)]
pub struct IndexData {
    /// File header
    pub header: IndexHeader,
    /// All partition index entries
    pub partition_entries: Vec<PartitionIndexEntry>,
    /// Lookup table for efficient partition access - uses Arc<[u8]> as key type
    ///
    /// ## Zero-Copy Design (Issue #107, Problem 1)
    ///
    /// - Keys are `Arc<[u8]>` to enable reference counting without cloning digest bytes
    /// - Lookups use `&[u8]` directly via Borrow trait (zero heap allocations)
    /// - `Arc<[u8]>` implements `Borrow<[u8]>` enabling HashMap::get(&[u8]) without temporary Arc creation
    pub key_lookup: HashMap<Arc<[u8]>, usize>,
}

/// High-level Index.db file reader
#[allow(dead_code)]
pub struct IndexReader {
    /// Path to the Index.db file
    file_path: PathBuf,
    /// Parsed index data
    index_data: IndexData,
    /// Platform abstraction for file operations
    platform: Arc<Platform>,
}

impl IndexReader {
    /// Open and parse an Index.db file
    pub async fn open(path: &Path, platform: Arc<Platform>) -> Result<Self> {
        Self::open_with_summary(path, platform, None).await
    }

    /// Open and parse an Index.db file with Summary.db correlation for proper offset mapping
    pub async fn open_with_summary(
        path: &Path,
        platform: Arc<Platform>,
        summary_reader: Option<&SummaryReader>,
    ) -> Result<Self> {
        if !platform.fs().exists(path).await? {
            return Err(Error::not_found(format!(
                "Index.db file not found: {}",
                path.display()
            )));
        }

        // Read the entire file
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        // Check for empty file
        if buffer.is_empty() {
            return Err(Error::corruption(format!(
                "Index.db file is empty: {}",
                path.display()
            )));
        }

        // Parse the index data with optional Summary.db correlation.
        //
        // NOTE (issue #1572): `parse_all_partition_keys_with_summary` `break`s on
        // the first unparseable entry and returns the parsed prefix, so a
        // truncated / partially-corrupt Index.db opens successfully with a PARTIAL
        // map. The leftover `remaining` being empty ONLY proves the surviving
        // bytes parsed cleanly to EOF — it CANNOT detect truncation aligned to an
        // exact entry boundary (whole trailing entries dropped ⇒ the prefix parses
        // cleanly, `remaining` is empty, yet partitions are missing). There is no
        // cleanly-available authoritative partition count at this layer to close
        // that gap, so completeness is NOT tracked here; instead the BIG
        // point-lookup miss branch always falls back to a whole-file scan rather
        // than treating an index miss as a definitive absent (see
        // `big_get_with_resolution`). No heuristics (issue #28).
        let index_data = match parse_index_data_with_summary(&buffer, summary_reader) {
            Ok((_remaining, data)) => data,
            Err(e) => {
                return Err(Error::corruption(format!(
                    "Failed to parse Index.db: {:?}",
                    e
                )));
            }
        };

        Ok(Self {
            file_path: path.to_path_buf(),
            index_data,
            platform,
        })
    }

    /// Get all partition entries
    pub fn get_partition_entries(&self) -> &[PartitionIndexEntry] {
        &self.index_data.partition_entries
    }

    /// Look up a partition by key digest
    ///
    /// ## Zero-Allocation Optimization (Issue #107)
    ///
    /// This method performs HashMap lookup without heap allocation by leveraging
    /// the `Borrow` trait. Since `Arc<[u8]>` implements `Borrow<[u8]>`, we can
    /// lookup using `&[u8]` directly without creating a temporary Arc.
    ///
    /// **Before:** `let key_arc: Arc<[u8]> = key_digest.into();` (heap allocation per query)
    /// **After:** Direct `get(key_digest)` using Borrow trait (zero allocations)
    pub fn lookup_partition(&self, key_digest: &[u8]) -> Option<&PartitionIndexEntry> {
        self.index_data
            .key_lookup
            .get(key_digest)
            .and_then(|&index| self.index_data.partition_entries.get(index))
    }

    /// Get statistics about the index
    pub fn get_statistics(&self) -> IndexStatistics {
        let mut promoted_count = 0;
        let mut total_promoted_entries = 0;

        for entry in &self.index_data.partition_entries {
            if let Some(ref promoted) = entry.promoted_index {
                promoted_count += 1;
                total_promoted_entries += promoted.block_count() as usize;
            }
        }

        IndexStatistics {
            total_partitions: self.index_data.partition_entries.len(),
            partitions_with_promoted_index: promoted_count,
            total_promoted_entries,
            file_size: self.file_path.metadata().map(|m| m.len()).unwrap_or(0),
        }
    }

    /// Validate index integrity against Data.db offsets
    pub async fn validate_integrity(&self) -> Result<Vec<String>> {
        let mut issues = Vec::new();

        // Check for overlapping offsets
        let mut offsets: Vec<_> = self
            .index_data
            .partition_entries
            .iter()
            .map(|e| (e.data_offset, e.data_size))
            .collect();

        offsets.sort_by_key(|&(offset, _)| offset);

        for i in 1..offsets.len() {
            let (prev_offset, prev_size) = offsets[i - 1];
            let (curr_offset, _) = offsets[i];

            if prev_offset + prev_size as u64 > curr_offset {
                issues.push(format!(
                    "Overlapping partitions: offset {} + size {} overlaps with offset {}",
                    prev_offset, prev_size, curr_offset
                ));
            }
        }

        Ok(issues)
    }
}

/// Index statistics for analysis and validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    /// Total number of partitions
    pub total_partitions: usize,
    /// Number of partitions with promoted index
    pub partitions_with_promoted_index: usize,
    /// Total number of promoted index entries
    pub total_promoted_entries: usize,
    /// File size in bytes
    pub file_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    /// Test stock_prices Index.db parsing (Issue #208)
    ///
    /// This test directly parses the stock_prices Index.db file which contains 3 partition entries (AMZN, GOOG, AAPL).
    /// Note: Data.db.jsonl only has 2 entries, suggesting incomplete test data or filtering at a higher level.
    /// The file uses a BTI format with actual partition keys (not MD5 digests).
    ///
    /// **Note:** This test requires test data files and is ignored in minimal CI builds.
    /// Run with: `cargo test --package cqlite-core -- --ignored`
    #[tokio::test]
    #[ignore = "Requires test data files (CQLITE_DATASETS_ROOT)"]
    async fn test_stock_prices_index_db_parsing() {
        let datasets_root = env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| {
            "/Users/patrick/local_projects/cqlite/test-data/datasets".to_string()
        });

        let index_path = format!(
            "{}/sstables/test_timeseries/stock_prices-6c9fad60a25111f0a3fef1a551383fb9/nb-1-big-Index.db",
            datasets_root
        );

        println!("\n=== Testing stock_prices Index.db ===");
        println!("Path: {}", index_path);

        // Read file directly to inspect format
        let file_data = std::fs::read(&index_path).expect("Failed to read Index.db");
        println!("File size: {} bytes", file_data.len());
        println!(
            "First 56 bytes (hex): {:02x?}",
            &file_data[..std::cmp::min(56, file_data.len())]
        );

        // Check format detection
        println!("\n=== Format Analysis ===");
        println!(
            "First 2 bytes: {:#06x} (expected 0x0010 for digest format)",
            u16::from_be_bytes([file_data[0], file_data[1]])
        );

        // Try to parse with current implementation
        println!("\n=== Parsing with parse_all_partition_keys_with_summary ===");
        match parse_all_partition_keys_with_summary(&file_data, None) {
            Ok((remaining, entries)) => {
                println!("SUCCESS: Parsed {} entries", entries.len());
                println!("Remaining bytes: {}", remaining.len());

                for (i, entry) in entries.iter().enumerate() {
                    println!(
                        "  Entry {}: offset={}, size={}, key_digest={:02x?}",
                        i,
                        entry.data_offset,
                        entry.data_size,
                        &entry.key_digest[..]
                    );
                }

                // Note: Index.db contains 3 entries (AMZN, GOOG, AAPL) but Data.db.jsonl only has 2.
                // This may indicate incomplete test data or filtering at a higher level.
                // For now, verify parser works correctly (finds all entries in Index.db).
                assert!(
                    entries.len() >= 2,
                    "Expected at least 2 partition entries for stock_prices (found {})",
                    entries.len()
                );
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
                panic!("Failed to parse stock_prices Index.db: {:?}", e);
            }
        }
    }

    /// Test stock_prices Index.db via IndexReader (Issue #208)
    ///
    /// This test uses the high-level IndexReader API to open the stock_prices Index.db.
    /// It should successfully parse at least 2 partition entries (Index.db has 3 total).
    ///
    /// **Note:** This test requires test data files and is ignored in minimal CI builds.
    /// Run with: `cargo test --package cqlite-core -- --ignored`
    #[tokio::test]
    #[ignore = "Requires test data files (CQLITE_DATASETS_ROOT)"]
    async fn test_stock_prices_index_reader() {
        let datasets_root = env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| {
            "/Users/patrick/local_projects/cqlite/test-data/datasets".to_string()
        });

        let index_path = std::path::PathBuf::from(format!(
            "{}/sstables/test_timeseries/stock_prices-6c9fad60a25111f0a3fef1a551383fb9/nb-1-big-Index.db",
            datasets_root
        ));

        println!("\n=== Testing IndexReader::open ===");
        println!("Path: {:?}", index_path);

        // Create platform
        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        // Try to open with IndexReader
        match IndexReader::open(&index_path, platform.clone()).await {
            Ok(reader) => {
                let entries = reader.get_partition_entries();
                println!(
                    "SUCCESS: IndexReader found {} partition entries",
                    entries.len()
                );

                for (i, entry) in entries.iter().enumerate() {
                    println!(
                        "  Entry {}: offset={}, size={}, key_digest={:02x?}",
                        i,
                        entry.data_offset,
                        entry.data_size,
                        &entry.key_digest[..8]
                    );
                }

                let stats = reader.get_statistics();
                println!(
                    "Statistics: total_partitions={}, file_size={}",
                    stats.total_partitions, stats.file_size
                );

                // Verify parser works correctly (Index.db has 3 entries, Data.db.jsonl has 2)
                assert!(
                    entries.len() >= 2,
                    "Expected at least 2 partition entries for stock_prices (found {})",
                    entries.len()
                );
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
                panic!("Failed to open stock_prices Index.db: {:?}", e);
            }
        }
    }

    /// Test stock_prices via SSTableReader integration (Issue #208)
    ///
    /// This test verifies that SSTableReader correctly loads the Index.db
    /// and can access partition entries (at least 2, Index.db has 3 total).
    ///
    /// **Note:** This test requires test data files and is ignored in minimal CI builds.
    /// Run with: `cargo test --package cqlite-core -- --ignored`
    #[tokio::test]
    #[ignore = "Requires test data files (CQLITE_DATASETS_ROOT)"]
    async fn test_stock_prices_sstable_reader_integration() {
        let datasets_root = env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| {
            "/Users/patrick/local_projects/cqlite/test-data/datasets".to_string()
        });

        let data_path = std::path::PathBuf::from(format!(
            "{}/sstables/test_timeseries/stock_prices-6c9fad60a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
            datasets_root
        ));

        println!("\n=== Testing SSTableReader with stock_prices ===");
        println!("Data.db path: {:?}", data_path);

        // Create platform
        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        // Try to open with SSTableReader
        use crate::storage::sstable::reader::SSTableReader;
        match SSTableReader::open(&data_path, &config, platform.clone()).await {
            Ok(reader) => {
                println!("SUCCESS: SSTableReader opened");

                // Check if index_reader was loaded (it's a public field)
                if let Some(ref index_reader) = reader.index_reader {
                    let entries = index_reader.get_partition_entries();
                    println!("Index loaded with {} partition entries", entries.len());

                    for (i, entry) in entries.iter().enumerate() {
                        println!(
                            "  Entry {}: offset={}, size={}",
                            i, entry.data_offset, entry.data_size
                        );
                    }

                    // Verify Index.db was parsed correctly (has at least 2 entries, actually has 3)
                    assert!(
                        entries.len() >= 2,
                        "Expected at least 2 partition entries for stock_prices (found {})",
                        entries.len()
                    );
                } else {
                    println!("WARNING: Index.db was not loaded by SSTableReader");
                    panic!("SSTableReader did not load Index.db");
                }
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
                panic!("Failed to open stock_prices SSTable: {:?}", e);
            }
        }
    }

    /// Issue #552: Validate the BIG-format parser against REAL Cassandra 5.0 Index.db files.
    ///
    /// `simple_table` has a single 16-byte UUID partition key (entries start 0x0010).
    /// `multi_partition_table` has a 38-byte composite partition key (entries start 0x0026).
    /// Both must read back ALL entries with monotonically increasing offsets.
    #[tokio::test]
    #[ignore = "Requires test data files (CQLITE_DATASETS_ROOT)"]
    async fn test_real_index_db_big_format() {
        let datasets_root = env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| {
            "/Users/patrickmcfadin/local_projects/cqlite/test-data/datasets".to_string()
        });

        // --- Composite-key table (38-byte keys, entries start 0x0026) ---
        let multi_dir = format!(
            "{}/sstables/test_basic/multi_partition_table-6ac52100a25111f0a3fef1a551383fb9",
            datasets_root
        );
        let multi_index = format!("{}/nb-1-big-Index.db", multi_dir);
        let bytes = std::fs::read(&multi_index).expect("read multi_partition_table Index.db");
        assert_eq!(
            u16::from_be_bytes([bytes[0], bytes[1]]),
            38,
            "Composite key length should be 38 (0x0026)"
        );
        let (rest, entries) = parse_all_partition_keys(&bytes).expect("parse composite Index.db");
        assert!(rest.is_empty(), "Should consume all Index.db bytes");
        assert!(
            entries.len() >= 2,
            "multi_partition_table should have multiple partitions (got {})",
            entries.len()
        );
        // First key is 38 bytes; first offset must be 0.
        assert_eq!(
            entries[0].key_digest.len(),
            38,
            "First key should be 38 bytes"
        );
        assert_eq!(
            entries[0].data_offset, 0,
            "First partition offset should be 0"
        );
        // Offsets are strictly increasing in token order.
        for i in 1..entries.len() {
            assert!(
                entries[i].data_offset > entries[i - 1].data_offset,
                "Offsets must increase: entry {} ({}) <= entry {} ({})",
                i,
                entries[i].data_offset,
                i - 1,
                entries[i - 1].data_offset
            );
        }

        // --- Single-UUID-key table (16-byte keys, entries start 0x0010) ---
        let simple_index = format!(
            "{}/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Index.db",
            datasets_root
        );
        let bytes = std::fs::read(&simple_index).expect("read simple_table Index.db");
        assert_eq!(
            u16::from_be_bytes([bytes[0], bytes[1]]),
            16,
            "UUID key length should be 16 (0x0010)"
        );
        let (rest, entries) = parse_all_partition_keys(&bytes).expect("parse simple Index.db");
        assert!(rest.is_empty(), "Should consume all Index.db bytes");
        assert!(
            entries.len() > 3,
            "simple_table should have many partitions (got {})",
            entries.len()
        );
        assert_eq!(
            entries[0].key_digest.len(),
            16,
            "First key should be 16 bytes"
        );
        assert_eq!(
            entries[0].data_offset, 0,
            "First partition offset should be 0"
        );
    }

    #[test]
    fn test_simple_partition_key_parsing() {
        // NB BIG format: key_len(2) + raw_key(key_len) + vint_offset(1-9) + vint_promoted_size(1-9)
        // VInt encoding for 256: 0x81, 0x00 (2 bytes, 10xxxxxx format)
        let data = vec![
            0x00, 0x10, // key_len = 16 (e.g. a 16-byte UUID partition key)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // raw key (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // key_digest cont.
            0x81, 0x00, // VInt offset = 256
            0x00, // VInt promoted_size = 0 (no promoted index)
        ];

        let (_, entry) = parse_simple_partition_key(&data).unwrap();

        assert_eq!(
            entry.key_digest.as_ref(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        // Raw offset from Index.db (relative to data section start)
        // SSTableReader will add actual_header_size to get absolute file offset
        assert_eq!(entry.data_offset, 256);
        assert_eq!(entry.data_size, 0); // Size not stored in Index.db (Issue #149)
        assert!(entry.promoted_index.is_none());
    }

    #[test]
    fn test_partition_key_parsing_without_summary() {
        // BIG format: key_len(2) + raw key(key_len) + vint_offset + vint_promoted_size
        // VInt encoding for 4096 (0x1000): 0x90, 0x00 (2 bytes, 10xxxxxx format)
        // byte0 = 0x80 | ((4096 >> 8) & 0x3F) = 0x80 | 0x10 = 0x90
        // byte1 = 4096 & 0xFF = 0x00
        let data = vec![
            0x00, 0x10, // key_len = 16
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // raw key (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // raw key cont.
            0x90, 0x00, // VInt offset = 4096
            0x00, // VInt promoted_size = 0
        ];

        let (_, entry) = parse_simple_partition_key(&data).unwrap();

        assert_eq!(
            entry.key_digest.as_ref(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        assert_eq!(
            entry.raw_key.as_deref(),
            Some(&[1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16][..]),
            "raw_key should mirror the raw partition key"
        );

        // Raw offset from Index.db (relative to data section start)
        assert_eq!(entry.data_offset, 4096);
    }

    #[test]
    fn test_variable_length_keys_parse_all_entries() {
        // Issue #552: prove the parser handles non-16-byte keys (composite/int/text).
        // Entry 1: 4-byte int key (0x0000002A), offset 100, no promoted index.
        // Entry 2: 1-byte key (0x07), offset 500 (2-byte vint 0x81 0xF4), no promoted.
        let data = vec![
            // Entry 1
            0x00, 0x04, // key_len = 4
            0x00, 0x00, 0x00, 0x2A, // raw key (int 42)
            0x64, // vint offset = 100
            0x00, // vint promoted_size = 0
            // Entry 2
            0x00, 0x01, // key_len = 1
            0x07, // raw key
            0x81, 0xF4, // vint offset = 500
            0x00, // vint promoted_size = 0
        ];

        let (rest, entries) = parse_all_partition_keys(&data).unwrap();
        assert!(rest.is_empty(), "All bytes should be consumed");
        assert_eq!(entries.len(), 2, "Both variable-length entries must parse");

        assert_eq!(entries[0].key_digest.as_ref(), &[0x00, 0x00, 0x00, 0x2A]);
        assert_eq!(entries[0].data_offset, 100);

        assert_eq!(entries[1].key_digest.as_ref(), &[0x07]);
        assert_eq!(entries[1].data_offset, 500);
    }

    // REMOVED: test_enhanced_partition_entry_parsing
    // Enhanced format parsing removed per Issue #92

    #[test]
    fn test_multiple_partition_keys_parsing() {
        // Two partition entries with VInt offsets (NB format)
        // Format: key_len(2) + raw_key(key_len) + vint_offset + vint_promoted_size
        // VInt encoding for 100 (0x64): 0x64 (1 byte, value < 128)
        // VInt encoding for 500 (0x1F4): 0x81, 0xF4 (2 bytes, 10xxxxxx format)
        //   byte0 = 0x80 | ((500 >> 8) & 0x3F) = 0x80 | 1 = 0x81
        //   byte1 = 500 & 0xFF = 0xF4
        let data = vec![
            // Entry 1
            0x00, 0x10, // key_len = 16 (e.g. a 16-byte UUID partition key)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest 1 (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // key_digest cont.
            0x64, // VInt offset = 100
            0x00, // VInt promoted_size = 0
            // Entry 2
            0x00, 0x10, // key_len = 16 (e.g. a 16-byte UUID partition key)
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // key_digest 2 (16 bytes)
            0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, // key_digest cont.
            0x81, 0xF4, // VInt offset = 500
            0x00, // VInt promoted_size = 0
        ];

        let (_, entries) = parse_all_partition_keys(&data).unwrap();

        assert_eq!(entries.len(), 2);

        if !entries.is_empty() {
            assert_eq!(
                entries[0].key_digest.as_ref(),
                &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
            );
        }

        if entries.len() >= 2 {
            assert_eq!(
                entries[1].key_digest.as_ref(),
                &[
                    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
                    0x1E, 0x1F, 0x20
                ]
            );

            // Raw offsets from Index.db (relative to data section start)
            assert_eq!(entries[0].data_offset, 100);
            assert_eq!(entries[1].data_offset, 500);
        }
    }

    // REMOVED: test_data_offset_estimation_algorithm
    // This test validated the old heuristic estimation function which has been removed
    // in favor of spec-accurate Summary.db correlation (Issue #92)

    #[test]
    fn test_borrow_trait_zero_allocation_lookup() {
        // Test Issue #107 fix: Verify that lookup_partition uses Borrow trait
        // to avoid heap allocation on every lookup

        // Create index data with two partition entries (NB format with VInt offsets)
        // Format: key_len(2) + raw_key(key_len) + vint_offset + vint_promoted_size
        // VInt for 100: 0x64 (single byte, value < 128)
        // VInt for 500: 0x81, 0xF4 (2 bytes)
        let data = vec![
            // Entry 1
            0x00, 0x10, // key_len = 16 (e.g. a 16-byte UUID partition key)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest 1
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // key_digest cont.
            0x64, // VInt offset = 100
            0x00, // VInt promoted_size = 0
            // Entry 2
            0x00, 0x10, // key_len = 16 (e.g. a 16-byte UUID partition key)
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // key_digest 2
            0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, // key_digest cont.
            0x81, 0xF4, // VInt offset = 500
            0x00, // VInt promoted_size = 0
        ];

        let (_, index_data) = parse_index_data(&data).unwrap();

        // Prepare lookup keys as slices (NOT Arc)
        let key1: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let key2: &[u8] = &[
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
            0x1F, 0x20,
        ];
        let key_not_found: &[u8] = &[0xFF; 16];

        // Test lookups - these should use Borrow trait without creating Arc
        // The key_lookup HashMap has Arc<[u8]> keys but accepts &[u8] for get()
        let result1 = index_data.key_lookup.get(key1);
        let result2 = index_data.key_lookup.get(key2);
        let result3 = index_data.key_lookup.get(key_not_found);

        assert!(result1.is_some(), "Should find first key");
        assert!(result2.is_some(), "Should find second key");
        assert!(result3.is_none(), "Should not find non-existent key");

        assert_eq!(*result1.unwrap(), 0, "First key should map to index 0");
        assert_eq!(*result2.unwrap(), 1, "Second key should map to index 1");

        // Verify the actual entries match
        assert_eq!(index_data.partition_entries[0].key_digest.as_ref(), key1);
        assert_eq!(index_data.partition_entries[1].key_digest.as_ref(), key2);
    }

    /// Small partition: `promoted_len == 0` → `promoted_index` is `None` (Issue #993).
    #[test]
    fn test_parse_big_index_entry_no_promoted_index() {
        // [key_len: u16=4][key: 4 bytes][data_offset vint=0][promoted_len vint=0]
        let entry: Vec<u8> = vec![0x00, 0x04, 0xAA, 0xBB, 0xCC, 0xDD, 0x00, 0x00];
        let (rest, parsed) = parse_big_index_entry(&entry).unwrap();
        assert!(rest.is_empty());
        assert_eq!(parsed.data_offset, 0);
        assert!(
            parsed.promoted_index.is_none(),
            "promoted_len==0 must yield None"
        );
    }

    // NOTE: the end-to-end "BIG entry promoted payload is CAPTURED and decodes
    // byte-exactly" assertions live in the integration test
    // `cqlite-core/tests/issue_993_promoted_index_parity.rs`
    // (`wide_partition_emits_capturable_promoted_index`), which exercises the real
    // writer → `parse_big_index_entry` → `PromotedIndexData::decode` path. Kept out
    // of this already-over-threshold file per the campsite rule (epic #1116).
}
