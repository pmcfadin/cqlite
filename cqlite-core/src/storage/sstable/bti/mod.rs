//! BTI (Big Trie-Indexed) SSTable format implementation
//!
//! This module implements support for Cassandra 5.0's BTI format, which uses
//! trie-based indexes for improved performance over the legacy BIG format.

pub mod nodes;
pub mod encoder;
pub mod parser;

use crate::error::{Error, Result};
use crate::parser::header::CassandraVersion;
use std::collections::HashMap;

/// BTI format magic number ("da" in hexspeak)
pub const BTI_MAGIC_NUMBER: u32 = 0x6461_0000;

/// BTI format version
pub const BTI_FORMAT_VERSION: u16 = 0x0001;

/// Maximum trie depth to prevent infinite recursion
pub const MAX_TRIE_DEPTH: usize = 128;

/// Maximum page size for BTI nodes
pub const BTI_PAGE_SIZE: usize = 4096;

/// BTI format detection result
#[derive(Debug, Clone, PartialEq)]
pub enum FormatType {
    /// Legacy BIG format
    Big,
    /// BTI (Big Trie-Indexed) format
    Bti,
}

/// BTI SSTable metadata
#[derive(Debug, Clone)]
pub struct BtiMetadata {
    /// Format version
    pub version: u16,
    /// Cassandra version that created this BTI
    pub cassandra_version: CassandraVersion,
    /// Root trie node offset in Partitions.db
    pub partition_trie_root: u64,
    /// Root trie node offset in Rows.db (if present)
    pub row_trie_root: Option<u64>,
    /// Number of partitions in the SSTable
    pub partition_count: u64,
    /// Additional BTI-specific properties
    pub properties: HashMap<String, String>,
}

/// Detect BTI format from magic number
pub fn detect_format(magic_number: u32) -> FormatType {
    match magic_number {
        BTI_MAGIC_NUMBER => FormatType::Bti,
        _ => FormatType::Big, // Default to BIG format for all other magic numbers
    }
}

/// Check if a magic number indicates BTI format
pub fn is_bti_format(magic_number: u32) -> bool {
    magic_number == BTI_MAGIC_NUMBER
}

/// BTI-specific error types
#[derive(Debug, Clone)]
pub enum BtiError {
    /// Invalid trie node type
    InvalidNodeType(u8),
    /// Trie depth exceeded maximum
    MaxDepthExceeded(usize),
    /// Invalid byte-comparable key
    InvalidByteComparableKey(String),
    /// Corrupted trie structure
    CorruptedTrie(String),
    /// Missing BTI component file
    MissingComponent(String),
}

impl std::fmt::Display for BtiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BtiError::InvalidNodeType(node_type) => {
                write!(f, "Invalid BTI trie node type: 0x{:02x}", node_type)
            }
            BtiError::MaxDepthExceeded(depth) => {
                write!(f, "BTI trie depth exceeded maximum: {} > {}", depth, MAX_TRIE_DEPTH)
            }
            BtiError::InvalidByteComparableKey(key) => {
                write!(f, "Invalid byte-comparable key: {}", key)
            }
            BtiError::CorruptedTrie(msg) => {
                write!(f, "Corrupted BTI trie structure: {}", msg)
            }
            BtiError::MissingComponent(component) => {
                write!(f, "Missing BTI component: {}", component)
            }
        }
    }
}

impl std::error::Error for BtiError {}

impl From<BtiError> for Error {
    fn from(err: BtiError) -> Self {
        Error::ParseError(format!("BTI error: {}", err))
    }
}

/// BTI lookup result
#[derive(Debug, Clone)]
pub struct BtiLookupResult {
    /// Data file offset
    pub data_offset: u64,
    /// Data size (if known)
    pub data_size: Option<u32>,
    /// Row index offset (for large partitions)
    pub row_index_offset: Option<u64>,
}

/// BTI format configuration
#[derive(Debug, Clone)]
pub struct BtiConfig {
    /// Enable page-aware reading optimizations
    pub page_aware_reading: bool,
    /// Maximum nodes to cache in memory
    pub max_cached_nodes: usize,
    /// Enable pointer compression
    pub pointer_compression: bool,
}

impl Default for BtiConfig {
    fn default() -> Self {
        Self {
            page_aware_reading: true,
            max_cached_nodes: 1024,
            pointer_compression: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bti_magic_number_detection() {
        assert_eq!(detect_format(BTI_MAGIC_NUMBER), FormatType::Bti);
        assert_eq!(detect_format(0x6F61_0000), FormatType::Big); // Legacy 'oa' format
        assert_eq!(detect_format(0x0040_0000), FormatType::Big); // Cassandra 5.0 'nb' format
        
        assert!(is_bti_format(BTI_MAGIC_NUMBER));
        assert!(!is_bti_format(0x6F61_0000));
    }

    #[test]
    fn test_bti_error_display() {
        let err = BtiError::InvalidNodeType(0xFF);
        assert!(err.to_string().contains("Invalid BTI trie node type: 0xFF"));

        let err = BtiError::MaxDepthExceeded(150);
        assert!(err.to_string().contains("BTI trie depth exceeded maximum: 150"));

        let err = BtiError::InvalidByteComparableKey("bad_key".to_string());
        assert!(err.to_string().contains("Invalid byte-comparable key: bad_key"));
    }

    #[test]
    fn test_bti_config_default() {
        let config = BtiConfig::default();
        assert!(config.page_aware_reading);
        assert_eq!(config.max_cached_nodes, 1024);
        assert!(config.pointer_compression);
    }
}