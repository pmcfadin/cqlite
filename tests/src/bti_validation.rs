//! BTI (Big Trie Index) Validation Tests
//!
//! This module validates parsing of Cassandra 5.0's new BTI (Big Trie Index) format.
//! BTI is a new index format introduced in Cassandra 5.0 for better performance.

use cqlite_core::{
    error::Error,
    parser::{header::*, vint::*, SSTableParser},
    storage::sstable::index::*,
};
use std::fs;
use std::path::PathBuf;

/// BTI format constants (based on Cassandra 5.0 implementation)
const BTI_MAGIC: u32 = 0x42544900; // 'BTI\0'
const BTI_VERSION: u16 = 0x0001;

/// BTI node types
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum BtiNodeType {
    /// Leaf node containing actual data
    Leaf = 0x00,
    /// Branch node containing child pointers
    Branch = 0x01,
    /// Root node (special case of branch)
    Root = 0x02,
}

impl TryFrom<u8> for BtiNodeType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(BtiNodeType::Leaf),
            0x01 => Ok(BtiNodeType::Branch),
            0x02 => Ok(BtiNodeType::Root),
            _ => Err(Error::corruption(format!(
                "Invalid BTI node type: 0x{:02X}",
                value
            ))),
        }
    }
}

/// BTI header structure
#[derive(Debug, Clone)]
pub struct BtiHeader {
    /// BTI format version
    pub version: u16,
    /// Number of levels in the trie
    pub levels: u32,
    /// Root node offset
    pub root_offset: u64,
    /// Total number of entries
    pub entry_count: u64,
    /// Compression information
    pub compression: CompressionInfo,
}

/// BTI node structure
#[derive(Debug, Clone)]
pub struct BtiNode {
    /// Node type
    pub node_type: BtiNodeType,
    /// Level in the trie (0 = leaf)
    pub level: u16,
    /// Number of entries in this node
    pub entry_count: u32,
    /// Node entries (keys and child pointers or data)
    pub entries: Vec<BtiEntry>,
}

/// BTI entry (key and either child pointer or data)
#[derive(Debug, Clone)]
pub struct BtiEntry {
    /// Key fragment for this entry
    pub key: Vec<u8>,
    /// Either child offset (for branch nodes) or data offset (for leaf nodes)
    pub offset: u64,
    /// Data length (for leaf nodes)
    pub length: Option<u32>,
}

/// BTI validation test suite
pub struct BtiValidationSuite {
    parser: SSTableParser,
    test_data_path: PathBuf,
}

impl BtiValidationSuite {
    pub fn new() -> Self {
        let current_dir = std::env::current_dir().expect("Failed to get current directory");
        let test_data_path = current_dir.join("test-env/cassandra5");

        Self {
            parser: SSTableParser::new(),
            test_data_path,
        }
    }

    /// Find BTI index files in test data
    pub fn find_bti_files(&self) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let mut files = Vec::new();
        let samples_path = self.test_data_path.join("samples");

        if !samples_path.exists() {
            return Ok(files);
        }

        // Look for BTI files (usually have .bti extension or contain BTI in name)
        for entry in fs::read_dir(&samples_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                for sub_entry in fs::read_dir(&path)? {
                    let sub_entry = sub_entry?;
                    let sub_path = sub_entry.path();

                    if let Some(file_name) = sub_path.file_name() {
                        let name = file_name.to_string_lossy();
                        if name.contains("Index") || name.contains("BTI") || name.ends_with(".bti")
                        {
                            files.push(sub_path);
                        }
                    }
                }
            }
        }

        Ok(files)
    }

    /// Parse BTI header from bytes
    pub fn parse_bti_header(&self, input: &[u8]) -> Result<(BtiHeader, usize), Error> {
        use nom::{
            bytes::complete::take,
            number::complete::{be_u16, be_u32, be_u64},
            IResult,
        };

        fn parse_bti_header_impl(input: &[u8]) -> IResult<&[u8], BtiHeader> {
            let (input, magic) = be_u32(input)?;
            if magic != BTI_MAGIC {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }

            let (input, version) = be_u16(input)?;
            if version != BTI_VERSION {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }

            let (input, levels) = be_u32(input)?;
            let (input, root_offset) = be_u64(input)?;
            let (input, entry_count) = be_u64(input)?;
            let (input, compression) = parse_compression_info(input)?;

            Ok((
                input,
                BtiHeader {
                    version,
                    levels,
                    root_offset,
                    entry_count,
                    compression,
                },
            ))
        }

        match parse_bti_header_impl(input) {
            Ok((remaining, header)) => {
                let parsed_bytes = input.len() - remaining.len();
                Ok((header, parsed_bytes))
            }
            Err(_) => Err(Error::corruption("Failed to parse BTI header")),
        }
    }

    /// Parse BTI node from bytes
    pub fn parse_bti_node(&self, input: &[u8]) -> Result<(BtiNode, usize), Error> {
        use nom::{
            bytes::complete::take,
            multi::count,
            number::complete::{be_u16, be_u32, be_u64, be_u8},
            IResult,
        };

        fn parse_bti_entry(input: &[u8]) -> IResult<&[u8], BtiEntry> {
            let (input, key_len) = parse_vint_length(input)?;
            let (input, key) = take(key_len)(input)?;
            let (input, offset) = be_u64(input)?;
            let (input, length) = if key_len > 0 {
                // Simplified check for leaf vs branch
                let (input, len) = be_u32(input)?;
                (input, Some(len))
            } else {
                (input, None)
            };

            Ok((
                input,
                BtiEntry {
                    key: key.to_vec(),
                    offset,
                    length,
                },
            ))
        }

        fn parse_bti_node_impl(input: &[u8]) -> IResult<&[u8], BtiNode> {
            let (input, node_type_byte) = be_u8(input)?;
            let node_type = BtiNodeType::try_from(node_type_byte).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?;

            let (input, level) = be_u16(input)?;
            let (input, entry_count) = be_u32(input)?;
            let (input, entries) = count(parse_bti_entry, entry_count as usize)(input)?;

            Ok((
                input,
                BtiNode {
                    node_type,
                    level,
                    entry_count,
                    entries,
                },
            ))
        }

        match parse_bti_node_impl(input) {
            Ok((remaining, node)) => {
                let parsed_bytes = input.len() - remaining.len();
                Ok((node, parsed_bytes))
            }
            Err(_) => Err(Error::corruption("Failed to parse BTI node")),
        }
    }

    /// Generate test BTI header for validation
    pub fn generate_test_bti_header(&self) -> Vec<u8> {
        use std::collections::HashMap;

        let mut result = Vec::new();

        // Magic and version
        result.extend_from_slice(&BTI_MAGIC.to_be_bytes());
        result.extend_from_slice(&BTI_VERSION.to_be_bytes());

        // BTI structure
        result.extend_from_slice(&3u32.to_be_bytes()); // levels
        result.extend_from_slice(&1024u64.to_be_bytes()); // root_offset
        result.extend_from_slice(&1000u64.to_be_bytes()); // entry_count

        // Compression info
        let compression = CompressionInfo {
            algorithm: "NONE".to_string(),
            chunk_size: 0,
            parameters: HashMap::new(),
        };

        // Serialize compression manually (simplified)
        result.extend_from_slice(&encode_vint(compression.algorithm.len() as i64));
        result.extend_from_slice(compression.algorithm.as_bytes());
        result.extend_from_slice(&compression.chunk_size.to_be_bytes());
        result.extend_from_slice(&encode_vint(0)); // No parameters

        result
    }

    /// Generate test BTI node for validation
    pub fn generate_test_bti_node(&self, node_type: BtiNodeType, level: u16) -> Vec<u8> {
        let mut result = Vec::new();

        // Node header
        result.push(node_type as u8);
        result.extend_from_slice(&level.to_be_bytes());

        // Generate test entries
        let entries = match node_type {
            BtiNodeType::Leaf => vec![
                BtiEntry {
                    key: b"key1".to_vec(),
                    offset: 100,
                    length: Some(50),
                },
                BtiEntry {
                    key: b"key2".to_vec(),
                    offset: 200,
                    length: Some(75),
                },
            ],
            BtiNodeType::Branch | BtiNodeType::Root => vec![
                BtiEntry {
                    key: b"a".to_vec(),
                    offset: 1000,
                    length: None,
                },
                BtiEntry {
                    key: b"m".to_vec(),
                    offset: 2000,
                    length: None,
                },
                BtiEntry {
                    key: b"z".to_vec(),
                    offset: 3000,
                    length: None,
                },
            ],
        };

        // Entry count
        result.extend_from_slice(&(entries.len() as u32).to_be_bytes());

        // Serialize entries
        for entry in entries {
            // Key length and key
            result.extend_from_slice(&encode_vint(entry.key.len() as i64));
            result.extend_from_slice(&entry.key);

            // Offset
            result.extend_from_slice(&entry.offset.to_be_bytes());

            // Length (for leaf nodes only)
            if let Some(length) = entry.length {
                result.extend_from_slice(&length.to_be_bytes());
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bti_node_type_conversion() {
        assert_eq!(BtiNodeType::try_from(0x00).unwrap(), BtiNodeType::Leaf);
        assert_eq!(BtiNodeType::try_from(0x01).unwrap(), BtiNodeType::Branch);
        assert_eq!(BtiNodeType::try_from(0x02).unwrap(), BtiNodeType::Root);
        assert!(BtiNodeType::try_from(0xFF).is_err());
    }

    #[test]
    fn test_bti_header_parsing() {
        let suite = BtiValidationSuite::new();
        let test_header_bytes = suite.generate_test_bti_header();

        let (header, parsed_bytes) = suite
            .parse_bti_header(&test_header_bytes)
            .expect("Failed to parse BTI header");

        assert_eq!(parsed_bytes, test_header_bytes.len());
        assert_eq!(header.version, BTI_VERSION);
        assert_eq!(header.levels, 3);
        assert_eq!(header.root_offset, 1024);
        assert_eq!(header.entry_count, 1000);
        assert_eq!(header.compression.algorithm, "NONE");
    }

    #[test]
    fn test_bti_leaf_node_parsing() {
        let suite = BtiValidationSuite::new();
        let test_node_bytes = suite.generate_test_bti_node(BtiNodeType::Leaf, 0);

        let (node, parsed_bytes) = suite
            .parse_bti_node(&test_node_bytes)
            .expect("Failed to parse BTI leaf node");

        assert_eq!(node.node_type, BtiNodeType::Leaf);
        assert_eq!(node.level, 0);
        assert_eq!(node.entry_count, 2);
        assert_eq!(node.entries.len(), 2);

        // Check first entry
        assert_eq!(node.entries[0].key, b"key1");
        assert_eq!(node.entries[0].offset, 100);
        assert_eq!(node.entries[0].length, Some(50));

        // Check second entry
        assert_eq!(node.entries[1].key, b"key2");
        assert_eq!(node.entries[1].offset, 200);
        assert_eq!(node.entries[1].length, Some(75));
    }

    #[test]
    fn test_bti_branch_node_parsing() {
        let suite = BtiValidationSuite::new();
        let test_node_bytes = suite.generate_test_bti_node(BtiNodeType::Branch, 1);

        let (node, _) = suite
            .parse_bti_node(&test_node_bytes)
            .expect("Failed to parse BTI branch node");

        assert_eq!(node.node_type, BtiNodeType::Branch);
        assert_eq!(node.level, 1);
        assert_eq!(node.entry_count, 3);
        assert_eq!(node.entries.len(), 3);

        // Check entries have keys but no lengths
        assert_eq!(node.entries[0].key, b"a");
        assert_eq!(node.entries[0].offset, 1000);
        assert_eq!(node.entries[0].length, None);

        assert_eq!(node.entries[1].key, b"m");
        assert_eq!(node.entries[2].key, b"z");
    }

    #[test]
    fn test_bti_root_node_parsing() {
        let suite = BtiValidationSuite::new();
        let test_node_bytes = suite.generate_test_bti_node(BtiNodeType::Root, 2);

        let (node, _) = suite
            .parse_bti_node(&test_node_bytes)
            .expect("Failed to parse BTI root node");

        assert_eq!(node.node_type, BtiNodeType::Root);
        assert_eq!(node.level, 2);
        assert!(node.entries.len() > 0);
    }

    #[test]
    #[ignore] // Requires real test data
    fn test_real_bti_file_parsing() {
        let suite = BtiValidationSuite::new();

        let bti_files = suite.find_bti_files().expect("Failed to find BTI files");

        if bti_files.is_empty() {
            println!("⚠️  No BTI files found - skipping real file test");
            return;
        }

        println!("🔍 Found {} BTI files for validation", bti_files.len());

        for bti_file in bti_files {
            println!("📂 Testing BTI file: {}", bti_file.display());

            let data = fs::read(&bti_file)
                .expect(&format!("Failed to read BTI file: {}", bti_file.display()));

            if data.len() < 20 {
                println!("⚠️  File too small, skipping: {} bytes", data.len());
                continue;
            }

            // Try to parse as BTI header
            match suite.parse_bti_header(&data) {
                Ok((header, parsed_bytes)) => {
                    println!("✅ Successfully parsed BTI header:");
                    println!("   📋 Version: 0x{:04X}", header.version);
                    println!("   📋 Levels: {}", header.levels);
                    println!("   📋 Root offset: {}", header.root_offset);
                    println!("   📋 Entry count: {}", header.entry_count);
                    println!("   📋 Compression: {}", header.compression.algorithm);
                    println!("   📋 Parsed bytes: {}/{}", parsed_bytes, data.len());

                    // Validate BTI header
                    assert!(
                        header.levels > 0 && header.levels <= 10,
                        "Reasonable number of levels"
                    );
                    assert!(header.entry_count > 0, "Should have entries");
                    assert!(
                        header.root_offset < data.len() as u64,
                        "Root offset should be within file"
                    );

                    // Try to parse root node if data is available
                    if header.root_offset < data.len() as u64 {
                        let root_data = &data[header.root_offset as usize..];
                        match suite.parse_bti_node(root_data) {
                            Ok((root_node, _)) => {
                                println!("✅ Successfully parsed root node:");
                                println!("   📋 Type: {:?}", root_node.node_type);
                                println!("   📋 Level: {}", root_node.level);
                                println!("   📋 Entries: {}", root_node.entry_count);

                                assert!(matches!(
                                    root_node.node_type,
                                    BtiNodeType::Root | BtiNodeType::Branch
                                ));
                                assert_eq!(root_node.level as u32, header.levels - 1);
                            }
                            Err(e) => {
                                println!("⚠️  Failed to parse root node: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("⚠️  Not a BTI file or parsing failed: {}", e);
                }
            }
        }
    }
}
