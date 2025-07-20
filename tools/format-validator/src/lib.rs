//! Cassandra 5+ SSTable Format Validation Toolkit
//!
//! This toolkit provides comprehensive validation and analysis tools for
//! Cassandra 5+ SSTable format compliance, with zero tolerance for deviations.

pub mod analyzer;
pub mod checker;
pub mod detector;
pub mod validator;

use std::path::Path;
use thiserror::Error;

/// Format validation error types
#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Invalid magic number: expected {expected:#010x}, found {found:#010x}")]
    InvalidMagic { expected: u32, found: u32 },
    
    #[error("Unsupported format version: {version}")]
    UnsupportedVersion { version: String },
    
    #[error("Checksum mismatch: expected {expected:#010x}, calculated {calculated:#010x}")]
    ChecksumMismatch { expected: u32, calculated: u32 },
    
    #[error("Invalid VInt encoding at offset {offset}: {reason}")]
    InvalidVInt { offset: usize, reason: String },
    
    #[error("File truncated: expected {expected} bytes, found {found}")]
    FileTruncated { expected: usize, found: usize },
    
    #[error("Invalid UTF-8 string at offset {offset}: {reason}")]
    InvalidUtf8 { offset: usize, reason: String },
    
    #[error("Compression error: {reason}")]
    CompressionError { reason: String },
    
    #[error("Structure violation: {reason}")]
    StructureViolation { reason: String },
    
    #[error("Cross-reference error: {reason}")]
    CrossReference { reason: String },
    
    #[error("BTI format error: {reason}")]
    BtiFormat { reason: String },
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Parse error: {0}")]
    Parse(String),
}

/// Validation result with detailed information
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub file_path: String,
    pub format_version: Option<String>,
    pub is_valid: bool,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<String>,
    pub statistics: ValidationStatistics,
}

/// Detailed validation statistics
#[derive(Debug, Clone, Default)]
pub struct ValidationStatistics {
    pub file_size: u64,
    pub header_size: usize,
    pub data_size: u64,
    pub index_size: u64,
    pub compression_ratio: Option<f64>,
    pub partition_count: Option<u64>,
    pub row_count: Option<u64>,
    pub validation_time_ms: u64,
}

/// Format specification constants
pub mod format_constants {
    /// Magic number for Cassandra 5+ BigFormat 'oa'
    pub const BIG_FORMAT_OA_MAGIC: u32 = 0x6F61_0000;
    
    /// Magic number for BTI format 'da'
    pub const BTI_FORMAT_DA_MAGIC: u32 = 0x6461_0000;
    
    /// Current supported version
    pub const SUPPORTED_VERSION: u16 = 0x0001;
    
    /// Statistics.db magic number
    pub const STATISTICS_MAGIC: u32 = 0x5354_4154; // "STAT"
    
    /// Maximum VInt size for safety
    pub const MAX_VINT_SIZE: usize = 9;
    
    /// Default BTI block granularity
    pub const BTI_DEFAULT_BLOCK_SIZE: u32 = 16384; // 16KB
}

/// File type detection
#[derive(Debug, Clone, PartialEq)]
pub enum SSTableFileType {
    Data,
    Index,
    Summary,
    Filter,
    Statistics,
    CompressionInfo,
    Partitions, // BTI
    Rows,       // BTI
    Digest,
    Toc,
    Unknown,
}

impl SSTableFileType {
    pub fn from_path(path: &Path) -> Self {
        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            if file_name.ends_with("-Data.db") {
                Self::Data
            } else if file_name.ends_with("-Index.db") {
                Self::Index
            } else if file_name.ends_with("-Summary.db") {
                Self::Summary
            } else if file_name.ends_with("-Filter.db") {
                Self::Filter
            } else if file_name.ends_with("-Statistics.db") {
                Self::Statistics
            } else if file_name.ends_with("-CompressionInfo.db") {
                Self::CompressionInfo
            } else if file_name.ends_with("-Partitions.db") {
                Self::Partitions
            } else if file_name.ends_with("-Rows.db") {
                Self::Rows
            } else if file_name.ends_with("-Digest.crc32") {
                Self::Digest
            } else if file_name.ends_with("-TOC.txt") {
                Self::Toc
            } else {
                Self::Unknown
            }
        } else {
            Self::Unknown
        }
    }
}

/// Comprehensive validation traits
pub trait FormatValidator {
    fn validate(&self, file_path: &Path) -> Result<ValidationResult, ValidationError>;
    fn validate_bytes(&self, data: &[u8], file_type: SSTableFileType) -> Result<ValidationResult, ValidationError>;
}

pub trait HexAnalyzer {
    fn analyze_hex(&self, data: &[u8], offset: usize, length: usize) -> String;
    fn find_magic_numbers(&self, data: &[u8]) -> Vec<(usize, u32)>;
    fn analyze_vints(&self, data: &[u8]) -> Vec<(usize, i64, usize)>;
}

pub trait DeviationDetector {
    fn compare_with_reference(&self, file1: &Path, file2: &Path) -> Result<Vec<String>, ValidationError>;
    fn detect_format_deviations(&self, data: &[u8]) -> Vec<String>;
}

/// Utility functions
pub mod utils {
    use super::*;
    use std::fs::File;
    use std::io::Read;
    
    /// Read file into memory with size limit
    pub fn read_file_safe(path: &Path, max_size: usize) -> Result<Vec<u8>, ValidationError> {
        let mut file = File::open(path)?;
        let metadata = file.metadata()?;
        
        if metadata.len() > max_size as u64 {
            return Err(ValidationError::StructureViolation {
                reason: format!("File too large: {} bytes (max: {})", metadata.len(), max_size)
            });
        }
        
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }
    
    /// Calculate CRC32 checksum
    pub fn calculate_crc32(data: &[u8]) -> u32 {
        crc32fast::hash(data)
    }
    
    /// Verify file magic number
    pub fn verify_magic(data: &[u8], expected: u32) -> Result<(), ValidationError> {
        if data.len() < 4 {
            return Err(ValidationError::FileTruncated {
                expected: 4,
                found: data.len()
            });
        }
        
        let found = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if found != expected {
            return Err(ValidationError::InvalidMagic {
                expected,
                found
            });
        }
        
        Ok(())
    }
    
    /// Format bytes as hex dump
    pub fn format_hex_dump(data: &[u8], offset: usize, length: usize) -> String {
        let start = offset.min(data.len());
        let end = (offset + length).min(data.len());
        let slice = &data[start..end];
        
        let mut result = String::new();
        for (i, chunk) in slice.chunks(16).enumerate() {
            let addr = start + i * 16;
            result.push_str(&format!("{:08x}: ", addr));
            
            // Hex bytes
            for (j, byte) in chunk.iter().enumerate() {
                if j == 8 {
                    result.push(' ');
                }
                result.push_str(&format!("{:02x} ", byte));
            }
            
            // Padding for short lines
            for _ in chunk.len()..16 {
                result.push_str("   ");
            }
            
            result.push_str(" |");
            
            // ASCII representation
            for byte in chunk {
                if byte.is_ascii_graphic() || *byte == b' ' {
                    result.push(*byte as char);
                } else {
                    result.push('.');
                }
            }
            
            result.push_str("|\n");
        }
        
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::utils::*;
    
    #[test]
    fn test_file_type_detection() {
        use std::path::PathBuf;
        
        assert_eq!(
            SSTableFileType::from_path(&PathBuf::from("test-keyspace-table-1-Data.db")),
            SSTableFileType::Data
        );
        
        assert_eq!(
            SSTableFileType::from_path(&PathBuf::from("test-keyspace-table-1-Partitions.db")),
            SSTableFileType::Partitions
        );
    }
    
    #[test]
    fn test_magic_verification() {
        let data = vec![0x6F, 0x61, 0x00, 0x00, 0x00, 0x01];
        assert!(verify_magic(&data, format_constants::BIG_FORMAT_OA_MAGIC).is_ok());
        
        let bad_data = vec![0xFF, 0xFF, 0x00, 0x00];
        assert!(verify_magic(&bad_data, format_constants::BIG_FORMAT_OA_MAGIC).is_err());
    }
    
    #[test]
    fn test_crc32_calculation() {
        let data = b"hello world";
        let checksum = calculate_crc32(data);
        assert_ne!(checksum, 0);
        
        // Verify consistency
        assert_eq!(checksum, calculate_crc32(data));
    }
    
    #[test]
    fn test_hex_dump_formatting() {
        let data = b"Hello, World! This is a test.";
        let dump = format_hex_dump(data, 0, data.len());
        assert!(dump.contains("48656c6c6f")); // "Hello" in hex
        assert!(dump.contains("Hello"));       // ASCII representation
    }
}