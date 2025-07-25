//! SSTable format detection and version identification
//!
//! This module provides bulletproof detection of SSTable format versions
//! across all Cassandra versions (2.x, 3.x, 4.x, 5.x) with automatic
//! format-specific parser selection.

use std::path::Path;
use std::collections::HashMap;
use crate::{Error, Result};

/// SSTable format versions supported by CQLite
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SSTableFormat {
    /// Cassandra 2.x formats (ic, jb, etc.)
    V2x(String),
    /// Cassandra 3.x formats (ma, mb, mc, md, me)
    V3x(String),
    /// Cassandra 4.x formats (na, nb)
    V4x(String),
    /// Cassandra 5.x formats (oa, etc.)
    V5x(String),
    /// Unknown or unsupported format
    Unknown(String),
}

impl SSTableFormat {
    /// Get the format version string
    pub fn version(&self) -> &str {
        match self {
            SSTableFormat::V2x(v) => v,
            SSTableFormat::V3x(v) => v,
            SSTableFormat::V4x(v) => v,
            SSTableFormat::V5x(v) => v,
            SSTableFormat::Unknown(v) => v,
        }
    }

    /// Check if format supports compression
    pub fn supports_compression(&self) -> bool {
        match self {
            SSTableFormat::V2x(_) => true,
            SSTableFormat::V3x(_) => true,
            SSTableFormat::V4x(_) => true,
            SSTableFormat::V5x(_) => true,
            SSTableFormat::Unknown(_) => false,
        }
    }

    /// Check if format uses chunk-based compression
    pub fn uses_chunk_compression(&self) -> bool {
        match self {
            SSTableFormat::V2x(_) => true,
            SSTableFormat::V3x(_) => true,
            SSTableFormat::V4x(_) => true,
            SSTableFormat::V5x(_) => true,
            SSTableFormat::Unknown(_) => false,
        }
    }

    /// Get expected compression algorithm
    pub fn default_compression(&self) -> &'static str {
        match self {
            SSTableFormat::V2x(_) => "SnappyCompressor",
            SSTableFormat::V3x(_) => "LZ4Compressor",
            SSTableFormat::V4x(_) => "LZ4Compressor",
            SSTableFormat::V5x(_) => "LZ4Compressor",
            SSTableFormat::Unknown(_) => "LZ4Compressor",
        }
    }
}

/// SSTable format detector with comprehensive version support
pub struct FormatDetector {
    /// Known format version mappings
    format_map: HashMap<String, SSTableFormat>,
}

impl FormatDetector {
    /// Create a new format detector with all known versions
    pub fn new() -> Self {
        let mut format_map = HashMap::new();
        
        // Cassandra 2.x formats
        format_map.insert("ic".to_string(), SSTableFormat::V2x("ic".to_string()));
        format_map.insert("jb".to_string(), SSTableFormat::V2x("jb".to_string()));
        
        // Cassandra 3.x formats
        format_map.insert("ma".to_string(), SSTableFormat::V3x("ma".to_string()));
        format_map.insert("mb".to_string(), SSTableFormat::V3x("mb".to_string()));
        format_map.insert("mc".to_string(), SSTableFormat::V3x("mc".to_string()));
        format_map.insert("md".to_string(), SSTableFormat::V3x("md".to_string()));
        format_map.insert("me".to_string(), SSTableFormat::V3x("me".to_string()));
        
        // Cassandra 4.x formats
        format_map.insert("na".to_string(), SSTableFormat::V4x("na".to_string()));
        format_map.insert("nb".to_string(), SSTableFormat::V4x("nb".to_string()));
        
        // Cassandra 5.x formats
        format_map.insert("oa".to_string(), SSTableFormat::V5x("oa".to_string()));
        
        Self { format_map }
    }

    /// Detect SSTable format from file path
    /// 
    /// SSTable files follow pattern: {version}-{generation}-{size}-{component}.db
    /// Example: nb-1-big-Data.db
    pub fn detect_from_path(&self, path: &Path) -> Result<SSTableFormat> {
        let filename = path.file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| Error::InvalidPath(format!("Invalid SSTable filename: {:?}", path)))?;

        // Extract format version from filename
        let parts: Vec<&str> = filename.split('-').collect();
        if parts.len() < 4 {
            return Err(Error::InvalidFormat(format!(
                "Invalid SSTable filename format: {}", filename
            )));
        }

        let version = parts[0];
        self.detect_from_version(version)
    }

    /// Detect format from version string
    pub fn detect_from_version(&self, version: &str) -> Result<SSTableFormat> {
        self.format_map.get(version)
            .cloned()
            .or_else(|| Some(SSTableFormat::Unknown(version.to_string())))
            .ok_or_else(|| Error::UnsupportedFormat(format!("Unknown SSTable version: {}", version)))
    }

    /// Detect format from multiple SSTable files in a directory
    pub fn detect_from_directory(&self, dir: &Path) -> Result<SSTableFormat> {
        use std::fs;
        
        let entries = fs::read_dir(dir)
            .map_err(|e| Error::Io(e))?;

        for entry in entries {
            let entry = entry.map_err(|e| Error::Io(e))?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("db") {
                if let Ok(format) = self.detect_from_path(&path) {
                    return Ok(format);
                }
            }
        }

        Err(Error::InvalidFormat("No valid SSTable files found in directory".to_string()))
    }

    /// Get all supported format versions
    pub fn supported_versions(&self) -> Vec<String> {
        self.format_map.keys().cloned().collect()
    }

    /// Check if a format version is supported
    pub fn is_supported(&self, version: &str) -> bool {
        self.format_map.contains_key(version)
    }
}

impl Default for FormatDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// SSTable file components
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SSTableComponent {
    Data,
    Index,
    Summary,
    Filter,
    CompressionInfo,
    Statistics,
    Digest,
    TOC,
}

impl SSTableComponent {
    /// Parse component from filename
    pub fn from_filename(filename: &str) -> Option<Self> {
        if filename.ends_with("-Data.db") {
            Some(SSTableComponent::Data)
        } else if filename.ends_with("-Index.db") {
            Some(SSTableComponent::Index)
        } else if filename.ends_with("-Summary.db") {
            Some(SSTableComponent::Summary)
        } else if filename.ends_with("-Filter.db") {
            Some(SSTableComponent::Filter)
        } else if filename.ends_with("-CompressionInfo.db") {
            Some(SSTableComponent::CompressionInfo)
        } else if filename.ends_with("-Statistics.db") {
            Some(SSTableComponent::Statistics)
        } else if filename.ends_with("-Digest.crc32") {
            Some(SSTableComponent::Digest)
        } else if filename.ends_with("-TOC.txt") {
            Some(SSTableComponent::TOC)
        } else {
            None
        }
    }

    /// Get component file suffix
    pub fn suffix(&self) -> &'static str {
        match self {
            SSTableComponent::Data => "Data.db",
            SSTableComponent::Index => "Index.db",
            SSTableComponent::Summary => "Summary.db",
            SSTableComponent::Filter => "Filter.db",
            SSTableComponent::CompressionInfo => "CompressionInfo.db",
            SSTableComponent::Statistics => "Statistics.db",
            SSTableComponent::Digest => "Digest.crc32",
            SSTableComponent::TOC => "TOC.txt",
        }
    }
}

/// SSTable file info extracted from path
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    pub format: SSTableFormat,
    pub generation: u64,
    pub size: String,
    pub component: SSTableComponent,
    pub base_name: String,
}

impl SSTableInfo {
    /// Parse SSTable info from file path
    pub fn from_path(path: &Path) -> Result<Self> {
        let detector = FormatDetector::new();
        let format = detector.detect_from_path(path)?;
        
        let filename = path.file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| Error::InvalidPath(format!("Invalid SSTable filename: {:?}", path)))?;

        let parts: Vec<&str> = filename.split('-').collect();
        if parts.len() < 4 {
            return Err(Error::InvalidFormat(format!(
                "Invalid SSTable filename format: {}", filename
            )));
        }

        let generation = parts[1].parse::<u64>()
            .map_err(|_| Error::InvalidFormat(format!("Invalid generation number: {}", parts[1])))?;

        let size = parts[2].to_string();
        
        let component_suffix = parts[3..].join("-");
        let component = SSTableComponent::from_filename(&format!("-{}", component_suffix))
            .ok_or_else(|| Error::InvalidFormat(format!("Unknown component: {}", component_suffix)))?;

        let base_name = format!("{}-{}-{}", parts[0], parts[1], parts[2]);

        Ok(SSTableInfo {
            format,
            generation,
            size,
            component,
            base_name,
        })
    }

    /// Get path to companion component file
    pub fn companion_path(&self, component: SSTableComponent, base_dir: &Path) -> std::path::PathBuf {
        base_dir.join(format!("{}-{}", self.base_name, component.suffix()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_format_detection() {
        let detector = FormatDetector::new();
        
        // Test various format versions
        assert_eq!(detector.detect_from_version("nb").unwrap(), SSTableFormat::V4x("nb".to_string()));
        assert_eq!(detector.detect_from_version("ma").unwrap(), SSTableFormat::V3x("ma".to_string()));
        assert_eq!(detector.detect_from_version("oa").unwrap(), SSTableFormat::V5x("oa".to_string()));
    }

    #[test]
    fn test_path_parsing() {
        let detector = FormatDetector::new();
        let path = PathBuf::from("nb-1-big-Data.db");
        
        let format = detector.detect_from_path(&path).unwrap();
        assert_eq!(format, SSTableFormat::V4x("nb".to_string()));
    }

    #[test]
    fn test_sstable_info_parsing() {
        let path = PathBuf::from("nb-1-big-Data.db");
        let info = SSTableInfo::from_path(&path).unwrap();
        
        assert_eq!(info.format, SSTableFormat::V4x("nb".to_string()));
        assert_eq!(info.generation, 1);
        assert_eq!(info.size, "big");
        assert_eq!(info.component, SSTableComponent::Data);
        assert_eq!(info.base_name, "nb-1-big");
    }

    #[test]
    fn test_component_detection() {
        assert_eq!(SSTableComponent::from_filename("nb-1-big-Data.db"), Some(SSTableComponent::Data));
        assert_eq!(SSTableComponent::from_filename("nb-1-big-CompressionInfo.db"), Some(SSTableComponent::CompressionInfo));
        assert_eq!(SSTableComponent::from_filename("nb-1-big-TOC.txt"), Some(SSTableComponent::TOC));
    }

    #[test]
    fn test_format_features() {
        let format = SSTableFormat::V4x("nb".to_string());
        assert!(format.supports_compression());
        assert!(format.uses_chunk_compression());
        assert_eq!(format.default_compression(), "LZ4Compressor");
    }
}