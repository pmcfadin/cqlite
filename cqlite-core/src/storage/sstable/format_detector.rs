//! SSTable format detection and version identification
//!
//! This module provides bulletproof detection of SSTable format versions
//! across all Cassandra versions (2.x, 3.x, 4.x, 5.x) with automatic
//! format-specific parser selection.

use crate::{Error, Result};
use std::collections::HashMap;
use std::path::Path;

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
        // BTI format (BtiFormat.java:287-420): `da` is the only BTI version letter.
        // Mapped to V5x rather than Unknown so callers can select the BTI read path.
        // Full BTI routing is completed in VG5; here we ensure `da` is not Unknown.
        format_map.insert("da".to_string(), SSTableFormat::V5x("da".to_string()));

        Self { format_map }
    }

    /// Detect SSTable format from file path
    ///
    /// SSTable files follow pattern: {version}-{generation}-{size}-{component}.db
    /// Example: nb-1-big-Data.db
    pub fn detect_from_path(&self, path: &Path) -> Result<SSTableFormat> {
        let filename = path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| Error::InvalidPath(format!("Invalid SSTable filename: {:?}", path)))?;

        // Extract format version from filename
        let parts: Vec<&str> = filename.split('-').collect();
        if parts.len() < 4 {
            return Err(Error::InvalidFormat(format!(
                "Invalid SSTable filename format: {}",
                filename
            )));
        }

        let version = parts[0];
        self.detect_from_version(version)
    }

    /// Detect format from version string
    pub fn detect_from_version(&self, version: &str) -> Result<SSTableFormat> {
        self.format_map
            .get(version)
            .cloned()
            .or_else(|| Some(SSTableFormat::Unknown(version.to_string())))
            .ok_or_else(|| {
                Error::UnsupportedFormat(format!("Unknown SSTable version: {}", version))
            })
    }

    /// Detect format from multiple SSTable files in a directory
    pub fn detect_from_directory(&self, dir: &Path) -> Result<SSTableFormat> {
        use std::fs;

        let entries = fs::read_dir(dir).map_err(Error::Io)?;

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("db") {
                if let Ok(format) = self.detect_from_path(&path) {
                    return Ok(format);
                }
            }
        }

        Err(Error::InvalidFormat(
            "No valid SSTable files found in directory".to_string(),
        ))
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
#[allow(clippy::upper_case_acronyms)]
pub enum SSTableComponent {
    Data,
    Index,
    Summary,
    Filter,
    CompressionInfo,
    Statistics,
    Digest,
    /// Per-chunk CRC32 file for uncompressed SSTables (Cassandra `CRC.db`).
    ///
    /// Optional: emitted for uncompressed tables only (mutually exclusive with
    /// `CompressionInfo.db`) and shared by BIG and BTI formats. Recognized here
    /// for consistency with `directory::types::SSTableComponent::Crc`.
    Crc,
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
        } else if filename.ends_with("-CRC.db") {
            Some(SSTableComponent::Crc)
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
            SSTableComponent::Crc => "CRC.db",
            SSTableComponent::TOC => "TOC.txt",
        }
    }
}

/// SSTable file info extracted from path
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    pub format: SSTableFormat,
    /// Raw SSTable identifier as a string.
    ///
    /// Cassandra 5.0 supports two id forms (Descriptor.java:95):
    /// - Sequential integer: `"1"`, `"2"`, …
    /// - UUID-based hex string: `"6aa08200a25111f0a3fef1a551383fb9"`
    ///
    /// This field stores the raw string so both forms round-trip correctly.
    /// Use `generation_numeric()` when you need an integer id.
    pub sstable_id: String,
    /// The `<format>` name segment (`"big"` or `"bti"`).
    pub size: String,
    pub component: SSTableComponent,
    pub base_name: String,
}

impl SSTableInfo {
    /// Return the sequential generation number if the id is a plain integer.
    ///
    /// Returns `None` for UUID-based ids.
    pub fn generation_numeric(&self) -> Option<u64> {
        self.sstable_id.parse::<u64>().ok()
    }

    /// Parse SSTable info from file path.
    ///
    /// Accepts filenames with both sequential and UUID-based SSTable ids:
    /// - `nb-1-big-Data.db`
    /// - `nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db`
    ///
    /// The format segment (`big` / `bti`) is located by scanning right-to-left
    /// from the end of the parts list, so multi-segment UUID ids do not
    /// interfere with detection.
    pub fn from_path(path: &Path) -> Result<Self> {
        let filename = path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| Error::InvalidPath(format!("Invalid SSTable filename: {:?}", path)))?;

        // Strip extension for splitting.
        let base = if let Some(b) = filename.strip_suffix(".db") {
            b
        } else if let Some(b) = filename.strip_suffix(".txt") {
            b
        } else {
            filename
        };

        let parts: Vec<&str> = base.split('-').collect();
        if parts.len() < 4 {
            return Err(Error::InvalidFormat(format!(
                "Invalid SSTable filename format: {}",
                filename
            )));
        }

        let version = parts[0];

        // Locate the format segment ("big"/"bti") by scanning right-to-left.
        // Start at index 2 so we never confuse the version with the format.
        let format_idx = parts[2..]
            .iter()
            .enumerate()
            .rev()
            .find(|(_, p)| **p == "big" || **p == "bti")
            .map(|(i, _)| i + 2);

        let format_idx = format_idx.ok_or_else(|| {
            Error::InvalidFormat(format!(
                "No 'big' or 'bti' format segment found in {:?}",
                filename
            ))
        })?;

        let size = parts[format_idx].to_string(); // "big" or "bti"

        // id is everything between version and format
        let sstable_id = parts[1..format_idx].join("-");

        // component is everything after format, re-joined with original extension
        let component_base = parts[format_idx + 1..].join("-");
        let extension = if filename.ends_with(".db") {
            ".db"
        } else {
            ".txt"
        };
        let component_filename = format!("-{}{}", component_base, extension);
        let component = SSTableComponent::from_filename(&component_filename).ok_or_else(|| {
            Error::InvalidFormat(format!(
                "Unknown SSTable component {:?} in {:?}",
                component_base, filename
            ))
        })?;

        // base_name is <version>-<id>-<format>  (used to build sibling filenames)
        let base_name = format!("{}-{}-{}", version, sstable_id, size);

        let detector = FormatDetector::new();
        let format = detector.detect_from_version(version)?;

        Ok(SSTableInfo {
            format,
            sstable_id,
            size,
            component,
            base_name,
        })
    }

    /// Get path to companion component file
    pub fn companion_path(
        &self,
        component: SSTableComponent,
        base_dir: &Path,
    ) -> std::path::PathBuf {
        base_dir.join(format!("{}-{}", self.base_name, component.suffix()))
    }
}

impl Default for SSTableInfo {
    fn default() -> Self {
        Self {
            format: SSTableFormat::Unknown("unknown".to_string()),
            sstable_id: "0".to_string(),
            size: "unknown".to_string(),
            component: SSTableComponent::Data,
            base_name: "unknown".to_string(),
        }
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
        assert_eq!(
            detector.detect_from_version("nb").unwrap(),
            SSTableFormat::V4x("nb".to_string())
        );
        assert_eq!(
            detector.detect_from_version("ma").unwrap(),
            SSTableFormat::V3x("ma".to_string())
        );
        assert_eq!(
            detector.detect_from_version("oa").unwrap(),
            SSTableFormat::V5x("oa".to_string())
        );
    }

    #[test]
    fn test_path_parsing() {
        let detector = FormatDetector::new();
        let path = PathBuf::from("nb-1-big-Data.db");

        let format = detector.detect_from_path(&path).unwrap();
        assert_eq!(format, SSTableFormat::V4x("nb".to_string()));
    }

    #[test]
    fn test_sstable_info_parsing_sequential_id() {
        let path = PathBuf::from("nb-1-big-Data.db");
        let info = SSTableInfo::from_path(&path).unwrap();

        assert_eq!(info.format, SSTableFormat::V4x("nb".to_string()));
        assert_eq!(info.sstable_id, "1");
        assert_eq!(info.generation_numeric(), Some(1));
        assert_eq!(info.size, "big");
        assert_eq!(info.component, SSTableComponent::Data);
        assert_eq!(info.base_name, "nb-1-big");
    }

    /// Regression test: UUID-based SSTable ids must parse without error.
    ///
    /// Real Cassandra 5.0 clusters write UUID-based ids by default
    /// (`uuid_sstable_identifiers_enabled: true`, Descriptor.java:95).
    /// The old code called `parts[1].parse::<u64>()` which failed for hex ids.
    #[test]
    fn test_sstable_info_parsing_uuid_id() {
        let path = PathBuf::from("nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db");
        let info = SSTableInfo::from_path(&path).unwrap();

        assert_eq!(info.format, SSTableFormat::V4x("nb".to_string()));
        assert_eq!(info.sstable_id, "6aa08200a25111f0a3fef1a551383fb9");
        assert_eq!(info.generation_numeric(), None, "UUID id is not numeric");
        assert_eq!(info.size, "big");
        assert_eq!(info.component, SSTableComponent::Data);
        assert_eq!(info.base_name, "nb-6aa08200a25111f0a3fef1a551383fb9-big");
    }

    /// Regression test: oa-version UUID-id files parse correctly.
    #[test]
    fn test_sstable_info_parsing_oa_uuid_id() {
        let path = PathBuf::from("oa-6aa08200a25111f0a3fef1a551383fb9-big-Data.db");
        let info = SSTableInfo::from_path(&path).unwrap();
        assert_eq!(info.format, SSTableFormat::V5x("oa".to_string()));
        assert_eq!(info.sstable_id, "6aa08200a25111f0a3fef1a551383fb9");
        assert_eq!(info.size, "big");
    }

    /// BTI da-version Data.db files parse correctly; `da` maps to V5x (not Unknown).
    #[test]
    fn test_sstable_info_parsing_da_bti_data() {
        let path = PathBuf::from("da-1-bti-Data.db");
        let info = SSTableInfo::from_path(&path).unwrap();
        assert_eq!(info.sstable_id, "1");
        assert_eq!(info.size, "bti");
        assert_eq!(info.component, SSTableComponent::Data);
        // FormatDetector must map `da` to V5x, not Unknown (VG1 requirement).
        assert_eq!(
            info.format,
            SSTableFormat::V5x("da".to_string()),
            "da must be V5x, not Unknown"
        );
    }

    /// FormatDetector: `da` must resolve to V5x (not Unknown).
    #[test]
    fn test_format_detector_da_is_v5x_not_unknown() {
        let detector = FormatDetector::new();
        let fmt = detector.detect_from_version("da").unwrap();
        assert_eq!(
            fmt,
            SSTableFormat::V5x("da".to_string()),
            "FormatDetector must return V5x for 'da', not Unknown"
        );
        assert!(
            detector.is_supported("da"),
            "da must be a supported version"
        );
    }

    /// BTI Partitions.db is a BTI-specific component not in SSTableComponent enum;
    /// from_path returns an error for unknown components.
    #[test]
    fn test_sstable_info_parsing_da_bti_partitions_unknown() {
        let path = PathBuf::from("da-1-bti-Partitions.db");
        // Partitions.db is a BTI-specific component not registered in SSTableComponent
        let result = SSTableInfo::from_path(&path);
        assert!(
            result.is_err(),
            "da-1-bti-Partitions.db should fail with unknown component"
        );
    }

    #[test]
    fn test_component_detection() {
        assert_eq!(
            SSTableComponent::from_filename("nb-1-big-Data.db"),
            Some(SSTableComponent::Data)
        );
        assert_eq!(
            SSTableComponent::from_filename("nb-1-big-CompressionInfo.db"),
            Some(SSTableComponent::CompressionInfo)
        );
        assert_eq!(
            SSTableComponent::from_filename("nb-1-big-TOC.txt"),
            Some(SSTableComponent::TOC)
        );
    }

    /// `CRC.db` must round-trip in BOTH `SSTableComponent` models (#1048).
    ///
    /// Cassandra emits `CRC.db` for uncompressed SSTables. The
    /// `directory::types` enum already recognized it (#966); this confirms the
    /// filename-scan `format_detector` enum now agrees.
    #[test]
    fn test_crc_component_round_trips_in_both_models() {
        // format_detector model: from_filename -> variant -> suffix
        assert_eq!(
            SSTableComponent::from_filename("nb-1-big-CRC.db"),
            Some(SSTableComponent::Crc)
        );
        assert_eq!(SSTableComponent::Crc.suffix(), "CRC.db");

        // directory::types model: from_str -> variant -> file_extension
        use crate::storage::sstable::directory::types::SSTableComponent as DirComponent;
        let dir_component = "CRC.db"
            .parse::<DirComponent>()
            .expect("directory model must parse CRC.db");
        assert_eq!(dir_component, DirComponent::Crc);
        assert_eq!(dir_component.file_extension(), "CRC.db");
    }

    #[test]
    fn test_format_features() {
        let format = SSTableFormat::V4x("nb".to_string());
        assert!(format.supports_compression());
        assert!(format.uses_chunk_compression());
        assert_eq!(format.default_compression(), "LZ4Compressor");
    }
}
