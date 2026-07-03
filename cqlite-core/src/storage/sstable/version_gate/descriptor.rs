//! SSTable filename descriptor parsing (`<version>-<id>-<format>-<component>`).
//!
//! Extracts the two-letter version string, id, format family, and component
//! from a Cassandra SSTable filename or path. See the module-level docs in
//! [`super`] for the authority chain and SSTable-ID forms.

use std::path::Path;

use crate::{Error, Result};

/// SSTable format family: BIG (`big`) or BTI (`bti`).
///
/// Matches the `<format>` segment of the Cassandra filename pattern
/// `<version>-<id>-<format>-<component>.db`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SsTableFormat {
    /// "big" – the classic BIG format (Cassandra 3.0 – 5.0).
    Big,
    /// "bti" – the trie-based BTI format (Cassandra 5.0+).
    Bti,
}

impl SsTableFormat {
    /// Parse format name from string (`"big"` or `"bti"`).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "big" => Some(Self::Big),
            "bti" => Some(Self::Bti),
            _ => None,
        }
    }

    /// Return the canonical lowercase name used in filenames.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Big => "big",
            Self::Bti => "bti",
        }
    }
}

/// Parsed Cassandra SSTable descriptor extracted from a filename.
///
/// Filename pattern (Descriptor.java:251):
/// ```text
/// <version>-<id>-<format>-<component>.db
/// ```
///
/// Both sequential integer IDs (`1`, `2`, …) and UUID-ish hex string IDs
/// (`6aa08200a25111f0a3fef1a551383fb9`) are accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsTableDescriptor {
    /// Two-letter version string, e.g. `"nb"`, `"oa"`, `"da"`.
    pub version: String,
    /// Raw SSTable id as found in the filename (integer string or hex UUID).
    pub sstable_id: String,
    /// Format family (`big` or `bti`).
    pub format: SsTableFormat,
    /// Component suffix after the last `-`, e.g. `"Data.db"`.
    pub component: String,
}

impl SsTableDescriptor {
    /// Parse a Cassandra SSTable descriptor from a filename or file path.
    ///
    /// Accepts both:
    /// - `nb-1-big-Data.db`               (sequential integer id)
    /// - `nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db`  (UUID hex id)
    /// - `oa-00000000-0000-0000-0000-000000000001-big-Data.db` (hyphenated UUID)
    ///
    /// Returns an error if the filename does not contain at least four
    /// dash-separated segments or if the format segment is not `big` or `bti`.
    pub fn parse(path: &Path) -> Result<Self> {
        let filename = path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| Error::InvalidPath(format!("Invalid SSTable path: {:?}", path)))?;

        Self::parse_filename(filename)
    }

    /// Parse from a bare filename string (no directory component required).
    pub fn parse_filename(filename: &str) -> Result<Self> {
        // Strip the `.db` extension if present so we can reason about the parts.
        let base = if let Some(b) = filename.strip_suffix(".db") {
            b
        } else if let Some(b) = filename.strip_suffix(".txt") {
            // TOC.txt – strip .txt instead
            b
        } else {
            filename
        };

        // Split on `-`.  The component itself may contain `-` (e.g. `TOC`
        // doesn't, but `CompressionInfo` doesn't either – however, future
        // components could).  We therefore split from the left and treat
        // everything from part[3] onwards as the component.
        //
        // Pattern: <version>-<id>-<format>-<component>
        //   parts[0] = version  (always 2 lowercase letters: [a-z]{2})
        //   parts[1..n-2] = id  (one or more dash-joined segments)
        //   parts[n-1] = format ("big" or "bti")
        //   parts[n] = component (rest of original, including original `.db` suffix)
        //
        // We search for the format segment by scanning right-to-left after
        // the first part for "big" or "bti", which avoids being tripped up
        // by dash-separated UUID ids.

        let parts: Vec<&str> = base.split('-').collect();
        if parts.len() < 4 {
            return Err(Error::InvalidFormat(format!(
                "SSTable filename has fewer than 4 dash-separated segments: {:?}",
                filename
            )));
        }

        let version = parts[0];
        // Validate version is exactly 2 lowercase letters.
        if version.len() != 2 || !version.chars().all(|c| c.is_ascii_lowercase()) {
            return Err(Error::InvalidFormat(format!(
                "SSTable version segment must be 2 lowercase letters, got {:?} in {:?}",
                version, filename
            )));
        }

        // Find the format segment by scanning right-to-left (skip the last
        // component part), starting from parts[2].
        // Strategy: look for "big" or "bti" starting from the second-to-last
        // non-component position.  The component name never equals "big" or "bti".
        let format_idx = parts[2..]
            .iter()
            .enumerate()
            .rev()
            .find(|(_, p)| **p == "big" || **p == "bti")
            .map(|(i, _)| i + 2); // offset back to original parts index

        let format_idx = format_idx.ok_or_else(|| {
            Error::InvalidFormat(format!(
                "No 'big' or 'bti' format segment found in {:?}",
                filename
            ))
        })?;

        let format = SsTableFormat::parse(parts[format_idx]).ok_or_else(|| {
            Error::InvalidFormat(format!(
                "Unknown format {:?} in {:?}",
                parts[format_idx], filename
            ))
        })?;

        // id is everything between version and format
        let sstable_id = parts[1..format_idx].join("-");

        // component is everything after format, re-joined and with extension restored
        let component_base = parts[format_idx + 1..].join("-");
        // Re-attach original extension
        let extension = if filename.ends_with(".db") {
            ".db"
        } else {
            ".txt"
        };
        let component = format!("{}{}", component_base, extension);

        Ok(Self {
            version: version.to_string(),
            sstable_id,
            format,
            component,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_descriptor_sequential_id() {
        let desc = SsTableDescriptor::parse_filename("nb-1-big-Data.db").unwrap();
        assert_eq!(desc.version, "nb");
        assert_eq!(desc.sstable_id, "1");
        assert_eq!(desc.format, SsTableFormat::Big);
        assert_eq!(desc.component, "Data.db");
    }

    #[test]
    fn test_descriptor_uuid_id_no_hyphens() {
        // UUID form used in the CQLite test corpus: 32-hex-char id with no hyphens
        let filename = "nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db";
        let desc = SsTableDescriptor::parse_filename(filename).unwrap();
        assert_eq!(desc.version, "nb");
        assert_eq!(desc.sstable_id, "6aa08200a25111f0a3fef1a551383fb9");
        assert_eq!(desc.format, SsTableFormat::Big);
        assert_eq!(desc.component, "Data.db");
    }

    #[test]
    fn test_descriptor_oa_version() {
        let desc = SsTableDescriptor::parse_filename("oa-1-big-Data.db").unwrap();
        assert_eq!(desc.version, "oa");
        assert_eq!(desc.format, SsTableFormat::Big);
    }

    #[test]
    fn test_descriptor_da_bti_version() {
        let desc = SsTableDescriptor::parse_filename("da-1-bti-Partitions.db").unwrap();
        assert_eq!(desc.version, "da");
        assert_eq!(desc.format, SsTableFormat::Bti);
        assert_eq!(desc.component, "Partitions.db");
    }

    #[test]
    fn test_descriptor_legacy_versions() {
        for version in &["ma", "mb", "mc", "md", "me", "na"] {
            let filename = format!("{}-3-big-Data.db", version);
            let desc = SsTableDescriptor::parse_filename(&filename).unwrap();
            assert_eq!(desc.version, *version, "version mismatch for {}", filename);
            assert_eq!(desc.format, SsTableFormat::Big);
        }
    }

    #[test]
    fn test_descriptor_toc_txt() {
        let desc = SsTableDescriptor::parse_filename("nb-1-big-TOC.txt").unwrap();
        assert_eq!(desc.version, "nb");
        assert_eq!(desc.component, "TOC.txt");
    }

    #[test]
    fn test_descriptor_compression_info() {
        let desc = SsTableDescriptor::parse_filename("nb-1-big-CompressionInfo.db").unwrap();
        assert_eq!(desc.component, "CompressionInfo.db");
    }

    #[test]
    fn test_descriptor_invalid_too_few_parts() {
        assert!(SsTableDescriptor::parse_filename("nb-Data.db").is_err());
        assert!(SsTableDescriptor::parse_filename("Data.db").is_err());
    }

    #[test]
    fn test_descriptor_invalid_version_not_two_letters() {
        assert!(SsTableDescriptor::parse_filename("nba-1-big-Data.db").is_err());
        assert!(SsTableDescriptor::parse_filename("n-1-big-Data.db").is_err());
    }

    #[test]
    fn test_descriptor_invalid_no_format_segment() {
        assert!(SsTableDescriptor::parse_filename("nb-1-xxx-Data.db").is_err());
    }

    #[test]
    fn test_descriptor_from_path() {
        let path = PathBuf::from(
            "test-data/datasets/sstables/test_basic/simple_table-6aa08200/nb-1-big-Data.db",
        );
        let desc = SsTableDescriptor::parse(&path).unwrap();
        assert_eq!(desc.version, "nb");
        assert_eq!(desc.format, SsTableFormat::Big);
    }

    // -----------------------------------------------------------------------
    // Docker-generated fixture filenames
    // These filenames come from Cassandra 5.0.8 containers run with:
    //   storage_compatibility_mode: NONE  (for oa)
    //   sstable.selected_format: bti       (for da)
    // -----------------------------------------------------------------------

    /// `oa-2-big-Data.db` generated by Cassandra 5.0.8 with
    /// `storage_compatibility_mode: NONE`.
    #[test]
    fn test_descriptor_docker_oa_sequential() {
        let desc = SsTableDescriptor::parse_filename("oa-2-big-Data.db").unwrap();
        assert_eq!(desc.version, "oa");
        assert_eq!(desc.sstable_id, "2");
        assert_eq!(desc.format, SsTableFormat::Big);
        assert_eq!(desc.component, "Data.db");
    }

    /// `da-2-bti-Data.db` generated by Cassandra 5.0.8 with BTI format enabled.
    #[test]
    fn test_descriptor_docker_da_bti() {
        let desc = SsTableDescriptor::parse_filename("da-2-bti-Data.db").unwrap();
        assert_eq!(desc.version, "da");
        assert_eq!(desc.sstable_id, "2");
        assert_eq!(desc.format, SsTableFormat::Bti);
        assert_eq!(desc.component, "Data.db");
    }

    /// `da-2-bti-Partitions.db` — BTI-specific index component.
    #[test]
    fn test_descriptor_docker_da_bti_partitions() {
        let desc = SsTableDescriptor::parse_filename("da-2-bti-Partitions.db").unwrap();
        assert_eq!(desc.version, "da");
        assert_eq!(desc.format, SsTableFormat::Bti);
        assert_eq!(desc.component, "Partitions.db");
    }
}
