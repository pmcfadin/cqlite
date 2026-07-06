//! SSTable component-name derivation for the reader open path.
//!
//! Since issue #1597 (Epic G / G1) the reader no longer performs speculative
//! compression detection here. The open path parses `CompressionInfo.db` exactly
//! once via `compression_info::CompressionInfo::parse` and derives the compression
//! algorithm from that single result — see `reader::SSTableReader::open` and
//! `load_compression_info_metadata`. The former `detect_and_initialize_compression`
//! machinery (a second `parse_binary`, a ~25-generation `exists()` probe loop, and
//! filename/entropy heuristics) is deleted.
//!
//! What remains is [`extract_sstable_base_name`], the deterministic
//! `SsTableDescriptor`-derived base-name helper still used to locate the `CRC.db`
//! sidecar for uncompressed BIG tables (`load_crc_reader`).

use std::path::Path;

/// Extract SSTable base name from path (e.g., "nb-1-big-Data.db" -> "nb-1-big")
pub fn extract_sstable_base_name(path: &Path) -> Option<String> {
    let filename = path.file_name()?.to_str()?;

    // Require a .db component (preserves the legacy contract: no-extension and
    // wrong-extension names yield None).
    let filename_no_ext = filename.strip_suffix(".db")?;

    // Prefer the descriptor parser, which finds the big/bti format segment even
    // when the SSTable id is a hyphenated UUID (e.g.
    // "da-00000000-0000-0000-0000-000000000001-bti-Data.db") that a fixed
    // parts[0..3] split would mangle into the wrong base name, so the
    // "*-CompressionInfo.db" sidecar lookup failed and compressed data was read
    // as uncompressed (roborev #970).
    if let Ok(d) = crate::storage::sstable::version_gate::SsTableDescriptor::parse(path) {
        return Some(format!(
            "{}-{}-{}",
            d.version,
            d.sstable_id,
            d.format.as_str()
        ));
    }

    // Fallback for non-standard names the descriptor rejects: keep the legacy
    // {prefix}-{generation}-{format} heuristic so existing callers/tests are
    // unchanged (e.g. "nb-1-big.db" with only 3 parts still yields None).
    let parts: Vec<&str> = filename_no_ext.split('-').collect();
    if parts.len() >= 4 {
        Some(parts[0..3].join("-"))
    } else {
        log::warn!("Non-standard SSTable filename pattern: {}", filename);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_extract_sstable_base_name_standard_data_file() {
        let path = PathBuf::from("nb-1-big-Data.db");
        assert_eq!(
            extract_sstable_base_name(&path),
            Some("nb-1-big".to_string())
        );
    }

    #[test]
    fn test_extract_sstable_base_name_index_file() {
        let path = PathBuf::from("nb-2-big-Index.db");
        assert_eq!(
            extract_sstable_base_name(&path),
            Some("nb-2-big".to_string())
        );
    }

    #[test]
    fn test_extract_sstable_base_name_compression_info() {
        let path = PathBuf::from("nb-45-big-CompressionInfo.db");
        assert_eq!(
            extract_sstable_base_name(&path),
            Some("nb-45-big".to_string())
        );
    }

    #[test]
    fn test_extract_sstable_base_name_with_full_path() {
        let path = PathBuf::from("/var/lib/cassandra/data/keyspace/table-uuid/nb-1-big-Data.db");
        assert_eq!(
            extract_sstable_base_name(&path),
            Some("nb-1-big".to_string())
        );
    }

    #[test]
    fn test_extract_sstable_base_name_statistics() {
        let path = PathBuf::from("nb-100-big-Statistics.db");
        assert_eq!(
            extract_sstable_base_name(&path),
            Some("nb-100-big".to_string())
        );
    }

    #[test]
    fn test_extract_sstable_base_name_too_few_parts() {
        // Non-standard naming - should return None
        let path = PathBuf::from("invalid.db");
        assert_eq!(extract_sstable_base_name(&path), None);
    }

    #[test]
    fn test_extract_sstable_base_name_no_extension() {
        let path = PathBuf::from("nb-1-big-Data");
        assert_eq!(extract_sstable_base_name(&path), None);
    }

    #[test]
    fn test_extract_sstable_base_name_wrong_extension() {
        let path = PathBuf::from("nb-1-big-Data.txt");
        assert_eq!(extract_sstable_base_name(&path), None);
    }

    #[test]
    fn test_extract_sstable_base_name_three_parts() {
        // Only 3 parts after split - should return None
        let path = PathBuf::from("nb-1-big.db");
        assert_eq!(extract_sstable_base_name(&path), None);
    }
}
