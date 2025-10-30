//! Compression detection and initialization for SSTable readers.
//!
//! This module handles:
//! - Compression algorithm detection from headers and files
//! - CompressionInfo.db discovery and parsing
//! - Heuristic fallback detection (legacy formats only)

use log::{debug, warn};
use std::path::Path;

use crate::{parser::SSTableHeader, Result};

use super::super::compression::{CompressionAlgorithm, CompressionInfo, CompressionReader};

/// Detect and initialize compression reader using multi-strategy approach
pub(crate) async fn detect_and_initialize_compression(
    header: &SSTableHeader,
    path: &Path,
) -> Result<Option<CompressionReader>> {
    // Strategy 1: Check header compression info
    if header.compression.algorithm != "NONE" {
        let algorithm = CompressionAlgorithm::from(header.compression.algorithm.as_str());
        debug!("Header indicates compression: {:?}", algorithm);

        // Validate compression algorithm is supported
        match algorithm {
            CompressionAlgorithm::Lz4
            | CompressionAlgorithm::Snappy
            | CompressionAlgorithm::Deflate
            | CompressionAlgorithm::Zstd => {
                return Ok(Some(CompressionReader::new(algorithm)));
            }
            CompressionAlgorithm::None => {
                // Continue to other detection methods
            }
        }
    }

    // Strategy 2: Check for CompressionInfo.db file in the same directory
    let parent_dir = path.parent().unwrap_or(Path::new("."));

    // Try to find compression info files using comprehensive discovery
    if let Some(compression_reader) = discover_compression_info(path, parent_dir).await? {
        return Ok(Some(compression_reader));
    }

    // Strategy 3: Heuristic detection (only for legacy formats)
    #[cfg(feature = "legacy-heuristics")]
    {
        if let Some(algorithm) = detect_compression_heuristic(header, path).await? {
            debug!("Heuristic detection found compression: {:?}", algorithm);
            return Ok(Some(CompressionReader::new(algorithm)));
        }
    }

    // Strategy 4: Check filename patterns for compression hints (legacy only)
    #[cfg(feature = "legacy-heuristics")]
    {
        if let Some(algorithm) = detect_compression_from_filename(path) {
            debug!("Filename pattern suggests compression: {:?}", algorithm);
            return Ok(Some(CompressionReader::new(algorithm)));
        }
    }

    debug!("No compression detected for {:?}", path);
    Ok(None)
}

/// Heuristic compression detection based on file format and data analysis (legacy only)
#[cfg(feature = "legacy-heuristics")]
async fn detect_compression_heuristic(
    header: &SSTableHeader,
    _path: &Path,
) -> Result<Option<CompressionAlgorithm>> {
    // IMPORTANT: This function should ONLY be used for legacy formats where
    // compression metadata is not available. Modern formats (V5_0NewBig, V5_0Bti)
    // must use metadata-driven compression detection.

    match header.cassandra_version {
        crate::parser::header::CassandraVersion::V5_0NewBig
        | crate::parser::header::CassandraVersion::V5_0Bti
        | crate::parser::header::CassandraVersion::V5_0Alpha
        | crate::parser::header::CassandraVersion::V5_0Beta
        | crate::parser::header::CassandraVersion::V5_0Release
        | crate::parser::header::CassandraVersion::V5_0DataFormat
        | crate::parser::header::CassandraVersion::V5_0FormatC
        | crate::parser::header::CassandraVersion::V5_0FormatD
        | crate::parser::header::CassandraVersion::V5_0FormatE
        | crate::parser::header::CassandraVersion::V5_0FormatF
        | crate::parser::header::CassandraVersion::V5_0FormatG
        | crate::parser::header::CassandraVersion::V5_0StaticColumns
        | crate::parser::header::CassandraVersion::V5_0Uncompressed => {
            // Modern formats should never use heuristics - this is an error
            log::error!(
                "Heuristic compression detection called for modern format: {:?}",
                header.cassandra_version
            );
            Ok(None)
        }
        crate::parser::header::CassandraVersion::Legacy => {
            // For legacy formats, try to detect based on file patterns and entropy
            // This is inherently unreliable and should be avoided when possible
            log::warn!("Using unreliable heuristic compression detection for legacy format");

            // Basic heuristics for legacy formats only
            // This is a fallback when metadata is completely unavailable
            if header.compression.algorithm != "NONE" {
                // Try to parse the algorithm string if present
                match header.compression.algorithm.to_uppercase().as_str() {
                    "LZ4" => Ok(Some(CompressionAlgorithm::Lz4)),
                    "SNAPPY" => Ok(Some(CompressionAlgorithm::Snappy)),
                    "ZSTD" => Ok(Some(CompressionAlgorithm::Zstd)),
                    "DEFLATE" => Ok(Some(CompressionAlgorithm::Deflate)),
                    _ => {
                        log::warn!(
                            "Unknown compression algorithm in header: {}",
                            header.compression.algorithm
                        );
                        Ok(None)
                    }
                }
            } else {
                Ok(None)
            }
        }
    }
}

/// Detect compression algorithm from filename patterns (legacy only)
#[cfg(feature = "legacy-heuristics")]
fn detect_compression_from_filename(path: &Path) -> Option<CompressionAlgorithm> {
    let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

    // Check for compression hints in filename
    if filename.contains("lz4") || filename.contains("LZ4") {
        Some(CompressionAlgorithm::Lz4)
    } else if filename.contains("snappy") || filename.contains("SNAPPY") {
        Some(CompressionAlgorithm::Snappy)
    } else if filename.contains("deflate") || filename.contains("DEFLATE") {
        Some(CompressionAlgorithm::Deflate)
    } else if filename.contains("zstd") || filename.contains("ZSTD") {
        Some(CompressionAlgorithm::Zstd)
    } else {
        None
    }
}

/// Discover compression info files using comprehensive pattern matching and directory scanning
async fn discover_compression_info(
    sstable_path: &Path,
    parent_dir: &Path,
) -> Result<Option<CompressionReader>> {
    // Stage 1: Try standard patterns first (most common cases)
    let standard_patterns = get_standard_compression_patterns(sstable_path);

    for pattern in &standard_patterns {
        let compression_info_path = parent_dir.join(pattern);
        if compression_info_path.exists() {
            match load_compression_info(&compression_info_path).await {
                Ok(compression_info) => {
                    let algorithm = compression_info.get_algorithm();
                    debug!(
                        "Found CompressionInfo at {:?} with algorithm: {:?}, chunks: {}",
                        compression_info_path,
                        algorithm,
                        compression_info.chunk_count()
                    );

                    if algorithm != CompressionAlgorithm::None {
                        return Ok(Some(CompressionReader::new(algorithm)));
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to load CompressionInfo from {:?}: {}",
                        compression_info_path, e
                    );
                    continue;
                }
            }
        }
    }

    // Stage 2: Directory scanning for any *CompressionInfo.db files
    match scan_directory_for_compression_files(parent_dir, sstable_path).await {
        Ok(Some(compression_reader)) => {
            return Ok(Some(compression_reader));
        }
        Ok(None) => {
            // Continue to fallback strategies
        }
        Err(e) => {
            warn!("Directory scan failed: {}", e);
            // Continue to fallback strategies
        }
    }

    Ok(None)
}

/// Get standard compression filename patterns based on SSTable path
fn get_standard_compression_patterns(sstable_path: &Path) -> Vec<String> {
    let mut patterns = Vec::new();

    // Extract base name using improved logic
    if let Some(base_name) = extract_sstable_base_name(sstable_path) {
        patterns.push(format!("{}-CompressionInfo.db", base_name));
    }

    // Common generation patterns found in real data
    let generations = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 35, 40, 45, 46, 47, 50, 55,
    ];
    for generation in &generations {
        patterns.push(format!("nb-{}-big-CompressionInfo.db", generation));
    }

    // Standard fallback patterns
    patterns.push("CompressionInfo.db".to_string());

    // File stem based pattern as fallback
    if let Some(stem) = sstable_path.file_stem().and_then(|s| s.to_str()) {
        patterns.push(format!("{}-CompressionInfo.db", stem));
    }

    patterns
}

/// Scan directory for any compression files and try to match them to the SSTable
async fn scan_directory_for_compression_files(
    dir: &Path,
    sstable_path: &Path,
) -> Result<Option<CompressionReader>> {
    use std::fs;

    // Read directory entries
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            warn!("Cannot read directory {:?}: {}", dir, e);
            return Ok(None);
        }
    };

    let mut compression_files = Vec::new();

    // Find all *CompressionInfo.db files
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.ends_with("CompressionInfo.db") {
                    compression_files.push(path);
                }
            }
        }
    }

    // Sort by preference (exact matches first, then generation-based)
    compression_files.sort_by(|a, b| {
        let score_a = score_compression_file_match(a, sstable_path);
        let score_b = score_compression_file_match(b, sstable_path);
        score_b.cmp(&score_a) // Higher score first
    });

    // Try each compression file in order of preference
    for compression_path in compression_files {
        match load_compression_info(&compression_path).await {
            Ok(compression_info) => {
                let algorithm = compression_info.get_algorithm();
                log::debug!(
                    "Found CompressionInfo via directory scan at {:?} with algorithm: {:?}, chunks: {}",
                    compression_path,
                    algorithm,
                    compression_info.chunk_count()
                );

                if algorithm != CompressionAlgorithm::None {
                    return Ok(Some(CompressionReader::new(algorithm)));
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to load CompressionInfo from {:?}: {}",
                    compression_path,
                    e
                );
                continue;
            }
        }
    }

    Ok(None)
}

/// Score how well a compression file matches the SSTable (higher is better)
fn score_compression_file_match(compression_path: &Path, sstable_path: &Path) -> i32 {
    let Some(comp_name) = compression_path.file_name().and_then(|n| n.to_str()) else {
        return 0;
    };
    let Some(sstable_name) = sstable_path.file_name().and_then(|n| n.to_str()) else {
        return 0;
    };

    let mut score = 0;

    // Exact base name match gets highest score
    if let Some(base_name) = extract_sstable_base_name(sstable_path) {
        if comp_name.starts_with(&base_name) {
            score += 100;
        }
    }

    // Generation number matching
    if let Some(sstable_gen) = extract_generation_number(sstable_name) {
        if let Some(comp_gen) = extract_generation_number(comp_name) {
            if sstable_gen == comp_gen {
                score += 50;
            }
        }
    }

    // Format matching (nb-*-big pattern)
    if sstable_name.contains("nb-")
        && sstable_name.contains("-big-")
        && comp_name.contains("nb-")
        && comp_name.contains("-big-")
    {
        score += 25;
    }

    // Generic CompressionInfo.db gets lowest score
    if comp_name == "CompressionInfo.db" {
        score += 1;
    }

    score
}

/// Extract generation number from filename (e.g., "nb-45-big" -> Some(45))
fn extract_generation_number(filename: &str) -> Option<u32> {
    if let Some(start) = filename.find("nb-") {
        let after_nb = &filename[start + 3..];
        if let Some(end) = after_nb.find('-') {
            let gen_str = &after_nb[..end];
            gen_str.parse().ok()
        } else {
            None
        }
    } else {
        None
    }
}

/// Load compression info from file
async fn load_compression_info(path: &Path) -> Result<CompressionInfo> {
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    let mut file = File::open(path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    CompressionInfo::parse_binary(&buffer)
}

/// Extract SSTable base name from path (e.g., "nb-1-big-Data.db" -> "nb-1-big")
pub fn extract_sstable_base_name(path: &Path) -> Option<String> {
    let filename = path.file_name()?.to_str()?;

    // Remove .db extension first
    let filename_no_ext = filename.strip_suffix(".db")?;

    // Parse SSTable filename pattern: {prefix}-{generation}-{format}-{component}
    let parts: Vec<&str> = filename_no_ext.split('-').collect();

    if parts.len() >= 4 {
        // Join prefix, generation, and format: "nb-1-big"
        Some(parts[0..3].join("-"))
    } else {
        // Fallback for non-standard naming
        log::warn!("Non-standard SSTable filename pattern: {}", filename);
        None
    }
}
