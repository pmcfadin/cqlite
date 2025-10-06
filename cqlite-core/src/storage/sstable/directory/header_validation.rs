use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

use super::types::SSTableComponent;

/// Validate that all component files have consistent headers
pub(crate) fn validate_component_headers(
    components: &HashMap<SSTableComponent, PathBuf>,
) -> Result<Vec<String>> {
    let mut inconsistencies = Vec::new();
    let mut generation_info = None;
    let mut table_info = None;

    for (component, path) in components {
        match extract_header_info(path) {
            Ok(header) => {
                // Check generation consistency
                if let Some(ref expected_gen) = generation_info {
                    if header.generation != *expected_gen {
                        inconsistencies.push(format!(
                            "Generation mismatch in {:?}: expected {}, found {}",
                            component, expected_gen, header.generation
                        ));
                    }
                } else {
                    generation_info = Some(header.generation);
                }

                // Check table ID consistency
                if let Some(ref expected_table) = table_info {
                    if header.table_id != *expected_table {
                        inconsistencies.push(format!(
                            "Table ID mismatch in {:?}: expected {}, found {}",
                            component, expected_table, header.table_id
                        ));
                    }
                } else {
                    table_info = Some(header.table_id.clone());
                }
            }
            Err(e) => {
                inconsistencies.push(format!("Failed to read header from {:?}: {}", component, e));
            }
        }
    }

    Ok(inconsistencies)
}

/// Extract header information from SSTable component file
fn extract_header_info(path: &Path) -> Result<HeaderInfo> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut header_bytes = vec![0u8; 32]; // Read first 32 bytes for header analysis

    reader
        .read_exact(&mut header_bytes)
        .map_err(|e| Error::corruption(format!("Failed to read header from {:?}: {}", path, e)))?;

    // Parse generation from filename as fallback
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::invalid_path(format!("Invalid filename: {:?}", path)))?;

    let generation = if let Some(dash_pos) = filename.find('-') {
        let second_part = &filename[dash_pos + 1..];
        if let Some(second_dash) = second_part.find('-') {
            second_part[..second_dash].parse::<u32>().map_err(|_| {
                Error::invalid_format(format!("Invalid generation in filename: {}", filename))
            })?
        } else {
            return Err(Error::invalid_format(format!(
                "Invalid SSTable filename format: {}",
                filename
            )));
        }
    } else {
        return Err(Error::invalid_format(format!(
            "Invalid SSTable filename format: {}",
            filename
        )));
    };

    // For now, use a placeholder table ID (in real implementation, this would be extracted from the binary header)
    let table_id = format!("table_{}", generation);

    Ok(HeaderInfo {
        generation,
        table_id,
        format_version: header_bytes[0], // Simplified header parsing
    })
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct HeaderInfo {
    generation: u32,
    table_id: String,
    format_version: u8,
}
