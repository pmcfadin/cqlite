use std::fs;
use std::path::Path;
use std::str::FromStr;

use crate::error::{Error, Result};

use super::types::SSTableComponent;

/// Parse TOC.txt file to get list of components with enhanced validation
/// Returns (valid_components, unknown_components)
pub fn parse_toc_file_detailed<P: AsRef<Path>>(
    path: P,
) -> Result<(Vec<SSTableComponent>, Vec<String>)> {
    let path_ref = path.as_ref();

    // Enhanced file validation
    if !path_ref.exists() {
        return Err(Error::not_found(format!(
            "TOC.txt file does not exist: {:?}",
            path_ref
        )));
    }

    if !path_ref.is_file() {
        return Err(Error::invalid_path(format!(
            "TOC.txt path is not a file: {:?}",
            path_ref
        )));
    }

    let content = fs::read_to_string(path_ref)
        .map_err(|e| Error::storage(format!("Failed to read TOC file: {:?}: {}", path_ref, e)))?;

    if content.trim().is_empty() {
        return Err(Error::corruption(format!(
            "TOC.txt file is empty: {:?}",
            path_ref
        )));
    }

    let mut components = Vec::new();
    let mut unknown_components = Vec::new();
    let mut line_number = 0;

    for line in content.lines() {
        line_number += 1;
        let line = line.trim();

        if line.is_empty() || line.starts_with('#') {
            continue; // Skip empty lines and comments
        }

        match SSTableComponent::from_str(line) {
            Ok(component) => {
                if !components.contains(&component) {
                    components.push(component);
                } else {
                    log::warn!(
                        "Duplicate component in TOC.txt line {}: {}",
                        line_number,
                        line
                    );
                }
            }
            Err(_) => {
                unknown_components.push(line.to_string());
                log::warn!(
                    "Unknown component in TOC.txt line {}: {}",
                    line_number,
                    line
                );
            }
        }
    }

    Ok((components, unknown_components))
}

/// Parse TOC.txt file to get list of components with enhanced validation (backward compatibility)
pub fn parse_toc_file<P: AsRef<Path>>(path: P) -> Result<Vec<SSTableComponent>> {
    let (components, _) = parse_toc_file_detailed(path)?;
    Ok(components)
}
