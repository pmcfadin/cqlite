use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use crate::error::{Error, Result};

use super::types::{SSTableComponent, SSTableGeneration, SecondaryIndex};

/// Extract table name from directory name (strips UUID suffix)
pub(crate) fn extract_table_name(dir_name: &str) -> Result<String> {
    // Directory format: "tablename-{32-char-uuid}"
    // Find the last hyphen and take everything before it
    if let Some(hyphen_pos) = dir_name.rfind('-') {
        let table_name = &dir_name[..hyphen_pos];
        if table_name.is_empty() {
            return Err(Error::invalid_path(format!(
                "Empty table name in directory: {}",
                dir_name
            )));
        }
        Ok(table_name.to_string())
    } else {
        // Fallback: use entire directory name if no UUID suffix
        Ok(dir_name.to_string())
    }
}

/// Scan directory for SSTable files and group by generation
pub(crate) fn scan_sstable_files(path: &Path, table_name: &str) -> Result<Vec<SSTableGeneration>> {
    let entries = fs::read_dir(path)
        .map_err(|e| Error::storage(format!("Failed to read directory: {:?}: {}", path, e)))?;

    let mut generations_map: HashMap<(u32, String), SSTableGeneration> = HashMap::new();
    let mut found_files = 0;
    let mut valid_sstable_files = 0;

    for entry in entries {
        let entry = entry?;
        let file_path = entry.path();
        found_files += 1;

        if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
            // Enhanced validation: Check if file exists and is readable
            if !file_path.is_file() {
                continue; // Skip directories and non-files
            }

            // Check file accessibility
            if let Err(e) = fs::metadata(&file_path) {
                eprintln!("Warning: Cannot access file {:?}: {}", file_path, e);
                continue;
            }

            if let Some((generation, format, component)) = parse_sstable_filename(file_name)? {
                valid_sstable_files += 1;
                let key = (generation, format.clone());

                let generation_obj =
                    generations_map
                        .entry(key.clone())
                        .or_insert_with(|| SSTableGeneration {
                            generation,
                            format,
                            table_name: table_name.to_string(),
                            components: HashMap::new(),
                            base_path: path.to_path_buf(),
                        });

                generation_obj.components.insert(component, file_path);
            }
        }
    }

    // Enhanced validation and reporting
    if found_files == 0 {
        return Err(Error::not_found(format!(
            "Directory appears to be empty: {:?}",
            path
        )));
    }

    if valid_sstable_files == 0 {
        return Err(Error::invalid_format(format!(
            "No valid SSTable files found in directory: {:?}. Found {} files total, but none match the expected SSTable naming pattern (e.g., nb-1-big-Data.db)",
            path,
            found_files
        )));
    }

    // Sort generations by number (newest first)
    let mut generations: Vec<SSTableGeneration> = generations_map.into_values().collect();
    generations.sort_by(|a, b| b.generation.cmp(&a.generation));

    // Log summary for debugging
    eprintln!(
        "Directory scan completed: {} total files, {} SSTable files, {} generations found",
        found_files,
        valid_sstable_files,
        generations.len()
    );

    Ok(generations)
}

/// Parse SSTable filename to extract generation, format, and component
/// Examples: "nb-1-big-Data.db" -> (1, "big", Data)
///           "nb-2-da-Partitions.db" -> (2, "da", Partitions)
pub(crate) fn parse_sstable_filename(
    filename: &str,
) -> Result<Option<(u32, String, SSTableComponent)>> {
    // Pattern: {prefix}-{generation}-{format}-{component}
    let parts: Vec<&str> = filename.split('-').collect();

    if parts.len() < 4 {
        return Ok(None); // Not an SSTable file
    }

    // Extract generation number (second part)
    let generation: u32 = parts[1].parse().map_err(|_| {
        Error::invalid_format(format!(
            "Invalid generation number in filename: {}",
            filename
        ))
    })?;

    // Extract format (third part)
    let format = parts[2].to_string();

    // Extract component (everything after third hyphen)
    let component_str = parts[3..].join("-");
    let component = SSTableComponent::from_str(&component_str)?;

    Ok(Some((generation, format, component)))
}

/// Scan directory for secondary index subdirectories
pub(crate) fn scan_secondary_indexes(path: &Path, table_name: &str) -> Result<Vec<SecondaryIndex>> {
    let entries = fs::read_dir(path)
        .map_err(|e| Error::storage(format!("Failed to read directory: {:?}: {}", path, e)))?;

    let mut secondary_indexes = Vec::new();

    for entry in entries {
        let entry = entry?;
        let entry_path = entry.path();

        if entry_path.is_dir() {
            if let Some(dir_name) = entry_path.file_name().and_then(|n| n.to_str()) {
                // Check if this is a secondary index directory (starts with '.' and ends with '_idx')
                if dir_name.starts_with('.') && dir_name.ends_with("_idx") {
                    // Extract index name (e.g., ".users_metadata_idx" -> "metadata_idx")
                    let index_name = dir_name[1..].to_string(); // Remove leading '.'

                    // Validate that the index name matches the table
                    let expected_prefix = format!("{}_", table_name);
                    if index_name.starts_with(&expected_prefix) {
                        // Scan SSTable files in the secondary index directory
                        let index_generations = scan_sstable_files(&entry_path, table_name)?;

                        if !index_generations.is_empty() {
                            secondary_indexes.push(SecondaryIndex {
                                index_name,
                                index_path: entry_path,
                                generations: index_generations,
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(secondary_indexes)
}
