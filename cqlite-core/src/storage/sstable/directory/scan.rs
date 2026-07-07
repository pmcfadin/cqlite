use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use crate::error::{Error, Result};
use crate::storage::sstable::version_gate::SsTableDescriptor;

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
                tracing::warn!("Cannot access file {:?}: {}", file_path, e);
                continue;
            }

            if let Some((version, generation, format, component)) =
                parse_sstable_filename(file_name)?
            {
                valid_sstable_files += 1;
                let key = (generation, format.clone());

                let generation_obj =
                    generations_map
                        .entry(key.clone())
                        .or_insert_with(|| SSTableGeneration {
                            version: version.clone(),
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
    tracing::debug!(
        "Directory scan completed: {} total files, {} SSTable files, {} generations found",
        found_files,
        valid_sstable_files,
        generations.len()
    );

    Ok(generations)
}

/// Parse SSTable filename to extract version, generation, format, and component.
///
/// Returns `Ok(Some((version, generation, format, component)))` for recognised
/// SSTable filenames.  Returns `Ok(None)` for non-SSTable files (e.g. `.jmx`,
/// unknown extensions) or files whose SSTable id is not a plain integer (UUID-
/// based ids are not yet supported by the `u32` generation field; see the
/// `SSTableGeneration.sstable_id` tracking item).
///
/// # Examples
/// ```text
/// "nb-1-big-Data.db"     -> Some(("nb", 1, "big",  Data))
/// "da-2-bti-Rows.db"     -> Some(("da", 2, "bti",  Rows))
/// "oa-1-big-Data.db"     -> Some(("oa", 1, "big",  Data))
/// ".gitignore"           -> None
/// ```
///
/// The version letter is extracted via [`SsTableDescriptor`] which also validates
/// the filename structure and supports both sequential and UUID-based SSTable ids.
/// UUID-based ids are silently skipped (returning `None`) because the current
/// `SSTableGeneration.generation` field is `u32`.
pub(crate) fn parse_sstable_filename(
    filename: &str,
) -> Result<Option<(String, u32, String, SSTableComponent)>> {
    use std::path::Path;

    // Use SsTableDescriptor for authoritative filename parsing (handles both
    // sequential and UUID-based ids, validates version letter format).
    let desc = match SsTableDescriptor::parse(Path::new(filename)) {
        Ok(d) => d,
        Err(_) => {
            // Not a recognised SSTable filename (e.g. .gitignore, jmx files).
            return Ok(None);
        }
    };

    // The generation field is u32; UUID-based ids (e.g. "6aa08200a251…") cannot
    // be coerced to u32.  Skip them silently — these files will still be picked
    // up once SSTableGeneration is upgraded to carry a String sstable_id.
    let generation: u32 = match desc.sstable_id.parse() {
        Ok(n) => n,
        Err(_) => {
            tracing::debug!(
                "parse_sstable_filename: skipping UUID-id file {} (not a plain integer generation)",
                filename
            );
            return Ok(None);
        }
    };

    let format = desc.format.as_str().to_string();
    let version = desc.version.clone();

    // Return None for unrecognized components instead of propagating error.
    // This follows the same pattern as TOC parsing (toc.rs:53-73).
    let component = match SSTableComponent::from_str(&desc.component) {
        Ok(c) => c,
        Err(_) => {
            tracing::debug!(
                "Ignoring file with unrecognized component extension: {}",
                filename
            );
            return Ok(None);
        }
    };

    Ok(Some((version, generation, format, component)))
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
