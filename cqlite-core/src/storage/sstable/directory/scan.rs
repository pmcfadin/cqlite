use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use crate::error::{Error, Result};
use crate::storage::sstable::generation::{list_generations, GenerationId, Scope, TocPolicy};

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

/// Scan directory for SSTable files and group by generation.
///
/// Which files form generations is the generation module's answer (issue #4626, task 3.5), not a
/// second filename parser: Scope Live over this one directory, so a committed compaction's
/// obsolete inputs and an unfinished transaction's outputs are not read, and a generation without
/// `TOC.txt` is still a generation (this reader validates its own components). A generation whose
/// id is a UUID cannot be carried by `SSTableGeneration.generation` (`u32`); it is a NAMED error
/// here, never silently dropped (G13).
pub(crate) fn scan_sstable_files(path: &Path, table_name: &str) -> Result<Vec<SSTableGeneration>> {
    let found_files = fs::read_dir(path)
        .map_err(|e| Error::storage(format!("Failed to read directory: {:?}: {}", path, e)))?
        .count();
    if found_files == 0 {
        return Err(Error::not_found(format!(
            "Directory appears to be empty: {:?}",
            path
        )));
    }

    let listing = list_generations(path, &Scope::Live, TocPolicy::Accept)
        .map_err(|e| Error::storage(format!("Failed to list SSTables in {:?}: {}", path, e)))?;

    let mut generations: Vec<SSTableGeneration> = Vec::new();
    for listed in listing.live.iter().chain(listing.orphans.iter()) {
        let GenerationId::Sequence(generation) = listed.id() else {
            return Err(Error::unsupported_format(format!(
                "SSTable generation {} in {:?} has a UUID id, which SSTableDirectory cannot carry \
                 (its generation number is a u32)",
                listed.base_name(),
                path
            )));
        };
        let mut components = HashMap::new();
        for component in listed.components() {
            // A name that is not a known component (an auxiliary dump, a custom component) is not
            // part of the reader's component map.
            if let Ok(kind) = SSTableComponent::from_str(&component.name) {
                components.insert(kind, component.path.clone());
            }
        }
        generations.push(SSTableGeneration {
            version: listed.version().to_string(),
            generation: *generation,
            format: listed.format().as_str().to_string(),
            table_name: table_name.to_string(),
            components,
            base_path: path.to_path_buf(),
        });
    }

    if generations.is_empty() {
        return Err(Error::invalid_format(format!(
            "No valid SSTable files found in directory: {:?}. Found {} files total, but none match the expected SSTable naming pattern (e.g., nb-1-big-Data.db)",
            path,
            found_files
        )));
    }

    // Sort generations by number (newest first)
    generations.sort_by_key(|b| std::cmp::Reverse(b.generation));

    tracing::debug!(
        "Directory scan completed: {} total files, {} generations found",
        found_files,
        generations.len()
    );

    Ok(generations)
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
