//! TOC.txt writer - writes table of contents
//!
//! Generates the TOC.txt component listing all SSTable files.
//! Acts as a publication barrier - SSTable is not visible until TOC.txt exists.
//!
//! Critical requirements:
//! - MUST be written LAST (after all other components)
//! - Lists all component files
//! - Plain text format, one component per line
//! - Self-referential: TOC.txt must list itself

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::storage::sstable::directory::types::SSTableComponent;

/// Entry representing a component to be written to TOC.txt
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentEntry {
    /// Component type (e.g., Data, Index, etc.)
    pub component: SSTableComponent,
}

impl ComponentEntry {
    /// Create a new component entry
    pub fn new(component: SSTableComponent) -> Self {
        Self { component }
    }

    /// Get the filename for this component
    pub fn filename(&self) -> &'static str {
        self.component.file_extension()
    }
}

/// TOC.txt component writer
///
/// Writes the table of contents listing all SSTable component files.
/// This file acts as the publication barrier - an SSTable is not considered
/// complete and readable until TOC.txt exists.
///
/// # Format
///
/// Plain text file with one component filename per line:
/// ```text
/// Data.db
/// Index.db
/// Filter.db
/// Statistics.db
/// Summary.db
/// CompressionInfo.db
/// Digest.crc32
/// TOC.txt
/// ```
///
/// # Critical Requirements
///
/// 1. TOC.txt MUST be written LAST after all other components
/// 2. TOC.txt MUST include itself in the listing
/// 3. Component order in the file doesn't matter (implementation orders for consistency)
///
/// # Example
///
/// ```rust,ignore
/// let mut components = vec![
///     ComponentEntry::new(SSTableComponent::Data),
///     ComponentEntry::new(SSTableComponent::Index),
///     ComponentEntry::new(SSTableComponent::Statistics),
///     ComponentEntry::new(SSTableComponent::Filter),
///     ComponentEntry::new(SSTableComponent::Summary),
///     ComponentEntry::new(SSTableComponent::CompressionInfo),
///     ComponentEntry::new(SSTableComponent::Digest),
/// ];
///
/// let writer = TocWriter::new(toc_path);
/// writer.write(&components)?;
/// ```
#[derive(Debug)]
pub struct TocWriter {
    /// Path to the TOC.txt file to write
    path: PathBuf,
}

impl TocWriter {
    /// Create a new TOC.txt writer
    ///
    /// # Arguments
    ///
    /// * `path` - Full path to the TOC.txt file (e.g., "nb-1-big-TOC.txt")
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Write TOC.txt file with the given component list
    ///
    /// Automatically adds TOC.txt itself to the listing if not already present.
    /// Components are written in a consistent order for deterministic output.
    ///
    /// # Arguments
    ///
    /// * `components` - List of components to include in TOC.txt
    ///
    /// # Returns
    ///
    /// Ok(()) on success, Error on I/O failure
    ///
    /// # Errors
    ///
    /// Returns Error::Storage if:
    /// - Unable to create the TOC.txt file
    /// - Unable to write component names
    /// - Unable to flush/sync the file
    pub fn write(&self, components: &[ComponentEntry]) -> Result<()> {
        // Ensure TOC itself is in the list (add if missing)
        let mut final_components: Vec<ComponentEntry> = components.to_vec();

        if !final_components.iter().any(|e| e.component == SSTableComponent::TOC) {
            final_components.push(ComponentEntry::new(SSTableComponent::TOC));
        }

        // Sort components for deterministic output (matches Cassandra behavior)
        // Order: Data, Statistics, Digest, TOC, CompressionInfo, Filter, Index, Summary
        final_components.sort_by_key(|entry| {
            match entry.component {
                SSTableComponent::Data => 0,
                SSTableComponent::Statistics => 1,
                SSTableComponent::Digest => 2,
                SSTableComponent::TOC => 3,
                SSTableComponent::CompressionInfo => 4,
                SSTableComponent::Filter => 5,
                SSTableComponent::Index => 6,
                SSTableComponent::Summary => 7,
                SSTableComponent::Partitions => 8,
                SSTableComponent::Rows => 9,
            }
        });

        // Create file with buffered writing
        let file = File::create(&self.path).map_err(|e| {
            Error::storage(format!(
                "Failed to create TOC.txt file {:?}: {}",
                self.path, e
            ))
        })?;

        let mut writer = BufWriter::new(file);

        // Write each component filename on its own line
        for entry in &final_components {
            writeln!(writer, "{}", entry.filename()).map_err(|e| {
                Error::storage(format!(
                    "Failed to write component {} to TOC.txt: {}",
                    entry.filename(),
                    e
                ))
            })?;
        }

        // Flush buffer and sync to disk (durability requirement)
        writer.flush().map_err(|e| {
            Error::storage(format!("Failed to flush TOC.txt: {}", e))
        })?;

        let file = writer.into_inner().map_err(|e| {
            Error::storage(format!("Failed to extract file from buffer: {}", e))
        })?;

        file.sync_all().map_err(|e| {
            Error::storage(format!("Failed to sync TOC.txt to disk: {}", e))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_toc_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let toc_path = temp_dir.path().join("nb-1-big-TOC.txt");

        let components = vec![
            ComponentEntry::new(SSTableComponent::Data),
            ComponentEntry::new(SSTableComponent::Index),
            ComponentEntry::new(SSTableComponent::Statistics),
        ];

        let writer = TocWriter::new(toc_path.clone());
        writer.write(&components).unwrap();

        // Verify file exists
        assert!(toc_path.exists());

        // Read and verify contents
        let contents = fs::read_to_string(&toc_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();

        // Should include the 3 components plus TOC.txt itself
        assert_eq!(lines.len(), 4);

        // Verify TOC.txt is included
        assert!(lines.contains(&"TOC.txt"));
        assert!(lines.contains(&"Data.db"));
        assert!(lines.contains(&"Index.db"));
        assert!(lines.contains(&"Statistics.db"));
    }

    #[test]
    fn test_toc_writer_all_components() {
        let temp_dir = TempDir::new().unwrap();
        let toc_path = temp_dir.path().join("nb-1-big-TOC.txt");

        let components = vec![
            ComponentEntry::new(SSTableComponent::Data),
            ComponentEntry::new(SSTableComponent::Index),
            ComponentEntry::new(SSTableComponent::Statistics),
            ComponentEntry::new(SSTableComponent::Filter),
            ComponentEntry::new(SSTableComponent::Summary),
            ComponentEntry::new(SSTableComponent::CompressionInfo),
            ComponentEntry::new(SSTableComponent::Digest),
        ];

        let writer = TocWriter::new(toc_path.clone());
        writer.write(&components).unwrap();

        let contents = fs::read_to_string(&toc_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();

        assert_eq!(lines.len(), 8); // 7 components + TOC.txt

        // Verify order matches expected canonical order
        assert_eq!(lines[0], "Data.db");
        assert_eq!(lines[1], "Statistics.db");
        assert_eq!(lines[2], "Digest.crc32");
        assert_eq!(lines[3], "TOC.txt");
        assert_eq!(lines[4], "CompressionInfo.db");
        assert_eq!(lines[5], "Filter.db");
        assert_eq!(lines[6], "Index.db");
        assert_eq!(lines[7], "Summary.db");
    }

    #[test]
    fn test_toc_writer_already_includes_toc() {
        let temp_dir = TempDir::new().unwrap();
        let toc_path = temp_dir.path().join("nb-1-big-TOC.txt");

        // Include TOC explicitly
        let components = vec![
            ComponentEntry::new(SSTableComponent::Data),
            ComponentEntry::new(SSTableComponent::TOC),
            ComponentEntry::new(SSTableComponent::Statistics),
        ];

        let writer = TocWriter::new(toc_path.clone());
        writer.write(&components).unwrap();

        let contents = fs::read_to_string(&toc_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();

        // Should not duplicate TOC.txt
        assert_eq!(lines.len(), 3);
        assert_eq!(lines.iter().filter(|&&l| l == "TOC.txt").count(), 1);
    }

    #[test]
    fn test_toc_writer_empty_components() {
        let temp_dir = TempDir::new().unwrap();
        let toc_path = temp_dir.path().join("nb-1-big-TOC.txt");

        let components = vec![];

        let writer = TocWriter::new(toc_path.clone());
        writer.write(&components).unwrap();

        let contents = fs::read_to_string(&toc_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();

        // Should still write TOC.txt itself
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "TOC.txt");
    }

    #[test]
    fn test_component_entry_filename() {
        let data_entry = ComponentEntry::new(SSTableComponent::Data);
        assert_eq!(data_entry.filename(), "Data.db");

        let digest_entry = ComponentEntry::new(SSTableComponent::Digest);
        assert_eq!(digest_entry.filename(), "Digest.crc32");

        let toc_entry = ComponentEntry::new(SSTableComponent::TOC);
        assert_eq!(toc_entry.filename(), "TOC.txt");
    }
}
