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
/// CompressionInfo.db
/// Data.db
/// Digest.crc32
/// Filter.db
/// Index.db
/// Statistics.db
/// Summary.db
/// TOC.txt
/// ```
///
/// # Critical Requirements
///
/// 1. TOC.txt MUST be written LAST after all other components
/// 2. TOC.txt MUST include itself in the listing
/// 3. Component order is Cassandra 5.0.8's (sorted file names, see `write`); readers
///    treat the file as a set
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
    /// Components are written in Cassandra 5.0.8's order: distinct file names in
    /// case-sensitive `String` natural order (see the ORDER RULE in the body).
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

        if !final_components
            .iter()
            .any(|e| e.component == SSTableComponent::TOC)
        {
            final_components.push(ComponentEntry::new(SSTableComponent::TOC));
        }

        // ORDER RULE (#4486), pinned at `cassandra-5.0.8` (CASSANDRA-20494):
        //   TOCComponent.updateTOC builds `new TreeSet<>(Collections2.transform(
        //   components, Component::name))` and writes `new ArrayList<>(set)` with
        //   FileUtils.write(..., CREATE, TRUNCATE_EXISTING, SYNC).
        // i.e. the TOC lines are the DISTINCT component FILE-NAME strings (`Data.db`,
        // `Digest.crc32`, `TOC.txt`, ...: the on-disk suffix after `<version>-<fmt>-`,
        // no other normalisation) in `String.compareTo` natural order: UTF-16 code
        // unit order, CASE-SENSITIVE (so `CRC.db` < `CompressionInfo.db` < `Data.db`),
        // not "alphabetical, case-folded". For these ASCII names that equals Rust's
        // `str` byte order, which is what `sort`/`dedup` on the file-name use here.
        // Each line ends in `\n`. (5.0.2 wrote an arbitrary insertion order; every
        // CQLite reader treats the TOC as a set, pinned by #4482 stage 1.)
        final_components.sort_by(|a, b| a.filename().cmp(b.filename()));
        final_components.dedup_by(|a, b| a.filename() == b.filename());

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
        writer
            .flush()
            .map_err(|e| Error::storage(format!("Failed to flush TOC.txt: {}", e)))?;

        let file = writer
            .into_inner()
            .map_err(|e| Error::storage(format!("Failed to extract file from buffer: {}", e)))?;

        file.sync_all()
            .map_err(|e| Error::storage(format!("Failed to sync TOC.txt to disk: {}", e)))?;

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

        // Cassandra 5.0.8 order (#4486): sorted file names, case-sensitive.
        assert_eq!(
            lines,
            [
                "CompressionInfo.db",
                "Data.db",
                "Digest.crc32",
                "Filter.db",
                "Index.db",
                "Statistics.db",
                "Summary.db",
                "TOC.txt"
            ]
        );
    }

    #[test]
    fn test_toc_writer_order_is_case_sensitive_and_deduplicated() {
        let temp_dir = TempDir::new().unwrap();
        let toc_path = temp_dir.path().join("da-1-bti-TOC.txt");
        // CRC.db sorts BEFORE CompressionInfo.db ('R' < 'o'), Rows.db after Partitions.db.
        let components: Vec<_> = [
            SSTableComponent::Rows,
            SSTableComponent::Data,
            SSTableComponent::Data,
            SSTableComponent::CompressionInfo,
            SSTableComponent::Partitions,
            SSTableComponent::Crc,
        ]
        .into_iter()
        .map(ComponentEntry::new)
        .collect();
        TocWriter::new(toc_path.clone()).write(&components).unwrap();
        let contents = fs::read_to_string(&toc_path).unwrap();
        assert_eq!(
            contents,
            "CRC.db\nCompressionInfo.db\nData.db\nPartitions.db\nRows.db\nTOC.txt\n"
        );
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
