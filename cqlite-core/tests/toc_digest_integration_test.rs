//! Integration tests for TOC.txt and Digest.crc32 writers
//!
//! Tests the complete workflow of writing SSTable metadata files.

#[cfg(feature = "write-support")]
mod toc_digest_tests {
    use cqlite_core::storage::sstable::directory::types::SSTableComponent;
    use cqlite_core::storage::sstable::writer::{ComponentEntry, DigestWriter, TocWriter};
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_complete_sstable_publication_workflow() {
        // This test demonstrates the complete workflow of writing an SSTable:
        // 1. Create component files
        // 2. Write Digest.crc32
        // 3. Write TOC.txt (publication barrier)

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Step 1: Simulate creating component files
        let data_path = base_path.join("nb-1-big-Data.db");
        let index_path = base_path.join("nb-1-big-Index.db");
        let stats_path = base_path.join("nb-1-big-Statistics.db");

        // Write some dummy data to Data.db
        let test_data = b"This is test SSTable data for integration testing.";
        fs::write(&data_path, test_data).unwrap();
        fs::write(&index_path, b"Index data").unwrap();
        fs::write(&stats_path, b"Statistics data").unwrap();

        // Step 2: Write Digest.crc32 for Data.db
        let digest_path = base_path.join("nb-1-big-Digest.crc32");
        let digest_writer = DigestWriter::new(digest_path.clone());
        let crc32 = digest_writer.write_for_file(&data_path).unwrap();

        // Verify Digest.crc32 was written correctly
        assert!(digest_path.exists());
        let digest_contents = fs::read_to_string(&digest_path).unwrap();
        assert_eq!(digest_contents, format!("{}", crc32));

        // Verify CRC32 matches expected value
        let expected_crc32 = crc32fast::hash(test_data);
        assert_eq!(crc32, expected_crc32);

        // Step 3: Write TOC.txt (publication barrier - MUST be last)
        let toc_path = base_path.join("nb-1-big-TOC.txt");
        let components = vec![
            ComponentEntry::new(SSTableComponent::Data),
            ComponentEntry::new(SSTableComponent::Index),
            ComponentEntry::new(SSTableComponent::Statistics),
            ComponentEntry::new(SSTableComponent::Digest),
        ];

        let toc_writer = TocWriter::new(toc_path.clone());
        toc_writer.write(&components).unwrap();

        // Verify TOC.txt was written correctly
        assert!(toc_path.exists());
        let toc_contents = fs::read_to_string(&toc_path).unwrap();
        let lines: Vec<&str> = toc_contents.lines().collect();

        // Should have all components plus TOC.txt itself
        assert_eq!(lines.len(), 5);
        assert!(lines.contains(&"Data.db"));
        assert!(lines.contains(&"Index.db"));
        assert!(lines.contains(&"Statistics.db"));
        assert!(lines.contains(&"Digest.crc32"));
        assert!(lines.contains(&"TOC.txt"));

        // Verify TOC.txt is the last file (publication barrier)
        // In a real implementation, this would be enforced by the SSTableWriter coordinator
    }

    #[test]
    fn test_digest_validation_workflow() {
        // This test demonstrates how to validate a component file using its digest

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create a test data file
        let data_path = base_path.join("nb-1-big-Data.db");
        let test_data = b"Test data for validation";
        fs::write(&data_path, test_data).unwrap();

        // Write the digest
        let digest_path = base_path.join("nb-1-big-Digest.crc32");
        let digest_writer = DigestWriter::new(digest_path.clone());
        let original_crc32 = digest_writer.write_for_file(&data_path).unwrap();

        // Simulate validation: read the digest and recompute CRC32
        let stored_crc32_str = fs::read_to_string(&digest_path).unwrap();
        let stored_crc32: u32 = stored_crc32_str.parse().unwrap();

        let recomputed_crc32 = DigestWriter::compute_crc32(&data_path).unwrap();

        // Validation succeeds if checksums match
        assert_eq!(stored_crc32, recomputed_crc32);
        assert_eq!(stored_crc32, original_crc32);

        // Simulate corruption: modify the file
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&data_path)
            .unwrap();
        file.write_all(b" CORRUPTED").unwrap();
        drop(file);

        // Recompute CRC32 after corruption
        let corrupted_crc32 = DigestWriter::compute_crc32(&data_path).unwrap();

        // Validation should fail (checksums differ)
        assert_ne!(stored_crc32, corrupted_crc32);
    }

    #[test]
    fn test_toc_ordering_matches_cassandra() {
        // Verify that our TOC.txt component ordering matches Cassandra's

        let temp_dir = TempDir::new().unwrap();
        let toc_path = temp_dir.path().join("nb-1-big-TOC.txt");

        let components = vec![
            ComponentEntry::new(SSTableComponent::Summary),
            ComponentEntry::new(SSTableComponent::Filter),
            ComponentEntry::new(SSTableComponent::Index),
            ComponentEntry::new(SSTableComponent::CompressionInfo),
            ComponentEntry::new(SSTableComponent::Digest),
            ComponentEntry::new(SSTableComponent::Statistics),
            ComponentEntry::new(SSTableComponent::Data),
        ];

        let writer = TocWriter::new(toc_path.clone());
        writer.write(&components).unwrap();

        let contents = fs::read_to_string(&toc_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();

        // Verify the canonical order
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
    fn test_empty_digest_file() {
        // Test computing digest for an empty file
        let temp_dir = TempDir::new().unwrap();
        let empty_file = temp_dir.path().join("empty-Data.db");
        let digest_path = temp_dir.path().join("empty-Digest.crc32");

        // Create empty file
        fs::File::create(&empty_file).unwrap();

        // Compute and write digest
        let digest_writer = DigestWriter::new(digest_path.clone());
        let crc32 = digest_writer.write_for_file(&empty_file).unwrap();

        // CRC32 of empty data is 0
        assert_eq!(crc32, 0);

        let contents = fs::read_to_string(&digest_path).unwrap();
        assert_eq!(contents, "0");
    }
}
