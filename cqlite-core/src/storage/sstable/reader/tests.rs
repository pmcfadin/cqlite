//! Tests for SSTable reader functionality

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::super::compression::extract_sstable_base_name;
    use super::super::types::*;
    use crate::RowKey;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_reader_stats() {
        let stats = SSTableReaderStats {
            file_size: 1024,
            entry_count: 100,
            table_count: 1,
            block_count: 10,
            index_size: 128,
            bloom_filter_size: 64,
            compression_ratio: 0.8,
            cache_hit_rate: 0.9,
        };

        assert_eq!(stats.file_size, 1024);
        assert_eq!(stats.entry_count, 100);
        assert_eq!(stats.compression_ratio, 0.8);
    }

    #[tokio::test]
    async fn test_reader_config() {
        let config = SSTableReaderConfig::default();
        assert_eq!(config.read_buffer_size, 64 * 1024);
        assert!(config.validate_checksums);
        assert!(config.use_bloom_filter);
    }

    #[tokio::test]
    async fn test_block_meta() {
        let meta = BlockMeta {
            offset: 1024,
            compressed_size: 512,
            uncompressed_size: 1024,
            checksum: 0x1234_5678,
            first_key: RowKey::from("key1"),
            last_key: RowKey::from("key10"),
            entry_count: 10,
        };

        assert_eq!(meta.offset, 1024);
        assert_eq!(meta.compressed_size, 512);
        assert_eq!(meta.entry_count, 10);
    }

    #[test]
    fn test_extract_sstable_base_name() {
        // Test standard SSTable naming pattern
        let path = PathBuf::from("nb-1-big-Data.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("nb-1-big".to_string()));

        // Test with different components
        let path = PathBuf::from("nb-2-da-Index.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("nb-2-da".to_string()));

        let path = PathBuf::from("nb-3-big-Statistics.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("nb-3-big".to_string()));

        let path = PathBuf::from("keyspace-table-nb-456-big-Summary.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("keyspace-table-nb".to_string()));

        // Test with full path
        let path = PathBuf::from("/some/dir/nb-1-big-Data.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("nb-1-big".to_string()));

        // Test edge cases
        let path = PathBuf::from("not-enough-parts.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, None);

        let path = PathBuf::from("no-extension");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, None);

        // Test that the extracted base names correctly build component paths
        let data_path = PathBuf::from("/test/dir/nb-1-big-Data.db");
        let base_name = extract_sstable_base_name(&data_path).unwrap();

        let expected_index_path = data_path
            .parent()
            .unwrap()
            .join(format!("{}-Index.db", base_name));
        let expected_summary_path = data_path
            .parent()
            .unwrap()
            .join(format!("{}-Summary.db", base_name));
        let expected_stats_path = data_path
            .parent()
            .unwrap()
            .join(format!("{}-Statistics.db", base_name));

        assert_eq!(
            expected_index_path.file_name().unwrap(),
            "nb-1-big-Index.db"
        );
        assert_eq!(
            expected_summary_path.file_name().unwrap(),
            "nb-1-big-Summary.db"
        );
        assert_eq!(
            expected_stats_path.file_name().unwrap(),
            "nb-1-big-Statistics.db"
        );
    }
}
