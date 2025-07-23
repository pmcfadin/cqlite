//! Comprehensive tests for enhanced Statistics.db parsing
//!
//! This module tests the enhanced parser against real Cassandra 5.0 'nb' format files

#[cfg(test)]
mod tests {
    use super::super::enhanced_statistics_parser::*;
    use super::super::statistics::*;
    use crate::platform::Platform;
    use crate::storage::sstable::statistics_reader::StatisticsReader;
    use crate::Config;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::fs;

    /// Test the enhanced parser against real Statistics.db files
    #[tokio::test]
    async fn test_enhanced_parser_real_files() {
        let test_files = [
            "test-env/cassandra5/sstables/users-46436710673711f0b2cf19d64e7cbecb/nb-1-big-Statistics.db",
            "test-env/cassandra5/sstables/all_types-46200090673711f0b2cf19d64e7cbecb/nb-1-big-Statistics.db",
            "test-env/cassandra5/sstables/collections_table-462afd10673711f0b2cf19d64e7cbecb/nb-1-big-Statistics.db",
            "test-env/cassandra5/sstables/time_series-464cb5e0673711f0b2cf19d64e7cbecb/nb-1-big-Statistics.db",
            "test-env/cassandra5/sstables/multi_clustering-465604b0673711f0b2cf19d64e7cbecb/nb-1-big-Statistics.db",
        ];

        let mut successful_parses = 0;
        let mut total_files = 0;

        for test_file_path in &test_files {
            let path = Path::new(test_file_path);
            if !path.exists() {
                println!("  ⏭️  Skipping missing test file: {}", test_file_path);
                continue;
            }

            total_files += 1;
            println!("🔍 Testing enhanced parser on: {}", test_file_path);

            // Read the file
            match fs::read(path).await {
                Ok(file_data) => {
                    println!("  📊 File size: {} bytes", file_data.len());
                    
                    // Test hex dump of first 64 bytes for analysis
                    println!("  🔍 File header (first 64 bytes):");
                    for (i, chunk) in file_data[..std::cmp::min(64, file_data.len())].chunks(16).enumerate() {
                        print!("    {:08x}  ", i * 16);
                        for byte in chunk {
                            print!("{:02x} ", byte);
                        }
                        println!();
                    }

                    // Test enhanced parsing
                    match parse_enhanced_statistics_file(&file_data) {
                        Ok((_, statistics)) => {
                            successful_parses += 1;
                            println!("  ✅ Successfully parsed with enhanced parser!");
                            
                            // Validate the parsed data
                            validate_enhanced_statistics(&statistics, test_file_path);
                            
                            // Test analysis
                            let summary = StatisticsAnalyzer::analyze(&statistics);
                            println!("  📈 Analysis - Rows: {}, Health: {:.1}, Compression: {:.1}%", 
                                summary.total_rows, summary.health_score, summary.compression_efficiency);
                            
                            // Test report generation
                            let report = generate_enhanced_report(&statistics);
                            assert!(!report.is_empty(), "Report should not be empty");
                            println!("  📋 Generated {} character report", report.len());
                        }
                        Err(e) => {
                            println!("  ❌ Enhanced parser failed: {:?}", e);
                            
                            // Try to use StatisticsReader as fallback
                            let config = Config::default();
                            let platform = Arc::new(Platform::new(&config).await.unwrap());
                            
                            match StatisticsReader::open(path, platform).await {
                                Ok(stats_reader) => {
                                    println!("  ✅ Fallback StatisticsReader succeeded");
                                    let compact = stats_reader.compact_summary();
                                    println!("  📊 {}", compact);
                                }
                                Err(reader_err) => {
                                    println!("  ❌ StatisticsReader also failed: {}", reader_err);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("  ❌ Failed to read file: {}", e);
                }
            }
            println!();
        }

        println!("📊 Enhanced parser test summary:");
        println!("  Total files tested: {}", total_files);
        println!("  Successful parses: {}", successful_parses);
        println!("  Success rate: {:.1}%", 
            if total_files > 0 { (successful_parses as f64 / total_files as f64) * 100.0 } else { 0.0 });

        // We expect at least some files to parse successfully
        assert!(successful_parses > 0, "At least one file should parse successfully");
    }

    /// Validate the enhanced statistics data
    fn validate_enhanced_statistics(stats: &SSTableStatistics, file_path: &str) {
        // Header validation
        assert_eq!(stats.header.version, 4, "Expected version 4 for nb format");
        assert!(stats.header.data_length > 0, "Data length should be positive");
        
        // Row statistics validation
        assert!(stats.row_stats.total_rows >= stats.row_stats.live_rows, 
            "Total rows should be >= live rows");
        assert!(stats.row_stats.partition_count > 0, "Should have partitions");
        
        // Table statistics validation
        assert!(stats.table_stats.disk_size > 0, "Disk size should be positive");
        assert!(stats.table_stats.compression_ratio > 0.0 && stats.table_stats.compression_ratio <= 1.0,
            "Compression ratio should be between 0 and 1");
        
        // Metadata validation
        assert!(stats.metadata.contains_key("format"), "Should have format metadata");
        assert!(stats.metadata.contains_key("parser_version"), "Should have parser version");
        
        println!("  ✅ Statistics validation passed for {}", 
            file_path.split('/').last().unwrap_or("unknown"));
    }

    /// Generate a comprehensive report for enhanced statistics
    fn generate_enhanced_report(stats: &SSTableStatistics) -> String {
        let mut report = String::new();
        
        report.push_str("# Enhanced Statistics.db Analysis Report\n\n");
        
        // Header information
        report.push_str("## Header Information\n");
        report.push_str(&format!("- **Format Version**: {} (0x{:08X})\n", 
            stats.header.version, stats.header.version));
        report.push_str(&format!("- **Statistics Kind**: 0x{:08X}\n", stats.header.statistics_kind));
        report.push_str(&format!("- **Data Length**: {} bytes\n", stats.header.data_length));
        report.push_str(&format!("- **Metadata**: [{}, {}, {}]\n", 
            stats.header.metadata1, stats.header.metadata2, stats.header.metadata3));
        
        if let Some(table_id) = stats.header.table_id {
            report.push_str(&format!("- **Table ID**: {}\n", 
                table_id.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join("")));
        }
        report.push('\n');
        
        // Row statistics
        report.push_str("## Row Statistics\n");
        report.push_str(&format!("- **Total Rows**: {}\n", stats.row_stats.total_rows));
        report.push_str(&format!("- **Live Rows**: {} ({:.1}%)\n", 
            stats.row_stats.live_rows,
            if stats.row_stats.total_rows > 0 {
                (stats.row_stats.live_rows as f64 / stats.row_stats.total_rows as f64) * 100.0
            } else { 0.0 }
        ));
        report.push_str(&format!("- **Tombstones**: {}\n", stats.row_stats.tombstone_count));
        report.push_str(&format!("- **Partitions**: {}\n", stats.row_stats.partition_count));
        report.push_str(&format!("- **Avg Rows/Partition**: {:.1}\n", stats.row_stats.avg_rows_per_partition));
        report.push('\n');
        
        // Table statistics
        report.push_str("## Table Statistics\n");
        report.push_str(&format!("- **Disk Size**: {:.2} MB\n", 
            stats.table_stats.disk_size as f64 / 1_048_576.0));
        report.push_str(&format!("- **Compression Ratio**: {:.2}%\n", 
            stats.table_stats.compression_ratio * 100.0));
        report.push_str(&format!("- **Block Count**: {}\n", stats.table_stats.block_count));
        report.push_str(&format!("- **Index Size**: {:.2} KB\n", 
            stats.table_stats.index_size as f64 / 1024.0));
        report.push('\n');
        
        // Compression statistics
        report.push_str("## Compression Statistics\n");
        report.push_str(&format!("- **Algorithm**: {}\n", stats.compression_stats.algorithm));
        report.push_str(&format!("- **Original Size**: {:.2} MB\n", 
            stats.compression_stats.original_size as f64 / 1_048_576.0));
        report.push_str(&format!("- **Compressed Size**: {:.2} MB\n", 
            stats.compression_stats.compressed_size as f64 / 1_048_576.0));
        report.push_str(&format!("- **Compression Speed**: {:.1} MB/s\n", 
            stats.compression_stats.compression_speed));
        report.push('\n');
        
        // Metadata
        if !stats.metadata.is_empty() {
            report.push_str("## Metadata\n");
            for (key, value) in &stats.metadata {
                report.push_str(&format!("- **{}**: {}\n", key, value));
            }
            report.push('\n');
        }
        
        // Analysis
        let analysis = StatisticsAnalyzer::analyze(stats);
        report.push_str("## Analysis Summary\n");
        report.push_str(&format!("- **Health Score**: {:.1}/100\n", analysis.health_score));
        report.push_str(&format!("- **Data Efficiency**: {:.1}%\n", analysis.data_efficiency));
        report.push_str(&format!("- **Timestamp Range**: {:.1} days\n", analysis.timestamp_range_days));
        
        if !analysis.query_performance_hints.is_empty() {
            report.push_str("\n### Performance Hints\n");
            for hint in &analysis.query_performance_hints {
                report.push_str(&format!("- {}\n", hint));
            }
        }
        
        if !analysis.storage_recommendations.is_empty() {
            report.push_str("\n### Storage Recommendations\n");
            for rec in &analysis.storage_recommendations {
                report.push_str(&format!("- {}\n", rec));
            }
        }
        
        report
    }

    /// Test individual components of the enhanced parser
    #[test]
    fn test_enhanced_parser_components() {
        // Test header parsing with real data pattern
        let header_data = vec![
            0x00, 0x00, 0x00, 0x04, // version = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14, 0xd4, // checksum = 5332
        ];

        let result = parse_nb_format_header(&header_data);
        assert!(result.is_ok(), "Header parsing should succeed");

        let (_, header) = result.unwrap();
        assert_eq!(header.version, 4);
        assert_eq!(header.statistics_kind, 0x26291b05);
        assert_eq!(header.data_length, 44);
        assert_eq!(header.metadata2, 101); // This becomes our row count estimate

        // Test statistics extraction
        let dummy_binary_data = vec![0u8; 1000];
        let result = parse_nb_format_statistics_data(&dummy_binary_data, &header);
        assert!(result.is_ok(), "Statistics data extraction should succeed");

        let (row_stats, timestamp_stats, table_stats, partition_stats, compression_stats) = result.unwrap();
        
        // Validate extracted statistics
        assert_eq!(row_stats.total_rows, 101); // Should match metadata2
        assert!(row_stats.live_rows > 0);
        assert!(row_stats.partition_count > 0);
        assert!(timestamp_stats.max_timestamp > timestamp_stats.min_timestamp);
        assert!(table_stats.disk_size > 0);
        assert!(partition_stats.avg_partition_size > 0.0);
        assert_eq!(compression_stats.algorithm, "LZ4");
    }

    /// Test fallback parser behavior
    #[test]
    fn test_parser_fallback() {
        // Test with invalid data that should fail both parsers
        let invalid_data = vec![0xFF; 10];
        let result = parse_statistics_with_fallback(&invalid_data);
        assert!(result.is_err(), "Invalid data should fail to parse");

        // Test with valid header but no data
        let minimal_data = vec![
            0x00, 0x00, 0x00, 0x04, // version = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14, 0xd4, // checksum = 5332
        ];

        let result = parse_statistics_with_fallback(&minimal_data);
        assert!(result.is_ok(), "Valid header should parse successfully");
    }
}