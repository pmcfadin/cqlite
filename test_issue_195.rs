use std::fs;

fn main() {
    // Read the Statistics.db file that's failing
    let path = "/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db";
    let file_bytes = fs::read(path).expect("Failed to read file");

    println!("File size: {} bytes", file_bytes.len());

    // Try to parse it
    match cqlite_core::parser::enhanced_statistics_parser::parse_enhanced_statistics_file(&file_bytes) {
        Ok((_, stats)) => {
            println!("SUCCESS: Parsed statistics");
            println!("  min_timestamp: {}", stats.timestamp_stats.min_timestamp);
        }
        Err(e) => {
            println!("ERROR: Failed to parse: {:?}", e);
        }
    }
}
