//! Simple standalone test for Cassandra 5 fixtures
//! This test verifies the minimal fixture infrastructure works

use std::fs;
use std::path::PathBuf;

#[test]
fn test_cassandra5_fixture_integrity() {
    // Get path to fixtures
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/cassandra5/minimal/simple_table");

    println!("Looking for fixtures at: {fixture_path:?}");

    // Test that fixture directory exists
    assert!(fixture_path.exists(), "Fixture directory does not exist");

    // Test required files exist
    let required_files = [
        "Data.db",
        "Statistics.db",
        "Index.db",
        "Summary.db",
        "Filter.db",
        "Digest.crc32",
        "TOC.txt",
    ];

    for file in &required_files {
        let file_path = fixture_path.join(file);
        assert!(file_path.exists(), "Required file {file} does not exist");

        let metadata = fs::metadata(&file_path).unwrap();
        assert!(
            metadata.len() > 0 || file == &"Filter.db",
            "File {file} is empty"
        );

        println!("✅ {} - {} bytes", file, metadata.len());
    }

    // Test Data.db format
    let data_path = fixture_path.join("Data.db");
    let data = fs::read(&data_path).unwrap();

    assert!(data.len() >= 4, "Data.db too small");
    assert_eq!(&data[0..2], b"nb", "Invalid Cassandra format marker");

    let version = u16::from_be_bytes([data[2], data[3]]);
    assert_eq!(version, 1, "Expected version 1, got {version}");

    println!("✅ Data.db format validated - Cassandra format with version {version}");

    // Test TOC.txt content
    let toc_path = fixture_path.join("TOC.txt");
    let toc_content = fs::read_to_string(&toc_path).unwrap();
    let components: Vec<&str> = toc_content.lines().collect();

    assert!(
        components.len() >= 6,
        "TOC should list at least 6 components"
    );
    assert!(components.contains(&"Data.db"), "TOC should list Data.db");
    assert!(
        components.contains(&"Statistics.db"),
        "TOC should list Statistics.db"
    );

    println!(
        "✅ TOC.txt validated - {} components listed",
        components.len()
    );

    // Test total fixture size is reasonable
    let total_size: u64 = required_files
        .iter()
        .map(|file| fs::metadata(fixture_path.join(file)).unwrap().len())
        .sum();

    assert!(
        total_size < 1024,
        "Total fixture size should be < 1KB, got {total_size} bytes"
    );

    println!("✅ Total fixture size: {total_size} bytes (meets minimal size requirement)");

    println!("🎉 All Cassandra 5 fixture integrity checks passed!");
}

#[test]
fn test_data_db_content_parsing() {
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/cassandra5/minimal/simple_table");

    let data_path = fixture_path.join("Data.db");
    let data = fs::read(&data_path).unwrap();

    // Parse the minimal content we created
    // Format: nb(2) + version(2) + partition_size(4) + row_count(4) + key(4) + value_len(4) + value

    assert!(data.len() >= 12, "Data too small for expected format");

    // Skip header to get to row data
    let mut offset = 12; // nb + version + partition_size + row_count

    // Read key
    if offset + 4 <= data.len() {
        let key_bytes = &data[offset..offset + 4];
        let key = i32::from_be_bytes([key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3]]);
        println!("✅ Found key: {key}");
        assert_eq!(key, 1, "Expected key=1");
        offset += 4;
    }

    // Read value length and value
    if offset + 4 <= data.len() {
        let value_len_bytes = &data[offset..offset + 4];
        let value_len = u32::from_be_bytes([
            value_len_bytes[0],
            value_len_bytes[1],
            value_len_bytes[2],
            value_len_bytes[3],
        ]) as usize;
        offset += 4;

        if offset + value_len <= data.len() {
            let value_bytes = &data[offset..offset + value_len];
            let value = String::from_utf8(value_bytes.to_vec()).unwrap();
            println!("✅ Found value: '{value}'");
            assert_eq!(value, "test", "Expected value='test'");
        }
    }

    println!("🎉 Data.db content parsing successful!");
}
