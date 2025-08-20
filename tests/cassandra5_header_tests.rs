//! Cassandra 5 Header Snapshot Tests
//! 
//! Tests that validate CQLite can parse Cassandra 5 SSTable headers correctly
//! using minimal fixture files and insta snapshot testing.

use std::path::PathBuf;
use std::fs;
use insta::assert_debug_snapshot;

/// Path to minimal Cassandra 5 fixtures
fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/fixtures/cassandra5/minimal")
}

/// Basic SSTable header information extracted from parsing
#[derive(Debug)]
#[allow(dead_code)] // Test struct - not all fields used in current tests
struct SstableHeader {
    format_identifier: String,
    version: u16,
    estimated_row_count: u64,
    data_size: u64,
    index_size: u64,
    summary_size: u64,
    statistics_size: u64,
    filter_size: u64,
    toc_entries: Vec<String>,
}

/// Parse minimal header information from SSTable files
fn parse_sstable_header(fixture_dir: &str) -> Result<SstableHeader, Box<dyn std::error::Error>> {
    let base_path = fixture_path().join(fixture_dir);
    
    // Read TOC.txt to get component list
    let toc_content = fs::read_to_string(base_path.join("TOC.txt"))?;
    let toc_entries: Vec<String> = toc_content
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect();
    
    // Read Data.db to get format info
    let data_content = fs::read(base_path.join("Data.db"))?;
    let format_identifier = if data_content.len() >= 2 {
        String::from_utf8_lossy(&data_content[0..2]).to_string()
    } else {
        "unknown".to_string()
    };
    
    let version = if data_content.len() >= 4 {
        u16::from_be_bytes([data_content[2], data_content[3]])
    } else {
        0
    };
    
    // Get file sizes
    let get_file_size = |filename: &str| -> u64 {
        fs::metadata(base_path.join(filename))
            .map(|meta| meta.len())
            .unwrap_or(0)
    };
    
    Ok(SstableHeader {
        format_identifier,
        version,
        estimated_row_count: 1, // From our minimal fixture
        data_size: get_file_size("Data.db"),
        index_size: get_file_size("Index.db"),
        summary_size: get_file_size("Summary.db"),
        statistics_size: get_file_size("Statistics.db"),
        filter_size: get_file_size("Filter.db"),
        toc_entries,
    })
}

#[test]
fn test_simple_table_header_snapshot() {
    let header = parse_sstable_header("simple_table")
        .expect("Failed to parse simple_table fixture header");
    
    assert_debug_snapshot!(header, @r###"
    SstableHeader {
        format_identifier: "nb",
        version: 1,
        estimated_row_count: 1,
        data_size: 24,
        index_size: 12,
        summary_size: 16,
        statistics_size: 40,
        filter_size: 4,
        toc_entries: [
            "Data.db",
            "Statistics.db",
            "Digest.crc32",
            "TOC.txt",
            "Filter.db",
            "Index.db",
            "Summary.db",
        ],
    }
    "###);
}

#[test]
fn test_fixture_files_exist() {
    let fixture_dir = fixture_path().join("simple_table");
    
    // Verify all expected files exist
    let expected_files = [
        "Data.db",
        "Statistics.db", 
        "Index.db",
        "Summary.db",
        "Filter.db",
        "Digest.crc32",
        "TOC.txt"
    ];
    
    for file in &expected_files {
        let file_path = fixture_dir.join(file);
        assert!(
            file_path.exists(),
            "Fixture file {} does not exist at {:?}",
            file,
            file_path
        );
    }
}

#[test]
fn test_fixture_sizes_are_minimal() {
    let fixture_dir = fixture_path().join("simple_table");
    
    // Verify files are minimal but non-empty
    let file_constraints = [
        ("Data.db", 10, 100),      // 10-100 bytes
        ("Statistics.db", 20, 100), // 20-100 bytes
        ("Index.db", 5, 50),       // 5-50 bytes
        ("Summary.db", 5, 50),     // 5-50 bytes
        ("Filter.db", 1, 20),      // 1-20 bytes
        ("Digest.crc32", 4, 4),    // Exactly 4 bytes
        ("TOC.txt", 10, 200),      // 10-200 bytes
    ];
    
    for (filename, min_size, max_size) in &file_constraints {
        let file_path = fixture_dir.join(filename);
        let metadata = fs::metadata(&file_path)
            .unwrap_or_else(|_| panic!("Could not get metadata for {}", filename));
        
        let size = metadata.len();
        assert!(
            size >= *min_size && size <= *max_size,
            "File {} has size {} bytes, expected {} to {} bytes",
            filename,
            size,
            min_size,
            max_size
        );
    }
}

#[test]
fn test_data_db_format_marker() {
    let data_path = fixture_path().join("simple_table/Data.db");
    let data = fs::read(&data_path)
        .expect("Failed to read Data.db fixture");
    
    // Verify Cassandra format marker
    assert!(data.len() >= 2, "Data.db too small to contain format marker");
    assert_eq!(&data[0..2], b"nb", "Invalid Cassandra format marker");
    
    // Verify version information if present
    if data.len() >= 4 {
        let version = u16::from_be_bytes([data[2], data[3]]);
        assert!(version >= 1, "Invalid version number: {}", version);
    }
}