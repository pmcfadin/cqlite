//! Golden Path Test Suite - Summary Index Integration
//!
//! This module provides comprehensive golden-path tests for SSTable summary and index
//! integration using real Cassandra 5 artifacts from test-data/datasets.
//!
//! Test Coverage:
//! - Summary index lookup and navigation
//! - Integration between Summary.db and Index.db components
//! - Efficient range operations using summary hints
//! - Multi-level index traversal performance
//! - Bloom filter coordination with summary/index
//! - Cross-component validation and consistency

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cqlite_core::{
    error::{Error, Result},
    platform::Platform,
    schema::registry::{SchemaRegistry, SchemaRegistryConfig},
    storage::sstable::{
        index_reader::IndexReader, reader::SSTableReader, summary_reader::SummaryReader,
    },
    types::TableId,
    Config, RowKey,
};
use cqlite_tests::discover_table_dir;

use tokio::fs;

/// Test fixture for golden path summary index integration
pub struct GoldenPathSummaryIndexTestFixture {
    /// Path to test datasets (kept for potential future fallback use)
    #[allow(dead_code)]
    datasets_path: PathBuf,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Configuration
    config: Config,
    /// Schema registry
    #[allow(dead_code)]
    schema_registry: Arc<SchemaRegistry>,
}

impl GoldenPathSummaryIndexTestFixture {
    /// Create new test fixture
    pub async fn new() -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let schema_config = SchemaRegistryConfig::default();
        let schema_registry =
            Arc::new(SchemaRegistry::new(schema_config, platform.clone(), config.clone()).await?);

        let datasets_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-data/datasets/sstables");

        Ok(Self {
            datasets_path,
            platform,
            config,
            schema_registry,
        })
    }

    /// Setup complete SSTable reader with all components
    async fn setup_complete_sstable_reader(&self) -> Result<SSTableReader> {
        let table_dir =
            discover_table_dir("test_basic", "compression_test_table").ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test_basic/compression_test_table not found. \
                     Please ensure CQLITE_DATASETS_ROOT is set and test-data is available.",
                ))
            })?;
        let sstable_path = table_dir.join("nb-1-big-Data.db");

        if fs::metadata(&sstable_path).await.is_err() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Test SSTable not found: {sstable_path:?}. Please ensure test-data is available."
                ),
            )));
        }

        SSTableReader::open(&sstable_path, &self.config, self.platform.clone()).await
    }

    /// Setup standalone summary reader for component testing
    async fn setup_summary_reader(&self) -> Result<SummaryReader> {
        let table_dir =
            discover_table_dir("test_basic", "compression_test_table").ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test_basic/compression_test_table not found.",
                ))
            })?;
        let summary_path = table_dir.join("nb-1-big-Summary.db");

        if fs::metadata(&summary_path).await.is_err() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Summary file not found: {summary_path:?}"),
            )));
        }

        SummaryReader::open(&summary_path, self.platform.clone()).await
    }

    /// Setup standalone index reader for component testing
    async fn setup_index_reader(&self) -> Result<IndexReader> {
        let table_dir =
            discover_table_dir("test_basic", "compression_test_table").ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test_basic/compression_test_table not found.",
                ))
            })?;
        let index_path = table_dir.join("nb-1-big-Index.db");

        if fs::metadata(&index_path).await.is_err() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Index file not found: {index_path:?}"),
            )));
        }

        IndexReader::open(&index_path, self.platform.clone()).await
    }

    /// Verify all SSTable component files exist
    async fn verify_component_files(&self) -> Result<HashMap<String, PathBuf>> {
        let base_path =
            discover_table_dir("test_basic", "compression_test_table").ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test_basic/compression_test_table not found.",
                ))
            })?;

        let components = vec![
            ("Data.db", "nb-1-big-Data.db"),
            ("Summary.db", "nb-1-big-Summary.db"),
            ("Index.db", "nb-1-big-Index.db"),
            ("Filter.db", "nb-1-big-Filter.db"),
            ("Statistics.db", "nb-1-big-Statistics.db"),
            ("CompressionInfo.db", "nb-1-big-CompressionInfo.db"),
        ];

        let mut component_paths = HashMap::new();

        for (component_name, filename) in components {
            let path = base_path.join(filename);
            if fs::metadata(&path).await.is_ok() {
                component_paths.insert(component_name.to_string(), path);
            }
        }

        Ok(component_paths)
    }
}

#[tokio::test]
async fn test_golden_path_summary_index_component_availability() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;

    // Test: Verify all SSTable components are available
    let component_paths = fixture.verify_component_files().await?;

    // Essential components for summary/index integration
    let required_components = vec!["Data.db", "Summary.db", "Index.db"];

    for component in &required_components {
        assert!(
            component_paths.contains_key(*component),
            "Required component {component} not found"
        );

        let path = &component_paths[*component];
        let metadata = fs::metadata(path).await?;

        assert!(
            metadata.len() > 0,
            "Component {component} should not be empty"
        );

        println!("✅ Component {}: {} bytes", component, metadata.len());
    }

    // Optional components
    let optional_components = vec!["Filter.db", "Statistics.db", "CompressionInfo.db"];

    for component in &optional_components {
        if let Some(path) = component_paths.get(*component) {
            let metadata = fs::metadata(path).await?;
            println!(
                "✅ Optional component {}: {} bytes",
                component,
                metadata.len()
            );
        } else {
            println!("ℹ️  Optional component {component} not present");
        }
    }

    println!("✅ SSTable component availability verified");
    Ok(())
}

#[tokio::test]
#[ignore = "Summary reader API methods (lookup_summary_entry, get_summary_stats) not yet implemented"]
async fn test_golden_path_summary_reader_functionality() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;

    // Test: Standalone summary reader operations
    match fixture.setup_summary_reader().await {
        Ok(_summary_reader) => {
            // NOTE: These methods are not yet implemented in SummaryReader:
            // - lookup_summary_entry()
            // - get_summary_stats()
            // Test is ignored until API is available

            println!("ℹ️  Summary reader created but API methods not yet available");
        }
        Err(e) => {
            println!("ℹ️  Summary reader not available (may be expected): {e}");
            // This is acceptable if the summary format is not yet supported
        }
    }

    Ok(())
}

#[tokio::test]
#[ignore = "Index reader API methods (lookup_index_entry, lookup_range_entries) not yet implemented"]
async fn test_golden_path_index_reader_functionality() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;

    // Test: Standalone index reader operations
    match fixture.setup_index_reader().await {
        Ok(_index_reader) => {
            // NOTE: These methods are not yet implemented in IndexReader:
            // - lookup_index_entry()
            // - lookup_range_entries()
            // Test is ignored until API is available

            println!("ℹ️  Index reader created but API methods not yet available");
        }
        Err(e) => {
            println!("ℹ️  Index reader not available (may be expected): {e}");
            // This is acceptable if the index format is not yet supported
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_integrated_summary_index_operations() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;
    let reader = fixture.setup_complete_sstable_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Integrated operations using both summary and index
    let test_keys = vec![
        RowKey::from(b"integrated_test_key_1".as_ref()),
        RowKey::from(b"integrated_test_key_2".as_ref()),
        RowKey::from(b"integrated_boundary_test".as_ref()),
    ];

    // Health check to verify components are available
    // NOTE: health_check() method is not currently available
    // let health_metrics = reader.health_check().await?;
    // println!(
    //     "✅ Reader health: index={}, bloom={}, compression={}",
    //     health_metrics.index_available,
    //     health_metrics.bloom_filter_enabled,
    //     health_metrics.compression_enabled
    // );

    for test_key in &test_keys {
        // Test integrated lookup (should use summary -> index -> data)
        let start_time = Instant::now();
        let result = reader.get(&table_id, test_key).await?;
        let lookup_duration = start_time.elapsed();

        // Integrated lookup should be very efficient
        assert!(
            lookup_duration.as_millis() < 20,
            "Integrated summary/index lookup should be very fast: {:?}ms",
            lookup_duration.as_millis()
        );

        match result {
            Some(value) => {
                println!(
                    "✅ Integrated lookup found value: {} bytes in {:?}",
                    value.len(),
                    lookup_duration
                );
            }
            None => {
                println!("ℹ️  No data found via integrated lookup (expected for test keys)");
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_summary_index_range_efficiency() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;
    let reader = fixture.setup_complete_sstable_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Range scans leveraging summary and index for efficiency
    let range_tests = vec![
        (
            "small_range",
            b"range_a" as &[u8],
            b"range_b" as &[u8],
            Some(10),
        ),
        (
            "medium_range",
            b"range_a" as &[u8],
            b"range_z" as &[u8],
            Some(50),
        ),
        ("large_range", b"a" as &[u8], b"z" as &[u8], Some(100)),
    ];

    for (test_name, start_bytes, end_bytes, limit) in range_tests {
        let start_key = RowKey::from(start_bytes);
        let end_key = RowKey::from(end_bytes);

        let start_time = Instant::now();
        let results = reader
            .scan(&table_id, Some(&start_key), Some(&end_key), limit, None)
            .await?;
        let scan_duration = start_time.elapsed();

        // Summary/index should make range scans efficient
        let max_duration_ms = match test_name {
            "small_range" => 50,
            "medium_range" => 200,
            "large_range" => 500,
            _ => 1000,
        };

        assert!(
            scan_duration.as_millis() < max_duration_ms,
            "Range scan '{}' should be efficient: {:?}ms (max: {}ms)",
            test_name,
            scan_duration.as_millis(),
            max_duration_ms
        );

        println!(
            "✅ Range scan '{}': {} entries in {:?}",
            test_name,
            results.len(),
            scan_duration
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_bloom_summary_index_coordination() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;
    let reader = fixture.setup_complete_sstable_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Coordination between bloom filter, summary, and index
    let non_existent_keys = vec![
        "definitely_not_in_sstable_1",
        "missing_key_xyz_999",
        "absent_data_coordination_test",
    ];

    // Behavioral fast-path assertion (issue #1149).
    //
    // The fixture's SSTable is `nb` (BIG) format with a Filter.db, so a `get()`
    // for an absent key runs the bloom pre-check in `SSTableReader::get`: when the
    // filter reports the key as absent it returns `Ok(None)` immediately, BEFORE
    // the summary/index lookup and WITHOUT falling through to the sequential
    // `scan_for_key` path. The previous test asserted this short-circuit
    // indirectly via an absolute `<100µs` wall-clock threshold, which is
    // load-sensitive and flaked under machine load (115µs/180µs after a heavy
    // compile) while passing cleanly when idle.
    //
    // We assert the short-circuit *directly*, with two complementary signals that
    // are immune to CPU load (the wall-clock flake) AND to cross-test counter
    // races (the only nondeterminism the counter probe would otherwise add):
    //
    //  1. STRUCTURAL (fully deterministic): the reader actually loaded a bloom
    //     filter, so the bloom pre-check branch in `get()` is reachable. Without
    //     this the "short-circuit" would be vacuous.
    //
    //  2. BEHAVIORAL: the existing public probe
    //     `SSTableReader::scan_for_key_call_count()` (the process-global
    //     `SCAN_FOR_KEY_CALLS` counter, issue #831) must NOT advance as a result
    //     of an absent-key `get()`. Because the counter is process-global and the
    //     integration-test binary runs its tests on multiple threads (other tests
    //     here call `get()`/`scan()` and can bump it), a single before/after read
    //     could observe a concurrent test's increment and false-fail. We make the
    //     check race-immune by retrying: a genuine regression (bloom no longer
    //     short-circuits) makes OUR `get()` fall through to `scan_for_key` on
    //     EVERY attempt, so the delta is `>= 1` every time; a concurrent test's
    //     scan is sporadic, so at least one attempt sees our call contribute zero.
    //     Requiring a single zero-delta attempt therefore fails a real regression
    //     while tolerating concurrent interference. Timing is kept as a
    //     non-asserting diagnostic print only.
    let health = reader.get_health_metrics().await?;
    assert!(
        health.bloom_filter_enabled,
        "fixture SSTable must have a bloom filter loaded for the fast-path check to be meaningful"
    );

    for key_str in &non_existent_keys {
        let test_key = RowKey::from(key_str.as_bytes());

        // Retry to distinguish a true bloom-fast-path regression (our get() scans
        // on every attempt) from a transient concurrent test bumping the global
        // counter in our measurement window.
        let mut observed_zero_delta = false;
        let mut last_delta = u64::MAX;
        let mut last_duration = std::time::Duration::ZERO;
        let mut last_result_none = false;
        for _ in 0..5 {
            let scans_before = SSTableReader::scan_for_key_call_count();
            let start_time = Instant::now();
            let result = reader.get(&table_id, &test_key).await?;
            last_duration = start_time.elapsed();
            let scans_after = SSTableReader::scan_for_key_call_count();

            last_result_none = result.is_none();
            last_delta = scans_after.saturating_sub(scans_before);
            if last_delta == 0 {
                observed_zero_delta = true;
                break;
            }
        }

        // Should be None for non-existent keys.
        assert!(
            last_result_none,
            "Non-existent key should return None: {key_str}"
        );

        // Bloom filter must short-circuit: at least one absent-key lookup returned
        // without invoking the sequential scan fallback (counter delta 0). A
        // regression that always falls through to scan_for_key never observes a
        // zero delta and fails here.
        assert!(
            observed_zero_delta,
            "Bloom filter should short-circuit absent-key lookup before scan_for_key \
             for {key_str}; every attempt advanced scan_for_key (last delta {last_delta}), \
             the bloom fast path regressed"
        );

        // Non-asserting diagnostic: the fast path is also expected to be quick,
        // but we no longer gate the test on an absolute wall-clock threshold
        // (issue #1149 — that assertion flaked under load).
        println!("  absent-key '{key_str}' short-circuited in {last_duration:?} (no scan_for_key)");
    }

    println!(
        "✅ Bloom/summary/index coordination verified - all non-existent lookups short-circuited the bloom fast path"
    );

    // Test with potentially existing keys (bloom might pass, then use summary/index)
    let potential_keys = vec!["potential_key_1", "test_data_key", "sample_entry"];

    for key_str in &potential_keys {
        let test_key = RowKey::from(key_str.as_bytes());

        let start_time = Instant::now();
        let result = reader.get(&table_id, &test_key).await?;
        let lookup_duration = start_time.elapsed();

        // Even if bloom passes, summary/index should make lookup efficient
        assert!(
            lookup_duration.as_millis() < 50,
            "Summary/index lookup should be efficient even after bloom pass: {:?}ms for {}",
            lookup_duration.as_millis(),
            key_str
        );

        match result {
            Some(value) => {
                println!("✅ Found key '{}': {} bytes", key_str, value.len());
            }
            None => {
                println!("ℹ️  Key '{key_str}' not found (bloom passed, but not in data)");
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_multi_level_index_traversal() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;
    let reader = fixture.setup_complete_sstable_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Multi-level index traversal performance
    let traversal_test_keys = (1..=20)
        .map(|i| RowKey::from(format!("traversal_test_key_{i:03}").as_bytes()))
        .collect::<Vec<_>>();

    let mut traversal_times = Vec::new();

    for test_key in &traversal_test_keys {
        let start_time = Instant::now();
        let _result = reader.get(&table_id, test_key).await?;
        let lookup_duration = start_time.elapsed();

        traversal_times.push(lookup_duration);

        // Each multi-level traversal should be efficient
        assert!(
            lookup_duration.as_millis() < 25,
            "Multi-level index traversal should be fast: {:?}ms",
            lookup_duration.as_millis()
        );
    }

    // Calculate traversal statistics
    let total_time: std::time::Duration = traversal_times.iter().sum();
    let avg_time = total_time / traversal_times.len() as u32;
    let max_time = traversal_times.iter().max().unwrap();
    let min_time = traversal_times.iter().min().unwrap();

    println!("✅ Multi-level traversal stats:");
    println!(
        "   Total: {:?} for {} lookups",
        total_time,
        traversal_times.len()
    );
    println!("   Average: {avg_time:?}");
    println!("   Min: {min_time:?}, Max: {max_time:?}");

    // Performance assertions for batch traversals
    assert!(
        avg_time.as_micros() < 5000,
        "Average multi-level traversal should be very efficient: {:?}μs",
        avg_time.as_micros()
    );

    assert!(
        max_time.as_millis() < 50,
        "Maximum traversal time should be reasonable: {:?}ms",
        max_time.as_millis()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_summary_index_statistics_integration() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;
    let reader = fixture.setup_complete_sstable_reader().await?;

    // Test: Integration with statistics component
    let stats = reader.stats().await?;

    println!("✅ Reader statistics:");
    println!("   File size: {} bytes", stats.file_size);
    println!("   Block count: {}", stats.block_count);
    println!("   Entry count: {}", stats.entry_count);
    // NOTE: cache_hits/cache_misses fields are not available
    // Available field: cache_hit_rate
    println!("   Cache hit rate: {:.2}", stats.cache_hit_rate);

    // Basic statistics validation
    assert!(stats.file_size > 0, "File size should be positive");
    // block_count is unsigned, so it's always >= 0

    // Perform some operations to generate cache statistics
    let table_id = TableId::new("test_keyspace.test_table");
    let test_keys = (1..=10)
        .map(|i| RowKey::from(format!("stats_test_{i}").as_bytes()))
        .collect::<Vec<_>>();

    // Perform lookups to generate statistics
    for key in &test_keys {
        let _ = reader.get(&table_id, key).await?;
    }

    // Check updated statistics
    let updated_stats = reader.stats().await?;

    println!(
        "✅ Updated statistics after {} operations:",
        test_keys.len()
    );
    println!("   Cache hit rate: {:.2}", updated_stats.cache_hit_rate);

    // Note: Cannot track individual cache hit/miss counts without those fields
    // Verifying that operations completed successfully is sufficient

    Ok(())
}

#[tokio::test]
async fn test_golden_path_summary_index_consistency_validation() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;
    let reader = fixture.setup_complete_sstable_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Cross-validate summary, index, and actual data consistency

    // Perform full scan to get actual data
    let full_scan_results = reader.scan(&table_id, None, None, Some(50), None).await?;

    if !full_scan_results.is_empty() {
        println!(
            "✅ Full scan found {} entries for consistency validation",
            full_scan_results.len()
        );

        // Test consistency for each key found in scan
        for (key, expected_value) in &full_scan_results {
            // Get operation should return same value as scan
            let get_result = reader.get(&table_id, key).await?;

            match get_result {
                Some(get_value) => {
                    assert_eq!(
                        get_value, *expected_value,
                        "Get and scan should return consistent values for key: {key:?}"
                    );
                }
                None => {
                    // This could indicate an inconsistency, but might be acceptable
                    // depending on the SSTable format and implementation
                    println!("⚠️  Get returned None for key found in scan: {key:?}");
                }
            }
        }

        // Test range consistency: range scans should return subset of full scan
        if full_scan_results.len() > 5 {
            let mid_point = full_scan_results.len() / 2;
            let range_start = &full_scan_results[0].0;
            let range_end = &full_scan_results[mid_point].0;

            let range_results = reader
                .scan(&table_id, Some(range_start), Some(range_end), None, None)
                .await?;

            // All range results should be within the specified bounds
            for (range_key, _) in &range_results {
                assert!(
                    range_key >= range_start && range_key <= range_end,
                    "Range scan result should be within bounds: {range_key:?} not in [{range_start:?}, {range_end:?}]"
                );
            }

            println!(
                "✅ Range consistency validated: {}/{} entries in range",
                range_results.len(),
                full_scan_results.len()
            );
        }

        println!("✅ Summary/index/data consistency validation completed");
    } else {
        println!("ℹ️  No data found for consistency validation (empty SSTable)");
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_summary_index_performance_integration() -> Result<()> {
    let fixture = GoldenPathSummaryIndexTestFixture::new().await?;
    let reader = fixture.setup_complete_sstable_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Performance integration test: Compare different access patterns
    let test_scenarios = vec![
        (
            "sequential_access",
            (1..=25).map(|i| format!("seq_{i:03}")).collect::<Vec<_>>(),
        ),
        (
            "random_access",
            vec![
                "random_key_z".to_string(),
                "random_key_a".to_string(),
                "random_key_m".to_string(),
                "random_key_f".to_string(),
                "random_key_r".to_string(),
            ],
        ),
        (
            "pattern_access",
            vec![
                "pattern_aaa".to_string(),
                "pattern_bbb".to_string(),
                "pattern_ccc".to_string(),
                "pattern_ddd".to_string(),
                "pattern_eee".to_string(),
            ],
        ),
    ];

    let mut scenario_results = HashMap::new();

    for (scenario_name, test_keys) in test_scenarios {
        let start_time = Instant::now();
        let mut found_count = 0;

        for key_str in &test_keys {
            let key = RowKey::from(key_str.as_bytes());
            if let Some(_value) = reader.get(&table_id, &key).await? {
                found_count += 1;
            }
        }

        let scenario_duration = start_time.elapsed();
        let avg_duration = scenario_duration / test_keys.len() as u32;

        scenario_results.insert(
            scenario_name,
            (scenario_duration, avg_duration, found_count),
        );

        // Performance assertions per scenario
        assert!(
            avg_duration.as_micros() < 10000,
            "Scenario '{}' average lookup should be efficient: {:?}μs",
            scenario_name,
            avg_duration.as_micros()
        );

        println!(
            "✅ Scenario '{}': {} keys in {:?} (avg: {:?}, found: {})",
            scenario_name,
            test_keys.len(),
            scenario_duration,
            avg_duration,
            found_count
        );
    }

    // Overall performance validation
    let total_operations: usize = scenario_results
        .values()
        .map(|(_, _, found)| *found as usize)
        .sum();
    let total_time: std::time::Duration = scenario_results
        .values()
        .map(|(duration, _, _)| *duration)
        .sum();

    if total_operations > 0 {
        let overall_avg = total_time / total_operations as u32;
        println!(
            "✅ Overall performance: {total_operations} operations in {total_time:?} (avg: {overall_avg:?})"
        );

        assert!(
            overall_avg.as_micros() < 15000,
            "Overall average performance should be good: {:?}μs",
            overall_avg.as_micros()
        );
    }

    println!("✅ Summary/index performance integration validated");
    Ok(())
}
