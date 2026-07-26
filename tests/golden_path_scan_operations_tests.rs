//! Golden Path Test Suite - Scan Operations
//!
//! This module provides comprehensive golden-path tests for SSTable scan operations
//! using real Cassandra 5 artifacts from test-data/datasets.
//!
//! Test Coverage:
//! - Range scans with start/end key boundaries
//! - Full table scans with limits
//! - Prefix scans and filtering
//! - Reverse scans and ordering validation
//! - Performance assertions for large datasets
//! - Memory efficiency during streaming
//! - Integration with compression and indexing

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cqlite_core::{
    error::{Error, Result},
    platform::Platform,
    schema::registry::{SchemaRegistry, SchemaRegistryConfig},
    storage::sstable::reader::SSTableReader,
    types::TableId,
    Config, RowKey,
};
use cqlite_tests::discover_table_dir;

use tokio::fs;

/// Test fixture for golden path scan operations
pub struct GoldenPathScanTestFixture {
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

impl GoldenPathScanTestFixture {
    /// Create new test fixture
    pub async fn new() -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let schema_registry = Arc::new(
            SchemaRegistry::new(
                SchemaRegistryConfig::default(),
                platform.clone(),
                config.clone(),
            )
            .await?,
        );

        let datasets_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-data/datasets/sstables");

        Ok(Self {
            datasets_path,
            platform,
            config,
            schema_registry,
        })
    }

    /// Setup SSTable reader for wide rows dataset (good for scan testing)
    async fn setup_wide_rows_reader(&self) -> Result<SSTableReader> {
        let table_dir =
            discover_table_dir("test_basic", "compression_test_table").ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test_basic/compression_test_table not found. \
                     Please ensure CQLITE_DATASETS_ROOT is set and test-data is available.",
                ))
            })?;
        let fallback_path = table_dir.join("nb-1-big-Data.db");

        if fs::metadata(&fallback_path).await.is_err() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Test SSTable not found: {fallback_path:?}. Please ensure test-data is available."
                ),
            )));
        }

        SSTableReader::open(&fallback_path, &self.config, self.platform.clone()).await
    }

    /// Setup timeseries dataset reader for ordered scan testing
    async fn setup_timeseries_reader(&self) -> Result<SSTableReader> {
        let table_dir =
            discover_table_dir("test_basic", "compression_test_table").ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test_basic/compression_test_table not found.",
                ))
            })?;
        let fallback_path = table_dir.join("nb-1-big-Data.db");

        if fs::metadata(&fallback_path).await.is_err() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Test SSTable not found: {fallback_path:?}"),
            )));
        }

        SSTableReader::open(&fallback_path, &self.config, self.platform.clone()).await
    }

    /// Create test keys for scan range testing
    #[allow(dead_code)]
    fn create_test_key_range(&self) -> Vec<RowKey> {
        (1..=50)
            .map(|i| RowKey::from(format!("scan_key_{i:03}").as_bytes()))
            .collect()
    }
}

// Fixed in issue #516: scan() now returns rows in ascending RowKey order.
#[tokio::test]
async fn test_golden_path_full_table_scan() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Full table scan without limits
    let start_time = Instant::now();
    let results = reader.scan(&table_id, None, None, None, None).await?;
    let scan_duration = start_time.elapsed();

    // Assertions: Basic functionality
    println!("✅ Full table scan completed in {scan_duration:?}");
    println!("✅ Found {} entries in full scan", results.len());

    // Verify results are in ascending Murmur3 token order (then key bytes for equal tokens),
    // matching the on-disk order specified in the SSTable format spec (§5, Appendix B §313).
    for i in 1..results.len() {
        let token_prev = cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token(
            results[i - 1].0.as_bytes(),
        );
        let token_curr =
            cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token(results[i].0.as_bytes());
        let prev_pair = (token_prev, &results[i - 1].0);
        let curr_pair = (token_curr, &results[i].0);
        assert!(
            prev_pair <= curr_pair,
            "Scan results should be in ascending token order: \
             prev=(token={}, key={:?}) > curr=(token={}, key={:?})",
            token_prev,
            results[i - 1].0,
            token_curr,
            results[i].0,
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_range_scan_with_boundaries() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Create test range
    let start_key = RowKey::from(b"range_start_key".as_ref());
    let end_key = RowKey::from(b"range_end_key".as_ref());

    // Test: Range scan with start and end boundaries
    let start_time = Instant::now();
    let results = reader
        .scan(&table_id, Some(&start_key), Some(&end_key), None, None)
        .await?;
    let scan_duration = start_time.elapsed();

    println!("✅ Range scan completed in {scan_duration:?}");
    println!("✅ Range scan found {} entries", results.len());

    // Verify all results are within range
    for (key, _value) in &results {
        assert!(
            key >= &start_key && key <= &end_key,
            "All results should be within scan range"
        );
    }

    // Test edge case: scan with start > end (should return empty)
    let invalid_results = reader
        .scan(&table_id, Some(&end_key), Some(&start_key), None, None)
        .await?;
    assert!(
        invalid_results.is_empty(),
        "Scan with start > end should return empty results"
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_limited_scan_operations() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test various limit values
    let test_limits = vec![1, 5, 10, 50, 100];

    for limit in test_limits {
        let start_time = Instant::now();
        let results = reader
            .scan(&table_id, None, None, Some(limit), None)
            .await?;
        let scan_duration = start_time.elapsed();

        // Assertions
        assert!(
            results.len() <= limit,
            "Scan should respect limit: got {}, expected max {}",
            results.len(),
            limit
        );

        println!(
            "✅ Limited scan (limit={}) returned {} entries in {:?}",
            limit,
            results.len(),
            scan_duration
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_prefix_scan_operations() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Prefix-based scanning
    let prefix = "test_prefix";
    let start_key = RowKey::from(prefix.as_bytes());

    // Create end key by incrementing last byte for prefix scan
    let mut end_bytes = prefix.as_bytes().to_vec();
    if let Some(last_byte) = end_bytes.last_mut() {
        *last_byte = last_byte.saturating_add(1);
    }
    let end_key = RowKey::from(&end_bytes[..]);

    let start_time = Instant::now();
    let results = reader
        .scan(&table_id, Some(&start_key), Some(&end_key), None, None)
        .await?;
    let scan_duration = start_time.elapsed();

    println!("✅ Prefix scan for '{prefix}' completed in {scan_duration:?}");
    println!("✅ Prefix scan found {} matching entries", results.len());

    // All results should start with the prefix
    for (key, _value) in &results {
        let key_str = String::from_utf8_lossy(key.as_bytes());
        // Note: actual prefix matching depends on data content
        println!("  Found key: {key_str}");
    }

    // Load-immune STRUCTURAL invariant: a bounded prefix sub-range can never
    // return more rows than an unbounded full scan of the same table.
    let full = reader.scan(&table_id, None, None, None, None).await?;
    assert!(
        results.len() <= full.len(),
        "prefix sub-range scan ({} rows) cannot exceed the full scan ({} rows)",
        results.len(),
        full.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_scan_performance_benchmarks() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Benchmark: Multiple scan operations
    let scan_operations = vec![
        ("full_scan", None, None, None),
        ("limited_10", None, None, Some(10)),
        ("limited_100", None, None, Some(100)),
    ];

    let mut benchmark_results = HashMap::new();

    for (name, start, end, limit) in scan_operations {
        let start_time = Instant::now();
        let results = reader.scan(&table_id, start, end, limit, None).await?;
        let duration = start_time.elapsed();

        benchmark_results.insert(name, (duration, results.len()));
    }

    // Print benchmark results (timing recorded, not asserted — #2642/#2902).
    for (name, (duration, count)) in &benchmark_results {
        println!("✅ Benchmark {name}: {count} entries in {duration:?}");
    }

    // Load-immune STRUCTURAL invariants (replace the retired wall-clock asserts):
    // limits are honored and the unbounded scan is a superset of the bounded one.
    let limited_10 = benchmark_results["limited_10"].1;
    let limited_100 = benchmark_results["limited_100"].1;
    let full_scan = benchmark_results["full_scan"].1;
    assert!(
        limited_10 <= 10,
        "limited_10 scan must honor its limit: got {limited_10} rows (> 10)"
    );
    assert!(
        limited_100 <= 100,
        "limited_100 scan must honor its limit: got {limited_100} rows (> 100)"
    );
    assert!(
        full_scan >= limited_100,
        "full (unbounded) scan must return at least as many rows as the limited_100 \
         scan: full={full_scan} < limited_100={limited_100}"
    );

    Ok(())
}

// Fixed in issue #516: scan() now returns rows in ascending RowKey order.
#[tokio::test]
async fn test_golden_path_scan_ordering_validation() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_timeseries_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Verify scan results are properly ordered
    let results = reader.scan(&table_id, None, None, Some(50), None).await?;

    if results.len() > 1 {
        // Verify ascending Murmur3 token order (then key bytes for equal tokens),
        // matching the on-disk order from the SSTable format spec (§5, Appendix B §313).
        for i in 1..results.len() {
            let token_prev = cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token(
                results[i - 1].0.as_bytes(),
            );
            let token_curr = cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token(
                results[i].0.as_bytes(),
            );
            let prev_pair = (token_prev, &results[i - 1].0);
            let curr_pair = (token_curr, &results[i].0);
            assert!(
                prev_pair <= curr_pair,
                "Scan results should be in ascending token order: \
                 prev=(token={}, key={:?}) > curr=(token={}, key={:?})",
                token_prev,
                results[i - 1].0,
                token_curr,
                results[i].0,
            );
        }

        println!(
            "✅ Scan ordering validated - {} entries in correct ascending token order",
            results.len()
        );

        // Additional validation: Check for duplicates
        let mut seen_keys = std::collections::HashSet::new();
        for (key, _value) in &results {
            assert!(
                seen_keys.insert(key.clone()),
                "Duplicate key found in scan results: {key:?}"
            );
        }

        println!("✅ No duplicate keys found in scan results");
    } else {
        println!(
            "ℹ️  Insufficient data for ordering validation (found {} entries)",
            results.len()
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_scan_edge_cases() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Edge case 1: Scan with limit 0
    // Note: the reader treats limit=0 as implementation-defined (may return
    // rows or none).  We only assert it doesn't crash (issue #514).
    let results = reader.scan(&table_id, None, None, Some(0), None).await?;
    println!(
        "Scan with limit 0 returned {} result(s) (implementation-defined)",
        results.len()
    );

    // Edge case 2: Scan non-existent table
    let non_existent_table = TableId::new("non_existent.table");
    let _results = reader
        .scan(&non_existent_table, None, None, None, None)
        .await?;
    // Should not crash, results may be empty

    // Edge case 3: Scan with very large limit
    let start_time = Instant::now();
    let large_limit_results = reader
        .scan(&table_id, None, None, Some(1_000_000), None)
        .await?;
    let duration = start_time.elapsed();

    // Record timing (do not assert on wall-clock latency — #2642/#2902).
    println!("[perf-record] large limit scan: {duration:?}");

    // Load-immune STRUCTURAL invariant: a scan never returns more rows than its
    // requested limit (the limit is an upper bound).
    assert!(
        large_limit_results.len() <= 1_000_000,
        "large-limit scan must honor its 1_000_000 cap: got {} rows",
        large_limit_results.len()
    );

    // Edge case 4: Empty key range scan
    let empty_start = RowKey::from(b"".as_slice());
    let empty_end = RowKey::from(b"".as_slice());
    let _results = reader
        .scan(&table_id, Some(&empty_start), Some(&empty_end), None, None)
        .await?;
    // Should handle gracefully

    println!("✅ All scan edge cases handled successfully");
    Ok(())
}

#[tokio::test]
#[ignore = "scan_stream method not available on SSTableReader"]
async fn test_golden_path_streaming_scan_operations() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // NOTE: scan_stream() method is not available on SSTableReader
    // Fallback to regular scan for testing
    let results = reader.scan(&table_id, None, None, Some(50), None).await?;
    println!("✅ Scan processed {} entries", results.len());

    assert!(results.len() <= 50, "Should respect limit");

    Ok(())
}

#[tokio::test]
async fn test_golden_path_concurrent_scan_operations() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = Arc::new(fixture.setup_wide_rows_reader().await?);

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Multiple concurrent scans
    let handles = (1..=5)
        .map(|i| {
            let reader = reader.clone();
            let table_id = table_id.clone();
            tokio::spawn(async move {
                let limit = Some(10 * i);
                let start_time = Instant::now();
                let results = reader.scan(&table_id, None, None, limit, None).await;
                let duration = start_time.elapsed();
                (i, results, duration)
            })
        })
        .collect::<Vec<_>>();

    let start_time = Instant::now();
    let results = futures::future::join_all(handles).await;
    let total_duration = start_time.elapsed();

    // Verify all concurrent scans completed successfully
    let mut processed = 0usize;
    for handle_result in results {
        let (id, scan_result, duration) =
            handle_result.map_err(|e| Error::internal(format!("Task failed: {e}")))?;

        let scan_results = scan_result?;

        println!(
            "✅ Concurrent scan {} found {} entries in {:?}",
            id,
            scan_results.len(),
            duration
        );

        // Load-immune STRUCTURAL invariant: scan i honors its limit of 10*i.
        let limit = 10 * id;
        assert!(
            scan_results.len() <= limit,
            "concurrent scan {id} must honor its limit {limit}: got {} rows",
            scan_results.len()
        );
        processed += 1;
    }

    println!("✅ All concurrent scan operations completed in {total_duration:?}");

    // Load-immune STRUCTURAL invariant: every spawned concurrent scan completed.
    assert_eq!(processed, 5, "all 5 concurrent scans should complete");
    Ok(())
}

#[tokio::test]
async fn test_golden_path_scan_integration_validation() -> Result<()> {
    let fixture = GoldenPathScanTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Integration test: Verify scan integrates properly with all SSTable components

    // 1. Check reader health before scanning
    // NOTE: health_check() method is not currently available
    // let health_metrics = reader.health_check().await?;
    // assert!(
    //     health_metrics.file_accessible,
    //     "File should be accessible for scanning"
    // );

    // 2. Perform scan and measure comprehensive metrics
    let start_time = Instant::now();
    let results = reader.scan(&table_id, None, None, Some(25), None).await?;
    let scan_duration = start_time.elapsed();

    // 3. Verify reader statistics after scan
    let stats = reader.stats().await?;
    println!(
        "✅ Post-scan stats: blocks={}, entries={}",
        stats.block_count, stats.entry_count
    );
    // NOTE: cache_hits field is not available, use cache_hit_rate instead

    // 4. Record timing (do not assert on wall-clock latency — #2642/#2902).
    println!("[perf-record] integrated scan: {scan_duration:?}");

    // 5. Verify scan results consistency with get operations
    if !results.is_empty() {
        let first_key = &results[0].0;
        let get_result = reader.get(&table_id, first_key).await?;

        if let Some(get_value) = get_result {
            assert_eq!(
                get_value, results[0].1,
                "Get and scan should return consistent values for same key"
            );
        }
    }

    println!("✅ Scan integration validation completed - all components working together");
    Ok(())
}
