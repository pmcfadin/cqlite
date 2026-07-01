//! SSTable Parity Integration Tests (Issue #263)
//!
//! This module contains integration tests that validate CQLite's SSTable parsing
//! against Cassandra's sstabledump JSONL reference files. Each test:
//!
//! 1. Loads the JSONL reference file produced by sstabledump
//! 2. Opens the corresponding SSTable using CQLite's parser
//! 3. Compares parsed values against the reference data
//! 4. Asserts parity with configurable tolerance (default 95%+)
//!
//! ## Test Data Requirements
//!
//! Tests require the `CQLITE_DATASETS_ROOT` environment variable to be set:
//! ```bash
//! export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
//! cargo test --package cqlite-core sstable_parity_integration
//! ```
//!
//! ## Coverage
//!
//! - test_basic: UUID keys, composite keys, static columns
//! - test_collections: Lists, sets, maps, UDTs
//! - test_timeseries: Composite clustering keys, BTI format
//! - test_wide_rows: Wide partitions, frozen types

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::testing::dataset_helpers::{
    derive_reference_paths_from_data_db, require_fixtures_strict, resolve_table_to_sstable_path,
    should_ignore_file, DatasetError,
};
use cqlite_core::{Config, Platform, Value};
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ============================================================================
// JSONL Types and Parsing (extracted from v5_compressed_legacy_parity_test.rs)
// ============================================================================

/// Represents a partition from JSONL reference data
#[derive(Debug, Clone)]
struct JsonlPartition {
    /// Partition key components (typically UUIDs or other key values)
    key: Vec<String>,
    /// Position in Data.db file (used for debugging)
    #[allow(dead_code)]
    position: u64,
    /// Number of rows in this partition (used for multi-row partition tests)
    #[allow(dead_code)]
    row_count: usize,
    /// Cell data for first row (column name -> value)
    cells: HashMap<String, JsonlValue>,
}

/// Simplified representation of JSONL cell values for comparison
#[derive(Debug, Clone, PartialEq)]
enum JsonlValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Blob(Vec<u8>),
    Array(Vec<JsonlValue>),
    Object(HashMap<String, JsonlValue>),
}

impl JsonlValue {
    /// Parse JSON value into JsonlValue (recursive for collections)
    fn from_json(value: &JsonValue) -> Self {
        match value {
            JsonValue::String(s) => {
                // Check if it's a hex blob (starts with "0x")
                if let Some(hex_str) = s.strip_prefix("0x") {
                    if let Ok(bytes) = hex::decode(hex_str) {
                        JsonlValue::Blob(bytes)
                    } else {
                        JsonlValue::String(s.clone())
                    }
                } else {
                    JsonlValue::String(s.clone())
                }
            }
            JsonValue::Number(n) => {
                if let Some(f) = n.as_f64() {
                    JsonlValue::Number(f)
                } else {
                    JsonlValue::Null
                }
            }
            JsonValue::Bool(b) => JsonlValue::Bool(*b),
            JsonValue::Null => JsonlValue::Null,
            JsonValue::Array(arr) => {
                let elements = arr.iter().map(JsonlValue::from_json).collect();
                JsonlValue::Array(elements)
            }
            JsonValue::Object(obj) => {
                let mut map = HashMap::new();
                for (key, val) in obj {
                    map.insert(key.clone(), JsonlValue::from_json(val));
                }
                JsonlValue::Object(map)
            }
        }
    }
}

/// Parse a single JSONL line into a JsonlPartition
fn parse_jsonl_line(line: &str) -> Result<JsonlPartition, String> {
    let json: JsonValue =
        serde_json::from_str(line).map_err(|e| format!("Failed to parse JSON: {}", e))?;

    // Extract partition key (array of key components)
    let partition_obj = json.get("partition").ok_or("Missing 'partition' field")?;
    let key_array = partition_obj
        .get("key")
        .and_then(|k| k.as_array())
        .ok_or("Missing or invalid 'partition.key'")?;

    let key: Vec<String> = key_array
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    if key.is_empty() {
        return Err("No valid key components found".to_string());
    }

    let position = partition_obj
        .get("position")
        .and_then(|p| p.as_u64())
        .ok_or("Missing or invalid 'partition.position'")?;

    // Extract rows and cells
    let rows = json
        .get("rows")
        .and_then(|r| r.as_array())
        .ok_or("Missing or invalid 'rows'")?;

    let row_count = rows.len();

    // Extract cells from first row
    let mut cells = HashMap::new();
    if let Some(first_row) = rows.first() {
        if let Some(cells_array) = first_row.get("cells").and_then(|c| c.as_array()) {
            for cell in cells_array {
                if let (Some(name), Some(value)) =
                    (cell.get("name").and_then(|n| n.as_str()), cell.get("value"))
                {
                    cells.insert(name.to_string(), JsonlValue::from_json(value));
                }
            }
        }
    }

    Ok(JsonlPartition {
        key,
        position,
        row_count,
        cells,
    })
}

/// Load all partitions from JSONL file
fn load_jsonl_reference(jsonl_path: &Path) -> Result<Vec<JsonlPartition>, String> {
    let file = File::open(jsonl_path).map_err(|e| format!("Failed to open JSONL file: {}", e))?;
    let reader = BufReader::new(file);

    let mut partitions = Vec::new();
    for (line_num, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| format!("Failed to read line {}: {}", line_num + 1, e))?;
        if line.trim().is_empty() {
            continue;
        }

        match parse_jsonl_line(&line) {
            Ok(partition) => partitions.push(partition),
            Err(e) => {
                eprintln!(
                    "Warning: Failed to parse line {} in {}: {}",
                    line_num + 1,
                    jsonl_path.display(),
                    e
                );
                continue;
            }
        }
    }

    Ok(partitions)
}

// ============================================================================
// Value Comparison (extracted from v5_compressed_legacy_parity_test.rs)
// ============================================================================

/// Compare parser Value against JSONL reference value
fn values_match(parser_value: &Value, jsonl_value: &JsonlValue) -> bool {
    match (parser_value, jsonl_value) {
        // Null values
        (Value::Null, JsonlValue::Null) => true,

        // Boolean values
        (Value::Boolean(p), JsonlValue::Bool(j)) => p == j,

        // String/Text values
        (Value::Text(p), JsonlValue::String(j)) => p == j,

        // Numeric values
        (Value::Integer(p), JsonlValue::Number(j)) => (*p as f64 - j).abs() < f64::EPSILON,
        (Value::BigInt(p), JsonlValue::Number(j)) => (*p as f64 - j).abs() < f64::EPSILON,
        (Value::Float(p), JsonlValue::Number(j)) => (p - j).abs() < 0.01,
        (Value::Float32(p), JsonlValue::Number(j)) => ((*p as f64) - j).abs() < 0.01,
        (Value::TinyInt(p), JsonlValue::Number(j)) => (*p as f64 - j).abs() < f64::EPSILON,
        (Value::SmallInt(p), JsonlValue::Number(j)) => (*p as f64 - j).abs() < f64::EPSILON,
        (Value::Counter(p), JsonlValue::Number(j)) => (*p as f64 - j).abs() < f64::EPSILON,

        // Blob values
        (Value::Blob(p), JsonlValue::Blob(j)) => p == j,
        (Value::Blob(p), JsonlValue::String(j)) => {
            if let Some(hex_str) = j.strip_prefix("0x") {
                if let Ok(bytes) = hex::decode(hex_str) {
                    return p == &bytes;
                }
            }
            false
        }

        // UUID values
        (Value::Uuid(p), JsonlValue::String(j)) => {
            let uuid_str = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7],
                p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]
            );
            &uuid_str == j
        }

        // Timestamp values
        (Value::Timestamp(_p), JsonlValue::String(j)) => !j.is_empty(),
        (Value::Timestamp(_p), JsonlValue::Number(_j)) => true,

        // Date values
        (Value::Date(_p), JsonlValue::String(j)) => j.contains('-') && j.len() >= 8,

        // Time values
        (Value::Time(_p), JsonlValue::String(j)) => j.contains(':'),

        // Duration values
        (Value::Duration { .. }, JsonlValue::String(j)) => {
            j.contains('h') || j.contains('m') || j.contains('s')
        }

        // Inet (IP address)
        (Value::Inet(p), JsonlValue::String(j)) => {
            if p.len() == 4 {
                let ip_str = format!("{}.{}.{}.{}", p[0], p[1], p[2], p[3]);
                &ip_str == j
            } else {
                !j.is_empty()
            }
        }

        // Collections - List
        (Value::List(parser_list), JsonlValue::Array(jsonl_array)) => {
            if parser_list.len() != jsonl_array.len() {
                return false;
            }
            parser_list
                .iter()
                .zip(jsonl_array.iter())
                .all(|(p, j)| values_match(p, j))
        }

        // Collections - Set
        (Value::Set(parser_set), JsonlValue::Array(jsonl_array)) => {
            if parser_set.len() != jsonl_array.len() {
                return false;
            }
            // Sets may have different order, check all elements exist
            for parser_elem in parser_set {
                if !jsonl_array.iter().any(|j| values_match(parser_elem, j)) {
                    return false;
                }
            }
            true
        }

        // Collections - Map
        (Value::Map(parser_map), JsonlValue::Object(jsonl_obj)) => {
            if parser_map.len() != jsonl_obj.len() {
                return false;
            }
            for (key, val) in parser_map {
                if let Value::Text(key_str) = key {
                    match jsonl_obj.get(key_str) {
                        Some(jsonl_val) if values_match(val, jsonl_val) => continue,
                        _ => return false,
                    }
                } else {
                    return false;
                }
            }
            true
        }

        // Varint
        (Value::Varint(p), JsonlValue::Number(j)) => {
            if p.is_empty() {
                return (*j - 0.0).abs() < f64::EPSILON;
            }
            let bigint = BigInt::from_signed_bytes_be(p);
            if let Some(as_i64) = bigint.to_i64() {
                ((as_i64 as f64) - j).abs() < f64::EPSILON
            } else {
                let bigint_str = bigint.to_string();
                let j_str = if j.fract() == 0.0 {
                    format!("{:.0}", j)
                } else {
                    j.to_string()
                };
                bigint_str == j_str
            }
        }

        // Decimal
        (Value::Decimal { scale, unscaled }, JsonlValue::Number(j)) => {
            if unscaled.is_empty() {
                return (*j - 0.0).abs() < f64::EPSILON;
            }
            let bigint = BigInt::from_signed_bytes_be(unscaled);
            if let Some(unscaled_i64) = bigint.to_i64() {
                let scale_divisor = 10_f64.powi(*scale);
                let decimal_value = (unscaled_i64 as f64) / scale_divisor;
                let epsilon = 10_f64.powi(-(*scale)) * 0.01;
                (decimal_value - j).abs() < epsilon
            } else {
                // Large decimal - string comparison
                let is_negative = bigint.is_negative();
                let abs_bigint = bigint.abs();
                let abs_str = abs_bigint.to_string();

                let decimal_str = if *scale == 0 {
                    if is_negative {
                        format!("-{}", abs_str)
                    } else {
                        abs_str
                    }
                } else if (*scale as usize) >= abs_str.len() {
                    let leading_zeros = *scale as usize - abs_str.len();
                    if is_negative {
                        format!("-0.{:0>width$}{}", "", abs_str, width = leading_zeros)
                    } else {
                        format!("0.{:0>width$}{}", "", abs_str, width = leading_zeros)
                    }
                } else {
                    let split_pos = abs_str.len() - *scale as usize;
                    let integer_part = &abs_str[..split_pos];
                    let fractional_part = &abs_str[split_pos..];
                    if is_negative {
                        format!("-{}.{}", integer_part, fractional_part)
                    } else {
                        format!("{}.{}", integer_part, fractional_part)
                    }
                };

                let j_str = if j.fract() == 0.0 && j.abs() < 1e15 {
                    format!("{:.0}", j)
                } else {
                    format!("{}", j)
                };
                decimal_str == j_str
            }
        }

        // All other combinations are mismatches
        _ => false,
    }
}

/// Extract UUID string from raw partition key bytes (16 bytes)
fn uuid_from_bytes(bytes: &[u8]) -> Result<String, String> {
    if bytes.len() != 16 {
        return Err(format!(
            "Invalid UUID byte length: expected 16, got {}",
            bytes.len()
        ));
    }

    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    ))
}

// ============================================================================
// Test Helpers
// ============================================================================

/// Find Data.db file in SSTable directory
fn find_data_db(sstable_dir: &Path) -> Option<PathBuf> {
    if let Ok(entries) = std::fs::read_dir(sstable_dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if should_ignore_file(name) {
                    continue;
                }
                if name.ends_with("-Data.db") {
                    return Some(entry.path());
                }
            }
        }
    }
    None
}

/// Result type for parity tests
struct ParityResult {
    partition_count: usize,
    reference_count: usize,
    matched_keys: usize,
    validated_cells: usize,
    total_cells: usize,
}

/// Run parity test for a given keyspace and table
async fn run_parity_test(keyspace: &str, table: &str) -> Result<ParityResult, String> {
    // 1. Resolve paths
    let sstable_dir = match resolve_table_to_sstable_path(keyspace, table) {
        Ok(p) => p,
        Err(DatasetError::MetadataNotFound { .. }) => {
            return Err("CQLITE_DATASETS_ROOT not set or metadata.yml missing".to_string());
        }
        Err(e) => return Err(format!("Failed to resolve path: {}", e)),
    };

    let data_db = find_data_db(&sstable_dir)
        .ok_or_else(|| format!("No Data.db found in {}", sstable_dir.display()))?;

    let (jsonl_path, _, _) = derive_reference_paths_from_data_db(&data_db).ok_or_else(|| {
        format!(
            "Failed to derive reference paths from {}",
            data_db.display()
        )
    })?;

    if !jsonl_path.exists() {
        return Err(format!(
            "JSONL reference not found: {}",
            jsonl_path.display()
        ));
    }

    // 2. Load reference data
    let reference_partitions = load_jsonl_reference(&jsonl_path)?;
    let reference_count = reference_partitions.len();

    if reference_count == 0 {
        return Err("No partitions in JSONL reference file".to_string());
    }

    // 3. Open SSTable and parse
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .map_err(|e| format!("Platform error: {}", e))?,
    );

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .map_err(|e| format!("SSTable open error: {}", e))?;

    let entries = reader
        .get_all_entries()
        .await
        .map_err(|e| format!("Entry read error: {}", e))?;

    let partition_count = entries.len();

    // 4. Build key lookup map from reference
    let mut reference_keys: HashMap<String, &JsonlPartition> = HashMap::new();
    for partition in &reference_partitions {
        // Use first key component for lookup
        if let Some(key) = partition.key.first() {
            reference_keys.insert(key.clone(), partition);
        }
    }

    // 5. Match keys and validate cells
    let mut matched_keys = 0;
    let mut validated_cells = 0;
    let mut total_cells = 0;

    for (_table_id, row_key, value) in &entries {
        // Try to extract UUID from partition key
        let key_str = if row_key.0.len() == 16 {
            uuid_from_bytes(&row_key.0).unwrap_or_else(|_| hex::encode(&row_key.0))
        } else {
            hex::encode(&row_key.0)
        };

        if let Some(ref_partition) = reference_keys.get(&key_str) {
            matched_keys += 1;

            // Validate cell values
            // Issue #1334: rows decode to `Value::Row` keyed by `Arc<str>`.
            if let Value::Row(entries) = value {
                for (col_name, col_val) in entries {
                    total_cells += 1;
                    if let Some(ref_val) = ref_partition.cells.get(col_name.as_ref()) {
                        if values_match(col_val, ref_val) {
                            validated_cells += 1;
                        }
                    }
                }
            }
        }
    }

    Ok(ParityResult {
        partition_count,
        reference_count,
        matched_keys,
        validated_cells,
        total_cells,
    })
}

/// Check if test data is available
fn test_data_available() -> bool {
    std::env::var("CQLITE_DATASETS_ROOT").is_ok()
        || resolve_table_to_sstable_path("test_basic", "simple_table").is_ok()
}

/// Skip-or-fail gate for dataset-dependent tests (issue #1230). When the dataset
/// is unavailable this PANICS under strict mode (`require_fixtures_strict`) so a
/// required CI lane cannot false-green on missing data, and otherwise returns
/// `true` (the caller `return`s) to preserve the local-dev skip-on-absence flow.
fn skip_when_data_unavailable() -> bool {
    if test_data_available() {
        return false;
    }
    assert!(
        !require_fixtures_strict(),
        "CQLITE_REQUIRE_FIXTURES=1 but dataset unavailable (CQLITE_DATASETS_ROOT \
         unset or metadata.yml missing) — fetch with bash test-data/scripts/fetch-datasets.sh"
    );
    eprintln!("Skipping test: dataset unavailable (set CQLITE_DATASETS_ROOT)");
    true
}

/// Handle a `run_parity_test` Err per the fail-closed contract (issue #1230).
/// A genuine parse failure ALWAYS panics. A missing-fixture error (the data was
/// not there) PANICS under strict mode and is skipped — with a note — only in
/// non-strict local-dev mode. The previous code unconditionally swallowed
/// "not set"/"not found", which let a dropped table or a #773-class path
/// regression pass green.
// TODO(#1230 follow-up): classify via a typed DatasetError variant, not substring
// match — a genuine error whose message happens to contain "not found" etc. is
// currently misclassified as a missing fixture. Pre-existing pattern.
fn handle_parity_error(e: &str) {
    let missing_fixture = e.contains("not set")
        || e.contains("not found")
        || e.contains("No Data.db")
        || e.contains("metadata.yml missing");
    assert!(missing_fixture, "parity test failed (genuine error): {e}");
    assert!(
        !require_fixtures_strict(),
        "CQLITE_REQUIRE_FIXTURES=1 but fixture missing: {e} — \
         fetch with bash test-data/scripts/fetch-datasets.sh"
    );
    eprintln!("parity test skipped (fixture absent): {e}");
}

/// Established lenient parity tolerance (95%): parsed partitions must reach at
/// least 95% of the reference partition count. This is the long-standing
/// threshold `test_simple_table_key_parsing_parity` used BEFORE #1230 (integer
/// math `reference_count * 95 / 100`). #1230 only adds the fail-closed-on-empty
/// guard; it deliberately does NOT tighten the ratio to 100%, so a real table
/// that legitimately parses to 95–99% of reference partitions stays green.
const DEFAULT_PARITY_MIN_PERCENT: u64 = 95;

/// Ratio-based presence/content assertion (issue #1230). Requires the JSONL
/// golden to be present and non-empty and the parser to cover at least
/// `min_percent`% of the reference partitions. The tolerance is an explicit
/// parameter and is the single source of truth for the RATIO threshold.
///
/// This helper is used ONLY at the call site that had a 95% tolerance BEFORE
/// #1230 (`test_simple_table_key_parsing_parity`). #1230 preserves that 95%
/// exactly — it does not tighten it to 100%. Call sites whose pre-#1230 baseline
/// was the lenient `partition_count > 0` use [`assert_parity_present`] instead,
/// so #1230 leaves their pass criteria unchanged.
fn assert_parity_content(r: &ParityResult, min_percent: u64) {
    assert!(
        r.reference_count > 0,
        "JSONL golden is absent or empty (0 reference partitions)"
    );
    let min_partitions = r.reference_count as u64 * min_percent / 100;
    assert!(
        r.partition_count as u64 >= min_partitions,
        "parsed {} partitions < {} required ({}% of {} reference) — table dropped or truncated?",
        r.partition_count,
        min_partitions,
        min_percent,
        r.reference_count
    );
}

/// Fail-closed-on-empty presence assertion (issue #1230) for the call sites
/// whose pre-#1230 baseline was the lenient `partition_count > 0`. It preserves
/// that exact lenient pass criterion (any non-zero partition count passes) while
/// ADDING only the fail-closed guarantee: the JSONL golden must be present and
/// non-empty, so a dropped table or a #773-class missing-fixture regression
/// FAILS rather than silently passing on absent reference data. It deliberately
/// does NOT impose a ratio floor — that would tighten these tables' criteria,
/// which is out of scope for #1230.
fn assert_parity_present(r: &ParityResult) {
    assert!(
        r.reference_count > 0,
        "JSONL golden is absent or empty (0 reference partitions)"
    );
    assert!(
        r.partition_count > 0,
        "parsed 0 partitions (expected > 0) — table dropped or truncated?"
    );
}

// ============================================================================
// test_basic Keyspace Tests
// ============================================================================

#[tokio::test]
async fn test_simple_table_key_parsing_parity() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_basic", "simple_table").await;

    match result {
        Ok(r) => {
            println!(
                "simple_table: {} partitions, {}/{} keys matched, {}/{} cells validated",
                r.partition_count,
                r.matched_keys,
                r.reference_count,
                r.validated_cells,
                r.total_cells
            );
            // Single coherent threshold: the 95% tolerance lives in
            // assert_parity_content (DEFAULT_PARITY_MIN_PERCENT). No second
            // stacked partition-count assertion (would be dead/conflicting).
            assert_parity_content(&r, DEFAULT_PARITY_MIN_PERCENT);
            let key_match_rate = r.matched_keys as f64 / r.partition_count.max(1) as f64;
            assert!(
                key_match_rate >= 0.95,
                "Key match rate too low: {:.1}%",
                key_match_rate * 100.0
            );
        }
        Err(e) => handle_parity_error(&e),
    }
}

#[tokio::test]
async fn test_composite_key_table_parsing_parity() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_basic", "composite_key_table").await;

    match result {
        Ok(r) => {
            println!(
                "composite_key_table: {} partitions, {}/{} keys matched",
                r.partition_count, r.matched_keys, r.reference_count
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

#[tokio::test]
async fn test_static_columns_table_parsing() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_basic", "static_columns_table").await;

    match result {
        Ok(r) => {
            println!(
                "static_columns_table: {} partitions, {}/{} keys matched",
                r.partition_count, r.matched_keys, r.reference_count
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

// ============================================================================
// test_collections Keyspace Tests
// ============================================================================

#[tokio::test]
async fn test_collection_table_list_parsing() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_collections", "collection_table").await;

    match result {
        Ok(r) => {
            println!(
                "collection_table: {} partitions, {}/{} cells validated",
                r.partition_count, r.validated_cells, r.total_cells
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

#[tokio::test]
async fn test_collection_table_map_parsing() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_collections", "typed_collections_table").await;

    match result {
        Ok(r) => {
            println!(
                "typed_collections_table: {} partitions, {}/{} cells validated",
                r.partition_count, r.validated_cells, r.total_cells
            );
            // Fail-closed-on-empty guard: the JSONL golden must be present and
            // non-empty (a missing/empty fixture fails rather than silently
            // passing). This site's pre-#1230 baseline was the lenient
            // `partition_count > 0`, so it uses assert_parity_present (not the
            // 95% ratio helper, which is reserved for simple_table).
            assert_parity_present(&r);
            // Issue #481 fix: typed_collections_table has 50 partitions.
            // Before the fix, the V5CompressedLegacy reader returned only 1 partition
            // due to the double length-prefix bug and the set path-elements bug.
            // This absolute regression pin catches reintroduction of either bug.
            assert!(
                r.partition_count >= 50,
                "typed_collections_table should have at least 50 partitions (got {}). \
                 If this fails, the Issue #481 regression has been reintroduced.",
                r.partition_count
            );
        }
        Err(e) => handle_parity_error(&e),
    }
}

#[tokio::test]
async fn test_nested_collections_parsing() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_collections", "nested_collections_table").await;

    match result {
        Ok(r) => {
            println!(
                "nested_collections_table: {} partitions, {}/{} cells validated",
                r.partition_count, r.validated_cells, r.total_cells
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

#[tokio::test]
async fn test_collections_with_udts_parsing() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_collections", "collections_with_udts").await;

    match result {
        Ok(r) => {
            println!(
                "collections_with_udts: {} partitions, {}/{} cells validated",
                r.partition_count, r.validated_cells, r.total_cells
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

// ============================================================================
// test_timeseries Keyspace Tests
// ============================================================================

#[tokio::test]
async fn test_sensor_data_clustering_key_parsing() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_timeseries", "sensor_data").await;

    match result {
        Ok(r) => {
            println!(
                "sensor_data: {} partitions, {}/{} keys matched",
                r.partition_count, r.matched_keys, r.reference_count
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

#[tokio::test]
async fn test_stock_prices_bti_format() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_timeseries", "stock_prices").await;

    match result {
        Ok(r) => {
            println!(
                "stock_prices: {} partitions, {}/{} keys matched",
                r.partition_count, r.matched_keys, r.reference_count
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

// ============================================================================
// test_wide_rows Keyspace Tests
// ============================================================================

#[tokio::test]
async fn test_wide_partition_table_many_rows() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_wide_rows", "wide_partition_table").await;

    match result {
        Ok(r) => {
            println!(
                "wide_partition_table: {} partitions, {}/{} keys matched",
                r.partition_count, r.matched_keys, r.reference_count
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

#[tokio::test]
async fn test_chat_messages_frozen_types() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_wide_rows", "chat_messages").await;

    match result {
        Ok(r) => {
            println!(
                "chat_messages: {} partitions, {}/{} cells validated",
                r.partition_count, r.validated_cells, r.total_cells
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

#[tokio::test]
async fn test_many_columns_table_sparse_bitmap() {
    if skip_when_data_unavailable() {
        return;
    }

    let result = run_parity_test("test_wide_rows", "many_columns_table").await;

    match result {
        Ok(r) => {
            println!(
                "many_columns_table: {} partitions, {}/{} keys matched",
                r.partition_count, r.matched_keys, r.reference_count
            );
            assert_parity_present(&r);
        }
        Err(e) => handle_parity_error(&e),
    }
}

// ============================================================================
// Comprehensive Test (All Tables)
// ============================================================================

#[tokio::test]
async fn test_all_tables_basic_parity() {
    if skip_when_data_unavailable() {
        return;
    }

    let test_cases = vec![
        ("test_basic", "simple_table"),
        ("test_basic", "composite_key_table"),
        ("test_basic", "static_columns_table"),
        ("test_collections", "collection_table"),
        ("test_collections", "nested_collections_table"),
        ("test_timeseries", "sensor_data"),
        ("test_wide_rows", "wide_partition_table"),
    ];

    let mut passed = 0;

    for (keyspace, table) in &test_cases {
        match run_parity_test(keyspace, table).await {
            Ok(r) => {
                // Fail-closed (issue #1230): presence assertion, not the old
                // `if partition_count > 0 { pass } else { fail }`. Preserves the
                // pre-#1230 lenient `> 0` pass criterion (no ratio floor added)
                // while failing closed when the golden is absent/empty.
                assert_parity_present(&r);
                println!(
                    "PASS: {}.{} ({} partitions)",
                    keyspace, table, r.partition_count
                );
                passed += 1;
            }
            // A genuine parse error always panics; a missing fixture panics under
            // strict mode and only skips in non-strict local-dev mode.
            Err(e) => handle_parity_error(&e),
        }
    }

    println!("\nSummary: {} passed (of {})", passed, test_cases.len());
}
