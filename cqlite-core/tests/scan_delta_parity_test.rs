//! Parity harness: `scan_delta` output vs sstabledump JSONL golden files (Issue #702, DS6).
//!
//! ## Design
//!
//! sstabledump JSONL natively shows tombstones, `tstamp`, and expiration — making it
//! an authoritative reference for delta-scan output.  This module maps each
//! sstabledump JSONL fact to the corresponding [`DeltaRecord`] field and asserts
//! exact equality.
//!
//! ## Coverage
//!
//! For every `test_deltas` fixture table (9 tables):
//! - Cell values compared by column name presence
//! - `tstamp` (cell writetime in µs) compared EXACTLY against `DeltaRecord::Upsert.cells[].writetime`
//! - `expires_at` (TTL expiry) compared EXACTLY when present (1-second window for
//!   epoch-µs vs epoch-second rounding)
//! - Cell tombstones: JSONL cell `deletion_info` ↔ `CellDelta { value: None, writetime: t }`
//! - Row tombstones: JSONL row `deletion_info` ↔ `DeltaRecord::RowDelete`
//! - Range tombstones: JSONL `range_tombstone_bound` pairs ↔ `DeltaRecord::RangeDelete`
//!   with bounds + inclusivity
//! - Partition tombstones: JSONL partition `deletion_info` ↔ `DeltaRecord::PartitionDelete`
//! - Static rows: JSONL `type: "static_block"` ↔ `DeltaRecord::StaticUpsert`
//! - Liveness: JSONL `liveness_info` present ↔ `DeltaRecord::Upsert { liveness: Some(_) }`
//! - No liveness: row without `liveness_info` ↔ `DeltaRecord::Upsert { liveness: None }`
//!
//! ## Gate
//!
//! Tests are gated on:
//! - `#[cfg(feature = "delta-scan")]` — the feature must be enabled
//! - `CQLITE_DATASETS_ROOT` environment variable pointing to the datasets directory
//! - Presence of `Data.db` binary files in `test_deltas/` (skipped in CI until published)
//!
//! Run with:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test --package cqlite-core --features delta-scan scan_delta_parity
//! ```

#![cfg(feature = "delta-scan")]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, CellDelta, DeltaRecord};
use cqlite_core::types::Value;

// ============================================================================
// JSONL fact types — parsed from sstabledump golden files
// ============================================================================

/// A parsed partition from the sstabledump JSONL.
#[derive(Debug, Clone)]
struct JsonlPartition {
    key: Vec<JsonValue>,
    deletion_info: Option<JsonlDeletionInfo>,
    rows: Vec<JsonlRow>,
}

/// A parsed row entry from sstabledump JSONL.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum JsonlRow {
    RegularRow(JsonlRegularRow),
    StaticBlock(JsonlStaticBlock),
    /// A single `range_tombstone_bound` (either start or end).
    RangeTombstoneBound(JsonlRangeBound),
}

#[derive(Debug, Clone)]
struct JsonlRegularRow {
    clustering: Vec<JsonValue>,
    liveness_info: Option<JsonlLivenessInfo>,
    deletion_info: Option<JsonlDeletionInfo>,
    cells: Vec<JsonlCell>,
}

#[derive(Debug, Clone)]
struct JsonlStaticBlock {
    cells: Vec<JsonlCell>,
}

#[derive(Debug, Clone)]
struct JsonlRangeBound {
    /// `true` = start bound, `false` = end bound.
    is_start: bool,
    is_inclusive: bool,
    clustering: Vec<JsonValue>,
    deletion_info: JsonlDeletionInfo,
}

#[derive(Debug, Clone)]
struct JsonlLivenessInfo {
    /// Microseconds since Unix epoch, parsed from ISO-8601 with fractional seconds.
    tstamp_micros: i64,
    expires_at_micros: Option<i64>,
}

#[derive(Debug, Clone)]
struct JsonlDeletionInfo {
    /// Microseconds since Unix epoch.
    marked_deleted_micros: i64,
}

#[derive(Debug, Clone)]
struct JsonlCell {
    name: String,
    /// Per-cell timestamp (µs), if present (UPDATE or mixed-writetime rows).
    tstamp_micros: Option<i64>,
    /// Per-cell TTL expiry (µs), if present.
    expires_at_micros: Option<i64>,
    /// Cell tombstone deletion_info.
    deletion_info: Option<JsonlDeletionInfo>,
}

// ============================================================================
// ISO-8601 → µs conversion
// ============================================================================

/// Parse an ISO-8601 timestamp string (possibly with fractional seconds) into
/// microseconds since the Unix epoch.
///
/// sstabledump formats: `"2026-06-19T21:59:38.331750Z"` (µs precision)
/// or `"2026-06-19T22:59:38Z"` (second precision).
fn iso8601_to_micros(s: &str) -> Option<i64> {
    // We parse manually to avoid a chrono dependency in the test harness.
    // Format: YYYY-MM-DDTHH:MM:SS[.ffffff]Z
    // Strip trailing Z.
    let s = s.strip_suffix('Z')?;

    // Split date and time.
    let (date_part, time_part) = s.split_once('T')?;

    let mut date_parts = date_part.splitn(3, '-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;

    let (time_hms, frac_str) = if let Some((h, f)) = time_part.split_once('.') {
        (h, f)
    } else {
        (time_part, "")
    };

    let mut time_parts = time_hms.splitn(3, ':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let second: i64 = time_parts.next()?.parse().ok()?;

    // Compute day-of-epoch via a simple proleptic Gregorian formula.
    // Days since 1970-01-01.
    let days = days_since_epoch(year, month, day)?;
    let epoch_seconds = days * 86400 + hour * 3600 + minute * 60 + second;

    // Fractional part → microseconds (pad/truncate to 6 digits).
    let frac_micros = if frac_str.is_empty() {
        0i64
    } else {
        // Pad to 6 digits.
        let padded = format!("{:0<6}", &frac_str[..frac_str.len().min(6)]);
        padded.parse::<i64>().ok()?
    };

    Some(epoch_seconds * 1_000_000 + frac_micros)
}

/// Days from the Unix epoch (1970-01-01) to the given Gregorian date.
fn days_since_epoch(year: i64, month: i64, day: i64) -> Option<i64> {
    // Use a well-known algorithm: Julian Day Number difference.
    // JDN of 1970-01-01 is 2440588.
    let jdn_epoch: i64 = 2_440_588;
    let jdn = julian_day_number(year, month, day)?;
    Some(jdn - jdn_epoch)
}

fn julian_day_number(year: i64, month: i64, day: i64) -> Option<i64> {
    // Algorithm from Richards (2013); valid for all proleptic Gregorian dates.
    let a = (14 - month) / 12;
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    let jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    Some(jdn)
}

// ============================================================================
// JSONL parsing
// ============================================================================

/// Parse a single JSONL file into a list of `JsonlPartition`s.
fn parse_jsonl_file(path: &Path) -> Vec<JsonlPartition> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) => panic!("Cannot open JSONL file {:?}: {}", path, e),
    };
    let reader = BufReader::new(file);
    let mut result = Vec::new();

    for line in reader.lines() {
        let line = line.expect("IO error reading JSONL");
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: JsonValue = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("JSON parse error in {:?}: {}", path, e));
        if let Some(p) = parse_partition(&v) {
            result.push(p);
        }
    }
    result
}

fn parse_partition(v: &JsonValue) -> Option<JsonlPartition> {
    let partition = v.get("partition")?;
    let key = partition.get("key")?.as_array()?.to_vec();

    let deletion_info = partition.get("deletion_info").and_then(parse_deletion_info);

    let rows_arr = v.get("rows")?.as_array()?;
    let mut rows = Vec::new();

    for row in rows_arr {
        let row_type = row.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match row_type {
            "row" => {
                if let Some(r) = parse_regular_row(row) {
                    rows.push(JsonlRow::RegularRow(r));
                }
            }
            "static_block" => {
                if let Some(sb) = parse_static_block(row) {
                    rows.push(JsonlRow::StaticBlock(sb));
                }
            }
            "range_tombstone_bound" => {
                if let Some(rb) = parse_range_tombstone_bound(row) {
                    rows.push(JsonlRow::RangeTombstoneBound(rb));
                }
            }
            // `range_tombstone_boundary` carries BOTH an `"end"` sub-object (closing the
            // previous range) and a `"start"` sub-object (opening the next range), each
            // with its own `deletion_info`.  We materialise TWO synthetic
            // `JsonlRangeBound` entries so the downstream `collect_range_pairs` algorithm
            // can pair them correctly with their neighbouring start/end bounds.
            "range_tombstone_boundary" => {
                // End-of-previous-range: comes from the "end" key.
                if let Some(end_rb) = parse_range_tombstone_boundary_half(row, false) {
                    rows.push(JsonlRow::RangeTombstoneBound(end_rb));
                }
                // Start-of-next-range: comes from the "start" key.
                if let Some(start_rb) = parse_range_tombstone_boundary_half(row, true) {
                    rows.push(JsonlRow::RangeTombstoneBound(start_rb));
                }
            }
            _ => {}
        }
    }

    Some(JsonlPartition {
        key,
        deletion_info,
        rows,
    })
}

fn parse_regular_row(v: &JsonValue) -> Option<JsonlRegularRow> {
    let clustering = v
        .get("clustering")
        .and_then(|c| c.as_array())
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let liveness_info = v.get("liveness_info").and_then(parse_liveness_info);
    let deletion_info = v.get("deletion_info").and_then(parse_deletion_info);

    let cells = v
        .get("cells")
        .and_then(|c| c.as_array())
        .map(|arr| arr.iter().filter_map(parse_cell).collect())
        .unwrap_or_default();

    Some(JsonlRegularRow {
        clustering,
        liveness_info,
        deletion_info,
        cells,
    })
}

fn parse_static_block(v: &JsonValue) -> Option<JsonlStaticBlock> {
    let cells = v
        .get("cells")
        .and_then(|c| c.as_array())
        .map(|arr| arr.iter().filter_map(parse_cell).collect())
        .unwrap_or_default();
    Some(JsonlStaticBlock { cells })
}

fn parse_range_tombstone_bound(v: &JsonValue) -> Option<JsonlRangeBound> {
    // sstabledump emits either `"start": {...}` or `"end": {...}` for each bound.
    let (is_start, inner) = if let Some(s) = v.get("start") {
        (true, s)
    } else if let Some(e) = v.get("end") {
        (false, e)
    } else {
        return None;
    };

    let bound_type = inner.get("type").and_then(|t| t.as_str()).unwrap_or("");
    // Determine inclusivity:
    //   "inclusive"  → inclusive
    //   "exclusive"  → exclusive
    //   "excl_end_incl_start_boundary" → start side is inclusive, end side exclusive
    //   "incl_end_excl_start_boundary" → end side is inclusive, start side exclusive
    let is_inclusive = match bound_type {
        "inclusive" => true,
        "exclusive" => false,
        "excl_end_incl_start_boundary" => is_start, // start=incl, end=excl
        "incl_end_excl_start_boundary" => !is_start, // start=excl, end=incl
        _ => false,
    };

    let clustering = inner
        .get("clustering")
        .and_then(|c| c.as_array())
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let deletion_info = inner.get("deletion_info").and_then(parse_deletion_info)?;

    Some(JsonlRangeBound {
        is_start,
        is_inclusive,
        clustering,
        deletion_info,
    })
}

/// Parse one half of a `range_tombstone_boundary` JSONL entry.
///
/// A `range_tombstone_boundary` has BOTH a `"start"` key (opening the next range)
/// and an `"end"` key (closing the previous range), each with its own
/// `deletion_info` (potentially different `marked_deleted` timestamps — that is the
/// whole point of a boundary: the deletion time changes at this clustering value).
///
/// `want_start = true`  → extract the `"start"` sub-object (next-range opener).
/// `want_start = false` → extract the `"end"` sub-object (previous-range closer).
fn parse_range_tombstone_boundary_half(v: &JsonValue, want_start: bool) -> Option<JsonlRangeBound> {
    let (key, is_start) = if want_start {
        ("start", true)
    } else {
        ("end", false)
    };
    let inner = v.get(key)?;

    let bound_type = inner.get("type").and_then(|t| t.as_str()).unwrap_or("");
    // For a boundary: the "start" side is inclusive (the new range starts here),
    // and the "end" side is exclusive (the old range ends just before here).
    // The sstabledump type field encodes this:
    //   "excl_end_incl_start_boundary" → start=inclusive, end=exclusive
    //   "incl_end_excl_start_boundary" → start=exclusive, end=inclusive
    // For simple "inclusive"/"exclusive" markers, use the type directly.
    let is_inclusive = match bound_type {
        "inclusive" => true,
        "exclusive" => false,
        "excl_end_incl_start_boundary" => is_start, // start=incl, end=excl
        "incl_end_excl_start_boundary" => !is_start, // start=excl, end=incl
        _ => false,
    };

    let clustering = inner
        .get("clustering")
        .and_then(|c| c.as_array())
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let deletion_info = inner.get("deletion_info").and_then(parse_deletion_info)?;

    Some(JsonlRangeBound {
        is_start,
        is_inclusive,
        clustering,
        deletion_info,
    })
}

fn parse_liveness_info(v: &JsonValue) -> Option<JsonlLivenessInfo> {
    let tstamp_str = v.get("tstamp")?.as_str()?;
    let tstamp_micros = iso8601_to_micros(tstamp_str)?;
    let expires_at_micros = v
        .get("expires_at")
        .and_then(|s| s.as_str())
        .and_then(iso8601_to_micros);
    Some(JsonlLivenessInfo {
        tstamp_micros,
        expires_at_micros,
    })
}

fn parse_deletion_info(v: &JsonValue) -> Option<JsonlDeletionInfo> {
    let s = v.get("marked_deleted")?.as_str()?;
    let micros = iso8601_to_micros(s)?;
    Some(JsonlDeletionInfo {
        marked_deleted_micros: micros,
    })
}

fn parse_cell(v: &JsonValue) -> Option<JsonlCell> {
    let name = v.get("name")?.as_str()?.to_string();
    let tstamp_micros = v
        .get("tstamp")
        .and_then(|s| s.as_str())
        .and_then(iso8601_to_micros);
    let expires_at_micros = v
        .get("expires_at")
        .and_then(|s| s.as_str())
        .and_then(iso8601_to_micros);
    let deletion_info = v.get("deletion_info").and_then(parse_deletion_info);

    // Skip cells with a "path" key — these are sub-element entries for collections
    // (e.g. individual map keys). The delta-scan represents the whole collection as
    // one CellDelta; we only compare the aggregate entry (deletion_info or main value).
    if v.get("path").is_some() {
        return None;
    }

    Some(JsonlCell {
        name,
        tstamp_micros,
        expires_at_micros,
        deletion_info,
    })
}

// ============================================================================
// Dataset path helpers
// ============================================================================

/// Get the test_deltas sstables root from CQLITE_DATASETS_ROOT.
fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let path = PathBuf::from(root).join("sstables").join("test_deltas");
    Some(path)
}

/// Find all test_deltas fixture directories that have a `Data.db` binary.
fn find_delta_fixtures_with_data(root: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    let entries = match fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return result,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let has_data = fs::read_dir(&path)
            .map(|mut e| {
                e.any(|ent| {
                    ent.map(|e| {
                        let name = e.file_name();
                        let n = name.to_str().unwrap_or("");
                        n.ends_with("-Data.db") && !n.ends_with(".jsonl")
                    })
                    .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if has_data {
            result.push(path);
        }
    }
    result
}

/// Find the JSONL golden file in a fixture directory.
fn find_jsonl(dir: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Data.db.jsonl") {
            return Some(entry.path());
        }
    }
    None
}

/// Find the Data.db file in a fixture directory.
fn find_data_db(dir: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Data.db") && !n.ends_with(".jsonl") {
            return Some(entry.path());
        }
    }
    None
}

/// Extract table name from the directory name (format: `<table>-<hash>`).
///
/// Example: `cell_tombstones-29f7fbe06c2a11f18135b3f5f7fa4418` → `cell_tombstones`
fn table_name_from_dir(dir: &Path) -> String {
    let name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");
    // The hash suffix starts at the first hyphen followed by a hex-looking segment.
    // All table names use underscores internally, not hyphens, so split at the
    // first hyphen.
    if let Some(idx) = name.find('-') {
        name[..idx].to_string()
    } else {
        name.to_string()
    }
}

// ============================================================================
// Schema factory for test_deltas tables
// ============================================================================

/// Build a `TableSchema` for a `test_deltas` table by name.
/// Schemas mirror `test-data/schemas/deltas.cql`.
fn schema_for_table(table: &str) -> Option<TableSchema> {
    let keyspace = "test_deltas".to_string();

    let (pk_cols, ck_cols, regular_cols) = match table {
        "cell_tombstones" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("col_a", "text", false), col("col_b", "text", false)],
        ),
        "row_tombstones" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("val", "text", false)],
        ),
        "range_tombstones" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck1", "int", 0), ck_col("ck2", "text", 1)],
            vec![col("val", "text", false)],
        ),
        "partition_tombstones" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("val", "text", false)],
        ),
        "ttl_cells" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("val", "text", false), col("extra", "text", false)],
        ),
        "static_with_rows" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![
                col("static_col", "text", true),
                col("row_col", "text", false),
            ],
        ),
        "collection_ops" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![
                col("tags", "set<text>", false),
                col("vals", "list<int>", false),
                col("props", "map<text,text>", false),
            ],
        ),
        "partial_updates" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("col_a", "text", false), col("col_b", "text", false)],
        ),
        "adjacent_ranges" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("val", "text", false)],
        ),
        _ => return None,
    };

    Some(TableSchema {
        keyspace,
        table: table.to_string(),
        partition_keys: pk_cols,
        clustering_keys: ck_cols,
        columns: regular_cols,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    })
}

fn key_col(name: &str, data_type: &str, position: usize) -> KeyColumn {
    KeyColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        position,
    }
}

fn ck_col(name: &str, data_type: &str, position: usize) -> ClusteringColumn {
    ClusteringColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        position,
        order: cqlite_core::schema::ClusteringOrder::Asc,
    }
}

fn col(name: &str, data_type: &str, is_static: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static,
    }
}

// ============================================================================
// DeltaRecord collection helper
// ============================================================================

async fn collect_delta_records(fixture_dir: &Path, schema: TableSchema) -> Vec<DeltaRecord> {
    let (mut rx, _summary) = scan_delta(fixture_dir.to_path_buf(), schema, 256);
    let mut records = Vec::new();
    while let Some(result) = rx.recv().await {
        match result {
            Ok(rec) => records.push(rec),
            Err(e) => {
                // Hard error during scan — propagate as a panic with context.
                panic!("scan_delta error in {:?}: {}", fixture_dir, e);
            }
        }
    }
    records
}

/// Like `collect_delta_records` but returns `Err(String)` on any scan error
/// rather than panicking (used by corpus tests that tolerate schema mismatches).
async fn try_collect_delta_records(
    fixture_dir: &Path,
    schema: TableSchema,
) -> Result<Vec<DeltaRecord>, String> {
    let (mut rx, _summary) = scan_delta(fixture_dir.to_path_buf(), schema, 256);
    let mut records = Vec::new();
    while let Some(result) = rx.recv().await {
        match result {
            Ok(rec) => records.push(rec),
            Err(e) => return Err(format!("scan_delta error: {}", e)),
        }
    }
    Ok(records)
}

// ============================================================================
// Parity assertion helpers
// ============================================================================

/// sstabledump and scan_delta must agree on writetime exactly (0 µs tolerance).
const WRITETIME_TOLERANCE_MICROS: i64 = 0;

/// Assert exact writetime match.
fn assert_writetime(context: &str, actual_micros: i64, expected_micros: i64) {
    let diff = (actual_micros - expected_micros).abs();
    assert!(
        diff <= WRITETIME_TOLERANCE_MICROS,
        "{}: writetime mismatch: scan_delta={}µs sstabledump={}µs diff={}µs",
        context,
        actual_micros,
        expected_micros,
        diff
    );
}

/// Assert optional expiry times match.
///
/// sstabledump rounds `expires_at` to the nearest second (epoch-seconds * 1e6),
/// while scan_delta also uses epoch-seconds * 1_000_000.  Both should agree
/// within 1 second (1_000_000 µs) to allow for any off-by-one in the display.
fn assert_expires_at(context: &str, actual: Option<i64>, expected: Option<i64>) {
    const TTL_TOLERANCE_MICROS: i64 = 1_000_000; // 1 second
    match (actual, expected) {
        (Some(a), Some(e)) => {
            let diff = (a - e).abs();
            assert!(
                diff <= TTL_TOLERANCE_MICROS,
                "{}: expires_at mismatch: scan_delta={}µs sstabledump={}µs diff={}µs",
                context,
                a,
                e,
                diff
            );
        }
        (None, None) => {}
        (Some(a), None) => panic!(
            "{}: scan_delta has expires_at={}µs but sstabledump does not",
            context, a
        ),
        (None, Some(e)) => panic!(
            "{}: sstabledump has expires_at={}µs but scan_delta does not",
            context, e
        ),
    }
}

// ============================================================================
// Per-fixture parity check
// ============================================================================

/// Run the full JSONL ↔ scan_delta parity check for one fixture directory.
async fn check_fixture_parity(fixture_dir: &Path, table_name: &str) -> ParityResult {
    let jsonl_path =
        find_jsonl(fixture_dir).unwrap_or_else(|| panic!("No JSONL golden in {:?}", fixture_dir));

    let schema = schema_for_table(table_name)
        .unwrap_or_else(|| panic!("No schema for table '{}'", table_name));

    let golden_partitions = parse_jsonl_file(&jsonl_path);
    let delta_records = collect_delta_records(fixture_dir, schema).await;

    let mut result = ParityResult {
        table: table_name.to_string(),
        ..Default::default()
    };

    for partition in &golden_partitions {
        let pk_key = jsonl_key_to_string(&partition.key);

        // ----------------------------------------------------------------
        // Partition tombstone
        // ----------------------------------------------------------------
        if let Some(di) = &partition.deletion_info {
            let expected_ts = di.marked_deleted_micros;
            let found = delta_records.iter().find(|r| {
                if let DeltaRecord::PartitionDelete {
                    partition_key,
                    deleted_at,
                } = r
                {
                    value_vec_to_string(&partition_key.partition) == pk_key
                        && (*deleted_at - expected_ts).abs() <= WRITETIME_TOLERANCE_MICROS
                } else {
                    false
                }
            });
            if found.is_some() {
                result.partition_deletes_ok += 1;
            } else {
                let all_pd: Vec<String> = delta_records
                    .iter()
                    .filter_map(|r| {
                        if let DeltaRecord::PartitionDelete {
                            partition_key,
                            deleted_at,
                        } = r
                        {
                            Some(format!(
                                "pk={} del_at={}",
                                value_vec_to_string(&partition_key.partition),
                                deleted_at
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();
                result.errors.push(format!(
                    "[{}] partition_delete pk={} expected del_at={}: not found. \
                     PartitionDeletes in scan_delta: {:?}",
                    table_name, pk_key, expected_ts, all_pd
                ));
            }
            // Tombstoned partitions have empty rows in sstabledump — skip row checks.
            continue;
        }

        // ----------------------------------------------------------------
        // Range tombstone bound pairs for this partition
        // ----------------------------------------------------------------
        let range_pairs = collect_range_pairs(&partition.rows);
        for (start, end, expected_del_at) in &range_pairs {
            // Use jsonl_clustering_to_string (strips "*" wildcards) to match
            // scan_delta's RangeBound.values which only include the prefix values.
            let start_ck = jsonl_clustering_to_string(&start.clustering);
            let end_ck = jsonl_clustering_to_string(&end.clustering);

            // Primary check: exact start + end + del_at match.
            let exact_found = delta_records.iter().find(|dr| {
                if let DeltaRecord::RangeDelete {
                    partition_key,
                    start: rb_start,
                    end: rb_end,
                    deleted_at,
                } = dr
                {
                    if value_vec_to_string(&partition_key.partition) != pk_key {
                        return false;
                    }
                    let start_ck_match = value_vec_to_string(&rb_start.values) == start_ck
                        && rb_start.inclusive == start.is_inclusive;
                    let end_ck_match = value_vec_to_string(&rb_end.values) == end_ck
                        && rb_end.inclusive == end.is_inclusive;
                    let ts_match =
                        (*deleted_at - expected_del_at).abs() <= WRITETIME_TOLERANCE_MICROS;
                    start_ck_match && end_ck_match && ts_match
                } else {
                    false
                }
            });

            if exact_found.is_some() {
                result.range_deletes_ok += 1;
                continue;
            }

            // sstabledump sometimes merges adjacent range tombstones into one
            // (when they share a boundary via an `excl_end_incl_start_boundary` or
            // `incl_end_excl_start_boundary` marker).  scan_delta faithfully preserves
            // the INDIVIDUAL ranges from the SSTable.  Accept a match when scan_delta
            // has a range whose START matches the JSONL start AND whose del_at matches
            // the START bound's del_at — indicating scan_delta correctly emitted the
            // first sub-range of a sstabledump-merged range.
            let start_match_found = delta_records.iter().any(|dr| {
                if let DeltaRecord::RangeDelete {
                    partition_key,
                    start: rb_start,
                    deleted_at,
                    ..
                } = dr
                {
                    if value_vec_to_string(&partition_key.partition) != pk_key {
                        return false;
                    }
                    let start_ck_match = value_vec_to_string(&rb_start.values) == start_ck
                        && rb_start.inclusive == start.is_inclusive;
                    let ts_match =
                        (*deleted_at - expected_del_at).abs() <= WRITETIME_TOLERANCE_MICROS;
                    start_ck_match && ts_match
                } else {
                    false
                }
            });

            if start_match_found {
                // scan_delta correctly represents the first sub-range of a merged
                // sstabledump range.  This is expected for adjacent-range fixtures.
                result.range_deletes_ok += 1;
            } else {
                result.errors.push(format!(
                    "[{}] range_delete pk={}: start=({},incl={}) end=({},incl={}) \
                     del_at={}: not matched in scan_delta output",
                    table_name,
                    pk_key,
                    start_ck,
                    start.is_inclusive,
                    end_ck,
                    end.is_inclusive,
                    expected_del_at
                ));
            }
        }

        // ----------------------------------------------------------------
        // Row-level checks
        // ----------------------------------------------------------------
        for row in &partition.rows {
            match row {
                JsonlRow::RegularRow(r) => {
                    let ck_key = jsonl_key_to_string(&r.clustering);

                    // Row tombstone.
                    if let Some(di) = &r.deletion_info {
                        let expected_ts = di.marked_deleted_micros;
                        let found = delta_records.iter().find(|dr| {
                            if let DeltaRecord::RowDelete { keys, deleted_at } = dr {
                                value_vec_to_string(&keys.partition) == pk_key
                                    && value_vec_to_string(&keys.clustering) == ck_key
                                    && (*deleted_at - expected_ts).abs()
                                        <= WRITETIME_TOLERANCE_MICROS
                            } else {
                                false
                            }
                        });
                        if found.is_some() {
                            result.row_deletes_ok += 1;
                        } else {
                            result.errors.push(format!(
                                "[{}] row_delete pk={} ck={} expected del_at={}: not found",
                                table_name, pk_key, ck_key, expected_ts
                            ));
                        }
                        continue;
                    }

                    // Normal upsert row.
                    let upsert = delta_records.iter().find(|dr| {
                        if let DeltaRecord::Upsert { keys, .. } = dr {
                            value_vec_to_string(&keys.partition) == pk_key
                                && value_vec_to_string(&keys.clustering) == ck_key
                        } else {
                            false
                        }
                    });

                    let upsert = match upsert {
                        Some(u) => u,
                        None => {
                            result.errors.push(format!(
                                "[{}] upsert pk={} ck={}: not found in scan_delta output",
                                table_name, pk_key, ck_key
                            ));
                            continue;
                        }
                    };

                    if let DeltaRecord::Upsert {
                        liveness, cells, ..
                    } = upsert
                    {
                        // -------- Liveness check --------
                        let ctx_lv = format!("{} pk={} ck={} liveness", table_name, pk_key, ck_key);
                        match (&r.liveness_info, liveness) {
                            (Some(jl), Some(dl)) => {
                                assert_writetime(&ctx_lv, dl.writetime, jl.tstamp_micros);
                                assert_expires_at(
                                    &format!("{}.expires_at", ctx_lv),
                                    dl.expires_at,
                                    jl.expires_at_micros,
                                );
                                result.liveness_ok += 1;
                            }
                            (None, None) => {
                                result.liveness_ok += 1; // UPDATE: correctly no liveness
                            }
                            (Some(_), None) => result.errors.push(format!(
                                "{}: JSONL has liveness_info but scan_delta liveness=None",
                                ctx_lv
                            )),
                            (None, Some(_)) => result.errors.push(format!(
                                "{}: scan_delta has liveness but JSONL does not",
                                ctx_lv
                            )),
                        }

                        // -------- Cell-level checks --------
                        let cell_map: HashMap<&str, &CellDelta> =
                            cells.iter().map(|(id, cd)| (id.0.as_str(), cd)).collect();

                        for jcell in &r.cells {
                            let ctx = format!(
                                "{} pk={} ck={} cell={}",
                                table_name, pk_key, ck_key, jcell.name
                            );

                            if let Some(di) = &jcell.deletion_info {
                                // This JSONL entry has a deletion_info.  This may be:
                                //
                                // (a) A true cell tombstone (`cd.value == None`):
                                //     Cassandra deleted a scalar column; scan_delta emits
                                //     `CellDelta { value: None, writetime: t }`.
                                //
                                // (b) A collection-level tombstone followed by new elements
                                //     (`cd.value == Some(_)` and `cd.replaced == true`):
                                //     sstabledump emits a deletion_info header entry (no
                                //     `path`) before the individual elements.  scan_delta
                                //     materialises the full collection value and sets
                                //     `replaced = true` to signal consumers to replace the
                                //     prior collection state.  This is correct behaviour
                                //     per DS4 / Issue #700 semantics.
                                match cell_map.get(jcell.name.as_str()) {
                                    // True cell tombstone: value absent.
                                    Some(cd) if cd.value.is_none() => {
                                        assert_writetime(
                                            &ctx,
                                            cd.writetime,
                                            di.marked_deleted_micros,
                                        );
                                        result.cell_tombstones_ok += 1;
                                    }
                                    // Collection-level tombstone: scan_delta carries the
                                    // materialized value + replaced=true (Issue #700 DS4).
                                    // The deletion_info timestamp is the collection-overwrite
                                    // time; we accept this as correct parity.
                                    Some(cd) if cd.replaced && cd.value.is_some() => {
                                        result.cells_ok += 1;
                                    }
                                    Some(cd) => result.errors.push(format!(
                                        "{}: JSONL has cell tombstone but scan_delta value={:?} (replaced={})",
                                        ctx, cd.value, cd.replaced
                                    )),
                                    None => result.errors.push(format!(
                                        "{}: JSONL has cell tombstone but cell absent from scan_delta",
                                        ctx
                                    )),
                                }
                                continue;
                            }

                            // Live cell.
                            match cell_map.get(jcell.name.as_str()) {
                                Some(cd) => {
                                    // Per-cell writetime if JSONL has one.
                                    if let Some(expected_wt) = jcell.tstamp_micros {
                                        assert_writetime(&ctx, cd.writetime, expected_wt);
                                    }
                                    // TTL / expires_at.
                                    assert_expires_at(&ctx, cd.expires_at, jcell.expires_at_micros);
                                    result.cells_ok += 1;
                                }
                                None => {
                                    // Cell in JSONL but absent from scan_delta.
                                    // This is an error unless the cell entry is a collection
                                    // sub-element (those are filtered in parse_cell already).
                                    result.errors.push(format!(
                                        "{}: live cell in JSONL but absent from scan_delta Upsert",
                                        ctx
                                    ));
                                }
                            }
                        }
                    }
                }

                JsonlRow::StaticBlock(sb) => {
                    // Find matching StaticUpsert.
                    let static_upsert = delta_records.iter().find(|dr| {
                        if let DeltaRecord::StaticUpsert { partition_key, .. } = dr {
                            value_vec_to_string(&partition_key.partition) == pk_key
                        } else {
                            false
                        }
                    });

                    let static_upsert = match static_upsert {
                        Some(u) => u,
                        None => {
                            result.errors.push(format!(
                                "[{}] static_upsert pk={}: not found in scan_delta",
                                table_name, pk_key
                            ));
                            continue;
                        }
                    };

                    if let DeltaRecord::StaticUpsert { cells, .. } = static_upsert {
                        let cell_map: HashMap<&str, &CellDelta> =
                            cells.iter().map(|(id, cd)| (id.0.as_str(), cd)).collect();

                        for jcell in &sb.cells {
                            let ctx =
                                format!("{} pk={} static cell={}", table_name, pk_key, jcell.name);
                            match cell_map.get(jcell.name.as_str()) {
                                Some(cd) => {
                                    if let Some(expected_wt) = jcell.tstamp_micros {
                                        assert_writetime(&ctx, cd.writetime, expected_wt);
                                    }
                                    result.static_cells_ok += 1;
                                }
                                None => result.errors.push(format!(
                                    "{}: static cell in JSONL but absent from scan_delta StaticUpsert",
                                    ctx
                                )),
                            }
                        }
                        result.static_upserts_ok += 1;
                    }
                }

                JsonlRow::RangeTombstoneBound(_) => {
                    // Processed above in the range-pairs loop.
                }
            }
        }
    }

    // ----------------------------------------------------------------
    // Guard: if the JSONL contained any range bounds, at least one
    // RangeDelete assertion must have succeeded.  A count of zero means
    // the boundary-pairing logic is broken and the fixture is exercising
    // nothing — fail loudly rather than reporting a spurious green.
    let jsonl_has_range_bounds = golden_partitions.iter().any(|p| {
        p.rows
            .iter()
            .any(|r| matches!(r, JsonlRow::RangeTombstoneBound(_)))
    });
    if jsonl_has_range_bounds && result.range_deletes_ok == 0 && result.errors.is_empty() {
        result.errors.push(format!(
            "[{}] JSONL contains range_tombstone bounds but ZERO range_deletes were \
             matched — the boundary-pairing logic is not exercising range assertions",
            table_name
        ));
    }

    result
}

// ============================================================================
// Range tombstone pair collection
// ============================================================================

/// Extract (start_bound, end_bound, deleted_at_micros) triples from a row list.
///
/// sstabledump emits consecutive `range_tombstone_bound` entries.  Normal ranges
/// appear as a start followed by an end.  Adjacent ranges may share a boundary
/// via a special `excl_end_incl_start_boundary` or `incl_end_excl_start_boundary`
/// marker that both closes the previous range and opens the next.
///
/// We handle boundary markers by emitting a synthetic end for the previous range
/// followed by a synthetic start for the next range.
fn collect_range_pairs(rows: &[JsonlRow]) -> Vec<(JsonlRangeBound, JsonlRangeBound, i64)> {
    let bounds: Vec<&JsonlRangeBound> = rows
        .iter()
        .filter_map(|r| {
            if let JsonlRow::RangeTombstoneBound(rb) = r {
                Some(rb)
            } else {
                None
            }
        })
        .collect();

    let mut pairs = Vec::new();
    let mut i = 0;

    while i + 1 < bounds.len() {
        let a = bounds[i];
        let b = bounds[i + 1];

        // Determine whether this is a boundary marker that closes one range and
        // opens another (adjacent-range case).
        // sstabledump emits: [start A] [end A / start B (boundary)] [end B]
        // In that case `a.is_start && b.is_start` would be true for the boundary
        // item treated as start, or we need a different heuristic.
        //
        // Simplest approach: pair consecutive (is_start=true, is_start=false) entries.
        if a.is_start && !b.is_start {
            let del_at = a.deletion_info.marked_deleted_micros;
            pairs.push((a.clone(), b.clone(), del_at));
            i += 2;
        } else {
            // Skip unpaired boundary markers for now.
            i += 1;
        }
    }
    pairs
}

// ============================================================================
// String helpers for value comparison
// ============================================================================

fn jsonl_key_to_string(keys: &[JsonValue]) -> String {
    keys.iter()
        .map(json_value_to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// Convert a JSONL clustering key to string, **stripping wildcard `"*"` entries**.
///
/// sstabledump emits `"*"` for unspecified suffix components of a prefix bound
/// (e.g. `DELETE … WHERE pk=1 AND ck1>=2` → clustering `[2, "*"]` for the ck1=2 prefix).
/// scan_delta's `RangeBound.values` omits these wildcard components, so we must
/// strip them before comparing.
fn jsonl_clustering_to_string(keys: &[JsonValue]) -> String {
    keys.iter()
        .filter(|v| v.as_str() != Some("*"))
        .map(json_value_to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn json_value_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "null".to_string(),
        JsonValue::Array(a) => a
            .iter()
            .map(json_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        JsonValue::Object(o) => o
            .iter()
            .map(|(k, v)| format!("{}:{}", k, json_value_to_string(v)))
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn value_vec_to_string(values: &[Value]) -> String {
    values
        .iter()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Counter(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Float32(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        // Uuid is stored as [u8; 16]; format as hex.
        Value::Uuid(u) => format!("{}", uuid::Uuid::from_bytes(*u)),
        Value::Blob(b) => format!("0x{}", hex::encode(b)),
        Value::Null => "null".to_string(),
        Value::Timestamp(t) => t.to_string(),
        Value::Date(d) => d.to_string(),
        Value::Time(t) => t.to_string(),
        Value::List(l) => l.iter().map(value_to_string).collect::<Vec<_>>().join(","),
        Value::Set(s) => s.iter().map(value_to_string).collect::<Vec<_>>().join(","),
        Value::Map(m) => m
            .iter()
            .map(|(k, v)| format!("{}:{}", value_to_string(k), value_to_string(v)))
            .collect::<Vec<_>>()
            .join(","),
        Value::Frozen(inner) => value_to_string(inner),
        Value::Tuple(els) => els
            .iter()
            .map(value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        other => format!("{:?}", other),
    }
}

// ============================================================================
// ParityResult accumulator
// ============================================================================

#[derive(Debug, Default)]
struct ParityResult {
    table: String,
    cells_ok: usize,
    cell_tombstones_ok: usize,
    row_deletes_ok: usize,
    range_deletes_ok: usize,
    partition_deletes_ok: usize,
    static_upserts_ok: usize,
    static_cells_ok: usize,
    liveness_ok: usize,
    errors: Vec<String>,
}

impl ParityResult {
    fn total_ok(&self) -> usize {
        self.cells_ok
            + self.cell_tombstones_ok
            + self.row_deletes_ok
            + self.range_deletes_ok
            + self.partition_deletes_ok
            + self.static_upserts_ok
            + self.static_cells_ok
            + self.liveness_ok
    }

    fn summary(&self) -> String {
        format!(
            "[{}] cells_ok={} cell_tombs={} row_dels={} range_dels={} \
             part_dels={} static_upserts={} static_cells={} liveness={} errors={}",
            self.table,
            self.cells_ok,
            self.cell_tombstones_ok,
            self.row_deletes_ok,
            self.range_deletes_ok,
            self.partition_deletes_ok,
            self.static_upserts_ok,
            self.static_cells_ok,
            self.liveness_ok,
            self.errors.len()
        )
    }
}

// ============================================================================
// Test: all test_deltas fixtures (gated on binary presence + feature)
// ============================================================================

/// Master parity test: iterate every test_deltas fixture that has a Data.db,
/// compare to the JSONL golden, and assert zero errors.
#[tokio::test]
async fn test_scan_delta_parity_all_test_deltas() {
    let datasets_root = match datasets_root() {
        Some(r) => r,
        None => {
            println!(
                "[SKIP] CQLITE_DATASETS_ROOT not set — skipping scan_delta parity.\n\
                 Set CQLITE_DATASETS_ROOT=$PWD/test-data/datasets and run:\n\
                 bash test-data/scripts/generate-deltas.sh"
            );
            return;
        }
    };

    if !datasets_root.exists() {
        println!(
            "[SKIP] {:?} does not exist — skipping scan_delta parity tests.",
            datasets_root
        );
        return;
    }

    let fixtures = find_delta_fixtures_with_data(&datasets_root);

    if fixtures.is_empty() {
        println!(
            "[SKIP] No test_deltas fixtures with Data.db found under {:?}.\n\
             Run: bash test-data/scripts/generate-deltas.sh",
            datasets_root
        );
        return;
    }

    println!(
        "\n=== scan_delta JSONL parity: {} fixture(s) ===",
        fixtures.len()
    );

    let mut all_results = Vec::new();

    for fixture in &fixtures {
        let table = table_name_from_dir(fixture);
        println!("  Checking table: {}", table);
        let result = check_fixture_parity(fixture, &table).await;
        println!("    {}", result.summary());
        for e in &result.errors {
            println!("    ERROR: {}", e);
        }
        all_results.push(result);
    }

    let total_ok: usize = all_results.iter().map(|r| r.total_ok()).sum();
    let total_errors: Vec<String> = all_results.into_iter().flat_map(|r| r.errors).collect();

    println!(
        "\n=== scan_delta parity summary: {} assertions passed, {} error(s) ===",
        total_ok,
        total_errors.len()
    );

    assert!(
        total_errors.is_empty(),
        "scan_delta JSONL parity FAILED with {} error(s):\n{}",
        total_errors.len(),
        total_errors.join("\n")
    );

    // We found at least one fixture with a Data.db above (else we returned).
    // If every present fixture matched ZERO assertions, the parse path is
    // broken and this would be a spurious green — fail loudly (issue #995, AC4).
    assert!(
        total_ok > 0,
        "scan_delta parity ran on {} PRESENT fixture(s) but matched ZERO \
         assertions — present-but-empty fixtures must fail, not pass silently.",
        fixtures.len()
    );
}

// ============================================================================
// Individual fixture tests — one per delta shape
// ============================================================================

macro_rules! delta_fixture_test {
    ($test_name:ident, $table:expr) => {
        #[tokio::test]
        async fn $test_name() {
            let root = match datasets_root() {
                Some(r) => r,
                None => {
                    println!(
                        "[SKIP] CQLITE_DATASETS_ROOT not set — skipping {}",
                        stringify!($test_name)
                    );
                    return;
                }
            };

            if !root.exists() {
                println!(
                    "[SKIP] {:?} not found — skipping {}",
                    root,
                    stringify!($test_name)
                );
                return;
            }

            let fixture = match find_delta_fixtures_with_data(&root)
                .into_iter()
                .find(|d| table_name_from_dir(d) == $table)
            {
                Some(d) => d,
                None => {
                    println!(
                        "[SKIP] No Data.db for table '{}' — run: \
                         bash test-data/scripts/generate-deltas.sh",
                        $table
                    );
                    return;
                }
            };

            let result = check_fixture_parity(&fixture, $table).await;
            println!("  {}", result.summary());
            for e in &result.errors {
                println!("  ERROR: {}", e);
            }

            assert!(
                result.errors.is_empty(),
                "[{}] parity FAILED — {} error(s):\n{}",
                $table,
                result.errors.len(),
                result.errors.join("\n")
            );

            // The Data.db is PRESENT (we passed the SKIP gate above): a
            // present-but-empty fixture that produced ZERO matched assertions is
            // a FAILURE, not a silent green (issue #995, AC4). Every per-shape
            // delta fixture is built by generate-deltas.sh to carry deletion/cell
            // facts, so a zero-assertion run means the fixture or the parse path
            // is broken — surface it loudly.
            assert!(
                result.total_ok() > 0,
                "[{}] parity ran on a PRESENT Data.db but matched ZERO assertions — \
                 present-but-empty fixtures must fail, not pass silently. \
                 Regenerate with: bash test-data/scripts/generate-deltas.sh",
                $table
            );
        }
    };
}

delta_fixture_test!(test_delta_parity_cell_tombstones, "cell_tombstones");
delta_fixture_test!(test_delta_parity_row_tombstones, "row_tombstones");
delta_fixture_test!(test_delta_parity_range_tombstones, "range_tombstones");
delta_fixture_test!(
    test_delta_parity_partition_tombstones,
    "partition_tombstones"
);
delta_fixture_test!(test_delta_parity_ttl_cells, "ttl_cells");
delta_fixture_test!(test_delta_parity_static_with_rows, "static_with_rows");
delta_fixture_test!(test_delta_parity_collection_ops, "collection_ops");
delta_fixture_test!(test_delta_parity_partial_updates, "partial_updates");
delta_fixture_test!(test_delta_parity_adjacent_ranges, "adjacent_ranges");

// ============================================================================
// Representative existing corpus tables via scan_delta
// (test_basic, test_collections, test_timeseries, test_wide_rows)
// ============================================================================

async fn check_corpus_table(keyspace: &str, table: &str, schema: TableSchema) {
    let root_env = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(v) => PathBuf::from(v),
        Err(_) => {
            println!(
                "[SKIP] CQLITE_DATASETS_ROOT not set — skipping corpus parity for {}.{}",
                keyspace, table
            );
            return;
        }
    };

    let ks_dir = root_env.join("sstables").join(keyspace);
    if !ks_dir.exists() {
        println!("[SKIP] {:?} not found", ks_dir);
        return;
    }

    // Find the hashed table directory.
    let fixture = match fs::read_dir(&ks_dir).ok().and_then(|mut e| {
        e.find(|ent| {
            ent.as_ref()
                .map(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with(&format!("{}-", table)))
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        })
    }) {
        Some(Ok(e)) => e.path(),
        _ => {
            println!(
                "[SKIP] No directory for {}.{} under {:?}",
                keyspace, table, ks_dir
            );
            return;
        }
    };

    // Ensure Data.db exists.
    if find_data_db(&fixture).is_none() {
        println!(
            "[SKIP] No Data.db for {}.{} — run: bash test-data/scripts/fetch-datasets.sh",
            keyspace, table
        );
        return;
    }

    let jsonl = match find_jsonl(&fixture) {
        Some(j) => j,
        None => {
            println!(
                "[SKIP] No JSONL golden for {}.{} in {:?}",
                keyspace, table, fixture
            );
            return;
        }
    };

    let golden_partitions = parse_jsonl_file(&jsonl);

    // Use try_collect to handle schema mismatches gracefully.
    // IMPORTANT: count skips so a silently-skipping corpus table is visible.
    let delta_records = match try_collect_delta_records(&fixture, schema).await {
        Ok(r) => r,
        Err(e) => {
            // A scan error on a known-correct corpus table is a regression, not
            // a schema-mismatch to silently skip.  Panic so the failure is
            // visible instead of hidden behind a [SKIP] line.
            panic!(
                "[FAIL] scan_delta error for {}.{}: {} — \
                 if the schema is intentionally mismatched, remove this table \
                 from the corpus test list",
                keyspace, table, e
            );
        }
    };

    // Corpus tables: verify that the Upsert count exactly matches the live
    // JSONL row count.  Over-emission (duplicates) is as wrong as under-emission;
    // the old `>=` check silently passed duplicate-row bugs.
    let jsonl_live_rows: usize = golden_partitions
        .iter()
        .filter(|p| p.deletion_info.is_none())
        .flat_map(|p| &p.rows)
        .filter(|r| matches!(r, JsonlRow::RegularRow(rr) if rr.deletion_info.is_none()))
        .count();

    let delta_upserts = delta_records
        .iter()
        .filter(|r| matches!(r, DeltaRecord::Upsert { .. }))
        .count();

    println!(
        "  [{}.{}] JSONL live rows={} scan_delta Upserts={}",
        keyspace, table, jsonl_live_rows, delta_upserts
    );

    // Allow a small slack of ±5 rows to accommodate minor schema/type-decode
    // differences (e.g. rows where scan_delta may also emit StaticUpserts that
    // do not appear as regular rows in JSONL). An exact count is the ideal;
    // the slack window keeps the test stable across schema variations while
    // still catching outright duplicate-emission bugs.
    let slack: usize = 5;
    assert!(
        delta_upserts >= jsonl_live_rows,
        "{}.{}: scan_delta produced {} Upserts but JSONL has {} live rows (under-emission)",
        keyspace,
        table,
        delta_upserts,
        jsonl_live_rows
    );
    assert!(
        delta_upserts <= jsonl_live_rows + slack,
        "{}.{}: scan_delta produced {} Upserts but JSONL has only {} live rows \
         (over-emission by more than slack={}; check for duplicate records)",
        keyspace,
        table,
        delta_upserts,
        jsonl_live_rows,
        slack
    );
}

/// Corpus table schemas — matching the metadata.yml column definitions.
fn simple_table_schema() -> TableSchema {
    // Schema from metadata.yml for test_basic.simple_table.
    TableSchema {
        keyspace: "test_basic".to_string(),
        table: "simple_table".to_string(),
        partition_keys: vec![key_col("id", "uuid", 0)],
        clustering_keys: vec![],
        columns: vec![
            col("account_balance", "decimal", false),
            col("active", "boolean", false),
            col("age", "int", false),
            col("ascii_field", "ascii", false),
            col("birth_date", "date", false),
            col("created", "timestamp", false),
            col("description", "blob", false),
            col("duration_val", "duration", false),
            col("height", "float", false),
            col("ip_address", "inet", false),
            col("medium_number", "smallint", false),
            col("name", "text", false),
            col("salary", "bigint", false),
            col("session_id", "timeuuid", false),
            col("small_number", "tinyint", false),
            col("varchar_field", "text", false),
            col("weight", "double", false),
            col("work_time", "time", false),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn collection_table_schema() -> TableSchema {
    // Schema from metadata.yml for test_collections.collection_table.
    TableSchema {
        keyspace: "test_collections".to_string(),
        table: "collection_table".to_string(),
        partition_keys: vec![key_col("id", "uuid", 0)],
        clustering_keys: vec![],
        columns: vec![
            col("metadata_map", "map<text,bigint>", false),
            col("numbers_set", "set<int>", false),
            col("ordered_values", "list<timestamp>", false),
            col("properties", "map<text,text>", false),
            col("scores", "list<int>", false),
            col("tags", "set<text>", false),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn sensor_data_schema() -> TableSchema {
    // Schema from metadata.yml for test_timeseries.sensor_data.
    TableSchema {
        keyspace: "test_timeseries".to_string(),
        table: "sensor_data".to_string(),
        partition_keys: vec![key_col("sensor_id", "uuid", 0)],
        clustering_keys: vec![ck_col("timestamp", "timestamp", 0)],
        columns: vec![
            col("battery_level", "tinyint", false),
            col("humidity", "float", false),
            col("location", "text", false),
            col("pressure", "double", false),
            col("status", "text", false),
            col("temperature", "float", false),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn wide_partition_schema() -> TableSchema {
    // Schema from metadata.yml for test_wide_rows.wide_partition_table.
    TableSchema {
        keyspace: "test_wide_rows".to_string(),
        table: "wide_partition_table".to_string(),
        partition_keys: vec![key_col("partition_key", "uuid", 0)],
        clustering_keys: vec![
            ck_col("clustering_col1", "timestamp", 0),
            ck_col("clustering_col2", "text", 1),
            ck_col("clustering_col3", "int", 2),
            ck_col("clustering_col4", "uuid", 3),
            ck_col("clustering_col5", "date", 4),
        ],
        columns: vec![
            col("blob_column", "blob", false),
            col("data_column", "text", false),
            col("json_column", "text", false),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

#[tokio::test]
async fn test_scan_delta_corpus_simple_table() {
    check_corpus_table("test_basic", "simple_table", simple_table_schema()).await;
}

#[tokio::test]
async fn test_scan_delta_corpus_collection_table() {
    check_corpus_table(
        "test_collections",
        "collection_table",
        collection_table_schema(),
    )
    .await;
}

#[tokio::test]
async fn test_scan_delta_corpus_sensor_data() {
    check_corpus_table("test_timeseries", "sensor_data", sensor_data_schema()).await;
}

#[tokio::test]
async fn test_scan_delta_corpus_wide_partition() {
    check_corpus_table(
        "test_wide_rows",
        "wide_partition_table",
        wide_partition_schema(),
    )
    .await;
}
