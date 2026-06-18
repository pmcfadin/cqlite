//! Issue #693: Thread per-cell write timestamps from SSTable scan to QueryRow.
//!
//! The scan chain `Database::scan` → `SSTableManager::scan` → V5CompressedLegacy
//! parser previously materialized cells into `Value::Map` and dropped the per-cell
//! `timestamp` and `ttl` that the parser already decoded. As a result,
//! `WRITETIME(col)` and `TTL(col)` always returned `null`.
//!
//! The fix (this PR) adds a parallel `scan_with_cell_metadata` path that, when
//! `ProjectionFlags::include_cell_metadata` is true, returns a
//! `HashMap<String, CellWriteMetadata>` for each row alongside the value — the
//! executor then attaches it to the `QueryRow` so `evaluate_writetime_ttl` can
//! return the real timestamp instead of falling through to `Value::Null`.
//!
//! These tests:
//! 1. Assert that scanning `test_basic.simple_table` via the full query path
//!    yields a non-null, non-zero `WRITETIME(name)` for every row.
//! 2. Cross-check that the returned timestamp matches the `tstamp` recorded in
//!    the JSONL golden file (sstabledump ground truth).
//! 3. Skip cleanly when datasets are absent (same pattern as other integration
//!    tests in this directory).
//!
//! Requires `CQLITE_DATASETS_ROOT` and real Data.db files
//! (`bash test-data/scripts/fetch-datasets.sh`).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::Database;

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn get_schemas_dir() -> Option<PathBuf> {
    if let Some(datasets_root) = get_datasets_root() {
        let schemas_dir = datasets_root.parent()?.join("schemas");
        if schemas_dir.exists() {
            return Some(schemas_dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    schemas_dir.exists().then_some(schemas_dir)
}

/// Open `test_basic` database, or return `None` if datasets aren't available.
async fn setup_basic_db() -> Option<Database> {
    let datasets_root = get_datasets_root()?;
    let schemas_dir = get_schemas_dir()?;
    let schema_path = schemas_dir.join("basic-types.cql");
    if !schema_path.exists() {
        return None;
    }
    let data_dir = datasets_root.join("sstables");
    if !data_dir.exists() {
        return None;
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_basic/".to_string()),
    };

    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

/// WRITETIME(col) must return a non-null, positive integer (microseconds) for
/// every row in `test_basic.simple_table`.
///
/// Before issue #693 was fixed, the scan chain dropped per-cell timestamps and
/// every WRITETIME() call returned `null`.
#[tokio::test]
async fn writetime_returns_non_null_for_simple_table() {
    let Some(db) = setup_basic_db().await else {
        eprintln!("writetime_returns_non_null_for_simple_table: SKIPPED (no datasets)");
        return;
    };

    let result = db
        .execute("SELECT id, WRITETIME(name) FROM test_basic.simple_table LIMIT 10")
        .await
        .expect("WRITETIME query should succeed");

    if result.rows.is_empty() {
        eprintln!(
            "writetime_returns_non_null_for_simple_table: SKIPPED (0 rows — Data.db absent?)"
        );
        return;
    }

    let mut non_null_count = 0_usize;
    for row in &result.rows {
        let wt = row
            .values
            .get("writetime(name)")
            .cloned()
            .unwrap_or(Value::Null);
        match wt {
            Value::Null => {
                // Print which row had null for debugging
                let id = row.values.get("id").cloned().unwrap_or(Value::Null);
                eprintln!("WRITETIME(name) is null for id={:?}", id);
            }
            Value::BigInt(ts) => {
                assert!(
                    ts > 0,
                    "WRITETIME must be a positive microsecond epoch: got {}",
                    ts
                );
                // Sanity-check: Cassandra 5.0 SSTables were written in 2025.
                // 2025-01-01T00:00:00Z in microseconds ≈ 1_735_689_600_000_000.
                // A WRITETIME less than that is almost certainly wrong.
                assert!(
                    ts > 1_735_000_000_000_000_i64,
                    "WRITETIME timestamp looks wrong (too small): {}",
                    ts
                );
                non_null_count += 1;
            }
            other => {
                panic!("WRITETIME(name) expected BigInt or Null, got {:?}", other);
            }
        }
    }

    assert!(
        non_null_count > 0,
        "At least one WRITETIME(name) value must be non-null — \
         issue #693 scan threading is broken if all are null"
    );
    assert_eq!(
        non_null_count,
        result.rows.len(),
        "ALL rows must have a non-null WRITETIME(name): {}/{} were non-null",
        non_null_count,
        result.rows.len()
    );
}

/// TTL(col) must return null for a non-expiring column (`simple_table.name`
/// has no TTL), not an error or a garbage value.
#[tokio::test]
async fn ttl_returns_null_for_non_expiring_column() {
    let Some(db) = setup_basic_db().await else {
        eprintln!("ttl_returns_null_for_non_expiring_column: SKIPPED (no datasets)");
        return;
    };

    let result = db
        .execute("SELECT id, TTL(name) FROM test_basic.simple_table LIMIT 5")
        .await
        .expect("TTL query should succeed");

    if result.rows.is_empty() {
        eprintln!("ttl_returns_null_for_non_expiring_column: SKIPPED (0 rows)");
        return;
    }

    for row in &result.rows {
        let ttl = row.values.get("ttl(name)").cloned().unwrap_or(Value::Null);
        assert!(
            matches!(ttl, Value::Null),
            "TTL(name) must be null for a non-expiring column, got {:?}",
            ttl
        );
    }
}

/// WRITETIME(col) values decoded from the SSTable must match the per-row
/// `liveness_info.tstamp` recorded by sstabledump in the JSONL golden.
///
/// The golden file contains the authoritative sstabledump output; any mismatch
/// indicates a bug in the delta-decoding or timestamp-propagation logic.
#[tokio::test]
async fn writetime_matches_sstabledump_golden() {
    let Some(db) = setup_basic_db().await else {
        eprintln!("writetime_matches_sstabledump_golden: SKIPPED (no datasets)");
        return;
    };

    // Load the JSONL golden for simple_table.
    let datasets_root = match get_datasets_root() {
        Some(r) => r,
        None => {
            eprintln!("writetime_matches_sstabledump_golden: SKIPPED (no datasets root)");
            return;
        }
    };
    let golden_path = datasets_root.join("sstables").join("test_basic");

    // Find the simple_table directory (name includes UUID suffix).
    let table_dir = std::fs::read_dir(&golden_path).ok().and_then(|entries| {
        entries.flatten().find(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.starts_with("simple_table-"))
                .unwrap_or(false)
        })
    });

    let Some(table_dir) = table_dir else {
        eprintln!(
            "writetime_matches_sstabledump_golden: SKIPPED (simple_table directory not found)"
        );
        return;
    };

    let jsonl_path = table_dir.path().join("nb-1-big-Data.db.jsonl");
    if !jsonl_path.exists() {
        eprintln!("writetime_matches_sstabledump_golden: SKIPPED (JSONL golden not found)");
        return;
    }

    // Parse the golden into a map: UUID string → tstamp_micros
    let jsonl_content = std::fs::read_to_string(&jsonl_path).expect("read JSONL golden");
    let mut golden: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for line in jsonl_content.lines() {
        let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let uuid = entry["partition"]["key"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tstamp_str = entry["rows"]
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|r| r["liveness_info"]["tstamp"].as_str())
            .map(|s| s.to_string());

        if let (Some(uuid), Some(ts_str)) = (uuid, tstamp_str) {
            // Parse ISO-8601 timestamp to epoch-microseconds.
            // Format: "2025-10-06T01:12:05.394120Z"
            if let Ok(ts_micros) = parse_iso8601_to_micros(&ts_str) {
                golden.insert(uuid, ts_micros);
            }
        }
    }

    if golden.is_empty() {
        eprintln!(
            "writetime_matches_sstabledump_golden: SKIPPED (golden is empty — parse failure?)"
        );
        return;
    }

    // Run the WRITETIME query.
    let result = db
        .execute("SELECT id, WRITETIME(name) FROM test_basic.simple_table LIMIT 20")
        .await
        .expect("WRITETIME query should succeed");

    if result.rows.is_empty() {
        eprintln!("writetime_matches_sstabledump_golden: SKIPPED (0 rows)");
        return;
    }

    let mut checked = 0_usize;
    for row in &result.rows {
        // Get the UUID as a string (it's a Value::Uuid displayed as a hyphenated string).
        let id_str = match row.values.get("id") {
            Some(Value::Uuid(bytes)) if bytes.len() == 16 => {
                let b = bytes.as_slice();
                format!(
                    "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                    u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
                    u16::from_be_bytes([b[4], b[5]]),
                    u16::from_be_bytes([b[6], b[7]]),
                    u16::from_be_bytes([b[8], b[9]]),
                    u64::from_be_bytes([0, 0, b[10], b[11], b[12], b[13], b[14], b[15]])
                )
            }
            Some(Value::Text(s)) => s.clone(),
            Some(other) => format!("{:?}", other),
            None => continue,
        };

        let Some(&expected_ts) = golden.get(&id_str) else {
            // Row not in golden (filtered or different LIMIT) — skip this row.
            continue;
        };

        let got_ts = match row.values.get("writetime(name)") {
            Some(Value::BigInt(ts)) => *ts,
            Some(Value::Null) => {
                panic!(
                    "WRITETIME(name) is null for id={} — issue #693 threading is broken",
                    id_str
                );
            }
            other => panic!("Unexpected WRITETIME value {:?} for id={}", other, id_str),
        };

        assert_eq!(
            got_ts, expected_ts,
            "WRITETIME(name) mismatch for id={}: got {} but golden says {}",
            id_str, got_ts, expected_ts
        );
        checked += 1;
    }

    assert!(
        checked > 0,
        "No rows were cross-checked against the golden — golden IDs don't match query output?"
    );
    eprintln!(
        "writetime_matches_sstabledump_golden: cross-checked {} rows against golden",
        checked
    );
}

/// Parse an ISO-8601 timestamp string (`"2025-10-06T01:12:05.394120Z"`) into
/// microseconds since the Unix epoch.  Only the `*Z` (UTC) form is needed here.
fn parse_iso8601_to_micros(s: &str) -> Result<i64, String> {
    let s = s.trim_end_matches('Z');
    // Expected format: YYYY-MM-DDTHH:MM:SS.ffffff
    let (date_part, time_part) = s
        .split_once('T')
        .ok_or_else(|| format!("no T separator in '{}'", s))?;

    let parts: Vec<&str> = date_part.split('-').collect();
    if parts.len() != 3 {
        return Err(format!("bad date part '{}'", date_part));
    }
    let year: i64 = parts[0].parse().map_err(|e| format!("{}", e))?;
    let month: u32 = parts[1].parse().map_err(|e| format!("{}", e))?;
    let day: u32 = parts[2].parse().map_err(|e| format!("{}", e))?;

    let (hms, frac) = time_part.split_once('.').unwrap_or((time_part, "0"));
    let hms_parts: Vec<&str> = hms.split(':').collect();
    if hms_parts.len() != 3 {
        return Err(format!("bad time part '{}'", hms));
    }
    let hour: i64 = hms_parts[0].parse().map_err(|e| format!("{}", e))?;
    let min: i64 = hms_parts[1].parse().map_err(|e| format!("{}", e))?;
    let sec: i64 = hms_parts[2].parse().map_err(|e| format!("{}", e))?;

    // Pad / truncate fractional part to 6 digits (microseconds).
    let frac_padded = format!("{:0<6}", &frac[..frac.len().min(6)]);
    let micros_frac: i64 = frac_padded.parse().map_err(|e| format!("{}", e))?;

    // Compute days since epoch using the Gregorian calendar formula.
    let epoch_days = days_since_epoch(year, month, day)?;

    let total_seconds = epoch_days * 86_400 + hour * 3_600 + min * 60 + sec;
    Ok(total_seconds * 1_000_000 + micros_frac)
}

/// Compute days since 1970-01-01 for a Gregorian calendar date.
fn days_since_epoch(year: i64, month: u32, day: u32) -> Result<i64, String> {
    // Zeller / Julian Day Number approach, reduced to epoch days.
    // Reference: https://en.wikipedia.org/wiki/Julian_day#Converting_Gregorian_calendar_date_to_Julian_Day_Number
    let m = month as i64;
    let d = day as i64;
    let y = if m <= 2 { year - 1 } else { year };
    let m2 = if m <= 2 { m + 12 } else { m };
    let a = y / 100;
    let b = 2 - a + a / 4;
    let jdn = 36_525 * (y + 4_716) / 100 + 306_001 * (m2 + 1) / 10_000 + d + b - 1_524;
    // Julian Day Number for 1970-01-01 is 2_440_588.
    Ok(jdn - 2_440_588)
}
