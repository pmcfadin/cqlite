//! Issue #1011: Preserve TTL and local deletion time metadata.
//!
//! Proves CQLite preserves Cassandra expiration metadata from BOTH sources:
//!
//!   1. **Data.db cells** (via `delta-scan`): per-row liveness `expires_at`
//!      (the cell's `localDeletionTime`, in epoch microseconds) and the derived
//!      TTL seconds, compared POSITIONALLY against the committed sstabledump
//!      JSONL goldens.  Covers:
//!        * `test_deltas/ttl_cells`        — mixed expiring (ttl=3600) + live rows
//!        * `test_tomb/gc_before_boundary` — two TTL cells (86400 / 86401) whose
//!          decoded localDeletionTimes differ by exactly 1 second, plus the
//!          gc-eligibility boundary edge case.
//!
//!   2. **Statistics.db** (via the enhanced Statistics parser): the authoritative
//!      EncodingStats `minLocalDeletionTime` and `minTTL` integers that Cassandra
//!      persisted, compared byte-for-byte against the parenthesised integers in
//!      the committed `*-Statistics.db.txt` reference dumps.  Covers:
//!        * `test_basic/ttl_test_table`     — real (non-epoch) local-deletion-time
//!        * `test_tomb/tombstone_histogram` — gc_grace_seconds=0, non-empty
//!          estimated-tombstone-drop-times histogram in the dump
//!        * `test_tomb/gc_before_boundary`
//!
//! ## localDeletionTime caveat
//!
//! Cassandra computes `localDeletionTime` from the server WALL-CLOCK at write
//! time (for a TTL cell: `nowInSeconds + ttl`), NOT from `USING TIMESTAMP`. So
//! the absolute localDeletionTime values are 2026-based and change every time the
//! fixtures are regenerated.  We therefore NEVER hardcode an epoch constant: we
//! compare CQLite's decoded value against the value captured in the COMMITTED
//! golden (JSONL `expires_at` / Statistics `minLocalDeletionTime`).  The TTL
//! (86400 / 86401 / 3600) and writetime (`tstamp`) ARE deterministic and asserted
//! exactly.
//!
//! ## STATS max-LDT + tombstone-drop histogram (issue #1073 — gap closed)
//!
//! CQLite now decodes the STATS-component `SSTable max local deletion time`
//! (the "no tombstones" sentinel normalizes to `i64::MAX`) and the
//! estimated-tombstone-drop-times histogram (issue #1073).  The two
//! `statistics_metadata` scenarios that depend on those are therefore asserted
//! against the REAL decoded values: `timestamp_stats.max_deletion_time` equals
//! the dump's parenthesised integer, and `tombstone_drop_times` reproduces the
//! dump's histogram bucket cardinality and total count.  A focused parity test
//! lives in `issue_1073_statistics_max_ldt_tombstone_histogram_parity.rs`; here
//! we keep the assertions in the combined #1011 scenario so a regression in
//! either source trips this lane too.
//!
//! ## Discipline
//!
//! - SKIP cleanly (print `[SKIP]`, return) when `CQLITE_DATASETS_ROOT` is unset or
//!   the binary `Data.db` / `Statistics.db` is absent.
//! - FAIL loudly if a committed JSONL/Statistics reference carries facts but ZERO
//!   were matched (no silent pass).
//! - Ordered vectors are compared POSITIONALLY (no set-membership).
//! - No path/name type heuristics: type facts come from schema/Statistics/JSONL.

#![cfg(feature = "delta-scan")]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, DeltaRecord};

// ===========================================================================
// Dataset path helpers
// ===========================================================================

fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let path = PathBuf::from(root).join("sstables");
    path.exists().then_some(path)
}

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value ("1"/"true").
/// In strict mode, every code path that would otherwise SKIP because the dataset
/// root is unset or a required binary fixture is absent must PANIC instead, so a
/// CI gate cannot false-pass on missing data (issue #972).
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Skip cleanly (default) or PANIC (strict mode) when a required fixture is absent.
fn skip_or_panic(fixture: &str, reason: &str) {
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but fixture {fixture} is absent — {reason}; \
             fetch/generate it (bash test-data/scripts/fetch-datasets.sh)"
        );
    }
    println!("[SKIP] {reason}");
}

/// Find the table directory whose name starts with `prefix` inside `<root>/<ks>`,
/// preferring one that actually has a binary `Data.db` (some fixtures ship
/// JSONL-only directories alongside the real one).
fn find_table_dir(keyspace: &str, prefix: &str, require_data: bool) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks_dir = root.join(keyspace);
    let mut candidates: Vec<PathBuf> = fs::read_dir(&ks_dir)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let s = name.to_str()?;
            if s.starts_with(prefix) {
                Some(e.path())
            } else {
                None
            }
        })
        .collect();
    candidates.sort();
    if require_data {
        candidates.into_iter().find(|d| find_data_db(d).is_some())
    } else {
        candidates.into_iter().next()
    }
}

fn find_data_db(dir: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Data.db") && !n.ends_with(".jsonl") && !n.starts_with("._") {
            return Some(entry.path());
        }
    }
    None
}

fn find_jsonl(dir: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Data.db.jsonl") && !n.starts_with("._") {
            return Some(entry.path());
        }
    }
    None
}

fn find_statistics_db(dir: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Statistics.db") && !n.ends_with(".txt") && !n.starts_with("._") {
            return Some(entry.path());
        }
    }
    None
}

fn find_statistics_txt(dir: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Statistics.db.txt") && !n.starts_with("._") {
            return Some(entry.path());
        }
    }
    None
}

// ===========================================================================
// ISO-8601 -> microseconds (no chrono dependency)
// ===========================================================================

/// Parse an ISO-8601 UTC timestamp (`2026-06-25T22:59:05Z` or with fractional
/// seconds) into microseconds since the Unix epoch.
fn iso8601_to_micros(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date_part, time_part) = s.split_once('T')?;

    let mut dp = date_part.splitn(3, '-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;

    let (hms, frac) = time_part.split_once('.').unwrap_or((time_part, ""));
    let mut tp = hms.splitn(3, ':');
    let hour: i64 = tp.next()?.parse().ok()?;
    let minute: i64 = tp.next()?.parse().ok()?;
    let second: i64 = tp.next()?.parse().ok()?;

    let days = days_since_epoch(year, month, day)?;
    let epoch_seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    let frac_micros = if frac.is_empty() {
        0i64
    } else {
        format!("{:0<6}", &frac[..frac.len().min(6)])
            .parse::<i64>()
            .ok()?
    };
    Some(epoch_seconds * 1_000_000 + frac_micros)
}

fn days_since_epoch(year: i64, month: i64, day: i64) -> Option<i64> {
    // Julian Day Number difference; JDN of 1970-01-01 is 2440588.
    let a = (14 - month) / 12;
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    let jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    Some(jdn - 2_440_588)
}

// ===========================================================================
// JSONL golden parsing (row-level liveness)
// ===========================================================================

/// One live row parsed from a sstabledump JSONL golden file.
#[derive(Debug, Clone)]
struct GoldenRow {
    partition_key: Vec<String>,
    clustering_key: Vec<String>,
    /// `liveness_info.tstamp` (writetime) in epoch micros — deterministic.
    tstamp_micros: i64,
    /// `liveness_info.ttl` seconds (None for non-expiring rows) — deterministic.
    ttl_secs: Option<i64>,
    /// `liveness_info.expires_at` == localDeletionTime, epoch micros.  Wall-clock
    /// derived, NOT deterministic across regenerations — compared relatively /
    /// against the golden, never against a hardcoded constant.
    expires_at_micros: Option<i64>,
}

/// Parse all live rows from a JSONL file.  Returns rows in file order.
fn parse_golden_rows(jsonl_path: &Path) -> Vec<GoldenRow> {
    let content = match fs::read_to_string(jsonl_path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let partition_key: Vec<String> = entry["partition"]["key"]
            .as_array()
            .map(|a| a.iter().map(json_scalar_to_string).collect())
            .unwrap_or_default();

        let Some(rows) = entry["rows"].as_array() else {
            continue;
        };
        for row in rows {
            if row["type"].as_str() != Some("row") {
                continue;
            }
            let clustering_key: Vec<String> = row["clustering"]
                .as_array()
                .map(|a| a.iter().map(json_scalar_to_string).collect())
                .unwrap_or_default();

            let Some(tstamp_str) = row["liveness_info"]["tstamp"].as_str() else {
                continue;
            };
            let Some(tstamp_micros) = iso8601_to_micros(tstamp_str) else {
                continue;
            };

            let ttl_secs = row["liveness_info"]["ttl"].as_i64();
            let expires_at_micros = row["liveness_info"]["expires_at"]
                .as_str()
                .and_then(iso8601_to_micros);

            out.push(GoldenRow {
                partition_key: partition_key.clone(),
                clustering_key,
                tstamp_micros,
                ttl_secs,
                expires_at_micros,
            });
        }
    }
    out
}

fn json_scalar_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ===========================================================================
// Schemas (derived from Statistics.db.txt / fixture metadata, not the path)
// ===========================================================================

fn key_col(name: &str, ty: &str, pos: usize) -> KeyColumn {
    KeyColumn {
        name: name.to_string(),
        data_type: ty.to_string(),
        position: pos,
    }
}

fn ck_col(name: &str, ty: &str, pos: usize) -> ClusteringColumn {
    ClusteringColumn {
        name: name.to_string(),
        data_type: ty.to_string(),
        position: pos,
        order: cqlite_core::schema::ClusteringOrder::Asc,
    }
}

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// `test_deltas.ttl_cells`: pk int, ck int, val text, extra text.
fn ttl_cells_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_deltas".to_string(),
        table: "ttl_cells".to_string(),
        partition_keys: vec![key_col("pk", "int", 0)],
        clustering_keys: vec![ck_col("ck", "int", 0)],
        columns: vec![col("val", "text"), col("extra", "text")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// `test_tomb.gc_before_boundary`: pk int, ck int, val text
/// (KeyType Int32, ClusteringTypes [Int32], RegularColumns val:UTF8 — from the
/// Statistics.db.txt dump, NOT the directory name).
fn gc_before_boundary_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_tomb".to_string(),
        table: "gc_before_boundary".to_string(),
        partition_keys: vec![key_col("pk", "int", 0)],
        clustering_keys: vec![ck_col("ck", "int", 0)],
        columns: vec![col("val", "text")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

// ===========================================================================
// delta-scan collection
// ===========================================================================

async fn collect_upserts(fixture_dir: &Path, schema: TableSchema) -> Vec<DeltaRecord> {
    let (mut rx, _summary) = scan_delta(fixture_dir.to_path_buf(), schema, 256);
    let mut records = Vec::new();
    while let Some(result) = rx.recv().await {
        match result {
            Ok(rec) => records.push(rec),
            Err(e) => panic!("scan_delta error in {:?}: {}", fixture_dir, e),
        }
    }
    records
}

/// A decoded liveness fact for one (pk, ck) row, extracted from a delta-scan
/// `Upsert` record.
#[derive(Debug, Clone)]
struct DecodedRow {
    partition_key: Vec<String>,
    clustering_key: Vec<String>,
    writetime: i64,
    /// `liveness.expires_at` == decoded localDeletionTime (epoch micros).
    expires_at: Option<i64>,
}

fn value_to_string(v: &cqlite_core::types::Value) -> String {
    use cqlite_core::types::Value;
    match v {
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::Text(s) => String::from_utf8_lossy(s).into_owned(),
        Value::Boolean(b) => b.to_string(),
        Value::Uuid(u) => format!("{}", uuid::Uuid::from_bytes(*u)),
        other => format!("{:?}", other),
    }
}

fn decode_rows(records: &[DeltaRecord]) -> Vec<DecodedRow> {
    let mut out = Vec::new();
    for rec in records {
        if let DeltaRecord::Upsert { keys, liveness, .. } = rec {
            // Only rows with liveness (INSERT) carry the row-level localDeletionTime.
            let Some(lv) = liveness else { continue };
            out.push(DecodedRow {
                partition_key: keys.partition.iter().map(value_to_string).collect(),
                clustering_key: keys.clustering.iter().map(value_to_string).collect(),
                writetime: lv.writetime,
                expires_at: lv.expires_at,
            });
        }
    }
    out
}

/// Match a decoded row to a golden row by (pk, ck) positional key equality.
fn find_decoded<'a>(decoded: &'a [DecodedRow], golden: &GoldenRow) -> Option<&'a DecodedRow> {
    decoded.iter().find(|d| {
        d.partition_key == golden.partition_key && d.clustering_key == golden.clustering_key
    })
}

// ===========================================================================
// Statistics.db.txt reference parsing (authoritative parenthesised integers)
// ===========================================================================

/// Trailing parenthesised integer, e.g. `... (1759799526)`.
fn paren_int(line: &str) -> Option<i64> {
    let open = line.rfind('(')?;
    let close = line[open..].find(')')? + open;
    line[open + 1..close].trim().parse().ok()
}

/// The literal parenthesised integer on the `SSTable max local deletion time`
/// line of a dump, including the "no tombstones (9223372036854775807)" sentinel
/// (which equals `i64::MAX`).  Returns `None` if the line is absent.
fn max_local_deletion_time_paren_int(txt: &Path) -> Option<i64> {
    let content = fs::read_to_string(txt).ok()?;
    content
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("SSTable max local deletion time:"))
        .and_then(paren_int)
}

/// Bare integer after the last colon, e.g. `TTL min: 0` or
/// `EncodingStats minTTL: 86400 (1 day)` (leading number after the colon).
fn trailing_int(line: &str) -> Option<i64> {
    let after = line.rsplit(':').next()?.trim();
    after.split_whitespace().next()?.parse().ok()
}

/// Authoritative facts pulled from a `*-Statistics.db.txt` dump.
#[derive(Debug)]
struct StatsReference {
    /// `EncodingStats minLocalDeletionTime: ... (N)` — decodable by CQLite.
    enc_min_local_deletion_time: i64,
    /// `EncodingStats minTTL: N` — decodable by CQLite.
    enc_min_ttl: i64,
    /// `SSTable max local deletion time: ... (N)` — NOT yet decoded by CQLite.
    /// `None` when the dump says `no tombstones`.
    sstable_max_local_deletion_time: Option<i64>,
    /// Estimated-tombstone-drop-times histogram buckets: (drop_time, count).
    tombstone_drop_buckets: Vec<(i64, i64)>,
}

fn parse_stats_reference(txt: &Path) -> StatsReference {
    let content = fs::read_to_string(txt)
        .unwrap_or_else(|e| panic!("read reference {} failed: {e}", txt.display()));

    let mut enc_min_ldt = None;
    let mut enc_min_ttl = None;
    let mut sstable_max_ldt = None;
    let mut tombstone_drop_buckets = Vec::new();
    let mut in_drop_histogram = false;

    for line in content.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("EncodingStats minLocalDeletionTime:") {
            enc_min_ldt = paren_int(trimmed);
        } else if trimmed.starts_with("EncodingStats minTTL:") {
            enc_min_ttl = trailing_int(trimmed);
        } else if trimmed.starts_with("SSTable max local deletion time:") {
            // "no tombstones (9223372036854775807)" => sentinel => None;
            // otherwise the parenthesised integer is the real max LDT.
            if trimmed.contains("no tombstones") {
                sstable_max_ldt = None;
            } else {
                sstable_max_ldt = paren_int(trimmed);
            }
        } else if trimmed.starts_with("Estimated tombstone drop times:") {
            in_drop_histogram = true;
            continue;
        } else if in_drop_histogram {
            // Histogram rows look like:
            //   "Drop Time                        | Count  (%)  Histogram"
            //   "1782342000 (06/24/2026 23:00:00) |     5 (100) OOOO..."
            // Stop when we hit "Percentiles" or another top-level section.
            if trimmed.starts_with("Percentiles") || !trimmed.contains('|') {
                in_drop_histogram = false;
                continue;
            }
            if trimmed.starts_with("Drop Time") {
                continue; // header row
            }
            if let Some((left, right)) = trimmed.split_once('|') {
                // left: "1782342000 (06/24/2026 23:00:00) "
                let drop_time = left.split_whitespace().next().and_then(|t| t.parse().ok());
                // right: "     5 (100) OOOO..."
                let count = right.split_whitespace().next().and_then(|t| t.parse().ok());
                if let (Some(d), Some(c)) = (drop_time, count) {
                    tombstone_drop_buckets.push((d, c));
                }
            }
        }
    }

    StatsReference {
        enc_min_local_deletion_time: enc_min_ldt.unwrap_or_else(|| {
            panic!(
                "{}: missing EncodingStats minLocalDeletionTime",
                txt.display()
            )
        }),
        enc_min_ttl: enc_min_ttl
            .unwrap_or_else(|| panic!("{}: missing EncodingStats minTTL", txt.display())),
        sstable_max_local_deletion_time: sstable_max_ldt,
        tombstone_drop_buckets,
    }
}

// ===========================================================================
// Scenario 1: ttl_cells — mixed expiring + live (Data.db cells)
//   Manifest: cass.tombstone_ttl.ttl_cells.local_deletion_time
//             cass.tombstone_ttl.ttl_cells.mixed_expiring_and_live
// ===========================================================================

#[tokio::test]
async fn ttl_cells_mixed_expiring_and_live() {
    let name = "ttl_cells_mixed_expiring_and_live";
    if datasets_root().is_none() {
        skip_or_panic(
            "dataset root",
            &format!("{name}: CQLITE_DATASETS_ROOT unset"),
        );
        return;
    }
    let Some(dir) = find_table_dir("test_deltas", "ttl_cells-", true) else {
        skip_or_panic(
            "test_deltas/ttl_cells Data.db",
            &format!("{name}: no test_deltas/ttl_cells fixture with Data.db"),
        );
        return;
    };
    let Some(jsonl) = find_jsonl(&dir) else {
        skip_or_panic(
            "test_deltas/ttl_cells JSONL golden",
            &format!("{name}: no JSONL golden in {}", dir.display()),
        );
        return;
    };

    let golden = parse_golden_rows(&jsonl);
    assert!(
        !golden.is_empty(),
        "{name}: committed JSONL {} carries no rows — reference is broken (fail closed)",
        jsonl.display()
    );

    // The golden MUST carry both expiring and non-expiring rows for this scenario.
    let golden_expiring = golden.iter().filter(|r| r.ttl_secs.is_some()).count();
    let golden_live = golden.iter().filter(|r| r.ttl_secs.is_none()).count();
    assert!(
        golden_expiring > 0 && golden_live > 0,
        "{name}: golden must mix expiring ({golden_expiring}) and live ({golden_live}) rows"
    );

    let records = collect_upserts(&dir, ttl_cells_schema()).await;
    // 0-ROWS-WHEN-PRESENT IS A FAILURE, UNCONDITIONALLY (roborev round 5 on #3725).
    // This used to print `[SKIP] … (Data.db absent at scan time?)` and return SUCCESSFULLY.
    // That guess was provably wrong and the skip was a vacuous pass: `skip_or_panic` above
    // has ALREADY verified `test_deltas/ttl_cells`'s Data.db is present, and the golden was
    // just asserted to mix expiring and live rows. So zero scanned records here cannot mean
    // an absent fixture — it means the READER produced nothing from real data, i.e. a
    // decoder regression, which is exactly what this parity target exists to catch. It is
    // NOT routed through `skip_or_panic`: that helper is for an ABSENT fixture and would
    // let this pass in the default (non-strict) mode, where a decoder regression is just as
    // real. CLAUDE.md: "never let a dataset-dependent test pass on an empty dataset
    // (0-rows-when-present = failure)".
    assert!(
        !records.is_empty(),
        "{name}: scan_delta produced 0 records from a fixture whose Data.db is present and \
         whose golden has rows — a decoder regression, not a missing fixture"
    );
    let decoded = decode_rows(&records);

    let mut expiring_matched = 0usize;
    let mut live_matched = 0usize;

    for g in &golden {
        let Some(d) = find_decoded(&decoded, g) else {
            continue;
        };

        // Writetime is deterministic — exact equality.
        assert_eq!(
            d.writetime, g.tstamp_micros,
            "{name}: writetime mismatch pk={:?} ck={:?}: cqlite={} golden={}",
            g.partition_key, g.clustering_key, d.writetime, g.tstamp_micros
        );

        match g.ttl_secs {
            Some(ttl) => {
                // Expiring row: CQLite must carry a localDeletionTime (expires_at).
                let exp = d.expires_at.unwrap_or_else(|| {
                    panic!(
                        "{name}: EXPIRING row pk={:?} ck={:?} (ttl={ttl}) has NO decoded \
                         expires_at — TTL metadata dropped",
                        g.partition_key, g.clustering_key
                    )
                });
                let golden_exp = g.expires_at_micros.expect("expiring golden has expires_at");

                // localDeletionTime is wall-clock derived (nowInSeconds + ttl), NOT
                // a function of writetime: compare CQLite's decoded value against the
                // committed golden value (1s tolerance for second-rounding of the
                // golden's whole-second `expires_at`), never against a hardcoded
                // constant and never derived from writetime.
                let diff = (exp - golden_exp).abs();
                assert!(
                    diff <= 1_000_000,
                    "{name}: localDeletionTime mismatch pk={:?} ck={:?}: cqlite={} golden={} diff={}us",
                    g.partition_key, g.clustering_key, exp, golden_exp, diff
                );

                // The golden TTL is the deterministic fact. Cross-check the golden's
                // own internal consistency: for a cell written WITHOUT `USING
                // TIMESTAMP`, `localDeletionTime = floor(nowInSeconds) + ttl` while
                // `writetime = now_micros`, so `expires_at - writetime` equals `ttl`
                // MINUS the sub-second fraction of the writetime — i.e. it lands in
                // `(ttl-1, ttl]`. We require it within 1s of `ttl` (never larger),
                // proving the `ttl` we assert against is the real cell TTL. This is a
                // relative check; no epoch constant is hardcoded.
                let golden_span_micros = golden_exp - g.tstamp_micros;
                let lo = (ttl - 1) * 1_000_000;
                let hi = ttl * 1_000_000;
                assert!(
                    golden_span_micros > lo && golden_span_micros <= hi,
                    "{name}: golden inconsistency pk={:?} ck={:?}: \
                     expires_at-tstamp={golden_span_micros}us not in ({lo},{hi}] for ttl={ttl}",
                    g.partition_key,
                    g.clustering_key
                );
                expiring_matched += 1;
            }
            None => {
                // Live (non-expiring) row: CQLite must carry NO localDeletionTime.
                assert!(
                    d.expires_at.is_none(),
                    "{name}: LIVE row pk={:?} ck={:?} unexpectedly has expires_at={:?}",
                    g.partition_key,
                    g.clustering_key,
                    d.expires_at
                );
                live_matched += 1;
            }
        }
    }

    // Fail closed: the golden carries facts, so SOME of each kind must have matched.
    assert!(
        expiring_matched > 0,
        "{name}: golden has {golden_expiring} expiring rows but ZERO matched in delta-scan"
    );
    assert!(
        live_matched > 0,
        "{name}: golden has {golden_live} live rows but ZERO matched in delta-scan"
    );

    println!(
        "[PASS] {name}: {expiring_matched} expiring + {live_matched} non-expiring rows matched \
         (golden expiring={golden_expiring} live={golden_live})"
    );
}

// ===========================================================================
// Scenario 2: gc_before_boundary — two TTL cells differ by 1s + gc edge case
//   Manifest: cass.tombstone_ttl.ttl_expiry.gc_before_boundary
// ===========================================================================

#[tokio::test]
async fn gc_before_boundary_local_deletion_times() {
    let name = "gc_before_boundary_local_deletion_times";
    if datasets_root().is_none() {
        skip_or_panic(
            "dataset root",
            &format!("{name}: CQLITE_DATASETS_ROOT unset"),
        );
        return;
    }
    let Some(dir) = find_table_dir("test_tomb", "gc_before_boundary-", true) else {
        skip_or_panic(
            "test_tomb/gc_before_boundary Data.db",
            &format!("{name}: no test_tomb/gc_before_boundary fixture with Data.db"),
        );
        return;
    };
    let Some(jsonl) = find_jsonl(&dir) else {
        skip_or_panic(
            "test_tomb/gc_before_boundary JSONL golden",
            &format!("{name}: no JSONL golden in {}", dir.display()),
        );
        return;
    };

    let golden = parse_golden_rows(&jsonl);
    assert!(
        !golden.is_empty(),
        "{name}: committed JSONL {} carries no rows (fail closed)",
        jsonl.display()
    );

    // Locate the two expiring rows (ttl 86400 and 86401) by their golden TTL.
    let g_86400 = golden
        .iter()
        .find(|r| r.ttl_secs == Some(86400))
        .unwrap_or_else(|| panic!("{name}: golden missing the ttl=86400 row"));
    let g_86401 = golden
        .iter()
        .find(|r| r.ttl_secs == Some(86401))
        .unwrap_or_else(|| panic!("{name}: golden missing the ttl=86401 row"));
    let g_no_ttl = golden
        .iter()
        .find(|r| r.ttl_secs.is_none())
        .unwrap_or_else(|| panic!("{name}: golden missing the no-TTL row"));

    let records = collect_upserts(&dir, gc_before_boundary_schema()).await;
    // 0-ROWS-WHEN-PRESENT IS A FAILURE (roborev round 5 on #3725) — see the sibling guard
    // above for the full reasoning. The fixture's Data.db is already verified present and
    // the golden already validated, so zero records is a decoder regression, and skipping
    // on it was a vacuous pass in a target the merge gate now executes.
    assert!(
        !records.is_empty(),
        "{name}: scan_delta produced 0 records from a fixture whose Data.db is present and \
         whose golden has rows — a decoder regression, not a missing fixture"
    );
    let decoded = decode_rows(&records);

    let d_86400 = find_decoded(&decoded, g_86400)
        .unwrap_or_else(|| panic!("{name}: ttl=86400 row not decoded by delta-scan"));
    let d_86401 = find_decoded(&decoded, g_86401)
        .unwrap_or_else(|| panic!("{name}: ttl=86401 row not decoded by delta-scan"));
    let d_no_ttl = find_decoded(&decoded, g_no_ttl)
        .unwrap_or_else(|| panic!("{name}: no-TTL row not decoded by delta-scan"));

    let ldt_86400 = d_86400
        .expires_at
        .unwrap_or_else(|| panic!("{name}: ttl=86400 row has no decoded localDeletionTime"));
    let ldt_86401 = d_86401
        .expires_at
        .unwrap_or_else(|| panic!("{name}: ttl=86401 row has no decoded localDeletionTime"));

    // (a) The two decoded localDeletionTimes equal their committed golden values.
    let golden_ldt_86400 = g_86400.expires_at_micros.expect("golden expires_at 86400");
    let golden_ldt_86401 = g_86401.expires_at_micros.expect("golden expires_at 86401");
    assert!(
        (ldt_86400 - golden_ldt_86400).abs() <= 1_000_000,
        "{name}: ttl=86400 localDeletionTime cqlite={ldt_86400} golden={golden_ldt_86400}"
    );
    assert!(
        (ldt_86401 - golden_ldt_86401).abs() <= 1_000_000,
        "{name}: ttl=86401 localDeletionTime cqlite={ldt_86401} golden={golden_ldt_86401}"
    );

    // (b) The two TTLs are exactly 86400 and 86401 (deterministic golden facts).
    assert_eq!(g_86400.ttl_secs, Some(86400), "{name}: golden ttl #1");
    assert_eq!(g_86401.ttl_secs, Some(86401), "{name}: golden ttl #2");

    // (c) The two decoded localDeletionTimes differ by EXACTLY 1 second
    //     (relative comparison — robust to regeneration). Because both rows share
    //     one writetime and `localDeletionTime = nowInSeconds + ttl`, the LDT delta
    //     MUST equal the TTL delta (86401 - 86400 = 1s). This is the deterministic
    //     cross-check that CQLite decoded two DISTINCT, correct localDeletionTimes —
    //     not derived from `USING TIMESTAMP` (which is the 2021 writetime).
    let diff_secs = (ldt_86401 - ldt_86400) / 1_000_000;
    let ttl_diff = g_86401.ttl_secs.unwrap() - g_86400.ttl_secs.unwrap();
    assert_eq!(
        diff_secs, ttl_diff,
        "{name}: localDeletionTime delta ({diff_secs}s) must equal TTL delta ({ttl_diff}s): \
         86401_ldt={ldt_86401} 86400_ldt={ldt_86400}"
    );
    assert_eq!(
        diff_secs, 1,
        "{name}: the two LDTs must differ by exactly 1s"
    );

    // (d) The no-TTL row carries no localDeletionTime.
    assert!(
        d_no_ttl.expires_at.is_none(),
        "{name}: no-TTL row unexpectedly has expires_at={:?}",
        d_no_ttl.expires_at
    );

    // (e) gc-eligibility boundary edge case.
    //     Derive gc_before from the goldens (the midpoint between the two LDTs):
    //     it lands strictly between them, so the boundary cell (smaller LDT) is
    //     gc-eligible (LDT < gc_before) while the one-past cell (larger LDT) is
    //     not.  The decision MUST differ between the two given the SAME gc_before.
    let gc_before = ldt_86400 + (ldt_86401 - ldt_86400) / 2; // strictly between
    assert!(
        ldt_86400 < gc_before && gc_before < ldt_86401,
        "{name}: derived gc_before={gc_before} must sit strictly between {ldt_86400} and {ldt_86401}"
    );
    let boundary_eligible = ldt_86400 < gc_before;
    let past_eligible = ldt_86401 < gc_before;
    assert!(
        boundary_eligible && !past_eligible,
        "{name}: gc-eligibility must differ at the boundary: boundary(86400)={boundary_eligible} \
         past(86401)={past_eligible} given gc_before={gc_before}"
    );

    println!(
        "[PASS] {name}: ldt(86400)={ldt_86400} ldt(86401)={ldt_86401} diff=1s; \
         gc_before={gc_before} => boundary_eligible={boundary_eligible} past_eligible={past_eligible}; \
         no-TTL row clean. (3 rows checked)"
    );
}

// ===========================================================================
// Scenario 3: Statistics.db — EncodingStats minLocalDeletionTime / minTTL
//   strict byte-for-byte vs the *-Statistics.db.txt parenthesised integers.
//   This is the DECODABLE half of:
//     cass.statistics_metadata.max_local_deletion_time.tombstones_ttl
//   The undecoded max-LDT half is asserted as the current placeholder behaviour
//   below (reported as a partial, NOT hacked green).
// ===========================================================================

/// Drive the strict EncodingStats parity for one fixture; returns true if the
/// binary was present and compared, false if it was skipped.
fn assert_encoding_stats_parity(name: &str, keyspace: &str, prefix: &str) -> bool {
    let Some(dir) = find_table_dir(keyspace, prefix, false) else {
        skip_or_panic(
            &format!("{keyspace}/{prefix} fixture dir"),
            &format!("{name}: no {keyspace}/{prefix} fixture dir"),
        );
        return false;
    };
    let txt = find_statistics_txt(&dir).unwrap_or_else(|| {
        panic!(
            "{name}: committed Statistics.db.txt missing in {} (fail closed)",
            dir.display()
        )
    });
    let reference = parse_stats_reference(&txt);

    let Some(db) = find_statistics_db(&dir) else {
        skip_or_panic(
            &format!("{keyspace}/{prefix} Statistics.db"),
            &format!("{name}: binary Statistics.db absent in {}", dir.display()),
        );
        return false;
    };
    let bytes =
        fs::read(&db).unwrap_or_else(|e| panic!("{name}: read {} failed: {e}", db.display()));
    let (_, stats) = parse_statistics_with_fallback(&bytes, None)
        .unwrap_or_else(|e| panic!("{name}: CQLite failed to decode {}: {e:?}", db.display()));

    // EncodingStats minLocalDeletionTime — wall-clock-derived but captured in the
    // committed dump; compared against THAT integer, not a hardcoded constant.
    assert_eq!(
        stats.timestamp_stats.min_deletion_time, reference.enc_min_local_deletion_time,
        "{name}: minLocalDeletionTime cqlite={} cassandra={}",
        stats.timestamp_stats.min_deletion_time, reference.enc_min_local_deletion_time
    );

    // EncodingStats minTTL — deterministic.
    let cqlite_min_ttl = stats.timestamp_stats.min_ttl.unwrap_or(0);
    assert_eq!(
        cqlite_min_ttl, reference.enc_min_ttl,
        "{name}: minTTL cqlite={cqlite_min_ttl} cassandra={}",
        reference.enc_min_ttl
    );

    println!(
        "[PASS] {name}: EncodingStats minLocalDeletionTime={} minTTL={} byte-parity vs {}",
        reference.enc_min_local_deletion_time,
        reference.enc_min_ttl,
        txt.file_name().and_then(|n| n.to_str()).unwrap_or("?")
    );
    true
}

#[tokio::test]
async fn statistics_encoding_stats_local_deletion_parity() {
    let name = "statistics_encoding_stats_local_deletion_parity";
    if datasets_root().is_none() {
        skip_or_panic(
            "dataset root",
            &format!("{name}: CQLITE_DATASETS_ROOT unset"),
        );
        return;
    }

    let mut compared = 0usize;
    // ttl_test_table has a real non-epoch min/max LDT and minTTL=86400.
    if assert_encoding_stats_parity(
        "ttl_test_table.encoding_stats",
        "test_basic",
        "ttl_test_table-",
    ) {
        compared += 1;
    }
    // gc_before_boundary: minTTL=86400, real min LDT.
    if assert_encoding_stats_parity(
        "gc_before_boundary.encoding_stats",
        "test_tomb",
        "gc_before_boundary-",
    ) {
        compared += 1;
    }
    // tombstone_histogram: minTTL=0, real min LDT (gc_grace_seconds=0).
    if assert_encoding_stats_parity(
        "tombstone_histogram.encoding_stats",
        "test_tomb",
        "tombstone_histogram-",
    ) {
        compared += 1;
    }

    if compared == 0 {
        println!("[SKIP] {name}: no Statistics.db binaries present — nothing compared");
        return;
    }
    println!("[PASS] {name}: {compared} fixture(s) EncodingStats LDT/TTL byte-parity");
}

// ===========================================================================
// Scenario 4: Statistics.db tombstone-drop-times histogram + max LDT.
//   Manifest: cass.statistics_metadata.tombstone_histogram.deletion_times
//             cass.statistics_metadata.max_local_deletion_time.tombstones_ttl
//
//   Issue #1073 closed the gap: CQLite now decodes the STATS-component
//   `SSTable max local deletion time` (the "no tombstones" sentinel normalizes
//   to i64::MAX) and the estimated-tombstone-drop-times histogram.  We assert
//   the REAL decoded values against the committed reference dump:
//     (a) `timestamp_stats.max_deletion_time` equals the dump's parenthesised
//         integer (real max LDT for ttl_test_table; i64::MAX sentinel for
//         tombstone_histogram), and
//     (b) `tombstone_drop_times` reproduces the dump's histogram bucket
//         cardinality and total count.
//   We still fail loudly if the dump carries facts but CQLite matched zero.
// ===========================================================================

#[tokio::test]
async fn statistics_tombstone_histogram_and_max_ldt_reference_facts() {
    let name = "statistics_tombstone_histogram_and_max_ldt_reference_facts";
    if datasets_root().is_none() {
        skip_or_panic(
            "dataset root",
            &format!("{name}: CQLITE_DATASETS_ROOT unset"),
        );
        return;
    }

    // --- (1) tombstone_histogram fixture: non-empty drop-times histogram. ---
    {
        let Some(dir) = find_table_dir("test_tomb", "tombstone_histogram-", false) else {
            skip_or_panic(
                "test_tomb/tombstone_histogram fixture dir",
                &format!("{name}: no tombstone_histogram fixture dir"),
            );
            return;
        };
        let txt = find_statistics_txt(&dir).unwrap_or_else(|| {
            panic!("{name}: tombstone_histogram Statistics.db.txt missing (fail closed)")
        });
        let reference = parse_stats_reference(&txt);

        // Fail closed: the reference MUST carry histogram buckets for this scenario.
        assert!(
            !reference.tombstone_drop_buckets.is_empty(),
            "{name}: tombstone_histogram dump carries ZERO drop-time buckets — \
             reference broken or parser regressed"
        );
        let total_count: i64 = reference
            .tombstone_drop_buckets
            .iter()
            .map(|(_, c)| c)
            .sum();
        let bucket_count = reference.tombstone_drop_buckets.len();
        assert!(
            total_count > 0,
            "{name}: histogram total count must be > 0 (got {total_count})"
        );

        // Issue #1073: CQLite now decodes the estimated-tombstone-drop-times
        // histogram into `tombstone_drop_times`. Assert it is non-empty and that
        // its total count equals the dump's bucket-count total.
        if let Some(db) = find_statistics_db(&dir) {
            let bytes = fs::read(&db).unwrap();
            let (_, stats) = parse_statistics_with_fallback(&bytes, None)
                .unwrap_or_else(|e| panic!("{name}: decode {} failed: {e:?}", db.display()));
            // Fail loudly: the dump carries histogram facts, so CQLite must too.
            assert!(
                !stats.tombstone_drop_times.is_empty(),
                "{name}: dump carries {bucket_count} drop-time bucket(s) (total {total_count}) \
                 but CQLite decoded ZERO — histogram decode regressed (no silent pass)"
            );
            assert_eq!(
                stats.tombstone_drop_times.len(),
                bucket_count,
                "{name}: drop-time bucket cardinality cqlite={} cassandra={bucket_count}",
                stats.tombstone_drop_times.len()
            );
            let cqlite_total: i64 = stats
                .tombstone_drop_times
                .iter()
                .map(|(_, c)| *c as i64)
                .sum();
            assert_eq!(
                cqlite_total, total_count,
                "{name}: drop-time total count cqlite={cqlite_total} cassandra={total_count}"
            );

            // Issue #1073: this fixture's dump reports
            // `SSTable max local deletion time: no tombstones (9223372036854775807)`.
            // Parse that literal integer from the dump and assert CQLite decoded
            // the same value (it equals i64::MAX). Parsed, never hardcoded.
            let sentinel = max_local_deletion_time_paren_int(&txt).unwrap_or_else(|| {
                panic!("{name}: tombstone_histogram dump missing 'SSTable max local deletion time'")
            });
            assert_eq!(
                sentinel,
                i64::MAX,
                "{name}: tombstone_histogram dump sentinel must be i64::MAX (got {sentinel})"
            );
            assert_eq!(
                stats.timestamp_stats.max_deletion_time, sentinel,
                "{name}: SSTable max local deletion time (no-tombstones sentinel) \
                 cqlite={} cassandra={sentinel}",
                stats.timestamp_stats.max_deletion_time
            );
            println!(
                "[PASS] {name}: tombstone-drop histogram reference has {bucket_count} bucket(s), \
                 total count {total_count}; CQLite decoded {} bucket(s), total {cqlite_total}; \
                 max LDT sentinel == i64::MAX (issue #1073)",
                stats.tombstone_drop_times.len()
            );
        } else {
            skip_or_panic(
                "test_tomb/tombstone_histogram Statistics.db",
                &format!(
                    "{name}: tombstone-drop histogram reference has {bucket_count} bucket(s), \
                     total {total_count}; binary Statistics.db absent (decode not exercised)"
                ),
            );
        }
    }

    // --- (2) ttl_test_table fixture: real SSTable max local deletion time. ---
    {
        let Some(dir) = find_table_dir("test_basic", "ttl_test_table-", false) else {
            skip_or_panic(
                "test_basic/ttl_test_table fixture dir",
                &format!("{name}: no ttl_test_table fixture dir"),
            );
            return;
        };
        let txt = find_statistics_txt(&dir)
            .unwrap_or_else(|| panic!("{name}: ttl_test_table Statistics.db.txt missing"));
        let reference = parse_stats_reference(&txt);

        // Fail closed: this fixture MUST carry a real (non-sentinel) max LDT.
        let max_ldt = reference
            .sstable_max_local_deletion_time
            .unwrap_or_else(|| {
                panic!(
                    "{name}: ttl_test_table dump has no real SSTable max local deletion time \
                 (got sentinel) — reference broken for this scenario"
                )
            });
        // Sanity: max LDT >= min LDT in the same dump.
        assert!(
            max_ldt >= reference.enc_min_local_deletion_time,
            "{name}: max LDT {max_ldt} < min LDT {}",
            reference.enc_min_local_deletion_time
        );

        if let Some(db) = find_statistics_db(&dir) {
            let bytes = fs::read(&db).unwrap();
            let (_, stats) = parse_statistics_with_fallback(&bytes, None)
                .unwrap_or_else(|e| panic!("{name}: decode {} failed: {e:?}", db.display()));
            // Issue #1073: CQLite now decodes the real STATS `SSTable max local
            // deletion time`. Assert byte-parity against the dump's parenthesised
            // integer (here min=1759799525 max=1759799526, so max != min proves a
            // real decode, not a min placeholder). Parsed from the dump, never
            // hardcoded.
            assert_eq!(
                stats.timestamp_stats.max_deletion_time, max_ldt,
                "{name}: SSTable max local deletion time cqlite={} cassandra={max_ldt}",
                stats.timestamp_stats.max_deletion_time
            );
            // Cross-check this fixture exercises a real (non-placeholder) max: it
            // differs from the decoded min baseline.
            assert_ne!(
                stats.timestamp_stats.max_deletion_time, stats.timestamp_stats.min_deletion_time,
                "{name}: ttl_test_table must exercise max LDT != min (real decode, not placeholder)"
            );
            println!(
                "[PASS] {name}: ttl_test_table max LDT cqlite={} == cassandra={max_ldt} (min={}); \
                 issue #1073 real decode",
                stats.timestamp_stats.max_deletion_time, reference.enc_min_local_deletion_time
            );
        } else {
            skip_or_panic(
                "test_basic/ttl_test_table Statistics.db",
                &format!("{name}: ttl_test_table reference max LDT={max_ldt}; binary absent"),
            );
        }
    }
}
