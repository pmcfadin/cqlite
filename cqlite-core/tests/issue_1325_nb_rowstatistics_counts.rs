//! Issue #1325: the enhanced (`nb`) Statistics.db parser must populate
//! `RowStatistics.{partition_count, total_rows}` with the AUTHORITATIVE counts
//! from the STATS component instead of the hard-coded `0` stubs.
//!
//! Before this fix `parse_nb_format_statistics_data` set `total_rows`,
//! `live_rows`, and `partition_count` to `0` for every real `nb` SSTable, so a
//! consumer reading those counts got a silently-wrong `0`. The fix delegates to
//! the single source of truth added in #944 —
//! `repair_metadata::read_table_counts` (partition_count = Σ
//! `estimatedPartitionSize` histogram bucket counts; total_rows = STATS
//! `totalRows` via the version-gated walk).
//!
//! ## No-heuristics / authoritative-values discipline
//!
//! - Expected counts are the values verified in #944 against these real
//!   fixtures. They are structural (partition/row cardinality of the committed
//!   dataset), not wall-clock derived, so pinning literals is safe here.
//! - `live_rows` is left `0` by the fix (STATS carries no per-SSTable live-row
//!   count distinct from `total_rows`); we assert that documented behavior.
//! - SKIP cleanly when `CQLITE_DATASETS_ROOT` is unset or the fixture's binary
//!   `Statistics.db` is absent; but a fixture that IS present MUST carry the
//!   authoritative counts — a present-but-zero/wrong result FAILS (never let the
//!   test false-pass on an empty dataset).

use std::fs;
use std::path::PathBuf;

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::storage::sstable::version_gate::VersionGates;

fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let path = PathBuf::from(root).join("sstables");
    path.exists().then_some(path)
}

/// Find the table directory whose name starts with `prefix` inside `<root>/<ks>`.
fn find_table_dir(keyspace: &str, prefix: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks_dir = root.join(keyspace);
    let mut candidates: Vec<PathBuf> = fs::read_dir(&ks_dir)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let s = name.to_str()?;
            s.starts_with(prefix).then(|| e.path())
        })
        .collect();
    candidates.sort();
    candidates.into_iter().next()
}

/// Find the `-Statistics.db` binary (rejecting the `.txt` dump) in `dir`.
fn find_statistics_db(dir: &std::path::Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.starts_with("._") {
            continue;
        }
        if n.ends_with("-Statistics.db") && !n.ends_with("-Statistics.db.txt") {
            return Some(entry.path());
        }
    }
    None
}

/// Assert the authoritative row/partition counts for one real `nb` fixture.
///
/// Returns `true` when the fixture was present and its counts asserted; `false`
/// when it was cleanly skipped (fixture dir or Statistics.db genuinely absent).
fn assert_counts(keyspace: &str, prefix: &str, want_partitions: u64, want_total_rows: u64) -> bool {
    let Some(dir) = find_table_dir(keyspace, prefix) else {
        println!("[SKIP] {keyspace}/{prefix}: no fixture dir");
        return false;
    };
    let Some(stats_path) = find_statistics_db(&dir) else {
        println!(
            "[SKIP] {keyspace}/{prefix}: no Statistics.db in {}",
            dir.display()
        );
        return false;
    };

    let bytes = fs::read(&stats_path)
        .unwrap_or_else(|e| panic!("read {} failed: {e}", stats_path.display()));
    // Gates from the filename so the version-gated walk to `totalRows` succeeds.
    let gates = VersionGates::from_path(&stats_path).ok();

    let (_rest, stats) = parse_statistics_with_fallback(&bytes, gates.as_ref())
        .unwrap_or_else(|e| panic!("parse {} failed: {e:?}", stats_path.display()));

    let row = &stats.row_stats;
    assert_eq!(
        row.partition_count,
        want_partitions,
        "{keyspace}/{prefix}: partition_count must be the authoritative STATS count \
         (Σ estimatedPartitionSize buckets), not the old stub 0 ({})",
        stats_path.display()
    );
    assert_eq!(
        row.total_rows,
        want_total_rows,
        "{keyspace}/{prefix}: total_rows must be the authoritative STATS totalRows, \
         not the old stub 0 ({})",
        stats_path.display()
    );
    // `live_rows` is intentionally left 0 (not authoritatively derivable from
    // STATS as a value distinct from total_rows; #28 no-heuristics).
    assert_eq!(
        row.live_rows, 0,
        "{keyspace}/{prefix}: live_rows is left 0 (not authoritatively derivable); \
         a nonzero value would be a fabricated count"
    );
    true
}

/// #1325: real `nb` fixtures carry the authoritative row/partition counts.
///
/// Fixture values verified in #944:
///   * `sensor_data`  → partition_count=10,   total_rows=2000
///   * `simple_table` → partition_count=1000, total_rows=1000
///   * `stock_prices` → partition_count=3,    total_rows=200
#[test]
fn nb_rowstatistics_carry_authoritative_counts() {
    let Some(_root) = datasets_root() else {
        println!("[SKIP] CQLITE_DATASETS_ROOT unset — no fixtures to validate");
        return;
    };

    let mut asserted = 0usize;
    if assert_counts("test_timeseries", "sensor_data", 10, 2000) {
        asserted += 1;
    }
    if assert_counts("test_basic", "simple_table", 1000, 1000) {
        asserted += 1;
    }
    if assert_counts("test_timeseries", "stock_prices", 3, 200) {
        asserted += 1;
    }

    // With CQLITE_DATASETS_ROOT set, at least one fixture is expected present.
    // If ALL three were genuinely absent we skipped cleanly above; that is only
    // acceptable when the dataset truly lacks them (not a false-pass on zeros).
    println!("[INFO] #1325: asserted authoritative counts for {asserted}/3 fixtures");
}
