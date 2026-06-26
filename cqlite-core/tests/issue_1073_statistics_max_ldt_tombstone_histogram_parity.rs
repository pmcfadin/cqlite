//! Issue #1073: Statistics.db `SSTable max local deletion time` + estimated
//! tombstone-drop-times histogram decode parity.
//!
//! Task 1 (PR for #1073) wired the STATS-component decoder
//! (`parse_stats_extras`) into `parse_statistics_with_fallback`, so the enhanced
//! Statistics parser now fills:
//!
//!   * `timestamp_stats.max_deletion_time` with the authoritative STATS
//!     `SSTable max local deletion time` integer (the "no tombstones" sentinel
//!     `9223372036854775807` normalizes to `i64::MAX`), and
//!   * `tombstone_drop_times: Vec<(i64, u64)>` with the estimated
//!     tombstone-drop-times histogram buckets `(drop_time, count)`.
//!
//! This test pins both against the committed `nb-1-big-Statistics.db.txt`
//! sstablemetadata goldens for two fixtures:
//!
//!   * `test_basic/ttl_test_table`     — a real (non-sentinel) max LDT that
//!     differs from the min LDT, plus a 1-bucket drop-times histogram.
//!   * `test_tomb/tombstone_histogram` — `no tombstones` max LDT sentinel
//!     (`i64::MAX`) alongside a non-empty (1-bucket) drop-times histogram.
//!
//! ## Discipline (copied from issue_1011)
//!
//! - We NEVER hardcode an epoch integer: every expected value is parsed at
//!   runtime from the committed dump (fixtures get regenerated and the
//!   wall-clock-derived LDT values change). The "no tombstones" case parses the
//!   dump's literal `9223372036854775807`, which equals `i64::MAX`.
//! - SKIP cleanly (`[SKIP]`, return) when `CQLITE_DATASETS_ROOT` is unset or the
//!   binary `Data.db` / `Statistics.db` is absent; in strict mode
//!   (`CQLITE_REQUIRE_FIXTURES=1`/`true`) PANIC instead so a CI gate cannot
//!   false-pass on missing data (issue #972).
//! - FAIL loudly if the dump carries drop-time / max-LDT facts but CQLite
//!   matched ZERO (no silent pass).
//! - No path/name type heuristics: the facts come from the Statistics dump.

use std::fs;
use std::path::{Path, PathBuf};

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;

// ===========================================================================
// Dataset path / SKIP helpers (mirrors issue_1011)
// ===========================================================================

fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let path = PathBuf::from(root).join("sstables");
    path.exists().then_some(path)
}

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value ("1"/"true").
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

fn find_with_suffix(dir: &Path, suffix: &str, reject: &[&str]) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.starts_with("._") {
            continue;
        }
        if n.ends_with(suffix) && !reject.iter().any(|r| n.ends_with(r)) {
            return Some(entry.path());
        }
    }
    None
}

fn find_statistics_db(dir: &Path) -> Option<PathBuf> {
    find_with_suffix(dir, "-Statistics.db", &["-Statistics.db.txt"])
}

fn find_statistics_txt(dir: &Path) -> Option<PathBuf> {
    find_with_suffix(dir, "-Statistics.db.txt", &[])
}

fn find_data_db(dir: &Path) -> Option<PathBuf> {
    find_with_suffix(dir, "-Data.db", &["-Data.db.jsonl"])
}

// ===========================================================================
// Statistics.db.txt reference parsing (authoritative integers from the dump)
// ===========================================================================

/// Trailing parenthesised integer, e.g. `... (1759799526)`.
fn paren_int(line: &str) -> Option<i64> {
    let open = line.rfind('(')?;
    let close = line[open..].find(')')? + open;
    line[open + 1..close].trim().parse().ok()
}

/// Authoritative facts pulled from a `*-Statistics.db.txt` dump.
#[derive(Debug)]
struct StatsReference {
    /// `SSTable max local deletion time: ... (N)` — the literal parenthesised
    /// integer (for the "no tombstones" sentinel this is `9223372036854775807`,
    /// which equals `i64::MAX`).
    sstable_max_local_deletion_time: i64,
    /// Estimated-tombstone-drop-times histogram buckets: (drop_time, count).
    tombstone_drop_buckets: Vec<(i64, u64)>,
}

fn parse_stats_reference(txt: &Path) -> StatsReference {
    let content = fs::read_to_string(txt)
        .unwrap_or_else(|e| panic!("read reference {} failed: {e}", txt.display()));

    let mut max_ldt = None;
    let mut tombstone_drop_buckets = Vec::new();
    let mut in_drop_histogram = false;

    for line in content.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("SSTable max local deletion time:") {
            // Both the real case ("... (1759799526)") and the sentinel case
            // ("no tombstones (9223372036854775807)") carry a parenthesised
            // integer; parse it literally (sentinel == i64::MAX).
            max_ldt = paren_int(trimmed);
        } else if trimmed.starts_with("Estimated tombstone drop times:") {
            in_drop_histogram = true;
            continue;
        } else if in_drop_histogram {
            // Histogram rows look like:
            //   "Drop Time                        | Count  (%)  Histogram"
            //   "1782342000 (06/24/2026 23:00:00) |     5 (100) OOOO..."
            // Stop at "Percentiles" or any line without the column separator.
            if trimmed.starts_with("Percentiles") || !trimmed.contains('|') {
                in_drop_histogram = false;
                continue;
            }
            if trimmed.starts_with("Drop Time") {
                continue; // header row
            }
            if let Some((left, right)) = trimmed.split_once('|') {
                let drop_time = left.split_whitespace().next().and_then(|t| t.parse().ok());
                let count = right.split_whitespace().next().and_then(|t| t.parse().ok());
                if let (Some(d), Some(c)) = (drop_time, count) {
                    tombstone_drop_buckets.push((d, c));
                }
            }
        }
    }

    StatsReference {
        sstable_max_local_deletion_time: max_ldt.unwrap_or_else(|| {
            panic!(
                "{}: missing 'SSTable max local deletion time' line",
                txt.display()
            )
        }),
        tombstone_drop_buckets,
    }
}

// ===========================================================================
// Parity assertion driver
// ===========================================================================

/// Compare CQLite's decoded STATS max-LDT + tombstone-drop histogram against the
/// committed dump for one fixture. Returns `true` if the binary was present and
/// compared, `false` if it was skipped.
fn assert_max_ldt_and_histogram_parity(name: &str, keyspace: &str, prefix: &str) -> bool {
    let Some(dir) = find_table_dir(keyspace, prefix) else {
        skip_or_panic(
            &format!("{keyspace}/{prefix} fixture dir"),
            &format!("{name}: no {keyspace}/{prefix} fixture dir"),
        );
        return false;
    };

    // The committed dump is the golden — it MUST be present (fail closed).
    let txt = find_statistics_txt(&dir).unwrap_or_else(|| {
        panic!(
            "{name}: committed Statistics.db.txt missing in {} (fail closed)",
            dir.display()
        )
    });
    let reference = parse_stats_reference(&txt);

    // Fail closed: this scenario only proves anything if the dump carries a
    // non-empty drop-times histogram with a positive total count.
    assert!(
        !reference.tombstone_drop_buckets.is_empty(),
        "{name}: dump {} carries ZERO tombstone-drop buckets — reference broken \
         or parser regressed (this scenario requires a histogram)",
        txt.display()
    );
    let ref_total: u64 = reference
        .tombstone_drop_buckets
        .iter()
        .map(|(_, c)| c)
        .sum();
    assert!(
        ref_total > 0,
        "{name}: dump histogram total count must be > 0 (got {ref_total})"
    );

    // SKIP if the binary fixtures are absent (worktrees ship dumps only).
    if find_data_db(&dir).is_none() {
        skip_or_panic(
            &format!("{keyspace}/{prefix} Data.db"),
            &format!("{name}: binary Data.db absent in {}", dir.display()),
        );
        return false;
    }
    let Some(db) = find_statistics_db(&dir) else {
        skip_or_panic(
            &format!("{keyspace}/{prefix} Statistics.db"),
            &format!("{name}: binary Statistics.db absent in {}", dir.display()),
        );
        return false;
    };

    let bytes =
        fs::read(&db).unwrap_or_else(|e| panic!("{name}: read {} failed: {e}", db.display()));
    // `None` gates → nb/legacy default, correct for these nb fixtures.
    let (_, stats) = parse_statistics_with_fallback(&bytes, None)
        .unwrap_or_else(|e| panic!("{name}: CQLite failed to decode {}: {e:?}", db.display()));

    // (1) SSTable max local deletion time parity. The dump's parenthesised
    //     integer is the authoritative value; the "no tombstones" sentinel is
    //     literally 9223372036854775807 == i64::MAX, which CQLite normalizes to
    //     i64::MAX, so a direct equality holds in both cases.
    assert_eq!(
        stats.timestamp_stats.max_deletion_time, reference.sstable_max_local_deletion_time,
        "{name}: SSTable max local deletion time cqlite={} cassandra={}",
        stats.timestamp_stats.max_deletion_time, reference.sstable_max_local_deletion_time
    );

    // (2) Estimated tombstone-drop-times histogram parity. Bucket points are
    //     Cassandra-rounded; we require bucket CARDINALITY and TOTAL COUNT parity
    //     (the must), and additionally check point parity when the cardinalities
    //     allow a 1:1 ordered comparison.
    let cqlite_buckets = &stats.tombstone_drop_times;
    assert!(
        !cqlite_buckets.is_empty(),
        "{name}: dump carries {} drop-time bucket(s) (total {ref_total}) but CQLite \
         decoded ZERO — histogram decode regressed (no silent pass)",
        reference.tombstone_drop_buckets.len()
    );
    assert_eq!(
        cqlite_buckets.len(),
        reference.tombstone_drop_buckets.len(),
        "{name}: drop-time bucket cardinality cqlite={} cassandra={}",
        cqlite_buckets.len(),
        reference.tombstone_drop_buckets.len()
    );
    let cqlite_total: u64 = cqlite_buckets.iter().map(|(_, c)| c).sum();
    assert_eq!(
        cqlite_total, ref_total,
        "{name}: drop-time total count cqlite={cqlite_total} cassandra={ref_total}"
    );

    // Best-effort point parity: when both sides have the same cardinality, the
    // bucket drop-time points should match the dump's (Cassandra-rounded) points
    // positionally. This is asserted as an exact ordered comparison since the
    // cardinalities are equal.
    let cqlite_points: Vec<i64> = cqlite_buckets.iter().map(|(d, _)| *d).collect();
    let ref_points: Vec<i64> = reference
        .tombstone_drop_buckets
        .iter()
        .map(|(d, _)| *d)
        .collect();
    assert_eq!(
        cqlite_points, ref_points,
        "{name}: drop-time bucket points cqlite={cqlite_points:?} cassandra={ref_points:?}"
    );

    println!(
        "[PASS] {name}: max_local_deletion_time={} ({} buckets, total {ref_total}) byte-parity vs {}",
        reference.sstable_max_local_deletion_time,
        reference.tombstone_drop_buckets.len(),
        txt.file_name().and_then(|n| n.to_str()).unwrap_or("?")
    );
    true
}

#[test]
fn statistics_max_ldt_and_tombstone_histogram_parity() {
    let name = "statistics_max_ldt_and_tombstone_histogram_parity";
    if datasets_root().is_none() {
        skip_or_panic(
            "dataset root",
            &format!("{name}: CQLITE_DATASETS_ROOT unset"),
        );
        return;
    }

    let mut compared = 0usize;

    // ttl_test_table: real (non-sentinel) max LDT (1759799526 != min 1759799525)
    // and a 1-bucket drop-times histogram (count 400).
    if assert_max_ldt_and_histogram_parity(
        "ttl_test_table.max_ldt_histogram",
        "test_basic",
        "ttl_test_table-",
    ) {
        compared += 1;
    }

    // tombstone_histogram: "no tombstones" max-LDT sentinel (== i64::MAX) with a
    // non-empty 1-bucket drop-times histogram (count 5).
    if assert_max_ldt_and_histogram_parity(
        "tombstone_histogram.max_ldt_histogram",
        "test_tomb",
        "tombstone_histogram-",
    ) {
        compared += 1;
    }

    if compared == 0 {
        println!("[SKIP] {name}: no Statistics.db binaries present — nothing compared");
        return;
    }
    println!(
        "[PASS] {name}: {compared} fixture(s) STATS max-LDT + tombstone-drop histogram byte-parity"
    );
}
