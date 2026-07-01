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
//!
//! ## Safety property (roborev #1325 follow-up)
//!
//! `parse_statistics_with_fallback` is a PUBLIC entry point reachable with `None`
//! gates for ANY format, and Statistics.db does NOT self-describe its SSTable
//! version. Synthesizing `nb` gates for an unknown-format buffer is a guess that
//! can mis-walk an `oa`/`da` STATS layout and expose a BOGUS nonzero `totalRows`
//! (worse than an honest 0; violates the no-heuristics mandate #28). The
//! `oa_da_none_gates_never_fabricates_total_rows` test proves that path is closed:
//! feeding a real `oa`/`da` Statistics.db through the `None`-gates entry point
//! yields `total_rows == 0` (honest/unavailable), while the gates-provided path
//! (version derived from the filename) still yields the authoritative count.

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

    // Path A — gates-provided: gates from the filename so the version-gated walk
    // to `totalRows` succeeds via the caller-threaded path. This is the normal
    // production path (`StatisticsReader::open` derives gates from the filename).
    let gates = VersionGates::from_path(&stats_path).ok();
    assert_row_counts(
        keyspace,
        prefix,
        &bytes,
        &stats_path,
        gates.as_ref(),
        "gates-provided",
        want_partitions,
        want_total_rows,
    );

    // Path B — None-gates (#1325 roborev finding): `parse_statistics_with_fallback`
    // is a PUBLIC entry point reachable with `None` gates for ANY format, and
    // Statistics.db does NOT self-describe its SSTable version. With an unknown
    // format we MUST NOT synthesize `nb` gates (a guess that could mis-walk an
    // oa/da layout and expose a bogus `totalRows`; no-heuristics #28). So the
    // None-gates path reports counts HONESTLY:
    //   * `partition_count` is still authoritative — it is the Σ of the leading,
    //     fully self-describing `estimatedPartitionSize` histogram, which needs
    //     NO version gates.
    //   * `total_rows` becomes 0 meaning "not authoritatively available" (the
    //     gated walk to `totalRows` is not attempted without gates), NEVER a
    //     guessed count.
    assert_none_gates_honest(keyspace, prefix, &bytes, &stats_path, want_partitions);

    true
}

/// Parse `bytes` with `None` gates through the public entry point and assert the
/// HONEST no-heuristics behavior: `partition_count` is the authoritative
/// self-describing count, but `total_rows`/`live_rows` are 0 (not authoritatively
/// available without gates — never a guess).
fn assert_none_gates_honest(
    keyspace: &str,
    prefix: &str,
    bytes: &[u8],
    stats_path: &std::path::Path,
    want_partitions: u64,
) {
    let (_rest, stats) = parse_statistics_with_fallback(bytes, None)
        .unwrap_or_else(|e| panic!("parse {} [None-gates] failed: {e:?}", stats_path.display()));
    let row = &stats.row_stats;
    assert_eq!(
        row.partition_count,
        want_partitions,
        "{keyspace}/{prefix} [None-gates]: partition_count is self-describing and must \
         still be authoritative ({})",
        stats_path.display()
    );
    assert_eq!(
        row.total_rows,
        0,
        "{keyspace}/{prefix} [None-gates]: total_rows must be 0 (unavailable without \
         gates), NOT synthesized from guessed nb gates ({})",
        stats_path.display()
    );
    assert_eq!(
        row.live_rows, 0,
        "{keyspace}/{prefix} [None-gates]: live_rows must stay 0"
    );
}

/// Parse `bytes` with the given `gates` and assert the authoritative counts.
/// `path_label` distinguishes the gates-provided vs None-gates entry point in
/// failure messages.
#[allow(clippy::too_many_arguments)]
fn assert_row_counts(
    keyspace: &str,
    prefix: &str,
    bytes: &[u8],
    stats_path: &std::path::Path,
    gates: Option<&VersionGates>,
    path_label: &str,
    want_partitions: u64,
    want_total_rows: u64,
) {
    let (_rest, stats) = parse_statistics_with_fallback(bytes, gates).unwrap_or_else(|e| {
        panic!(
            "parse {} [{path_label}] failed: {e:?}",
            stats_path.display()
        )
    });

    let row = &stats.row_stats;
    assert_eq!(
        row.partition_count,
        want_partitions,
        "{keyspace}/{prefix} [{path_label}]: partition_count must be the authoritative \
         STATS count (Σ estimatedPartitionSize buckets), not the old stub 0 ({})",
        stats_path.display()
    );
    assert_eq!(
        row.total_rows,
        want_total_rows,
        "{keyspace}/{prefix} [{path_label}]: total_rows must be the authoritative STATS \
         totalRows, not the old stub 0 ({})",
        stats_path.display()
    );
    // `live_rows` is intentionally left 0 (not authoritatively derivable from
    // STATS as a value distinct from total_rows; #28 no-heuristics).
    assert_eq!(
        row.live_rows, 0,
        "{keyspace}/{prefix} [{path_label}]: live_rows is left 0 (not authoritatively \
         derivable); a nonzero value would be a fabricated count"
    );
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

    // With CQLITE_DATASETS_ROOT configured, at least one fixture MUST have been
    // exercised. A configured-but-nothing-asserted run is a false-pass (the
    // dataset is present but empty/wrong), which the project rule forbids —
    // "never let a dataset-dependent test pass on an empty dataset". The clean
    // skip is only allowed when no dataset root is configured at all (handled
    // by the early return above).
    println!("[INFO] #1325: asserted authoritative counts for {asserted}/3 fixtures");
    assert!(
        asserted > 0,
        "CQLITE_DATASETS_ROOT is configured but no #1325 fixture Statistics.db was \
         exercised — refusing to false-pass on an empty/missing dataset"
    );
}

/// Locate ALL `-Statistics.db` binaries under any `<root>/<keyspace>/<table>`
/// directory whose filename version segment is in `versions` (e.g. `["oa", "da"]`),
/// sorted for determinism. Callers iterate candidates because a single fixture's
/// gated `total_rows` can legitimately be unavailable (an unmodeled improved
/// min/max block blocks the walk) even though the file is valid.
///
/// Every table directory is scanned exhaustively: ALL generations' matching
/// `*-Statistics.db` files are collected, not just the first (`find_statistics_db`
/// stops at the first hit, which could miss the oa/da SSTable that proves the
/// gated-nonzero path or that would violate the None-gates safety assertion).
fn find_all_stats_db_for_versions(versions: &[&str]) -> Vec<PathBuf> {
    let Some(root) = datasets_root() else {
        return Vec::new();
    };
    let mut found: Vec<PathBuf> = Vec::new();
    let Ok(ks_iter) = fs::read_dir(&root) else {
        return Vec::new();
    };
    for ks in ks_iter.flatten() {
        let ks_path = ks.path();
        if !ks_path.is_dir() {
            continue;
        }
        let Ok(tbl_iter) = fs::read_dir(&ks_path) else {
            continue;
        };
        for tbl in tbl_iter.flatten() {
            let tbl_path = tbl.path();
            if !tbl_path.is_dir() {
                continue;
            }
            // Enumerate EVERY entry in the table dir and keep every matching
            // `*-Statistics.db` (all generations), not just the first one.
            let Ok(file_iter) = fs::read_dir(&tbl_path) else {
                continue;
            };
            for file in file_iter.flatten() {
                let name_os = file.file_name();
                let name = name_os.to_str().unwrap_or("");
                if name.starts_with("._") {
                    continue;
                }
                if !name.ends_with("-Statistics.db") || name.ends_with("-Statistics.db.txt") {
                    continue;
                }
                let version = name.split('-').next().unwrap_or("");
                if versions.contains(&version) {
                    found.push(file.path());
                }
            }
        }
    }
    found.sort();
    found
}

/// #1325 (roborev safety property): a NON-nb (`oa`/`da`) Statistics.db fed through
/// the `None`-gates public entry point must NOT yield a fabricated nonzero
/// `total_rows`. Statistics.db does not self-describe its version, so the
/// `None`-gates path cannot synthesize `nb` gates (that guess could mis-walk the
/// oa/da min/max block and expose a bogus count). It must report `total_rows == 0`
/// (honest/unavailable) while the gates-provided path (version from the filename)
/// still decodes the authoritative count.
///
/// SKIP cleanly when no oa/da fixture is available; but when one IS present the
/// None-gates safety assertion MUST run for EVERY candidate (no vacuous pass).
///
/// Finding #2 (roborev): `read_table_counts` can legitimately return
/// unavailable/0 for a valid oa/da file whose improved min/max block cannot be
/// traversed (the walk stops before `totalRows`). Asserting the gated path
/// decodes a nonzero on the FIRST fixture is therefore fragile. Instead we
/// iterate candidates: the None-gates-never-fabricates assertion applies to
/// EVERY candidate, and the gated-path-still-works (nonzero) assertion is
/// applied to whichever fixture first yields an authoritative gated
/// `total_rows > 0` (skip cleanly if none is decodable).
#[test]
fn oa_da_none_gates_never_fabricates_total_rows() {
    let candidates = find_all_stats_db_for_versions(&["oa", "da"]);
    if candidates.is_empty() {
        println!(
            "[SKIP] no oa/da Statistics.db fixture available under datasets root \
             (CQLITE_DATASETS_ROOT set: {})",
            datasets_root().is_some()
        );
        return;
    }

    let mut gated_nonzero_verified: Option<(PathBuf, u64)> = None;

    for stats_path in &candidates {
        let bytes = fs::read(stats_path)
            .unwrap_or_else(|e| panic!("read {} failed: {e}", stats_path.display()));

        // Unsafe path (must be CLOSED for EVERY candidate): None gates → format is
        // unknown → total_rows must be honest 0, NEVER a fabricated nonzero from
        // synthesized nb gates.
        let (_rest, none_stats) = parse_statistics_with_fallback(&bytes, None)
            .unwrap_or_else(|e| panic!("parse {} [None] failed: {e:?}", stats_path.display()));
        assert_eq!(
            none_stats.row_stats.total_rows,
            0,
            "SAFETY: an oa/da Statistics.db through the None-gates public entry point must \
             report total_rows=0 (unavailable), NOT a fabricated count from guessed nb gates ({})",
            stats_path.display()
        );
        assert_eq!(
            none_stats.row_stats.live_rows,
            0,
            "None-gates live_rows must stay 0 ({})",
            stats_path.display()
        );

        // Safe path (must still WORK for at least one fixture): gates derived from
        // the filename are authoritative for the file's real version. Some valid
        // fixtures legitimately report unavailable/0 (unmodeled improved min/max
        // block blocks the walk) — those do NOT falsify the fix, so only record
        // the first candidate that yields an authoritative nonzero.
        if gated_nonzero_verified.is_none() {
            let gates = VersionGates::from_path(stats_path)
                .unwrap_or_else(|e| panic!("derive gates from {}: {e:?}", stats_path.display()));
            let (_rest, gated_stats) = parse_statistics_with_fallback(&bytes, Some(&gates))
                .unwrap_or_else(|e| panic!("parse {} [gated] failed: {e:?}", stats_path.display()));
            if gated_stats.row_stats.total_rows > 0 {
                gated_nonzero_verified =
                    Some((stats_path.clone(), gated_stats.row_stats.total_rows));
            }
        }
    }

    match gated_nonzero_verified {
        Some((path, total_rows)) => println!(
            "[INFO] #1325 safety: {} candidate(s) validated None-gates=0; gated path proven on \
             {} → total_rows={} (authoritative)",
            candidates.len(),
            path.display(),
            total_rows
        ),
        None => println!(
            "[SKIP-GATED] {} oa/da candidate(s) validated None-gates=0, but none exposed an \
             authoritatively gated total_rows>0 (all improved min/max blocks unmodeled); \
             None-gates-never-fabricates still verified",
            candidates.len()
        ),
    }
}
