//! Issue #4201 (spec R8.1) — bounded-memory budget lane for `diff_table` over every
//! `test_wide_rows` table (dhat-gated).
//!
//! R8: a full-table diff folds each partition's pair of trails into the running report and drops
//! them before the next partition. The lane stages each `test_wide_rows` table IDENTICALLY on both
//! sides (a pure `equal` case, so the whole enumerate-and-fold path runs without divergence noise)
//! and asserts corpus-wide peak live heap stays inside the 128 MiB project budget AND a tight
//! measured ceiling. Non-vacuous: every table must compare at least one partition with at least
//! one `equal` cell and zero divergence, the table count must be non-zero, and
//! `CQLITE_REQUIRE_FIXTURES=1` turns an absent corpus into a failure (a skip would be a green run
//! that measured nothing).
//!
//! `dhat`'s `max_bytes` is the peak over the profiler's whole lifetime, so the ceiling is a
//! corpus-wide maximum, never one table's peak (same note as `issue_4204_diagnose_deep_memory_budget`).
//!
//! ```text
//! env CQLITE_DATASETS_ROOT=<root> CQLITE_REQUIRE_FIXTURES=1 \
//!   cargo test --package cqlite-core --features dhat-heap \
//!   --test issue_4201_diff_memory_budget -- --test-threads=1
//! ```

#![cfg(all(feature = "dhat-heap", feature = "write-support"))]

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use cqlite_core::storage::write_engine::snapshot_diff::{
    diff_table, DiffOptions, DiffSide, MAX_EFFECTIVE_BUDGET,
};

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use rebuild_fixtures::{require_fixtures_strict, table_schema};

/// CLAUDE.md's project memory target.
const HEAP_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

/// Tight corpus-wide peak-heap regression net alongside the 128 MiB contract ceiling (without it
/// the budget is ~140x the observed peak and could absorb a total loss of per-partition folding).
///
/// Measured 2026-11-21 on this branch: all 8 `test_wide_rows` tables, 450 partitions, identical
/// sides: corpus-wide peak live heap **936,278 B** (total allocated 266 MB). Ceiling 3 MiB ~= 3.4x
/// headroom.
const CEILING_PEAK_BYTES: u64 = 3 * 1024 * 1024;

const KEYSPACE: &str = "test_wide_rows";
const SCHEMA_FILE: &str = "wide-rows.cql";

fn discover_tables() -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for root in datasets_root::sstables_root_candidates() {
        let Ok(entries) = std::fs::read_dir(root.join(KEYSPACE)) else {
            continue;
        };
        for entry in entries.flatten() {
            let dir_name = entry.file_name().to_string_lossy().to_string();
            let Some((table, id)) = dir_name.rsplit_once('-') else {
                continue;
            };
            if entry.path().is_dir()
                && id.len() == 32
                && id.chars().all(|c| c.is_ascii_hexdigit())
                && !names.iter().any(|n| n == table)
                && datasets_root::sstables_root_for_table(KEYSPACE, table).is_some()
            {
                names.push(table.to_string());
            }
        }
    }
    names.sort();
    names
}

/// A side over every live generation directly in `dir` (newest first), read-only.
fn side_of(dir: &std::path::Path) -> DiffSide {
    let mut found: Vec<(u64, std::path::PathBuf)> = std::fs::read_dir(dir)
        .expect("read table dir")
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_string();
            let stem = name.strip_suffix("-Data.db")?;
            dir.join(format!("{stem}-TOC.txt"))
                .is_file()
                .then_some(())?;
            Some((stem.split('-').nth(1)?.parse::<u64>().ok()?, path))
        })
        .collect();
    found.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    DiffSide {
        dir: dir.to_path_buf(),
        generations: found.into_iter().map(|(_, p)| p).collect(),
        listed_names: Vec::new(),
    }
}

#[test]
#[serial_test::serial]
fn diff_every_wide_rows_table_within_the_heap_budget() {
    let tables = discover_tables();
    if tables.is_empty() {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but no {KEYSPACE} table is present; {}",
                datasets_root::describe_roots()
            );
        }
        eprintln!("[issue_4201] Skipping R8.1 diff memory budget: no {KEYSPACE} fixture present");
        return;
    }
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    // Everything allocated per case BEFORE the profiler starts.
    let cases: Vec<(String, DiffSide, cqlite_core::schema::TableSchema)> = tables
        .iter()
        .map(|t| {
            let root = datasets_root::sstables_root_for_table(KEYSPACE, t).expect("root");
            let dir = datasets_root::table_generation_dirs(&root, KEYSPACE, t)
                .into_iter()
                .next()
                .unwrap_or_else(|| panic!("{KEYSPACE}.{t}: no generation directory"));
            let side = side_of(&dir);
            assert!(
                !side.generations.is_empty(),
                "{KEYSPACE}.{t}: no live generation"
            );
            (t.clone(), side, table_schema(SCHEMA_FILE, t, KEYSPACE))
        })
        .collect();

    let _profiler = dhat::Profiler::builder().testing().build();
    // Both the default budget and the LARGEST request (`u64::MAX`, clamped to
    // `MAX_EFFECTIVE_BUDGET`): the peak must stay inside the 128 MiB target either way.
    assert!(
        MAX_EFFECTIVE_BUDGET * 4 <= HEAP_BUDGET_BYTES,
        "cap leaves room for trails + fold"
    );
    let mut compared = 0u64;
    for budget in [64 * 1024 * 1024, u64::MAX] {
        let options = DiffOptions {
            now_secs: 1_794_545_000,
            max_result_bytes: budget,
        };
        for (table, side, schema) in &cases {
            let report = rt
                .block_on(diff_table(
                    side,
                    side,
                    &format!("{KEYSPACE}.{table}"),
                    schema,
                    None,
                    &options,
                ))
                .unwrap_or_else(|e| panic!("{KEYSPACE}.{table}: {e}"));
            assert!(
                report.summary.partitions_compared > 0,
                "{table}: compared no partition"
            );
            assert!(
                report.summary.equal > 0,
                "{table}: no equal cell (scanned nothing)"
            );
            assert_eq!(
                report.summary.only_a
                    + report.summary.only_b
                    + report.summary.newer_in_a
                    + report.summary.newer_in_b
                    + report.summary.conflicts
                    + report.summary.tombstones_only_a
                    + report.summary.tombstones_only_b,
                0,
                "{table}: identical sides must not diverge"
            );
            compared += report.summary.partitions_compared;
        }
    }
    let stats = dhat::HeapStats::get();
    eprintln!(
        "[issue_4201] {} tables, {compared} partitions; corpus-wide peak {} B, total {} B",
        cases.len(),
        stats.max_bytes,
        stats.total_bytes
    );
    assert!(compared > 0, "measured nothing");
    assert!(stats.total_bytes > 0, "profiler observed no allocation");
    assert!(
        (stats.max_bytes as u64) < HEAP_BUDGET_BYTES,
        "peak live heap {} B exceeds the 128 MiB budget",
        stats.max_bytes
    );
    assert!(
        (stats.max_bytes as u64) <= CEILING_PEAK_BYTES,
        "peak live heap {} B exceeds the pinned ceiling {CEILING_PEAK_BYTES} B",
        stats.max_bytes
    );
}
