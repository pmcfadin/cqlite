//! Issue #4204 (spec R3.3) — bounded-memory budget lane for `diagnose --deep` over every
//! `test_wide_rows` table (dhat-gated).
//!
//! R3.3: `--deep` holds ONE partition's worth of structure plus the bounded histograms and top-N
//! lists, never the table. The lane runs `diagnose_table(.., deep: true, top_n: 5)` over EVERY
//! `test_wide_rows` table, twice per table — without a schema (scan only) and with the committed
//! `wide-rows.cql` (adds the dry-compaction reclaim prediction) — under the dhat profiler, and
//! asserts corpus-wide peak live heap stays inside the 128 MiB project budget AND a tight
//! measured ceiling. Non-vacuous: each table must report a populated `deep` for every generation,
//! the table count must be non-zero, and `CQLITE_REQUIRE_FIXTURES=1` turns an absent corpus into
//! a failure (a skip would be a green run that measured nothing).
//!
//! `dhat`'s `max_bytes` is the peak over the profiler's whole lifetime, so the peak ceiling is a
//! corpus-wide maximum, never one table's peak (same note as `issue_4197_rebuild_memory_budget`).
//!
//! ```text
//! env CQLITE_DATASETS_ROOT=<root> CQLITE_REQUIRE_FIXTURES=1 \
//!   cargo test --package cqlite-core --features dhat-heap \
//!   --test issue_4204_diagnose_deep_memory_budget -- --test-threads=1
//! ```

#![cfg(all(
    feature = "dhat-heap",
    feature = "write-support",
    not(feature = "tombstones")
))]

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use cqlite_core::storage::sstable::diagnose::{diagnose_table, DiagnoseOptions};

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use rebuild_fixtures::{require_fixtures_strict, table_schema};

/// CLAUDE.md's project memory target (the constant the compaction/rebuild lanes pin).
const HEAP_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

/// Tight corpus-wide peak-heap regression net alongside the 128 MiB contract ceiling (without it
/// the budget is ~330x the observed peak and could absorb a total loss of streaming).
///
/// Measured 2026-11-13 on this branch: all 8 `test_wide_rows` tables, scan-only AND with-schema
/// (16 generation-runs): corpus-wide peak live heap **381,355 B** (total allocated 36.9 MB).
/// Ceiling 1 MiB ≈ 2.7x headroom. Peak is O(one partition + bounded histograms/top-N), and the
/// per-run partition-start bookkeeping is a single open partition, never a per-partition vector.
const CEILING_PEAK_BYTES: u64 = 1024 * 1024;

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

#[test]
#[serial_test::serial]
fn deep_diagnose_every_wide_rows_table_within_the_heap_budget() {
    let tables = discover_tables();
    if tables.is_empty() {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but no {KEYSPACE} table is present; {}",
                datasets_root::describe_roots()
            );
        }
        eprintln!("[issue_4204] Skipping R3.3 deep memory budget: no {KEYSPACE} fixture present");
        return;
    }
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    // Everything allocated per case BEFORE the profiler starts.
    let cases: Vec<(String, std::path::PathBuf, cqlite_core::schema::TableSchema)> = tables
        .iter()
        .map(|t| {
            let root = datasets_root::sstables_root_for_table(KEYSPACE, t).expect("root");
            let dir = datasets_root::table_generation_dirs(&root, KEYSPACE, t)
                .into_iter()
                .next()
                .unwrap_or_else(|| panic!("{KEYSPACE}.{t}: no generation directory"));
            (t.clone(), dir, table_schema(SCHEMA_FILE, t, KEYSPACE))
        })
        .collect();

    let _profiler = dhat::Profiler::builder().testing().build();
    let mut measured_generations = 0usize;
    let mut gaps: Vec<String> = Vec::new();
    for (table, dir, schema) in &cases {
        for with_schema in [false, true] {
            let options = DiagnoseOptions {
                now_secs: 1_794_545_000,
                deep: true,
                top_n: 5,
                gc_grace_seconds: Some(864_000),
                schema: with_schema.then(|| schema.clone()),
            };
            let report = rt
                .block_on(diagnose_table(dir, &options))
                .unwrap_or_else(|e| panic!("{KEYSPACE}.{table} (schema={with_schema}): {e}"));
            if !report.unreadable_generations.is_empty() {
                gaps.push(format!("{table} (schema={with_schema})"));
                continue;
            }
            assert!(
                !report.generations.is_empty(),
                "{table}: no generation reported"
            );
            for g in &report.generations {
                let deep = g.deep.as_ref().unwrap_or_else(|| {
                    panic!(
                        "{table} gen {}: --deep produced no deep report",
                        g.generation
                    )
                });
                assert!(
                    deep.partition_size_histogram
                        .iter()
                        .map(|b| b.2)
                        .sum::<u64>()
                        > 0,
                    "{table} gen {}: empty size histogram (scanned nothing)",
                    g.generation
                );
                measured_generations += 1;
            }
        }
    }
    let stats = dhat::HeapStats::get();
    eprintln!(
        "[issue_4204] measured {measured_generations} generation-runs over {} tables; corpus-wide peak {} B, total {} B, gaps {gaps:?}",
        cases.len(),
        stats.max_bytes,
        stats.total_bytes
    );
    assert!(measured_generations > 0, "measured nothing");
    assert!(
        gaps.is_empty(),
        "tables whose generation could not be diagnosed: {gaps:?}"
    );
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
