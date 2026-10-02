//! Issue #4197 (spec R6.1) — bounded-memory budget lane for
//! `rebuild_components` over `test_wide_rows` (dhat-gated).
//!
//! **What it pins.** Spec R6 says rebuild holds at most ONE partition's
//! structural state resident on the read side (Index/Summary/Filter/CRC) and
//! one partition's decoded mutations for a `statistics` rebuild. R6.1 makes
//! that measurable: run `rebuild_components` requesting EVERY component
//! INCLUDING `statistics` (the expensive one — a full per-partition decode)
//! over EVERY committed `test_wide_rows` table under the dhat profiler, and
//! assert peak live heap stays within the same budget the existing
//! single-input compaction lane uses.
//!
//! **The budget, and where it comes from.** 128 MiB — CLAUDE.md's project
//! memory target, and the exact constant the k-way-merge/compaction memory
//! lane pins (`test_issue_827_merge_streaming_memory.rs`'s
//! `HEAP_BUDGET_BYTES`, and `memory_budget.rs`'s `PROJECT_HEAP_BUDGET_BYTES`).
//! Same pattern, same budget, per R6.1's wording: a rebuild of a table must
//! not cost more resident heap than a single-input compaction of it.
//!
//! **Non-vacuous by construction.** Per table the guard asserts the run was
//! NOT refused, that it regenerated at least one component, and that the
//! profiler observed a non-zero allocation volume, BEFORE any ceiling is
//! checked. A present fixture that rebuilt nothing is a setup FAILURE, never a
//! passing 0-byte budget. Across the corpus it additionally asserts a non-zero
//! table count and reconciles the set of tables whose decode fails against a
//! DECLARED list (see `DECODE_GAP_TABLES`) — a new failure FAILs, and so does
//! a stale declaration.
//!
//! **Which figures are per-table and which are corpus-wide.** `dhat`'s
//! `max_bytes` is the peak over the PROFILER's lifetime and only one profiler
//! may exist per process, so the peak-heap ceilings here are corpus-wide
//! maxima — never a single table's peak (sampling `max_bytes` per iteration
//! yields a monotone non-decreasing series). The genuinely per-table figures
//! are the `total_bytes` delta (allocation volume) and the `curr_bytes`
//! reading (live heap still resident), and it is the latter that bounds state
//! carried from one table to the next (`CEILING_RETAINED_BYTES`, whose doc
//! comment records exactly what that bound does and does not claim).
//!
//! **Corpus discovered from disk** (issue #1229): the table list is read from
//! the dataset root, never hard-coded, so a ninth `test_wide_rows` table is
//! covered the day it is fetched.
//!
//! ## Run via:
//! ```text
//! env CQLITE_DATASETS_ROOT=<root> \
//!   cargo test --package cqlite-core --features cli-helpers,dhat-heap,arrow \
//!   --test issue_4197_rebuild_memory_budget -- --test-threads=1
//! ```
//! (`--test-threads=1` is mandatory: `dhat::Profiler` installs a process-global
//! allocator and permits only one live profiler per process.)

#![cfg(all(
    feature = "dhat-heap",
    feature = "write-support",
    not(feature = "tombstones")
))]

// The dhat allocator must be the global allocator to observe every allocation.
// This test binary is separate from all others, so installing it here does not
// affect normal builds or other test binaries.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::path::PathBuf;

use cqlite_core::storage::write_engine::rebuild::{rebuild_components, Component, RebuildOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use rebuild_fixtures::{copy_fixture_dir, require_fixtures_strict, single_data_db, table_schema};

/// CLAUDE.md's project memory target, and the constant the existing
/// single-input compaction memory lane pins — R6.1's "existing lane threshold".
const HEAP_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

/// A TIGHT peak-heap regression net alongside the 128 MiB contract ceiling —
/// the same two-ceiling shape `memory_budget.rs` uses (a pinned measured value
/// "also < 128 MiB"). Without it the R6.1 budget would be ~50x the observed
/// peak and could absorb a total loss of streaming without reddening.
///
/// Measured 2026-10-02 on this branch, all 7 measurable `test_wide_rows`
/// tables, all components including `statistics`: CORPUS-WIDE peak live heap
/// 2,601,427–2,602,003 B across repeated runs. Ceiling 4 MiB ≈ 1.6x headroom
/// over the high reading; peak varies more than allocation totals do, so the
/// slack is deliberately wider than a counts-based net would need.
///
/// **What this is and is not.** `dhat::HeapStats::max_bytes` is the peak over
/// the PROFILER's whole lifetime, so sampling it inside the per-table loop
/// yields a monotone non-decreasing series: a "peak" read after table N is the
/// running corpus-wide maximum at that point, NOT that table's own peak, and
/// a flat series would say nothing about accumulation. The per-table signals
/// here are therefore the two deltas that genuinely are per-table —
/// `total_bytes` (allocation volume) and `curr_bytes` (live-heap retention,
/// see [`CEILING_RETAINED_BYTES`]) — and `max_bytes` is asserted only as the
/// corpus-wide ceiling it actually measures.
const CEILING_PEAK_BYTES: u64 = 4 * 1024 * 1024;

/// Retention net: live heap STILL RESIDENT at the end of each table's
/// rebuild. Unlike `max_bytes` this is a genuinely PER-TABLE read, and it is
/// the one that can show state carried from one table to the next — a rebuild
/// retaining a partition's worth of structure per table makes this grow with
/// the iteration count and cross the ceiling.
///
/// Measured 2026-10-02, across several runs of all 7 measurable tables:
/// 14,859–17,163 B after each table. Ceiling 64 KiB ≈ 3.7x headroom over the
/// high reading, which still reddens on retention as small as ~7 KB per table
/// across this corpus.
///
/// **What is NOT claimed.** The series is *approximately* flat but not
/// run-to-run stable: observed runs vary between bit-identical (14,859 B after
/// every table) and a few-hundred-bytes-per-table upward drift, and the
/// allocation TOTALS move slightly between runs too, so the residual is
/// process-global state outside the rebuild (interning/one-cell caches), not
/// per-partition structure. A first-vs-last "nothing accumulates" equality
/// was written, measured, and REMOVED for that reason — it reddened on a clean
/// tree. So what this lane asserts is a BOUND on per-table residency, not
/// bit-flatness; the full series is printed for diagnosis either way. The
/// per-table `curr_bytes` DELTA reads +5,217 B rather than ~0 only because the
/// `RebuildReport` is still alive at the sample point and is dropped at the end
/// of the iteration — hence the ABSOLUTE value is what is asserted.
///
/// (Probe-verified: injecting a synthetic 2 KiB-per-table leak into the loop
/// makes the series rise monotonically, 16,907 B → 31,499 B.)
const CEILING_RETAINED_BYTES: usize = 64 * 1024;

const KEYSPACE: &str = "test_wide_rows";
const SCHEMA_FILE: &str = "wide-rows.cql";

/// Every component, `statistics` included (R6.1: "requesting every component
/// including `statistics`"). `crc` is correctly `skipped_not_applicable` for a
/// compressed input, which costs nothing and is not an error.
const ALL_COMPONENTS: [Component; 7] = [
    Component::Index,
    Component::Summary,
    Component::Filter,
    Component::Digest,
    Component::Toc,
    Component::Crc,
    Component::Statistics,
];

/// Tables whose FULL DECODE hits a PRE-EXISTING gap unrelated to rebuild, so
/// `rebuild_components` returns `Err` rather than a measurable run.
///
/// `wide_partition_table` has 5 clustering columns including a `DATE` one and
/// hits the shared clustering-comparator's "Type mismatch … comparator=Date"
/// error in `ClusteringKey::compare`/`merge_entry_to_mutation` — the same path
/// a COMPACTION of this table would take, documented in
/// `issue_4197_rebuild_index_parity.rs` and out of scope for #4197.
///
/// This list is a fail-closed reconciliation, not an excusal: the test FAILs if
/// any OTHER table errors, and FAILs just as loudly if a table named here
/// succeeds (the declaration is then stale and must be deleted).
const DECODE_GAP_TABLES: [&str; 1] = ["wide_partition_table"];

/// Every `test_wide_rows` table name present on disk, discovered from the
/// dataset corpus (issue #1229 — never a hard-coded list/count). A generation
/// directory is named `<table>-<32-hex-id>`.
fn discover_tables() -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for root in datasets_root::sstables_root_candidates() {
        let keyspace_dir = root.join(KEYSPACE);
        let Ok(entries) = std::fs::read_dir(&keyspace_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if !entry.path().is_dir() {
                continue;
            }
            let dir_name = entry.file_name().to_string_lossy().to_string();
            let Some((table, id)) = dir_name.rsplit_once('-') else {
                continue;
            };
            if table.is_empty()
                || id.len() != 32
                || !id.chars().all(|c| c.is_ascii_hexdigit())
                || names.iter().any(|n| n == table)
            {
                continue;
            }
            // Resolve the root PER TABLE (issue #3220) — a name discovered
            // under one candidate root must still resolve to a root that
            // actually holds its `Data.db`.
            if datasets_root::sstables_root_for_table(KEYSPACE, table).is_some() {
                names.push(table.to_string());
            }
        }
    }
    names.sort();
    names
}

/// One table's disposable working copy: the fixture's whole component set in a
/// `TempDir`, with every DERIVED component deleted so the rebuild really has to
/// produce them. `Statistics.db` is kept — it is the authoritative
/// `EncodingStats` baseline an `index` rebuild must take (spec R2), and this
/// lane measures memory, not the R2.5 refusal path.
struct Prepared {
    table: String,
    schema: cqlite_core::schema::TableSchema,
    data_db: PathBuf,
    out_dir: PathBuf,
    // Keeps the temp tree alive for the duration of the run.
    _temp: TempDir,
}

fn prepare(table: &str) -> Prepared {
    let root = datasets_root::sstables_root_for_table(KEYSPACE, table)
        .unwrap_or_else(|| panic!("{KEYSPACE}.{table}: resolved above, must resolve here"));
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, table)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{table}: no usable generation directory"));
    let schema = table_schema(SCHEMA_FILE, table, KEYSPACE);
    let temp = TempDir::new().expect("tempdir");
    let working = copy_fixture_dir(&fixture_dir, temp.path());
    let data_db = single_data_db(&working);
    let prefix = data_db
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .trim_end_matches("Data.db")
        .to_string();
    for suffix in [
        "Index.db",
        "Summary.db",
        "Filter.db",
        "Digest.crc32",
        "TOC.txt",
    ] {
        let path = working.join(format!("{prefix}{suffix}"));
        // Not every fixture carries every derived component; only delete what
        // is there (the rebuild regenerates the requested set either way).
        let _ = std::fs::remove_file(path);
    }
    let out_dir = temp.path().join("out");
    Prepared {
        table: table.to_string(),
        schema,
        data_db,
        out_dir,
        _temp: temp,
    }
}

/// R6.1 — an all-components (incl. `statistics`) rebuild of every
/// `test_wide_rows` table stays within the compaction lane's 128 MiB peak-heap
/// budget.
///
/// A single `#[test]` covers the whole corpus: `dhat` installs a process-wide
/// global allocator and permits only ONE live `Profiler`, so a second dhat test
/// in this binary would panic in `Profiler::build`.
#[test]
#[serial_test::serial]
fn rebuild_every_wide_rows_table_within_the_compaction_heap_budget() {
    let tables = discover_tables();
    if tables.is_empty() {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but no {KEYSPACE} table is present; {}",
                datasets_root::describe_roots()
            );
        }
        eprintln!(
            "[issue_4197] Skipping R6.1 rebuild memory budget: no {KEYSPACE} fixture present \
             (run test-data/scripts/fetch-datasets.sh + export the CQLITE_DATASETS_ROOT line it \
             prints). {}",
            datasets_root::describe_roots()
        );
        return;
    }

    // Build the runtime and every working copy BEFORE the profiler starts, so
    // fixture copying and runtime setup are not attributed to the rebuild's
    // own peak (the same discipline memory_budget.rs / #827 use).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let prepared: Vec<Prepared> = tables.iter().map(|t| prepare(t)).collect();

    // Live heap still resident at the end of each measured table — the
    // per-table series that CAN show accumulation (see CEILING_RETAINED_BYTES).
    // Pre-allocated to capacity BEFORE the profiler starts, and holding plain
    // `(index, bytes)` rather than owned `String`s, so this bookkeeping
    // allocates NOTHING inside the measured loop: an earlier draft pushed
    // `(String, usize)` into a growing `Vec` and the series then drifted
    // ~360 B across the corpus — the test measuring its own notes rather than
    // the rebuild.
    let mut resident_after: Vec<(usize, usize)> = Vec::with_capacity(prepared.len());
    let mut decode_gaps: Vec<String> = Vec::with_capacity(prepared.len());

    let _profiler = dhat::Profiler::builder().testing().build();

    let mut measured = 0usize;
    for (idx, case) in prepared.iter().enumerate() {
        let options = RebuildOptions {
            out_dir: case.out_dir.clone(),
            statistics_recovery_source: None,
        };
        let before = dhat::HeapStats::get();
        let result = rt.block_on(rebuild_components(
            &case.data_db,
            &case.schema,
            &ALL_COMPONENTS,
            &options,
        ));
        let after = dhat::HeapStats::get();

        let report = match result {
            Ok(report) => report,
            Err(e) => {
                // A pre-existing decode gap, or a NEW failure — reconciled
                // against DECODE_GAP_TABLES after the loop.
                eprintln!(
                    "[issue_4197] {KEYSPACE}.{}: rebuild_components returned Err: {e}",
                    case.table
                );
                decode_gaps.push(case.table.clone());
                continue;
            }
        };

        assert!(
            report.refused.is_none(),
            "{KEYSPACE}.{}: a healthy committed fixture must not refuse — a refused run stops \
             walking early and makes this memory measurement vacuous; refusal={:?}",
            case.table,
            report.refused
        );
        assert!(
            !report.regenerated.is_empty(),
            "{KEYSPACE}.{}: regenerated nothing — a rebuild that produced no component measures \
             no memory; report={report:?}",
            case.table
        );
        let allocated = after.total_bytes.saturating_sub(before.total_bytes);
        assert!(
            allocated > 0,
            "{KEYSPACE}.{}: the profiler observed 0 allocated bytes across an all-components \
             rebuild — the measurement did not happen; refusing a vacuous pass",
            case.table
        );
        // A TRUE per-table figure (unlike `max_bytes`, which is the profiler's
        // lifetime peak): live heap still resident now, and the signed delta
        // across this table's rebuild.
        let retained = after.curr_bytes;
        let retained_delta = after.curr_bytes as i64 - before.curr_bytes as i64;
        assert!(
            retained <= CEILING_RETAINED_BYTES,
            "{KEYSPACE}.{}: {retained} B of live heap is still resident after this table's \
             rebuild, over the pinned {CEILING_RETAINED_BYTES} B non-accumulation ceiling \
             (measured 14,859-17,163 B). Growth here across the loop means rebuild \
             is carrying state from one table to the next — investigate before re-pinning.",
            case.table
        );
        // The two `max_bytes` ceilings are CORPUS-WIDE, not per-table: dhat's
        // `max_bytes` is the peak over the profiler's whole lifetime, so this
        // is the running maximum observed through the end of this table.
        // Asserted in-loop purely to fail fast on the table that crosses it.
        assert!(
            (after.max_bytes as u64) <= HEAP_BUDGET_BYTES,
            "corpus-wide peak live heap reached {} B through {KEYSPACE}.{} — over the {} B \
             ({} MiB) budget a single-input compaction of the same table is held to (spec \
             R6.1). Rebuild must hold at most ONE partition's structural state (and, for \
             `statistics`, one partition's decoded mutations) resident.",
            after.max_bytes,
            case.table,
            HEAP_BUDGET_BYTES,
            HEAP_BUDGET_BYTES / (1024 * 1024)
        );
        assert!(
            (after.max_bytes as u64) <= CEILING_PEAK_BYTES,
            "corpus-wide peak live heap reached {} B through {KEYSPACE}.{} — over the PINNED \
             ceiling {} B (measured ~2.60 MB corpus-wide). Still within the 128 MiB contract \
             budget, but a jump this size means rebuild stopped holding one partition at a time \
             — investigate before re-pinning this constant.",
            after.max_bytes,
            case.table,
            CEILING_PEAK_BYTES
        );
        eprintln!(
            "[issue_4197] R6.1 {KEYSPACE}.{}: regenerated {:?}; {allocated} B allocated, \
             {retained} B still resident (delta {retained_delta:+}); corpus-wide peak live \
             heap through this table {} B (budget {} B)",
            case.table, report.regenerated, after.max_bytes, HEAP_BUDGET_BYTES
        );
        resident_after.push((idx, after.curr_bytes));
        measured += 1;
    }

    // Fail-closed reconciliation of the declared decode gap (never a silent
    // excusal list): the errored set must be EXACTLY the declared one.
    let mut expected_gaps: Vec<String> = DECODE_GAP_TABLES
        .iter()
        .filter(|t| tables.iter().any(|present| present == *t))
        .map(|t| t.to_string())
        .collect();
    expected_gaps.sort();
    decode_gaps.sort();
    assert_eq!(
        decode_gaps, expected_gaps,
        "the set of {KEYSPACE} tables whose rebuild decode FAILS must be exactly the declared \
         DECODE_GAP_TABLES list. A table in `actual` but not `expected` is a NEW regression; one \
         in `expected` but not `actual` means the gap is fixed and the declaration is stale — \
         delete it. actual={decode_gaps:?} expected={expected_gaps:?} discovered={tables:?}"
    );

    // Affirmative measurement (CLAUDE.md: a census reports `0 RECOGNISED`,
    // never a bare 0) — and a corpus that measured nothing is a FAILURE.
    assert!(
        measured > 0,
        "0 RECOGNISED: {} {KEYSPACE} tables were discovered but NONE produced a measurable \
         rebuild; refusing a vacuous budget pass. discovered={tables:?}",
        tables.len()
    );
    // The resident series, REPORTED for diagnosis. The merge-gating bound on
    // it is the per-table `CEILING_RETAINED_BYTES` assert in the loop above;
    // this file deliberately does NOT assert first-vs-last flatness — see that
    // constant's doc comment for why (the series is not run-to-run stable).
    let series: Vec<(&str, usize)> = resident_after
        .iter()
        .map(|(idx, bytes)| (prepared[*idx].table.as_str(), *bytes))
        .collect();

    let stats = dhat::HeapStats::get();
    eprintln!(
        "[issue_4197] R6.1: {measured} of {} discovered {KEYSPACE} tables measured ({} declared \
         decode gap(s)); corpus-wide peak live heap {} B <= {} B budget (pinned ceiling {} B). \
         Resident-after-each-table series (the per-table accumulation signal): {series:?}",
        tables.len(),
        expected_gaps.len(),
        stats.max_bytes,
        HEAP_BUDGET_BYTES,
        CEILING_PEAK_BYTES
    );
    assert!(
        (stats.max_bytes as u64) <= HEAP_BUDGET_BYTES,
        "corpus-wide peak live heap {} B exceeded the {} B budget",
        stats.max_bytes,
        HEAP_BUDGET_BYTES
    );
    assert!(
        (stats.max_bytes as u64) <= CEILING_PEAK_BYTES,
        "corpus-wide peak live heap {} B exceeded the pinned ceiling {} B",
        stats.max_bytes,
        CEILING_PEAK_BYTES
    );
}
