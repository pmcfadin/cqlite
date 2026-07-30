//! Issue #3129: the **1-GENERATION-vs-N-GENERATION** differential-equality axis,
//! a second axis of the #1918 point-vs-full lane (whose corpus, pinned-`now`
//! contract, SKIP/anti-vacuity contract and result normalizer it reuses verbatim
//! via `super::*`).
//!
//! ## The blind spot this closes
//!
//! The parent lane's only axis is *point vs full at a FIXED generation count*.
//! Both of its arms route through the SAME reconciliation kernel for a given
//! fixture, so a divergence between the **single-generation** read path
//! (`SSTableReader` + the row-level shadow decision) and the **multi-generation**
//! merge kernel (`generation_merge.rs`: `merge_generations_for_read`,
//! `..._with_metadata`, `stream_generations_for_read`,
//! `seek_merge_generations_for_read`) is invisible to it: both arms reproduce the
//! same wrong answer and the lane stays green. That is exactly how #3129 (a
//! phantom all-null row inside a deleted partition, visible only at ≥2
//! generations) had to be found by inspection.
//!
//! Consequence for a user: the answer to a `SELECT` would depend on the table's
//! compaction state (N generations → 1). This axis makes that class of
//! non-determinism a **test failure**, not a code-review finding.
//!
//! ## How the same logical content is materialized at 1 and at N generations
//!
//! Compaction is NOT used (it would drag the write path in as a second variable,
//! and a CQLite-written-and-CQLite-read pair is invariant to a uniform error,
//! #3042). Instead each case starts from a fixture directory that holds EXACTLY
//! ONE Cassandra-written generation and materializes two temp trees from those
//! same bytes:
//!
//!   * the **1-gen** tree: a byte copy of the single generation, and
//!   * the **N-gen** tree: the same generation copied N times under distinct
//!     generation numbers (`nb-3-big-*` → `nb-4-big-*`, …).
//!
//! Both trees hold identical logical content by construction — every cell in
//! every copy carries the same key, the same write timestamp and the same value —
//! so Cassandra's reconciliation rules make the merged result of N copies exactly
//! the result of one copy (a timestamp/value tie reconciles to the same cell;
//! `DeletionTime.deletes(ts) = ts <= markedForDeleteAt` is likewise idempotent).
//! The N-gen tree nevertheless engages the `KWayMerger` cross-generation kernel
//! that a 1-gen read never touches. Any inequality is therefore a defect in that
//! kernel (or in single-gen reconciliation), which is precisely the axis.
//!
//! Note this is a CQLite-vs-CQLite *differential* (like the parent lane), not an
//! oracle: it asserts internal consistency between two read paths over
//! **Cassandra-written bytes**. The Cassandra-oracle counterpart for the absolute
//! result set stays `query_semantics_oracle_parity.rs` / the physical goldens.
//!
//! ## Contracts
//!
//!   * **Pinned `now`** (`CQLITE_TTL_NOW_OVERRIDE_SECS`, `PINNED_NOW_SECS`) — never
//!     wall-clock, so TTL expiry is identical on both arms and across runs.
//!   * **Anti-vacuity** — every case declares the EXACT row count its 1-gen full
//!     scan must return and the number of partitions it must discover. `0 == 0`
//!     can never pass, and a fixture that silently loses rows fails loudly.
//!   * **Fixture-drift detection** — a case whose source directory stops holding
//!     exactly one generation FAILs (the axis would otherwise silently compare
//!     N-vs-M and lose its meaning).
//!   * **SKIP contract** — absent keyspace/`*-Data.db`/schema SKIPs cleanly unless
//!     `CQLITE_REQUIRE_FIXTURES=1` (then it fails closed), matching the parent.
//!   * **Divergence detection is itself tested** — `one_vs_n_comparator_reports_a_seeded_divergence`
//!     feeds the comparator deliberately divergent row sets and asserts the
//!     reported message names the case and shows the diverging rows.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::config::ReadPathMode;
use cqlite_core::query::result::QueryRow;

use super::{
    discover_pk_ints, normalize, open_db, schema_path, skip_or_fail, sstables_root, table_has_data,
    MAX_KEYS_PER_TABLE, PINNED_NOW_SECS, TTL_NOW_OVERRIDE_ENV,
};

/// Sidecar files that live next to the real SSTable components in the committed
/// corpus (`*-Data.db.jsonl` physical goldens, `*-Statistics.db.txt` renders).
/// They are NOT components and are excluded from both materialized trees, so the
/// 1-gen and N-gen trees differ in nothing but generation multiplicity.
fn is_sidecar(name: &str) -> bool {
    name.ends_with(".jsonl") || name.ends_with(".db.txt")
}

/// One SSTable generation's on-disk component files, plus the file-name prefix
/// (`<version>-<generation>-<format>-`) they share.
struct GenerationFiles {
    generation: u64,
    prefix: String,
    files: Vec<PathBuf>,
}

/// Group a table directory's component files by generation number, parsed from the
/// canonical `<version>-<generation>-<format>-<component>` file name (Cassandra 5.0
/// `nb-3-big-Data.db`, `da-1-bti-Data.db`). Authoritative name structure only — no
/// content sniffing.
fn scan_generations(dir: &Path) -> Result<Vec<GenerationFiles>, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("read_dir {}: {e}", dir.display()))?;
    // generation -> (prefix, files)
    let mut by_gen: BTreeMap<u64, (String, Vec<PathBuf>)> = BTreeMap::new();
    for entry in entries {
        let entry = entry.map_err(|e| format!("dir entry in {}: {e}", dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if is_sidecar(name) {
            continue;
        }
        let parts: Vec<&str> = name.splitn(4, '-').collect();
        if parts.len() != 4 {
            return Err(format!(
                "component file {name} in {} does not match \
                 <version>-<generation>-<format>-<component>",
                dir.display()
            ));
        }
        let generation: u64 = parts[1].parse().map_err(|_| {
            format!(
                "component file {name} in {}: generation token {:?} is not an integer",
                dir.display(),
                parts[1]
            )
        })?;
        let prefix = format!("{}-{}-{}-", parts[0], parts[1], parts[2]);
        by_gen
            .entry(generation)
            .or_insert_with(|| (prefix, Vec::new()))
            .1
            .push(path);
    }
    Ok(by_gen
        .into_iter()
        .map(|(generation, (prefix, files))| GenerationFiles {
            generation,
            prefix,
            files,
        })
        .collect())
}

/// A materialized fixture tree: `<tmp>/sstables/<keyspace>/<table>-<uuid>/` holding
/// `generations` copies of the source generation. The `TempDir` is retained so the
/// tree outlives every read.
struct MaterializedTable {
    _tmp: tempfile::TempDir,
    /// The `sstables` root to hand to `open_db` (the ingestion `data_dir`).
    root: PathBuf,
    generations: usize,
}

/// Copy `source`'s single generation into a fresh temp tree `generations` times,
/// each copy under a distinct generation number (source generation, then
/// source+1, source+2, …). `generations == 1` reproduces the source verbatim.
fn materialize(
    source_dir: &Path,
    keyspace: &str,
    gen_files: &GenerationFiles,
    generations: usize,
) -> Result<MaterializedTable, String> {
    if generations == 0 {
        return Err("materialize: generations must be >= 1".into());
    }
    let table_dir_name = source_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("source dir {} has no name", source_dir.display()))?;
    let tmp = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let root = tmp.path().join("sstables");
    let table_dir = root.join(keyspace).join(table_dir_name);
    std::fs::create_dir_all(&table_dir)
        .map_err(|e| format!("create {}: {e}", table_dir.display()))?;

    for copy in 0..generations {
        let target_gen = gen_files
            .generation
            .checked_add(copy as u64)
            .ok_or_else(|| "materialize: generation number overflow".to_string())?;
        for src in &gen_files.files {
            let name = src
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| format!("component {} has no name", src.display()))?;
            let suffix = name.strip_prefix(&gen_files.prefix).ok_or_else(|| {
                format!(
                    "component {name} does not start with the generation prefix {}",
                    gen_files.prefix
                )
            })?;
            let mut new_prefix = gen_files.prefix.clone();
            // Rewrite ONLY the generation token of the shared prefix.
            let prefix_parts: Vec<&str> =
                gen_files.prefix.trim_end_matches('-').split('-').collect();
            if prefix_parts.len() == 3 {
                new_prefix = format!("{}-{target_gen}-{}-", prefix_parts[0], prefix_parts[2]);
            }
            let dest = table_dir.join(format!("{new_prefix}{suffix}"));
            std::fs::copy(src, &dest)
                .map_err(|e| format!("copy {} -> {}: {e}", src.display(), dest.display()))?;
        }
    }

    Ok(MaterializedTable {
        _tmp: tmp,
        root,
        generations,
    })
}

/// Locate the single `<table>-<uuid>` directory for `table` under `<root>/<keyspace>`.
fn table_dir(root: &Path, keyspace: &str, table: &str) -> Result<PathBuf, String> {
    let ks_dir = root.join(keyspace);
    let entries =
        std::fs::read_dir(&ks_dir).map_err(|e| format!("read_dir {}: {e}", ks_dir.display()))?;
    let mut found: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&format!("{table}-")))
                    .unwrap_or(false)
        })
        .collect();
    found.sort();
    match found.len() {
        1 => Ok(found.remove(0)),
        0 => Err(format!(
            "no directory for {keyspace}.{table} under {}",
            ks_dir.display()
        )),
        n => Err(format!(
            "{n} directories for {keyspace}.{table} under {} — ambiguous",
            ks_dir.display()
        )),
    }
}

// ---------------------------------------------------------------------------
// Comparator (the seam the seeded-divergence test drives)
// ---------------------------------------------------------------------------

/// The most diverging rows spelled out individually in a failure message before
/// falling back to the full dumps (a bare `assert_eq!` on two large vectors is
/// unreadable, and this lane exists to make a FUTURE regression diagnosable).
const MAX_REPORTED_DIFFS: usize = 5;

/// Compare a 1-generation result set against the N-generation result set for the
/// same logical content: rows, values AND order must match. `Ok(rows)` = agreed
/// row count; `Err(msg)` names the case, the query and the diverging rows.
fn compare_generation_axis(
    label: &str,
    query: &str,
    generations: usize,
    one_gen: &[QueryRow],
    n_gen: &[QueryRow],
) -> Result<usize, String> {
    let one = normalize(one_gen);
    let many = normalize(n_gen);
    if one == many {
        return Ok(one.len());
    }

    let mut diffs = String::new();
    let mut reported = 0usize;
    for idx in 0..one.len().max(many.len()) {
        let a = one.get(idx);
        let b = many.get(idx);
        if a == b {
            continue;
        }
        if reported == MAX_REPORTED_DIFFS {
            diffs.push_str("  … (further divergences truncated)\n");
            break;
        }
        reported += 1;
        diffs.push_str(&format!(
            "  row {idx}:\n    1-gen:  {}\n    {generations}-gen: {}\n",
            a.map(String::as_str).unwrap_or("<absent>"),
            b.map(String::as_str).unwrap_or("<absent>")
        ));
    }

    Err(format!(
        "1-gen-vs-{generations}-gen DIVERGENCE [{label}] for `{query}`: the SAME bytes read at 1 \
         generation returned {} row(s) but at {generations} generations returned {} row(s) / \
         different values or order. A SELECT's answer must not depend on the table's compaction \
         state (issue #3129).\n{diffs}  1-gen result: {one:#?}\n  {generations}-gen result: {many:#?}",
        one.len(),
        many.len()
    ))
}

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

/// One 1-vs-N case. Every table has a single INT partition key (as in the parent
/// lane) and a SINGLE source generation on disk.
struct GenerationCase {
    keyspace: &'static str,
    table: &'static str,
    schema: &'static str,
    pk_column: &'static str,
    /// Generations materialized on the N-gen arm (≥ 2 per the axis definition).
    n_generations: usize,
    /// EXACT row count the 1-gen full scan must return (anti-vacuity: pins the
    /// comparison away from `0 == 0` and catches a fixture that loses rows).
    expected_full_scan_rows: usize,
    /// EXACT number of distinct partition keys the 1-gen full scan must discover.
    expected_partitions: usize,
    /// Reconciliation classes covered (asserted exhaustive by the driver).
    divergence_classes: &'static [&'static str],
    /// `Some(reason)` = this shape ALREADY diverges on `main` for a defect tracked
    /// elsewhere, so it is NOT part of the enforcing lane (it would make the axis
    /// permanently red and hide a future regression in the shapes that do agree).
    /// Such a case runs only in the `#[ignore]`d
    /// `one_vs_n_generation_known_divergences` repro below. When the cited defect
    /// is fixed, set this to `None` — do NOT delete the case.
    known_divergent: Option<&'static str>,
}

/// Every entry is a fixture directory that currently holds exactly ONE
/// Cassandra-written generation (verified at run time), spanning the
/// reconciliation classes whose cross-generation kernel differs from the
/// single-generation path: row/cell/partition tombstones, range tombstones,
/// statics and TTL expiry.
const CORPUS: &[GenerationCase] = &[
    // Row tombstone shadowing a live row (post-major-compaction, 1 SSTable): the
    // row-level shadow decision must reach the same verdict on both arms.
    GenerationCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "shadow_row_delete",
        schema: "compaction-tombstone-ttl-parity.cql",
        pk_column: "id",
        n_generations: 2,
        expected_full_scan_rows: 3,
        expected_partitions: 2,
        divergence_classes: &["tombstone"],
        known_divergent: None,
    },
    // Range tombstone spanning generations, compacted to one SSTable: the range
    // deletion must cover the same clustering interval on both arms.
    GenerationCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "rt_cross_gen",
        schema: "compaction-tombstone-ttl-parity.cql",
        pk_column: "id",
        n_generations: 2,
        expected_full_scan_rows: 2,
        expected_partitions: 1,
        divergence_classes: &["range_tombstone"],
        known_divergent: None,
    },
    // WIDE range tombstone (~3k live rows): the axis at scale, and the only case
    // whose N-gen arm merges thousands of duplicate clustering keys — an ordering
    // or dedup defect in the merge kernel shows up here first.
    GenerationCase {
        keyspace: "test_tomb",
        table: "wide_range_tombstone",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        n_generations: 2,
        expected_full_scan_rows: 2987,
        expected_partitions: 1,
        divergence_classes: &["range_tombstone", "wide_partition"],
        known_divergent: None,
    },
    // BTI (`da`) WIDE partitions (3 × 300 rows, LZ4-compressed): the
    // multi-generation merge over trie-indexed SSTables, where the 1-gen arm
    // resolves rows through `Partitions.db`/`Rows.db` directly. Also the axis's
    // compressed-data arm.
    GenerationCase {
        keyspace: "test_da",
        table: "wide_table",
        schema: "wide-table-bti.cql",
        pk_column: "pk",
        n_generations: 2,
        expected_full_scan_rows: 900,
        expected_partitions: 3,
        divergence_classes: &["bti", "wide_partition", "compressed"],
        known_divergent: None,
    },
    // ---------------------------------------------------------------------
    // Known-divergent shapes: real 1-vs-N divergences this axis FOUND on
    // `main` (not seeded). They are excluded from the enforcing lane so it can
    // guard the agreeing shapes, and run in the `#[ignore]`d repro below.
    // ---------------------------------------------------------------------
    // TTL localDeletionTime boundary: at 2 generations the two TTL-EXPIRED rows
    // (ck=1, ck=2) come back as all-null phantom rows — the #3129 phantom shape
    // reached through expiry instead of a cell tombstone. Cassandra yields neither
    // (no live cell, no live primary-key liveness marker).
    GenerationCase {
        keyspace: "test_tomb",
        table: "gc_before_boundary",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        n_generations: 2,
        expected_full_scan_rows: 1,
        expected_partitions: 1,
        divergence_classes: &["ttl"],
        known_divergent: Some(
            "2-gen arm resurrects TTL-expired rows ck=1,ck=2 as all-null phantom rows \
             (the #3129 multi-gen liveness defect, reached via expiry); expected to clear \
             with the #3129 fix (PR #3122) — flip to None then",
        ),
    },
    // TTL expired-vs-live: same class as `gc_before_boundary`, on the
    // post-major-compaction fixture (1 live row at the pinned `now`, 2 at 3 gens).
    GenerationCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "ttl_expired_live",
        schema: "compaction-tombstone-ttl-parity.cql",
        pk_column: "id",
        n_generations: 3,
        expected_full_scan_rows: 1,
        expected_partitions: 1,
        divergence_classes: &["ttl"],
        known_divergent: Some(
            "3-gen arm returns 2 rows where the 1-gen arm returns 1 — same multi-gen \
             liveness defect as gc_before_boundary (#3129); flip to None with its fix",
        ),
    },
    // Live static cell surviving adjacent row/cell/range tombstones: a DIFFERENT
    // mechanism — the N-gen arm emits a separate static-only row AND strips
    // `stat_col` from the clustering rows, where the 1-gen arm merges the static
    // cell into every row (Cassandra's behaviour). Static injection in the
    // multi-generation kernel, adjacent to #3121.
    GenerationCase {
        keyspace: "test_tomb",
        table: "static_with_tombstones",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        n_generations: 2,
        expected_full_scan_rows: 3,
        expected_partitions: 1,
        divergence_classes: &["static", "tombstone"],
        known_divergent: Some(
            "N-gen arm emits an extra static-only row and drops the static column from \
             the clustering rows (multi-gen static injection; #3121-adjacent, needs its \
             own issue) — flip to None with that fix",
        ),
    },
];

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Run one case: materialize 1-gen and N-gen trees from the same bytes and assert
/// the two read paths agree on the full scan, on every per-partition point read
/// (under BOTH forced read-path modes) and on the multi-key `IN`.
/// `Ok(true)` = compared, `Ok(false)` = SKIPped.
async fn run_case(case: &GenerationCase) -> Result<bool, String> {
    if case.n_generations < 2 {
        return Err(format!(
            "case {}.{}: n_generations must be >= 2 for a 1-vs-N axis",
            case.keyspace, case.table
        ));
    }
    let Some(root) = sstables_root(case.keyspace) else {
        return skip_or_fail(&format!("keyspace {} absent", case.keyspace));
    };
    if !table_has_data(&root, case.keyspace, case.table) {
        return skip_or_fail(&format!(
            "table {}.{} has no fetched *-Data.db",
            case.keyspace, case.table
        ));
    }
    let Some(schema) = schema_path(case.schema) else {
        return skip_or_fail(&format!("schema {} absent", case.schema));
    };

    let source = table_dir(&root, case.keyspace, case.table)?;
    let gens = scan_generations(&source)?;
    // Fixture-drift guard: the axis is only meaningful when the SOURCE holds
    // exactly one generation (else it compares M-vs-N and proves nothing about
    // the single-generation path).
    if gens.len() != 1 {
        return Err(format!(
            "case {}.{}: source fixture {} holds {} generations, but this axis requires exactly \
             1 (it materializes the N-gen arm itself). The fixture changed — re-point the case \
             at a single-generation table or regenerate.",
            case.keyspace,
            case.table,
            source.display(),
            gens.len()
        ));
    }
    let source_gen = &gens[0];

    let one = materialize(&source, case.keyspace, source_gen, 1)?;
    let many = materialize(&source, case.keyspace, source_gen, case.n_generations)?;
    // Sanity: the N-gen tree must really hold N generations (a silently-failed
    // rename would make this axis a tautology).
    let materialized = scan_generations(
        &many.root.join(case.keyspace).join(
            source
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default(),
        ),
    )?;
    if materialized.len() != case.n_generations {
        return Err(format!(
            "case {}.{}: materialized N-gen tree holds {} generations, expected {} — the axis \
             would degenerate to a 1-vs-1 comparison",
            case.keyspace,
            case.table,
            materialized.len(),
            case.n_generations
        ));
    }

    let label = format!("{}.{}", case.keyspace, case.table);
    let one_full = open_db(&one.root, &schema, case.keyspace, ReadPathMode::Full).await?;
    let many_full = open_db(&many.root, &schema, case.keyspace, ReadPathMode::Full).await?;

    // (1) Full scan: `stream_generations_for_read`/`merge_generations_for_read` on
    // the N-gen arm vs the plain single-SSTable scan on the 1-gen arm.
    let scan_query = format!("SELECT * FROM {}.{}", case.keyspace, case.table);
    let scan_rows =
        compare_query(&one_full, &many_full, &label, &scan_query, many.generations).await?;
    if scan_rows != case.expected_full_scan_rows {
        return Err(format!(
            "case {label}: full scan agreed on {scan_rows} rows across both arms but the fixture \
             must yield exactly {} — equal-but-wrong is still wrong (anti-vacuity)",
            case.expected_full_scan_rows
        ));
    }

    // (2) Per-partition point reads under BOTH forced read-path modes.
    let discovered = discover_pk_ints(&one_full, case.keyspace, case.table, case.pk_column).await?;
    if discovered.len() != case.expected_partitions {
        return Err(format!(
            "case {label}: discovered {} distinct partition keys, expected exactly {} \
             (anti-vacuity / fixture drift)",
            discovered.len(),
            case.expected_partitions
        ));
    }
    // `discover_pk_ints` caps its result at MAX_KEYS_PER_TABLE, so a fixture with
    // more partitions than the cap yields a TRUNCATED probe set — in which case the
    // per-partition rows cannot be expected to account for the whole scan.
    let truncated = discovered.len() >= MAX_KEYS_PER_TABLE;
    let keys: Vec<i64> = discovered;

    let one_point = open_db(&one.root, &schema, case.keyspace, ReadPathMode::Point).await?;
    let many_point = open_db(&many.root, &schema, case.keyspace, ReadPathMode::Point).await?;

    let mut point_rows_total = 0usize;
    for mode_label in ["full", "point"] {
        let (a, b) = match mode_label {
            "point" => (&one_point, &many_point),
            _ => (&one_full, &many_full),
        };
        for k in &keys {
            let query = format!(
                "SELECT * FROM {}.{} WHERE {} = {}",
                case.keyspace, case.table, case.pk_column, k
            );
            let rows = compare_query(
                a,
                b,
                &format!("{label} [read_path={mode_label}]"),
                &query,
                many.generations,
            )
            .await?;
            if mode_label == "full" {
                point_rows_total += rows;
            }
        }
        // (3) `IN` over every discovered key (the multi-partition targeted path).
        if keys.len() >= 2 {
            let list = keys
                .iter()
                .map(|k| k.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "SELECT * FROM {}.{} WHERE {} IN ({})",
                case.keyspace, case.table, case.pk_column, list
            );
            compare_query(
                a,
                b,
                &format!("{label} [read_path={mode_label}] IN"),
                &query,
                many.generations,
            )
            .await?;
        }
    }

    // Anti-vacuity across the per-partition arm: the partition reads must together
    // account for the full-scan rows, so a probe set that compares nothing per
    // partition cannot pass. When the probe set was truncated by the cap only the
    // weaker "compared something" invariant is available.
    if truncated {
        if point_rows_total == 0 {
            return Err(format!(
                "case {label}: per-partition reads returned 0 rows over a truncated probe set — \
                 the point arm compared nothing"
            ));
        }
    } else if point_rows_total != case.expected_full_scan_rows {
        return Err(format!(
            "case {label}: per-partition reads returned {point_rows_total} rows in total but the \
             full scan returned {} — the point arm is not covering the fixture's rows",
            case.expected_full_scan_rows
        ));
    }

    eprintln!(
        "PASS {label} — 1-gen vs {}-gen identical: full scan ({} rows) + {} partition reads × 2 \
         read-path modes (classes: {:?})",
        case.n_generations,
        scan_rows,
        keys.len(),
        case.divergence_classes
    );
    Ok(true)
}

/// Execute `query` on both arms and compare.
async fn compare_query(
    one_gen_db: &cqlite_core::Database,
    n_gen_db: &cqlite_core::Database,
    label: &str,
    query: &str,
    generations: usize,
) -> Result<usize, String> {
    let one = one_gen_db
        .execute(query)
        .await
        .map_err(|e| format!("1-gen arm failed for `{query}` [{label}]: {e}"))?;
    let many = n_gen_db
        .execute(query)
        .await
        .map_err(|e| format!("{generations}-gen arm failed for `{query}` [{label}]: {e}"))?;
    compare_generation_axis(label, query, generations, &one.rows, &many.rows)
}

/// The ENFORCING lane: every corpus case that is not a documented known
/// divergence must return an identical result set at 1 and at N generations.
#[tokio::test]
async fn one_vs_n_generation_differential_equality() {
    // Corpus coverage: the ENFORCING set must span the reconciliation classes whose
    // cross-generation kernel differs from the single-generation path, so the axis
    // can never quietly narrow to a trivially-live corpus.
    let covered: std::collections::BTreeSet<&str> = CORPUS
        .iter()
        .filter(|c| c.known_divergent.is_none())
        .flat_map(|c| c.divergence_classes.iter().copied())
        .collect();
    for required in ["tombstone", "range_tombstone", "wide_partition", "bti"] {
        assert!(
            covered.contains(required),
            "the ENFORCING 1-vs-N corpus must cover the {required:?} reconciliation class"
        );
    }
    // Every known-divergent case must carry a substantive reason (never a bare
    // marker), so an exclusion can never be undocumented.
    for case in CORPUS.iter() {
        if let Some(reason) = case.known_divergent {
            assert!(
                reason.len() > 40,
                "known-divergent case {}.{} must document WHY and what clears it: {reason:?}",
                case.keyspace,
                case.table
            );
        }
    }

    std::env::set_var(TTL_NOW_OVERRIDE_ENV, PINNED_NOW_SECS.to_string());

    let mut ran = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for case in CORPUS.iter().filter(|c| c.known_divergent.is_none()) {
        match run_case(case).await {
            Ok(true) => ran += 1,
            Ok(false) => {}
            Err(e) => failures.push(format!("{}.{}: {e}", case.keyspace, case.table)),
        }
    }

    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);

    assert!(
        failures.is_empty(),
        "1-gen-vs-N-gen differential failures:\n{}",
        failures.join("\n\n")
    );

    if super::require_fixtures() {
        assert!(
            ran > 0,
            "CQLITE_REQUIRE_FIXTURES=1 but no 1-vs-N case ran (fixtures absent) — fail-closed"
        );
    } else if ran == 0 {
        eprintln!(
            "SKIP one_vs_n_generation_differential: no fixtures present \
             (set CQLITE_REQUIRE_FIXTURES=1 to fail-close)"
        );
    }
}

/// The known-divergent shapes, as a runnable reproducer. `#[ignore]`d because it
/// is EXPECTED TO FAIL on today's `main`: it runs the very same axis over the
/// `known_divergent` cases, i.e. the real 1-vs-N divergences this axis found
/// (TTL-expired rows resurrecting as all-null phantom rows at N generations, and
/// multi-generation static injection). It is the ready-made triage entry point for
/// those follow-ups:
///
/// ```text
/// cargo test -p cqlite-core --features cli-helpers --test point_vs_full_differential \
///   -- --ignored one_vs_n_generation_known_divergences --nocapture
/// ```
///
/// It is NOT a bug-pin: it asserts the CORRECT property (1-gen == N-gen), so when a
/// fix lands it goes green and the corresponding case's `known_divergent` flips to
/// `None`, moving it into the enforcing lane above.
#[tokio::test]
#[ignore = "expected-red reproducer for the known 1-vs-N divergences (see each case's known_divergent reason)"]
async fn one_vs_n_generation_known_divergences() {
    std::env::set_var(TTL_NOW_OVERRIDE_ENV, PINNED_NOW_SECS.to_string());

    let mut failures: Vec<String> = Vec::new();
    let mut cleared: Vec<String> = Vec::new();
    for case in CORPUS.iter().filter(|c| c.known_divergent.is_some()) {
        match run_case(case).await {
            Ok(true) => cleared.push(format!("{}.{}", case.keyspace, case.table)),
            Ok(false) => {}
            Err(e) => failures.push(format!(
                "{}.{} (known: {}): {e}",
                case.keyspace,
                case.table,
                case.known_divergent.unwrap_or("")
            )),
        }
    }

    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);

    if !cleared.is_empty() {
        eprintln!(
            "NOTE these known-divergent cases now AGREE at 1 vs N generations — set their \
             `known_divergent` to None so the enforcing lane guards them: {cleared:?}"
        );
    }
    assert!(
        failures.is_empty(),
        "known 1-gen-vs-N-gen divergences (expected red until their fixes land):\n{}",
        failures.join("\n\n")
    );
}

/// Seeded-divergence verification (issue #3129 AC6): the comparator must REPORT a
/// divergence — with the case label, the query and the diverging rows — rather
/// than pass, for each way the two arms can disagree (extra row, changed value,
/// reordering, missing row).
#[test]
fn one_vs_n_comparator_reports_a_seeded_divergence() {
    use cqlite_core::query::result::RowMetadata;
    use cqlite_core::types::{RowKey, Value};

    fn row(id: i32) -> QueryRow {
        let mut values = std::collections::HashMap::new();
        values.insert("id".into(), Value::Integer(id));
        QueryRow {
            values,
            key: RowKey::from(id.to_be_bytes().to_vec()),
            metadata: RowMetadata::default(),
            cell_metadata: None,
        }
    }

    let one_gen = vec![row(1), row(2)];

    // Identical content compares equal and reports the agreed row count.
    assert_eq!(
        compare_generation_axis("ks.t", "SELECT *", 2, &one_gen, &one_gen.clone()),
        Ok(2)
    );

    // (a) The #3129 shape: the N-gen arm emits a PHANTOM row the 1-gen arm does not.
    let phantom = vec![row(1), row(2), row(3)];
    let err = compare_generation_axis("ks.t", "SELECT * FROM ks.t", 2, &one_gen, &phantom)
        .expect_err("an extra N-gen row must be reported as a divergence");
    assert!(
        err.contains("1-gen-vs-2-gen DIVERGENCE"),
        "message must name the axis: {err}"
    );
    assert!(err.contains("ks.t"), "message must name the case: {err}");
    assert!(
        err.contains("SELECT * FROM ks.t"),
        "message must name the query: {err}"
    );
    assert!(
        err.contains("returned 2 row(s)") && err.contains("returned 3 row(s)"),
        "message must give both row counts: {err}"
    );
    assert!(
        err.contains("row 2:") && err.contains("1-gen:  <absent>"),
        "message must show the diverging row and mark the missing side: {err}"
    );
    assert!(
        err.contains("Integer(3)"),
        "message must show the diverging row's VALUES: {err}"
    );

    // (b) A changed value diverges.
    let altered = vec![row(1), row(9)];
    let err = compare_generation_axis("ks.t", "SELECT *", 2, &one_gen, &altered)
        .expect_err("a differing row value must be reported");
    assert!(err.contains("row 1:"), "must locate the row: {err}");
    assert!(
        err.contains("Integer(2)") && err.contains("Integer(9)"),
        "must show both sides' values: {err}"
    );

    // (c) A reordered result set diverges (order is asserted, not just the multiset).
    let reordered = vec![row(2), row(1)];
    assert!(
        compare_generation_axis("ks.t", "SELECT *", 2, &one_gen, &reordered).is_err(),
        "a reordered N-gen result must be reported as a divergence"
    );

    // (d) A row the N-gen arm LOSES diverges (the opposite direction: over-shadowing).
    let dropped = vec![row(1)];
    let err = compare_generation_axis("ks.t", "SELECT *", 2, &one_gen, &dropped)
        .expect_err("a missing N-gen row must be reported");
    assert!(
        err.contains("2-gen: <absent>"),
        "must mark the N-gen side as absent: {err}"
    );
}
