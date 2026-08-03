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
//! ## LIMITATION — this axis exercises structural merge, NOT precedence
//!
//! Because the N copies are BYTE-IDENTICAL, every cross-generation comparison the
//! merge kernel makes is a **TIE**: same key, same write timestamp, same value,
//! same `markedForDeleteAt`. So the axis covers the merge kernel's *structural*
//! behaviour — cross-generation interleaving and ordering, deduplication of equal
//! keys, static-row injection, application of a partition/range/row tombstone that
//! is present in every generation, and the seek path for a partition that must
//! return nothing — but it can NOT cover **last-write-wins precedence**: no case
//! here has a NEWER generation whose tombstone must shadow an OLDER generation's
//! live row, nor a newer cell that must win over an older one, because a tie never
//! forces a winner. That asymmetric class is covered by the multi-generation
//! fixtures of the parent lane (`test_tomb.resurrection_gc0`,
//! `resurrection_gc_positive`, `skipped_partition_delete` — real 2-generation
//! Cassandra writes with `T_GEN2` deletes over `T_GEN1` rows) and by the
//! Cassandra-oracle lanes; extending THIS axis to precedence would require
//! synthesizing non-identical generations, which reintroduces the write path as a
//! second variable (#3042).
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

use serial_test::serial;

use cqlite_core::config::ReadPathMode;
use cqlite_core::query::result::QueryRow;

use super::{
    describe_search, discover_pk_ints, normalize, open_db, pin_read_clock, schema_path,
    skip_or_fail, sstables_root_for_table, MAX_KEYS_PER_TABLE,
};

/// Sidecar files that live next to the real SSTable components in the committed
/// corpus (`*-Data.db.jsonl` physical goldens, `*-Statistics.db.txt` renders).
/// They are NOT components and are excluded from both materialized trees, so the
/// 1-gen and N-gen trees differ in nothing but generation multiplicity.
fn is_sidecar(name: &str) -> bool {
    name.ends_with(".jsonl") || name.ends_with(".db.txt")
}

/// One SSTable generation's on-disk component files, with the three descriptor
/// fields its file names encode kept STRUCTURED (never re-parsed from a string) so
/// renaming a copy can only ever change the generation token.
struct GenerationFiles {
    /// Format version (`nb`, `da`, …).
    version: String,
    /// Format family (`big`, `bti`).
    format: String,
    generation: u64,
    files: Vec<PathBuf>,
}

impl GenerationFiles {
    /// The `<version>-<generation>-<format>-` file-name prefix every component of
    /// this generation shares, for `generation` (defaults to this generation's own).
    fn prefix_for(&self, generation: u64) -> String {
        format!("{}-{generation}-{}-", self.version, self.format)
    }
}

/// Group a table directory's component files by generation number, parsed from the
/// canonical `<version>-<generation>-<format>-<component>` file name (Cassandra 5.0
/// `nb-3-big-Data.db`, `da-1-bti-Data.db`). Authoritative name structure only — no
/// content sniffing.
fn scan_generations(dir: &Path) -> Result<Vec<GenerationFiles>, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("read_dir {}: {e}", dir.display()))?;
    // generation -> ((version, format), files)
    let mut by_gen: BTreeMap<u64, ((String, String), Vec<PathBuf>)> = BTreeMap::new();
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
        let descriptor = (parts[0].to_string(), parts[2].to_string());
        let slot = by_gen
            .entry(generation)
            .or_insert_with(|| (descriptor.clone(), Vec::new()));
        // Every component of one generation must agree on version+format; a
        // disagreement means the name parse is wrong, never something to average over.
        if slot.0 != descriptor {
            return Err(format!(
                "component file {name} in {} declares {descriptor:?} but generation {generation} \
                 was already seen as {:?}",
                dir.display(),
                slot.0
            ));
        }
        slot.1.push(path);
    }
    Ok(by_gen
        .into_iter()
        .map(|(generation, ((version, format), files))| GenerationFiles {
            version,
            format,
            generation,
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
    /// The materialized `<keyspace>/<table>-<uuid>` directory itself (returned so
    /// the caller re-scans exactly what was written, never a re-derived path).
    table_dir: PathBuf,
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

    let source_prefix = gen_files.prefix_for(gen_files.generation);
    for copy in 0..generations {
        let target_gen = gen_files
            .generation
            .checked_add(copy as u64)
            .ok_or_else(|| "materialize: generation number overflow".to_string())?;
        // Rewrite ONLY the generation token; version/format/component are verbatim.
        let target_prefix = gen_files.prefix_for(target_gen);
        for src in &gen_files.files {
            let name = src
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| format!("component {} has no name", src.display()))?;
            let component = name.strip_prefix(&source_prefix).ok_or_else(|| {
                format!(
                    "component {name} does not start with the generation prefix {source_prefix}"
                )
            })?;
            let dest = table_dir.join(format!("{target_prefix}{component}"));
            std::fs::copy(src, &dest)
                .map_err(|e| format!("copy {} -> {}: {e}", src.display(), dest.display()))?;
        }
    }

    Ok(MaterializedTable {
        _tmp: tmp,
        root,
        table_dir,
        generations,
    })
}

/// True when `dir` holds at least one `*-Data.db`, i.e. it really carries an SSTable
/// generation. The committed corpus keeps SUPERSEDED `<table>-<uuid>` directories for
/// some tables that hold only sidecars (`*-Data.db.jsonl`, `*-Statistics.db.txt`) plus
/// a `Digest.crc32`/`TOC.txt` — no `Data.db`, hence no rows and no generation. This is
/// the same authoritative "has fetched binaries" test the parent lane's
/// `table_has_data` applies, not a content guess.
fn dir_has_data_component(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok()).any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// Locate the single `<table>-<uuid>` directory for `table` under `<root>/<keyspace>`
/// that actually carries SSTable components.
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
                && dir_has_data_component(p)
        })
        .collect();
    found.sort();
    match found.len() {
        1 => Ok(found.remove(0)),
        0 => Err(format!(
            "no directory carrying a *-Data.db for {keyspace}.{table} under {}",
            ks_dir.display()
        )),
        n => Err(format!(
            "{n} directories carrying a *-Data.db for {keyspace}.{table} under {} — ambiguous",
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

/// The token every 1-vs-N *divergence* error carries. The expected-divergence pin
/// (`one_vs_n_generation_quarantine_still_diverges`) matches on it to tell a REAL
/// divergence apart from a harness/fixture error (absent fixture, generation drift,
/// an anti-vacuity row-count mismatch) — otherwise "any `Err`" would let a broken
/// harness masquerade as "the quarantined defect is still there".
const DIVERGENCE_MARKER: &str = "DIVERGENCE";

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
        "1-gen-vs-{generations}-gen {DIVERGENCE_MARKER} [{label}] for `{query}`: the SAME bytes read at 1 \
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
    /// Partition keys probed IN ADDITION to the ones the 1-gen full scan discovers,
    /// every one of which must reconcile to ZERO rows (covered by a partition
    /// tombstone, or absent from the fixture entirely).
    ///
    /// Why the field must exist (issue #3129 AC2): discovery runs
    /// `SELECT <pk> FROM …`, so a partition that returns NOTHING yields no key and is
    /// therefore never probed — leaving the N-gen seek path
    /// (`seek_merge_generations_for_read`) untested for exactly the shape #3129 is
    /// about, a POINT query against a deleted partition that must return no rows and
    /// instead produced a phantom row. Mirrors the parent lane's `probe_keys`.
    ///
    /// Enforced at run time: each key must be ABSENT from the discovered set and must
    /// return 0 rows on BOTH arms under BOTH read-path modes — so a fixture change
    /// that makes such a key live (or a probe that silently matched live rows) fails
    /// loudly instead of degrading into an ordinary probe.
    empty_probe_keys: &'static [i64],
    /// Reconciliation classes covered (asserted exhaustive by the driver).
    divergence_classes: &'static [&'static str],
    /// `Some(reason)` = this shape ALREADY diverges on `main` for a defect tracked
    /// elsewhere, so it is NOT part of the enforcing lane (it would make the axis
    /// permanently red and hide a future regression in the shapes that do agree).
    /// The reason MUST cite the tracking issue (`#<number>`, asserted) — a waiver
    /// with no cited issue is not a waiver.
    ///
    /// Such a case is instead pinned by `one_vs_n_generation_quarantine_still_diverges`
    /// below, which asserts it STILL diverges and fails with instructions the moment
    /// it stops — so the quarantine releases itself. When that fires, set this to
    /// `None` (moving the case into the enforcing lane) — do NOT delete the case.
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
        // id=1 keeps live rows (ck=2,3) while ck=1 is row-deleted, so no partition of
        // this fixture is empty; 999 is absent from it entirely, which probes the
        // N-gen seek path's MISS branch (no generation holds the key).
        empty_probe_keys: &[999],
        divergence_classes: &["tombstone", "absent_partition"],
        known_divergent: None,
    },
    // Partition TOMBSTONES covering whole partitions, in the SAME single generation
    // as the live ones (5 partitions × 3 rows, `DELETE WHERE pk=2` and `pk=4`). This
    // is the #3129 shape itself: at N generations every copy carries both the
    // partition tombstone and the rows it covers, so the cross-generation merge must
    // still return NOTHING for pk=2/pk=4 — and a POINT read of those keys goes
    // through `seek_merge_generations_for_read`, which no discovered (live) key can
    // reach because discovery never yields a fully-deleted partition.
    GenerationCase {
        keyspace: "test_deltas",
        table: "partition_tombstones",
        schema: "deltas.cql",
        pk_column: "pk",
        n_generations: 3,
        expected_full_scan_rows: 9,
        expected_partitions: 3,
        empty_probe_keys: &[2, 4, 999],
        divergence_classes: &[
            "partition_tombstone",
            "deleted_partition",
            "absent_partition",
            "compressed",
        ],
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
        empty_probe_keys: &[999],
        divergence_classes: &["range_tombstone", "absent_partition"],
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
        empty_probe_keys: &[999],
        divergence_classes: &["range_tombstone", "wide_partition", "absent_partition"],
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
        empty_probe_keys: &[999],
        divergence_classes: &["bti", "wide_partition", "compressed", "absent_partition"],
        known_divergent: None,
    },
    // ---------------------------------------------------------------------
    // Known-divergent shapes: real 1-vs-N divergences this axis FOUND on
    // `main` (not seeded). They are excluded from the ENFORCING lane so it can
    // guard the agreeing shapes, and are instead PINNED as expected divergences by
    // `one_vs_n_generation_quarantine_still_diverges` below, which fails the moment
    // one of them starts agreeing (so the quarantine releases itself).
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
        empty_probe_keys: &[],
        divergence_classes: &["ttl"],
        known_divergent: Some(
            "issue #2189: the 2-gen arm resurrects the TTL-EXPIRED rows ck=1,ck=2 as all-null \
             phantom rows. Root cause is that `MergeEntry` carries no primary-key row-liveness \
             marker, so the multi-generation kernel has no row-liveness rule at all and cannot \
             tell an expired row from a live one. NOT fixed by the #3129 partition-shadow work \
             (PR #3122): `merged_row_shadowed_by_partition` short-circuits on `cover = None` and \
             this fixture has no partition deletion. Flip to None when #2189 lands",
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
        empty_probe_keys: &[],
        divergence_classes: &["ttl"],
        known_divergent: Some(
            "issue #2189: the 3-gen arm returns 2 rows where the 1-gen arm returns 1 — the same \
             missing multi-generation row-liveness rule as gc_before_boundary (`MergeEntry` has no \
             primary-key liveness marker), reached through an already-expired TTL cell rather than \
             a cell tombstone. Not addressed by PR #3122 (no partition deletion here, so the \
             partition-shadow decision is `cover = None`). Flip to None when #2189 lands",
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
        empty_probe_keys: &[],
        divergence_classes: &["static", "tombstone"],
        known_divergent: Some(
            "issue #3168: the multi-generation read path never injects static cells, so the N-gen \
             arm emits an extra static-only row AND drops `stat_col` from every clustering row, \
             where the 1-gen arm merges the static cell into each row (Cassandra's behaviour). \
             Distinct mechanism from the TTL family (#2189) and adjacent to #3121, which fixed the \
             SINGLE-generation static/row-tombstone order. Flip to None when #3168 lands",
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
    // TABLE-granular root resolution (issue #3220): search every candidate root for
    // THIS table's `*-Data.db` rather than committing to the first root that merely
    // holds the keyspace.
    let Some(root) = sstables_root_for_table(case.keyspace, case.table) else {
        return skip_or_fail(&describe_search(case.keyspace, case.table));
    };
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
    // Sanity, BOTH arms: the trees must really hold 1 and N generations (a
    // silently-failed rename, or a materializer that never copied, would turn this
    // axis into a 1-vs-1 tautology that can never fail).
    let one_materialized = scan_generations(&one.table_dir)?;
    if one_materialized.len() != 1 {
        return Err(format!(
            "case {}.{}: materialized 1-gen tree holds {} generations, expected exactly 1",
            case.keyspace,
            case.table,
            one_materialized.len()
        ));
    }
    let materialized = scan_generations(&many.table_dir)?;
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
    // Merge the discovered (live) keys with the case's declared EMPTY probe keys —
    // partitions that return nothing and so can never be discovered (issue #3129
    // AC2: a POINT read of a deleted partition is the shape that produced a phantom
    // row, and it reaches `seek_merge_generations_for_read` on the N-gen arm).
    // Deduplicated + sorted so the probe order is deterministic.
    let mut key_set: BTreeMap<i64, ()> = discovered.iter().map(|k| (*k, ())).collect();
    for k in case.empty_probe_keys {
        if discovered.contains(k) {
            return Err(format!(
                "case {label}: key {k} is declared an EMPTY probe key (must reconcile to zero \
                 rows) but the 1-gen full scan DISCOVERED it as live — the fixture changed, so \
                 the deleted/absent-partition probe no longer probes an empty partition"
            ));
        }
        key_set.insert(*k, ());
    }
    let keys: Vec<i64> = key_set.into_keys().collect();

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
            // An empty probe key must return NOTHING on both arms, in both modes.
            // `compare_query` only proves the two arms AGREE — and `0 == 0` is exactly
            // what a phantom-row defect would break in one direction and a
            // both-arms-wrong fixture drift in the other, so the absolute count is
            // pinned here.
            if case.empty_probe_keys.contains(k) && rows != 0 {
                return Err(format!(
                    "case {label} [read_path={mode_label}]: `{query}` returned {rows} row(s) on \
                     BOTH arms, but {k} is a deleted/absent partition that must return ZERO rows \
                     (a partition tombstone covers it, or it is not in the fixture) — \
                     equal-but-wrong is still wrong"
                ));
            }
            if mode_label == "full" {
                point_rows_total += rows;
            }
        }
        // (3) `IN` over every probed key — discovered AND empty (the multi-partition
        // targeted path, including a list that mixes live and deleted partitions).
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
        "PASS {label} — 1-gen vs {}-gen identical: full scan ({} rows) + {} partition reads \
         ({} of them deleted/absent partitions asserted empty) × 2 read-path modes \
         (classes: {:?})",
        case.n_generations,
        scan_rows,
        keys.len(),
        case.empty_probe_keys.len(),
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

// ---------------------------------------------------------------------------
// Corpus invariants (checked by BOTH tests below, so neither can drift)
// ---------------------------------------------------------------------------

/// Reconciliation classes the ENFORCING set must cover: the axis can never quietly
/// narrow to a trivially-live corpus. `partition_tombstone`/`deleted_partition` are
/// required because a POINT read of a fully-deleted partition is the #3129 shape
/// itself (AC2, `seek_merge_generations_for_read`).
const REQUIRED_ENFORCED_CLASSES: &[&str] = &[
    "tombstone",
    "range_tombstone",
    "partition_tombstone",
    "deleted_partition",
    "absent_partition",
    "wide_partition",
    "bti",
];

/// Classes that must be covered by an ASSERTED case — enforcing OR pinned as an
/// expected divergence. `ttl` and `static` are currently quarantined (#2189, #3168),
/// so they are asserted by `one_vs_n_generation_quarantine_still_diverges`; without
/// this list a quarantined class would satisfy NO assertion at all, which is exactly
/// how those two shapes went unguarded.
const REQUIRED_ASSERTED_CLASSES: &[&str] = &["ttl", "static"];

/// True when `reason` cites a tracking issue (`#` followed by at least one digit).
/// Doctrine: a waiver with no cited issue is not a waiver.
fn cites_an_issue(reason: &str) -> bool {
    reason
        .split('#')
        .skip(1)
        .any(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
}

/// Structural invariants over `CORPUS`. Called by both the enforcing lane and the
/// expected-divergence pin, so a corpus edit cannot satisfy one and break the other.
fn assert_corpus_invariants() {
    let enforced: std::collections::BTreeSet<&str> = CORPUS
        .iter()
        .filter(|c| c.known_divergent.is_none())
        .flat_map(|c| c.divergence_classes.iter().copied())
        .collect();
    for required in REQUIRED_ENFORCED_CLASSES {
        assert!(
            enforced.contains(required),
            "the ENFORCING 1-vs-N corpus must cover the {required:?} reconciliation class"
        );
    }

    // Every class in the corpus must be asserted SOMEWHERE. Every case is either
    // enforcing or pinned, so the union covers both sets.
    let asserted: std::collections::BTreeSet<&str> = CORPUS
        .iter()
        .flat_map(|c| c.divergence_classes.iter().copied())
        .collect();
    for required in REQUIRED_ASSERTED_CLASSES {
        assert!(
            asserted.contains(required),
            "the {required:?} class must be covered by an ASSERTED case — either the enforcing \
             lane or the expected-divergence pin (a quarantined class that satisfies no \
             assertion is unguarded, green whether broken or fixed)"
        );
    }

    // At least one ENFORCING case must probe a partition that returns NOTHING,
    // otherwise the N-gen seek path is never asked for a deleted/absent partition
    // (issue #3129 AC2) — discovery alone can never produce such a key.
    assert!(
        CORPUS
            .iter()
            .filter(|c| c.known_divergent.is_none())
            .any(|c| !c.empty_probe_keys.is_empty()),
        "at least one ENFORCING case must declare `empty_probe_keys` so a POINT/`IN` read of a \
         deleted or absent partition reaches `seek_merge_generations_for_read` (issue #3129)"
    );

    // Every quarantined case must carry a substantive reason that CITES its tracking
    // issue, so an exclusion can never be undocumented or untracked.
    for case in CORPUS.iter() {
        if let Some(reason) = case.known_divergent {
            assert!(
                reason.len() > 40,
                "known-divergent case {}.{} must document WHY and what clears it: {reason:?}",
                case.keyspace,
                case.table
            );
            assert!(
                cites_an_issue(reason),
                "known-divergent case {}.{} must cite its tracking issue as `#<number>` — a \
                 waiver with no cited issue is not a waiver: {reason:?}",
                case.keyspace,
                case.table
            );
        }
    }
}

/// The ENFORCING lane: every corpus case that is not a documented known
/// divergence must return an identical result set at 1 and at N generations.
///
/// `#[serial]`: pins the process-global `CQLITE_TTL_NOW_OVERRIDE_SECS` read seam
/// that sibling tests in this same binary also pin — see `super::pin_read_clock`.
#[tokio::test]
#[serial]
async fn one_vs_n_generation_differential_equality() {
    assert_corpus_invariants();

    let _clock = pin_read_clock();

    let mut ran = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for case in CORPUS.iter().filter(|c| c.known_divergent.is_none()) {
        match run_case(case).await {
            Ok(true) => ran += 1,
            Ok(false) => {}
            Err(e) => failures.push(format!("{}.{}: {e}", case.keyspace, case.table)),
        }
    }

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

/// The quarantine's RELEASE SIGNAL: an EXPECTED-DIVERGENCE pin over every
/// `known_divergent` case. It is deliberately NOT `#[ignore]`d, so the gate's
/// `core-tests` and CI run it on every change.
///
/// A `#[ignore]`d "expected-red reproducer" is a ratchet that never releases: the
/// gate never runs it, so when a quarantined defect is FIXED nothing tells anyone to
/// flip `known_divergent` to `None`, and the quarantined shapes stay unguarded
/// indefinitely — green whether broken or fixed. This test inverts the assertion
/// instead: each quarantined case MUST still diverge, and the moment one stops
/// diverging the test FAILS with instructions to move it into the enforcing lane.
///
/// Anti-vacuity, three ways:
///   * only an error carrying `DIVERGENCE_MARKER` counts as "still diverging"; any
///     other error (fixture drift, an expected-row-count mismatch, a materialization
///     failure) is a HARNESS failure reported separately, never mistaken for the
///     quarantined defect;
///   * a case that AGREES is a failure with the flip-it instruction (the release
///     signal), never a silent pass;
///   * an absent fixture SKIPs, and under `CQLITE_REQUIRE_FIXTURES=1` a run that
///     pinned nothing fails closed — so this can never pass on an empty corpus.
///
/// Triage entry point for the cited follow-ups (#2189 TTL row liveness, #3168
/// multi-generation static injection):
///
/// ```text
/// cargo test -p cqlite-core --features cli-helpers --test point_vs_full_differential \
///   -- one_vs_n_generation_quarantine_still_diverges --nocapture
/// ```
///
/// `#[serial]`: pins the process-global TTL-now seam (see `super::pin_read_clock`).
#[tokio::test]
#[serial]
async fn one_vs_n_generation_quarantine_still_diverges() {
    assert_corpus_invariants();

    let _clock = pin_read_clock();

    let quarantined: Vec<&GenerationCase> = CORPUS
        .iter()
        .filter(|c| c.known_divergent.is_some())
        .collect();

    let mut pinned = 0usize;
    let mut cleared: Vec<String> = Vec::new();
    let mut harness_failures: Vec<String> = Vec::new();
    for case in &quarantined {
        let label = format!("{}.{}", case.keyspace, case.table);
        let reason = case.known_divergent.unwrap_or_default();
        match run_case(case).await {
            // The case now AGREES at 1 vs N generations: the cited defect is fixed
            // (or the quarantine was wrong). Either way it must move into the
            // enforcing lane — that is this test's whole purpose.
            Ok(true) => cleared.push(format!(
                "  `{label}` NO LONGER DIVERGES — flip its `known_divergent` to None so the \
                 ENFORCING lane guards it (and drop its class from REQUIRED_ASSERTED_CLASSES \
                 only if another case still covers it). Quarantine reason was: {reason}"
            )),
            Ok(false) => {}
            // The expected divergence: the case is still broken exactly as documented.
            Err(e) if e.contains(DIVERGENCE_MARKER) => {
                pinned += 1;
                eprintln!("PINNED (still diverging, as expected) {label}: {reason}");
            }
            // Anything else is a harness/fixture problem, NOT the quarantined defect.
            Err(e) => harness_failures.push(format!(
                "  `{label}`: expected a 1-vs-N {DIVERGENCE_MARKER} but the harness failed for a \
                 DIFFERENT reason, which must never be counted as \"still diverging\": {e}"
            )),
        }
    }

    assert!(
        harness_failures.is_empty(),
        "the expected-divergence pin hit harness/fixture failures:\n{}",
        harness_failures.join("\n\n")
    );
    assert!(
        cleared.is_empty(),
        "quarantined 1-vs-N case(s) now AGREE — the quarantine must be released:\n{}",
        cleared.join("\n\n")
    );

    if quarantined.is_empty() {
        eprintln!(
            "NOTE the 1-vs-N quarantine is EMPTY — every corpus case is enforced. \
             Delete this pin (and REQUIRED_ASSERTED_CLASSES) once that is permanent."
        );
    } else if super::require_fixtures() {
        assert!(
            pinned > 0,
            "CQLITE_REQUIRE_FIXTURES=1 but the expected-divergence pin ran no case \
             (fixtures absent) — fail-closed"
        );
    } else if pinned == 0 {
        eprintln!(
            "SKIP one_vs_n_generation_quarantine_still_diverges: no fixtures present \
             (set CQLITE_REQUIRE_FIXTURES=1 to fail-close)"
        );
    }
}

/// The quarantine guard itself must be non-vacuous: `cites_an_issue` has to REJECT
/// substantive-but-untracked prose, else a future quarantine could be added without a
/// tracking issue ("a waiver with no cited issue is not a waiver").
#[test]
fn quarantine_reason_guard_requires_an_issue_reference() {
    assert!(cites_an_issue(
        "issue #2189: no row-liveness marker in MergeEntry"
    ));
    assert!(cites_an_issue("tracked at the end of the sentence (#3168)"));
    // Substantive prose that cites nothing, and a bare `#` with no number, are both
    // rejected.
    assert!(!cites_an_issue(
        "the N-gen arm emits an extra static-only row and drops the static column, \
         which is clearly wrong and will be fixed later"
    ));
    assert!(!cites_an_issue("see the # section of the design doc"));
    assert!(!cites_an_issue(""));
    // Every quarantine reason actually in the corpus must satisfy the guard (the
    // driver asserts this too; asserted here so a corpus edit fails even when the
    // fixtures are absent and the async lanes SKIP).
    for case in CORPUS.iter() {
        if let Some(reason) = case.known_divergent {
            assert!(
                cites_an_issue(reason),
                "{}.{} quarantine reason cites no issue: {reason:?}",
                case.keyspace,
                case.table
            );
        }
    }
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
