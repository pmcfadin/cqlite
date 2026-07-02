//! Issue #1384 — end-to-end zombie prevention for a PARTIAL compaction with an
//! EXCLUDED overlapping SSTable.
//!
//! The mechanism (Cassandra `CompactionController.maxPurgeableTimestamp`) lives in
//! [`compute_max_purgeable_timestamp`] (the per-compaction overlap bound) and the
//! three purge gate sites in `merge/reconcile.rs::purge_gc_grace`:
//!
//!   * (a) cell tombstones            — `cell.timestamp < max_purgeable_timestamp`
//!   * (b) whole-row tombstones       — `row_del      < max_purgeable_timestamp`
//!   * (c) complex-deletion markers   — `mfda         < max_purgeable_timestamp`
//!
//! Issue #1014 proves resurrection safety over committed Cassandra fixtures, but no
//! test constructs an EXCLUDED overlapping SSTable holding LIVE data that a
//! gc-purgeable tombstone shadows, runs a PARTIAL compaction over only the included
//! subset, and verifies zombie prevention across ALL generations. This suite does.
//!
//! ## Fixture construction (all in-code, CQLite's own write path)
//!
//! Every input SSTable is produced by CQLite's [`WriteEngine`] flush, so this suite
//! needs NO external dataset and runs anywhere `write-support` is built. Timestamps
//! and the pinned evaluation instant are FIXED constants — no wall clock ever drives
//! an expiry / gc-grace / overlap decision (gc_grace boundary tests are especially
//! prone to one-second flakes).
//!
//! ## The three merge surfaces exercised
//!
//! 1. **overlap gate (partial merge)** — the real merge read path
//!    ([`KWayMerger`]) with `purge_safe = false` and an explicit
//!    `with_max_purgeable_timestamp(bound)` computed by
//!    [`compute_max_purgeable_timestamp`] over the EXCLUDED SSTable. This is the
//!    exact plumbing `WriteEngine::maintenance_step → start_merge` uses for a
//!    background PARTIAL compaction. (The one-shot public [`compact_sstables`]
//!    cannot thread an overlap bound — it always passes `None` — so the overlap
//!    gate is UNREACHABLE through it; the background path is where #935 lives.)
//! 2. **real one-shot compaction** ([`compact_sstables`]) — for the FULL /
//!    purge-safe baseline that writes and re-reads a merged output SSTable.
//! 3. **cross-generation read** — the physical union {A, B, C} read together
//!    through [`KWayMerger`], proving no zombie surfaces downstream.
//!
//! ## PurgeCounts audit (criterion 4) — observed BEHAVIORALLY
//!
//! `PurgeCounts` is a private merge-internal struct and `MergeStats` exposes no
//! purge tally; the only public surface is the process-global
//! `cqlite.compaction.tombstones_purged` OTLP counter, which is documented as a
//! single process-wide record that "cannot be swapped per-test" and so cannot be
//! isolated under parallel execution. The audit is therefore observed
//! AUTHORITATIVELY from the merge OUTPUT: a RETAINED tombstone == zero purges; an
//! ABSENT tombstone whose newer live value now surfaces == exactly one purge. This
//! is deterministic under parallelism and is the same output-based evidence issue
//! #1014 / #1382 use.
//!
//! ## Schemas
//!
//! The cell- and row-tombstone scenarios use a collection-free schema
//! (`pk, ck, v, w`); the complex-deletion scenario uses a collection-only schema
//! (`pk, ck, tags`). They are kept separate on purpose: mixing a `set<int>` column
//! into the scalar schema perturbs the regular-column decode of a row that carries
//! no collection cells — an unrelated read-path quirk this suite must not conflate
//! with the overlap gate under test.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{
    compact_sstables, compute_max_purgeable_timestamp, CellData, MergeEntry, MergeStep, RowData,
};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::{TombstoneType, Value};
use tempfile::TempDir;

// ===========================================================================
// Pinned time constants (NEVER a wall clock)
// ===========================================================================

/// The single partition/clustering key under test. The (live, tombstone, live)
/// triple all target the SAME `(pk=1, ck=0)` so the tombstone genuinely shadows
/// the excluded SSTable's live data.
const PK: i32 = 1;
const CK: i32 = 0;

/// Write timestamp (micros) of the live cell in A and the EXCLUDED SSTable C
/// (zombie scenario). The tombstone (`TS_TOMB`) is NEWER so it wins last-write-wins
/// within the compaction set.
const TS_LIVE: i64 = 5_000_000; // 5s
/// Write timestamp (micros, `markedForDeleteAt`) of the tombstone in B.
const TS_TOMB: i64 = 10_000_000; // 10s  (> TS_LIVE)
/// A NEWER live write timestamp for the "purge resumes when safe" scenario: C's
/// live value at a HIGHER writetime than the tombstone, so the overlap bound rises
/// ABOVE the tombstone's mfda and the purge becomes safe.
const TS_LIVE_NEWER: i64 = 20_000_000; // 20s  (> TS_TOMB)

/// The tombstone's on-disk `localDeletionTime` (GC-clock seconds). Pinned well
/// BELOW `GC_BEFORE` so the tombstone is unambiguously gc-purgeable — the ONLY
/// thing that then decides its fate is the overlap gate.
const TOMB_LDT_SECS: i32 = 100;
/// gcBefore (GC-clock seconds). `TOMB_LDT_SECS < GC_BEFORE` ⇒ gc-purgeable.
const GC_BEFORE: i64 = 1_000;
/// Pinned evaluation instant (GC-clock seconds), far past every LDT so nothing
/// samples a wall clock and no live cell is spuriously TTL-expired (no TTLs here).
const NOW_SECS: i64 = 10_000;

// ===========================================================================
// Schemas
// ===========================================================================

/// Collection-free schema for the cell / row tombstone scenarios: `pk int, ck int,
/// v text, w text`. `w` is a live anchor kept in every SSTable so a `v` cell
/// tombstone survives on a live row rather than collapsing to a phantom key-only
/// row.
fn schema_scalar() -> TableSchema {
    base_schema(vec![
        col("pk", "int", false),
        col("ck", "int", false),
        col("v", "text", true),
        col("w", "text", true),
    ])
}

/// Collection-only schema for the complex-deletion scenario: `pk int, ck int,
/// tags set<int>`.
fn schema_collection() -> TableSchema {
    base_schema(vec![
        col("pk", "int", false),
        col("ck", "int", false),
        col("tags", "set<int>", true),
    ])
}

fn base_schema(columns: Vec<Column>) -> TableSchema {
    TableSchema {
        keyspace: "zk".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable,
        default: None,
        is_static: false,
    }
}

fn table_id() -> TableId {
    TableId::new("zk", "t")
}

fn pk_key() -> PartitionKey {
    PartitionKey::single("pk", Value::Integer(PK))
}

fn ck_key() -> Option<ClusteringKey> {
    Some(ClusteringKey::single("ck", Value::Integer(CK)))
}

// ===========================================================================
// Mutation builders (scalar schema)
// ===========================================================================

/// Live `v = <value>` plus a live `w` anchor at `ts` micros.
fn live_v(value: &str, ts: i64) -> Mutation {
    Mutation::new(
        table_id(),
        pk_key(),
        ck_key(),
        vec![
            CellOperation::Write {
                column: "v".to_string(),
                value: Value::Text(value.to_string()),
            },
            CellOperation::Write {
                column: "w".to_string(),
                value: Value::Text("anchor".to_string()),
            },
        ],
        ts,
        None,
    )
}

/// A CELL tombstone deleting `v` at `TS_TOMB` micros with a fixed, gc-purgeable
/// `localDeletionTime`. A live `w` at the same timestamp keeps the row live so the
/// `v` cell tombstone is observable.
fn cell_tomb_v() -> Mutation {
    Mutation::new(
        table_id(),
        pk_key(),
        ck_key(),
        vec![
            CellOperation::Delete {
                column: "v".to_string(),
                local_deletion_time: Some(TOMB_LDT_SECS),
            },
            CellOperation::Write {
                column: "w".to_string(),
                value: Value::Text("anchor".to_string()),
            },
        ],
        TS_TOMB,
        None,
    )
}

/// A whole-ROW tombstone at `TS_TOMB` micros with the fixed gc-purgeable
/// `localDeletionTime`.
fn row_tomb() -> Mutation {
    Mutation::new(
        table_id(),
        pk_key(),
        ck_key(),
        vec![CellOperation::DeleteRow],
        TS_TOMB,
        None,
    )
    .with_local_deletion_time(TOMB_LDT_SECS)
}

// ===========================================================================
// Mutation builders (collection schema)
// ===========================================================================

/// A COMPLEX-deletion marker over the non-frozen `tags` set at `TS_TOMB` micros
/// with the fixed gc-purgeable `localDeletionTime`.
fn complex_deletion_tags() -> Mutation {
    Mutation::new(
        table_id(),
        pk_key(),
        ck_key(),
        vec![CellOperation::ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: TS_TOMB,
            local_deletion_time: TOMB_LDT_SECS,
        }],
        TS_TOMB,
        None,
    )
}

/// A live `tags` set member so the EXCLUDED SSTable holds live collection data the
/// complex-deletion marker would shadow.
fn live_tag_element(member: i32, ts: i64) -> Mutation {
    Mutation::new(
        table_id(),
        pk_key(),
        ck_key(),
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Set(vec![Value::Integer(member)]),
        }],
        ts,
        None,
    )
}

// ===========================================================================
// Runtime + flush + discovery helpers
// ===========================================================================

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

/// Flush `muts` into ONE input SSTable under `data_dir`.
fn flush_batch(data_dir: &Path, wal_dir: &Path, sch: &TableSchema, muts: Vec<Mutation>) {
    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), sch.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in muts {
        engine.write(m).expect("write mutation");
    }
    let r = rt();
    r.block_on(engine.flush()).expect("flush").expect("info");
    r.block_on(engine.close()).expect("close engine");
}

/// Discover `nb-*-big-Data.db` inputs under `dir` (recursive), newest-gen first.
fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect(dir: &Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect(&path, out, depth - 1);
        }
    }
}

/// Filter out empty (fully-purged) Data.db files so an all-purged output reads
/// back as ZERO rows rather than erroring on an unparseable empty header.
fn non_empty(inputs: &[PathBuf]) -> Vec<PathBuf> {
    inputs
        .iter()
        .filter(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
        .cloned()
        .collect()
}

// ===========================================================================
// Merge observation
// ===========================================================================

/// A collapsed view of the surviving `(pk=PK, ck=CK)` entry after a merge.
#[derive(Debug, Default)]
struct Observed {
    /// The row survived as a whole-row tombstone (`RowData::Tombstone`).
    row_tombstone: bool,
    /// `v` survived as a live text value (a zombie if the tombstone should win).
    v_live: Option<String>,
    /// `v` survived as a cell tombstone.
    v_cell_tombstone: bool,
    /// A complex-deletion marker over `tags` survived.
    tags_complex_deletion: bool,
    /// A live `tags` element survived (a zombie if the marker should win).
    tags_live_element: bool,
}

/// Run a set of SSTables through the SAME merge read path the compactor uses, with
/// the given `purge_safe` / overlap bound, and collapse the surviving `(PK, CK)`
/// state. `now`/`gc_before` are pinned.
fn observe(
    inputs: Vec<PathBuf>,
    sch: &TableSchema,
    gc_before: Option<i64>,
    now_secs: Option<i64>,
    purge_safe: bool,
    overlap_bound: Option<i64>,
) -> Observed {
    let inputs = non_empty(&inputs);
    let mut obs = Observed::default();
    if inputs.is_empty() {
        return obs;
    }
    let mut merger = KWayMerger::new_with_gc(inputs, sch, gc_before, now_secs)
        .expect("merger")
        .with_purge_safe(purge_safe)
        .with_max_purgeable_timestamp(overlap_bound);
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    collapse(&entry, &mut obs);
                }
            }
        }
    }
    obs
}

fn collapse(entry: &MergeEntry, obs: &mut Observed) {
    if entry.complex_deletions.iter().any(|c| c.column == "tags") {
        obs.tags_complex_deletion = true;
    }
    match &entry.row_data {
        RowData::Tombstone { .. } => obs.row_tombstone = true,
        RowData::Live { cells } => {
            for c in cells {
                observe_cell(c, obs);
            }
        }
    }
}

fn observe_cell(c: &CellData, obs: &mut Observed) {
    match c.column.as_str() {
        "v" => match &c.value {
            Value::Text(t) => obs.v_live = Some(t.clone()),
            Value::Tombstone(info) if info.tombstone_type == TombstoneType::CellTombstone => {
                obs.v_cell_tombstone = true;
            }
            _ => {}
        },
        "tags" => {
            let is_live_element = c.is_complex_element && !c.is_deleted;
            let is_live_set_value = matches!(&c.value, Value::Set(s) if !s.is_empty());
            if is_live_element || is_live_set_value {
                obs.tags_live_element = true;
            }
        }
        _ => {}
    }
}

// ===========================================================================
// Scenario scaffolding: build A (older live), B (tombstone), C (excluded live).
// ===========================================================================

/// A built (A live, B tombstone, C excluded live) scenario over one schema.
struct Scenario {
    _temp: TempDir,
    /// Compaction SET = {A older live, B tombstone}, newest-gen first.
    included: Vec<PathBuf>,
    /// The EXCLUDED overlapping SSTable C (holds live data under the same key).
    excluded: Vec<PathBuf>,
    schema: TableSchema,
}

impl Scenario {
    /// Flush A then B into the SAME included dir (two generations), and C into the
    /// excluded dir. Asserts the discovered shapes so a fixture regression fails
    /// loudly rather than silently comparing zero facts.
    fn build(
        sch: TableSchema,
        a_muts: Vec<Mutation>,
        b_muts: Vec<Mutation>,
        c_muts: Vec<Mutation>,
    ) -> Self {
        let temp = TempDir::new().unwrap();
        let inc_dir = temp.path().join("included");
        let exc_dir = temp.path().join("excluded");
        let wal = temp.path().join("wal");

        flush_batch(&inc_dir, &wal.join("a"), &sch, a_muts);
        flush_batch(&inc_dir, &wal.join("b"), &sch, b_muts);
        flush_batch(&exc_dir, &wal.join("c"), &sch, c_muts);

        let included = discover_inputs(&inc_dir);
        let excluded = discover_inputs(&exc_dir);
        assert_eq!(
            included.len(),
            2,
            "included set must be two SSTables (A live + B tombstone), got {included:?}"
        );
        assert_eq!(
            excluded.len(),
            1,
            "excluded set must be one SSTable (C), got {excluded:?}"
        );
        Scenario {
            _temp: temp,
            included,
            excluded,
            schema: sch,
        }
    }

    /// The overlap bound the background partial path would compute from C.
    fn overlap_bound(&self) -> i64 {
        compute_max_purgeable_timestamp(&self.excluded)
            .expect("excluded SSTable C exposes a min timestamp")
    }

    /// Observe the PARTIAL merge of {A, B} (purge-unsafe) with the overlap bound
    /// derived from the excluded C. This is exactly the merger config the
    /// background `start_merge` partial-compaction path builds.
    fn observe_partial(&self) -> Observed {
        observe(
            self.included.clone(),
            &self.schema,
            Some(GC_BEFORE),
            Some(NOW_SECS),
            /* purge_safe */ false,
            Some(self.overlap_bound()),
        )
    }

    /// Observe the physical union {A, B, C} read together with the SAME overlap
    /// discipline (purge-unsafe + the overlap bound). This is the strongest "no
    /// zombie downstream" check — it is the union a later read spans after a
    /// partial compaction leaves C in place.
    fn observe_across_all(&self) -> Observed {
        let mut all = self.included.clone();
        all.extend(self.excluded.clone());
        observe(
            all,
            &self.schema,
            Some(GC_BEFORE),
            Some(NOW_SECS),
            /* purge_safe */ false,
            Some(self.overlap_bound()),
        )
    }
}

// ===========================================================================
// Criterion 1 — ZOMBIE PREVENTION (cell tombstone), overlap gate BLOCKS purge.
// ===========================================================================

/// A = live `v1`@5s, B = cell tombstone of `v`@10s (gc-purgeable LDT), C
/// (EXCLUDED) = another live `v1`@5s.
///
/// (i) The PARTIAL compaction of {A, B} must RETAIN the `v` cell tombstone: the
///     overlap gate sees the tombstone's `markedForDeleteAt` (10s) is NOT strictly
///     below C's min timestamp (5s), so purging it could resurrect C's `v`.
/// (ii) A full read across {A, B, C} shows `v` DELETED — no zombie surfaces.
/// PurgeCounts audit: ZERO purges (tombstone retained in the partial output).
#[test]
fn zombie_prevention_cell_tombstone_retained_under_overlap_gate() {
    let s = Scenario::build(
        schema_scalar(),
        vec![live_v("v1", TS_LIVE)],
        vec![cell_tomb_v()],
        vec![live_v("v1", TS_LIVE)],
    );

    let bound = s.overlap_bound();
    assert_eq!(bound, TS_LIVE, "overlap bound == C's min write timestamp");
    assert!(
        TS_TOMB >= bound,
        "tombstone mfda ({TS_TOMB}) must NOT be strictly below the overlap bound ({bound})"
    );

    // (i) PARTIAL merge retains the cell tombstone (zero purges).
    let partial = s.observe_partial();
    assert!(
        partial.v_cell_tombstone,
        "cell tombstone MUST be retained under the overlap gate (mfda not below C's min ts); \
         got {partial:?}"
    );
    assert!(
        partial.v_live.is_none(),
        "the `v` cell must NOT resurrect as a live value in the partial output; got {partial:?}"
    );

    // (ii) full cross-generation read: `v` stays deleted (no zombie).
    let all = s.observe_across_all();
    assert!(
        all.v_live.is_none(),
        "ZOMBIE: `v` must stay deleted across {{A,B,C}}, but a live value resurfaced: {all:?}"
    );
    assert!(
        all.v_cell_tombstone,
        "the `v` cell tombstone must still shadow C's live `v1` across all generations; \
         got {all:?}"
    );
}

// ===========================================================================
// Criterion 1 (variant) — ZOMBIE PREVENTION (row tombstone).
// ===========================================================================

/// Same as above but B is a WHOLE-ROW tombstone shadowing C's live row.
/// Exercises the row-tombstone overlap gate site (reconcile.rs `row_del` gate).
#[test]
fn zombie_prevention_row_tombstone_retained_under_overlap_gate() {
    let s = Scenario::build(
        schema_scalar(),
        vec![live_v("v1", TS_LIVE)],
        vec![row_tomb()],
        vec![live_v("v1", TS_LIVE)],
    );
    let bound = s.overlap_bound();
    assert!(TS_TOMB >= bound, "row-delete mfda not below overlap bound");

    let partial = s.observe_partial();
    assert!(
        partial.row_tombstone,
        "row tombstone MUST be retained under the overlap gate; got {partial:?}"
    );
    assert!(
        partial.v_live.is_none(),
        "the row must NOT resurrect a live `v` in the partial output; got {partial:?}"
    );

    let all = s.observe_across_all();
    assert!(
        all.v_live.is_none(),
        "ZOMBIE: the deleted row must stay dead across {{A,B,C}}; got {all:?}"
    );
    assert!(
        all.row_tombstone,
        "the row tombstone must still shadow C's live row across all generations; got {all:?}"
    );
}

// ===========================================================================
// Criterion 3 — ZOMBIE PREVENTION (complex / collection deletion marker).
// ===========================================================================

/// Same shape but on the collection schema: A = live `tags={7}`@5s, B = complex
/// deletion marker over `tags`@10s (gc-purgeable), C (EXCLUDED) = live `tags={7}`@5s.
/// Exercises the complex-deletion overlap gate site (reconcile.rs `mfda` gate).
#[test]
fn zombie_prevention_complex_deletion_retained_under_overlap_gate() {
    let s = Scenario::build(
        schema_collection(),
        vec![live_tag_element(7, TS_LIVE)],
        vec![complex_deletion_tags()],
        vec![live_tag_element(7, TS_LIVE)],
    );
    let bound = s.overlap_bound();
    assert!(TS_TOMB >= bound, "marker mfda not below overlap bound");

    let partial = s.observe_partial();
    assert!(
        partial.tags_complex_deletion,
        "complex-deletion marker MUST be retained under the overlap gate; got {partial:?}"
    );

    let all = s.observe_across_all();
    // The marker (mfda=10s) shadows C's live element (ts=5s): no live element must
    // resurface below the marker.
    assert!(
        !all.tags_live_element,
        "ZOMBIE: the complex-deletion marker must shadow C's live `tags` element across \
         {{A,B,C}}; a live element resurfaced: {all:?}"
    );
    assert!(
        all.tags_complex_deletion,
        "the complex-deletion marker must still shadow C's live element across generations; \
         got {all:?}"
    );
}

// ===========================================================================
// Criterion 2 — PURGE RESUMES WHEN SAFE (cell tombstone).
// ===========================================================================

/// A = live `v1`@5s, B = cell tombstone of `v`@10s, C (EXCLUDED) = a NEWER live
/// `v2`@20s (`min ts > 10`). Now the overlap bound (20s) is STRICTLY ABOVE the
/// tombstone's mfda (10s), so BOTH gates (gc + overlap) pass and the tombstone IS
/// purged — proving the gate is not permanently conservative.
///
/// A full read across {A, B, C} returns C's NEWER live value (`v2`): C's write
/// (20s) is newer than the deletion (10s) and legitimately wins.
/// PurgeCounts audit: EXACTLY ONE purge (the cell tombstone leaves the partial
/// output).
#[test]
fn purge_resumes_when_overlap_bound_above_tombstone() {
    let s = Scenario::build(
        schema_scalar(),
        vec![live_v("v1", TS_LIVE)],
        vec![cell_tomb_v()],
        vec![live_v("v2", TS_LIVE_NEWER)],
    );

    let bound = s.overlap_bound();
    assert_eq!(
        bound, TS_LIVE_NEWER,
        "overlap bound == C's (newer) min write timestamp"
    );
    assert!(
        TS_TOMB < bound,
        "tombstone mfda ({TS_TOMB}) must be STRICTLY below the overlap bound ({bound}) so the \
         purge is safe"
    );

    // PARTIAL merge now PURGES the cell tombstone: the `v` cell is absent from the
    // partial output (no cell tombstone, no live value — one purge).
    let partial = s.observe_partial();
    assert!(
        !partial.v_cell_tombstone,
        "the cell tombstone MUST be purged once the overlap bound rises above its mfda; \
         got {partial:?}"
    );
    assert!(
        partial.v_live.is_none(),
        "no live `v` exists in the compaction set {{A,B}} after the delete; got {partial:?}"
    );

    // Full cross-generation read: C's NEWER live value wins (not a zombie — C's
    // write at 20s legitimately post-dates the 10s delete).
    let all = s.observe_across_all();
    assert_eq!(
        all.v_live.as_deref(),
        Some("v2"),
        "C's newer live `v2` (ts=20s > delete@10s) must surface across {{A,B,C}}; got {all:?}"
    );
}

// ===========================================================================
// Criterion 4 — PurgeCounts audit (behavioral), explicit side-by-side.
// ===========================================================================

/// The two ends of the gate, asserted together as the PurgeCounts audit:
///   * retained scenario (C at ts=5s): ZERO purges  → tombstone present in output.
///   * purge scenario   (C at ts=20s): ONE  purge   → tombstone absent from output.
///
/// (PurgeCounts is a private merge internal and the process-global purge counter
/// cannot be isolated per-test under parallelism — the observable, deterministic,
/// authoritative signal for "N purges" is the presence/absence of the tombstone in
/// the merged output. See module docs.)
#[test]
fn purge_counts_audit_zero_then_one() {
    // Zero purges: overlap gate blocks (C at TS_LIVE).
    let retained = Scenario::build(
        schema_scalar(),
        vec![live_v("v1", TS_LIVE)],
        vec![cell_tomb_v()],
        vec![live_v("v1", TS_LIVE)],
    );
    let obs0 = retained.observe_partial();
    assert!(
        obs0.v_cell_tombstone && obs0.v_live.is_none(),
        "audit(zero purges): the tombstone must be retained; got {obs0:?}"
    );

    // One purge: overlap gate passes (C newer, at TS_LIVE_NEWER).
    let purged = Scenario::build(
        schema_scalar(),
        vec![live_v("v1", TS_LIVE)],
        vec![cell_tomb_v()],
        vec![live_v("v2", TS_LIVE_NEWER)],
    );
    let obs1 = purged.observe_partial();
    assert!(
        !obs1.v_cell_tombstone && obs1.v_live.is_none(),
        "audit(one purge): the tombstone must be purged (absent from output); got {obs1:?}"
    );
}

// ===========================================================================
// FULL / purge-safe baseline via the REAL one-shot compaction surface.
// ===========================================================================

/// A FULL/purge-safe compaction of ONLY {A, B} via the real [`compact_sstables`]
/// surface PURGES the gc-eligible cell tombstone — the baseline the overlap gate
/// deviates from. Re-reads the written output SSTable.
///
/// This is the one scenario driven end-to-end through the public one-shot
/// compaction API (which cannot thread an overlap bound); it proves the partial
/// path's RETENTION above is a deliberate overlap-safety deviation from the
/// full-compaction baseline, not a no-op.
#[test]
fn full_compaction_purges_cell_tombstone_baseline() {
    let s = Scenario::build(
        schema_scalar(),
        vec![live_v("v1", TS_LIVE)],
        vec![cell_tomb_v()],
        // C is irrelevant to a full compaction of {A,B} (no outside set); built
        // only to satisfy the scaffolding shape.
        vec![live_v("v1", TS_LIVE)],
    );

    let temp = TempDir::new().unwrap();
    let out_dir = temp.path().join("out");
    rt().block_on(compact_sstables(
        s.included.clone(),
        &out_dir,
        &s.schema,
        1,
        Some(GC_BEFORE),
        Some(NOW_SECS),
        /* purge_safe */ true,
    ))
    .expect("full compaction");

    let out = discover_inputs(&out_dir);
    let obs = observe(out, &s.schema, None, None, true, None);
    assert!(
        !obs.v_cell_tombstone,
        "full/purge-safe compaction MUST purge the gc-eligible cell tombstone; got {obs:?}"
    );
    assert!(
        obs.v_live.is_none(),
        "the deleted `v` must not resurrect in a full compaction output; got {obs:?}"
    );
}
