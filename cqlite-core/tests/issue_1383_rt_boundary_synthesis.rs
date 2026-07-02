//! Issue #1383: range-tombstone BOUNDARY-MARKER synthesis across a MULTI-SSTable
//! k-way merge.
//!
//! `issue_933_range_tombstone_compaction.rs` proves range tombstones shadow cells
//! and re-emit their markers during compaction, but every one of its range
//! tombstones is reassembled from a SINGLE SSTable. This module fills the gap it
//! left open: forcing the REAL `compact_sstables` k-way merge to SYNTHESIZE a
//! boundary at a clustering point where the OPEN bound lives in one SSTable and the
//! CLOSE/reopen lives in another with a DIFFERENT deletion time.
//!
//! ## What "boundary synthesis" means here (Cassandra ⇄ CQLite)
//! Cassandra encodes such an overlap as ONE on-disk BOUNDARY marker
//! (`ClusteringBoundOrBoundary`, kind 2 = EXCL_END_INCL_START / kind 5 =
//! INCL_END_EXCL_START) that CLOSES the older range and OPENS the newer range at the
//! same clustering point, carrying TWO deletion-time pairs. CQLite models the
//! cross-SSTable union as a NON-OVERLAPPING canonical sequence (see
//! `KWayMerger::coalesce_range_tombstones`): the boundary surfaces as TWO adjacent
//! markers that MEET at the boundary clustering point with COMPLEMENTARY
//! inclusivity — an `Exclusive(b)` END on the older range immediately followed by an
//! `Inclusive(b)` START on the newer range (kind-2 EXCL_END_INCL_START). That
//! complementary-adjacent pair IS exactly what Cassandra's kind-2 boundary decodes
//! into (`compaction.rs` bound_kind 2/5 → emit close + open at the shared point),
//! and it carries BOTH deletion times. The deterministic BYTE grammar of the
//! coalesced boundary marker itself is pinned separately by
//! `issue_992_range_boundary_grammar.rs` (`range_boundary_marker_exact_grammar_writer`,
//! kind-2/kind-5); this module pins that the MERGE SYNTHESIZES it from two
//! separate SSTables with different deletion times, plus the shadowing it implies.
//!
//! All tests drive the REAL `compact_sstables` (never `reference_merge`).
//!
//! No external Cassandra fixture is required for criteria 1–4: CQLite's writer emits
//! the same on-disk markers Cassandra does and the reader/merge consume them. The
//! Cassandra byte-oracle (criterion 5) is split in two: the eventual byte-diff lives
//! in the `#[ignore]`d `crit5_cassandra_oracle_two_gen_open_ended_boundary` (blocked
//! on #1410 + no matching open-ended fixture), while the NON-ignored
//! `crit5_oracle_fixture_contract_guard` enforces the fail-closed skip-on-absent /
//! strict-lane-panic contract in every lane.

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{compact_sstables, KWayMerger, MergeStep, RowData};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone, TableId,
};
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_core::types::Value;
use tempfile::TempDir;

const KS: &str = "rt_boundary_ks";
const TBL: &str = "rt_boundary_items";

// Pinned partition. A single partition keeps the whole scenario in one merge
// cluster so the boundary synthesis is unambiguous.
const PID: i32 = 1;

// A within-grace local-deletion-time (far future) so gc-grace never purges any
// marker in these tests, regardless of wall clock (no wall-clock race). Each range
// gets a DISTINCT far-future LDT (`NEVER_PURGE_LDT_BASE + ts`) so the test can prove
// boundary synthesis preserves BOTH full deletion-time pairs (markedForDeleteAt AND
// localDeletionTime) on each side — a regression that copied the wrong side's LDT
// would be caught (roborev #2613 Medium). All variants stay far in the future.
const NEVER_PURGE_LDT_BASE: i32 = 2_000_000_000;

/// The distinct far-future local-deletion-time a range with deletion time `ts`
/// carries. Injective in `ts` so the two sides of a boundary differ.
fn ldt_for(ts: i64) -> i32 {
    NEVER_PURGE_LDT_BASE + (ts as i32)
}

fn schema(order: ClusteringOrder) -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn write_row(ck: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(PID)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// A range-tombstone-only mutation (no row content) for the pinned partition. The
/// `local_deletion_time` is a DISTINCT far-future value (`ldt_for(ts)`) so gc-grace
/// never purges the marker AND the two sides of a synthesized boundary carry
/// different LDTs (so the test verifies both are preserved, not just mfda).
fn range_delete(start: ClusteringBound, end: ClusteringBound, ts: i64) -> Mutation {
    let mut m = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(PID)),
        None,
        vec![],
        ts,
        None,
    );
    m.range_tombstones.push(RangeTombstone {
        start,
        end,
        deletion_time: ts,
        local_deletion_time: ldt_for(ts),
    });
    m
}

fn incl(ck: i32) -> ClusteringBound {
    ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(ck)))
}

fn excl(ck: i32) -> ClusteringBound {
    ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(ck)))
}

fn flush_batch(engine: &mut WriteEngine, rt: &tokio::runtime::Runtime, muts: Vec<Mutation>) {
    for m in muts {
        engine.write(m).expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("sstable info");
}

/// Discover the flushed input generations (newest generation first), recursing
/// into the WriteEngine's keyspace/table subdirectories.
fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
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
    let mut found = Vec::new();
    collect(dir, &mut found, 8);
    // newest generation first (run index 0 = newest)
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

/// A decoded range marker as it surfaces through the compaction read path: its
/// start/end bounds plus deletion time. (`ck` extracted for readable asserts.)
#[derive(Clone, Debug, PartialEq, Eq)]
struct Marker {
    start: ClusteringBound,
    end: ClusteringBound,
    deletion_time: i64,
    local_deletion_time: i32,
}

/// What a read-back of one or more SSTables yields through the compaction read
/// path: surviving `ck` live rows (sorted) and the surviving range markers (in the
/// order emitted, i.e. clustering order).
struct ReadBack {
    live_rows: Vec<i32>,
    markers: Vec<Marker>,
}

fn read_back(inputs: Vec<PathBuf>, schema: &TableSchema) -> ReadBack {
    let mut merger = KWayMerger::new(inputs, schema).expect("KWayMerger::new");
    let mut live_rows = Vec::new();
    let mut markers = Vec::new();
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    if let Some(rt) = &entry.range_deletion {
                        markers.push(Marker {
                            start: rt.start.clone(),
                            end: rt.end.clone(),
                            deletion_time: rt.deletion_time,
                            local_deletion_time: rt.local_deletion_time,
                        });
                        continue;
                    }
                    if let RowData::Live { cells } = &entry.row_data {
                        // A genuine data row has at least one non-key cell.
                        let has_data = cells.iter().any(|c| c.column != "ck" && c.column != "id");
                        if has_data {
                            if let Some(ck) = entry.clustering_key.as_ref().and_then(ck_value) {
                                live_rows.push(ck);
                            }
                        }
                    }
                }
            }
        }
    }
    live_rows.sort_unstable();
    ReadBack { live_rows, markers }
}

fn ck_value(ck: &ClusteringKey) -> Option<i32> {
    match ck.columns.first().map(|(_, v)| v) {
        Some(Value::Integer(n)) => Some(*n),
        _ => None,
    }
}

fn bound_ck(bound: &ClusteringBound) -> Option<i32> {
    match bound {
        ClusteringBound::Inclusive(k) | ClusteringBound::Exclusive(k) => ck_value(k),
        _ => None,
    }
}

fn compact(inputs: Vec<PathBuf>, out_dir: &Path, schema: &TableSchema, generation: u64) -> PathBuf {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(
            inputs, out_dir, schema, generation, None, None, /* purge_safe */ true,
        ))
        .expect("compaction");
    report.output.data_path
}

/// Assert the two synthesized markers meet at `boundary_ck` with the classic
/// EXCL_END_INCL_START (kind-2) shape and carry BOTH deletion times. `close_ts` is
/// the deletion time of the range CLOSING at the boundary (the `[Bottom, Excl(b))`
/// side), `open_ts` the range OPENING at it (the `[Incl(b), Top]` side).
///
/// This is the CQLite decoded form of Cassandra's kind-2 boundary marker: an
/// `Exclusive(b)` end immediately adjacent to an `Inclusive(b)` start at the same
/// clustering point, closing `close_ts` and opening `open_ts`.
fn assert_boundary_at(markers: &[Marker], boundary_ck: i32, close_ts: i64, open_ts: i64) {
    assert_eq!(
        markers.len(),
        2,
        "boundary synthesis must produce exactly TWO disjoint markers meeting at the boundary, \
         got {markers:#?}"
    );
    let closing = &markers[0];
    let opening = &markers[1];

    // Closing range: [Bottom, Exclusive(boundary_ck)) @ close_ts.
    assert!(
        matches!(closing.start, ClusteringBound::Bottom),
        "closing range starts open-from-Bottom, got {:?}",
        closing.start
    );
    assert!(
        matches!(&closing.end, ClusteringBound::Exclusive(_)),
        "closing range must END EXCLUSIVE at the boundary (the EXCL_END half of the kind-2 \
         boundary), got {:?}",
        closing.end
    );
    assert_eq!(
        bound_ck(&closing.end),
        Some(boundary_ck),
        "closing range's exclusive end sits at the boundary clustering point"
    );
    assert_eq!(
        closing.deletion_time, close_ts,
        "closing range carries the CLOSE deletion time (markedForDeleteAt)"
    );
    assert_eq!(
        closing.local_deletion_time,
        ldt_for(close_ts),
        "closing range carries the CLOSE side's OWN localDeletionTime — proving both full \
         deletion-time pairs are preserved through synthesis, not just markedForDeleteAt"
    );

    // Opening range: [Inclusive(boundary_ck), Top] @ open_ts.
    assert!(
        matches!(&opening.start, ClusteringBound::Inclusive(_)),
        "opening range must START INCLUSIVE at the boundary (the INCL_START half of the kind-2 \
         boundary), got {:?}",
        opening.start
    );
    assert_eq!(
        bound_ck(&opening.start),
        Some(boundary_ck),
        "opening range's inclusive start sits at the SAME boundary clustering point as the \
         closing range's exclusive end — the two meet, forming the boundary"
    );
    assert!(
        matches!(opening.end, ClusteringBound::Top),
        "opening range runs open-to-Top, got {:?}",
        opening.end
    );
    assert_eq!(
        opening.deletion_time, open_ts,
        "opening range carries the OPEN deletion time (markedForDeleteAt)"
    );
    assert_eq!(
        opening.local_deletion_time,
        ldt_for(open_ts),
        "opening range carries the OPEN side's OWN localDeletionTime (distinct from the close \
         side's) — the two deletion-time pairs are not conflated"
    );

    // BOTH deletion-time PAIRS are present in the decoded stream (the two halves of
    // the boundary's two deletion-time pairs): distinct markedForDeleteAt AND
    // distinct localDeletionTime per side.
    assert_ne!(
        close_ts, open_ts,
        "test precondition: the two ranges must carry DIFFERENT deletion times so this is a \
         genuine boundary (RT_INCL_END_EXCL_START class), not a same-ts coalesce"
    );
    assert_ne!(
        closing.local_deletion_time, opening.local_deletion_time,
        "test precondition: the two ranges carry DISTINCT localDeletionTimes so a swap of the \
         wrong side's LDT would be detectable"
    );
}

/// Build a fresh WriteEngine over `data_dir` and return `(engine, runtime)`.
fn engine(
    temp: &TempDir,
    sub: &str,
    schema: &TableSchema,
) -> (WriteEngine, tokio::runtime::Runtime) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let e = WriteEngine::new(WriteEngineConfig::new(
        temp.path().join(format!("{sub}-data")),
        temp.path().join(format!("{sub}-wal")),
        schema.clone(),
    ))
    .unwrap();
    (e, rt)
}

// ════════════════════════════════════════════════════════════════════════════
// Criterion 1 — BOUNDARY SYNTHESIS
// ════════════════════════════════════════════════════════════════════════════

/// gen-1 RT `[Bottom, ck=5)` @ts=10, gen-2 RT `[ck=3, Top]` @ts=20 → the k-way
/// merge must SYNTHESIZE a boundary at ck=3: the older range CLOSES just before 3
/// (`Exclusive(3)` @ts=10) and the newer range OPENS at 3 (`Inclusive(3)` @ts=20),
/// carrying BOTH deletion times. Neither input SSTable alone contains this
/// boundary — it exists only because the OPEN bound is in gen-2 and the reach of
/// the older range is in gen-1. Drives the REAL `compact_sstables`.
#[test]
fn crit1_boundary_synthesis_across_two_sstables() {
    let schema = schema(ClusteringOrder::Asc);
    let temp = TempDir::new().unwrap();
    let (mut engine, rt) = engine(&temp, "c1", &schema);

    // gen-1: range [Bottom, 5) @ ts=10 (open-from-bottom, close EXCLUSIVE at 5).
    flush_batch(
        &mut engine,
        &rt,
        vec![range_delete(ClusteringBound::Bottom, excl(5), 10)],
    );
    // gen-2: range [3, Top] @ ts=20 (open INCLUSIVE at 3, open-to-top).
    flush_batch(
        &mut engine,
        &rt,
        vec![range_delete(incl(3), ClusteringBound::Top, 20)],
    );
    rt.block_on(engine.close()).unwrap();

    let inputs = discover_inputs(&temp.path().join("c1-data"));
    assert!(
        inputs.len() >= 2,
        "expected >= 2 input generations, got {}",
        inputs.len()
    );

    let out_dir = temp.path().join("c1-out");
    let output = compact(inputs, &out_dir, &schema, 1_383_001);
    let rb = read_back(vec![output], &schema);

    // The heart of #1383: the boundary at ck=3 closing ts=10 + opening ts=20.
    assert_boundary_at(&rb.markers, 3, /* close */ 10, /* open */ 20);
    assert!(
        rb.live_rows.is_empty(),
        "no live rows were written, only range tombstones"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// Criterion 2 — SHADOWING ACROSS THE BOUNDARY
// ════════════════════════════════════════════════════════════════════════════

/// Live cells across the synthesized boundary are shadowed by the correct side.
/// Same RT geometry as crit1 (`[Bottom,5)` @10 in gen-1, `[3,Top]` @20 in gen-2),
/// with live rows chosen so each shadow rule is exercised by a DISTINCT clustering
/// key (avoiding LWW conflation of same-ck writes):
///   * ck=4 @ts=15 → ABSENT: covered by the ts=20 side (`[3,Top]`), 15 <= 20.
///   * ck=6 @ts=25 → SURVIVES: in the ts=20 side but 25 > 20.
///   * ck=2 @ts=15 → SURVIVES: only the ts=10 side (`[Bottom,3)`) covers ck=2, 15 > 10.
///   * ck=1 @ts=5  → ABSENT: covered by the ts=10 side, 5 <= 10.
#[test]
fn crit2_shadowing_across_boundary() {
    let schema = schema(ClusteringOrder::Asc);
    let temp = TempDir::new().unwrap();
    let (mut engine, rt) = engine(&temp, "c2", &schema);

    // gen-1: the ts=10 range + rows whose survival depends on the ts=10 side.
    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(ClusteringBound::Bottom, excl(5), 10),
            write_row(2, "ck2-ts15", 15), // survives (ts=10 side, 15>10)
            write_row(1, "ck1-ts5", 5),   // absent (ts=10 side, 5<=10)
            write_row(6, "ck6-ts25", 25), // survives (ts=20 side, 25>20)
        ],
    );
    // gen-2: the ts=20 range + a row shadowed by the ts=20 side.
    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(incl(3), ClusteringBound::Top, 20),
            write_row(4, "ck4-ts15", 15), // absent (ts=20 side, 15<=20)
        ],
    );
    rt.block_on(engine.close()).unwrap();

    let inputs = discover_inputs(&temp.path().join("c2-data"));
    let out_dir = temp.path().join("c2-out");
    let output = compact(inputs, &out_dir, &schema, 1_383_002);
    let rb = read_back(vec![output], &schema);

    assert_eq!(
        rb.live_rows,
        vec![2, 6],
        "ck=2 (postdates the ts=10 side) and ck=6 (postdates the ts=20 side) survive; \
         ck=1 (shadowed by ts=10) and ck=4 (shadowed by ts=20) are gone"
    );
    // The synthesized boundary must STILL be present and correct alongside the rows.
    assert_boundary_at(&rb.markers, 3, /* close */ 10, /* open */ 20);
}

// ════════════════════════════════════════════════════════════════════════════
// Criterion 3 — INVERSE ORDERING (older RT in gen-2)
// ════════════════════════════════════════════════════════════════════════════

/// Swap which GENERATION holds each RT — the NEWER range (`[3,Top]` @ts=20) now
/// lives in gen-1 and the OLDER range (`[Bottom,5)` @ts=10) in gen-2. Coalescing
/// depends only on deletion time, not generation placement, so the synthesized
/// boundary and the survivals are IDENTICAL to crit2. This proves the boundary is
/// generation-order-independent (it is a function of the (range, ts) pairs, not
/// which SSTable carried them).
#[test]
fn crit3_inverse_generation_ordering_mirrors_boundary() {
    let schema = schema(ClusteringOrder::Asc);
    let temp = TempDir::new().unwrap();
    let (mut engine, rt) = engine(&temp, "c3", &schema);

    // gen-1 now holds the NEWER range + the same live rows as crit2.
    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(incl(3), ClusteringBound::Top, 20),
            write_row(2, "ck2-ts15", 15),
            write_row(1, "ck1-ts5", 5),
            write_row(6, "ck6-ts25", 25),
        ],
    );
    // gen-2 now holds the OLDER range + the shadowed row.
    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(ClusteringBound::Bottom, excl(5), 10),
            write_row(4, "ck4-ts15", 15),
        ],
    );
    rt.block_on(engine.close()).unwrap();

    let inputs = discover_inputs(&temp.path().join("c3-data"));
    let out_dir = temp.path().join("c3-out");
    let output = compact(inputs, &out_dir, &schema, 1_383_003);
    let rb = read_back(vec![output], &schema);

    // Mirrored boundary (identical to crit2): ck=3, close ts=10 / open ts=20.
    assert_boundary_at(&rb.markers, 3, /* close */ 10, /* open */ 20);
    // Identical survivals.
    assert_eq!(
        rb.live_rows,
        vec![2, 6],
        "swapping the generation each RT lives in does not change the synthesized boundary or \
         the surviving rows"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// Criterion 4 — READ-BACK EQUIVALENCE
// ════════════════════════════════════════════════════════════════════════════

/// scan(compacted) == scan(gen-1 + gen-2), row-for-row AND marker-for-marker.
/// Reading the compacted output through the k-way merge must yield exactly what
/// reading the two ORIGINAL generations together yields — the compaction is a
/// faithful materialization of the merge (no rows resurrected, no markers dropped
/// or corrupted, boundary synthesized identically on both paths).
#[test]
fn crit4_readback_equivalence_compacted_vs_inputs() {
    let schema = schema(ClusteringOrder::Asc);
    let temp = TempDir::new().unwrap();
    let (mut engine, rt) = engine(&temp, "c4", &schema);

    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(ClusteringBound::Bottom, excl(5), 10),
            write_row(2, "ck2-ts15", 15),
            write_row(1, "ck1-ts5", 5),
            write_row(6, "ck6-ts25", 25),
        ],
    );
    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(incl(3), ClusteringBound::Top, 20),
            write_row(4, "ck4-ts15", 15),
        ],
    );
    rt.block_on(engine.close()).unwrap();

    let inputs = discover_inputs(&temp.path().join("c4-data"));
    assert!(inputs.len() >= 2, "expected >= 2 inputs");

    // Path A: merge the two ORIGINAL input generations directly.
    let via_inputs = read_back(inputs.clone(), &schema);

    // Path B: compact the inputs, then read the single compacted output back.
    let out_dir = temp.path().join("c4-out");
    let output = compact(inputs, &out_dir, &schema, 1_383_004);
    let via_compacted = read_back(vec![output], &schema);

    assert_eq!(
        via_compacted.live_rows, via_inputs.live_rows,
        "scan(compacted) live rows must equal scan(gen-1 + gen-2) live rows"
    );
    assert_eq!(
        via_compacted.markers, via_inputs.markers,
        "scan(compacted) markers must equal scan(gen-1 + gen-2) markers (boundary synthesized \
         identically on both read paths)"
    );
    // And both carry the synthesized boundary.
    assert_boundary_at(&via_compacted.markers, 3, 10, 20);
}

// ════════════════════════════════════════════════════════════════════════════
// Criterion 5 — CASSANDRA BYTE ORACLE (fail-closed skip-on-absent)
// ════════════════════════════════════════════════════════════════════════════

/// The keyspace under which the closest Cassandra-compacted range-tombstone-merge
/// oracles live (#1387's tombstone/TTL/RT byte-fixture family). This keyspace may
/// ALREADY be present for OTHER #1387 fixtures (`rt_cross_gen`, `shadow_row_delete`,
/// …) without the #1383-specific oracle existing — so presence is scoped to the
/// specific table below, NOT the keyspace (roborev #2616 High).
const ORACLE_KEYSPACE: &str = "test_compaction_tombstone_ttl";

/// The SPECIFIC table a #1383 open-ended two-gen boundary oracle would be written
/// under (distinct from #1387's bounded `rt_cross_gen`). It does not exist yet; when
/// committed, its directory is `{ORACLE_TABLE}-<uuid>/` carrying an `nb-*-big-Data.db`.
const ORACLE_TABLE: &str = "rt_open_ended_boundary";

/// Presence of the SPECIFIC #1383 open-ended-boundary oracle fixture:
///   * `None`       → `CQLITE_DATASETS_ROOT` unset (dataset root unknown).
///   * `Some(false)` → root known, but no `{ORACLE_TABLE}-*` dir with a Data.db (the
///     #1383 oracle is absent — regardless of whether the keyspace exists for other
///     #1387 fixtures).
///   * `Some(true)`  → a `{ORACLE_TABLE}-*` dir with a compacted `nb-*-big-Data.db`.
fn oracle_fixture_present() -> Option<bool> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = Path::new(&root).join("sstables").join(ORACLE_KEYSPACE);
    let Ok(entries) = base.read_dir() else {
        // Keyspace dir absent/unreadable ⇒ the specific oracle is absent.
        return Some(false);
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(&format!("{ORACLE_TABLE}-")) {
            continue;
        }
        // Only a table dir carrying a compacted Data.db counts as "present".
        if let Ok(inner) = entry.path().read_dir() {
            let has_data = inner.flatten().any(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.starts_with("nb-") && n.ends_with("-big-Data.db")
            });
            if has_data {
                return Some(true);
            }
        }
    }
    Some(false)
}

/// Strict fixture lane. Accepts both `1` and `true` (suite convention; roborev
/// #2616 Low).
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Fail-closed fixture-presence contract for the eventual Cassandra byte oracle
/// (roborev #2613 Low, #2616 High). This test is NOT `#[ignore]`, so it runs in the
/// normal AND strict (`CQLITE_REQUIRE_FIXTURES=1|true`) fixture lanes and enforces the
/// contract the ignored byte-diff test documents but cannot execute. Presence is
/// scoped to the SPECIFIC `{ORACLE_TABLE}` fixture, so a dataset that already carries
/// the #1387 keyspace for OTHER fixtures does NOT trip this guard:
///   * dataset root unset + not strict → SKIP;
///   * #1383 oracle fixture absent + not strict → SKIP (local-only / not yet built);
///   * absent + strict → PANIC (strict lane never silently passes on a missing
///     oracle it claims to require — but only for the #1383-specific fixture);
///   * #1383 oracle fixture PRESENT → surface the GAP: enable the byte-diff test.
///
/// It deliberately does NOT attempt a byte diff (blocked on #1410); it only guards
/// presence/shape so the strict-lane claim is real.
#[test]
fn crit5_oracle_fixture_contract_guard() {
    match oracle_fixture_present() {
        None => {
            if require_fixtures_strict() {
                panic!("CQLITE_REQUIRE_FIXTURES set but CQLITE_DATASETS_ROOT unset");
            }
            eprintln!(
                "[issue_1383] crit5 guard SKIP: CQLITE_DATASETS_ROOT unset (Cassandra open-ended \
                 two-gen boundary oracle unavailable)"
            );
        }
        Some(false) => {
            if require_fixtures_strict() {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES set but the #1383 open-ended two-gen boundary oracle \
                     fixture {ORACLE_KEYSPACE}.{ORACLE_TABLE} is absent. It has not been \
                     commissioned yet (the #1387 rt_cross_gen fixture uses BOUNDED ranges and does \
                     not match this scenario); build it + land #1410 to enable the byte diff."
                );
            }
            eprintln!(
                "[issue_1383] crit5 guard SKIP: #1383 oracle fixture \
                 {ORACLE_KEYSPACE}.{ORACLE_TABLE} absent (local-only / not yet built). See #1410 \
                 + the fixture gap on crit5_cassandra_oracle_two_gen_open_ended_boundary."
            );
        }
        Some(true) => panic!(
            "[issue_1383] crit5 guard: the #1383 oracle fixture {ORACLE_KEYSPACE}.{ORACLE_TABLE} \
             is now PRESENT. Enable crit5_cassandra_oracle_two_gen_open_ended_boundary (drop its \
             #[ignore] once #1410 has landed) and wire the byte diff."
        ),
    }
}

/// A byte-compare against a Cassandra-compacted reference of the SAME two-generation
/// open-ended-boundary fixture used by criteria 1–4.
///
/// STATUS — `#[ignore]`: no matching fixture yet AND blocked on #1410.
///
/// The #1387 tombstone/TTL/RT byte-fixture family (keyspace
/// `test_compaction_tombstone_ttl`, scenario `rt_cross_gen`) is the closest existing
/// Cassandra-compacted range-tombstone-merge oracle, but it does NOT match this
/// scenario:
///   * `rt_cross_gen` merges two BOUNDED inclusive ranges (`[10,20]` and `[15,25]`),
///     whereas #1383 merges two OPEN-ENDED ranges (`[Bottom,5)` and `[3,Top]`) — a
///     different marker geometry (open Bottom/Top bounds vs closed bounds).
///   * `rt_cross_gen` is itself `#[ignore = "blocked on #1410"]`: building those
///     fixtures revealed the `compute_baseline_min` localDeletionTime baseline bug
///     (#1410), so its byte diff cannot pass until #1410 lands.
///
/// A dedicated open-ended two-gen boundary byte fixture was NOT commissioned here
/// (out of scope for this test-authoring issue). Because this test is `#[ignore]`, it
/// does NOT itself run in the strict fixture lane — the fail-closed presence contract
/// is enforced by the NON-ignored `crit5_oracle_fixture_contract_guard` above, which
/// runs in every lane. This test carries the eventual byte-diff body.
///
/// When #1410 is fixed AND an open-ended two-gen boundary fixture is committed, drop
/// the `#[ignore]` and diff Data.db/Index.db/Summary.db/Digest.crc32 as in
/// `issue_1387_tombstone_ttl_compaction_byte_parity.rs`.
#[test]
#[ignore = "blocked on #1410 (compute_baseline_min LDT-baseline byte divergence) + no matching \
            open-ended two-gen boundary Cassandra fixture (#1387 rt_cross_gen uses bounded ranges)"]
fn crit5_cassandra_oracle_two_gen_open_ended_boundary() {
    // Enabling this test (dropping #[ignore]) requires BOTH #1410 fixed AND a
    // committed open-ended two-gen boundary fixture; until then the presence/shape
    // contract is guarded by crit5_oracle_fixture_contract_guard.
    match oracle_fixture_present() {
        None | Some(false) => {
            eprintln!("[issue_1383] crit5 byte-diff SKIP: oracle fixture absent (see guard)");
        }
        Some(true) => panic!(
            "[issue_1383] crit5: oracle fixture {ORACLE_KEYSPACE}.{ORACLE_TABLE} PRESENT but the \
             open-ended two-gen boundary byte diff is not yet wired (blocked on #1410). Wire the \
             Data.db/Index.db/Summary.db/Digest.crc32 diff here once #1410 lands."
        ),
    }
}
