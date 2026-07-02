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
//! Cassandra byte-oracle (criterion 5) is a fail-closed skip-on-absent test — see
//! `crit5_cassandra_oracle_two_gen_open_ended_boundary` for why it is `#[ignore]`.

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
// marker in these tests, regardless of wall clock (no wall-clock race).
const NEVER_PURGE_LDT: i32 = 2_000_000_000;

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
/// `local_deletion_time` is pinned far in the future so gc-grace never purges the
/// marker.
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
        local_deletion_time: NEVER_PURGE_LDT,
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
        "closing range carries the CLOSE deletion time"
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
        "opening range carries the OPEN deletion time"
    );

    // BOTH deletion times are present in the decoded stream (the two halves of the
    // boundary's two deletion-time pairs).
    assert_ne!(
        close_ts, open_ts,
        "test precondition: the two ranges must carry DIFFERENT deletion times so this is a \
         genuine boundary (RT_INCL_END_EXCL_START class), not a same-ts coalesce"
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

/// A byte-compare against a Cassandra-compacted reference of the SAME two-generation
/// open-ended-boundary fixture used by criteria 1–4.
///
/// STATUS — no matching fixture yet; blocked on #1410.
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
/// (out of scope for this test-authoring issue). This test therefore:
///   * SKIPs when the `test_compaction_tombstone_ttl` reference keyspace is absent
///     (it is local-only / not in the pinned CI dataset today);
///   * PANICs under `CQLITE_REQUIRE_FIXTURES=1` so a strict lane never silently
///     passes on the missing oracle;
///   * is `#[ignore]` because even WITH the reference present the byte compare is
///     blocked on #1410 (the same LDT-baseline divergence class) — matching the
///     doctrine in the issue brief to reference #1410 rather than file a duplicate.
///
/// When #1410 is fixed AND an open-ended two-gen boundary fixture is committed, drop
/// the `#[ignore]` and diff Data.db/Index.db/Summary.db/Digest.crc32 as in
/// `issue_1387_tombstone_ttl_compaction_byte_parity.rs`.
#[test]
#[ignore = "blocked on #1410 (compute_baseline_min LDT-baseline byte divergence) + no matching \
            open-ended two-gen boundary Cassandra fixture (#1387 rt_cross_gen uses bounded ranges)"]
fn crit5_cassandra_oracle_two_gen_open_ended_boundary() {
    const ORACLE_KEYSPACE: &str = "test_compaction_tombstone_ttl";
    let require_strict = std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1")
        .unwrap_or(false);

    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => r,
        Err(_) => {
            if require_strict {
                panic!("CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT unset");
            }
            eprintln!(
                "[issue_1383] crit5 SKIP: CQLITE_DATASETS_ROOT unset (Cassandra open-ended \
                 two-gen boundary oracle unavailable)"
            );
            return;
        }
    };

    let base = Path::new(&root).join("sstables").join(ORACLE_KEYSPACE);
    let present = base
        .read_dir()
        .ok()
        .map(|mut d| d.next().is_some())
        .unwrap_or(false);
    if !present {
        if require_strict {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but Cassandra oracle keyspace {ORACLE_KEYSPACE} absent \
                 under {base:?}. #1383 crit5 needs an open-ended two-gen boundary fixture; the \
                 #1387 rt_cross_gen fixture (bounded ranges) does not match this scenario."
            );
        }
        eprintln!(
            "[issue_1383] crit5 SKIP: Cassandra oracle keyspace {ORACLE_KEYSPACE} absent under \
             {base:?} (local-only / not in the pinned CI dataset). See #1410 + the fixture gap \
             documented on this test."
        );
        return;
    }

    // Reference present: the byte compare is still blocked on #1410 AND on a
    // matching open-ended fixture (rt_cross_gen is bounded). Do NOT attempt a byte
    // diff against a mismatched fixture — surface the gap instead.
    panic!(
        "[issue_1383] crit5: Cassandra oracle keyspace {ORACLE_KEYSPACE} is PRESENT, but no \
         open-ended two-gen boundary fixture matching #1383 exists yet, and the byte compare is \
         blocked on #1410. Commission an open-ended-RT two-gen fixture (distinct from #1387 \
         rt_cross_gen's bounded ranges) and land #1410 before enabling this diff."
    );
}
