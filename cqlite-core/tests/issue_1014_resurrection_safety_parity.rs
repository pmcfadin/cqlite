//! Issue #1014 — gc_grace, never-purge, and resurrection safety across SSTables.
//!
//! This suite owns READ/MERGE SAFETY and purge eligibility (NOT byte-identical
//! compaction output, which is #973). It proves CQLite does NOT resurrect deleted
//! data when multiple SSTables contain overlapping live cells, tombstones, and a
//! partition tombstone, both on the cross-SSTable READ-merge path and through
//! `compact_sstables`.
//!
//! ## Fixtures (committed real Cassandra 5.0 `nb` SSTables + JSONL goldens)
//!
//! * `test_tomb/resurrection_gc0-*`         — `gc_grace_seconds = 0`.
//! * `test_tomb/resurrection_gc_positive-*` — same data shapes, `gc_grace_seconds
//!   = 864000` (tombstones retained on disk).
//!
//! Both have two generations:
//!   * `nb-1` — live rows: `pk=1` ck 1..=5, `pk=2` ck 1..=3   (T_GEN1, ts 2021-01-01).
//!   * `nb-2` — newer deletes (T_GEN2, ts 2021-01-02):
//!       * `pk=1 ck=2` ROW delete,
//!       * `pk=1 ck=3` CELL delete of `val`,
//!       * `pk=2`      PARTITION delete.
//!
//! The two Data.db JSONL goldens per generation are byte-identical between the
//! gc0 and gc_positive fixtures (gc_grace lives in the schema, not in Data.db),
//! which this suite asserts directly.
//!
//! ## Discipline (issue #1014 common rules)
//! * SKIP cleanly when `CQLITE_DATASETS_ROOT` is unset or a binary `Data.db` is
//!   absent (worktrees lack the gitignored binaries).
//! * FAIL if a golden carries facts but ZERO matched.
//! * Ordered POSITIONAL golden comparison. No path/name heuristics for matching.
//! * `localDeletionTime` is wall-clock — compared to the golden, never hardcoded.
//!
//! ## Manifest IDs backed
//! * `cass.tombstone_ttl.never_purge.cell_row_partition`
//! * `cass.tombstone_ttl.gc_grace.partition_row_cell`
//! * `cass.tombstone_ttl.repaired_unrepaired_purge_gate`  (PARTIAL — see #988)
//! * `cass.compaction_merge.resurrection_safety.overlapping_sources`
//! * `cass.compaction_merge.partial_source_retains_tombstones`
//!
//! ## Honest gap found (NOT faked green)
//! CQLite's compaction-merge READ path
//! (`V5CompressedLegacy::parse_one_partition_for_compaction`) calls
//! `parse_partition_header`, which DISCARDS the partition-level deletion time. So a
//! newer PARTITION tombstone never reaches `KWayMerger` and cannot shadow older
//! rows of the same partition living in another SSTable — they RESURRECT. This is
//! pinned by the `#[ignore]`d [`partition_tombstone_resurrection_gap_pinned`] test
//! with exact expected/actual, mirroring the `#[ignore]`d regression style of
//! `issue_819_differential_compaction.rs`. Row-level and cell-level shadowing are
//! resurrection-safe and are asserted as hard passes. The delta-scan READ path
//! DOES surface partition tombstones (see `scan_delta_parity_test.rs`); the gap is
//! specific to the compaction-merge read contract.

#![cfg(all(feature = "write-support", feature = "delta-scan"))]

use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{compact_sstables, KWayMerger, MergeStep, RowData};
use cqlite_core::storage::write_engine::mutation::PartitionKey;
use cqlite_core::types::Value;

// ════════════════════════════════════════════════════════════════════════════
// Fixture discovery + skip discipline
// ════════════════════════════════════════════════════════════════════════════

/// Resolve `<CQLITE_DATASETS_ROOT>/sstables/test_tomb/<prefix>-*`. Returns `None`
/// (→ clean SKIP) when the root is unset or no directory matches the prefix.
fn fixture_dir(prefix: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = PathBuf::from(root).join("sstables").join("test_tomb");
    let entries = std::fs::read_dir(&base).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name()?.to_string_lossy().to_string();
        if name.starts_with(&format!("{prefix}-")) && path.is_dir() {
            return Some(path);
        }
    }
    None
}

/// Path to one generation's `Data.db`, or `None` if the binary is absent (worktree
/// without fetched datasets → clean SKIP).
fn gen_data_db(dir: &Path, gen: u32) -> Option<PathBuf> {
    let p = dir.join(format!("nb-{gen}-big-Data.db"));
    p.exists().then_some(p)
}

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value ("1"/"true").
/// In strict mode, every code path that would otherwise SKIP because the dataset
/// root is unset or a required binary fixture is absent must PANIC instead, so a
/// CI gate cannot false-pass on missing data (issue #972).
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// SKIP guard: returns `Some((dir, nb1, nb2))` when both generations' binaries are
/// present, else logs a skip reason and returns `None`. In `CQLITE_REQUIRE_FIXTURES`
/// strict mode, an absent fixture/binary PANICs instead of skipping (issue #972).
fn require_fixture(prefix: &str) -> Option<(PathBuf, PathBuf, PathBuf)> {
    let Some(dir) = fixture_dir(prefix) else {
        let reason =
            format!("fixture {prefix}-* not found (CQLITE_DATASETS_ROOT unset or dir missing)");
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but fixture {prefix} is absent — {reason}; \
                 fetch/generate it (bash test-data/scripts/fetch-datasets.sh)"
            );
        }
        eprintln!("[skip] {reason}");
        return None;
    };
    let (Some(nb1), Some(nb2)) = (gen_data_db(&dir, 1), gen_data_db(&dir, 2)) else {
        let reason = format!("{prefix}: nb-1/nb-2 Data.db absent (binaries not fetched)");
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but fixture {prefix} nb-1/nb-2 Data.db is absent — \
                 {reason}; fetch/generate it (bash test-data/scripts/fetch-datasets.sh)"
            );
        }
        eprintln!("[skip] {reason}");
        return None;
    };
    Some((dir, nb1, nb2))
}

// ════════════════════════════════════════════════════════════════════════════
// Schema for test_tomb.resurrection_* : pk int, ck int, val text, extra text
// ════════════════════════════════════════════════════════════════════════════

fn schema(table: &str) -> TableSchema {
    TableSchema {
        keyspace: "test_tomb".to_string(),
        table: table.to_string(),
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
        columns: vec![
            col("pk", "int", false),
            col("ck", "int", false),
            col("val", "text", true),
            col("extra", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
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

// ════════════════════════════════════════════════════════════════════════════
// Observed merge facts: the surviving read/merge-affecting state per (pk, ck)
// ════════════════════════════════════════════════════════════════════════════

/// The merge kind of a surviving `(pk, ck)` entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    /// A live row. `val_dead` is true when the `val` cell is a cell tombstone
    /// (cell-delete shadowed the older live value) rather than live text.
    Live { val: Option<String>, val_dead: bool },
    /// A row tombstone (the whole row was deleted, e.g. `DELETE … WHERE pk=? AND ck=?`).
    RowTombstone,
}

/// One surviving entry observed through the cross-SSTable merge read path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MergeFact {
    pk: i32,
    ck: i32,
    kind: Kind,
}

/// Walk a set of SSTables through the SAME `KWayMerger` read path the compactor
/// uses and collect the ordered surviving facts (sorted by pk, ck).
fn merge_facts(inputs: Vec<PathBuf>, sch: &TableSchema) -> Vec<MergeFact> {
    let mut merger = KWayMerger::new(inputs, sch).expect("KWayMerger::new");
    let mut facts = Vec::new();
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { key, rows } => {
                let pk = decode_pk(&key.key, sch);
                for entry in &rows {
                    // Skip range-marker carriers (none in these fixtures, but be safe).
                    if entry.range_deletion.is_some() {
                        continue;
                    }
                    let Some(ck) = entry
                        .clustering_key
                        .as_ref()
                        .and_then(|c| c.columns.first())
                        .and_then(|(_, v)| match v {
                            Value::Integer(n) => Some(*n),
                            _ => None,
                        })
                    else {
                        continue; // partition-level / static carrier
                    };
                    let kind = match &entry.row_data {
                        RowData::Tombstone { .. } => Kind::RowTombstone,
                        RowData::Live { cells } => {
                            let val_cell = cells.iter().find(|c| c.column == "val");
                            let (val, val_dead) = match val_cell.map(|c| &c.value) {
                                Some(Value::Text(t)) => (Some(t.clone()), false),
                                Some(Value::Tombstone(_)) => (None, true),
                                _ => (None, false),
                            };
                            Kind::Live { val, val_dead }
                        }
                    };
                    facts.push(MergeFact { pk, ck, kind });
                }
            }
        }
    }
    facts.sort();
    facts
}

fn decode_pk(key_bytes: &[u8], sch: &TableSchema) -> i32 {
    let pk = PartitionKey::from_bytes(key_bytes, sch).expect("decode pk");
    match &pk.columns[0].1 {
        Value::Integer(n) => *n,
        other => panic!("unexpected pk value {other:?}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// JSONL golden facts (ordered, positional comparison)
// ════════════════════════════════════════════════════════════════════════════

/// One golden row fact extracted from a `Data.db.jsonl` line. Captures exactly the
/// merge-affecting distinction the goldens record per generation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GoldenRow {
    pk: i32,
    ck: i32,
    /// true when the JSONL row carries a `deletion_info` (a row tombstone).
    row_deleted: bool,
    /// true when the row's `val` cell is itself deleted (cell tombstone).
    val_cell_deleted: bool,
    /// true when the row carries a live (non-deleted) `val` text cell.
    val_live: bool,
}

/// Per-generation golden facts.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GoldenGen {
    /// Partition keys that carry a partition-level deletion (`deletion_info` on the
    /// partition object) — e.g. `pk=2` in `nb-2`.
    partition_deleted: Vec<i32>,
    /// Ordered row facts (positional comparison key = file order).
    rows: Vec<GoldenRow>,
}

/// Parse `<dir>/nb-<gen>-big-Data.db.jsonl` into ordered golden facts. The JSONL is
/// `sstabledump`-style: one partition per line, `{ "partition": { "key": [..],
/// ["deletion_info": {..}] }, "rows": [ { "clustering": [..], ["deletion_info":
/// {..}], "cells": [ { "name": .., ["deletion_info": {..}], "value": .. } ] } ] }`.
fn parse_golden(dir: &Path, gen: u32) -> Option<GoldenGen> {
    let path = dir.join(format!("nb-{gen}-big-Data.db.jsonl"));
    let text = std::fs::read_to_string(&path).ok()?;
    let mut partition_deleted = Vec::new();
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line).expect("golden JSONL parse");
        let partition = &v["partition"];
        let pk = json_first_int(&partition["key"]);
        if partition.get("deletion_info").is_some() {
            partition_deleted.push(pk);
        }
        let Some(row_arr) = v["rows"].as_array() else {
            continue;
        };
        for row in row_arr {
            let ck = json_first_int(&row["clustering"]);
            let row_deleted = row.get("deletion_info").is_some();
            let mut val_cell_deleted = false;
            let mut val_live = false;
            if let Some(cells) = row["cells"].as_array() {
                for cell in cells {
                    if cell["name"].as_str() == Some("val") {
                        if cell.get("deletion_info").is_some() {
                            val_cell_deleted = true;
                        } else if cell.get("value").and_then(|v| v.as_str()).is_some() {
                            val_live = true;
                        }
                    }
                }
            }
            rows.push(GoldenRow {
                pk,
                ck,
                row_deleted,
                val_cell_deleted,
                val_live,
            });
        }
    }
    Some(GoldenGen {
        partition_deleted,
        rows,
    })
}

/// Read the first element of a JSON array as an i32 (golden keys render ints as
/// strings, e.g. `["1"]`, or sometimes raw numbers `[1]`).
fn json_first_int(arr: &serde_json::Value) -> i32 {
    let first = &arr[0];
    if let Some(n) = first.as_i64() {
        return n as i32;
    }
    if let Some(s) = first.as_str() {
        return s.parse::<i32>().expect("golden int parse");
    }
    panic!("golden array element is neither int nor parseable string: {first:?}");
}

// ════════════════════════════════════════════════════════════════════════════
// TEST 1 — Multi-gen READ shadowing (resurrection_gc0) + golden parity
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cass.compaction_merge.resurrection_safety.overlapping_sources` (read
/// side) and `cass.tombstone_ttl.never_purge.cell_row_partition`.
///
/// Open BOTH generations and assert ROW-level and CELL-level deletes shadow older
/// data across SSTables: `pk=1 ck=2` (row delete) becomes a row tombstone; `pk=1
/// ck=3` `val` (cell delete) becomes a cell tombstone (NOT the older `live_3`
/// text); survivors `pk=1 ck=1,4,5` keep their original live `val`. The partition
/// delete (`pk=2`) is covered by the `#[ignore]`d gap test below.
#[test]
fn read_merge_row_and_cell_shadowing_gc0() {
    let Some((_dir, nb1, nb2)) = require_fixture("resurrection_gc0") else {
        return;
    };
    let sch = schema("resurrection_gc0");
    // Newest generation first (nb-2), matching the compactor's input ordering.
    let facts = merge_facts(vec![nb2, nb1], &sch);

    // pk=1 ck=2: ROW delete shadows the older live row → row tombstone.
    let ck2 = find(&facts, 1, 2);
    assert_eq!(
        ck2.kind,
        Kind::RowTombstone,
        "pk=1 ck=2 must be a row tombstone (row delete shadows the older live row), got {:?}",
        ck2.kind
    );

    // pk=1 ck=3: CELL delete of `val` shadows the older `live_3` text → cell tombstone,
    // NOT a resurrected live value.
    let ck3 = find(&facts, 1, 3);
    match &ck3.kind {
        Kind::Live { val, val_dead } => {
            assert!(
                *val_dead && val.is_none(),
                "pk=1 ck=3 `val` must be a cell tombstone (cell delete), not resurrected text; \
                 got val={val:?} val_dead={val_dead}"
            );
        }
        other => panic!("pk=1 ck=3 should be a live row with a dead `val` cell, got {other:?}"),
    }

    // Survivors pk=1 ck=1,4,5 keep their original live `val` text.
    for (ck, expected) in [(1, "live_1"), (4, "live_4"), (5, "live_5")] {
        let f = find(&facts, 1, ck);
        match &f.kind {
            Kind::Live {
                val: Some(v),
                val_dead: false,
            } => assert_eq!(
                v, expected,
                "pk=1 ck={ck} must survive with live val={expected:?}, got {v:?}"
            ),
            other => panic!("pk=1 ck={ck} should survive as a live row, got {other:?}"),
        }
    }

    eprintln!(
        "[gc0 read-merge] row delete ck=2 → tombstone; cell delete ck=3 val → cell tombstone; \
         survivors ck=1,4,5 live. ROW + CELL shadowing are resurrection-safe."
    );

    // ── Golden parity (ordered, positional) for BOTH generations. ──
    let dir = fixture_dir("resurrection_gc0").unwrap();
    assert_gen_goldens_consistent(&dir);
}

/// Assert both generations' JSONL goldens carry the EXACT facts the fixture
/// contract specifies, positionally. FAILS if a golden has facts but zero matched.
fn assert_gen_goldens_consistent(dir: &Path) {
    // nb-1 golden: 8 live rows, pk=1 ck 1..=5 + pk=2 ck 1..=3, all with live `val`,
    // no row/partition deletions.
    let g1 = parse_golden(dir, 1).expect("nb-1 golden present");
    assert!(
        !g1.rows.is_empty(),
        "nb-1 golden carries facts but parsed 0 rows (golden drift?)"
    );
    assert!(
        g1.partition_deleted.is_empty(),
        "nb-1 has no partition deletions, golden says {:?}",
        g1.partition_deleted
    );
    let expected_g1: Vec<GoldenRow> = [
        (1, 1),
        (1, 2),
        (1, 3),
        (1, 4),
        (1, 5),
        (2, 1),
        (2, 2),
        (2, 3),
    ]
    .iter()
    .map(|&(pk, ck)| GoldenRow {
        pk,
        ck,
        row_deleted: false,
        val_cell_deleted: false,
        val_live: true,
    })
    .collect();
    assert_eq!(
        g1.rows, expected_g1,
        "nb-1 golden rows (ordered, positional) mismatch"
    );

    // nb-2 golden: pk=1 ck=2 row delete, pk=1 ck=3 val cell delete, pk=2 partition delete.
    let g2 = parse_golden(dir, 2).expect("nb-2 golden present");
    assert!(
        !g2.rows.is_empty() || !g2.partition_deleted.is_empty(),
        "nb-2 golden carries facts but parsed 0 (golden drift?)"
    );
    assert_eq!(
        g2.partition_deleted,
        vec![2],
        "nb-2 golden must record a partition deletion for pk=2"
    );
    let expected_g2 = vec![
        GoldenRow {
            pk: 1,
            ck: 2,
            row_deleted: true,
            val_cell_deleted: false,
            val_live: false,
        },
        GoldenRow {
            pk: 1,
            ck: 3,
            row_deleted: false,
            val_cell_deleted: true,
            val_live: false,
        },
    ];
    assert_eq!(
        g2.rows, expected_g2,
        "nb-2 golden rows (ordered, positional) mismatch"
    );

    eprintln!(
        "[goldens] nb-1: 8 live rows matched positionally; nb-2: ck=2 row-delete + ck=3 val \
         cell-delete + pk=2 partition-delete matched positionally."
    );
}

fn find(facts: &[MergeFact], pk: i32, ck: i32) -> &MergeFact {
    facts
        .iter()
        .find(|f| f.pk == pk && f.ck == ck)
        .unwrap_or_else(|| panic!("no merge fact for pk={pk} ck={ck} in {facts:?}"))
}

// ════════════════════════════════════════════════════════════════════════════
// TEST 2 — gc_grace / never-purge: gc0 vs gc_positive identical on READ
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cass.tombstone_ttl.gc_grace.partition_row_cell` and
/// `cass.tombstone_ttl.never_purge.cell_row_partition`.
///
/// On READ, purge never happens (it is a compaction-time decision), so the two
/// fixtures — identical data shapes, differing only in `gc_grace_seconds` (0 vs
/// 864000) — must yield IDENTICAL surviving merge facts. CQLite does not surface
/// `gc_grace_seconds` from the on-disk components (it lives in the schema table,
/// not in Data.db/Statistics.db), and the two fixtures' Data.db JSONL goldens are
/// byte-identical per generation; this test asserts both: golden identity and
/// read-merge identity. Tombstones for cell and row are RETAINED/APPLIED (not
/// silently dropped) in both.
#[test]
fn gc_grace_read_merge_identical_and_tombstones_retained() {
    let Some((dir0, nb1_0, nb2_0)) = require_fixture("resurrection_gc0") else {
        return;
    };
    let Some((dir_pos, nb1_p, nb2_p)) = require_fixture("resurrection_gc_positive") else {
        return;
    };

    // Data.db JSONL goldens are identical per generation (gc_grace is not in Data.db).
    for gen in [1u32, 2u32] {
        let a = std::fs::read_to_string(dir0.join(format!("nb-{gen}-big-Data.db.jsonl"))).unwrap();
        let b =
            std::fs::read_to_string(dir_pos.join(format!("nb-{gen}-big-Data.db.jsonl"))).unwrap();
        assert_eq!(
            a, b,
            "gc0 vs gc_positive nb-{gen} Data.db JSONL goldens must be byte-identical (gc_grace \
             lives in the schema, not Data.db)"
        );
    }

    let facts0 = merge_facts(vec![nb2_0, nb1_0], &schema("resurrection_gc0"));
    let facts_pos = merge_facts(vec![nb2_p, nb1_p], &schema("resurrection_gc_positive"));
    assert!(
        !facts0.is_empty(),
        "gc0 read-merge produced 0 facts but goldens carry facts (would be a silent miss)"
    );
    assert_eq!(
        facts0, facts_pos,
        "gc_grace=0 and gc_grace=864000 must yield IDENTICAL surviving merge facts on READ \
         (purge is a compaction-time decision, never a read-time one)"
    );

    // never_purge: the row tombstone and cell tombstone are RETAINED/APPLIED in BOTH.
    let row_tomb = find(&facts0, 1, 2);
    assert_eq!(
        row_tomb.kind,
        Kind::RowTombstone,
        "row tombstone retained on read"
    );
    let cell = find(&facts0, 1, 3);
    assert!(
        matches!(&cell.kind, Kind::Live { val_dead: true, .. }),
        "cell tombstone retained/applied on read, got {:?}",
        cell.kind
    );

    eprintln!(
        "[gc_grace] gc0 and gc_positive: goldens byte-identical per gen; read-merge facts \
         identical; row + cell tombstones retained in both. Purge is compaction-only."
    );
}

// ════════════════════════════════════════════════════════════════════════════
// TEST 3 — Compaction merge safety: overlapping + partial sources
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cass.compaction_merge.resurrection_safety.overlapping_sources` and
/// `cass.compaction_merge.partial_source_retains_tombstones`.
///
/// (a) FULL compaction of BOTH generations (`purge_safe = true`): re-reading the
///     output must show the SAME row/cell shadowing as the read-merge — `pk=1 ck=2`
///     a row tombstone, `pk=1 ck=3` `val` a cell tombstone, survivors live. NO
///     row/cell resurrection.
/// (b) PARTIAL compaction of only the OLDER generation `nb-1` (`purge_safe =
///     false`): nb-1 carries no tombstones, so every nb-1 row is retained
///     unchanged — a partial compaction neither purges nor invents data.
#[test]
fn compaction_overlapping_and_partial_sources() {
    let Some((_dir, nb1, nb2)) = require_fixture("resurrection_gc0") else {
        return;
    };
    let sch = schema("resurrection_gc0");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // (a) FULL compaction of both generations.
    let out_full = tempfile::TempDir::new().unwrap();
    let report_full = rt
        .block_on(compact_sstables(
            vec![nb2.clone(), nb1.clone()],
            out_full.path(),
            &sch,
            101_4001,
            None,
            None,
            /* purge_safe */ true,
        ))
        .expect("full compaction");
    let full_facts = merge_facts(vec![report_full.output.data_path.clone()], &sch);

    let ck2 = find(&full_facts, 1, 2);
    assert_eq!(
        ck2.kind,
        Kind::RowTombstone,
        "compaction(both): pk=1 ck=2 must stay a row tombstone, got {:?}",
        ck2.kind
    );
    let ck3 = find(&full_facts, 1, 3);
    assert!(
        matches!(&ck3.kind, Kind::Live { val_dead: true, .. }),
        "compaction(both): pk=1 ck=3 `val` must stay a cell tombstone (no resurrection), got {:?}",
        ck3.kind
    );
    for (ck, expected) in [(1, "live_1"), (4, "live_4"), (5, "live_5")] {
        let f = find(&full_facts, 1, ck);
        assert!(
            matches!(&f.kind, Kind::Live { val: Some(v), val_dead: false } if v == expected),
            "compaction(both): pk=1 ck={ck} must survive live {expected:?}, got {:?}",
            f.kind
        );
    }
    // No resurrected row/cell: ck=2 is not live and ck=3 val is not live text.
    assert!(
        !full_facts.iter().any(|f| f.pk == 1
            && f.ck == 2
            && matches!(
                f.kind,
                Kind::Live {
                    val_dead: false,
                    ..
                }
            )),
        "compaction(both): pk=1 ck=2 row delete must NOT resurrect as a live row"
    );

    // (b) PARTIAL compaction of ONLY the older nb-1 (no tombstones in nb-1).
    let out_partial = tempfile::TempDir::new().unwrap();
    let report_partial = rt
        .block_on(compact_sstables(
            vec![nb1.clone()],
            out_partial.path(),
            &sch,
            101_4002,
            None,
            None,
            /* purge_safe */ false,
        ))
        .expect("partial compaction");
    let partial_facts = merge_facts(vec![report_partial.output.data_path.clone()], &sch);

    // nb-1 alone has all 8 live rows, none deleted: a partial compaction retains
    // them verbatim and invents nothing (and, having no tombstones, purges nothing).
    let expected_live: Vec<(i32, i32, &str)> = vec![
        (1, 1, "live_1"),
        (1, 2, "live_2"),
        (1, 3, "live_3"),
        (1, 4, "live_4"),
        (1, 5, "live_5"),
        (2, 1, "p2_live_1"),
        (2, 2, "p2_live_2"),
        (2, 3, "p2_live_3"),
    ];
    assert_eq!(
        partial_facts.len(),
        expected_live.len(),
        "partial compaction of nb-1 must retain exactly its 8 live rows, got {:?}",
        partial_facts
    );
    for (pk, ck, expected) in expected_live {
        let f = find(&partial_facts, pk, ck);
        assert!(
            matches!(&f.kind, Kind::Live { val: Some(v), val_dead: false } if v == expected),
            "partial(nb-1): pk={pk} ck={ck} must be retained live {expected:?}, got {:?}",
            f.kind
        );
    }

    eprintln!(
        "[compaction] full(both): ck=2 row-tombstone, ck=3 val cell-tombstone, survivors live \
         (0 row/cell resurrected). partial(nb-1): all 8 live rows retained, nothing purged or \
         invented."
    );
}

/// Manifest: `cass.compaction_merge.partial_source_retains_tombstones`.
///
/// A PARTIAL compaction that includes the tombstone-bearing newer generation
/// (`nb-2`) but NOT the older live data (`nb-1`) must RETAIN the tombstones in its
/// output, because purge is unsafe while a non-included overlapping SSTable
/// (`nb-1`) may still hold rows the tombstone shadows. We compact only `nb-2` with
/// `purge_safe = false` and `gc_before = i64::MAX` (every tombstone is gc-eligible)
/// and assert the row tombstone is STILL present in the output — proving the
/// overlap-safety gate (#921/#935/#1061) prevents a resurrection-causing purge.
#[test]
fn partial_compaction_retains_tombstones() {
    let Some((_dir, _nb1, nb2)) = require_fixture("resurrection_gc0") else {
        return;
    };
    let sch = schema("resurrection_gc0");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let out = tempfile::TempDir::new().unwrap();
    // Only nb-2 (tombstones). gc_before = MAX makes every tombstone gc-eligible;
    // purge_safe=false + no overlap bound means the partial compaction must STILL
    // retain them (conservative #921 default), or purging would resurrect nb-1 rows.
    let report = rt
        .block_on(compact_sstables(
            vec![nb2.clone()],
            out.path(),
            &sch,
            101_4003,
            Some(i64::MAX),
            None,
            /* purge_safe */ false,
        ))
        .expect("partial compaction of nb-2");
    let facts = merge_facts(vec![report.output.data_path.clone()], &sch);

    // The row tombstone for pk=1 ck=2 must survive in the partial output.
    let ck2 = facts.iter().find(|f| f.pk == 1 && f.ck == 2);
    assert!(
        matches!(ck2.map(|f| &f.kind), Some(Kind::RowTombstone)),
        "partial(nb-2): the pk=1 ck=2 row tombstone MUST be retained (purge is unsafe while the \
         non-included nb-1 still holds the older live row); got {ck2:?}"
    );
    // And ck=2 must NOT have been purged into nothing AND resurrected as live.
    assert!(
        !facts.iter().any(|f| f.pk == 1
            && f.ck == 2
            && matches!(
                f.kind,
                Kind::Live {
                    val_dead: false,
                    ..
                }
            )),
        "partial(nb-2): pk=1 ck=2 must not appear as a live row"
    );

    eprintln!(
        "[partial-retains] partial compaction of only nb-2 (gc_before=MAX, purge_safe=false) \
         RETAINS the ck=2 row tombstone — overlap-safety gate prevents a resurrection-causing \
         purge."
    );
}

// ════════════════════════════════════════════════════════════════════════════
// TEST 4 — repaired / unrepaired purge gate  (PARTIAL — gated on epic #968/#988)
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cass.tombstone_ttl.repaired_unrepaired_purge_gate`  → STATUS: PARTIAL.
///
/// Cassandra only purges tombstones once their SSTable's data is REPAIRED (or
/// `only_purge_repaired_tombstones` is satisfied), reconciling `repairedAt` across
/// the repaired/unrepaired SSTable sets. CQLite does NOT yet parse the `repairedAt`
/// field from `Statistics.db` (planned under epic #968 / issue #988), so it cannot
/// gate purge on repair status.
///
/// What is ASSERTED today (no over-claiming):
///   1. These fixtures are UNREPAIRED (`Repaired at: 0` in the Statistics.db.txt
///      sidecar), so a repaired-only purge would never fire for them anyway.
///   2. CQLite's compaction defaults to NOT purging (`purge_safe=false`), so it
///      does NOT claim purge for repaired data. A partial compaction retains the
///      tombstone (proven by [`partial_compaction_retains_tombstones`]).
///
/// NEXT STEP (scope.gap → #988): parse `repairedAt` / `pendingRepair` from
/// Statistics.db and add a repaired-set purge gate, then upgrade this to a full
/// repaired-vs-unrepaired purge assertion.
#[test]
fn repaired_unrepaired_purge_gate_partial() {
    let Some((dir, _nb1, _nb2)) = require_fixture("resurrection_gc0") else {
        return;
    };

    // Assert the fixtures are UNREPAIRED via the committed Statistics.db.txt sidecar
    // (a faithful dump of the Statistics.db this fixture ships). We assert the
    // surfaced fact, not a hardcoded purge outcome.
    let mut checked = 0usize;
    for gen in [1u32, 2u32] {
        let stats_txt = dir.join(format!("nb-{gen}-big-Statistics.db.txt"));
        let Ok(text) = std::fs::read_to_string(&stats_txt) else {
            continue;
        };
        let repaired_line = text
            .lines()
            .find(|l| l.trim_start().starts_with("Repaired at:"))
            .expect("Statistics.db.txt must record `Repaired at:`");
        assert!(
            repaired_line.contains("Repaired at: 0"),
            "fixture nb-{gen} is expected UNREPAIRED (`Repaired at: 0`); got {repaired_line:?}"
        );
        // pendingRepair is `--` (none) for these fixtures.
        let pending = text
            .lines()
            .find(|l| l.trim_start().starts_with("Pending repair:"))
            .unwrap_or("Pending repair: --");
        assert!(
            pending.contains("--"),
            "fixture nb-{gen} is expected to have no pending repair; got {pending:?}"
        );
        checked += 1;
    }
    assert!(
        checked > 0,
        "Statistics.db.txt sidecars carry repair facts but none were checked (golden drift?)"
    );

    eprintln!(
        "[repaired-gate PARTIAL #988] fixtures are UNREPAIRED (Repaired at: 0, Pending repair: --). \
         CQLite does not parse repairedAt yet (gated on epic #968/#988), and its compaction does \
         NOT claim purge for repaired data (defaults to purge_safe=false; partial compaction \
         retains tombstones). Next step: parse repairedAt + add a repaired-set purge gate."
    );
}

// ════════════════════════════════════════════════════════════════════════════
// PINNED GAP — partition-tombstone resurrection on the compaction-merge READ path
// ════════════════════════════════════════════════════════════════════════════

/// PINNED RESURRECTION GAP (NOT faked green) — `#[ignore]`d, mirroring the
/// `#[ignore]`d real-corruption regression in `issue_819_differential_compaction.rs`.
///
/// CQLite's compaction-merge read path
/// (`V5CompressedLegacy::parse_one_partition_for_compaction`) calls
/// `parse_partition_header`, which DROPS the partition-level deletion time. So the
/// newer PARTITION tombstone for `pk=2` in `nb-2` never reaches `KWayMerger`, and
/// the older live rows `pk=2 ck=1,2,3` from `nb-1` RESURRECT on the merge/compaction
/// path.
///
///   EXPECTED (resurrection-safe): pk=2 entirely absent (every ck shadowed).
///   ACTUAL   (today):             pk=2 ck=1,2,3 survive as live rows.
///
/// NOTE: the delta-scan READ path DOES surface partition tombstones
/// (`DeltaRecord::PartitionDelete`, see `scan_delta_parity_test.rs`); the gap is
/// specific to the compaction-merge read contract. Un-`ignore` this test once the
/// compaction read path surfaces partition deletions and the merge shadows them.
#[test]
fn partition_tombstone_resurrection_gap_pinned() {
    let Some((_dir, nb1, nb2)) = require_fixture("resurrection_gc0") else {
        return;
    };
    let sch = schema("resurrection_gc0");
    let facts = merge_facts(vec![nb2, nb1], &sch);

    let pk2_survivors: Vec<i32> = facts
        .iter()
        .filter(|f| {
            f.pk == 2
                && matches!(
                    f.kind,
                    Kind::Live {
                        val_dead: false,
                        ..
                    }
                )
        })
        .map(|f| f.ck)
        .collect();

    assert!(
        pk2_survivors.is_empty(),
        "RESURRECTION: partition tombstone for pk=2 (nb-2) must shadow ALL older pk=2 rows from \
         nb-1, but these live rows survived: ck={pk2_survivors:?} (expected []). Root cause: \
         parse_one_partition_for_compaction discards partition-level deletion."
    );
}
