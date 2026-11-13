//! Issue #4530 — equal-timestamp reconcile ties resolve per CASSANDRA, in both input orders.
//!
//! Subject: the real `KWayMerger::merge_partition_rows`. Oracle: Cassandra 5.0.8
//! `Cells.resolveRegular` (`db/rows/Cells.java:79-129`) and `DeletionTime.supersedes`
//! (`db/DeletionTime.java:158`), re-stated independently below as a reference key — the tests
//! assert the NAMED winner, not merely that the two orders agree.
//!
//! The grid is exhaustive over a small palette (values, TTLs, LDTs) so every tie class is hit
//! with every relative ordering of the tie-break keys. Every pair is merged in BOTH orders.

use std::cmp::Reverse;

use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use crate::storage::write_engine::merge::{CellData, KWayMerger, MergeEntry, RowData};
use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey};
use crate::types::{TombstoneInfo, TombstoneType, Value};

const TS: i64 = 3;

fn merger() -> KWayMerger {
    let schema = TableSchema {
        keyspace: "t4530".into(),
        table: "t".into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![Column {
            name: "c0".into(),
            data_type: "text".into(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: Default::default(),
        dropped_columns: Default::default(),
    };
    KWayMerger {
        runs: vec![],
        heap: std::collections::BinaryHeap::new(),
        current_partition: None,
        gc_before_secs: None,
        now_secs: None,
        purge_safe: false,
        max_purgeable_timestamp: None,
        schema_arc: std::sync::Arc::new(schema.clone()),
        schema,
        _egress_slot: None,
    }
}

fn pkey() -> DecoratedKey {
    DecoratedKey::new(100, vec![0, 0, 0, 1])
}

fn ck() -> ClusteringKey {
    ClusteringKey {
        columns: vec![("ck".into(), Value::Integer(0))],
    }
}

/// What a contender looks like to Cassandra's `resolveRegular`.
#[derive(Clone, Copy, Debug)]
enum C {
    Live { value: u8 },
    Expiring { value: u8, ttl: u32, ldt: i32 },
    Tomb { ldt: i32 },
}

impl C {
    fn entry(self) -> MergeEntry {
        let cell = match self {
            C::Live { value } => CellData::new("c0".into(), Value::text(format!("v{value}")), TS),
            C::Expiring { value, ttl, ldt } => {
                let mut c = CellData::new("c0".into(), Value::text(format!("v{value}")), TS);
                c.ttl = Some(ttl);
                c.local_deletion_time = Some(ldt);
                c
            }
            C::Tomb { ldt } => {
                let mut c = CellData::new(
                    "c0".into(),
                    Value::Tombstone(Box::new(TombstoneInfo {
                        deletion_time: TS,
                        tombstone_type: TombstoneType::CellTombstone,
                        local_deletion_time: i64::from(ldt),
                        ttl: None,
                        range_start: None,
                        range_end: None,
                    })),
                    TS,
                );
                c.local_deletion_time = Some(ldt);
                c
            }
        };
        MergeEntry::new(
            0,
            pkey(),
            Some(ck()),
            TS,
            RowData::Live { cells: vec![cell] },
        )
    }

    /// `Cells.resolveRegular` as a sort key: larger wins. (1) expiring-or-tombstone beats live;
    /// (2) tombstone beats expiring; (3) larger localDeletionTime; (4) LOWER ttl; (5) larger value.
    fn key(self) -> (bool, bool, i32, Reverse<u32>, u8) {
        match self {
            C::Live { value } => (false, false, 0, Reverse(0), value),
            C::Expiring { value, ttl, ldt } => (true, false, ldt, Reverse(ttl), value),
            C::Tomb { ldt } => (true, true, ldt, Reverse(0), 0),
        }
    }
}

fn merge(rows: Vec<MergeEntry>) -> Vec<MergeEntry> {
    merger()
        .merge_partition_rows(rows)
        .expect("merge must not fail")
}

/// The single surviving cell of a one-row merge.
fn survivor(out: &[MergeEntry]) -> CellData {
    assert_eq!(out.len(), 1, "one clustering row expected: {out:?}");
    match &out[0].row_data {
        RowData::Live { cells } => {
            assert_eq!(cells.len(), 1, "one cell expected: {cells:?}");
            cells[0].clone()
        }
        other => panic!("expected a live row, got {other:?}"),
    }
}

fn describe(cell: &CellData) -> C {
    match &cell.value {
        Value::Tombstone(_) => C::Tomb {
            ldt: cell.local_deletion_time.expect("tombstone ldt"),
        },
        v => {
            let value = match v {
                Value::Text(b) => b[1] - b'0',
                other => panic!("unexpected value {other:?}"),
            };
            match cell.ttl {
                Some(ttl) => C::Expiring {
                    value,
                    ttl,
                    ldt: cell.local_deletion_time.expect("ldt"),
                },
                None => C::Live { value },
            }
        }
    }
}

/// Merge `a`,`b` in both orders; the survivor must be the Cassandra-named winner both times.
fn assert_cell_winner(a: C, b: C) {
    let want = if a.key() >= b.key() { a } else { b };
    for (x, y) in [(a, b), (b, a)] {
        let got = describe(&survivor(&merge(vec![x.entry(), y.entry()])));
        assert_eq!(
            got.key(),
            want.key(),
            "{a:?} vs {b:?} (order {x:?},{y:?}): Cassandra picks {want:?}, CQLite kept {got:?}"
        );
    }
}

#[test]
fn live_vs_live_larger_value_wins() {
    for v1 in 0..4 {
        for v2 in 0..4 {
            assert_cell_winner(C::Live { value: v1 }, C::Live { value: v2 });
        }
    }
}

#[test]
fn expiring_beats_live_whatever_the_values() {
    for v1 in 0..3 {
        for v2 in 0..3 {
            assert_cell_winner(
                C::Expiring {
                    value: v1,
                    ttl: 5,
                    ldt: 1000,
                },
                C::Live { value: v2 },
            );
        }
    }
}

#[test]
fn expiring_vs_expiring_larger_ldt_then_lower_ttl_then_larger_value() {
    for (t1, l1, v1) in grid3() {
        for (t2, l2, v2) in grid3() {
            assert_cell_winner(
                C::Expiring {
                    value: v1,
                    ttl: t1,
                    ldt: l1,
                },
                C::Expiring {
                    value: v2,
                    ttl: t2,
                    ldt: l2,
                },
            );
        }
    }
}

/// `(ttl, ldt, value)` palette: two TTLs, two LDTs, two values.
fn grid3() -> Vec<(u32, i32, u8)> {
    let mut v = Vec::new();
    for t in [1u32, 3] {
        for l in [1000, 1001] {
            for val in [0u8, 1] {
                v.push((t, l, val));
            }
        }
    }
    v
}

#[test]
fn tombstone_beats_expiring_and_live() {
    for ldt in [400, 406] {
        for el in [399, 400, 407] {
            assert_cell_winner(
                C::Tomb { ldt },
                C::Expiring {
                    value: 1,
                    ttl: 2,
                    ldt: el,
                },
            );
            assert_cell_winner(C::Tomb { ldt }, C::Live { value: 2 });
        }
    }
}

#[test]
fn tombstone_vs_tombstone_larger_ldt_wins() {
    for l1 in 400..404 {
        for l2 in 400..404 {
            assert_cell_winner(C::Tomb { ldt: l1 }, C::Tomb { ldt: l2 });
        }
    }
}

fn row_del(ldt: i32) -> MergeEntry {
    MergeEntry::new(
        0,
        pkey(),
        Some(ck()),
        TS,
        RowData::Tombstone {
            deletion_time: TS,
            local_deletion_time: ldt,
        },
    )
}

#[test]
fn row_deletion_vs_row_deletion_larger_ldt_wins() {
    for l1 in 400..404 {
        for l2 in 400..404 {
            let want = l1.max(l2);
            for (x, y) in [(l1, l2), (l2, l1)] {
                let out = merge(vec![row_del(x), row_del(y)]);
                assert_eq!(out.len(), 1);
                match &out[0].row_data {
                    RowData::Tombstone {
                        local_deletion_time,
                        ..
                    } => assert_eq!(
                        *local_deletion_time, want,
                        "row deletions ldt {l1}/{l2} merged as ({x},{y})"
                    ),
                    other => panic!("expected a row tombstone, got {other:?}"),
                }
            }
        }
    }
}

fn part_del(ldt: i32) -> MergeEntry {
    MergeEntry::new(0, pkey(), None, TS, RowData::Live { cells: Vec::new() })
        .with_partition_deletion((TS, ldt))
}

#[test]
fn partition_deletion_vs_partition_deletion_larger_ldt_wins() {
    for l1 in 400..404 {
        for l2 in 400..404 {
            let want = l1.max(l2);
            for (x, y) in [(l1, l2), (l2, l1)] {
                let out = merge(vec![part_del(x), part_del(y)]);
                let pd: Vec<_> = out.iter().filter_map(|e| e.partition_deletion).collect();
                assert_eq!(
                    pd,
                    vec![(TS, want)],
                    "partition deletions ldt {l1}/{l2} merged as ({x},{y})"
                );
            }
        }
    }
}
