//! Seeded stream generators for the reconcile laws (issue #4497). Reusable by #4257.
//!
//! A stream is a `Vec<`[`Spec`]`>`; [`build`] turns a spec into the real
//! [`MergeEntry`] the merger consumes. Specs are `Clone + Debug`, so a failing case
//! prints as a readable list (proptest shrinks it) instead of an opaque entry dump.
//!
//! Slots are deliberately tiny (3 clustering keys, 2 columns, timestamps 1..=6) so that
//! independent specs COLLIDE: equal-timestamp ties are the point of the exercise.

use proptest::prelude::*;

use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use crate::storage::write_engine::merge::{CellData, KWayMerger, MergeEntry, RowData};
use crate::storage::write_engine::mutation::{
    ClusteringBound, ClusteringKey, DecoratedKey, RangeTombstone,
};
use crate::types::{TombstoneInfo, TombstoneType, Value};

/// The pinned "now" (GC-clock seconds) used when TTL expiry is enabled.
pub(super) const NOW_SECS: i64 = 1000;
pub(super) const CKS: i32 = 3;
pub(super) const TS_MAX: i64 = 6;

/// How ambiguous the generated equal-timestamp ties are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Profile {
    /// Every equal-timestamp collision is RESOLVABLE by the rules CQLite implements:
    /// payload, expiry and LDT are pure functions of `(column, ts)`, so two cells at the
    /// same timestamp differ only by tombstone-vs-live (which both Cassandra and CQLite
    /// order identically).
    Clean,
    /// Payload, kind, expiry and LDT vary independently: equal-timestamp ties that
    /// Cassandra's `Cells.resolveRegular` orders by value / expiring-ness / LDT / TTL.
    Wild,
}

/// A live or expiring (TTL) cell write.
#[derive(Clone, Debug)]
pub(super) struct CellSpec {
    pub ck: i32,
    pub col: u8,
    pub ts: i64,
    /// Value discriminant (the payload is `"v{value}"`).
    pub value: u8,
    /// `Some((ttl, ldt))` for an expiring cell.
    pub expiring: Option<(u32, i32)>,
}

#[derive(Clone, Debug)]
pub(super) enum Spec {
    Cell(CellSpec),
    /// Cell tombstone: `(ck, col, ts, ldt)`.
    CellTomb {
        ck: i32,
        col: u8,
        ts: i64,
        ldt: i32,
    },
    /// Row deletion: `(ck, ts, ldt)`.
    RowDel {
        ck: i32,
        ts: i64,
        ldt: i32,
    },
    /// Range tombstone over `[lo, hi]` (inclusive): `(lo, hi, ts, ldt)`.
    RangeDel {
        lo: i32,
        hi: i32,
        ts: i64,
        ldt: i32,
    },
    /// Partition deletion: `(ts, ldt)`.
    PartDel {
        ts: i64,
        ldt: i32,
    },
}

pub(super) fn col_name(col: u8) -> String {
    format!("c{col}")
}

pub(super) fn schema() -> TableSchema {
    TableSchema {
        keyspace: "laws".to_string(),
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
            order: Default::default(),
        }],
        columns: (0..2u8)
            .map(|c| Column {
                name: col_name(c),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            })
            .collect(),
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// A merger over no inputs (the laws drive `merge_partition_rows` directly). `expire`
/// pins TTL expiry at [`NOW_SECS`]; GC purging stays OFF (purging legitimately breaks
/// associativity: a purged tombstone no longer shadows the third operand).
pub(super) fn merger(expire: bool) -> KWayMerger {
    let schema = schema();
    KWayMerger {
        runs: vec![],
        heap: std::collections::BinaryHeap::new(),
        current_partition: None,
        gc_before_secs: None,
        now_secs: expire.then_some(NOW_SECS),
        purge_safe: false,
        max_purgeable_timestamp: None,
        schema_arc: std::sync::Arc::new(schema.clone()),
        schema,
        _egress_slot: None,
    }
}

pub(super) fn pkey() -> DecoratedKey {
    DecoratedKey::new(100, vec![0, 0, 0, 1])
}

pub(super) fn ck(n: i32) -> ClusteringKey {
    ClusteringKey {
        columns: vec![("ck".to_string(), Value::Integer(n))],
    }
}

fn tomb_value(ts: i64, ldt: i32) -> Value {
    Value::Tombstone(Box::new(TombstoneInfo {
        deletion_time: ts,
        tombstone_type: TombstoneType::CellTombstone,
        local_deletion_time: i64::from(ldt),
        ttl: None,
        range_start: None,
        range_end: None,
    }))
}

/// Turn a spec into the real merge entry. EVERY entry gets `run_index` 0: the laws are
/// stated over CONTENT, the way Cassandra reconciles (`Cells.reconcile` never consults
/// file recency). CQLite's "newer file wins" tie-break would otherwise MASK an
/// order-dependence that the law is there to expose.
pub(super) fn build(spec: &Spec) -> MergeEntry {
    match spec {
        Spec::Cell(c) => {
            let mut cell =
                CellData::new(col_name(c.col), Value::text(format!("v{}", c.value)), c.ts);
            if let Some((ttl, ldt)) = c.expiring {
                cell.ttl = Some(ttl);
                cell.local_deletion_time = Some(ldt);
            }
            MergeEntry::new(
                0,
                pkey(),
                Some(ck(c.ck)),
                c.ts,
                RowData::Live { cells: vec![cell] },
            )
        }
        Spec::CellTomb {
            ck: k,
            col,
            ts,
            ldt,
        } => {
            let mut cell = CellData::new(col_name(*col), tomb_value(*ts, *ldt), *ts);
            cell.local_deletion_time = Some(*ldt);
            MergeEntry::new(
                0,
                pkey(),
                Some(ck(*k)),
                *ts,
                RowData::Live { cells: vec![cell] },
            )
        }
        Spec::RowDel { ck: k, ts, ldt } => MergeEntry::new(
            0,
            pkey(),
            Some(ck(*k)),
            *ts,
            RowData::Tombstone {
                deletion_time: *ts,
                local_deletion_time: *ldt,
            },
        ),
        Spec::RangeDel { lo, hi, ts, ldt } => {
            MergeEntry::new(0, pkey(), None, *ts, RowData::Live { cells: Vec::new() })
                .with_range_deletion(RangeTombstone {
                    start: ClusteringBound::Inclusive(ck(*lo)),
                    end: ClusteringBound::Inclusive(ck(*hi)),
                    deletion_time: *ts,
                    local_deletion_time: *ldt,
                })
        }
        Spec::PartDel { ts, ldt } => {
            MergeEntry::new(0, pkey(), None, *ts, RowData::Live { cells: Vec::new() })
                .with_partition_deletion((*ts, *ldt))
        }
    }
}

pub(super) fn build_all(specs: &[Spec]) -> Vec<MergeEntry> {
    specs.iter().map(build).collect()
}

// ─── strategies ────────────────────────────────────────────────────────────────

fn arb_ts() -> impl Strategy<Value = i64> {
    1..=TS_MAX
}

fn arb_ck() -> impl Strategy<Value = i32> {
    0..CKS
}

/// The `Clean` profile's deterministic expiry kind: a function of the timestamp only.
/// ts%3==1 -> expiring (ldt straddles `NOW_SECS` so both expired and live occur).
fn clean_expiring(ts: i64) -> Option<(u32, i32)> {
    (ts % 3 == 1).then(|| (10, (NOW_SECS - 4 + ts * 2) as i32))
}

fn clean_tomb_ldt(ts: i64) -> i32 {
    (500 + ts) as i32
}

pub(super) fn arb_spec(profile: Profile) -> BoxedStrategy<Spec> {
    let cell = match profile {
        Profile::Clean => (arb_ck(), 0u8..2, arb_ts())
            .prop_map(|(ck, col, ts)| {
                Spec::Cell(CellSpec {
                    ck,
                    col,
                    ts,
                    // value is a function of (col, ts)
                    value: (col as i64 * 10 + ts) as u8,
                    expiring: clean_expiring(ts),
                })
            })
            .boxed(),
        Profile::Wild => (
            arb_ck(),
            0u8..2,
            arb_ts(),
            0u8..3,
            prop::option::of((1u32..4, (NOW_SECS as i32 - 3)..(NOW_SECS as i32 + 4))),
        )
            .prop_map(|(ck, col, ts, value, expiring)| {
                Spec::Cell(CellSpec {
                    ck,
                    col,
                    ts,
                    value,
                    expiring,
                })
            })
            .boxed(),
    };
    let ldt = move |ts: i64, any: i32| match profile {
        Profile::Clean => clean_tomb_ldt(ts),
        Profile::Wild => any,
    };
    let cell_tomb = (arb_ck(), 0u8..2, arb_ts(), 400i32..406).prop_map(move |(ck, col, ts, l)| {
        Spec::CellTomb {
            ck,
            col,
            ts,
            ldt: ldt(ts, l),
        }
    });
    let row_del = (arb_ck(), arb_ts(), 400i32..406).prop_map(move |(ck, ts, l)| Spec::RowDel {
        ck,
        ts,
        ldt: ldt(ts, l),
    });
    let range_del =
        (arb_ck(), arb_ck(), arb_ts(), 400i32..406).prop_map(move |(a, b, ts, l)| Spec::RangeDel {
            lo: a.min(b),
            hi: a.max(b),
            ts,
            ldt: ldt(ts, l),
        });
    let part_del = (arb_ts(), 400i32..406).prop_map(move |(ts, l)| Spec::PartDel {
        ts,
        ldt: ldt(ts, l),
    });
    prop_oneof![
        8 => cell,
        3 => cell_tomb,
        2 => row_del,
        2 => range_del,
        1 => part_del,
    ]
    .boxed()
}

/// A stream of 0..=8 specs.
pub(super) fn arb_stream(profile: Profile) -> BoxedStrategy<Vec<Spec>> {
    prop::collection::vec(arb_spec(profile), 0..=8).boxed()
}
