//! Cell comparison (spec R2, R2A, R2B; owner Q21=b 2026-11-21).
//!
//! Each cell is keyed `(clustering, column, cell_path)`: a non-frozen collection element is its own
//! cell, exactly as in Cassandra. Values are compared on their serialized bytes (frozen values and
//! element values alike), never on formatted text.
//!
//! When the two sides hold the same key at the SAME write timestamp with differing content, the
//! status is `conflict` and `resolves_to` is the side Cassandra keeps. That decision is made by the
//! ONE implementation of `Cells.resolveRegular` (cassandra-5.0.8 `db/rows/Cells.java:79-128`,
//! `reconcile_rules::resolve_cell`): higher timestamp; at equal timestamp a tombstone or an
//! expiring cell beats a live one, a tombstone beats an expiring cell, the larger
//! `localDeletionTime` wins, then the lower TTL, then the larger serialized value
//! (`ValueAccessor.compare`, unsigned lexicographic).

use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::fold::{cmp_clustering, fmt_clustering, fmt_micros};
use super::side::SideTrail;
use super::types::{CellDiffEntry, CellSide, CellStatus, Side};
use crate::schema::TableSchema;
use crate::storage::sstable::writer::data_writer::encoding::serialize_value_into;
use crate::storage::write_engine::merge::trace::{TombstoneKind, Verdict};
use crate::storage::write_engine::mutation::ClusteringKey;
use crate::storage::write_engine::reconcile_rules::{resolve_cell, ReconcileCell};
use crate::util::value_fmt::ValueFormatter;
use crate::Value;

/// Exact (`Debug`) clustering, column and element path: the identity of one cell.
type CellKey = (String, String, Option<Vec<u8>>);

/// One side's version of a cell: a reconciled live winner, or a cell tombstone.
struct Version {
    clustering: Option<ClusteringKey>,
    timestamp: i64,
    tombstone: bool,
    ttl: Option<u32>,
    local_deletion_time: i32,
    bytes: Vec<u8>,
    display: String,
}

impl ReconcileCell for Version {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
    fn is_tombstone(&self) -> bool {
        self.tombstone
    }
    fn expiring_ttl(&self) -> Option<u32> {
        self.ttl
    }
    fn local_deletion_time(&self) -> i32 {
        self.local_deletion_time
    }
    fn value_bytes(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

impl Version {
    /// Same cell content: timestamp, tombstone-ness, expiry and serialized value bytes.
    fn same_as(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
            && self.tombstone == other.tombstone
            && self.ttl == other.ttl
            && self.local_deletion_time == other.local_deletion_time
            && self.bytes == other.bytes
    }

    fn view(&self) -> CellSide {
        CellSide {
            value: self.display.clone(),
            writetime: fmt_micros(self.timestamp),
        }
    }
}

#[derive(Default)]
struct SideCells {
    winners: BTreeMap<CellKey, Version>,
    /// Cell tombstones at keys where the side holds NO live winner.
    tombstones: BTreeMap<CellKey, Version>,
}

fn key_of(clustering: &Option<ClusteringKey>, column: &str, path: &Option<Vec<u8>>) -> CellKey {
    (format!("{clustering:?}"), column.to_string(), path.clone())
}

impl SideCells {
    fn from_trail(trail: &SideTrail) -> Self {
        let mut out = Self::default();
        for cell in trail
            .cells
            .iter()
            .filter(|cell| cell.verdict == Verdict::Winner)
        {
            let value = cell.value.as_ref().unwrap_or(&Value::Null);
            let mut bytes = Vec::new();
            // A value the writer cannot serialize is compared as empty bytes; it still differs
            // from a serializable one by timestamp/expiry, and never silently equals it unless the
            // bytes truly match.
            let _ = serialize_value_into(value, &mut bytes);
            out.winners.insert(
                key_of(&cell.clustering, &cell.column, &cell.cell_path),
                Version {
                    clustering: cell.clustering.clone(),
                    timestamp: cell.writetime,
                    tombstone: false,
                    ttl: cell
                        .ttl
                        .and_then(|t| u32::try_from(t).ok())
                        .filter(|t| *t != 0),
                    local_deletion_time: cell.expires_at.map_or(0, |e| e as u32 as i32),
                    bytes,
                    display: ValueFormatter::format_value(value),
                },
            );
        }
        for tombstone in trail
            .tombstones
            .iter()
            .filter(|t| t.kind == TombstoneKind::Cell)
        {
            let Some(column) = tombstone.column.as_deref() else {
                continue;
            };
            let key = key_of(&tombstone.clustering, column, &tombstone.cell_path);
            if out.winners.contains_key(&key) {
                continue;
            }
            out.tombstones.entry(key).or_insert(Version {
                clustering: tombstone.clustering.clone(),
                timestamp: tombstone.deletion_time,
                tombstone: true,
                ttl: None,
                local_deletion_time: tombstone.local_deletion_time,
                bytes: Vec::new(),
                display: "<tombstone>".to_string(),
            });
        }
        out
    }

    /// This side's version of `key` for comparison against `other_ts`: its live winner, or — only
    /// when it is at the SAME timestamp (the equal-timestamp tie `Cells.resolveRegular` decides) —
    /// its cell tombstone.
    fn at(&self, key: &CellKey, other_ts: i64) -> Option<&Version> {
        self.winners.get(key).or_else(|| {
            self.tombstones
                .get(key)
                .filter(|tombstone| tombstone.timestamp == other_ts)
        })
    }
}

fn entry(
    key: &CellKey,
    version: &Version,
    status: CellStatus,
    a: Option<&Version>,
    b: Option<&Version>,
    resolves_to: Option<Side>,
) -> (Option<ClusteringKey>, CellKey, CellDiffEntry) {
    (
        version.clustering.clone(),
        key.clone(),
        CellDiffEntry {
            clustering: version.clustering.as_ref().map(fmt_clustering),
            column: key.1.clone(),
            cell_path: key.2.as_ref().map(hex::encode),
            status,
            resolves_to,
            a: a.map(Version::view),
            b: b.map(Version::view),
        },
    )
}

/// Compare the two sides' winner sets: one status per `(clustering, column, cell_path)`.
pub(super) fn fold_cells(
    _partition: &str,
    a: &SideTrail,
    b: &SideTrail,
    schema: &TableSchema,
) -> crate::Result<Vec<CellDiffEntry>> {
    let (a, b) = (SideCells::from_trail(a), SideCells::from_trail(b));
    let keys: std::collections::BTreeSet<&CellKey> =
        a.winners.keys().chain(b.winners.keys()).collect();
    let mut out = Vec::new();
    for key in keys {
        let (left, right) = match (a.winners.get(key), b.winners.get(key)) {
            (Some(l), Some(r)) => (Some(l), Some(r)),
            (Some(l), None) => (Some(l), b.at(key, l.timestamp)),
            (None, Some(r)) => (a.at(key, r.timestamp), Some(r)),
            (None, None) => continue,
        };
        out.push(match (left, right) {
            (Some(l), Some(r)) => {
                let (status, resolves_to) = if l.same_as(r) {
                    (CellStatus::Equal, None)
                } else {
                    match l.timestamp.cmp(&r.timestamp) {
                        Ordering::Greater => (CellStatus::NewerInA, None),
                        Ordering::Less => (CellStatus::NewerInB, None),
                        Ordering::Equal => {
                            let keeps = if resolve_cell(l, r).0 {
                                Side::A
                            } else {
                                Side::B
                            };
                            (CellStatus::Conflict, Some(keeps))
                        }
                    }
                };
                entry(key, l, status, Some(l), Some(r), resolves_to)
            }
            (Some(l), None) => entry(key, l, CellStatus::OnlyA, Some(l), None, None),
            (None, Some(r)) => entry(key, r, CellStatus::OnlyB, None, Some(r), None),
            (None, None) => continue,
        });
    }
    out.sort_by(|x, y| {
        cmp_clustering(&x.0, &y.0, schema)
            .then_with(|| x.1 .1.cmp(&y.1 .1))
            .then_with(|| x.1.cmp(&y.1))
    });
    Ok(out.into_iter().map(|(_, _, entry)| entry).collect())
}
