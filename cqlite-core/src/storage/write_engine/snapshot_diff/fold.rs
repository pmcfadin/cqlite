//! The comparison fold (design.md §D1): two sides' reconciliation trails for ONE partition become
//! one [`PartitionDiff`]. No reconciliation happens here — every winner and every tombstone was
//! produced by the traced merger; this file only compares them (spec R2, R3, R4).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use super::resurrection::{gc_phase, shadows};
use super::side::SideTrail;
use super::types::{
    CellDiffEntry, CellSide, CellStatus, DiffError, DiffSummary, PartitionDiff, ResurrectionRisk,
    ShadowedCell, Side, TombstoneDiffEntry,
};
use crate::schema::TableSchema;
use crate::storage::write_engine::merge::trace::{
    CellDecision, TombstoneKind, TombstoneRecord, Verdict,
};
use crate::storage::write_engine::mutation::{ClusteringBound, ClusteringKey};
use crate::util::value_fmt::ValueFormatter;
use crate::Value;

pub(super) fn fmt_micros(micros: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)
        .map(|date| date.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        .unwrap_or_else(|| micros.to_string())
}

pub(super) fn fmt_secs(secs: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
        .map(|date| date.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| secs.to_string())
}

fn fmt_clustering(key: &ClusteringKey) -> String {
    key.columns
        .iter()
        .map(|(_, value)| ValueFormatter::format_value(value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn fmt_bound(bound: &ClusteringBound) -> String {
    match bound {
        ClusteringBound::Inclusive(key) => format!("inclusive:{}", fmt_clustering(key)),
        ClusteringBound::Exclusive(key) => format!("exclusive:{}", fmt_clustering(key)),
        ClusteringBound::Bottom => "bottom".to_string(),
        ClusteringBound::Top => "top".to_string(),
    }
}

fn kind_name(kind: TombstoneKind) -> &'static str {
    match kind {
        TombstoneKind::Partition => "partition",
        TombstoneKind::Range => "range",
        TombstoneKind::Row => "row",
        TombstoneKind::Cell => "cell",
        TombstoneKind::Collection => "collection",
    }
}

/// Total order on optional clustering keys: static (`None`) first, then schema order.
fn cmp_clustering(
    a: &Option<ClusteringKey>,
    b: &Option<ClusteringKey>,
    schema: &TableSchema,
) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => a.compare(b, schema).unwrap_or(Ordering::Equal),
    }
}

/// Identity of a cell for comparison: exact (JSON) clustering plus column. The trail does not
/// expose a collection element's path, so the elements of one non-frozen collection column share
/// one key and are compared as a multiset of `(value, writetime)`.
type CellKey = (String, String);

#[derive(Default)]
struct SideCells {
    cells: BTreeMap<CellKey, (Option<ClusteringKey>, Vec<(String, i64)>)>,
}

impl SideCells {
    fn from_trail(trail: &SideTrail) -> Self {
        let mut cells: BTreeMap<CellKey, (Option<ClusteringKey>, Vec<(String, i64)>)> =
            BTreeMap::new();
        for cell in trail
            .cells
            .iter()
            .filter(|cell| cell.verdict == Verdict::Winner)
        {
            let key = (
                serde_json::to_string(&cell.clustering).unwrap_or_default(),
                cell.column.clone(),
            );
            let rendered =
                ValueFormatter::format_value(cell.value.as_ref().unwrap_or(&Value::Null));
            cells
                .entry(key)
                .or_insert_with(|| (cell.clustering.clone(), Vec::new()))
                .1
                .push((rendered, cell.writetime));
        }
        for (_, versions) in cells.values_mut() {
            versions.sort();
        }
        Self { cells }
    }
}

fn side_view(versions: &[(String, i64)]) -> CellSide {
    let value = match versions {
        [(value, _)] => value.clone(),
        many => format!(
            "{{{}}}",
            many.iter()
                .map(|(value, _)| value.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    let writetime = versions.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
    CellSide {
        value,
        writetime: fmt_micros(writetime),
    }
}

fn multiset_minus(left: &[(String, i64)], right: &[(String, i64)]) -> Vec<(String, i64)> {
    let mut remaining: Vec<&(String, i64)> = right.iter().collect();
    let mut out = Vec::new();
    for item in left {
        if let Some(position) = remaining.iter().position(|candidate| *candidate == item) {
            remaining.swap_remove(position);
        } else {
            out.push(item.clone());
        }
    }
    out
}

/// Compare the two sides' winner sets (spec R2): one status per `(clustering, column)`.
fn fold_cells(
    partition: &str,
    a: &SideCells,
    b: &SideCells,
    schema: &TableSchema,
) -> std::result::Result<Vec<CellDiffEntry>, DiffError> {
    let keys: BTreeSet<&CellKey> = a.cells.keys().chain(b.cells.keys()).collect();
    let mut out: Vec<(Option<ClusteringKey>, CellKey, CellDiffEntry)> = Vec::new();
    for key in keys {
        let left = a.cells.get(key);
        let right = b.cells.get(key);
        let clustering = left
            .map(|(ck, _)| ck.clone())
            .or_else(|| right.map(|(ck, _)| ck.clone()))
            .unwrap_or(None);
        let (status, a_side, b_side) = match (left, right) {
            (Some((_, lv)), None) => (CellStatus::OnlyA, Some(side_view(lv)), None),
            (None, Some((_, rv))) => (CellStatus::OnlyB, None, Some(side_view(rv))),
            (Some((_, lv)), Some((_, rv))) => {
                let a_only = multiset_minus(lv, rv);
                let b_only = multiset_minus(rv, lv);
                let status = match (a_only.is_empty(), b_only.is_empty()) {
                    (true, true) => CellStatus::Equal,
                    (false, true) => CellStatus::OnlyA,
                    (true, false) => CellStatus::OnlyB,
                    (false, false) => {
                        let a_ts = a_only.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
                        let b_ts = b_only.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
                        match a_ts.cmp(&b_ts) {
                            Ordering::Greater => CellStatus::NewerInA,
                            Ordering::Less => CellStatus::NewerInB,
                            Ordering::Equal => {
                                return Err(DiffError::SameTimestampConflict {
                                    partition: partition.to_string(),
                                    clustering: clustering
                                        .as_ref()
                                        .map(fmt_clustering)
                                        .unwrap_or_else(|| "<static>".to_string()),
                                    column: key.1.clone(),
                                    writetime: fmt_micros(a_ts),
                                })
                            }
                        }
                    }
                };
                (status, Some(side_view(lv)), Some(side_view(rv)))
            }
            (None, None) => continue,
        };
        out.push((
            clustering.clone(),
            key.clone(),
            CellDiffEntry {
                clustering: clustering.as_ref().map(fmt_clustering),
                column: key.1.clone(),
                status,
                a: a_side,
                b: b_side,
            },
        ));
    }
    out.sort_by(|x, y| cmp_clustering(&x.0, &y.0, schema).then_with(|| x.1.cmp(&y.1)));
    Ok(out.into_iter().map(|(_, _, entry)| entry).collect())
}

/// Identity of a tombstone for the both-sides test (spec R3): kind, scope, deletion time and local
/// deletion time. A range tombstone's scope is its span; the `clustering` of the entry that
/// happened to carry it is not part of its identity. `run_index` and `droppable_at_now` are
/// per-side facts and are not part of it either.
fn tombstone_identity(tombstone: &TombstoneRecord) -> String {
    let clustering = if tombstone.kind == TombstoneKind::Range {
        None
    } else {
        tombstone.clustering.as_ref()
    };
    serde_json::to_string(&(
        kind_name(tombstone.kind),
        clustering,
        &tombstone.column,
        &tombstone.range_start,
        &tombstone.range_end,
        tombstone.deletion_time,
        tombstone.local_deletion_time,
    ))
    .unwrap_or_default()
}

fn distinct_tombstones(trail: &SideTrail) -> BTreeMap<String, &TombstoneRecord> {
    let mut out = BTreeMap::new();
    for tombstone in &trail.tombstones {
        out.entry(tombstone_identity(tombstone))
            .or_insert(tombstone);
    }
    out
}

fn winners(trail: &SideTrail) -> Vec<&CellDecision> {
    trail
        .cells
        .iter()
        .filter(|cell| cell.verdict == Verdict::Winner)
        .collect()
}

fn tombstone_entry(
    tombstone: &TombstoneRecord,
    holder: Side,
    other_winners: &[&CellDecision],
    gc_before: i64,
    schema: &TableSchema,
) -> crate::Result<TombstoneDiffEntry> {
    let mut shadowed: Vec<(Option<ClusteringKey>, ShadowedCell)> = Vec::new();
    for cell in other_winners {
        if shadows(tombstone, cell, schema)? {
            shadowed.push((
                cell.clustering.clone(),
                ShadowedCell {
                    clustering: cell.clustering.as_ref().map(fmt_clustering),
                    column: cell.column.clone(),
                    side: holder.other(),
                },
            ));
        }
    }
    shadowed.sort_by(|x, y| {
        cmp_clustering(&x.0, &y.0, schema).then_with(|| x.1.column.cmp(&y.1.column))
    });
    let mut shadowed_cells: Vec<ShadowedCell> =
        shadowed.into_iter().map(|(_, cell)| cell).collect();
    shadowed_cells.dedup();
    let resurrection_risk = if shadowed_cells.is_empty() {
        ResurrectionRisk {
            status: "no",
            phase: None,
            shadowed_cells,
        }
    } else {
        ResurrectionRisk {
            status: "yes",
            phase: Some(gc_phase(tombstone.local_deletion_time, gc_before)),
            shadowed_cells,
        }
    };
    let is_range = tombstone.kind == TombstoneKind::Range;
    Ok(TombstoneDiffEntry {
        kind: kind_name(tombstone.kind),
        clustering: if is_range {
            None
        } else {
            tombstone.clustering.as_ref().map(fmt_clustering)
        },
        column: tombstone.column.clone(),
        range_start: tombstone.range_start.as_ref().map(fmt_bound),
        range_end: tombstone.range_end.as_ref().map(fmt_bound),
        side: holder,
        deletion_time: fmt_micros(tombstone.deletion_time),
        local_deletion_time: fmt_secs(i64::from(tombstone.local_deletion_time as u32)),
        resurrection_risk,
    })
}

/// Compare the two sides' tombstones (spec R3) and classify each one-sided tombstone's
/// resurrection risk against the OTHER side's winners (spec R4 / R4A / R5).
fn fold_tombstones(
    a: &SideTrail,
    b: &SideTrail,
    gc_before: i64,
    schema: &TableSchema,
) -> crate::Result<Vec<TombstoneDiffEntry>> {
    let a_set = distinct_tombstones(a);
    let b_set = distinct_tombstones(b);
    let (a_winners, b_winners) = (winners(a), winners(b));
    let mut out: Vec<(&TombstoneRecord, TombstoneDiffEntry)> = Vec::new();
    for (identity, tombstone) in &a_set {
        if !b_set.contains_key(identity) {
            out.push((
                tombstone,
                tombstone_entry(tombstone, Side::A, &b_winners, gc_before, schema)?,
            ));
        }
    }
    for (identity, tombstone) in &b_set {
        if !a_set.contains_key(identity) {
            out.push((
                tombstone,
                tombstone_entry(tombstone, Side::B, &a_winners, gc_before, schema)?,
            ));
        }
    }
    // Deterministic, label-independent order (so a swapped diff lists them identically).
    out.sort_by(|(x, _), (y, _)| {
        kind_rank(x.kind)
            .cmp(&kind_rank(y.kind))
            .then_with(|| cmp_clustering(&x.clustering, &y.clustering, schema))
            .then_with(|| x.column.cmp(&y.column))
            .then_with(|| x.deletion_time.cmp(&y.deletion_time))
            .then_with(|| x.local_deletion_time.cmp(&y.local_deletion_time))
            .then_with(|| tombstone_identity(x).cmp(&tombstone_identity(y)))
    });
    Ok(out.into_iter().map(|(_, entry)| entry).collect())
}

fn kind_rank(kind: TombstoneKind) -> u8 {
    match kind {
        TombstoneKind::Partition => 0,
        TombstoneKind::Range => 1,
        TombstoneKind::Row => 2,
        TombstoneKind::Collection => 3,
        TombstoneKind::Cell => 4,
    }
}

/// Fold one partition's two trails into its [`PartitionDiff`].
pub(super) fn fold_partition(
    partition_key: String,
    a: &SideTrail,
    b: &SideTrail,
    gc_before: i64,
    schema: &TableSchema,
) -> std::result::Result<PartitionDiff, DiffError> {
    let cells = fold_cells(
        &partition_key,
        &SideCells::from_trail(a),
        &SideCells::from_trail(b),
        schema,
    )?;
    let tombstones = fold_tombstones(a, b, gc_before, schema)?;
    Ok(PartitionDiff {
        partition_key,
        cells,
        tombstones,
    })
}

impl DiffSummary {
    /// Count one folded partition into the running summary.
    pub(super) fn add(&mut self, partition: &PartitionDiff) {
        self.partitions_compared += 1;
        for cell in &partition.cells {
            match cell.status {
                CellStatus::Equal => self.equal += 1,
                CellStatus::OnlyA => self.only_a += 1,
                CellStatus::OnlyB => self.only_b += 1,
                CellStatus::NewerInA => self.newer_in_a += 1,
                CellStatus::NewerInB => self.newer_in_b += 1,
            }
        }
        for tombstone in &partition.tombstones {
            match tombstone.side {
                Side::A => self.tombstones_only_a += 1,
                Side::B => self.tombstones_only_b += 1,
            }
            match tombstone.resurrection_risk.phase {
                Some("before-gc-grace") => self.resurrection_risk_before_gc_grace += 1,
                Some("past-gc-grace") => self.resurrection_risk_past_gc_grace += 1,
                _ => {}
            }
        }
    }
}
