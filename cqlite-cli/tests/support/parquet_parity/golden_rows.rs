//! sstabledump JSONL golden → canonical rows, for the Parquet parity harness (#1490).
//!
//! # The oracle, and its documented scope
//!
//! The golden is Cassandra-written (`sstabledump` output committed as
//! `*-Data.db.jsonl`), which is what makes it a legitimate oracle for CQLite's
//! Parquet export: a CQLite-written/CQLite-read round-trip is invariant to a
//! uniform encoding error and cannot detect one (#3042).
//!
//! But a physical dump is NOT a reconciled result set (#1742): it enumerates
//! every cell on disk INCLUDING tombstones, deleted rows and TTL-expired rows,
//! while `SELECT *` (and therefore the Parquet export) returns the
//! post-reconciliation view. Comparing the two is only sound for a fixture whose
//! physical content and reconciled content coincide. So this module FAILS
//! CLOSED — with a message naming the construct — on any golden carrying:
//!
//!   * a partition-level or row-level deletion,
//!   * a range tombstone bound/boundary,
//!   * a `static_block` (static columns are not per-row values),
//!   * any TTL (row liveness or per-cell), which can expire between fixture
//!     generation and test time,
//!   * a per-cell tombstone.
//!
//! A fixture that grows one of those turns RED here instead of silently
//! comparing the wrong things. Reconciliation-sensitive tables belong to the
//! query-semantics oracle (`test-data/query-semantics-oracle.json`), not to a
//! physical-dump lane.
//!
//! # Two normalizations, both of them equalities rather than tolerances
//!
//! * **`Null` folds into `Absent`, recursively.** Arrow/Parquet has ONE null;
//!   sstabledump renders an absent cell by omitting it and a null UDT field as
//!   an explicit JSON `null`. Both denote the same absence in CQL, so the golden
//!   side folds `Null` into `Absent` at every depth. (This cannot mask an
//!   empty-vs-null bug: an empty text is `Text("")`, an empty collection is an
//!   empty container, and neither is `Absent`.)
//! * **A multicell collection with no live elements is `Absent`.** Cassandra
//!   reads a non-frozen collection whose elements are all gone as NULL, so an
//!   export writing NULL there is correct. Applied ONLY to non-frozen
//!   collections: a `frozen<set<int>>` really can hold an empty set, which stays
//!   an empty container and so still differs from NULL.

#![allow(dead_code)]

use std::collections::BTreeMap;

use super::canonical_jsonl::{
    CanonicalDocument, CanonicalRow, CanonicalValue, KeyKind, NormalizedFloat,
};
use super::cql_type::{ColumnType, CqlTypeSpec, SeqKind};

/// One row projected out of the golden: the primary-key components in declared
/// order, plus every declared column's canonical value.
pub struct GoldenRow {
    pub keys: Vec<CanonicalValue>,
    pub cells: BTreeMap<String, CanonicalValue>,
}

/// Project a whole golden document into canonical rows.
///
/// `key_columns` names the partition-key columns followed by the clustering
/// columns, in declared order — the same order the golden's `partition.key` and
/// `row.clustering` arrays use.
pub fn project_golden(
    doc: &CanonicalDocument,
    columns: &[ColumnType],
    partition_key: &[&str],
    clustering: &[&str],
) -> Result<Vec<GoldenRow>, String> {
    let mut out = Vec::new();
    for (pi, part) in doc.partitions.iter().enumerate() {
        if part.deletion.is_some() {
            return Err(ineligible(pi, "a partition-level deletion"));
        }
        if !part.range_bounds.is_empty() {
            return Err(ineligible(pi, "a range tombstone bound/boundary"));
        }
        if part.key.len() != partition_key.len() {
            return Err(format!(
                "partition {pi}: golden carries {} key component(s) but the case declares {} \
                 partition-key column(s) {partition_key:?}",
                part.key.len(),
                partition_key.len()
            ));
        }
        for (ri, row) in part.rows.iter().enumerate() {
            let where_ = format!("partition {pi} row {ri}");
            if row.row_type != "row" {
                return Err(ineligible_at(
                    &where_,
                    &format!("a '{}' entry", row.row_type),
                ));
            }
            if row.deletion.is_some() {
                return Err(ineligible_at(&where_, "a row-level deletion"));
            }
            if row.liveness.as_ref().and_then(|l| l.ttl_secs).is_some() {
                return Err(ineligible_at(&where_, "a row TTL"));
            }
            if row.clustering.len() != clustering.len() {
                return Err(format!(
                    "{where_}: golden carries {} clustering component(s) but the case declares \
                     {} clustering column(s) {clustering:?}",
                    row.clustering.len(),
                    clustering.len()
                ));
            }

            let mut keys: Vec<CanonicalValue> =
                Vec::with_capacity(part.key.len() + row.clustering.len());
            keys.extend(part.key.iter().cloned().map(fold_null));
            keys.extend(row.clustering.iter().cloned().map(fold_null));

            let mut cells: BTreeMap<String, CanonicalValue> = BTreeMap::new();
            // Key columns carry their value in the key arrays, not in `cells`.
            for (name, value) in partition_key
                .iter()
                .chain(clustering.iter())
                .zip(keys.iter())
            {
                cells.insert((*name).to_string(), value.clone());
            }

            reject_undeclared_cells(&where_, row, columns, partition_key, clustering)?;

            for col in columns {
                if partition_key.contains(&col.name.as_str())
                    || clustering.contains(&col.name.as_str())
                {
                    continue;
                }
                let value = project_column(&where_, row, col)?;
                cells.insert(col.name.clone(), narrow_floats(value, &col.spec));
            }

            // Every declared key column must have been filled by the key arrays.
            for name in partition_key.iter().chain(clustering.iter()) {
                let v = cells.get(*name).ok_or_else(|| {
                    format!("{where_}: declared key column '{name}' is not in the golden row")
                })?;
                if let Some(col) = columns.iter().find(|c| c.name == *name) {
                    let narrowed = narrow_floats(v.clone(), &col.spec);
                    cells.insert((*name).to_string(), narrowed);
                }
            }

            out.push(GoldenRow { keys, cells });
        }
    }
    Ok(out)
}

fn ineligible(pi: usize, what: &str) -> String {
    ineligible_at(&format!("partition {pi}"), what)
}

fn ineligible_at(where_: &str, what: &str) -> String {
    format!(
        "{where_} carries {what}: this fixture is NOT eligible for physical-dump \
         parity (a Parquet export is the RECONCILED result set, a JSONL dump is not — \
         issue #1742). Use the query-semantics oracle for it, or drop the case."
    )
}

/// A cell naming a column the case does not declare means the case's column list
/// has drifted from the schema the fixture was written with — never something to
/// skip past.
fn reject_undeclared_cells(
    where_: &str,
    row: &CanonicalRow,
    columns: &[ColumnType],
    partition_key: &[&str],
    clustering: &[&str],
) -> Result<(), String> {
    for cell in &row.cells {
        let declared = columns.iter().any(|c| c.name == cell.name)
            || partition_key.contains(&cell.name.as_str())
            || clustering.contains(&cell.name.as_str());
        if !declared {
            return Err(format!(
                "{where_}: golden carries a cell for column '{}' which the case does not \
                 declare — reconcile the case's column list with the fixture's CQL schema",
                cell.name
            ));
        }
    }
    Ok(())
}

/// Project ONE column of ONE golden row.
fn project_column(
    where_: &str,
    row: &CanonicalRow,
    col: &ColumnType,
) -> Result<CanonicalValue, String> {
    let mine: Vec<_> = row.cells.iter().filter(|c| c.name == col.name).collect();
    if mine.is_empty() {
        return Ok(CanonicalValue::Absent);
    }

    // A complex (collection-shell) deletion is the normal marker an INSERT of a
    // whole non-frozen collection writes; it shadows every element at or below
    // its timestamp.
    let mut shell_deleted_at: Option<i64> = None;
    for cell in &mine {
        if cell.path.is_empty() {
            if let Some(del) = &cell.deletion {
                if !col.is_multicell_collection() {
                    return Err(ineligible_at(
                        where_,
                        &format!("a cell tombstone on scalar column '{}'", col.name),
                    ));
                }
                let at = del.marked_deleted_micros.ok_or_else(|| {
                    format!(
                        "{where_}: column '{}' has a collection-shell deletion with no \
                         markedForDeleteAt timestamp",
                        col.name
                    )
                })?;
                shell_deleted_at = Some(shell_deleted_at.map_or(at, |cur: i64| cur.max(at)));
            }
        }
        if cell.ttl_secs.is_some() {
            return Err(ineligible_at(
                where_,
                &format!("a TTL on column '{}'", col.name),
            ));
        }
    }

    let row_writetime = row.liveness.as_ref().and_then(|l| l.tstamp_micros);
    let live = |cell: &&&super::canonical_jsonl::CanonicalCell| -> Result<bool, String> {
        let Some(deleted_at) = shell_deleted_at else {
            return Ok(true);
        };
        let wt = cell.writetime_micros.or(row_writetime).ok_or_else(|| {
            format!(
                "{where_}: column '{}' element has neither a cell writetime nor a row \
                 liveness timestamp, so shadowing by the collection-shell deletion cannot \
                 be decided",
                col.name
            )
        })?;
        Ok(wt > deleted_at)
    };

    if col.is_multicell_collection() {
        let (kind_key, value_kind) = match &col.spec {
            CqlTypeSpec::Seq {
                kind: SeqKind::Set,
                elem,
            } => (Some(elem.as_ref()), None),
            CqlTypeSpec::Seq {
                kind: SeqKind::List,
                elem,
            } => (None, Some(elem.as_ref())),
            CqlTypeSpec::Map { key, .. } => (Some(key.as_ref()), None),
            other => {
                return Err(format!(
                    "{where_}: column '{}' is multicell but its parsed type {other:?} is not a \
                     collection",
                    col.name
                ))
            }
        };
        let _ = value_kind;

        let mut list_items: Vec<CanonicalValue> = Vec::new();
        let mut map_items: Vec<(CanonicalValue, CanonicalValue)> = Vec::new();
        for cell in mine.iter().filter(|c| !c.path.is_empty()) {
            if cell.deletion.is_some() {
                return Err(ineligible_at(
                    where_,
                    &format!("a cell tombstone inside collection '{}'", col.name),
                ));
            }
            if !live(&cell)? {
                continue;
            }
            if cell.path.len() != 1 {
                return Err(format!(
                    "{where_}: column '{}' element has {} path components; the harness handles \
                     single-component collection paths only",
                    col.name,
                    cell.path.len()
                ));
            }
            match &col.spec {
                CqlTypeSpec::Seq {
                    kind: SeqKind::Set, ..
                } => list_items.push(coerce_path(&cell.path[0], kind_key)),
                CqlTypeSpec::Seq {
                    kind: SeqKind::List,
                    ..
                } => list_items.push(fold_null(cell.value.clone())),
                CqlTypeSpec::Map { .. } => map_items.push((
                    coerce_path(&cell.path[0], kind_key),
                    fold_null(cell.value.clone()),
                )),
                _ => unreachable!("guarded by the match above"),
            }
        }
        return Ok(match &col.spec {
            CqlTypeSpec::Map { .. } => {
                if map_items.is_empty() {
                    CanonicalValue::Absent
                } else {
                    CanonicalValue::Map(map_items)
                }
            }
            _ => {
                if list_items.is_empty() {
                    CanonicalValue::Absent
                } else {
                    CanonicalValue::List(list_items)
                }
            }
        });
    }

    // Scalar / frozen column: exactly one value-bearing cell.
    let value_cells: Vec<_> = mine.iter().filter(|c| c.path.is_empty()).collect();
    if value_cells.len() != 1 {
        return Err(format!(
            "{where_}: column '{}' is declared '{}' (single-cell) but the golden carries {} \
             cells for it — the declared type disagrees with the fixture",
            col.name,
            col.declared,
            value_cells.len()
        ));
    }
    Ok(fold_null(value_cells[0].value.clone()))
}

/// A collection path component arrives STRINGIFIED (`"-2"` for a `set<int>`
/// element). Coerce it back using the DECLARED element/key type, exactly as
/// `canonical_jsonl` coerces partition-key components.
fn coerce_path(raw: &CanonicalValue, elem: Option<&CqlTypeSpec>) -> CanonicalValue {
    let Some(CqlTypeSpec::Scalar(name)) = elem else {
        return fold_null(raw.clone());
    };
    if KeyKind::from_cql_type(name) != KeyKind::Integral {
        return fold_null(raw.clone());
    }
    match raw {
        CanonicalValue::Text(s) => match s.parse::<i128>() {
            Ok(i) => CanonicalValue::Int(i),
            Err(_) => CanonicalValue::Text(s.clone()),
        },
        other => fold_null(other.clone()),
    }
}

/// Recursively fold an explicit JSON `null` into `Absent` — Arrow has only one
/// null, and CQL does not distinguish the two.
pub fn fold_null(v: CanonicalValue) -> CanonicalValue {
    match v {
        CanonicalValue::Null => CanonicalValue::Absent,
        CanonicalValue::List(xs) => CanonicalValue::List(xs.into_iter().map(fold_null).collect()),
        CanonicalValue::Set(xs) => CanonicalValue::Set(xs.into_iter().map(fold_null).collect()),
        CanonicalValue::Map(kvs) => CanonicalValue::Map(
            kvs.into_iter()
                .map(|(k, v)| (fold_null(k), fold_null(v)))
                .collect(),
        ),
        CanonicalValue::Tuple(fs) => {
            CanonicalValue::Tuple(fs.into_iter().map(|(k, v)| (k, fold_null(v))).collect())
        }
        other => other,
    }
}

/// Re-narrow a CQL `float` to 32 bits wherever the DECLARED type says `float`.
///
/// sstabledump prints a `float` with Java's `Float.toString`, which round-trips
/// through 32 bits; parsed as JSON it becomes the nearest DOUBLE to that decimal
/// text, which is not the double a widened `f32` gives (1.84 vs
/// 1.8399999141693115). Narrowing the golden value to `f32` and widening it back
/// makes both sides hold the same double, WITHOUT introducing a tolerance: any
/// genuinely different float still differs.
pub fn narrow_floats(v: CanonicalValue, spec: &CqlTypeSpec) -> CanonicalValue {
    match (v, spec) {
        (CanonicalValue::Float(NormalizedFloat(f)), CqlTypeSpec::Scalar(name))
            if name == "float" =>
        {
            CanonicalValue::Float(NormalizedFloat(f as f32 as f64))
        }
        (CanonicalValue::List(xs), CqlTypeSpec::Seq { elem, .. }) => {
            CanonicalValue::List(xs.into_iter().map(|x| narrow_floats(x, elem)).collect())
        }
        (CanonicalValue::Set(xs), CqlTypeSpec::Seq { elem, .. }) => {
            CanonicalValue::Set(xs.into_iter().map(|x| narrow_floats(x, elem)).collect())
        }
        (CanonicalValue::Map(kvs), CqlTypeSpec::Map { key, value }) => CanonicalValue::Map(
            kvs.into_iter()
                .map(|(k, v)| (narrow_floats(k, key), narrow_floats(v, value)))
                .collect(),
        ),
        (CanonicalValue::List(xs), CqlTypeSpec::Tuple(specs)) if xs.len() == specs.len() => {
            CanonicalValue::List(
                xs.into_iter()
                    .zip(specs.iter())
                    .map(|(x, s)| narrow_floats(x, s))
                    .collect(),
            )
        }
        (other, _) => other,
    }
}
