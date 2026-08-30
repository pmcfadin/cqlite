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
//! # Five normalizations, all of them equalities rather than tolerances
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
//! * **Numbers are canonicalized by DECLARED type** — a `float` re-narrowed to
//!   32 bits, a `decimal` recovered to the EXACT unscaled-value/scale pair its
//!   literal denotes (never an `f64`). Both are exact, and both REFUSE rather
//!   than round; see [`normalize_declared_numbers`].
//! * **A STRING is typed by its DECLARED type, never by its SPELLING** — the
//!   shared parser turns any `Z`-suffixed timestamp spelling into a
//!   `Timestamp`, which is a guess it has to make (its other lanes have no
//!   schema) and which this harness must undo: a declared `text`/`varchar`/
//!   `ascii` value stays `Text` however it is spelled, and only a declared
//!   `timestamp` compares as an instant. See [`normalize_declared_strings`] —
//!   inferring a type from a value's bytes is the no-heuristics violation of
//!   issue #28.
//! * **A FROZEN map's JSON object becomes a canonical map** — sstabledump writes
//!   a frozen map as a JSON OBJECT, which `from_json` necessarily reads as a
//!   `Tuple`; the Arrow side reads the same column back as a `Map`. See
//!   [`coerce_declared_shape`].
//! * **A PRIMARY-KEY component is typed from its full declared type** —
//!   sstabledump writes every key component as a quoted STRING, so a `boolean`,
//!   `float`, `double` or `decimal` key arrives as text and must be converted to
//!   the same canonical form the Arrow side lands on. See
//!   [`canonicalize_key_component`], which is applied ONCE, before the value is
//!   put into BOTH `keys` and `cells`.

#![allow(dead_code)]

use std::collections::BTreeMap;

use super::canonical_jsonl::{
    CanonicalDocument, CanonicalRow, CanonicalValue, KeyKind, NormalizedFloat,
};
use super::cql_type::{ColumnType, CqlTypeSpec, SeqKind};
use super::decimal::{
    exact_from_golden_double, exact_from_text, ExactDecimal, EXPORT_DECIMAL_SCALE,
};

/// One row projected out of the golden: the primary-key components in declared
/// order, plus every declared column's canonical value.
#[derive(Debug)]
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

            // KEY components are canonicalized from their FULL declared type
            // ([`canonicalize_key_component`]) — ONCE, here, BEFORE they are
            // inserted into `keys` and cloned into `cells`, so the two can never
            // disagree about the same component.
            let mut keys: Vec<CanonicalValue> =
                Vec::with_capacity(part.key.len() + row.clustering.len());
            for (name, raw) in partition_key
                .iter()
                .zip(part.key.iter())
                .chain(clustering.iter().zip(row.clustering.iter()))
            {
                let value = match columns.iter().find(|c| c.name == **name) {
                    Some(col) => canonicalize_key_component(raw, col)
                        .map_err(|e| format!("{where_}: key column '{name}': {e}"))?,
                    // A key column the case does not declare: the check below
                    // fails the case by name, so there is nothing to guide by.
                    None => fold_null(raw.clone()),
                };
                keys.push(value);
            }

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
                // Shape FIRST (a frozen map's JSON object becomes a canonical
                // map), then strings, then numbers. The order is load-bearing in
                // both steps: `normalize_declared_numbers` recurses into a `Map`
                // but not into a `Tuple` standing in for one, and a frozen map's
                // KEYS only become reachable by
                // `normalize_declared_strings` once the object is a `Map`.
                let value = coerce_declared_shape(value, &col.spec);
                let value = normalize_declared_strings(value, &col.spec);
                let value = normalize_declared_numbers(value, &col.spec)
                    .map_err(|e| format!("{where_}: column '{}': {e}", col.name))?;
                cells.insert(col.name.clone(), value);
            }

            // Every declared key column must have been filled by the key arrays.
            //
            // A PRESENCE check only: the value was canonicalized once, above,
            // and `cells` holds a CLONE of the very `keys` entry. Re-normalizing
            // it here is what made the two able to disagree (round-6 finding:
            // the numeric pass ran on `cells` and not on `keys`, so a valid
            // `float`/`decimal` key produced a false primary-KEY difference
            // while its cell compared equal).
            for name in partition_key.iter().chain(clustering.iter()) {
                if !cells.contains_key(*name) {
                    return Err(format!(
                        "{where_}: declared key column '{name}' is not in the golden row"
                    ));
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

/// Reshape a golden value into the shape its DECLARED CQL type implies.
///
/// # The one shape JSON cannot carry: a frozen map
///
/// sstabledump writes a FROZEN map as a JSON OBJECT — `{"a": 1, "b": 2}` — and a
/// JSON object carries nothing that distinguishes "a map" from "a struct", so
/// `canonical_jsonl::from_json` necessarily reads it as a `Tuple`. The Arrow side
/// reads the same column back as a `Map`. Without this reshape the two would
/// report a FALSE VALUE DIFFERENCE for a value both sides agree on, on every
/// frozen-map column. (Latent today: the corpus's only frozen maps, `fm` and
/// `ma` on `test_compactionparityudt.udt_collections`, sit behind the #3556
/// whole-case gap and never reach the value comparison — they would start
/// diverging the day #3556 is fixed.)
///
/// The DECLARED type is what settles it, exactly as it settles the number
/// canonicalization above: a `Tuple` stays a `Tuple` for every declared type
/// that is NOT a map.
///
/// # What is and is not weakened
///
/// Nothing: the entry COUNT is preserved, every key and value is still compared
/// exactly, and no ordering is relaxed. Two details make that true.
///
/// * **Keys.** A JSON object's keys are always STRINGS, so a
///   `frozen<map<int,text>>` arrives with `"1"` where the Arrow side holds
///   `Int(1)`. The key is coerced back through the DECLARED key type by the same
///   rule collection PATH components and partition-key components use
///   ([`coerce_path`] / `CanonicalValue::from_json_key`), never blindly — a
///   `map<text,int>` key `"5"` stays `Text` and can therefore never compare
///   equal to an integer key `5`.
/// * **Order.** `CanonicalValue::Map` compares as an ordered sequence, so the
///   two sides' entry order has to agree. It does: the workspace pins
///   `serde_json`'s `preserve_order` feature, so the golden's object order IS
///   the order sstabledump wrote (Cassandra's key-comparator order), which is
///   the order the Arrow map carries. That dependency is load-bearing and is
///   asserted directly by `golden_json_object_order_is_preserved` in
///   `issue_1490_parquet_jsonl_parity.rs`, so dropping the feature reds with a
///   named explanation instead of silently mis-ordering a frozen map.
pub fn coerce_declared_shape(v: CanonicalValue, spec: &CqlTypeSpec) -> CanonicalValue {
    match (v, spec) {
        (CanonicalValue::Tuple(fields), CqlTypeSpec::Map { key, value }) => CanonicalValue::Map(
            fields
                .into_iter()
                .map(|(k, v)| (coerce_object_key(&k, key), coerce_declared_shape(v, value)))
                .collect(),
        ),
        // Already a map (the non-frozen, per-element path): recurse only.
        (CanonicalValue::Map(kvs), CqlTypeSpec::Map { key, value }) => CanonicalValue::Map(
            kvs.into_iter()
                .map(|(k, v)| {
                    (
                        coerce_declared_shape(k, key),
                        coerce_declared_shape(v, value),
                    )
                })
                .collect(),
        ),
        (CanonicalValue::List(xs), CqlTypeSpec::Seq { elem, .. }) => CanonicalValue::List(
            xs.into_iter()
                .map(|x| coerce_declared_shape(x, elem))
                .collect(),
        ),
        (CanonicalValue::Set(xs), CqlTypeSpec::Seq { elem, .. }) => CanonicalValue::Set(
            xs.into_iter()
                .map(|x| coerce_declared_shape(x, elem))
                .collect(),
        ),
        // A CQL tuple deliberately has NO arm here. It arrives as a positional
        // JSON ARRAY (canonical `List`) while the exported Arrow `Struct`
        // decodes to a canonical `Tuple` of NAMED fields, so the two sides are
        // in different representations. Coercing one into the other would have
        // to invent the field names the comparison would then check — so the
        // harness REFUSES the column's values by name instead
        // (`unsupported::CQL_TUPLE_VALUES`), and this recursion never runs for a
        // tuple. Removed rather than left in place: an arm that recursed into
        // the members while leaving the OUTER shapes mismatched was the silent
        // half of the round-4 finding.
        (other, _) => other,
    }
}

/// A JSON OBJECT key is always a string; decode it against the declared map-key
/// type, using the same machinery sstabledump's string-rendered KEY components
/// go through (so an integral key is coerced and a text key is not).
fn coerce_object_key(raw: &str, key_spec: &CqlTypeSpec) -> CanonicalValue {
    let kind = match key_spec {
        CqlTypeSpec::Scalar(name) => KeyKind::from_cql_type(name),
        // A non-scalar map key (a frozen collection/UDT/tuple key) is not a
        // numeric string, so no coercion applies; `from_json_key` falls through
        // to `from_json`, which still recognizes a timestamp spelling.
        _ => KeyKind::Other,
    };
    fold_null(CanonicalValue::from_json_key(
        &serde_json::Value::String(raw.to_string()),
        kind,
    ))
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

/// Restore a golden STRING value to the representation its DECLARED CQL type
/// implies, recursively.
///
/// # The defect this closes (no-heuristics, #28)
///
/// The shared JSON parser (`canonical_jsonl::CanonicalValue::from_json`) has one
/// value-level type inference in it: a quoted string that SPELLS a
/// `Z`-suffixed timestamp becomes `CanonicalValue::Timestamp`. It has to guess,
/// because it is used by lanes that have no schema — sstabledump renders a
/// `timestamp` as a quoted string, so without the declared type the spelling is
/// the only signal there is.
///
/// This harness is not one of those lanes: every column's DECLARED CQL type is
/// right here (`cql_type.rs`, copied from the committed Cassandra schema). So the
/// guess is both unnecessary and WRONG in one direction — a `text`/`varchar`/
/// `ascii` value that happens to spell a timestamp (`"2025-10-06 01:12:07.265Z"`
/// is legal text) became a `Timestamp` on the golden side while the Arrow `Utf8`
/// side stayed `Text`, and the two can never compare equal. That is a FALSE
/// parity failure produced by inferring a type from a value's shape, which is
/// exactly what the no-heuristics mandate forbids.
///
/// The rule, therefore: a golden value is a `Timestamp` if and only if its
/// declared type IS `timestamp`. Every other declared scalar restores the
/// original string — `text`/`varchar`/`ascii`, and equally `blob`/`uuid`/
/// `timeuuid`/`date`/`time`/`inet`/`duration`, all of which the Arrow side
/// renders as `Text` (`arrow_rows`). Nothing is weakened: the restored value is
/// the string the golden literally carried (`Timestamp.raw` is never rewritten),
/// so a genuine text difference still differs, and a declared `timestamp`
/// keeps comparing as an INSTANT (`.06Z` == `.060000Z`).
///
/// # Where it has to reach
///
/// Everywhere a string can sit, not just a top-level scalar: a collection
/// ELEMENT (`set<text>`/`list<text>`), a map KEY and a map VALUE, a tuple
/// member, and a primary-KEY component (which arrives through
/// `from_json_key`, whose fallback is the same `from_json`). Hence the recursion
/// below and the key-component pass in [`project_golden`].
///
/// # The one position it cannot reach, and why that is not a hidden guess
///
/// A UDT's FIELD types are not known to the harness (`CqlTypeSpec::Udt` is a
/// name only — a frozen UDT arrives as one JSON object whose values sstabledump
/// has already typed), so a timestamp-shaped string in a `text` UDT field is
/// left as the parser produced it. That is a REFUSAL to guess, not a guess: the
/// declared type genuinely is not available, and if such a field ever appears it
/// shows up as a loud value difference rather than as a silent pass. (The
/// corpus's UDT columns sit behind the #3556 whole-case gap today.)
pub fn normalize_declared_strings(v: CanonicalValue, spec: &CqlTypeSpec) -> CanonicalValue {
    match (v, spec) {
        (CanonicalValue::Timestamp { micros, raw }, CqlTypeSpec::Scalar(name)) => {
            if name == "timestamp" {
                CanonicalValue::Timestamp { micros, raw }
            } else {
                CanonicalValue::Text(raw)
            }
        }
        (CanonicalValue::List(xs), CqlTypeSpec::Seq { elem, .. }) => CanonicalValue::List(
            xs.into_iter()
                .map(|x| normalize_declared_strings(x, elem))
                .collect(),
        ),
        (CanonicalValue::Set(xs), CqlTypeSpec::Seq { elem, .. }) => CanonicalValue::Set(
            xs.into_iter()
                .map(|x| normalize_declared_strings(x, elem))
                .collect(),
        ),
        // Both halves: a map KEY is a value too, and a `map<text,int>` key is
        // precisely where a stringified timestamp spelling lands.
        (CanonicalValue::Map(kvs), CqlTypeSpec::Map { key, value }) => CanonicalValue::Map(
            kvs.into_iter()
                .map(|(k, v)| {
                    (
                        normalize_declared_strings(k, key),
                        normalize_declared_strings(v, value),
                    )
                })
                .collect(),
        ),
        // A CQL tuple arrives as a positional JSON array, matched member-wise —
        // the same shape `normalize_declared_numbers` uses.
        (CanonicalValue::List(xs), CqlTypeSpec::Tuple(specs)) if xs.len() == specs.len() => {
            CanonicalValue::List(
                xs.into_iter()
                    .zip(specs.iter())
                    .map(|(x, s)| normalize_declared_strings(x, s))
                    .collect(),
            )
        }
        // Anything else — a `Timestamp` under a collection/UDT declaration (a
        // shape disagreement the value comparison must report), or a non-string
        // variant — is left EXACTLY as it was.
        (other, _) => other,
    }
}

/// Canonicalize ONE primary-key component from its FULL declared type.
///
/// # The defect this closes (issue #1490 round 6)
///
/// `sstabledump` renders EVERY primary-key component as a quoted JSON string —
/// Cassandra serializes it through `AbstractType.getString`, so an `int` key `1`
/// is dumped as `"1"`, a `boolean` as `"true"`, a `float` through
/// `Float.toString` and a `decimal` through `BigDecimal.toString`. (Measured in
/// the committed corpus: `test_comp.lz4_table`'s golden carries
/// `"partition":{"key":["1"]}` for its `pk int`.)
///
/// The shared loader coerced exactly ONE of those families back:
/// `CanonicalValue::from_json_key` turns a clean numeric string into `Int` for an
/// INTEGRAL key column ([`KeyKind::Integral`]) and leaves everything else as
/// `Text`. So a declared `boolean` key stayed `Text("true")` while the Arrow side
/// held `Bool(true)`, a `float` key stayed `Text("1.84")` against `Float(...)`,
/// and a `decimal` key stayed `Text("10576.6")` against the exact decimal form —
/// a FALSE primary-key difference on every row of such a table, i.e. a
/// false FAIL, reported as if the export were wrong.
///
/// Worse, the two halves were canonicalized SEPARATELY: the numeric pass ran
/// over `cells` only, so a key's cell and the same key's sort-key component could
/// disagree with each other. This function is therefore called ONCE per
/// component, and its result is what goes into both.
///
/// # It converts, it never guesses
///
/// The conversion is driven entirely by the DECLARED type, and a value that does
/// not denote that type is a REFUSAL (`Err`), never a fallback to the string: a
/// declared `boolean` key holding `"maybe"` is a broken fixture or a broken
/// declaration, and comparing it as text would hide both. Non-scalar key types (a
/// `frozen<list<text>>` clustering key, which the corpus has and this lane does
/// not yet cover) are left untouched — a refusal to guess, and the case declaring
/// one is enumerated as uncovered rather than silently mishandled.
pub fn canonicalize_key_component(
    raw: &CanonicalValue,
    col: &ColumnType,
) -> Result<CanonicalValue, String> {
    let value = type_key_string(fold_null(raw.clone()), &col.spec)?;
    let value = normalize_declared_strings(value, &col.spec);
    normalize_declared_numbers(value, &col.spec)
}

/// Turn a key component's STRING rendering into the variant its declared scalar
/// type denotes. `Int` coercion is included so this function alone settles an
/// integral key, whether or not the loader's `KeySpec` already did it.
fn type_key_string(v: CanonicalValue, spec: &CqlTypeSpec) -> Result<CanonicalValue, String> {
    let (CanonicalValue::Text(s), CqlTypeSpec::Scalar(name)) = (&v, spec) else {
        return Ok(v);
    };
    let refuse = |what: &str| {
        Err(format!(
            "declared '{name}' but the golden renders the key component as {s:?}, which is not              {what}; the harness refuses to compare it as text rather than hide the disagreement"
        ))
    };
    match name.as_str() {
        "boolean" => match s.as_str() {
            // Cassandra's `BooleanType.getString` is Java's `Boolean.toString`.
            "true" => Ok(CanonicalValue::Bool(true)),
            "false" => Ok(CanonicalValue::Bool(false)),
            _ => refuse("'true' or 'false'"),
        },
        // Narrowing for `float` is applied by `normalize_declared_numbers`, which
        // runs next — the same rule a `float` CELL goes through, so the two
        // cannot diverge.
        "float" | "double" => match s.parse::<f64>() {
            Ok(f) if f.is_finite() => Ok(CanonicalValue::Float(NormalizedFloat(f))),
            _ => refuse("a finite decimal number"),
        },
        // The literal TEXT is still present here (a decimal CELL arrives as a
        // JSON number and has to be RECOVERED from a double instead), so this is
        // exact with nothing to refuse for ambiguity.
        "decimal" => {
            Ok(exact_from_text(s, EXPORT_DECIMAL_SCALE, "golden decimal key")?.canonical())
        }
        "int" | "bigint" | "smallint" | "tinyint" | "varint" | "counter" => {
            match s.parse::<i128>() {
                Ok(i) => Ok(CanonicalValue::Int(i)),
                Err(_) => refuse("an integer"),
            }
        }
        // text/varchar/ascii/blob/uuid/timeuuid/date/time/inet/duration all
        // compare as `Text` on the Arrow side, and `timestamp` is handled by
        // `normalize_declared_strings` — nothing to convert.
        _ => Ok(v),
    }
}

/// Canonicalize a golden NUMBER according to its DECLARED CQL type.
///
/// Two rules, both of them equalities rather than tolerances, and both able to
/// REFUSE (`Err`) rather than compare something they cannot compare exactly:
///
/// * **`float` re-narrows to 32 bits.** sstabledump prints a `float` with Java's
///   `Float.toString`, which round-trips through 32 bits; parsed as JSON it
///   becomes the nearest DOUBLE to that decimal text, which is not the double a
///   widened `f32` gives (1.84 vs 1.8399999141693115). Narrowing the golden
///   value to `f32` and widening it back makes both sides hold the same double,
///   WITHOUT introducing a tolerance: any genuinely different float still
///   differs, and the comparison stays exact-bit.
/// * **A `decimal` becomes an EXACT decimal, never an `f64`.** The exported
///   `Decimal128(38, s)` cell carries an exact unscaled value and scale
///   (`arrow_rows::decimal_to_canonical`), and the golden literal is recovered
///   to the same exact form:
///   - an integer-shaped literal (`1`, which sstabledump writes for a
///     whole-valued decimal, and which `canonical_jsonl` reads as `Int`) is
///     already exact — [`ExactDecimal::from_i128`], with no magnitude ceiling;
///   - a fractional literal reached this harness as an `f64`, so
///     [`exact_from_golden_double`] recovers it and VERIFIES the recovery,
///     refusing when the double cannot identify one decimal.
///
///   This replaces the previous `Int → Float(i as f64)` rule and its `|i| < 2^53`
///   ceiling: the whole point of the exact representation is that neither side
///   goes through a double, so neither side needs a magnitude bound. A `varint`
///   is deliberately NOT converted: it is an integer domain on both sides
///   (`Decimal128(_, 0)` → `Int`), so converting it would be the type confusion
///   this rule exists to avoid.
pub fn normalize_declared_numbers(
    v: CanonicalValue,
    spec: &CqlTypeSpec,
) -> Result<CanonicalValue, String> {
    Ok(match (v, spec) {
        (CanonicalValue::Float(NormalizedFloat(f)), CqlTypeSpec::Scalar(name))
            if name == "float" =>
        {
            CanonicalValue::Float(NormalizedFloat(f as f32 as f64))
        }
        (CanonicalValue::Float(NormalizedFloat(f)), CqlTypeSpec::Scalar(name))
            if name == "decimal" =>
        {
            exact_from_golden_double(f, EXPORT_DECIMAL_SCALE, "golden decimal")?.canonical()
        }
        (CanonicalValue::Int(i), CqlTypeSpec::Scalar(name)) if name == "decimal" => {
            ExactDecimal::from_i128(i).canonical()
        }
        (CanonicalValue::List(xs), CqlTypeSpec::Seq { elem, .. }) => CanonicalValue::List(
            xs.into_iter()
                .map(|x| normalize_declared_numbers(x, elem))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        (CanonicalValue::Set(xs), CqlTypeSpec::Seq { elem, .. }) => CanonicalValue::Set(
            xs.into_iter()
                .map(|x| normalize_declared_numbers(x, elem))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        (CanonicalValue::Map(kvs), CqlTypeSpec::Map { key, value }) => CanonicalValue::Map(
            kvs.into_iter()
                .map(|(k, v)| {
                    Ok((
                        normalize_declared_numbers(k, key)?,
                        normalize_declared_numbers(v, value)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
        (CanonicalValue::List(xs), CqlTypeSpec::Tuple(specs)) if xs.len() == specs.len() => {
            CanonicalValue::List(
                xs.into_iter()
                    .zip(specs.iter())
                    .map(|(x, s)| normalize_declared_numbers(x, s))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        (other, _) => other,
    })
}
