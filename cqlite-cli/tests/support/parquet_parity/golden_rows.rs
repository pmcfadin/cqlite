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
//! …and, for the same reason in the other direction, on a golden cell that is
//! PRESENT but carries NO AUTHORITATIVE VALUE — a missing `value` OR an explicit
//! `"value": null`, the two spellings of one state — and is not one of the
//! tombstone shapes above: see [`require_recognized_cell_shape`].
//!
//! A fixture that grows one of those turns RED here instead of silently
//! comparing the wrong things. Reconciliation-sensitive tables belong to the
//! query-semantics oracle (`test-data/query-semantics-oracle.json`), not to a
//! physical-dump lane.
//!
//! # Five normalizations, all of them equalities rather than tolerances
//!
//! The normalizations themselves live in ONE place — `declared.rs`, the single
//! declared-type-guided canonicalization entry point — because three review
//! rounds each found a different POSITION that canonicalized without consulting
//! the declared CQL type. This module's job is to decide WHICH POSITION each
//! raw golden piece sits at (a stringified `path` component is not a typed
//! `value`, and a primary-key component is not a cell) and to hand it, with the
//! declared type for that position, to `declared::canonicalize_golden`.
//!
//! * **`Null` folds into `Absent` — but ONLY where a null is legitimate.**
//!   Arrow/Parquet has ONE null; sstabledump renders an absent cell by omitting
//!   it and a null UDT field as an explicit JSON `null`. Both denote the same
//!   absence in CQL, so where CQL permits a null the golden side folds `Null`
//!   into `Absent`. Where CQL does NOT — a collection element, a map key or
//!   value, a primary-key or collection-path component — an absence in either
//!   spelling is REFUSED rather than folded, because folded it would AGREE with
//!   an export that wrongly wrote NULL (`declared::Position::permits_absence`).
//!   (Folding cannot mask an empty-vs-null bug either: an empty text is
//!   `Text("")`, an empty collection is an empty container, and neither is
//!   `Absent`.)
//! * **A multicell collection with no live elements is `Absent`.** Cassandra
//!   reads a non-frozen collection whose elements are all gone as NULL, so an
//!   export writing NULL there is correct. Applied ONLY to non-frozen
//!   collections: a `frozen<set<int>>` really can hold an empty set, which stays
//!   an empty container and so still differs from NULL. Decided HERE, since only
//!   this module sees the per-element cells.
//! * **Numbers are canonicalized by DECLARED type** — a `float` re-narrowed to
//!   32 bits, a `decimal` recovered to the EXACT unscaled-value/scale pair its
//!   literal denotes (never an `f64`). Both are exact, and both REFUSE rather
//!   than round.
//! * **A STRING is typed by its DECLARED type, never by its SPELLING** — a
//!   declared `text`/`varchar`/`ascii` value stays `Text` however it is spelled,
//!   and only a declared `timestamp` compares as an instant. Inferring a type
//!   from a value's bytes is the no-heuristics violation of issue #28.
//! * **A FROZEN map's JSON object becomes a canonical map**, and every
//!   STRINGIFIED position — a primary-key component, a multicell collection
//!   `path` component, a frozen map's JSON object key — is converted back
//!   through its declared scalar type, or REFUSED. A key component is
//!   canonicalized ONCE, before the value is put into BOTH `keys` and `cells`,
//!   so the two can never disagree about the same component.
//!
#![allow(dead_code)]

use std::collections::BTreeMap;

use super::canonical_jsonl::{self, CanonicalDocument, CanonicalRow, CanonicalValue};
use super::cql_type::{ColumnType, CqlTypeSpec, SeqKind};
use super::declared::{canonicalize_golden, Declared};
use super::failure::{Failures, Precondition};
use super::golden_text;
use super::{Fixture, ParityCase};

/// One row projected out of the golden: the primary-key components in declared
/// order, plus every declared column's canonical value.
#[derive(Debug)]
pub struct GoldenRow {
    pub keys: Vec<CanonicalValue>,
    pub cells: BTreeMap<String, CanonicalValue>,
}

/// Stage GOLDEN: load the committed sstabledump dump and project it.
///
/// # Every check here is a PRECONDITION, and preconditions are GAP-INDEPENDENT
///
/// This function establishes that the oracle EXISTS, PARSES, is STRUCTURALLY
/// valid, is physical-dump ELIGIBLE (#1742) and PROJECTS TO AT LEAST ONE ROW.
/// None of those is an assertion about the export; each is a validity
/// precondition of the comparison being meaningful at all. They therefore report
/// [`Failure::Precondition`], which no [`super::KnownGap`] can name or excuse
/// (`failure.rs`).
///
/// **Do not re-entangle them with the comparison.** The non-emptiness check in
/// particular used to live in `mod.rs::compare_inner`, i.e. only on the
/// comparison path — so a case whose export ABORTED behind a recorded gap never
/// reached it, and a golden whose partitions all held empty `rows` passed
/// vacuously (round 19). That is the THIRD appearance of one family in this
/// harness; rounds 12 and 13 are rolled up on [`Precondition`]. The rule the
/// three produced: an expected-failure marker must suppress ONLY the assertion
/// it names, never a precondition of the comparison. So a check that establishes
/// MEANINGFULNESS belongs HERE, at the point the oracle is loaded, and never
/// behind the export.
pub fn load_golden(
    case: &ParityCase,
    fixture: &Fixture,
    columns: &[ColumnType],
) -> Result<Vec<GoldenRow>, Failures> {
    // The golden TEXT is read here rather than through
    // `canonical_jsonl::load_golden_document_with_keys` so that a declared
    // `decimal`'s LITERAL survives the parse: the shared comparator turns a bare
    // JSON number into an `f64`, and an `f64` cannot identify the decimal it was
    // parsed from (`0.100000000000000001` and `0.1` are the same double).
    // `golden_text.rs` keeps every value's ORIGINAL TEXT through the
    // deserialization itself, quotes only the DECLARED `decimal`/`varint`
    // positions (round 11) and reaches a cell only at `rows[].cells[]` (round
    // 12). Two refusals keep it fail-closed: it errors on any line whose
    // sstabledump structure does not hold, and `declared.rs` REFUSES a declared
    // `decimal`/`varint` position that still arrives as a double.
    // PRECONDITION 3 — the golden EXISTS, is READABLE and PARSES.
    let raw = std::fs::read_to_string(&fixture.golden).map_err(|e| {
        Failures::precondition(
            Precondition::GoldenReadable,
            format!(
                "reading the sstabledump golden {} failed: {e}",
                fixture.golden.display()
            ),
        )
    })?;
    if let Some(marker) = golden_text::placeholder_marker(&raw) {
        return Err(Failures::precondition(
            Precondition::GoldenReadable,
            format!(
                "the sstabledump golden {} carries the placeholder marker {marker} — it is not \
                 a real Cassandra dump and cannot be an oracle",
                fixture.golden.display()
            ),
        ));
    }
    let content = golden_text::preserve_exact_lexemes(&raw, columns).map_err(|e| {
        Failures::precondition(
            Precondition::GoldenReadable,
            format!(
                "preserving the exact literals of the sstabledump golden {} failed: {e}",
                fixture.golden.display()
            ),
        )
    })?;
    // ELIGIBILITY is decided from the TEXT the parser is about to read, not from
    // what the parser managed to parse out of it: the shared parser is lenient by
    // design and turns a PRESENT-BUT-INVALID `ttl`, `deletion_info` or `rows`
    // into an absence or an empty collection, which is exactly how a golden that
    // this oracle must refuse (#1742) reads as live data. See
    // [`validate_golden_text`], and `golden_schema.rs`.
    // PRECONDITION 4 — the golden's sstabledump STRUCTURE holds (total).
    validate_golden_text(&content).map_err(|e| {
        Failures::precondition(
            Precondition::GoldenStructure,
            format!(
                "the sstabledump golden {} is not usable as a physical-dump oracle: {e}",
                fixture.golden.display()
            ),
        )
    })?;
    let golden_doc = canonical_jsonl::parse_document_str_with_keys(
        &content,
        &fixture.golden,
        true,
        &case.key_spec(),
    )
    .map_err(|e| {
        Failures::precondition(
            Precondition::GoldenReadable,
            format!("loading the sstabledump golden failed: {e}"),
        )
    })?;
    // PRECONDITION 5 — physical-dump ELIGIBILITY (#1742) and projectability.
    let rows = project_golden(&golden_doc, columns, case.partition_key, case.clustering)
        .map_err(|e| Failures::precondition(Precondition::GoldenEligible, e))?;
    // PRECONDITION 6 — the oracle is NON-EMPTY. Checked HERE, where the oracle
    // is LOADED, and never on the comparison path: see this function's header.
    require_nonempty_projection(rows.len())?;
    Ok(rows)
}

/// PRECONDITION: the golden projected to AT LEAST ONE ROW.
///
/// One function, taking a COUNT so both callers can share it: the LOAD site
/// (unconditionally, before any gap can short-circuit) and, as a backstop for
/// its direct callers, `mod.rs::compare`. The two sites must never be able to
/// disagree about what "an empty oracle" is, or about which class of failure it
/// produces.
///
/// A nonempty JSONL document whose partitions all carry empty `rows` arrays is
/// exactly this state, and is the round-19 finding: it parses, it is
/// structurally valid, it is eligible — and it witnesses nothing.
pub fn require_nonempty_projection(projected_rows: usize) -> Result<(), Failures> {
    if projected_rows == 0 {
        return Err(Failures::precondition(
            Precondition::GoldenNonEmpty,
            "the sstabledump golden projected to ZERO rows — a dataset-dependent comparison \
             must never pass on an empty oracle",
        ));
    }
    Ok(())
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
            // (`declared::canonicalize_golden` at the `PrimaryKey` position,
            // which is STRINGIFIED: sstabledump renders every key component
            // through Cassandra's `AbstractType.getString`, so a `boolean`
            // arrives as `"true"`, a `float` through `Float.toString` and a
            // `decimal` through `BigDecimal.toString`) — ONCE, here, BEFORE they are
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
                    Some(col) => canonicalize_golden(
                        raw.clone(),
                        &Declared::primary_key(
                            &col.spec,
                            format!("{where_} key column '{name}' ({})", col.declared),
                        ),
                    )
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
                // ONE declared-type-guided canonicalization for the whole column
                // value, at the `Cell` position — null folding, the frozen-map
                // reshape, string typing and the exact number rules, in one
                // descent that carries the declared type into every nested
                // position (`declared.rs`). Running them as separate passes is
                // what let a position pick up two of the three (round 6).
                let value = canonicalize_golden(
                    value,
                    &Declared::cell(
                        &col.spec,
                        format!("{where_} column '{}' ({})", col.name, col.declared),
                    ),
                )
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

// ---------------------------------------------------------------------------
// ELIGIBILITY, DECIDED FROM THE TEXT — before a malformed field can become an
// absence
// ---------------------------------------------------------------------------
//
// The refusals above are the load-bearing half of this oracle (#1742), and they
// were decided from fields the SHARED parser had already parsed SUCCESSFULLY.
// That parser is `cqlite-core`-owned and deliberately lenient: it reads every
// optional field through `get(..).and_then(as_str/as_i64/as_array)`, so a field
// that is PRESENT BUT INVALID becomes `None` — indistinguishable from a field
// that is absent. Three consequences, each of which turned an ineligible or
// malformed golden into "live data":
//
//   * `"ttl": "3600"` (a string, or any non-integer) → `ttl_secs: None` → the
//     TTL refusal above never fires, and the harness compares a dump whose rows
//     can EXPIRE between fixture generation and test time.
//   * `"deletion_info": 7` (or any non-object) at partition level →
//     `parse_deletion_info` returns `None` → the deletion refusal never fires,
//     and the dump's shadowed rows are compared against a reconciled export.
//   * `"rows": 7` / no `rows` at all → an EMPTY row list → a partition that
//     silently contributes NOTHING to the oracle.
//
// So eligibility is decided BEFORE the parse, from the golden TEXT the parser is
// about to read, and PRESENT-BUT-INVALID is a REFUSAL rather than an absence.
//
// That check is `golden_schema.rs`, and it is TOTAL: it validates EVERY field of
// the sstabledump structure this harness consumes against its expected JSON type
// AND the grammar of its content, driven by a description of that structure, and
// refuses a field the description does not cover. It is one pass rather than a
// list because the list was the defect generator — three review rounds each
// found another field that had been consumed without being validated (an array
// whose components were never checked to be JSON strings; a timestamp checked to
// be a string but never to PARSE). See that module's docs for the split between
// what it judges and what the DECLARED-TYPE descent judges.

/// REFUSE a golden document whose structure does not hold — the TOTAL
/// validation, run on the very text handed to
/// `canonical_jsonl::parse_document_str_with_keys` (see [`load_golden`]), so what
/// is validated is what is parsed.
///
/// Re-exported here because this is the module that RUNS it; the description and
/// the walk live in [`super::golden_schema`].
pub use super::golden_schema::validate_golden_text;

use super::golden_schema::ineligible_at;

fn ineligible(pi: usize, what: &str) -> String {
    ineligible_at(&format!("partition {pi}"), what)
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
///
/// # An ABSENT cell and a PRESENT cell with no value are two different things
///
/// A Cassandra NULL is the ABSENCE OF A CELL: sstabledump omits the cell
/// entirely, `mine` comes out empty, and `Absent` is the right answer — that is
/// the first thing this function decides, and it must keep working. A cell that
/// is PRESENT but carries no `value` is a different state, and the shared parser
/// renders it as the SAME `CanonicalValue::Absent` (`canonical_jsonl::parse_cell`
/// maps a missing `value` key to `Absent`). Presence is still representable
/// here, though — a present cell IS an entry in `mine` — so this function
/// decides the two apart itself rather than letting the second one become NULL.
/// [`require_recognized_cell_shape`] is that decision.
fn project_column(
    where_: &str,
    row: &CanonicalRow,
    col: &ColumnType,
) -> Result<CanonicalValue, String> {
    let mine: Vec<_> = row.cells.iter().filter(|c| c.name == col.name).collect();
    if mine.is_empty() {
        // The column has NO cell in this row: a Cassandra NULL.
        return Ok(CanonicalValue::Absent);
    }
    // Every PRESENT cell must be a shape this oracle recognizes, decided ONCE
    // here — the one place that knows both the declared type and which piece of
    // a cell carries the value at each position.
    for cell in &mine {
        require_recognized_cell_shape(where_, col, cell)?;
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
        match &col.spec {
            CqlTypeSpec::Seq { .. } | CqlTypeSpec::Map { .. } => {}
            other => {
                return Err(format!(
                    "{where_}: column '{}' is multicell but its parsed type {other:?} is not a \
                     collection",
                    col.name
                ))
            }
        }

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
            // Each piece is canonicalized AT ITS OWN declared position, because
            // only here is it known which pieces are STRINGIFIED: a set element
            // and a map KEY arrive as Cassandra's stringified `path`, while a
            // list element and a map VALUE arrive as typed JSON in `value`.
            // Routing the paths through the same declared-scalar conversion as a
            // primary-key component is the round-7 fix — they used to be coerced
            // for INTEGRAL element types only, so a `set<float>`, `set<decimal>`
            // or boolean-keyed map compared stringified text against typed Arrow
            // values.
            let at = |what: &str| format!("{where_} column '{}' {what}", col.name);
            match &col.spec {
                CqlTypeSpec::Seq {
                    kind: SeqKind::Set,
                    elem,
                } => list_items.push(canonicalize_golden(
                    cell.path[0].clone(),
                    &Declared::collection_path(elem, at("element path")),
                )?),
                CqlTypeSpec::Seq {
                    kind: SeqKind::List,
                    elem,
                } => list_items.push(canonicalize_golden(
                    cell.value.clone(),
                    &Declared::element(elem, at("element")),
                )?),
                CqlTypeSpec::Map { key, value } => map_items.push((
                    canonicalize_golden(
                        cell.path[0].clone(),
                        &Declared::collection_path(key, at("entry key path")),
                    )?,
                    canonicalize_golden(
                        cell.value.clone(),
                        &Declared::map_value(value, at("entry value")),
                    )?,
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
    // NOT folded here: `canonicalize_golden` folds an absence at the position
    // that OWNS it (`Position::permits_absence`), and a recursive fold applied
    // in advance would erase which spelling the golden used — turning a nested
    // `"value": null` into "a missing value" in the refusal that reports it.
    Ok(value_cells[0].value.clone())
}

/// REFUSE a PRESENT golden cell that carries NO AUTHORITATIVE VALUE — however
/// the golden spells that — unless the cell is one of the shapes whose value
/// legitimately lives somewhere other than `value`.
///
/// # THE PROPERTY, not a list of spellings
///
/// A present cell must carry an authoritative value for its column, and "no
/// authoritative value here" is REFUSED whichever way it is written. In this
/// pipeline the absence has TWO spellings and they are ONE state:
///
///   * a **missing `value` key** — the shared parser
///     (`canonical_jsonl::parse_cell`) maps it onto `CanonicalValue::Absent`;
///   * an **explicit `"value": null`** — the same parser maps it onto
///     `CanonicalValue::Null`, which the descent then folds into that same
///     `Absent` wherever an absence is legitimate.
///
/// Both are therefore checked by ONE predicate ([`carries_no_authoritative_value`])
/// rather than enumerated: the first version of this refusal blacklisted the
/// missing-key spelling alone, and the explicit `null` reached `Absent` by the
/// other route and compared EQUAL to an export that wrongly wrote NULL. A
/// nested null at a position where Cassandra legitimately emits one (a UDT
/// field, a tuple member) is not this state and is accepted — that decision
/// belongs to the position, and `declared::Position::permits_absence` makes it.
///
/// # Why this is a refusal and not a NULL
///
/// `Absent` is the very value an ABSENT cell (a Cassandra NULL) projects to.
/// Without this refusal such a cell is compared as NULL, so a golden that lost a
/// value AGREES with an export that wrongly writes NULL, and the harness reports
/// parity for the silent NULL-coercion this issue exists to catch (AC1, #1485).
/// A malformed oracle must red, never bless.
///
/// # The three shapes that carry no `value` and are legitimate
///
/// Each is recognized HERE and classified by its own rule elsewhere, so this
/// check never fires on one:
///
///   * a **cell tombstone** and a **non-frozen collection SHELL deletion** —
///     both carry `deletion_info` and no `value`. `project_column` refuses the
///     scalar tombstone as ineligible (#1742) and reads the shell's
///     `markedForDeleteAt` to decide which elements are shadowed.
///   * a **per-element tombstone** inside a collection — `deletion_info` plus a
///     `path`, refused as ineligible by the element loop.
///   * a **non-frozen SET element**, whose value IS its stringified `path`
///     (`sstabledump` writes `"value": ""`, or omits it): there is no `value` to
///     require, and the path is what gets canonicalized. A map KEY arrives the
///     same way, but a map cell also carries the entry's VALUE in `value`, so a
///     map cell with no `value` is malformed and IS refused.
fn require_recognized_cell_shape(
    where_: &str,
    col: &ColumnType,
    cell: &super::canonical_jsonl::CanonicalCell,
) -> Result<(), String> {
    if !carries_no_authoritative_value(&cell.value) {
        return Ok(());
    }
    if cell.deletion.is_some() {
        return Ok(());
    }
    let is_set_element = !cell.path.is_empty()
        && col.is_multicell_collection()
        && matches!(
            col.spec,
            CqlTypeSpec::Seq {
                kind: SeqKind::Set,
                ..
            }
        );
    if is_set_element {
        return Ok(());
    }
    Err(format!(
        "{where_}: column '{}' (declared '{}') has a cell that is PRESENT but carries no \
         `value`{}, and it is none of the shapes whose value lives elsewhere (a tombstone, a \
         collection-shell deletion, or a set element whose value is its `path`). A Cassandra \
         NULL is the ABSENCE OF A CELL, not a present cell with no value, so the harness \
         REFUSES this golden instead of reading it as NULL: read as NULL it would AGREE with \
         an export that wrongly writes NULL, which is the silent NULL-coercion (AC1, #1485) \
         this oracle exists to catch",
        col.name,
        col.declared,
        match cell.value {
            CanonicalValue::Null => " (it is spelled as an explicit JSON `null`)",
            _ => "",
        }
    ))
}

/// Does this cell value carry NO AUTHORITATIVE VALUE — in either of the two
/// spellings the pipeline can produce?
///
/// The whole point of the predicate is that it is asked ONCE, of the STATE,
/// rather than at each site of each spelling. `Absent` is a cell with no `value`
/// key; `Null` is `"value": null`. Both mean "there is nothing here", and at a
/// cell that is the one thing a golden may not say (see
/// [`require_recognized_cell_shape`]).
fn carries_no_authoritative_value(value: &CanonicalValue) -> bool {
    matches!(value, CanonicalValue::Absent | CanonicalValue::Null)
}

/// Recursively fold an explicit JSON `null` into `Absent` — Arrow has only one
/// null, and CQL does not distinguish the two.
///
/// The DECLARED-TYPE descent does not use this: it folds an absence at the
/// position that owns it (`declared::Position::permits_absence`), and REFUSES
/// one where CQL requires a value — a recursive fold applied in advance is
/// exactly what let a nested `null` become an `Absent` nobody judged. This
/// function survives for the ONE position with no declared type to descend
/// through: a key column the case does not declare, whose case is failed by name
/// immediately afterwards.
pub(super) fn fold_null(v: CanonicalValue) -> CanonicalValue {
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
