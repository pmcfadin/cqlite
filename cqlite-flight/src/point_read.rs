//! Partition point-read route detection (issue #2207, Stage 0).
//!
//! Decides, ONCE per ticket, whether a pushed predicate binds **every**
//! partition-key component to a single value — a full-PK equality (or an
//! `IN`/`Or` list of such equalities). Only such shapes route `do_get` to the
//! partition point-read path; anything else keeps the unchanged full-scan +
//! per-row filter path.
//!
//! The decision is derived from the typed predicate tree ([`FilterExpr`]) and
//! the table schema's partition-key definition ALONE — never from byte patterns
//! or any non-authoritative heuristic (no-heuristics mandate, issue #28). The
//! analyzer is TOTAL: any shape it cannot prove is a full-PK equality falls
//! through to [`PointReadRoute::Scan`].

use crate::filter::FilterExpr;
use cqlite_core::query::{SSTableFilterOp, SSTablePredicate};
use cqlite_core::schema::TableSchema;
use cqlite_core::types::Value;

/// The routing decision computed from a ticket's lowered filter + schema.
#[derive(Debug, Clone, PartialEq)]
pub enum PointReadRoute {
    /// Every partition-key component is bound to a single value (full-PK
    /// equality). The `Vec<Value>` holds the bound values in partition-key
    /// **schema order**, ready for `PartitionKey { columns }.to_bytes(schema)`.
    PartitionPointRead(Vec<Value>),
    /// A bounded set of full-PK equalities (`WHERE pk IN (...)` over the full PK,
    /// or an `Or` of full-PK equalities) — treated as N point reads. Each inner
    /// `Vec<Value>` is one key's component values in partition-key schema order.
    MultiPartitionPointRead(Vec<Vec<Value>>),
    /// Anything else — partial PK, clustering-only, range, secondary column,
    /// `IS NULL`, `Not`, mixed `Or`, or no predicate. Keeps the full-scan path.
    Scan,
}

/// Compute the point-read route from a lowered filter tree and the schema.
///
/// Returns [`PointReadRoute::Scan`] for every shape that is not provably a
/// full-PK equality (or an `IN`/`Or` list of them). Non-partition-key conjuncts
/// (e.g. `AND col = ?`) do NOT block a point route — they remain a residual
/// per-row filter that narrows, never widens, the result.
pub fn detect_route(filter: Option<&FilterExpr>, schema: &TableSchema) -> PointReadRoute {
    let pk_cols: Vec<&str> = schema
        .partition_keys
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    if pk_cols.is_empty() {
        return PointReadRoute::Scan;
    }
    let Some(filter) = filter else {
        return PointReadRoute::Scan;
    };

    // 1. A conjunction (or single leaf) that binds every PK component by equality.
    if let Some(values) = full_pk_equality(filter, &pk_cols) {
        return PointReadRoute::PartitionPointRead(values);
    }

    // 2. `IN` over a single-component partition key → N single-key lookups.
    //    (A composite-PK `IN` is a cartesian expansion we deliberately do not
    //    take here; it falls through to Scan.)
    if pk_cols.len() == 1 {
        if let Some(keys) = single_pk_in_list(filter, pk_cols[0]) {
            return PointReadRoute::MultiPartitionPointRead(keys);
        }
    }

    // 3. An `Or` whose every disjunct is itself a full-PK equality.
    if let FilterExpr::Or(disjuncts) = filter {
        if let Some(keys) = or_of_full_pk_equalities(disjuncts, &pk_cols) {
            return PointReadRoute::MultiPartitionPointRead(keys);
        }
    }

    PointReadRoute::Scan
}

/// Return the PK component values (in schema order) iff `filter` is a
/// conjunction of leaves that bind EVERY partition-key column by a single
/// equality, with no other (dis-qualifying) constraint on any PK column.
fn full_pk_equality(filter: &FilterExpr, pk_cols: &[&str]) -> Option<Vec<Value>> {
    let mut bindings: Vec<Option<Value>> = vec![None; pk_cols.len()];
    if !collect_pk_equalities(filter, pk_cols, &mut bindings) {
        return None;
    }
    // Every component must be bound exactly once.
    bindings.into_iter().collect::<Option<Vec<Value>>>()
}

/// Walk a conjunction, recording PK-column equality bindings. Returns `false`
/// (disqualifying the point route) if ANY partition-key column is constrained by
/// something other than a single top-level `=` conjunct (a range/`IN`, an
/// `IsNull`, an appearance under `Or`/`Not`, or a conflicting second binding).
///
/// Non-partition-key nodes are ignored for routing (they remain a residual
/// per-row filter), so `pk = ? AND col > ?` still routes on the PK equality.
fn collect_pk_equalities(
    expr: &FilterExpr,
    pk_cols: &[&str],
    bindings: &mut [Option<Value>],
) -> bool {
    match expr {
        FilterExpr::And(children) => children
            .iter()
            .all(|c| collect_pk_equalities(c, pk_cols, bindings)),
        FilterExpr::Leaf(pred) => {
            match pk_component_index(pred, pk_cols) {
                // A leaf on a non-PK column is an ignorable residual.
                None => !mentions_pk_predicate(pred, pk_cols),
                Some(idx) => {
                    // A PK-column leaf must be a plain single-value equality.
                    if !is_single_equality(pred) {
                        return false;
                    }
                    let value = pred.values[0].clone();
                    match &bindings[idx] {
                        // Conflicting second binding for the same component.
                        Some(existing) if existing != &value => false,
                        _ => {
                            bindings[idx] = Some(value);
                            true
                        }
                    }
                }
            }
        }
        // Any Or/Not/IsNull that touches a PK column disqualifies the point route;
        // one that is purely over non-PK columns is an ignorable residual.
        FilterExpr::Or(_) | FilterExpr::Not(_) | FilterExpr::IsNull(_) => {
            !mentions_pk_expr(expr, pk_cols)
        }
    }
}

/// Return the bound-value lists for `pk IN (v1, v2, ...)` iff `filter` is exactly
/// that single-column `IN` leaf on the sole partition-key column (with no other
/// constraint). Each returned key is a one-element `Vec<Value>`.
fn single_pk_in_list(filter: &FilterExpr, pk_col: &str) -> Option<Vec<Vec<Value>>> {
    let FilterExpr::Leaf(pred) = filter else {
        return None;
    };
    if pred.token_columns.is_some()
        || pred.column != pk_col
        || !matches!(pred.operation, SSTableFilterOp::In)
        || pred.values.is_empty()
    {
        return None;
    }
    Some(pred.values.iter().cloned().map(|v| vec![v]).collect())
}

/// Return one full-PK key per disjunct iff EVERY disjunct of an `Or` is itself a
/// full-PK equality (e.g. `(a=1 AND b=2) OR (a=3 AND b=4)`).
fn or_of_full_pk_equalities(disjuncts: &[FilterExpr], pk_cols: &[&str]) -> Option<Vec<Vec<Value>>> {
    if disjuncts.is_empty() {
        return None;
    }
    disjuncts
        .iter()
        .map(|d| full_pk_equality(d, pk_cols))
        .collect()
}

/// The partition-key component index a predicate constrains, or `None` when it is
/// not a (non-token) partition-key column predicate.
fn pk_component_index(pred: &SSTablePredicate, pk_cols: &[&str]) -> Option<usize> {
    if pred.token_columns.is_some() {
        return None;
    }
    pk_cols.iter().position(|c| *c == pred.column)
}

/// A plain single-value `=` on a stored column (not a token predicate).
fn is_single_equality(pred: &SSTablePredicate) -> bool {
    pred.token_columns.is_none()
        && matches!(pred.operation, SSTableFilterOp::Equal)
        && pred.values.len() == 1
}

/// Whether a predicate references a partition-key column (via its column name or
/// its `token_columns` set).
fn mentions_pk_predicate(pred: &SSTablePredicate, pk_cols: &[&str]) -> bool {
    if pk_cols.contains(&pred.column.as_str()) {
        return true;
    }
    match &pred.token_columns {
        Some(cols) => cols.iter().any(|c| pk_cols.contains(&c.as_str())),
        None => false,
    }
}

/// Whether any leaf/`IsNull` anywhere in `expr` references a partition-key column.
fn mentions_pk_expr(expr: &FilterExpr, pk_cols: &[&str]) -> bool {
    match expr {
        FilterExpr::And(children) | FilterExpr::Or(children) => {
            children.iter().any(|c| mentions_pk_expr(c, pk_cols))
        }
        FilterExpr::Not(inner) => mentions_pk_expr(inner, pk_cols),
        FilterExpr::Leaf(pred) => mentions_pk_predicate(pred, pk_cols),
        FilterExpr::IsNull(column) => pk_cols.contains(&column.as_str()),
    }
}
