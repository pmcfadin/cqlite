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

/// Maximum number of keys a full-PK `IN` list / `Or`-of-equalities may route as
/// [`PointReadRoute::MultiPartitionPointRead`] (design.md open question 2, fixed
/// named cap per the design's recommendation). Above this bound, N separate
/// per-SSTable seeks (each paying its own presence-oracle + index-descent cost)
/// stop being cheaper than one full k-way scan with a per-row `IN` filter, so
/// the route falls back to [`PointReadRoute::Scan`] instead — never a wrong
/// answer, just the faster path for a very large list. Chosen generously (a
/// typical pushed `IN` list is single/low-double-digit; Cassandra's own CQL
/// driver default page/batch sizes sit well under this) so ordinary `IN`
/// pushdowns are unaffected.
const MAX_MULTI_PARTITION_POINT_READ_KEYS: usize = 64;

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
///
/// The analyzer recurses through `And` conjuncts: a point-read *key-group* (a
/// full-PK equality, a single-component full-PK `IN`, or an `Or` of full-PK
/// equalities) is extracted from AMONG the conjuncts, and the REMAINING
/// conjuncts stay a residual per-row filter — the whole original filter is
/// re-evaluated per row on the point path ([`drive_merge`]'s `filter.keeps`), so
/// an extracted key-group need only be a SUPERSET of the matching partitions.
/// At most ONE key-group is extracted: if two different PK key-groups appear
/// under one `And` (e.g. `pk IN (1,2) AND pk IN (3,4)`), the route conservatively
/// falls back to [`PointReadRoute::Scan`] rather than intersect them — always a
/// correct answer via the scan path, never a wrong one.
///
/// [`drive_merge`]: crate::producer::MergeProducer::drive_merge
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

    let mut acc = RouteAcc::new(pk_cols.len());
    if !collect_route(filter, &pk_cols, &mut acc) {
        return PointReadRoute::Scan;
    }

    match acc.multi_group {
        // A multi-key group (`IN` / `Or`) fully specifies the PK by itself. Any
        // additional PK equality binding is a SECOND, different key-group under
        // the same `And` — conservatively fall back to the (always-correct) scan
        // path rather than intersect the two groups.
        Some(keys) => {
            if acc.bindings.iter().any(Option::is_some) {
                return PointReadRoute::Scan;
            }
            // Dedup the candidate keys BEFORE applying the cap (issue #2207): a
            // duplicate-heavy `IN` list must not over-fall-back to Scan when its
            // distinct-key count is within the cap (the merger dedups too).
            capped_multi_point_read(dedup_keys(keys))
        }
        // No multi-group: a plain full-PK equality routes; a partial (or empty)
        // PK binding cannot point-read.
        None => match acc.bindings.into_iter().collect::<Option<Vec<Value>>>() {
            Some(values) => PointReadRoute::PartitionPointRead(values),
            None => PointReadRoute::Scan,
        },
    }
}

/// Accumulator threaded through the conjunct walk: PK equality bindings (in
/// schema order) plus at most one extracted multi-key group.
struct RouteAcc {
    /// One slot per PK component; `Some` once bound by an `=` conjunct.
    bindings: Vec<Option<Value>>,
    /// The single extracted `IN`/`Or` key-group, if any.
    multi_group: Option<Vec<Vec<Value>>>,
    /// How many multi-key groups have been seen (>1 disqualifies the route).
    multi_group_count: usize,
}

impl RouteAcc {
    fn new(pk_len: usize) -> Self {
        RouteAcc {
            bindings: vec![None; pk_len],
            multi_group: None,
            multi_group_count: 0,
        }
    }

    /// Record an `=` binding for PK component `idx`. Returns `false` (disqualify)
    /// on a conflicting second value for the same component.
    fn bind(&mut self, idx: usize, value: Value) -> bool {
        match &self.bindings[idx] {
            Some(existing) if existing != &value => false,
            _ => {
                self.bindings[idx] = Some(value);
                true
            }
        }
    }

    /// Record an extracted multi-key group. Returns `false` (disqualify) if a
    /// second, different key-group is seen under the same `And`.
    fn add_multi_group(&mut self, keys: Vec<Vec<Value>>) -> bool {
        self.multi_group_count += 1;
        if self.multi_group_count > 1 {
            return false;
        }
        self.multi_group = Some(keys);
        true
    }
}

/// Walk the (possibly nested) conjunction, filling `acc`. Returns `false`
/// (disqualifying the point route) if ANY partition-key column is constrained by
/// a shape that is not a single `=` binding or an extractable key-group (a
/// range, a conflicting binding, a second key-group, an `IsNull`/`Not` on a PK
/// column, or an `Or` over PK columns that is not a clean full-PK disjunction).
///
/// Non-partition-key nodes are ignored for routing — they remain a residual
/// per-row filter — so `pk = ? AND col > ?` and `pk IN (?) AND col > ?` route.
fn collect_route(expr: &FilterExpr, pk_cols: &[&str], acc: &mut RouteAcc) -> bool {
    match expr {
        FilterExpr::And(children) => children.iter().all(|c| collect_route(c, pk_cols, acc)),
        FilterExpr::Leaf(pred) => classify_leaf(pred, pk_cols, acc),
        // An `Or` over PK columns is only routable as a full-PK disjunction key
        // group; one purely over non-PK columns is an ignorable residual.
        FilterExpr::Or(disjuncts) => {
            if !mentions_pk_expr(expr, pk_cols) {
                return true;
            }
            match or_of_full_pk_equalities(disjuncts, pk_cols) {
                Some(keys) => acc.add_multi_group(keys),
                None => false,
            }
        }
        // A `Not`/`IsNull` touching a PK column disqualifies; over non-PK columns
        // it is an ignorable residual.
        FilterExpr::Not(inner) => !mentions_pk_expr(inner, pk_cols),
        FilterExpr::IsNull(column) => !pk_cols.contains(&column.as_str()),
    }
}

/// Classify a single leaf conjunct into `acc`. A single-component full-PK `IN`
/// becomes a multi-key group; a PK-column `=` becomes a binding; a non-PK leaf
/// is an ignorable residual; anything else on a PK column disqualifies.
fn classify_leaf(pred: &SSTablePredicate, pk_cols: &[&str], acc: &mut RouteAcc) -> bool {
    // A single-component partition key bound by `IN` → N single-key lookups.
    // (A composite-PK `IN` is a cartesian expansion we deliberately do not take;
    // it falls through to the PK-column check below and disqualifies.)
    if pk_cols.len() == 1 {
        if let Some(keys) = sole_pk_in_leaf(pred, pk_cols[0]) {
            return acc.add_multi_group(keys);
        }
    }
    match pk_component_index(pred, pk_cols) {
        // A leaf on a non-PK column is an ignorable residual — unless it is a
        // token predicate that references a PK column, which disqualifies.
        None => !mentions_pk_predicate(pred, pk_cols),
        Some(idx) => {
            // A PK-column leaf must be a plain single-value equality.
            if !is_single_equality(pred) {
                return false;
            }
            acc.bind(idx, pred.values[0].clone())
        }
    }
}

/// Route `keys` as [`PointReadRoute::MultiPartitionPointRead`], or fall back to
/// [`PointReadRoute::Scan`] when the list exceeds
/// [`MAX_MULTI_PARTITION_POINT_READ_KEYS`] (design.md open question 2).
fn capped_multi_point_read(keys: Vec<Vec<Value>>) -> PointReadRoute {
    if keys.len() > MAX_MULTI_PARTITION_POINT_READ_KEYS {
        return PointReadRoute::Scan;
    }
    PointReadRoute::MultiPartitionPointRead(keys)
}

/// Order-preserving dedup of candidate keys. `Value` is only `PartialEq` (it may
/// hold floats), so a hash-set is unavailable; the list is bounded by the cap
/// check that follows, so the linear scan is cheap.
fn dedup_keys(keys: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    let mut out: Vec<Vec<Value>> = Vec::with_capacity(keys.len());
    for k in keys {
        if !out.contains(&k) {
            out.push(k);
        }
    }
    out
}

/// Return the PK component values (in schema order) iff `filter` is a
/// conjunction of leaves that bind EVERY partition-key column by a single
/// equality, with no other (dis-qualifying) constraint on any PK column.
fn full_pk_equality(filter: &FilterExpr, pk_cols: &[&str]) -> Option<Vec<Value>> {
    let mut acc = RouteAcc::new(pk_cols.len());
    if !collect_route(filter, pk_cols, &mut acc) {
        return None;
    }
    // A disjunct is a full-PK equality only when it is exactly that: every
    // component bound by `=`, no extracted sub-group.
    if acc.multi_group.is_some() {
        return None;
    }
    acc.bindings.into_iter().collect::<Option<Vec<Value>>>()
}

/// Return the bound-value lists for `pk IN (v1, v2, ...)` iff `pred` is that
/// single-column `IN` leaf on the sole partition-key column. Each returned key
/// is a one-element `Vec<Value>`.
fn sole_pk_in_leaf(pred: &SSTablePredicate, pk_col: &str) -> Option<Vec<Vec<Value>>> {
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
