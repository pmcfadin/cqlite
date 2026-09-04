//! Server-side scan filtering: token range, predicate pushdown, projection.
//!
//! Translates a [`FlightTicket`]'s filter fields into a [`ScanSpec`] the producer
//! applies while merging:
//! - **token range** drops whole partitions outside the split's `(start, end]`
//!   range (cross-replica dedup — see `PLAN.md` §2),
//! - **predicates** are an arbitrary nested boolean tree ([`FilterExpr`])
//!   evaluated per row with SQL Kleene three-valued logic (issue #834), and
//! - **projection** restricts the emitted columns.
//!
//! The ticket's recursive [`PredicateExpr`] is *lowered* once, at
//! [`ScanSpec::from_ticket`] time, into a [`FilterExpr`] whose comparison leaves
//! are fully type-resolved [`SSTablePredicate`]s. Lowering resolves each leaf
//! column's authoritative `CqlType` and parses its JSON operand, so type errors
//! surface up-front (consistent with the prior flat-predicate behaviour) rather
//! than per row. Leaf comparisons reuse cqlite-core's
//! [`cqlite_core::query::evaluate_leaf`] so Flight and `SELECT` share one copy of
//! the comparison logic (no heuristics, issue #28).

use cqlite_core::query::{evaluate_leaf, LeafOutcome, QueryRow, SSTableFilterOp, SSTablePredicate};
use cqlite_core::schema::{CqlType, TableSchema};
use cqlite_core::types::Value;

use crate::ticket::{
    token_in_half_open_range, FlightTicket, Predicate, PredicateExpr, PredicateOp,
};

/// Errors building a [`ScanSpec`] from a ticket.
#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    /// A predicate referenced a column not present in the schema.
    #[error("predicate column '{0}' is not in the table schema")]
    UnknownColumn(String),
    /// A column's CQL type string could not be parsed.
    #[error("invalid CQL type '{type_str}' for column '{column}': {source}")]
    InvalidType {
        /// Column name.
        column: String,
        /// The offending type string.
        type_str: String,
        /// Underlying parse error.
        source: cqlite_core::Error,
    },
    /// A predicate JSON operand could not be converted to the column's type.
    #[error("predicate on '{column}': {message}")]
    BadOperand {
        /// Column name.
        column: String,
        /// What went wrong.
        message: String,
    },
    /// Issue #3742: the ticket carried an explicitly EMPTY projection list, so
    /// the request has no output columns at all.
    #[error(
        "projection selects no columns: 'columns' is an empty list. \
         A request must select at least one output column; omit 'columns' \
         (JSON null) to select them all"
    )]
    EmptyProjection,
    /// Issue #3742: every name in the projection is absent from the table
    /// schema, so the resolved projection is empty. The offending names are
    /// carried so the client is told WHICH ones (`retain` used to drop them
    /// silently).
    #[error(
        "projection selects no columns: none of the projected columns exist in \
         the table schema: {}",
        .0.iter().map(|c| format!("'{c}'")).collect::<Vec<_>>().join(", ")
    )]
    UnknownProjectionColumns(Vec<String>),
    /// Issue #3742: the ticket carried an aggregation with neither group-by
    /// keys nor aggregates, whose output column set (`group_by` + `aggregates`)
    /// is therefore empty.
    #[error(
        "aggregation produces no output columns: 'group_by' and 'aggregates' \
         are both empty"
    )]
    EmptyAggregation,
}

/// Token-range membership test for one split.
#[derive(Debug, Clone, Copy)]
pub struct TokenFilter {
    start: i64,
    end: i64,
    /// Ring wrapping, DERIVED from `start`/`end` at construction the way
    /// `Range.isWrapAround` derives it (`start >= end`) — never taken from the
    /// ticket's wire flag (issue #3634). Kept as a field rather than recomputed
    /// per call because `contains`/`overlaps` are per-row and per-SSTable hot
    /// paths; it is private, and `ScanSpec::from_ticket` is the only constructor
    /// outside this crate's tests, so it cannot drift from the endpoints.
    wraparound: bool,
}

impl TokenFilter {
    /// True if `token` falls in this split's `(start, end]` range.
    pub fn contains(&self, token: i64) -> bool {
        token_in_half_open_range(token, self.start, self.end, self.wraparound)
    }

    /// Lower this split into the core-side [`ScanTokenBound`](cqlite_core::storage::sstable::reader::ScanTokenBound)
    /// that the Summary-guided streaming walk pushes into the per-SSTable scan
    /// (issue #2412 §C / #2413 Option A). The core bound mirrors this filter's
    /// half-open `(start, end]` membership EXACTLY (including the `start == end`
    /// FULL-ring convention, #2228); `token_filter_lowering_agrees_with_core` pins
    /// that agreement so the two never diverge.
    ///
    /// The core bound DERIVES its wrapping from the endpoints the way
    /// `Range.isWrapAround` does, so this filter's own `wraparound` flag is not
    /// carried across (issue #3634); the grid test covers both spellings.
    pub fn to_scan_bound(self) -> cqlite_core::storage::sstable::reader::ScanTokenBound {
        cqlite_core::storage::sstable::reader::ScanTokenBound {
            start_excl: self.start,
            end_incl: self.end,
        }
    }

    /// True if an SSTable spanning `[min_token, max_token]` (inclusive of both
    /// endpoints, since they are the smallest and largest partition tokens
    /// actually present) could contain any token in this split's `(start, end]`
    /// range.
    ///
    /// Half-open `(start, end]` semantics: a partition at exactly `start` is
    /// excluded, one at exactly `end` is included. For a non-wrapping range the
    /// spans overlap iff the SSTable reaches past `start` (`max_token > start`)
    /// and starts at or before `end` (`min_token <= end`). A wraparound range is
    /// `(start, MAX] ∪ [MIN, end]`, so overlap holds if the SSTable reaches past
    /// `start` OR starts at or before `end`.
    pub fn overlaps(&self, min_token: i64, max_token: i64) -> bool {
        // #2228: equal endpoints (`(T, T]`) denote the FULL ring, so every
        // SSTable span overlaps. Mirror the per-token semantics in
        // [`token_in_half_open_range`] regardless of the `wraparound` flag,
        // otherwise SSTable-level pruning could silently drop every table.
        if self.start == self.end {
            return true;
        }
        if self.wraparound {
            max_token > self.start || min_token <= self.end
        } else {
            max_token > self.start && min_token <= self.end
        }
    }
}

/// A typed, fully-resolved predicate tree, evaluated per row with SQL Kleene
/// three-valued logic (issue #834).
///
/// This is the lowered form of the ticket's [`PredicateExpr`]: comparison and
/// membership leaves carry a type-resolved [`SSTablePredicate`] (operand JSON
/// already parsed against the column's `CqlType`), and `IsNull` carries the bare
/// column name. Lowering happens once in [`ScanSpec::from_ticket`].
#[derive(Debug, Clone)]
pub enum FilterExpr {
    /// Conjunction. Empty `And` is `TRUE`.
    And(Vec<FilterExpr>),
    /// Disjunction. Empty `Or` is `FALSE`.
    Or(Vec<FilterExpr>),
    /// Negation.
    Not(Box<FilterExpr>),
    /// A type-resolved comparison or membership leaf.
    Leaf(SSTablePredicate),
    /// `column IS NULL` (true when the column is absent or `Null`).
    IsNull(String),
}

/// SQL three-valued (Kleene) truth value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kleene {
    /// Definitely true.
    True,
    /// Definitely false.
    False,
    /// Unknown (a `NULL`/missing operand).
    Unknown,
}

impl FilterExpr {
    /// Evaluate this tree against `row` with SQL Kleene logic.
    ///
    /// - `Leaf`: delegates to [`evaluate_leaf`] — a missing/`Null` column is
    ///   `Unknown`, otherwise a definite `True`/`False`.
    /// - `IsNull`: `True` when the column is absent or `Null`, else `False` —
    ///   never `Unknown`.
    /// - `Not`: `True↔False`, `Unknown` stays `Unknown`.
    /// - `And`: `False` if any conjunct is `False`; else `Unknown` if any is
    ///   `Unknown`; else `True` (empty `And` → `True`).
    /// - `Or`: `True` if any disjunct is `True`; else `Unknown` if any is
    ///   `Unknown`; else `False` (empty `Or` → `False`).
    pub fn evaluate(&self, row: &QueryRow) -> Kleene {
        match self {
            FilterExpr::Leaf(predicate) => match evaluate_leaf(row, predicate) {
                LeafOutcome::True => Kleene::True,
                LeafOutcome::False => Kleene::False,
                LeafOutcome::Unknown => Kleene::Unknown,
            },
            FilterExpr::IsNull(column) => {
                // Absent column or an explicit Null cell are both SQL NULL.
                match row.values.get(column.as_str()) {
                    None | Some(Value::Null) => Kleene::True,
                    Some(_) => Kleene::False,
                }
            }
            FilterExpr::Not(inner) => match inner.evaluate(row) {
                Kleene::True => Kleene::False,
                Kleene::False => Kleene::True,
                Kleene::Unknown => Kleene::Unknown,
            },
            FilterExpr::And(exprs) => {
                let mut saw_unknown = false;
                for e in exprs {
                    match e.evaluate(row) {
                        Kleene::False => return Kleene::False,
                        Kleene::Unknown => saw_unknown = true,
                        Kleene::True => {}
                    }
                }
                if saw_unknown {
                    Kleene::Unknown
                } else {
                    Kleene::True
                }
            }
            FilterExpr::Or(exprs) => {
                let mut saw_unknown = false;
                for e in exprs {
                    match e.evaluate(row) {
                        Kleene::True => return Kleene::True,
                        Kleene::Unknown => saw_unknown = true,
                        Kleene::False => {}
                    }
                }
                if saw_unknown {
                    Kleene::Unknown
                } else {
                    Kleene::False
                }
            }
        }
    }

    /// A row is kept iff the top-level expression evaluates to `True`. Both
    /// `False` and `Unknown` reject the row, matching SQL `WHERE` semantics.
    pub fn keeps(&self, row: &QueryRow) -> bool {
        matches!(self.evaluate(row), Kleene::True)
    }

    /// Insert every column name this predicate tree references into `out`.
    ///
    /// Used by the producer's projection-aware row assembly (issue #2324, roborev
    /// 1633): a column a predicate reads must be materialized even when it is
    /// projected OUT of the output, so it belongs in the assembler's "needed" set.
    pub fn collect_referenced_columns(&self, out: &mut std::collections::HashSet<String>) {
        match self {
            FilterExpr::Leaf(p) => {
                out.insert(p.column.clone());
            }
            FilterExpr::IsNull(column) => {
                out.insert(column.clone());
            }
            FilterExpr::Not(inner) => inner.collect_referenced_columns(out),
            FilterExpr::And(exprs) | FilterExpr::Or(exprs) => {
                for e in exprs {
                    e.collect_referenced_columns(out);
                }
            }
        }
    }
}

/// A fully-resolved scan: what to keep and which columns to emit.
#[derive(Debug, Clone, Default)]
pub struct ScanSpec {
    /// Partition-level token filter; `None` keeps every partition.
    pub token: Option<TokenFilter>,
    /// Row-level predicate tree; `None` keeps every row.
    pub filter: Option<FilterExpr>,
    /// Column projection; `None` emits all columns.
    pub projection: Option<Vec<String>>,
    /// Row cap (issue #2129): emit at most this many rows for the scan, counted
    /// AFTER token pruning and predicate filtering. `None` scans the full range.
    /// Ignored on the aggregation path (partial rows already collapse the set).
    pub limit: Option<u64>,
}

impl ScanSpec {
    /// Build a scan spec from a ticket against its table schema.
    ///
    /// The ticket's effective filter (v2 `filter` tree, or the v1 `predicates`
    /// folded into an `And`; see [`FlightTicket::effective_filter`]) is lowered
    /// to a typed [`FilterExpr`] here, so any unknown column or bad operand is
    /// reported up-front rather than per row.
    pub fn from_ticket(ticket: &FlightTicket, schema: &TableSchema) -> Result<Self, FilterError> {
        let token = match (ticket.token_start, ticket.token_end) {
            (None, None) => None,
            (start, end) => {
                let start = start.unwrap_or(i64::MIN);
                let end = end.unwrap_or(i64::MAX);
                Some(TokenFilter {
                    start,
                    end,
                    // DERIVED from the endpoints, never copied from the ticket
                    // (issue #3634). `ticket.wraparound` is a `#[serde(default)]`
                    // WIRE field that `validate()` does not check against
                    // `token_start`/`token_end`, so a client can present a flag
                    // that disagrees with its own bounds — a state Cassandra
                    // cannot express, since `Range.isWrapAround(left, right)` IS
                    // `left.compareTo(right) >= 0`.
                    //
                    // Deriving here is what keeps this filter consistent with the
                    // core bound it lowers to: `to_scan_bound` derives the same
                    // way, while `contains` and `overlaps` read this field, and
                    // `contains` alone gates FOUR serving paths (producer_stream,
                    // producer_point, producer_drive, statics) that take no
                    // pushdown. Copying an inconsistent flag here therefore made
                    // those paths answer differently from the pushdown for the
                    // SAME ticket — path-dependent rows, which is worse than
                    // either answer alone.
                    wraparound: start >= end,
                })
            }
        };

        // Preserve v1 validation: a flat `IN` predicate must carry a JSON array
        // operand. `effective_filter` folds a non-array operand into a singleton
        // for forward-compat, so reject the malformed v1 shape here (where the v1
        // `predicates` list is authoritative) to keep the original error.
        if ticket.filter.is_none() {
            for p in &ticket.predicates {
                if matches!(p.op, PredicateOp::In) && !p.value.is_array() {
                    return Err(FilterError::BadOperand {
                        column: p.column.clone(),
                        message: "IN requires a JSON array operand".into(),
                    });
                }
            }
        }

        // Issue #3742: ADMIT the request's OUTPUT COLUMN SET here, before any
        // producer, Arrow schema or response stream exists. Every `do_get` route
        // reaches this function through `CqliteFlightService::build_producer`
        // (`service.rs:481`), so a request that would emit zero output columns is
        // refused as `InvalidArgument` with no message on the wire, instead of
        // reaching `RecordBatch::try_new(<zero-field schema>, vec![])` and
        // surfacing arrow's refusal as a mid-stream `Status::Internal`.
        admit_output_columns(ticket, schema)?;

        let filter = ticket
            .effective_filter()
            .map(|expr| lower_predicate_expr(&expr, schema))
            .transpose()?;

        Ok(Self {
            token,
            filter,
            projection: ticket.columns.clone(),
            limit: ticket.limit,
        })
    }
}

/// Lower a ticket [`PredicateExpr`] into a typed [`FilterExpr`], resolving each
/// comparison/membership leaf's column type and parsing its JSON operand.
///
/// `pub(crate)` (issue #2605) so the feature-gated DataFusion `TableProvider`
/// spike can validate a translated DataFusion filter through THIS lowering — the
/// production one — instead of re-deriving operand coercion. A second
/// implementation of type resolution is how the two engines would come to
/// disagree about which rows a predicate keeps; a lowering FAILURE there is the
/// signal the filter is not pushable at all.
pub(crate) fn lower_predicate_expr(
    expr: &PredicateExpr,
    schema: &TableSchema,
) -> Result<FilterExpr, FilterError> {
    match expr {
        PredicateExpr::And { exprs } => Ok(FilterExpr::And(lower_children(exprs, schema)?)),
        PredicateExpr::Or { exprs } => Ok(FilterExpr::Or(lower_children(exprs, schema)?)),
        PredicateExpr::Not { expr } => Ok(FilterExpr::Not(Box::new(lower_predicate_expr(
            expr, schema,
        )?))),
        PredicateExpr::IsNull { column } => {
            // Validate the column exists so a typo surfaces at build time, like
            // every other leaf; the type itself is irrelevant to a null test.
            column_cql_type(schema, column)?;
            Ok(FilterExpr::IsNull(column.clone()))
        }
        PredicateExpr::Compare { column, op, value } => {
            let predicate = to_sstable_predicate(
                &Predicate {
                    column: column.clone(),
                    op: *op,
                    value: value.clone(),
                },
                schema,
            )?;
            Ok(FilterExpr::Leaf(predicate))
        }
        PredicateExpr::In { column, values } => {
            // An `In` node carries an explicit list; build the v1-shaped predicate
            // (op = In, value = JSON array) and reuse the existing lowering.
            let predicate = to_sstable_predicate(
                &Predicate {
                    column: column.clone(),
                    op: PredicateOp::In,
                    value: serde_json::Value::Array(values.clone()),
                },
                schema,
            )?;
            Ok(FilterExpr::Leaf(predicate))
        }
    }
}

/// Lower a list of sub-expressions.
fn lower_children(
    exprs: &[PredicateExpr],
    schema: &TableSchema,
) -> Result<Vec<FilterExpr>, FilterError> {
    exprs
        .iter()
        .map(|e| lower_predicate_expr(e, schema))
        .collect()
}

/// Locate a column's declared CQL type STRING in the schema, searching the same
/// three lists, in the same order, that `producer::schema_columns` builds the
/// emitted column set from (partition keys, then clustering keys, then the
/// remaining columns).
///
/// ONE lookup, used by both the predicate lowering below and the issue #3742
/// projection admission above: a second membership rule could disagree with the
/// `retain` that actually narrows the emitted columns, which is precisely the
/// disagreement #3742 exists to remove.
fn find_column_type_str<'a>(schema: &'a TableSchema, column: &str) -> Option<&'a String> {
    schema
        .partition_keys
        .iter()
        .find(|c| c.name == column)
        .map(|c| &c.data_type)
        .or_else(|| {
            schema
                .clustering_keys
                .iter()
                .find(|c| c.name == column)
                .map(|c| &c.data_type)
        })
        .or_else(|| {
            schema
                .columns
                .iter()
                .find(|c| c.name == column)
                .map(|c| &c.data_type)
        })
}

/// Reject a ticket whose request would emit ZERO OUTPUT COLUMNS (issue #3742).
///
/// The predicate is **total output columns**, never "the `aggregates` list is
/// empty":
///
/// * With an aggregation, the output columns are `group_by` + `aggregates`
///   (`agg.rs::partial_columns`), and the projection does not contribute — so
///   the aggregation is rejected only when BOTH halves are empty.
///   `{"group_by": ["c"], "aggregates": []}` is a LIVE wire shape: Trino lowers
///   `SELECT DISTINCT c` to `groupingKeys=[c], aggregations={}` and the
///   connector emits it verbatim (`CqliteFlightMetadata.java:569`). It has one
///   output column and MUST be admitted.
/// * Without an aggregation, the output columns are the resolved projection —
///   `MergeProducer::with_spec` keeps the schema columns whose name appears in
///   `spec.projection` — so the projection is rejected exactly when no projected
///   name matches a schema column. `columns: null` (all columns) is unaffected.
///
/// A projection mixing known and unknown names still resolves to a non-empty
/// set and is admitted unchanged; only names in a projection that resolves to
/// NOTHING are reported, which is the state that used to be silently emptied.
fn admit_output_columns(ticket: &FlightTicket, schema: &TableSchema) -> Result<(), FilterError> {
    if let Some(aggregation) = &ticket.aggregation {
        if aggregation.group_by.is_empty() && aggregation.aggregates.is_empty() {
            return Err(FilterError::EmptyAggregation);
        }
        // The aggregation defines the output; the projection is not consulted.
        return Ok(());
    }

    let Some(projection) = &ticket.columns else {
        // `None` emits all columns (`ticket.rs:256`).
        return Ok(());
    };
    if projection.is_empty() {
        return Err(FilterError::EmptyProjection);
    }
    if projection
        .iter()
        .any(|name| find_column_type_str(schema, name).is_some())
    {
        return Ok(());
    }
    Err(FilterError::UnknownProjectionColumns(projection.clone()))
}

/// Resolve a column's CQL type from the schema (searches all column lists).
fn column_cql_type(schema: &TableSchema, column: &str) -> Result<CqlType, FilterError> {
    let type_str = find_column_type_str(schema, column)
        .ok_or_else(|| FilterError::UnknownColumn(column.to_string()))?;

    CqlType::parse(type_str).map_err(|source| FilterError::InvalidType {
        column: column.to_string(),
        type_str: type_str.clone(),
        source,
    })
}

/// Convert a ticket predicate to a core `SSTablePredicate`.
fn to_sstable_predicate(
    p: &Predicate,
    schema: &TableSchema,
) -> Result<SSTablePredicate, FilterError> {
    let cql = column_cql_type(schema, &p.column)?;
    let operation = match p.op {
        PredicateOp::Equal => SSTableFilterOp::Equal,
        PredicateOp::In => SSTableFilterOp::In,
        PredicateOp::Gt => SSTableFilterOp::Gt,
        PredicateOp::Gte => SSTableFilterOp::Gte,
        PredicateOp::Lt => SSTableFilterOp::Lt,
        PredicateOp::Lte => SSTableFilterOp::Lte,
        PredicateOp::Prefix => SSTableFilterOp::Prefix,
    };

    // `In` carries a JSON array of operands; everything else a single operand.
    let values = match (&p.op, &p.value) {
        (PredicateOp::In, serde_json::Value::Array(items)) => items
            .iter()
            .map(|v| json_to_value(v, &cql, &p.column))
            .collect::<Result<Vec<_>, _>>()?,
        (PredicateOp::In, _) => {
            return Err(FilterError::BadOperand {
                column: p.column.clone(),
                message: "IN requires a JSON array operand".into(),
            })
        }
        (_, v) => vec![json_to_value(v, &cql, &p.column)?],
    };

    if matches!(p.op, PredicateOp::In) && values.is_empty() {
        return Err(FilterError::BadOperand {
            column: p.column.clone(),
            message: "IN requires at least one value".into(),
        });
    }

    Ok(SSTablePredicate {
        column: p.column.clone(),
        operation,
        values,
        // Flight pushes only ordinary column predicates, never token() ranges
        // (Epic #951 / #955 added this field for the CQL token-range path).
        token_columns: None,
    })
}

/// Convert a JSON operand to a typed `Value` using the column's CQL type.
///
/// Supports the scalar types that make sense to push down from a SQL engine.
/// Numeric coercion in `evaluate_predicates` smooths over int/bigint/float
/// differences, so integers map to the natural width.
fn json_to_value(
    json: &serde_json::Value,
    cql: &CqlType,
    column: &str,
) -> Result<Value, FilterError> {
    let bad = |message: String| FilterError::BadOperand {
        column: column.to_string(),
        message,
    };

    if json.is_null() {
        return Err(bad("null is not a valid predicate operand".into()));
    }

    let unwrap_frozen = |t: &CqlType| -> CqlType {
        match t {
            CqlType::Frozen(inner) => (**inner).clone(),
            other => other.clone(),
        }
    };

    match unwrap_frozen(cql) {
        CqlType::TinyInt | CqlType::SmallInt | CqlType::Int => {
            let n = json
                .as_i64()
                .ok_or_else(|| bad(format!("expected integer, got {json}")))?;
            // Avoid silent wrap; numeric coercion in evaluate_predicates handles
            // comparison against the column's actual integer width.
            i32::try_from(n)
                .map(Value::Integer)
                .map_err(|_| bad(format!("integer {n} out of range for an int column")))
        }
        CqlType::BigInt | CqlType::Counter | CqlType::Timestamp => json
            .as_i64()
            .map(Value::BigInt)
            .ok_or_else(|| bad(format!("expected integer, got {json}"))),
        CqlType::Float | CqlType::Double => json
            .as_f64()
            .map(Value::Float)
            .ok_or_else(|| bad(format!("expected number, got {json}"))),
        CqlType::Boolean => json
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| bad(format!("expected boolean, got {json}"))),
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => json
            .as_str()
            .map(|s| Value::text(s.to_string()))
            .ok_or_else(|| bad(format!("expected string, got {json}"))),
        CqlType::Uuid | CqlType::TimeUuid => {
            let s = json
                .as_str()
                .ok_or_else(|| bad(format!("expected uuid string, got {json}")))?;
            let bytes = parse_uuid(s).ok_or_else(|| bad(format!("invalid uuid '{s}'")))?;
            Ok(Value::Uuid(bytes))
        }
        other => Err(bad(format!(
            "predicate pushdown unsupported for type {other:?}"
        ))),
    }
}

/// Parse a canonical hyphenated UUID string into 16 bytes.
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

/// Drift-guard probe (issue #2239): does the server's operand lowering
/// (`json_to_value`) accept ANY predicate operand for `cql`?
///
/// This drives the REAL `json_to_value` path (the sole authority for which
/// operands the server can compare) with a small set of canonical operands and
/// reports whether the type is comparable server-side at all. It exists so the
/// `producer.rs` capability guard can assert that every advertised non-`"none"`
/// capability corresponds to a type the server can actually lower — without
/// duplicating `json_to_value`'s type table. It is NOT the connector-encoder
/// frontier (that is a Java-side concern; see `PredicateTreeTranslator`).
#[cfg(test)]
pub(crate) fn capability_json_to_value_probe(cql: &CqlType) -> bool {
    // One operand of each JSON shape json_to_value can consume. A type is
    // server-comparable iff at least one lowers successfully.
    let operands = [
        serde_json::json!(1),
        serde_json::json!(1.5),
        serde_json::json!(true),
        serde_json::json!("11111111-1111-1111-1111-111111111111"),
    ];
    operands
        .iter()
        .any(|op| json_to_value(op, cql, "probe").is_ok())
}

#[cfg(test)]
#[path = "filter_tests.rs"]
mod tests;
