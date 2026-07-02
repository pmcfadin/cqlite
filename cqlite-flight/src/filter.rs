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
}

/// Token-range membership test for one split.
#[derive(Debug, Clone, Copy)]
pub struct TokenFilter {
    start: i64,
    end: i64,
    wraparound: bool,
}

impl TokenFilter {
    /// True if `token` falls in this split's `(start, end]` range.
    pub fn contains(&self, token: i64) -> bool {
        token_in_half_open_range(token, self.start, self.end, self.wraparound)
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
            (start, end) => Some(TokenFilter {
                start: start.unwrap_or(i64::MIN),
                end: end.unwrap_or(i64::MAX),
                wraparound: ticket.wraparound,
            }),
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

        let filter = ticket
            .effective_filter()
            .map(|expr| lower_predicate_expr(&expr, schema))
            .transpose()?;

        Ok(Self {
            token,
            filter,
            projection: ticket.columns.clone(),
        })
    }
}

/// Lower a ticket [`PredicateExpr`] into a typed [`FilterExpr`], resolving each
/// comparison/membership leaf's column type and parsing its JSON operand.
fn lower_predicate_expr(
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

/// Resolve a column's CQL type from the schema (searches all column lists).
fn column_cql_type(schema: &TableSchema, column: &str) -> Result<CqlType, FilterError> {
    let type_str = schema
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
            .map(|s| Value::Text(s.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{clustering_schema, simple_schema, uuid_schema};
    use serde_json::json;

    fn ticket_with(predicates: Vec<Predicate>) -> FlightTicket {
        FlightTicket {
            keyspace: "flight_ks".into(),
            table: "items".into(),
            predicates,
            ..Default::default()
        }
    }

    /// A v1 flat predicate list lowers to `And([Leaf, ...])`. Pull the resolved
    /// `SSTablePredicate` for leaf `i` so the type/operand assertions below read
    /// like the pre-#834 ones.
    fn lowered_leaf(spec: &ScanSpec, i: usize) -> &SSTablePredicate {
        match spec.filter.as_ref().expect("filter present") {
            FilterExpr::And(exprs) => match &exprs[i] {
                FilterExpr::Leaf(p) => p,
                other => panic!("expected leaf, got {other:?}"),
            },
            other => panic!("expected And, got {other:?}"),
        }
    }

    /// Build a row with one named column value for evaluator tests.
    fn row_with(column: &str, value: Value) -> QueryRow {
        let mut values: std::collections::HashMap<std::sync::Arc<str>, Value> =
            std::collections::HashMap::new();
        values.insert(column.into(), value);
        QueryRow {
            values,
            key: cqlite_core::RowKey(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }

    fn empty_row() -> QueryRow {
        QueryRow {
            values: std::collections::HashMap::new(),
            key: cqlite_core::RowKey(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }

    fn score_gt(n: i64) -> FilterExpr {
        FilterExpr::Leaf(SSTablePredicate {
            column: "score".into(),
            operation: SSTableFilterOp::Gt,
            values: vec![Value::Integer(n as i32)],
            token_columns: None,
        })
    }

    #[test]
    fn token_filter_built_from_bounds() {
        let mut t = ticket_with(vec![]);
        t.token_start = Some(-10);
        t.token_end = Some(10);
        let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
        let tf = spec.token.expect("token filter present");
        assert!(!tf.contains(-10), "start exclusive");
        assert!(tf.contains(0));
        assert!(tf.contains(10), "end inclusive");
        assert!(!tf.contains(11));
    }

    #[test]
    fn overlaps_non_wraparound_boundaries() {
        // Split range (0, 100].
        let tf = TokenFilter {
            start: 0,
            end: 100,
            wraparound: false,
        };
        // Span entirely inside.
        assert!(tf.overlaps(10, 50));
        // Span straddling the start (max past start, min before start).
        assert!(tf.overlaps(-10, 10));
        // Span entirely below: max_token == start is NOT past the exclusive start.
        assert!(
            !tf.overlaps(-50, 0),
            "max_token==start excluded (half-open)"
        );
        assert!(!tf.overlaps(-50, -1));
        // Span entirely above: min_token == end is inclusive (overlaps).
        assert!(tf.overlaps(100, 200), "min_token==end included");
        assert!(!tf.overlaps(101, 200), "min_token>end excluded");
        // Span covering the whole range.
        assert!(tf.overlaps(i64::MIN, i64::MAX));
    }

    #[test]
    fn overlaps_wraparound_boundaries() {
        // Wraparound range (100, -100] = (100, MAX] ∪ [MIN, -100].
        let tf = TokenFilter {
            start: 100,
            end: -100,
            wraparound: true,
        };
        // Span in the high arm (past start).
        assert!(tf.overlaps(150, 200));
        // Span in the low arm (at or below end).
        assert!(tf.overlaps(-300, -200));
        assert!(tf.overlaps(-300, -100), "max_token reaching end inclusive");
        // Span entirely in the excluded middle gap (-99, 100].
        assert!(!tf.overlaps(-50, 50), "middle gap excluded");
        assert!(
            !tf.overlaps(0, 100),
            "max_token==start not past exclusive start"
        );
        // Span touching only the inclusive low end.
        assert!(tf.overlaps(-100, -100));
    }

    #[test]
    fn no_bounds_means_no_token_filter() {
        let spec = ScanSpec::from_ticket(&ticket_with(vec![]), &simple_schema()).unwrap();
        assert!(spec.token.is_none());
    }

    #[test]
    fn int_predicate_translates_with_natural_width() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Gt,
            value: json!(10),
        }]);
        let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
        let leaf = lowered_leaf(&spec, 0);
        assert_eq!(leaf.column, "score");
        assert!(matches!(leaf.operation, SSTableFilterOp::Gt));
        assert_eq!(leaf.values, vec![Value::Integer(10)]);
    }

    #[test]
    fn in_predicate_expands_json_array() {
        let t = ticket_with(vec![Predicate {
            column: "name".into(),
            op: PredicateOp::In,
            value: json!(["a", "b"]),
        }]);
        let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
        assert_eq!(
            lowered_leaf(&spec, 0).values,
            vec![Value::Text("a".into()), Value::Text("b".into())]
        );
    }

    #[test]
    fn uuid_predicate_parses_to_bytes() {
        let t = FlightTicket {
            keyspace: "flight_ks".into(),
            table: "uu".into(),
            predicates: vec![Predicate {
                column: "id".into(),
                op: PredicateOp::Equal,
                value: json!("00000000-0000-0000-0000-000000000001"),
            }],
            ..Default::default()
        };
        let spec = ScanSpec::from_ticket(&t, &uuid_schema()).unwrap();
        let mut expected = [0u8; 16];
        expected[15] = 1;
        assert_eq!(lowered_leaf(&spec, 0).values, vec![Value::Uuid(expected)]);
    }

    #[test]
    fn predicate_on_clustering_column_resolves_type() {
        let t = FlightTicket {
            keyspace: "flight_ks".into(),
            table: "wide".into(),
            predicates: vec![Predicate {
                column: "ck".into(),
                op: PredicateOp::Equal,
                value: json!("a"),
            }],
            ..Default::default()
        };
        let spec = ScanSpec::from_ticket(&t, &clustering_schema()).unwrap();
        assert_eq!(lowered_leaf(&spec, 0).values, vec![Value::Text("a".into())]);
    }

    #[test]
    fn unknown_column_is_rejected() {
        let t = ticket_with(vec![Predicate {
            column: "nope".into(),
            op: PredicateOp::Equal,
            value: json!(1),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::UnknownColumn(c) if c == "nope"));
    }

    #[test]
    fn type_mismatch_is_rejected() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Equal,
            value: json!("not a number"),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::BadOperand { .. }));
    }

    #[test]
    fn empty_in_is_rejected() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::In,
            value: json!([]),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::BadOperand { .. }));
    }

    #[test]
    fn v1_in_with_non_array_operand_is_rejected() {
        // A v1 flat IN predicate must carry a JSON array; a scalar operand is a
        // malformed legacy ticket and must error (not be folded into a singleton).
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::In,
            value: json!(5),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::BadOperand { .. }));
    }

    #[test]
    fn null_operand_is_rejected() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Equal,
            value: serde_json::Value::Null,
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::BadOperand { .. }));
    }

    #[test]
    fn out_of_range_int_is_rejected_not_truncated() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Equal,
            value: json!(i64::from(i32::MAX) + 1),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(
            matches!(err, FilterError::BadOperand { .. }),
            "must error, not wrap"
        );
    }

    // ---- Issue #834: Kleene three-valued evaluator ----

    /// A present, comparable leaf yields True/False; a missing or Null column
    /// yields Unknown (the SQL UNKNOWN that NOT/OR must propagate).
    #[test]
    fn leaf_null_or_missing_is_unknown() {
        let p = score_gt(10);
        assert_eq!(
            p.evaluate(&row_with("score", Value::Integer(20))),
            Kleene::True
        );
        assert_eq!(
            p.evaluate(&row_with("score", Value::Integer(5))),
            Kleene::False
        );
        assert_eq!(
            p.evaluate(&empty_row()),
            Kleene::Unknown,
            "missing → Unknown"
        );
        assert_eq!(
            p.evaluate(&row_with("score", Value::Null)),
            Kleene::Unknown,
            "Null cell → Unknown"
        );
    }

    /// `IS NULL` is always definite: True for absent/Null, False otherwise —
    /// never Unknown.
    #[test]
    fn is_null_is_definite() {
        let expr = FilterExpr::IsNull("score".into());
        assert_eq!(expr.evaluate(&empty_row()), Kleene::True, "absent IS NULL");
        assert_eq!(
            expr.evaluate(&row_with("score", Value::Null)),
            Kleene::True,
            "Null IS NULL"
        );
        assert_eq!(
            expr.evaluate(&row_with("score", Value::Integer(1))),
            Kleene::False,
            "present is not null"
        );
    }

    /// `NOT` flips True/False and leaves Unknown unchanged.
    #[test]
    fn not_truth_table_propagates_unknown() {
        let not = |k_row: QueryRow| FilterExpr::Not(Box::new(score_gt(10))).evaluate(&k_row);
        assert_eq!(not(row_with("score", Value::Integer(20))), Kleene::False);
        assert_eq!(not(row_with("score", Value::Integer(5))), Kleene::True);
        assert_eq!(not(empty_row()), Kleene::Unknown, "NOT Unknown = Unknown");
    }

    /// `AND` truth table including Unknown propagation; empty AND is True.
    #[test]
    fn and_truth_table() {
        let t = || score_gt(10); // True for score=20
        let f = || {
            FilterExpr::Leaf(SSTablePredicate {
                column: "score".into(),
                operation: SSTableFilterOp::Lt,
                values: vec![Value::Integer(0)],
                token_columns: None,
            })
        }; // False for score=20
        let u = || FilterExpr::Leaf(score_pred_on_missing()); // Unknown
        let row = row_with("score", Value::Integer(20));

        assert_eq!(
            FilterExpr::And(vec![]).evaluate(&row),
            Kleene::True,
            "empty AND"
        );
        assert_eq!(FilterExpr::And(vec![t(), t()]).evaluate(&row), Kleene::True);
        assert_eq!(
            FilterExpr::And(vec![t(), f()]).evaluate(&row),
            Kleene::False
        );
        // Any False dominates, even with an Unknown present.
        assert_eq!(
            FilterExpr::And(vec![u(), f()]).evaluate(&row),
            Kleene::False
        );
        // Unknown with no False → Unknown.
        assert_eq!(
            FilterExpr::And(vec![t(), u()]).evaluate(&row),
            Kleene::Unknown
        );
    }

    /// `OR` truth table including Unknown propagation; empty OR is False.
    #[test]
    fn or_truth_table() {
        let t = || score_gt(10);
        let f = || {
            FilterExpr::Leaf(SSTablePredicate {
                column: "score".into(),
                operation: SSTableFilterOp::Lt,
                values: vec![Value::Integer(0)],
                token_columns: None,
            })
        };
        let u = || FilterExpr::Leaf(score_pred_on_missing());
        let row = row_with("score", Value::Integer(20));

        assert_eq!(
            FilterExpr::Or(vec![]).evaluate(&row),
            Kleene::False,
            "empty OR"
        );
        assert_eq!(FilterExpr::Or(vec![f(), f()]).evaluate(&row), Kleene::False);
        assert_eq!(FilterExpr::Or(vec![f(), t()]).evaluate(&row), Kleene::True);
        // Any True dominates, even with an Unknown present.
        assert_eq!(FilterExpr::Or(vec![u(), t()]).evaluate(&row), Kleene::True);
        // Unknown with no True → Unknown.
        assert_eq!(
            FilterExpr::Or(vec![f(), u()]).evaluate(&row),
            Kleene::Unknown
        );
    }

    /// `keeps` is True-only: Unknown and False both reject (WHERE semantics).
    #[test]
    fn keeps_only_when_true() {
        let p = score_gt(10);
        assert!(p.keeps(&row_with("score", Value::Integer(20))));
        assert!(
            !p.keeps(&row_with("score", Value::Integer(5))),
            "False rejects"
        );
        assert!(!p.keeps(&empty_row()), "Unknown rejects");
    }

    /// A leaf that always evaluates Unknown because it tests a column the row
    /// never has — used to drive the Unknown cells of the truth tables.
    fn score_pred_on_missing() -> SSTablePredicate {
        SSTablePredicate {
            column: "absent_column".into(),
            operation: SSTableFilterOp::Gt,
            values: vec![Value::Integer(0)],
            token_columns: None,
        }
    }
}
