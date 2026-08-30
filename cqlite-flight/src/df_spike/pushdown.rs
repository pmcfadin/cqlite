//! DataFusion `Expr` → CQLite predicate translation (issue #2605).
//!
//! # The correctness rule this module exists to enforce
//!
//! A `TableProvider` that reports [`TableProviderFilterPushDown::Exact`] tells
//! DataFusion "I applied this predicate completely; do not re-check it". If the
//! provider then fails to apply it, rows that should have been filtered out are
//! returned — and in a BENCHMARK that shows up as the DataFusion arm being
//! *faster*, because it is wrong. So the translation is FAIL-CLOSED in one
//! direction only: anything not translated to a predicate the existing scan path
//! genuinely evaluates is reported [`TableProviderFilterPushDown::Unsupported`]
//! and left for DataFusion's own `FilterExec`.
//!
//! [`classify`] and [`translate_all`] share ONE translation function, so the
//! answer `supports_filters_pushdown` gives can never disagree with what `scan`
//! actually pushes.
//!
//! # Why the ticket-level `PredicateExpr` is the target, not `FilterExpr`
//!
//! Translation stops at the PUBLIC ticket shape (`crate::ticket::PredicateExpr`)
//! and hands it to production's own `crate::filter` lowering, which resolves each
//! column's `CqlType` from the schema and parses the operand against it. So the
//! spike inherits the real type resolution and the real Kleene evaluation
//! semantics instead of re-deriving them — a second implementation of operand
//! coercion is exactly the kind of thing that would make the two arms disagree.
//! A lowering failure (unknown column, un-coercible operand) is itself a
//! translation failure: [`Unsupported`](TableProviderFilterPushDown::Unsupported).
//!
//! # Kleene equivalence
//!
//! `FilterExpr::keeps` retains a row iff the tree evaluates to `True`; `False`
//! and `Unknown` both reject. That is SQL `WHERE` semantics, which is what
//! DataFusion expects of an `Exact` filter — so a translated comparison,
//! `AND`/`OR`/`NOT` combination, `IS [NOT] NULL`, or `IN` list keeps SQL meaning.

use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;

use cqlite_core::schema::TableSchema;

use crate::filter::FilterExpr;
use crate::ticket::{PredicateExpr, PredicateOp};

/// Classify each filter DataFusion offered: `Exact` for the ones this provider
/// will genuinely evaluate, `Unsupported` for everything else.
pub(crate) fn classify(
    filters: &[&Expr],
    schema: &TableSchema,
) -> Vec<TableProviderFilterPushDown> {
    filters
        .iter()
        .map(|expr| match translate(expr, schema) {
            Some(_) => TableProviderFilterPushDown::Exact,
            None => TableProviderFilterPushDown::Unsupported,
        })
        .collect()
}

/// Translate every filter that CAN be pushed into one conjunctive
/// [`PredicateExpr`], dropping (leaving to DataFusion) the ones that cannot.
///
/// Returns `None` when nothing was translatable, so the caller pushes no filter
/// at all rather than an empty `And` (which is `TRUE` and would merely add work).
pub(crate) fn translate_all(filters: &[Expr], schema: &TableSchema) -> Option<PredicateExpr> {
    let translated: Vec<PredicateExpr> = filters
        .iter()
        .filter_map(|e| translate(e, schema))
        .collect();
    match translated.len() {
        0 => None,
        1 => translated.into_iter().next(),
        _ => Some(PredicateExpr::And { exprs: translated }),
    }
}

/// Translate one DataFusion expression, verifying it also LOWERS against
/// `schema` — the check that makes an `Exact` claim honest.
fn translate(expr: &Expr, schema: &TableSchema) -> Option<PredicateExpr> {
    let candidate = to_predicate_expr(expr)?;
    // Fail closed on anything production's own lowering rejects: an unknown
    // column, or an operand that will not coerce to the column's CQL type.
    crate::filter::lower_predicate_expr(&candidate, schema).ok()?;
    Some(candidate)
}

/// Lower an already-validated candidate to the typed predicate tree. Separate
/// from [`translate`] so the caller can build the final `ScanSpec.filter`
/// through the SAME production lowering rather than a second code path.
pub(crate) fn lower(
    candidate: &PredicateExpr,
    schema: &TableSchema,
) -> Result<FilterExpr, crate::filter::FilterError> {
    crate::filter::lower_predicate_expr(candidate, schema)
}

/// Structural translation, with no schema knowledge. `None` means "not
/// representable in the existing scan path".
fn to_predicate_expr(expr: &Expr) -> Option<PredicateExpr> {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And => Some(PredicateExpr::And {
                exprs: vec![
                    to_predicate_expr(&binary.left)?,
                    to_predicate_expr(&binary.right)?,
                ],
            }),
            Operator::Or => Some(PredicateExpr::Or {
                exprs: vec![
                    to_predicate_expr(&binary.left)?,
                    to_predicate_expr(&binary.right)?,
                ],
            }),
            op => comparison(&binary.left, op, &binary.right),
        },
        Expr::Not(inner) => Some(PredicateExpr::Not {
            expr: Box::new(to_predicate_expr(inner)?),
        }),
        Expr::IsNull(inner) => Some(PredicateExpr::IsNull {
            column: column_name(inner)?,
        }),
        Expr::IsNotNull(inner) => Some(PredicateExpr::Not {
            expr: Box::new(PredicateExpr::IsNull {
                column: column_name(inner)?,
            }),
        }),
        Expr::InList(in_list) => {
            let column = column_name(&in_list.expr)?;
            let values: Option<Vec<serde_json::Value>> =
                in_list.list.iter().map(literal_json).collect();
            let membership = PredicateExpr::In {
                column,
                values: values?,
            };
            // An empty `IN ()` list is rejected by production lowering; treat a
            // negated list as `NOT IN`.
            if in_list.negated {
                Some(PredicateExpr::Not {
                    expr: Box::new(membership),
                })
            } else {
                Some(membership)
            }
        }
        Expr::Between(between) => {
            let column = column_name(&between.expr)?;
            let low = PredicateExpr::Compare {
                column: column.clone(),
                op: PredicateOp::Gte,
                value: literal_json(&between.low)?,
            };
            let high = PredicateExpr::Compare {
                column,
                op: PredicateOp::Lte,
                value: literal_json(&between.high)?,
            };
            let range = PredicateExpr::And {
                exprs: vec![low, high],
            };
            if between.negated {
                Some(PredicateExpr::Not {
                    expr: Box::new(range),
                })
            } else {
                Some(range)
            }
        }
        // Cast/alias wrappers are deliberately NOT unwrapped: a cast can change
        // comparison semantics (widening, timezone, string↔numeric), and the
        // spike has no oracle proving CQLite's coercion matches DataFusion's.
        // Leaving them to DataFusion's `FilterExec` is the fail-closed answer.
        _ => None,
    }
}

/// `column op literal` (or the mirrored `literal op column`).
fn comparison(left: &Expr, op: Operator, right: &Expr) -> Option<PredicateExpr> {
    // Try `column op literal` first, then the mirrored form with the operator
    // flipped — `5 < ck` is `ck > 5`, not `ck < 5`.
    if let (Some(column), Some(value)) = (column_name(left), literal_json(right)) {
        return compare_node(column, op, value);
    }
    if let (Some(value), Some(column)) = (literal_json(left), column_name(right)) {
        return compare_node(column, mirror(op)?, value);
    }
    None
}

/// Build the comparison node for a supported operator.
fn compare_node(column: String, op: Operator, value: serde_json::Value) -> Option<PredicateExpr> {
    let predicate_op = match op {
        Operator::Eq => PredicateOp::Equal,
        Operator::Lt => PredicateOp::Lt,
        Operator::LtEq => PredicateOp::Lte,
        Operator::Gt => PredicateOp::Gt,
        Operator::GtEq => PredicateOp::Gte,
        // `a <> v` is `NOT (a = v)`. Under Kleene a NULL `a` yields `Unknown`,
        // `Not(Unknown)` is `Unknown`, and the row is rejected — which is SQL's
        // answer too, so `Exact` stays honest.
        Operator::NotEq => {
            return Some(PredicateExpr::Not {
                expr: Box::new(PredicateExpr::Compare {
                    column,
                    op: PredicateOp::Equal,
                    value,
                }),
            })
        }
        _ => return None,
    };
    Some(PredicateExpr::Compare {
        column,
        op: predicate_op,
        value,
    })
}

/// Flip a comparison operator for the `literal op column` operand order.
fn mirror(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::NotEq => Some(Operator::NotEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

/// The bare column name, for an unqualified/qualified column reference only.
fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

/// A literal's JSON form, for the operand shapes `crate::filter`'s
/// `json_to_value` resolves against a CQL type.
///
/// Deliberately NARROW. Every type whose JSON spelling CQLite and DataFusion
/// might disagree about (timestamps, dates, intervals, decimals, dictionaries,
/// nested values) returns `None` and is left to DataFusion — an `Exact` claim on
/// an operand whose coercion is unproven is exactly the silent-wrong-rows hazard
/// this module is built to avoid.
fn literal_json(expr: &Expr) -> Option<serde_json::Value> {
    let Expr::Literal(scalar) = expr else {
        return None;
    };
    match scalar {
        ScalarValue::Boolean(Some(v)) => Some(serde_json::Value::Bool(*v)),
        ScalarValue::Int8(Some(v)) => Some(json_i64(i64::from(*v))),
        ScalarValue::Int16(Some(v)) => Some(json_i64(i64::from(*v))),
        ScalarValue::Int32(Some(v)) => Some(json_i64(i64::from(*v))),
        ScalarValue::Int64(Some(v)) => Some(json_i64(*v)),
        ScalarValue::UInt8(Some(v)) => Some(json_i64(i64::from(*v))),
        ScalarValue::UInt16(Some(v)) => Some(json_i64(i64::from(*v))),
        ScalarValue::UInt32(Some(v)) => Some(json_i64(i64::from(*v))),
        // A `u64` above `i64::MAX` has no lossless `i64` form; refuse rather
        // than wrap into a negative operand.
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok().map(json_i64),
        ScalarValue::Float32(Some(v)) => json_f64(f64::from(*v)),
        ScalarValue::Float64(Some(v)) => json_f64(*v),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
            Some(serde_json::Value::String(v.clone()))
        }
        // A NULL literal never makes a comparison true in SQL, but expressing
        // that here would require asserting CQLite's `Unknown` handling for a
        // NULL OPERAND (not a null column), which is untested. Refuse.
        _ => None,
    }
}

/// JSON integer.
fn json_i64(v: i64) -> serde_json::Value {
    serde_json::Value::Number(serde_json::Number::from(v))
}

/// JSON float; non-finite values have no JSON number form, so they are refused.
fn json_f64(v: f64) -> Option<serde_json::Value> {
    serde_json::Number::from_f64(v).map(serde_json::Value::Number)
}
