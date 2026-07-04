//! Output/column name derivation for SELECT expressions.
//!
//! Extracted from the optimizer so result-metadata naming, the aggregation
//! plan's row-value keys, and scan-projection column extraction all share ONE
//! set of pure functions and cannot disagree (issue #1763). The invariant this
//! module upholds: an aggregate's `metadata.columns[i].name` (via
//! [`result_column_name`]) equals the row value key that `finalize_group` emits
//! (via the aggregation plan's alias, derived from [`aggregate_output_name`]) —
//! an explicit alias when present, else the derived expression text, never a
//! synthetic `col_N`.

use super::select_ast::*;

/// The scan-projection column name for a SELECT expression, or `None` when the
/// expression does not name a stored column (aggregates, literals, arithmetic).
///
/// Issue #1763: an aliased aggregate (`COUNT(*) AS total`) is NOT a stored
/// column — it is excluded exactly as a bare aggregate is, so the alias never
/// enters the scan as a phantom column; its output name comes from the
/// aggregation plan, not a projected cell.
pub(crate) fn extract_column_name(expr: &SelectExpression) -> Option<String> {
    match expr {
        SelectExpression::Column(col_ref) => Some(col_ref.column.clone()),
        SelectExpression::Aliased(inner, _) if inner.is_aggregate() => None,
        SelectExpression::Aliased(_, alias) => Some(alias.clone()),
        _ => None,
    }
}

/// Borrow the `AggregateFunction` from an aggregate expression, unwrapping a
/// surrounding `Aliased` (`COUNT(*) AS total`). Returns `None` for non-aggregate
/// expressions.
pub(crate) fn unwrap_aggregate(expr: &SelectExpression) -> Option<&AggregateFunction> {
    match expr {
        SelectExpression::Aggregate(agg) => Some(agg),
        SelectExpression::Aliased(inner, _) => unwrap_aggregate(inner),
        _ => None,
    }
}

/// The output column name for a (possibly aliased) aggregate SELECT expression,
/// or `None` if `expr` is not an aggregate.
///
/// Issue #1763: the SINGLE source of truth for aggregate output names, shared by
/// the aggregation plan (which keys row values via `finalize_group`) and result-
/// metadata construction ([`result_column_name`]). Deriving both from this one
/// function guarantees `metadata.columns[i].name` equals the emitted row value
/// key for aggregates — an explicit alias when present, else the derived
/// expression text (never a synthetic `col_N`).
pub(crate) fn aggregate_output_name(expr: &SelectExpression) -> Option<String> {
    match expr {
        SelectExpression::Aggregate(agg) => Some(aggregate_column_and_alias(agg).1),
        SelectExpression::Aliased(inner, alias) if inner.is_aggregate() => Some(alias.clone()),
        _ => None,
    }
}

/// The result (output) column name for a projected SELECT expression at position
/// `index`, as it appears in `metadata.columns`.
///
/// Issue #1763: an aggregate is named by [`aggregate_output_name`] — the SAME
/// source that keys the emitted row values (`finalize_group` via the aggregation
/// plan alias) — so result metadata can never disagree with aggregate row value
/// keys. Non-aggregate expressions use the column name, an explicit alias, or the
/// synthetic `col_{index}` fallback. `WriteTimeTtl` is intentionally NOT handled
/// here — the caller names those before reaching this function.
pub(crate) fn result_column_name(expr: &SelectExpression, index: usize) -> String {
    if let Some(name) = aggregate_output_name(expr) {
        return name;
    }
    match expr {
        SelectExpression::Column(col_ref) => col_ref.column.clone(),
        SelectExpression::Aliased(_, alias) => alias.clone(),
        _ => format!("col_{index}"),
    }
}

/// Resolve `(column, alias)` for an aggregate. `COUNT(*)` and any aggregate
/// referencing `*` yields `("*", "Func(*)")`; a single named column yields
/// `(name, "Func_name")`; anything else falls back to `("*", "Func")`.
pub(crate) fn aggregate_column_and_alias(agg: &AggregateFunction) -> (String, String) {
    let references_star = agg.args.is_empty()
        || agg
            .args
            .iter()
            .any(|arg| matches!(arg, SelectExpression::Column(c) if c.column == "*"));

    if references_star {
        return ("*".to_string(), format!("{:?}(*)", agg.function));
    }

    match agg.args.first().and_then(extract_column_name) {
        Some(col_name) => {
            let alias = format!("{:?}_{}", agg.function, col_name);
            (col_name, alias)
        }
        None => ("*".to_string(), format!("{:?}", agg.function)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> SelectExpression {
        SelectExpression::Column(ColumnRef {
            table: None,
            column: name.to_string(),
        })
    }

    fn count_star() -> SelectExpression {
        SelectExpression::Aggregate(AggregateFunction {
            function: AggregateType::Count,
            args: vec![],
            distinct: false,
        })
    }

    fn sum_of(c: &str) -> SelectExpression {
        SelectExpression::Aggregate(AggregateFunction {
            function: AggregateType::Sum,
            args: vec![col(c)],
            distinct: false,
        })
    }

    /// The output name a bare aggregate exposes in metadata MUST equal the alias
    /// the aggregation plan keys its row value by — the derived expression text,
    /// never `col_N` (issue #1763).
    #[test]
    fn unaliased_aggregate_name_is_expression_text() {
        assert_eq!(result_column_name(&count_star(), 0), "Count(*)");
        assert_eq!(
            aggregate_output_name(&count_star()).as_deref(),
            Some("Count(*)")
        );
        // Metadata name == aggregation-plan alias (the single-source invariant).
        assert_eq!(
            result_column_name(&count_star(), 0),
            aggregate_column_and_alias(unwrap_aggregate(&count_star()).unwrap()).1
        );
        assert_eq!(result_column_name(&sum_of("value"), 3), "Sum_value");
    }

    /// An explicit alias wins for BOTH metadata naming and the plan's row-value
    /// key, so the two remain identical.
    #[test]
    fn aliased_aggregate_name_is_the_alias() {
        let aliased = SelectExpression::Aliased(Box::new(count_star()), "total".to_string());
        assert_eq!(result_column_name(&aliased, 0), "total");
        assert_eq!(aggregate_output_name(&aliased).as_deref(), Some("total"));
        // Unwraps to the inner aggregate for accumulation.
        assert!(unwrap_aggregate(&aliased).is_some());
    }

    /// An aliased aggregate is never emitted as a scan-projection column (it is
    /// not a stored cell), matching the bare-aggregate exclusion.
    #[test]
    fn aliased_aggregate_is_not_a_projection_column() {
        let aliased = SelectExpression::Aliased(Box::new(count_star()), "total".to_string());
        assert_eq!(extract_column_name(&aliased), None);
        assert_eq!(extract_column_name(&count_star()), None);
        // A plain column and a plain-aliased column still project normally.
        assert_eq!(extract_column_name(&col("name")).as_deref(), Some("name"));
        let aliased_col = SelectExpression::Aliased(Box::new(col("name")), "n".to_string());
        assert_eq!(extract_column_name(&aliased_col).as_deref(), Some("n"));
    }

    /// A non-aggregate expression is not an aggregate name and falls back to the
    /// synthetic positional name only when it is neither a column nor aliased.
    #[test]
    fn non_aggregate_naming_and_fallback() {
        assert_eq!(aggregate_output_name(&col("x")), None);
        assert_eq!(result_column_name(&col("x"), 2), "x");
        assert_eq!(
            result_column_name(
                &SelectExpression::Literal(crate::types::Value::Integer(1)),
                5
            ),
            "col_5"
        );
    }
}
