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
use crate::schema::CqlType;

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

/// For a plain-column SELECT projection (possibly aliased), return
/// `(source_column, output_name)` where `source_column` is the stored column the
/// scan reads and `output_name` is what appears in `metadata.columns`.
///
/// Issue #1763 (grouped dimensions): `finalize_group` keys a grouped dimension's
/// row VALUE by its SELECT OUTPUT name so it matches the metadata name, while the
/// group KEY is still read from the row by the raw stored column. This pairs the
/// two: `Column(x)` → `(x, x)`, `Aliased(Column(x), "a")` → `(x, "a")`. The
/// output side is exactly what [`extract_column_name`] returns, so a grouped
/// dimension's row key can never diverge from its metadata name. Returns `None`
/// for aggregates, literals, arithmetic, etc.
pub(crate) fn projection_source_and_output(expr: &SelectExpression) -> Option<(String, String)> {
    match expr {
        SelectExpression::Column(col_ref) => Some((col_ref.column.clone(), col_ref.column.clone())),
        SelectExpression::Aliased(inner, alias) => match inner.as_ref() {
            SelectExpression::Column(col_ref) => Some((col_ref.column.clone(), alias.clone())),
            _ => None,
        },
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

/// Source (stored) column names referenced by an aggregate's ARGUMENT
/// expressions, excluding the `*` wildcard.
///
/// Issue #1952: the SSTable scan projection MUST include these so grouped,
/// non-star aggregates read their input cells. `build_row_from_scan` filters
/// decoded cells by the scan projection; without the argument column a grouped
/// query that also projects a group dimension (`SELECT category, SUM(value) ...
/// GROUP BY category`) scans ONLY the dimension and silently aggregates from
/// missing inputs (SUM/AVG → 0/null, COUNT(col) → 0, MIN/MAX → null). Returns an
/// empty vec for non-aggregate expressions and for `COUNT(*)` (no argument
/// column). The SOURCE column is read directly (never an alias), matching the
/// column the aggregation plan accumulates via [`aggregate_column_and_alias`].
pub(crate) fn aggregate_arg_source_columns(expr: &SelectExpression) -> Vec<String> {
    let Some(agg) = unwrap_aggregate(expr) else {
        return Vec::new();
    };
    agg.args
        .iter()
        .filter_map(|arg| match arg {
            SelectExpression::Column(col_ref) if col_ref.column != "*" => {
                Some(col_ref.column.clone())
            }
            _ => None,
        })
        .collect()
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

/// Derive the CQL result type of an aggregate from the aggregate FUNCTION and
/// its argument's schema type — never from a schema lookup of the aggregate's
/// output name/alias (issue #1941). The name-lookup approach mis-typed
/// `COUNT(*) AS value` as the `value` column's type and fell back to `Text` for
/// any aggregate whose derived name matched no column.
///
/// Oracle — Cassandra's aggregate type rules, reconciled with the value type
/// CQLite's executor actually emits in `finalize_group`
/// (`select_executor::aggregation`):
///   * `COUNT(*)` / `COUNT(col)` → `bigint` (emitted `Value::BigInt`)
///   * `SUM(_)` / `AVG(_)`       → the RESULT type Cassandra returns for the
///     argument (issue #2202, `AggregateFcts.java`): every INTEGRAL argument
///     (`tinyint`/`smallint`/`int`/`bigint`/`counter`) returns THE SAME type
///     back — Cassandra does NOT promote `tinyint`/`smallint` to `int`; every
///     other numeric argument — `float`, `double`, `decimal`, `varint`, or an
///     unknown/unresolved argument — → `double`. The executor accumulates
///     integrally (`i64`, Cassandra wrapping, narrowed to the argument's own
///     width at finalize) for the integral cases and in `f64` otherwise, and
///     `finalize_group` emits the matching `Value` variant, so this metadata
///     type never lies about the produced value (the #1941 invariant).
///   * `MIN(col)` / `MAX(col)`   → the argument column's type (the value is
///     cloned through unchanged)
///
/// `arg_type` is the pre-resolved CQL type of the aggregate's argument column
/// (from the schema); it drives the SUM/AVG promotion above and the MIN/MAX
/// passthrough. Returns `None` solely for a MIN/MAX whose argument type is
/// unknown (no schema, or the argument is not a resolvable column), leaving the
/// caller's existing untyped fallback — SUM/AVG always resolve to a concrete
/// type (`double` when the argument is unknown).
pub(crate) fn aggregate_result_cql_type(
    function: &AggregateType,
    arg_type: Option<CqlType>,
) -> Option<CqlType> {
    match function {
        AggregateType::Count => Some(CqlType::BigInt),
        // Issue #2202: preserve Cassandra's integral SUM/AVG result types instead
        // of collapsing every numeric input to `double`.
        AggregateType::Sum | AggregateType::Avg => Some(sum_avg_result_cql_type(arg_type)),
        AggregateType::Min | AggregateType::Max => arg_type,
    }
}

/// Cassandra's SUM/AVG result-type rule (issue #2202, verified against
/// `AggregateFcts.java`): EVERY integral argument type returns THE SAME type —
/// `tinyint`→`tinyint`, `smallint`→`smallint`, `int`→`int`, `bigint`/`counter`→
/// `bigint`. Cassandra does NOT promote the narrow integral types to `int`
/// (its own docs warn of overflow risk precisely because the result stays the
/// input's narrow width). Every other numeric argument (`float`, `double`,
/// `decimal`, `varint`) — plus an unknown argument — yields `double`,
/// preserving CQLite's prior float behaviour with no regression. This is the
/// SINGLE source of truth the executor's accumulator
/// (`init_aggregate_accumulators`) also consults, so the emitted value variant
/// and the result metadata type can never disagree.
pub(crate) fn sum_avg_result_cql_type(arg_type: Option<CqlType>) -> CqlType {
    match arg_type {
        Some(t @ (CqlType::TinyInt | CqlType::SmallInt | CqlType::Int | CqlType::BigInt)) => t,
        Some(CqlType::Counter) => CqlType::BigInt,
        _ => CqlType::Double,
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

    /// Issue #1952: aggregate argument source columns are extracted for the scan
    /// projection — the SOURCE column for a named aggregate, nothing for
    /// `COUNT(*)` or a non-aggregate, so the scan never drops an aggregate input.
    #[test]
    fn aggregate_arg_source_columns_extracts_named_argument_only() {
        assert_eq!(
            aggregate_arg_source_columns(&sum_of("value")),
            vec!["value"]
        );
        // COUNT(*) references no stored column.
        assert!(aggregate_arg_source_columns(&count_star()).is_empty());
        // Aliased aggregate still yields its inner argument's source column.
        let aliased = SelectExpression::Aliased(Box::new(sum_of("value")), "s".to_string());
        assert_eq!(aggregate_arg_source_columns(&aliased), vec!["value"]);
        // Plain columns / non-aggregates contribute no aggregate-argument columns.
        assert!(aggregate_arg_source_columns(&col("value")).is_empty());
    }

    /// Issue #1941/#2202: aggregate result type comes from the function (+
    /// argument type), never a name lookup. COUNT → bigint; SUM/AVG return the
    /// SAME integral type back for every integral argument (Cassandra does NOT
    /// promote tinyint/smallint to int — verified against `AggregateFcts.java`)
    /// and fall back to double for float/double/unknown; MIN/MAX preserve the
    /// argument type and are the ONLY variants that return `None` when the
    /// argument type is unknown.
    #[test]
    fn aggregate_result_cql_type_derives_from_function_and_argument() {
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Count, Some(CqlType::Int)),
            Some(CqlType::BigInt),
            "COUNT is bigint regardless of any argument type"
        );
        // Issue #2202: SUM/AVG preserve Cassandra's narrow integral result types —
        // NO promotion to int for tinyint/smallint.
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Sum, Some(CqlType::Int)),
            Some(CqlType::Int),
            "SUM(int) is int"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Sum, Some(CqlType::BigInt)),
            Some(CqlType::BigInt),
            "SUM(bigint) is bigint"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Sum, Some(CqlType::SmallInt)),
            Some(CqlType::SmallInt),
            "SUM(smallint) stays smallint (Cassandra does not promote to int)"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Sum, Some(CqlType::TinyInt)),
            Some(CqlType::TinyInt),
            "SUM(tinyint) stays tinyint (Cassandra does not promote to int)"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Avg, Some(CqlType::Int)),
            Some(CqlType::Int),
            "AVG(int) is int"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Avg, Some(CqlType::BigInt)),
            Some(CqlType::BigInt),
            "AVG(bigint) is bigint"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Avg, Some(CqlType::SmallInt)),
            Some(CqlType::SmallInt),
            "AVG(smallint) stays smallint"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Avg, Some(CqlType::TinyInt)),
            Some(CqlType::TinyInt),
            "AVG(tinyint) stays tinyint"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Sum, Some(CqlType::Counter)),
            Some(CqlType::BigInt),
            "SUM(counter) is bigint"
        );
        // Float/double inputs still return double (no regression).
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Sum, Some(CqlType::Double)),
            Some(CqlType::Double),
            "SUM(double) stays double"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Avg, Some(CqlType::Float)),
            Some(CqlType::Double),
            "AVG(float) is double (no regression)"
        );
        // Unknown SUM/AVG argument → double (never None, unlike MIN/MAX).
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Sum, None),
            Some(CqlType::Double),
            "SUM with unknown argument falls back to double"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Min, Some(CqlType::Int)),
            Some(CqlType::Int),
            "MIN preserves the argument column's type"
        );
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Max, Some(CqlType::Double)),
            Some(CqlType::Double)
        );
        // Only MIN/MAX with an unknown argument type stay untyped.
        assert_eq!(aggregate_result_cql_type(&AggregateType::Min, None), None);
        assert_eq!(
            aggregate_result_cql_type(&AggregateType::Count, None),
            Some(CqlType::BigInt)
        );
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
