//! Query optimizer for SELECT statements - basic planning and predicate pushdown.

use super::select_ast::*;
use super::select_naming::{
    aggregate_column_and_alias, aggregate_output_name, projection_source_and_output,
    unwrap_aggregate,
};
use crate::{schema::SchemaManager, storage::StorageEngine, Error, Result, TableId, Value};
use std::collections::HashMap;
use std::sync::Arc;

// Test-only counter for the number of times `SelectOptimizer::optimize` runs
// (issue #1587, E5). A prepared statement must reuse its optimized plan across
// repeated executes with identical parameters, so this stays `<= 1` over many
// executes. Same thread-local rationale as the executor's other work counters
// (`#[tokio::test]` current-thread runtime keeps the future on this thread).
#[cfg(test)]
thread_local! {
    pub(crate) static OPTIMIZE_INVOCATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// Query optimizer for SELECT statements
#[derive(Debug)]
pub struct SelectOptimizer {
    #[allow(dead_code)]
    schema: Arc<SchemaManager>,
    #[allow(dead_code)]
    storage: Arc<StorageEngine>,
}

/// Optimized query execution plan
#[derive(Debug, Clone)]
pub struct OptimizedQueryPlan {
    pub statement: SelectStatement,
    pub execution_steps: Vec<ExecutionStep>,
    pub sstable_predicates: Vec<SSTablePredicate>,
    pub aggregation_plan: Option<AggregationPlan>,
}

/// Individual execution step
#[derive(Debug, Clone)]
pub enum ExecutionStep {
    SSTableScan {
        table: TableId,
        predicates: Vec<SSTablePredicate>,
        projection: Vec<String>,
    },
    Filter {
        expression: WhereExpression,
    },
    Sort {
        order_by: OrderByClause,
    },
    Aggregate {
        plan: AggregationPlan,
    },
    Limit {
        count: u64,
        offset: Option<u64>,
    },
    /// Cap rows emitted per partition key, applied upstream of `Limit`
    /// (Cassandra `PER PARTITION LIMIT`, Issue #757).
    PerPartitionLimit {
        count: u64,
    },
    Project {
        columns: Vec<SelectExpression>,
    },
}

/// SSTable-level predicate that can be pushed down
#[derive(Debug, Clone)]
pub struct SSTablePredicate {
    pub column: String,
    pub operation: SSTableFilterOp,
    pub values: Vec<Value>,
    /// When `Some`, this predicate constrains the Murmur3 *token* of the
    /// partition key formed by these columns (e.g. `WHERE token(pk) >= ?`),
    /// rather than a stored column value (Issue #955, Epic #951). The bound(s)
    /// in [`Self::values`] are `Value::BigInt` token values, and evaluation
    /// hashes the row's raw partition key with `cassandra_murmur3_token` and
    /// compares it to the bound — Cassandra's `Murmur3Partitioner` semantics.
    ///
    /// `None` for ordinary column predicates. Carries the partition-key column
    /// names (in declared order) so a multi-column `token(a, b)` is supported,
    /// keeping the representation typed rather than encoding intent in
    /// [`Self::column`].
    pub token_columns: Option<Vec<String>>,
}

impl SSTablePredicate {
    /// Construct an ordinary column predicate (`token_columns: None`).
    pub fn column(
        column: impl Into<String>,
        operation: SSTableFilterOp,
        values: Vec<Value>,
    ) -> Self {
        Self {
            column: column.into(),
            operation,
            values,
            token_columns: None,
        }
    }

    /// Construct a token predicate over `token_columns` (the partition-key
    /// columns, in declared order). `column` is a human-readable label
    /// (`"token(a, b)"`) used only for diagnostics; evaluation never reads a
    /// stored column with that name — it hashes the row key instead.
    pub fn token(
        token_columns: Vec<String>,
        operation: SSTableFilterOp,
        values: Vec<Value>,
    ) -> Self {
        let label = format!("token({})", token_columns.join(", "));
        Self {
            column: label,
            operation,
            values,
            token_columns: Some(token_columns),
        }
    }

    /// True if this predicate constrains the partition-key token rather than a
    /// stored column (Issue #955).
    pub fn is_token(&self) -> bool {
        self.token_columns.is_some()
    }
}

/// SSTable filter operations
#[derive(Debug, Clone)]
pub enum SSTableFilterOp {
    Equal,
    Range,
    /// Single-bound clustering inequalities (`>`, `>=`, `<`, `<=`). Each carries
    /// one bound value in `SSTablePredicate::values`. Issue #788.
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Prefix,
    BloomFilter,
}

/// Aggregation execution plan
#[derive(Debug, Clone)]
pub struct AggregationPlan {
    /// Raw stored column names `build_group_key` reads from each scanned row.
    pub group_by_columns: Vec<String>,
    /// Issue #1763: SELECT OUTPUT name per grouped dimension (parallel to
    /// `group_by_columns`); `finalize_group` emits the value under this name so it
    /// equals the metadata column name (both from `select_naming`). Alias when
    /// projected with one, else the column name.
    pub group_by_output_names: Vec<String>,
    pub aggregates: Vec<AggregateComputation>,
}

/// Individual aggregate computation
#[derive(Debug, Clone)]
pub struct AggregateComputation {
    pub function: AggregateType,
    pub column: String,
    pub alias: String,
    pub distinct: bool,
}

impl SelectOptimizer {
    /// Create a new query optimizer
    pub fn new(schema: Arc<SchemaManager>, storage: Arc<StorageEngine>) -> Self {
        Self { schema, storage }
    }

    /// Optimize a SELECT statement
    pub async fn optimize(&self, statement: SelectStatement) -> Result<OptimizedQueryPlan> {
        #[cfg(test)]
        OPTIMIZE_INVOCATIONS.with(|c| c.set(c.get() + 1));

        let mut plan = OptimizedQueryPlan {
            statement: statement.clone(),
            execution_steps: Vec::new(),
            sstable_predicates: Vec::new(),
            aggregation_plan: None,
        };

        // Constant expressions (no FROM) need no execution steps.
        let Some(from_clause) = statement.from_clause.as_ref() else {
            return Ok(plan);
        };
        let table_id = match from_clause {
            FromClause::Table(t) | FromClause::TableAlias(t, _) => t.clone(),
        };

        // Issue #1763: reject `SELECT DISTINCT <aggregate>` (e.g.
        // `SELECT DISTINCT COUNT(*)`). `is_aggregate()` now unwraps aliased
        // aggregates inside a `DISTINCT` clause, so `requires_aggregation()`
        // returns true and an aggregation is planned — but `plan_aggregation`
        // collects its computations ONLY from `SelectClause::Columns`, so a
        // DISTINCT aggregate would plan an aggregation with NO computations and
        // silently drop the result column. DISTINCT over an aggregate is also not
        // a meaningful shape (the aggregate already collapses all rows to a
        // single value, making DISTINCT redundant) and Cassandra rejects it.
        // Fail cleanly here rather than return a wrong result.
        if let SelectClause::Distinct(exprs) = &statement.select_clause {
            if exprs.iter().any(|e| e.is_aggregate()) {
                return Err(Error::query_execution(
                    "SELECT DISTINCT with an aggregate function is not supported; \
                     DISTINCT over an aggregate is redundant because the aggregate \
                     already collapses rows to a single value"
                        .to_string(),
                ));
            }
        }

        if let Some(where_clause) = &statement.where_clause {
            // Validate EVERY token() restriction in the WHERE tree before pushdown
            // (roborev FINDING: token() under OR/NOT was never traversed, so an
            // unsupported or non-pushable token restriction was silently dropped
            // while a sibling pushable predicate suppressed the residual Filter).
            // This whole-tree pass guarantees no token() restriction is ever
            // ignored: it is pushed, or it errors here.
            validate_token_forms_whole_tree(where_clause, true)?;
            plan.sstable_predicates = collect_sstable_predicates(where_clause)?;
        }

        plan.execution_steps.push(ExecutionStep::SSTableScan {
            table: table_id,
            predicates: plan.sstable_predicates.clone(),
            projection: extract_projection_columns(&statement.select_clause),
        });

        // If we couldn't push any predicates down, keep the original WHERE as
        // a post-scan filter step.
        if let Some(where_clause) = &statement.where_clause {
            if plan.sstable_predicates.is_empty() {
                plan.execution_steps.push(ExecutionStep::Filter {
                    expression: where_clause.clone(),
                });
            }
        }

        let needs_aggregation = statement.requires_aggregation();
        if needs_aggregation {
            let agg_plan = plan_aggregation(&statement);
            plan.execution_steps.push(ExecutionStep::Aggregate {
                plan: agg_plan.clone(),
            });
            plan.aggregation_plan = Some(agg_plan);
        }

        if let Some(order_by) = &statement.order_by {
            plan.execution_steps.push(ExecutionStep::Sort {
                order_by: order_by.clone(),
            });
        }

        // PER PARTITION LIMIT caps rows per partition before the query-wide
        // LIMIT, so it must be emitted ahead of the Limit step.
        if let Some(count) = statement.per_partition_limit {
            plan.execution_steps
                .push(ExecutionStep::PerPartitionLimit { count });
        }

        if let Some(limit) = &statement.limit {
            plan.execution_steps.push(ExecutionStep::Limit {
                count: limit.count,
                offset: statement.offset,
            });
        }

        // Aggregation already produces the final shape; an explicit Project
        // step on top would be redundant.
        if !needs_aggregation {
            if let SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) =
                &statement.select_clause
            {
                // Issue #1587 (E5): a bare-column SELECT (every item a plain
                // `Column`, no alias/function/arithmetic) is ALREADY projected by
                // the SSTable scan — `SSTableScan.projection` is exactly these
                // column names (see `extract_projection_columns`), and
                // `build_row_from_scan` keeps precisely those cells, keyed by the
                // same column name the `Project` step would re-key them to. So the
                // second `Project` re-projects every row into a byte-identical
                // value map: skip it and project each row ONCE (in the scan).
                //
                // The optimization is scoped to `SelectClause::Columns` (not
                // `Distinct`, which is not a pure projection) and to all-plain-
                // `Column` items — an alias / expression / `WRITETIME`/`TTL`
                // reshapes or renames the row and still needs the `Project` step.
                let is_bare_columns = matches!(&statement.select_clause, SelectClause::Columns(_))
                    && exprs
                        .iter()
                        .all(|e| matches!(e, SelectExpression::Column(_)));

                if !is_bare_columns {
                    plan.execution_steps.push(ExecutionStep::Project {
                        columns: exprs.clone(),
                    });
                }
            }
        }

        Ok(plan)
    }
}

/// Walk a WHERE expression tree, collecting comparisons that can be turned
/// into SSTable-level predicates. OR/NOT branches are intentionally skipped:
/// those require capabilities the SSTable filter pushdown doesn't have.
fn collect_sstable_predicates(expr: &WhereExpression) -> Result<Vec<SSTablePredicate>> {
    let mut out = Vec::new();
    fn walk(expr: &WhereExpression, out: &mut Vec<SSTablePredicate>) -> Result<()> {
        match expr {
            WhereExpression::Comparison(comp) => {
                // An unsupported `token(...)` form is a planning error here, not
                // a dropped predicate (roborev FINDING 2).
                if let Some(predicate) = comparison_to_sstable_predicate(comp)? {
                    out.push(predicate);
                }
            }
            WhereExpression::And(exprs) => {
                for e in exprs {
                    walk(e, out)?;
                }
            }
            WhereExpression::Parentheses(inner) => walk(inner, out)?,
            WhereExpression::Or(_) | WhereExpression::Not(_) => {}
        }
        Ok(())
    }
    walk(expr, &mut out)?;
    Ok(out)
}

/// Whole-WHERE-tree validation that NO `token(...)` restriction is ever silently
/// dropped, regardless of its position (top-level, AND, OR, NOT, parenthesized).
///
/// `collect_sstable_predicates` only descends pushable AND branches, so a
/// `token(...)` comparison under OR/NOT was previously never inspected: an
/// unsupported form (IN/BETWEEN/non-literal RHS/non-pk args) was not rejected,
/// and even a *supported* range/equality token form could not be pushed there.
/// In both cases, if a sibling predicate was pushable, the optimizer dropped the
/// residual Filter and the token restriction was ignored — wrong results
/// (roborev FINDING).
///
/// This pass visits every branch. At each `token(...)` comparison it:
///   * runs the full token-form validation (`token_comparison_to_predicate`),
///     so an UNSUPPORTED form is a planning error anywhere in the tree; and
///   * if the comparison is in a NON-PUSHABLE position (`pushable == false`,
///     i.e. under OR/NOT), rejects even a SUPPORTED token form with a clear
///     planning error.
///
/// Why reject supported token() forms under OR/NOT rather than evaluate them as
/// a residual filter: the row-level WHERE evaluator (`evaluate_select_expression`
/// in `select_executor.rs`) returns "Function expressions not yet implemented"
/// for `SelectExpression::Function`, so it CANNOT compute
/// `cassandra_murmur3_token(row key)` at the row level. Token evaluation exists
/// only in `evaluate_leaf` over pushed `SSTablePredicate`s, which are fed solely
/// by pushable AND branches. A token() leaf under OR/NOT can therefore be neither
/// pushed nor evaluated — so the only honest outcome is a clear error, never a
/// dropped restriction. This is a narrow, documented limitation
/// (`token()` under OR/NOT is unsupported).
///
/// `pushable` starts `true` at the root and at AND children (the positions
/// `collect_sstable_predicates` actually descends) and flips to `false` under
/// OR and NOT. `Parentheses` is transparent and preserves the current value.
fn validate_token_forms_whole_tree(expr: &WhereExpression, pushable: bool) -> Result<()> {
    match expr {
        WhereExpression::Comparison(comp) => {
            if is_token_comparison(comp) {
                // Reject unsupported token() forms regardless of position. The
                // returned predicate is discarded here; pushable positions are
                // lowered separately by `collect_sstable_predicates`.
                let _supported = comparison_to_sstable_predicate(comp)?;
                if !pushable {
                    return Err(Error::query_execution(
                        "token() restriction is only supported at the top level or within an \
                         AND conjunction; token() under OR/NOT cannot be pushed down and the \
                         row-level evaluator cannot compute a token, so it is rejected rather \
                         than silently ignored"
                            .to_string(),
                    ));
                }
            }
        }
        // AND children remain in a pushable position (the same branches
        // `collect_sstable_predicates` descends).
        WhereExpression::And(exprs) => {
            for e in exprs {
                validate_token_forms_whole_tree(e, pushable)?;
            }
        }
        // OR/NOT children are not pushed down; a token() inside them cannot be
        // enforced, so mark the subtree non-pushable.
        WhereExpression::Or(exprs) => {
            for e in exprs {
                validate_token_forms_whole_tree(e, false)?;
            }
        }
        WhereExpression::Not(inner) => validate_token_forms_whole_tree(inner, false)?,
        // Parentheses are transparent: they neither create nor remove a pushable
        // position, so the current `pushable` flag carries through.
        WhereExpression::Parentheses(inner) => validate_token_forms_whole_tree(inner, pushable)?,
    }
    Ok(())
}

/// True when a comparison's left side is a `token(...)` call (case-insensitive),
/// i.e. it denotes a partition-key token restriction rather than a column.
fn is_token_comparison(comp: &ComparisonExpression) -> bool {
    matches!(
        &comp.left,
        SelectExpression::Function(func) if func.name.eq_ignore_ascii_case("token")
    )
}

/// Returns `Ok(Some(pred))` for a pushable predicate, `Ok(None)` for a
/// comparison that is legitimately not pushed down (a residual Filter handles
/// it), and `Err` for an *invalid* restriction that must fail planning. The
/// only `Err` case is an unsupported `token(...)` form (see
/// `token_comparison_to_predicate`): a `token()` LHS always denotes a token
/// restriction, so an unsupported form is a query error, never a silently
/// dropped predicate (roborev FINDING 2). Bare-column comparisons still return
/// `Ok(None)` when not pushable.
fn comparison_to_sstable_predicate(
    comp: &ComparisonExpression,
) -> Result<Option<SSTablePredicate>> {
    // The left side is either a bare column or a `token(col, ...)` call. A
    // `token(...)` predicate constrains the partition-key token, not a stored
    // column, so it gets a token predicate (Issue #955); anything else only
    // supports the bare-column form (Issue #788's existing behaviour).
    match &comp.left {
        SelectExpression::Column(col_ref) => {
            Ok(column_comparison_to_predicate(col_ref.column.clone(), comp))
        }
        SelectExpression::Function(func) if func.name.eq_ignore_ascii_case("token") => {
            token_comparison_to_predicate(func, comp).map(Some)
        }
        _ => Ok(None),
    }
}

/// Convert a bare-column comparison (`col <op> ...`) to a pushed-down predicate.
fn column_comparison_to_predicate(
    column: String,
    comp: &ComparisonExpression,
) -> Option<SSTablePredicate> {
    use SSTableFilterOp as Op;
    match (&comp.operator, &comp.right) {
        (ComparisonOperator::Equal, ComparisonRightSide::Value(value_expr)) => Some(
            SSTablePredicate::column(column, Op::Equal, vec![literal_value(value_expr)?]),
        ),
        (ComparisonOperator::In, ComparisonRightSide::ValueList(value_exprs)) => {
            let values: Vec<Value> = value_exprs.iter().filter_map(literal_value).collect();
            (!values.is_empty()).then(|| SSTablePredicate::column(column, Op::In, values))
        }
        (ComparisonOperator::Between, ComparisonRightSide::Range(start_expr, end_expr)) => {
            let start = literal_value(start_expr)?;
            let end = literal_value(end_expr)?;
            Some(SSTablePredicate::column(
                column,
                Op::Range,
                vec![start, end],
            ))
        }
        // Single-bound clustering inequalities. Without these arms the operators
        // fall through to `None` and the restriction is silently dropped, so the
        // whole partition is returned instead of the requested slice (Issue #788).
        (ComparisonOperator::GreaterThan, ComparisonRightSide::Value(v)) => Some(
            SSTablePredicate::column(column, Op::Gt, vec![literal_value(v)?]),
        ),
        (ComparisonOperator::GreaterThanOrEqual, ComparisonRightSide::Value(v)) => Some(
            SSTablePredicate::column(column, Op::Gte, vec![literal_value(v)?]),
        ),
        (ComparisonOperator::LessThan, ComparisonRightSide::Value(v)) => Some(
            SSTablePredicate::column(column, Op::Lt, vec![literal_value(v)?]),
        ),
        (ComparisonOperator::LessThanOrEqual, ComparisonRightSide::Value(v)) => Some(
            SSTablePredicate::column(column, Op::Lte, vec![literal_value(v)?]),
        ),
        _ => None,
    }
}

/// Convert a `token(col, ...) <op> <bound>` comparison to a token predicate
/// (Issue #955). The bound must be an integer literal (the i64 token value).
/// `token(...) = ?` lowers to a token `Equal` predicate (an exact-token
/// restriction that `evaluate_leaf` evaluates by hashing the row key).
///
/// Unsupported `token()` forms are a PLANNING ERROR rather than a dropped
/// predicate (Issue #955 follow-up / roborev FINDING 2). Previously these
/// returned `None`; when the same query carried another pushable predicate
/// (e.g. `ck > 0`), the optimizer omitted the residual Filter step and the
/// token restriction was SILENTLY IGNORED — so `token(pk) IN (...) AND ck > 0`
/// returned all rows. Cassandra rejects `token() IN`/`BETWEEN` and non-literal
/// RHS on a token restriction, so erroring is the correct behaviour. We only
/// reject the unsupported token forms here; supported range/equality token
/// restrictions still lower normally.
fn token_comparison_to_predicate(
    func: &FunctionCall,
    comp: &ComparisonExpression,
) -> Result<SSTablePredicate> {
    use SSTableFilterOp as Op;
    // Collect the partition-key column names the token() is computed over.
    let mut token_columns = Vec::with_capacity(func.args.len());
    for arg in &func.args {
        match arg {
            SelectExpression::Column(col_ref) => token_columns.push(col_ref.column.clone()),
            other => {
                return Err(Error::query_execution(format!(
                    "token() argument must be a partition-key column; got {other:?}"
                )));
            }
        }
    }
    if token_columns.is_empty() {
        return Err(Error::query_execution(
            "token() restriction requires at least one partition-key column argument".to_string(),
        ));
    }

    let op = match &comp.operator {
        ComparisonOperator::GreaterThan => Op::Gt,
        ComparisonOperator::GreaterThanOrEqual => Op::Gte,
        ComparisonOperator::LessThan => Op::Lt,
        ComparisonOperator::LessThanOrEqual => Op::Lte,
        // `token(pk) = ?` is an exact-token restriction. Lower it to a token
        // `Equal` predicate so `evaluate_leaf`'s `Equal` arm hashes the row key
        // and compares it to the bound (Issue #955 follow-up).
        ComparisonOperator::Equal => Op::Equal,
        // `token(pk) IN (...)`, `token(pk) BETWEEN ...`, and any other operator
        // are not valid token restrictions. Reject rather than drop so the
        // restriction is never silently ignored.
        other => {
            return Err(Error::query_execution(format!(
                "unsupported token() restriction operator {other:?}; \
                 token() supports only range bounds (<, <=, >, >=) and equality (=)"
            )));
        }
    };
    let ComparisonRightSide::Value(value_expr) = &comp.right else {
        return Err(Error::query_execution(
            "token() restriction requires a single integer token bound on the right-hand side"
                .to_string(),
        ));
    };
    // The token bound is an i64; the parser emits integer literals as BigInt.
    let bound = match literal_value(value_expr) {
        Some(Value::BigInt(n)) => Value::BigInt(n),
        Some(Value::Integer(n)) => Value::BigInt(n as i64),
        _ => {
            return Err(Error::query_execution(
                "token() restriction bound must be an integer token value".to_string(),
            ));
        }
    };
    Ok(SSTablePredicate::token(token_columns, op, vec![bound]))
}

fn literal_value(expr: &SelectExpression) -> Option<Value> {
    match expr {
        SelectExpression::Literal(value) => Some(value.clone()),
        _ => None,
    }
}

/// Columns the SSTable scan must READ: the SOURCE stored column names, NOT the
/// SELECT output aliases.
///
/// Issue #1763: `build_row_from_scan` filters decoded cells by physical column
/// name, so an aliased projection (`category AS cat`) MUST scan `category` —
/// projecting `cat` would drop the cell (null value; the `Project` step / grouped
/// dimension could not find it). Output naming is applied afterwards (`Project`
/// step / metadata / `finalize_group`), all via `select_naming`. Aggregates name
/// no stored cell and are excluded.
fn extract_projection_columns(select_clause: &SelectClause) -> Vec<String> {
    match select_clause {
        SelectClause::All => Vec::new(),
        SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => exprs
            .iter()
            .filter_map(|e| projection_source_and_output(e).map(|(source, _)| source))
            .collect(),
    }
}

fn plan_aggregation(statement: &SelectStatement) -> AggregationPlan {
    let group_by_columns: Vec<String> = statement
        .group_by
        .as_ref()
        .map(|g| g.columns.iter().map(|col| col.column.clone()).collect())
        .unwrap_or_default();

    // Issue #1763 (grouped dimensions): map each grouped column to its SELECT
    // OUTPUT name via the SAME `select_naming` source `get_result_columns` uses,
    // so `finalize_group` emits the grouped value under the metadata column name.
    // `col AS alias` yields `alias`; bare or not-projected yields the column name.
    let mut output_for_source: HashMap<String, String> = HashMap::new();
    if let SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) = &statement.select_clause {
        for expr in exprs {
            if let Some((source, output)) = projection_source_and_output(expr) {
                // First projection of a source column wins (matches metadata order).
                output_for_source.entry(source).or_insert(output);
            }
        }
    }
    let group_by_output_names: Vec<String> = group_by_columns
        .iter()
        .map(|col| {
            output_for_source
                .get(col.as_str())
                .cloned()
                .unwrap_or_else(|| col.clone())
        })
        .collect();

    let mut aggregates = Vec::new();
    if let SelectClause::Columns(exprs) = &statement.select_clause {
        for expr in exprs {
            // Issue #1763: handle both bare `COUNT(*)` and aliased
            // `COUNT(*) AS total`. The output name (`alias`) is derived by the
            // SINGLE shared source `aggregate_output_name`, so the row value key
            // emitted by `finalize_group` can never diverge from the result
            // metadata column name built by `get_result_columns`.
            let Some((agg, alias)) = unwrap_aggregate(expr).zip(aggregate_output_name(expr)) else {
                continue;
            };
            let (column, _) = aggregate_column_and_alias(agg);
            aggregates.push(AggregateComputation {
                function: agg.function.clone(),
                column,
                alias,
                distinct: agg.distinct,
            });
        }
    }

    AggregationPlan {
        group_by_columns,
        group_by_output_names,
        aggregates,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{platform::Platform, schema::SchemaManager, storage::StorageEngine, Config};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_optimizer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());
        let optimizer = SelectOptimizer { schema, storage };
        assert!(std::mem::size_of_val(&optimizer) > 0);
    }

    async fn make_optimizer() -> SelectOptimizer {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());
        SelectOptimizer { schema, storage }
    }

    fn project_step_count(plan: &OptimizedQueryPlan) -> usize {
        plan.execution_steps
            .iter()
            .filter(|s| matches!(s, ExecutionStep::Project { .. }))
            .count()
    }

    /// Issue #1587 (E5): a bare-column SELECT is projected exactly ONCE — by the
    /// SSTable scan — with no redundant `Project` step re-projecting every row.
    /// "row-projection-passes" = the scan pass (always 1) + one per `Project`
    /// step; it must be 1 for a bare-column select (was 2 on main) and stay 2
    /// for an aliased projection, which reshapes/renames the row.
    #[tokio::test]
    async fn bare_column_select_projects_rows_once() {
        let optimizer = make_optimizer().await;

        let bare = crate::query::select_parser::parse_select("SELECT a, b, c FROM t").unwrap();
        let plan = optimizer.optimize(bare).await.unwrap();
        let projection_passes = 1 + project_step_count(&plan);
        assert_eq!(
            projection_passes, 1,
            "issue #1587: a bare-column SELECT must project each row once (scan only), not twice"
        );

        // The scan step already carries exactly the selected columns, in order.
        let scan_projection = plan
            .execution_steps
            .iter()
            .find_map(|s| match s {
                ExecutionStep::SSTableScan { projection, .. } => Some(projection.clone()),
                _ => None,
            })
            .expect("plan has an SSTable scan");
        assert_eq!(scan_projection, vec!["a", "b", "c"]);

        // Contrast: an aliased projection reshapes the row → keeps its Project pass.
        let mut aliased = crate::query::select_parser::parse_select("SELECT a FROM t").unwrap();
        aliased.select_clause = SelectClause::Columns(vec![SelectExpression::Aliased(
            Box::new(SelectExpression::Column(ColumnRef {
                table: None,
                column: "a".to_string(),
            })),
            "x".to_string(),
        )]);
        let aplan = optimizer.optimize(aliased).await.unwrap();
        assert_eq!(
            1 + project_step_count(&aplan),
            2,
            "an aliased projection must keep its Project pass"
        );
    }

    /// Build `<column> <op> <literal>` for predicate-conversion tests.
    fn cmp(op: ComparisonOperator, column: &str, value: Value) -> ComparisonExpression {
        ComparisonExpression {
            left: SelectExpression::Column(ColumnRef {
                table: None,
                column: column.to_string(),
            }),
            operator: op,
            right: ComparisonRightSide::Value(SelectExpression::Literal(value)),
        }
    }

    /// Issue #788: clustering-key inequalities must convert to single-bound
    /// SSTable predicates so they are evaluated post-scan instead of dropped.
    #[test]
    fn inequality_operators_convert_to_single_bound_predicates() {
        let cases = [
            (ComparisonOperator::GreaterThan, SSTableFilterOp::Gt),
            (ComparisonOperator::GreaterThanOrEqual, SSTableFilterOp::Gte),
            (ComparisonOperator::LessThan, SSTableFilterOp::Lt),
            (ComparisonOperator::LessThanOrEqual, SSTableFilterOp::Lte),
        ];
        for (op, expected_op) in cases {
            let comp = cmp(op.clone(), "ck", Value::Integer(200));
            let predicate = comparison_to_sstable_predicate(&comp)
                .expect("conversion must not error")
                .unwrap_or_else(|| panic!("operator {op:?} must convert to a predicate"));
            assert_eq!(predicate.column, "ck");
            assert!(
                std::mem::discriminant(&predicate.operation)
                    == std::mem::discriminant(&expected_op),
                "operator {op:?} produced {:?}, expected {expected_op:?}",
                predicate.operation
            );
            assert_eq!(predicate.values, vec![Value::Integer(200)]);
        }
    }

    /// Issue #788, end-to-end through the real parser: the exact query from the
    /// bug report must produce a plan that carries BOTH clustering inequalities
    /// alongside the partition equality. Before the fix only the `pk` equality
    /// survived `collect_sstable_predicates`, so the whole partition leaked.
    #[test]
    fn query_plan_carries_clustering_inequality_bounds() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select(
            "SELECT * FROM perf.wide_rows WHERE pk = 'p0000' AND ck >= 0 AND ck < 200",
        )
        .expect("issue #788 query must parse");
        let where_clause = statement
            .where_clause
            .expect("WHERE clause must be present");

        let predicates = collect_sstable_predicates(&where_clause).expect("planning must succeed");

        let has = |col: &str, want: &SSTableFilterOp| {
            predicates.iter().any(|p| {
                p.column == col
                    && std::mem::discriminant(&p.operation) == std::mem::discriminant(want)
            })
        };

        assert!(
            has("pk", &SSTableFilterOp::Equal),
            "partition equality must be pushed; got {predicates:?}"
        );
        assert!(
            has("ck", &SSTableFilterOp::Gte),
            "Issue #788: `ck >= 0` must be pushed as Gte (was dropped); got {predicates:?}"
        );
        assert!(
            has("ck", &SSTableFilterOp::Lt),
            "Issue #788: `ck < 200` must be pushed as Lt (was dropped); got {predicates:?}"
        );
        assert_eq!(
            predicates.len(),
            3,
            "all three restrictions must be captured; got {predicates:?}"
        );
    }

    /// Issue #955: `WHERE pk IN (1, 2, 3)` parses end-to-end and pushes down a
    /// single `In` predicate carrying all three literal values, so the executor
    /// can fan it out to targeted lookups.
    #[test]
    fn query_plan_carries_in_predicate() {
        use crate::query::select_parser::parse_select;

        let statement =
            parse_select("SELECT * FROM ks.t WHERE pk IN (1, 2, 3)").expect("IN query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        let predicates = collect_sstable_predicates(&where_clause).expect("planning must succeed");

        assert_eq!(predicates.len(), 1, "one IN predicate; got {predicates:?}");
        let p = &predicates[0];
        assert_eq!(p.column, "pk");
        assert!(matches!(p.operation, SSTableFilterOp::In));
        assert!(!p.is_token());
        assert_eq!(
            p.values,
            vec![Value::BigInt(1), Value::BigInt(2), Value::BigInt(3)]
        );
    }

    /// Issue #955: a `token(pk)` range restriction parses to typed token
    /// predicates (NOT bare-column predicates), carrying i64 bounds — including
    /// a negative lower bound (tokens span the full i64 range).
    #[test]
    fn query_plan_carries_token_range_predicate() {
        use crate::query::select_parser::parse_select;

        let statement =
            parse_select("SELECT * FROM ks.t WHERE token(pk) >= -100 AND token(pk) < 5000")
                .expect("token-range query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        let predicates = collect_sstable_predicates(&where_clause).expect("planning must succeed");

        assert_eq!(predicates.len(), 2, "two token bounds; got {predicates:?}");
        assert!(
            predicates.iter().all(|p| p.is_token()),
            "both predicates must be token predicates; got {predicates:?}"
        );
        let lower = predicates
            .iter()
            .find(|p| matches!(p.operation, SSTableFilterOp::Gte))
            .expect("a Gte token bound");
        assert_eq!(
            lower.token_columns.as_deref(),
            Some(["pk".to_string()].as_slice())
        );
        assert_eq!(lower.values, vec![Value::BigInt(-100)]);
        let upper = predicates
            .iter()
            .find(|p| matches!(p.operation, SSTableFilterOp::Lt))
            .expect("a Lt token bound");
        assert_eq!(upper.values, vec![Value::BigInt(5000)]);
    }

    /// FINDING 1 (Issue #955 follow-up): `token(pk) = <t>` must lower to a token
    /// `Equal` predicate — NOT be dropped. Previously it fell through to `None`,
    /// so when combined with another pushed predicate the residual Filter step
    /// was skipped and the exact-token restriction was silently ignored.
    #[test]
    fn token_equal_lowers_to_token_equal_predicate() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE token(pk) = 4242")
            .expect("token-equal query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        let predicates = collect_sstable_predicates(&where_clause).expect("planning must succeed");

        assert_eq!(
            predicates.len(),
            1,
            "token(pk) = ? must produce exactly one predicate (not be dropped); got {predicates:?}"
        );
        let p = &predicates[0];
        assert!(p.is_token(), "must be a token predicate; got {p:?}");
        assert!(
            matches!(p.operation, SSTableFilterOp::Equal),
            "token(pk) = ? must lower to a token Equal op; got {:?}",
            p.operation
        );
        assert_eq!(
            p.token_columns.as_deref(),
            Some(["pk".to_string()].as_slice())
        );
        assert_eq!(p.values, vec![Value::BigInt(4242)]);
    }

    /// FINDING 1: when `token(pk) = ?` is combined with another pushed predicate,
    /// BOTH must survive `collect_sstable_predicates`. The original bug dropped
    /// the token equality, and (because at least one predicate remained) the
    /// optimizer skipped the residual Filter, silently ignoring the token bound.
    #[test]
    fn token_equal_combined_with_other_predicate_keeps_both() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE token(pk) = 7 AND ck > 0")
            .expect("combined token-equal query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        let predicates = collect_sstable_predicates(&where_clause).expect("planning must succeed");

        assert!(
            predicates
                .iter()
                .any(|p| p.is_token() && matches!(p.operation, SSTableFilterOp::Equal)),
            "the token(pk) = 7 restriction must be pushed as a token Equal; got {predicates:?}"
        );
        assert!(
            predicates.iter().any(|p| !p.is_token() && p.column == "ck"),
            "the ck > 0 restriction must also be pushed; got {predicates:?}"
        );
    }

    /// roborev FINDING 2: `token(pk) IN (...)` is not a valid token restriction.
    /// It must surface a PLANNING ERROR rather than fall through to `None` (which
    /// would silently drop the restriction). Cassandra rejects `token() IN`.
    #[test]
    fn token_in_is_a_planning_error() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE token(pk) IN (1, 2, 3)")
            .expect("token-IN query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        let err = collect_sstable_predicates(&where_clause)
            .expect_err("token(pk) IN (...) must be rejected, not silently dropped");
        let msg = err.to_string();
        assert!(
            msg.contains("token()"),
            "error must explain the token() restriction; got: {msg}"
        );
    }

    /// roborev FINDING 2: combined with another pushable predicate, an unsupported
    /// `token(pk) IN (...) AND ck > 0` previously dropped the token restriction and
    /// (because `ck > 0` remained) skipped the residual Filter — silently returning
    /// all rows. It must now be a planning error.
    #[test]
    fn token_in_combined_with_other_predicate_is_a_planning_error() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE token(pk) IN (1, 2) AND ck > 0")
            .expect("combined token-IN query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        assert!(
            collect_sstable_predicates(&where_clause).is_err(),
            "token(pk) IN (...) AND ck > 0 must error, not silently ignore the token restriction"
        );
    }

    /// roborev FINDING 2: `token(pk) BETWEEN a AND b` is not a valid token
    /// restriction and must surface a planning error rather than be dropped.
    #[test]
    fn token_between_is_a_planning_error() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE token(pk) BETWEEN 1 AND 9")
            .expect("token-BETWEEN query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        let err = collect_sstable_predicates(&where_clause)
            .expect_err("token(pk) BETWEEN must be rejected, not silently dropped");
        assert!(
            err.to_string().contains("token()"),
            "error must explain the token() restriction; got: {err}"
        );
    }

    /// roborev FINDING (whole-tree): an UNSUPPORTED token() form under `NOT`
    /// (`ck > 0 AND NOT token(pk) IN (1, 2)`) must be a PLANNING ERROR. The
    /// pushdown walk never descends NOT, so previously the token IN restriction
    /// was not validated; `ck > 0` was pushed, the residual Filter was omitted,
    /// and the invalid token restriction was silently ignored.
    #[test]
    fn token_in_under_not_is_a_planning_error() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE ck > 0 AND NOT token(pk) IN (1, 2)")
            .expect("token-IN-under-NOT query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        let err = validate_token_forms_whole_tree(&where_clause, true)
            .expect_err("token(pk) IN under NOT must be rejected, not silently dropped");
        assert!(
            err.to_string().contains("token()"),
            "error must explain the token() restriction; got: {err}"
        );
    }

    /// roborev FINDING (whole-tree): an UNSUPPORTED token() form under `OR`
    /// (`token(pk) IN (...) OR ck = 3`) must be a planning error regardless of
    /// position.
    #[test]
    fn token_in_under_or_is_a_planning_error() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE token(pk) IN (1, 2) OR ck = 3")
            .expect("token-IN-under-OR query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        assert!(
            validate_token_forms_whole_tree(&where_clause, true).is_err(),
            "token(pk) IN (...) under OR must error, not silently ignore the token restriction"
        );
    }

    /// roborev FINDING (whole-tree): `token(pk) BETWEEN a AND b` under `OR` must
    /// also be rejected wherever it appears.
    #[test]
    fn token_between_under_or_is_a_planning_error() {
        use crate::query::select_parser::parse_select;

        let statement =
            parse_select("SELECT * FROM ks.t WHERE token(pk) BETWEEN 1 AND 9 OR ck = 3")
                .expect("token-BETWEEN-under-OR query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        assert!(
            validate_token_forms_whole_tree(&where_clause, true).is_err(),
            "token() BETWEEN under OR must error"
        );
    }

    /// roborev FINDING (whole-tree): a SUPPORTED token range under `OR`
    /// (`token(pk) > 5 OR ck = 3`) cannot be pushed down, and the row-level WHERE
    /// evaluator cannot compute a token, so it must be a CLEAR planning error
    /// rather than silently ignored. (Decision: reject; see
    /// `validate_token_forms_whole_tree` rationale.)
    #[test]
    fn supported_token_range_under_or_is_a_planning_error() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE token(pk) > 5 OR ck = 3")
            .expect("token-range-under-OR query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        let err = validate_token_forms_whole_tree(&where_clause, true).expect_err(
            "a supported token range under OR must error (cannot be pushed nor row-evaluated)",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("token()") && msg.contains("OR/NOT"),
            "error must explain the OR/NOT limitation; got: {msg}"
        );
    }

    /// roborev FINDING (whole-tree): a SUPPORTED token range under `NOT` must
    /// likewise be a clear planning error.
    #[test]
    fn supported_token_range_under_not_is_a_planning_error() {
        use crate::query::select_parser::parse_select;

        let statement = parse_select("SELECT * FROM ks.t WHERE ck = 3 AND NOT token(pk) > 5")
            .expect("token-range-under-NOT query must parse");
        let where_clause = statement.where_clause.expect("WHERE present");
        assert!(
            validate_token_forms_whole_tree(&where_clause, true).is_err(),
            "a supported token range under NOT must error"
        );
    }

    /// roborev FINDING (whole-tree) guard: top-level / AND-conjoined SUPPORTED
    /// token forms must STILL pass the whole-tree validation (they are pushed
    /// down by `collect_sstable_predicates`), so the pass must not over-reject.
    #[test]
    fn supported_token_forms_pass_whole_tree_validation() {
        use crate::query::select_parser::parse_select;

        for q in [
            "SELECT * FROM ks.t WHERE token(pk) > 0",
            "SELECT * FROM ks.t WHERE token(pk) >= -100 AND token(pk) < 5000",
            "SELECT * FROM ks.t WHERE token(pk) = 4242",
            "SELECT * FROM ks.t WHERE token(pk) = 7 AND ck > 0",
            // Nested AND inside parentheses stays pushable.
            "SELECT * FROM ks.t WHERE (token(pk) > 0 AND ck > 1)",
        ] {
            let statement = parse_select(q).unwrap_or_else(|e| panic!("{q} must parse: {e}"));
            let where_clause = statement.where_clause.expect("WHERE present");
            validate_token_forms_whole_tree(&where_clause, true)
                .unwrap_or_else(|e| panic!("{q} must pass whole-tree validation: {e}"));
        }
    }

    /// roborev FINDING 2 guard: supported token range/equality restrictions must
    /// STILL plan successfully after making unsupported forms an error.
    #[test]
    fn token_range_and_equality_still_plan() {
        use crate::query::select_parser::parse_select;

        for q in [
            "SELECT * FROM ks.t WHERE token(pk) > 0",
            "SELECT * FROM ks.t WHERE token(pk) >= -100 AND token(pk) < 5000",
            "SELECT * FROM ks.t WHERE token(pk) = 4242",
            "SELECT * FROM ks.t WHERE token(pk) = 7 AND ck > 0",
        ] {
            let statement = parse_select(q).unwrap_or_else(|e| panic!("{q} must parse: {e}"));
            let where_clause = statement.where_clause.expect("WHERE present");
            let predicates = collect_sstable_predicates(&where_clause)
                .unwrap_or_else(|e| panic!("{q} must plan without error: {e}"));
            assert!(
                predicates.iter().any(|p| p.is_token()),
                "{q} must still push a token predicate; got {predicates:?}"
            );
        }
    }
}
