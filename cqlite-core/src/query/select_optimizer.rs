//! Query optimizer for SELECT statements - basic planning and predicate pushdown.

use super::select_ast::*;
use crate::{schema::SchemaManager, storage::StorageEngine, Result, TableId, Value};
use std::sync::Arc;

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
}

/// SSTable filter operations
#[derive(Debug, Clone)]
pub enum SSTableFilterOp {
    Equal,
    Range,
    In,
    Prefix,
    BloomFilter,
}

/// Aggregation execution plan
#[derive(Debug, Clone)]
pub struct AggregationPlan {
    pub group_by_columns: Vec<String>,
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

        if let Some(where_clause) = &statement.where_clause {
            plan.sstable_predicates = collect_sstable_predicates(where_clause);
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
                plan.execution_steps.push(ExecutionStep::Project {
                    columns: exprs.clone(),
                });
            }
        }

        Ok(plan)
    }
}

/// Walk a WHERE expression tree, collecting comparisons that can be turned
/// into SSTable-level predicates. OR/NOT branches are intentionally skipped:
/// those require capabilities the SSTable filter pushdown doesn't have.
fn collect_sstable_predicates(expr: &WhereExpression) -> Vec<SSTablePredicate> {
    let mut out = Vec::new();
    fn walk(expr: &WhereExpression, out: &mut Vec<SSTablePredicate>) {
        match expr {
            WhereExpression::Comparison(comp) => {
                if let Some(predicate) = comparison_to_sstable_predicate(comp) {
                    out.push(predicate);
                }
            }
            WhereExpression::And(exprs) => {
                for e in exprs {
                    walk(e, out);
                }
            }
            WhereExpression::Parentheses(inner) => walk(inner, out),
            WhereExpression::Or(_) | WhereExpression::Not(_) => {}
        }
    }
    walk(expr, &mut out);
    out
}

fn comparison_to_sstable_predicate(comp: &ComparisonExpression) -> Option<SSTablePredicate> {
    let SelectExpression::Column(col_ref) = &comp.left else {
        return None;
    };
    let column = col_ref.column.clone();

    match (&comp.operator, &comp.right) {
        (ComparisonOperator::Equal, ComparisonRightSide::Value(value_expr)) => {
            let value = literal_value(value_expr)?;
            Some(SSTablePredicate {
                column,
                operation: SSTableFilterOp::Equal,
                values: vec![value],
            })
        }
        (ComparisonOperator::In, ComparisonRightSide::ValueList(value_exprs)) => {
            let values: Vec<Value> = value_exprs.iter().filter_map(literal_value).collect();
            (!values.is_empty()).then_some(SSTablePredicate {
                column,
                operation: SSTableFilterOp::In,
                values,
            })
        }
        (ComparisonOperator::Between, ComparisonRightSide::Range(start_expr, end_expr)) => {
            let start = literal_value(start_expr)?;
            let end = literal_value(end_expr)?;
            Some(SSTablePredicate {
                column,
                operation: SSTableFilterOp::Range,
                values: vec![start, end],
            })
        }
        _ => None,
    }
}

fn literal_value(expr: &SelectExpression) -> Option<Value> {
    match expr {
        SelectExpression::Literal(value) => Some(value.clone()),
        _ => None,
    }
}

fn extract_projection_columns(select_clause: &SelectClause) -> Vec<String> {
    match select_clause {
        SelectClause::All => Vec::new(),
        SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => {
            exprs.iter().filter_map(extract_column_name).collect()
        }
    }
}

fn extract_column_name(expr: &SelectExpression) -> Option<String> {
    match expr {
        SelectExpression::Column(col_ref) => Some(col_ref.column.clone()),
        SelectExpression::Aliased(_, alias) => Some(alias.clone()),
        _ => None,
    }
}

fn plan_aggregation(statement: &SelectStatement) -> AggregationPlan {
    let group_by_columns = statement
        .group_by
        .as_ref()
        .map(|g| g.columns.iter().map(|col| col.column.clone()).collect())
        .unwrap_or_default();

    let mut aggregates = Vec::new();
    if let SelectClause::Columns(exprs) = &statement.select_clause {
        for expr in exprs {
            if let SelectExpression::Aggregate(agg) = expr {
                let (column, alias) = aggregate_column_and_alias(agg);
                aggregates.push(AggregateComputation {
                    function: agg.function.clone(),
                    column,
                    alias,
                    distinct: agg.distinct,
                });
            }
        }
    }

    AggregationPlan {
        group_by_columns,
        aggregates,
    }
}

/// Resolve `(column, alias)` for an aggregate. `COUNT(*)` and any aggregate
/// referencing `*` yields `("*", "Func(*)")`; a single named column yields
/// `(name, "Func_name")`; anything else falls back to `("*", "Func")`.
fn aggregate_column_and_alias(agg: &AggregateFunction) -> (String, String) {
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
}
