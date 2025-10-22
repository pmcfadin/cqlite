//! Query optimizer for SELECT statements - basic planning and predicate pushdown.

use super::select_ast::*;
use crate::{error::Error, schema::SchemaManager, storage::StorageEngine, Result, TableId, Value};
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

        if statement.from_clause.is_none() {
            return Ok(plan);
        }

        let table_id = self.extract_table_id(
            statement
                .from_clause
                .as_ref()
                .ok_or_else(|| Error::internal("Missing FROM clause"))?,
        )?;

        if let Some(ref where_clause) = statement.where_clause {
            plan.sstable_predicates = self.extract_sstable_predicates(where_clause)?;
        }

        plan.execution_steps.push(ExecutionStep::SSTableScan {
            table: table_id,
            predicates: plan.sstable_predicates.clone(),
            projection: self.extract_projection_columns(&statement.select_clause),
        });

        if let Some(ref where_clause) = statement.where_clause {
            if let Some(filter) =
                self.extract_remaining_filters(where_clause, &plan.sstable_predicates)
            {
                plan.execution_steps
                    .push(ExecutionStep::Filter { expression: filter });
            }
        }

        if statement.requires_aggregation() {
            let agg_plan = self.plan_aggregation(&statement)?;
            plan.execution_steps.push(ExecutionStep::Aggregate {
                plan: agg_plan.clone(),
            });
            plan.aggregation_plan = Some(agg_plan);
        }

        if let Some(ref order_by) = statement.order_by {
            plan.execution_steps.push(ExecutionStep::Sort {
                order_by: order_by.clone(),
            });
        }

        if let Some(ref limit) = statement.limit {
            plan.execution_steps.push(ExecutionStep::Limit {
                count: limit.count,
                offset: statement.offset,
            });
        }

        if let SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) =
            &statement.select_clause
        {
            if !statement.requires_aggregation() {
                plan.execution_steps.push(ExecutionStep::Project {
                    columns: exprs.clone(),
                });
            }
        }

        Ok(plan)
    }

    fn extract_table_id(&self, from_clause: &FromClause) -> Result<TableId> {
        match from_clause {
            FromClause::Table(table_id) | FromClause::TableAlias(table_id, _) => {
                Ok(table_id.clone())
            }
        }
    }

    fn extract_sstable_predicates(
        &self,
        where_clause: &WhereExpression,
    ) -> Result<Vec<SSTablePredicate>> {
        let mut predicates = Vec::new();
        self.extract_predicates_recursive(where_clause, &mut predicates);
        Ok(predicates)
    }

    fn extract_predicates_recursive(
        &self,
        expr: &WhereExpression,
        predicates: &mut Vec<SSTablePredicate>,
    ) {
        match expr {
            WhereExpression::Comparison(comp) => {
                if let Some(predicate) = self.comparison_to_sstable_predicate(comp) {
                    predicates.push(predicate);
                }
            }
            WhereExpression::And(exprs) => {
                for expr in exprs {
                    self.extract_predicates_recursive(expr, predicates);
                }
            }
            WhereExpression::Parentheses(expr) => {
                self.extract_predicates_recursive(expr, predicates);
            }
            _ => {}
        }
    }

    fn comparison_to_sstable_predicate(
        &self,
        comp: &ComparisonExpression,
    ) -> Option<SSTablePredicate> {
        let SelectExpression::Column(col_ref) = &comp.left else {
            return None;
        };
        let column = col_ref.column.clone();

        match (&comp.operator, &comp.right) {
            (ComparisonOperator::Equal, ComparisonRightSide::Value(value_expr)) => self
                .extract_literal_value(value_expr)
                .map(|value| SSTablePredicate {
                    column,
                    operation: SSTableFilterOp::Equal,
                    values: vec![value],
                }),
            (ComparisonOperator::In, ComparisonRightSide::ValueList(value_exprs)) => {
                let values: Vec<Value> = value_exprs
                    .iter()
                    .filter_map(|expr| self.extract_literal_value(expr))
                    .collect();
                (!values.is_empty()).then(|| SSTablePredicate {
                    column,
                    operation: SSTableFilterOp::In,
                    values,
                })
            }
            (ComparisonOperator::Between, ComparisonRightSide::Range(start_expr, end_expr)) => {
                match (
                    self.extract_literal_value(start_expr),
                    self.extract_literal_value(end_expr),
                ) {
                    (Some(start), Some(end)) => Some(SSTablePredicate {
                        column,
                        operation: SSTableFilterOp::Range,
                        values: vec![start, end],
                    }),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn extract_literal_value(&self, expr: &SelectExpression) -> Option<Value> {
        match expr {
            SelectExpression::Literal(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn extract_projection_columns(&self, select_clause: &SelectClause) -> Vec<String> {
        match select_clause {
            SelectClause::All => vec![],
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => exprs
                .iter()
                .filter_map(|expr| self.extract_column_name(expr))
                .collect(),
        }
    }

    fn extract_column_name(&self, expr: &SelectExpression) -> Option<String> {
        match expr {
            SelectExpression::Column(col_ref) => Some(col_ref.column.clone()),
            SelectExpression::Aliased(_, alias) => Some(alias.clone()),
            _ => None,
        }
    }

    fn extract_remaining_filters(
        &self,
        where_clause: &WhereExpression,
        sstable_predicates: &[SSTablePredicate],
    ) -> Option<WhereExpression> {
        sstable_predicates.is_empty().then(|| where_clause.clone())
    }

    fn plan_aggregation(&self, statement: &SelectStatement) -> Result<AggregationPlan> {
        let group_by_columns = statement
            .group_by
            .as_ref()
            .map(|g| g.columns.iter().map(|col| col.column.clone()).collect())
            .unwrap_or_default();

        let mut aggregates = Vec::new();
        if let SelectClause::Columns(exprs) = &statement.select_clause {
            for expr in exprs {
                if let SelectExpression::Aggregate(agg) = expr {
                    let (column, alias) = if agg.args.is_empty()
                        || agg.args.iter().any(
                            |arg| matches!(arg, SelectExpression::Column(c) if c.column == "*"),
                        ) {
                        ("*".to_string(), format!("{:?}(*)", agg.function))
                    } else if let Some(col_name) = agg
                        .args
                        .first()
                        .and_then(|arg| self.extract_column_name(arg))
                    {
                        (col_name.clone(), format!("{:?}_{}", agg.function, col_name))
                    } else {
                        ("*".to_string(), format!("{:?}", agg.function))
                    };

                    aggregates.push(AggregateComputation {
                        function: agg.function.clone(),
                        column,
                        alias,
                        distinct: agg.distinct,
                    });
                }
            }
        }
        Ok(AggregationPlan {
            group_by_columns,
            aggregates,
        })
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
