//! Advanced CQL SELECT Abstract Syntax Tree
//!
//! This module defines comprehensive AST types for CQL SELECT statements that enable
//! the FIRST EVER direct querying of SSTable files without Cassandra.
//!
//! Features supported:
//! - Complex WHERE clauses with all comparison operators
//! - Aggregation functions (COUNT, SUM, AVG, MIN, MAX)
//! - GROUP BY and HAVING clauses
//! - ORDER BY with multiple columns
//! - Collection operations (list[index], map['key'], set contains)
//! - Advanced predicates (BETWEEN, IN, LIKE, regex patterns)
//! - Subqueries and joins (for future implementation)

use crate::{TableId, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Complete SELECT statement AST
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    /// SELECT clause - what to return
    pub select_clause: SelectClause,
    /// FROM clause - which table(s) to query
    pub from_clause: FromClause,
    /// WHERE clause - filtering conditions
    pub where_clause: Option<WhereExpression>,
    /// GROUP BY clause - grouping columns
    pub group_by: Option<GroupByClause>,
    /// HAVING clause - filtering after grouping
    pub having_clause: Option<WhereExpression>,
    /// ORDER BY clause - sorting specification
    pub order_by: Option<OrderByClause>,
    /// LIMIT clause - result size limitation
    pub limit: Option<LimitClause>,
    /// OFFSET clause - result pagination
    pub offset: Option<u64>,
    /// Allow filtering flag (for non-indexed queries)
    pub allow_filtering: bool,
}

/// SELECT clause - defines what columns/expressions to return
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectClause {
    /// SELECT * - all columns
    All,
    /// SELECT column1, column2, ... - specific columns
    Columns(Vec<SelectExpression>),
    /// SELECT DISTINCT column1, column2, ... - unique values only
    Distinct(Vec<SelectExpression>),
}

/// Expression in SELECT clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectExpression {
    /// Simple column reference
    Column(ColumnRef),
    /// Aggregate function
    Aggregate(AggregateFunction),
    /// Scalar function
    Function(FunctionCall),
    /// Literal value
    Literal(Value),
    /// Collection access (list[0], map['key'])
    CollectionAccess(CollectionAccessExpression),
    /// Arithmetic expression
    Arithmetic(ArithmeticExpression),
    /// Aliased expression (expr AS alias)
    Aliased(Box<SelectExpression>, String),
}

/// Column reference with optional table qualifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Table name (optional for simple queries)
    pub table: Option<String>,
    /// Column name
    pub column: String,
}

/// Aggregate function call
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateFunction {
    /// Function name (COUNT, SUM, AVG, MIN, MAX)
    pub function: AggregateType,
    /// Arguments (usually column references)
    pub args: Vec<SelectExpression>,
    /// DISTINCT modifier
    pub distinct: bool,
}

/// Types of aggregate functions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    // Future: StdDev, Variance, etc.
}

/// Scalar function call
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    /// Function name
    pub name: String,
    /// Arguments
    pub args: Vec<SelectExpression>,
}

/// Collection access operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CollectionAccessExpression {
    /// List element access: list[index]
    ListIndex(ColumnRef, Box<SelectExpression>),
    /// Map value access: map['key']
    MapKey(ColumnRef, Box<SelectExpression>),
    /// Set membership test: value IN set_column
    SetContains(ColumnRef, Box<SelectExpression>),
}

/// Arithmetic expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArithmeticExpression {
    /// Left operand
    pub left: Box<SelectExpression>,
    /// Operator
    pub operator: ArithmeticOperator,
    /// Right operand
    pub right: Box<SelectExpression>,
}

/// Arithmetic operators
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// FROM clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FromClause {
    /// Single table
    Table(TableId),
    /// Table with alias
    TableAlias(TableId, String),
    /// JOIN operations (for future implementation)
    Join(JoinExpression),
}

/// JOIN expressions (for future implementation)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinExpression {
    /// Left table/join
    pub left: Box<FromClause>,
    /// JOIN type
    pub join_type: JoinType,
    /// Right table
    pub right: Box<FromClause>,
    /// JOIN condition
    pub condition: WhereExpression,
}

/// Types of JOINs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

/// Advanced WHERE expression tree
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WhereExpression {
    /// Simple comparison
    Comparison(ComparisonExpression),
    /// Logical AND
    And(Vec<WhereExpression>),
    /// Logical OR  
    Or(Vec<WhereExpression>),
    /// Logical NOT
    Not(Box<WhereExpression>),
    /// Parenthesized expression
    Parentheses(Box<WhereExpression>),
}

/// Comparison expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComparisonExpression {
    /// Left side (usually column)
    pub left: SelectExpression,
    /// Comparison operator
    pub operator: ComparisonOperator,
    /// Right side (value, column, or expression)
    pub right: ComparisonRightSide,
}

/// Right side of comparison
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonRightSide {
    /// Single value
    Value(SelectExpression),
    /// List of values for IN/NOT IN
    ValueList(Vec<SelectExpression>),
    /// Range for BETWEEN
    Range(SelectExpression, SelectExpression),
}

/// Comparison operators
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    /// Equality
    Equal,
    /// Inequality
    NotEqual,
    /// Less than
    LessThan,
    /// Less than or equal
    LessThanOrEqual,
    /// Greater than
    GreaterThan,
    /// Greater than or equal
    GreaterThanOrEqual,
    /// IN operator
    In,
    /// NOT IN operator
    NotIn,
    /// LIKE operator (pattern matching)
    Like,
    /// NOT LIKE operator
    NotLike,
    /// BETWEEN operator
    Between,
    /// NOT BETWEEN operator
    NotBetween,
    /// IS NULL
    IsNull,
    /// IS NOT NULL
    IsNotNull,
    /// Regular expression matching
    Regex,
    /// Collection CONTAINS
    Contains,
    /// Collection CONTAINS KEY
    ContainsKey,
}

/// GROUP BY clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupByClause {
    /// Columns to group by
    pub columns: Vec<ColumnRef>,
}

/// ORDER BY clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByClause {
    /// Order specifications
    pub items: Vec<OrderByItem>,
}

/// Individual ORDER BY item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    /// Expression to order by
    pub expression: SelectExpression,
    /// Sort direction
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// LIMIT clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitClause {
    /// Maximum number of rows
    pub count: u64,
    /// Per-partition limit (Cassandra-specific)
    pub per_partition: bool,
}

impl SelectStatement {
    /// Create a simple SELECT * FROM table statement
    pub fn select_all_from(table: TableId) -> Self {
        Self {
            select_clause: SelectClause::All,
            from_clause: FromClause::Table(table),
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            offset: None,
            allow_filtering: false,
        }
    }

    /// Check if this query requires aggregation
    pub fn requires_aggregation(&self) -> bool {
        self.group_by.is_some() || self.has_aggregate_functions()
    }

    /// Check if this query has aggregate functions
    pub fn has_aggregate_functions(&self) -> bool {
        match &self.select_clause {
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => {
                exprs.iter().any(|expr| expr.is_aggregate())
            }
            SelectClause::All => false,
        }
    }

    /// Get all referenced columns (for query planning)
    pub fn get_referenced_columns(&self) -> Vec<ColumnRef> {
        let mut columns = Vec::new();

        // Columns from SELECT clause
        match &self.select_clause {
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => {
                for expr in exprs {
                    columns.extend(expr.get_column_refs());
                }
            }
            SelectClause::All => {
                // Will be resolved during planning
            }
        }

        // Columns from WHERE clause
        if let Some(where_expr) = &self.where_clause {
            columns.extend(where_expr.get_column_refs());
        }

        // Columns from GROUP BY
        if let Some(group_by) = &self.group_by {
            columns.extend(group_by.columns.clone());
        }

        // Columns from HAVING
        if let Some(having) = &self.having_clause {
            columns.extend(having.get_column_refs());
        }

        // Columns from ORDER BY
        if let Some(order_by) = &self.order_by {
            for item in &order_by.items {
                columns.extend(item.expression.get_column_refs());
            }
        }

        columns
    }
}

impl SelectExpression {
    /// Check if this expression is an aggregate function
    pub fn is_aggregate(&self) -> bool {
        matches!(self, SelectExpression::Aggregate(_))
    }

    /// Get all column references in this expression
    pub fn get_column_refs(&self) -> Vec<ColumnRef> {
        match self {
            SelectExpression::Column(col_ref) => vec![col_ref.clone()],
            SelectExpression::Aggregate(agg) => {
                let mut refs = Vec::new();
                for arg in &agg.args {
                    refs.extend(arg.get_column_refs());
                }
                refs
            }
            SelectExpression::Function(func) => {
                let mut refs = Vec::new();
                for arg in &func.args {
                    refs.extend(arg.get_column_refs());
                }
                refs
            }
            SelectExpression::CollectionAccess(access) => match access {
                CollectionAccessExpression::ListIndex(col_ref, index_expr) => {
                    let mut refs = vec![col_ref.clone()];
                    refs.extend(index_expr.get_column_refs());
                    refs
                }
                CollectionAccessExpression::MapKey(col_ref, key_expr) => {
                    let mut refs = vec![col_ref.clone()];
                    refs.extend(key_expr.get_column_refs());
                    refs
                }
                CollectionAccessExpression::SetContains(col_ref, value_expr) => {
                    let mut refs = vec![col_ref.clone()];
                    refs.extend(value_expr.get_column_refs());
                    refs
                }
            },
            SelectExpression::Arithmetic(arith) => {
                let mut refs = Vec::new();
                refs.extend(arith.left.get_column_refs());
                refs.extend(arith.right.get_column_refs());
                refs
            }
            SelectExpression::Aliased(expr, _) => expr.get_column_refs(),
            SelectExpression::Literal(_) => Vec::new(),
        }
    }
}

impl WhereExpression {
    /// Get all column references in this WHERE expression
    pub fn get_column_refs(&self) -> Vec<ColumnRef> {
        match self {
            WhereExpression::Comparison(comp) => {
                let mut refs = comp.left.get_column_refs();
                match &comp.right {
                    ComparisonRightSide::Value(expr) => {
                        refs.extend(expr.get_column_refs());
                    }
                    ComparisonRightSide::ValueList(exprs) => {
                        for expr in exprs {
                            refs.extend(expr.get_column_refs());
                        }
                    }
                    ComparisonRightSide::Range(start, end) => {
                        refs.extend(start.get_column_refs());
                        refs.extend(end.get_column_refs());
                    }
                }
                refs
            }
            WhereExpression::And(exprs) | WhereExpression::Or(exprs) => {
                let mut refs = Vec::new();
                for expr in exprs {
                    refs.extend(expr.get_column_refs());
                }
                refs
            }
            WhereExpression::Not(expr) | WhereExpression::Parentheses(expr) => {
                expr.get_column_refs()
            }
        }
    }

    /// Check if this WHERE expression can be pushed down to SSTable level
    pub fn can_pushdown_to_sstable(&self) -> bool {
        match self {
            WhereExpression::Comparison(comp) => {
                // Can pushdown simple column comparisons
                matches!(comp.left, SelectExpression::Column(_))
                    && matches!(
                        comp.operator,
                        ComparisonOperator::Equal
                            | ComparisonOperator::LessThan
                            | ComparisonOperator::LessThanOrEqual
                            | ComparisonOperator::GreaterThan
                            | ComparisonOperator::GreaterThanOrEqual
                            | ComparisonOperator::In
                            | ComparisonOperator::Between
                    )
            }
            WhereExpression::And(exprs) => {
                // Can pushdown if all sub-expressions can be pushed down
                exprs.iter().all(|expr| expr.can_pushdown_to_sstable())
            }
            WhereExpression::Or(_) => {
                // OR expressions are harder to pushdown efficiently
                false
            }
            WhereExpression::Not(_) => {
                // NOT expressions require full scan
                false
            }
            WhereExpression::Parentheses(expr) => expr.can_pushdown_to_sstable(),
        }
    }
}

impl ColumnRef {
    /// Create a simple column reference
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            table: None,
            column: column.into(),
        }
    }

    /// Create a qualified column reference
    pub fn qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            column: column.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_statement() {
        let stmt = SelectStatement::select_all_from(TableId::new("users"));
        assert_eq!(stmt.select_clause, SelectClause::All);
        assert!(!stmt.requires_aggregation());
    }

    #[test]
    fn test_aggregate_detection() {
        let stmt = SelectStatement {
            select_clause: SelectClause::Columns(vec![SelectExpression::Aggregate(
                AggregateFunction {
                    function: AggregateType::Count,
                    args: vec![SelectExpression::Column(ColumnRef::new("id"))],
                    distinct: false,
                },
            )]),
            from_clause: FromClause::Table(TableId::new("users")),
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            offset: None,
            allow_filtering: false,
        };

        assert!(stmt.requires_aggregation());
        assert!(stmt.has_aggregate_functions());
    }

    #[test]
    fn test_column_references() {
        let where_expr = WhereExpression::And(vec![
            WhereExpression::Comparison(ComparisonExpression {
                left: SelectExpression::Column(ColumnRef::new("age")),
                operator: ComparisonOperator::GreaterThan,
                right: ComparisonRightSide::Value(SelectExpression::Literal(Value::Integer(21))),
            }),
            WhereExpression::Comparison(ComparisonExpression {
                left: SelectExpression::Column(ColumnRef::new("city")),
                operator: ComparisonOperator::Equal,
                right: ComparisonRightSide::Value(SelectExpression::Literal(Value::Text(
                    "NYC".to_string(),
                ))),
            }),
        ]);

        let column_refs = where_expr.get_column_refs();
        assert_eq!(column_refs.len(), 2);
        assert!(column_refs.iter().any(|col| col.column == "age"));
        assert!(column_refs.iter().any(|col| col.column == "city"));
    }

    #[test]
    fn test_pushdown_capability() {
        let simple_comparison = WhereExpression::Comparison(ComparisonExpression {
            left: SelectExpression::Column(ColumnRef::new("id")),
            operator: ComparisonOperator::Equal,
            right: ComparisonRightSide::Value(SelectExpression::Literal(Value::Integer(123))),
        });

        assert!(simple_comparison.can_pushdown_to_sstable());

        let complex_or =
            WhereExpression::Or(vec![simple_comparison.clone(), simple_comparison.clone()]);

        assert!(!complex_or.can_pushdown_to_sstable());
    }
}
