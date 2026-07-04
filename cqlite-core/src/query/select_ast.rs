//! CQL SELECT Abstract Syntax Tree.
//!
//! AST types for SELECT statements executed directly against SSTable files.
//! Covers projections, WHERE expressions, aggregates, GROUP BY/HAVING,
//! ORDER BY, LIMIT/OFFSET, collection access, and arithmetic expressions.

use crate::{Error, Result, TableId, Value};
use serde::{Deserialize, Serialize};

/// Complete SELECT statement AST
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    /// SELECT clause - what to return
    pub select_clause: SelectClause,
    /// FROM clause - which table(s) to query (optional for constant expressions)
    pub from_clause: Option<FromClause>,
    /// WHERE clause - filtering conditions
    pub where_clause: Option<WhereExpression>,
    /// GROUP BY clause - grouping columns
    pub group_by: Option<GroupByClause>,
    /// HAVING clause - filtering after grouping
    pub having_clause: Option<WhereExpression>,
    /// ORDER BY clause - sorting specification
    pub order_by: Option<OrderByClause>,
    /// LIMIT clause - query-wide result size limitation
    pub limit: Option<LimitClause>,
    /// PER PARTITION LIMIT - cap on rows returned per partition, applied
    /// before the query-wide `limit` (Cassandra semantics, Issue #757)
    pub per_partition_limit: Option<u64>,
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
    /// `WRITETIME(col)` or `TTL(col)` — first-class metadata-retrieval functions.
    ///
    /// Using a dedicated variant avoids downstream string-matching on the function
    /// name and keeps the executor dispatch explicit and exhaustive.
    WriteTimeTtl(WriteTimeTtlCall),
    /// Literal value
    Literal(Value),
    /// Positional bind marker (`?`) carrying its 0-based index in the statement.
    ///
    /// Issue #961: produced by the SELECT parser whenever it encounters a `?`
    /// placeholder in a value position (e.g. the RHS of a WHERE comparison). It
    /// is a *transient* node: parameter binding
    /// (`bind_parameters`) rewrites every `BindMarker(i)` into the corresponding
    /// `Literal(params[i])` before the statement reaches the optimizer or
    /// executor. Reaching execution with an unbound marker is a logic error and
    /// surfaces as a query-execution error rather than a panic.
    BindMarker(usize),
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
}

/// Scalar function call
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    /// Function name
    pub name: String,
    /// Arguments
    pub args: Vec<SelectExpression>,
}

/// The two metadata-retrieval functions Cassandra exposes in SELECT.
///
/// These are first-class variants rather than being folded into `FunctionCall`
/// so the executor can dispatch on them without string-matching function names.
///
/// # Executor TODO (#692)
/// Evaluation is not yet wired: the executor must thread `writetime` / `ttl`
/// cell-level metadata from `SSTableReader` up through the row-scanning loop
/// and then return `Value::BigInt(micros)` / `Value::Int(seconds)` respectively.
/// Until that work lands, selecting these columns returns `Value::Null`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WriteTimeTtlFunction {
    /// `WRITETIME(col)` — returns the write timestamp in microseconds (bigint)
    WriteTime,
    /// `TTL(col)` — returns the remaining TTL in seconds (int), or NULL if no TTL
    Ttl,
}

/// A parsed `WRITETIME(col)` or `TTL(col)` select item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteTimeTtlCall {
    /// Which function was written
    pub function: WriteTimeTtlFunction,
    /// The single column argument (case-preserved from the source text)
    pub column: String,
    /// Optional alias (`WRITETIME(col) AS wt`)
    pub alias: Option<String>,
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

/// FROM clause. Cassandra CQL only supports single-table queries (no JOINs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FromClause {
    /// Single table
    Table(TableId),
    /// Table with alias (Cassandra CQL supports table aliases)
    TableAlias(TableId, String),
}

/// Advanced WHERE expression tree
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
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
}

impl SelectStatement {
    /// Create a simple SELECT * FROM table statement
    pub fn select_all_from(table: TableId) -> Self {
        Self {
            select_clause: SelectClause::All,
            from_clause: Some(FromClause::Table(table)),
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
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

    /// Count the positional `?` bind markers in this statement.
    ///
    /// Issue #961: the marker indices are assigned left-to-right by the parser,
    /// so the highest index plus one equals the required parameter count. Used by
    /// `execute_with_params` / prepared execution to enforce a strict arity check
    /// before binding.
    pub fn bind_marker_count(&self) -> usize {
        let mut max_plus_one = 0usize;
        if let SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) = &self.select_clause {
            for expr in exprs {
                expr.scan_bind_markers(&mut max_plus_one);
            }
        }
        if let Some(where_expr) = &self.where_clause {
            where_expr.scan_bind_markers(&mut max_plus_one);
        }
        if let Some(having) = &self.having_clause {
            having.scan_bind_markers(&mut max_plus_one);
        }
        max_plus_one
    }

    /// Substitute positional `?` bind markers with `params`, in place.
    ///
    /// Issue #961: each `SelectExpression::BindMarker(i)` is rewritten to
    /// `SelectExpression::Literal(params[i].clone())`. The supplied parameter
    /// count must exactly equal `bind_marker_count()`; too few or too many is a
    /// hard error (strict CQL arity). Binding happens *before* optimization, so
    /// the bound literals participate in partition-key classification, encoding,
    /// and typed coercion exactly as if they had been written inline.
    pub fn bind_parameters(&mut self, params: &[Value]) -> Result<()> {
        let expected = self.bind_marker_count();
        if params.len() != expected {
            return Err(Error::query_execution(format!(
                "Parameter count mismatch: query has {expected} bind marker(s), got {} parameter(s)",
                params.len()
            )));
        }
        if let SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) =
            &mut self.select_clause
        {
            for expr in exprs.iter_mut() {
                expr.bind_parameters(params)?;
            }
        }
        if let Some(where_expr) = &mut self.where_clause {
            where_expr.bind_parameters(params)?;
        }
        if let Some(having) = &mut self.having_clause {
            having.bind_parameters(params)?;
        }
        Ok(())
    }

    /// Get all referenced columns (for query planning).
    ///
    /// `SELECT *` contributes nothing here; the projection is resolved later
    /// against the schema during planning.
    pub fn get_referenced_columns(&self) -> Vec<ColumnRef> {
        let mut columns = Vec::new();

        if let SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) = &self.select_clause {
            for expr in exprs {
                columns.extend(expr.get_column_refs());
            }
        }

        if let Some(where_expr) = &self.where_clause {
            columns.extend(where_expr.get_column_refs());
        }

        if let Some(group_by) = &self.group_by {
            columns.extend(group_by.columns.iter().cloned());
        }

        if let Some(having) = &self.having_clause {
            columns.extend(having.get_column_refs());
        }

        if let Some(order_by) = &self.order_by {
            for item in &order_by.items {
                columns.extend(item.expression.get_column_refs());
            }
        }

        columns
    }
}

impl SelectExpression {
    /// Check if this expression is an aggregate function.
    ///
    /// An aliased aggregate (`COUNT(*) AS total`) is still an aggregate: unwrap
    /// `Aliased` so `SELECT COUNT(*) AS total` is planned through the aggregation
    /// step rather than falling into row-level projection (issue #1763).
    pub fn is_aggregate(&self) -> bool {
        match self {
            SelectExpression::Aggregate(_) => true,
            SelectExpression::Aliased(inner, _) => inner.is_aggregate(),
            _ => false,
        }
    }

    /// Update `max_plus_one` to `max(current, marker_index + 1)` over every
    /// `BindMarker` reachable from this expression (Issue #961).
    fn scan_bind_markers(&self, max_plus_one: &mut usize) {
        match self {
            SelectExpression::BindMarker(idx) => *max_plus_one = (*max_plus_one).max(idx + 1),
            SelectExpression::Aggregate(agg) => {
                for arg in &agg.args {
                    arg.scan_bind_markers(max_plus_one);
                }
            }
            SelectExpression::Function(func) => {
                for arg in &func.args {
                    arg.scan_bind_markers(max_plus_one);
                }
            }
            SelectExpression::CollectionAccess(access) => {
                let (_, sub) = match access {
                    CollectionAccessExpression::ListIndex(c, e)
                    | CollectionAccessExpression::MapKey(c, e)
                    | CollectionAccessExpression::SetContains(c, e) => (c, e),
                };
                sub.scan_bind_markers(max_plus_one);
            }
            SelectExpression::Arithmetic(arith) => {
                arith.left.scan_bind_markers(max_plus_one);
                arith.right.scan_bind_markers(max_plus_one);
            }
            SelectExpression::Aliased(expr, _) => expr.scan_bind_markers(max_plus_one),
            SelectExpression::Column(_)
            | SelectExpression::Literal(_)
            | SelectExpression::WriteTimeTtl(_) => {}
        }
    }

    /// Replace each `BindMarker(i)` reachable from this expression with
    /// `Literal(params[i])` (Issue #961). Caller guarantees `params` covers every
    /// marker index (`SelectStatement::bind_parameters` validates the count).
    fn bind_parameters(&mut self, params: &[Value]) -> Result<()> {
        match self {
            SelectExpression::BindMarker(idx) => {
                let value = params.get(*idx).ok_or_else(|| {
                    Error::query_execution(format!(
                        "Bind marker index {idx} has no corresponding parameter"
                    ))
                })?;
                *self = SelectExpression::Literal(value.clone());
            }
            SelectExpression::Aggregate(agg) => {
                for arg in agg.args.iter_mut() {
                    arg.bind_parameters(params)?;
                }
            }
            SelectExpression::Function(func) => {
                for arg in func.args.iter_mut() {
                    arg.bind_parameters(params)?;
                }
            }
            SelectExpression::CollectionAccess(access) => {
                let sub = match access {
                    CollectionAccessExpression::ListIndex(_, e)
                    | CollectionAccessExpression::MapKey(_, e)
                    | CollectionAccessExpression::SetContains(_, e) => e,
                };
                sub.bind_parameters(params)?;
            }
            SelectExpression::Arithmetic(arith) => {
                arith.left.bind_parameters(params)?;
                arith.right.bind_parameters(params)?;
            }
            SelectExpression::Aliased(expr, _) => expr.bind_parameters(params)?,
            SelectExpression::Column(_)
            | SelectExpression::Literal(_)
            | SelectExpression::WriteTimeTtl(_) => {}
        }
        Ok(())
    }

    /// Get all column references in this expression
    pub fn get_column_refs(&self) -> Vec<ColumnRef> {
        match self {
            SelectExpression::Column(col_ref) => vec![col_ref.clone()],
            SelectExpression::Aggregate(agg) => collect_refs(&agg.args),
            SelectExpression::Function(func) => collect_refs(&func.args),
            SelectExpression::WriteTimeTtl(call) => {
                vec![ColumnRef::new(call.column.clone())]
            }
            SelectExpression::CollectionAccess(access) => {
                let (col_ref, sub_expr) = match access {
                    CollectionAccessExpression::ListIndex(c, e)
                    | CollectionAccessExpression::MapKey(c, e)
                    | CollectionAccessExpression::SetContains(c, e) => (c, e),
                };
                let mut refs = vec![col_ref.clone()];
                refs.extend(sub_expr.get_column_refs());
                refs
            }
            SelectExpression::Arithmetic(arith) => {
                let mut refs = arith.left.get_column_refs();
                refs.extend(arith.right.get_column_refs());
                refs
            }
            SelectExpression::Aliased(expr, _) => expr.get_column_refs(),
            SelectExpression::Literal(_) | SelectExpression::BindMarker(_) => Vec::new(),
        }
    }
}

/// Collect column refs from each expression in `exprs`, in order.
fn collect_refs(exprs: &[SelectExpression]) -> Vec<ColumnRef> {
    exprs
        .iter()
        .flat_map(SelectExpression::get_column_refs)
        .collect()
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
                        refs.extend(collect_refs(exprs));
                    }
                    ComparisonRightSide::Range(start, end) => {
                        refs.extend(start.get_column_refs());
                        refs.extend(end.get_column_refs());
                    }
                }
                refs
            }
            WhereExpression::And(exprs) | WhereExpression::Or(exprs) => exprs
                .iter()
                .flat_map(WhereExpression::get_column_refs)
                .collect(),
            WhereExpression::Not(expr) | WhereExpression::Parentheses(expr) => {
                expr.get_column_refs()
            }
        }
    }

    /// Update `max_plus_one` to cover every `BindMarker` in this WHERE tree
    /// (Issue #961). Markers may appear on either side of a comparison.
    fn scan_bind_markers(&self, max_plus_one: &mut usize) {
        match self {
            WhereExpression::Comparison(comp) => {
                comp.left.scan_bind_markers(max_plus_one);
                match &comp.right {
                    ComparisonRightSide::Value(expr) => expr.scan_bind_markers(max_plus_one),
                    ComparisonRightSide::ValueList(exprs) => {
                        for expr in exprs {
                            expr.scan_bind_markers(max_plus_one);
                        }
                    }
                    ComparisonRightSide::Range(start, end) => {
                        start.scan_bind_markers(max_plus_one);
                        end.scan_bind_markers(max_plus_one);
                    }
                }
            }
            WhereExpression::And(exprs) | WhereExpression::Or(exprs) => {
                for expr in exprs {
                    expr.scan_bind_markers(max_plus_one);
                }
            }
            WhereExpression::Not(expr) | WhereExpression::Parentheses(expr) => {
                expr.scan_bind_markers(max_plus_one)
            }
        }
    }

    /// Replace each `BindMarker(i)` in this WHERE tree with `Literal(params[i])`
    /// (Issue #961). Markers may appear on either side of a comparison and inside
    /// `IN` value lists / `BETWEEN` ranges.
    fn bind_parameters(&mut self, params: &[Value]) -> Result<()> {
        match self {
            WhereExpression::Comparison(comp) => {
                comp.left.bind_parameters(params)?;
                match &mut comp.right {
                    ComparisonRightSide::Value(expr) => expr.bind_parameters(params)?,
                    ComparisonRightSide::ValueList(exprs) => {
                        for expr in exprs.iter_mut() {
                            expr.bind_parameters(params)?;
                        }
                    }
                    ComparisonRightSide::Range(start, end) => {
                        start.bind_parameters(params)?;
                        end.bind_parameters(params)?;
                    }
                }
            }
            WhereExpression::And(exprs) | WhereExpression::Or(exprs) => {
                for expr in exprs.iter_mut() {
                    expr.bind_parameters(params)?;
                }
            }
            WhereExpression::Not(expr) | WhereExpression::Parentheses(expr) => {
                expr.bind_parameters(params)?
            }
        }
        Ok(())
    }

    /// Check if this WHERE expression can be pushed down to SSTable level.
    ///
    /// OR and NOT are excluded: efficient pushdown of those would require
    /// index intersection / negative scans we don't currently support.
    pub fn can_pushdown_to_sstable(&self) -> bool {
        match self {
            WhereExpression::Comparison(comp) => {
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
                exprs.iter().all(WhereExpression::can_pushdown_to_sstable)
            }
            WhereExpression::Or(_) | WhereExpression::Not(_) => false,
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
            from_clause: Some(FromClause::Table(TableId::new("users"))),
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
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
